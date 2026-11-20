import re

from .codec import dumps, field, loads, name
from .errors import Conflict, InvalidDocument, NotFound
from .predicates import matches, validate_predicate

EVENTS = ("put", "delete")
ACTIONS = ("upsert", "delete", "patch")
# Bounds keep rule evaluation explainable and prevent unbounded fan-out.
MAX_RULE_FIELDS = 32
MAX_TEMPLATE_DEPTH = 8
MAX_TEMPLATE_NODES = 256
KEY_INTERPOLATION = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_.]*|key)\}")


def _template_nodes(template, depth=0):
    if depth > MAX_TEMPLATE_DEPTH:
        raise InvalidDocument("rule template nesting exceeds 8 levels")
    count = 1
    if isinstance(template, dict):
        if len(template) > MAX_RULE_FIELDS:
            raise InvalidDocument("rule template has too many fields")
        for value in template.values():
            count += _template_nodes(value, depth + 1)
    elif isinstance(template, list):
        if len(template) > MAX_RULE_FIELDS:
            raise InvalidDocument("rule template list is too long")
        for value in template:
            count += _template_nodes(value, depth + 1)
    if count > MAX_TEMPLATE_NODES:
        raise InvalidDocument("rule template exceeds 256 JSON nodes")
    return count


def normalize_rule(definition):
    if not isinstance(definition, dict):
        raise InvalidDocument("rule definition must be an object")
    allowed = {"name", "source", "events", "predicate", "action", "target", "target_key",
               "description", "template", "patch"}
    if set(definition) - allowed:
        raise InvalidDocument("rule accepts only: " + ",".join(sorted(allowed)))
    rule_name = name(definition.get("name"))
    source = name(definition.get("source"))
    target = name(definition.get("target"))
    events = definition.get("events", ["put"])
    if not isinstance(events, list) or not events or any(event not in EVENTS for event in events):
        raise InvalidDocument("rule events must be a nonempty subset of put/delete")
    if len(set(events)) != len(events):
        raise InvalidDocument("rule events must not repeat")
    validate_predicate(definition.get("predicate"))
    action = definition.get("action")
    if action not in ACTIONS:
        raise InvalidDocument("rule action must be one of upsert/delete/patch")
    target_key = definition.get("target_key")
    if not isinstance(target_key, str) or not target_key or len(target_key.encode()) > 512:
        raise InvalidDocument("rule target_key must be a nonempty string of at most 512 bytes")
    placeholders = KEY_INTERPOLATION.findall(target_key)
    if len(placeholders) > 8:
        raise InvalidDocument("rule target_key has too many interpolations")
    for placeholder in placeholders:
        if placeholder != "key":
            field({}, placeholder)
    description = definition.get("description", "")
    if not isinstance(description, str) or len(description.encode()) > 2048:
        raise InvalidDocument("rule description must be a string of at most 2 KiB")
    normalized = {"name": rule_name, "source": source, "events": sorted(events),
                  "predicate": definition.get("predicate"), "action": action,
                  "target": target, "target_key": target_key, "description": description}
    if action == "upsert":
        template = definition.get("template")
        if not isinstance(template, dict) or not template:
            raise InvalidDocument("upsert rule requires a nonempty template object")
        _template_nodes(template)
        normalized["template"] = loads(dumps(template))
    elif action == "patch":
        patch_body = definition.get("patch")
        if not isinstance(patch_body, dict) or not patch_body:
            raise InvalidDocument("patch rule requires a nonempty patch object")
        _template_nodes(patch_body)
        normalized["patch"] = loads(dumps(patch_body))
    else:
        normalized["template"] = None
    return normalized


def _resolve(template, context):
    """Substitute {"$field": "path"} / {"$key": true} leaves using event context."""
    if isinstance(template, dict) and set(template) == {"$key"}:
        if template["$key"] is not True:
            raise InvalidDocument("$key reference must be true")
        return context["key"]
    if isinstance(template, dict) and set(template) == {"$field"}:
        path = template["$field"]
        field({}, path)
        value = context["document"]
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return loads(dumps(value))
    if isinstance(template, dict):
        return {key: _resolve(value, context) for key, value in template.items()}
    if isinstance(template, list):
        return [_resolve(value, context) for value in template]
    return template


def _resolve_key(template, context):
    def substitute(match):
        token = match.group(1)
        if token == "key":
            value = context["key"]
        else:
            value = context["document"]
            for part in token.split("."):
                if not isinstance(value, dict) or part not in value:
                    raise InvalidDocument("target_key interpolation missing field: " + token)
                value = value[part]
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise InvalidDocument("target_key interpolation requires a scalar value: " + token)
        return str(value)

    rendered = KEY_INTERPOLATION.sub(substitute, template)
    if not rendered or len(rendered.encode()) > 512:
        raise InvalidDocument("rendered target_key must be a nonempty string of at most 512 bytes")
    return rendered


def build_effect(rule, event, document, key):
    """Pure projection from a matched event to the derived operation body."""
    context = {"document": document if document is not None else {}, "key": key, "event": event}
    target_key = _resolve_key(rule["target_key"], context)
    if rule["action"] == "delete":
        return {"action": "delete", "collection": rule["target"], "key": target_key, "document": None}
    if rule["action"] == "upsert":
        body = _resolve(rule["template"], context)
        if not isinstance(body, dict):
            raise InvalidDocument("rule template must project to an object")
        return {"action": "upsert", "collection": rule["target"], "key": target_key, "document": body}
    body = _resolve(rule["patch"], context)
    if not isinstance(body, dict):
        raise InvalidDocument("rule patch must project to an object")
    return {"action": "patch", "collection": rule["target"], "key": target_key, "document": body}


def rule_matches(rule, event, document):
    if event not in rule["events"]:
        return False
    if event == "delete":
        return rule["predicate"] is None or matches(document or {}, rule["predicate"])
    return document is not None and matches(document, rule["predicate"])


class TriggerRules:
    """Versioned catalog of document change trigger rules."""

    def __init__(self, store):
        self.store = store

    def _bump(self, connection):
        self.store.set("rule_epoch", int(self.store.get("rule_epoch")) + 1)

    def register(self, definition, expected_version=None):
        normalized = normalize_rule(definition)
        rule_name = normalized["name"]
        with self.store.atomic(writable=True) as connection:
            row = connection.execute("SELECT current_version FROM rule_registry WHERE rule_name=?",
                                     (rule_name,)).fetchone()
            if row is None:
                if expected_version is not None:
                    raise Conflict("unknown rule: " + rule_name)
                version = 1
                replacing = None
            else:
                version = row["current_version"] + 1
                replacing = row["current_version"]
                if expected_version is not None and expected_version != replacing:
                    raise Conflict("rule version changed; expected " + str(replacing))
                prior = connection.execute("SELECT state FROM rule_versions WHERE rule_name=? AND version=?",
                                           (rule_name, replacing)).fetchone()
                if prior["state"] != "active":
                    raise Conflict("a disabled rule cannot be replaced; register under a new name")
            epoch = int(self.store.get("rule_epoch")) + 1
            if replacing is not None:
                connection.execute("UPDATE rule_versions SET state='superseded' WHERE rule_name=? AND version=?",
                                   (rule_name, replacing))
            connection.execute("INSERT INTO rule_versions VALUES (?,?,?,?,?)",
                               (rule_name, version, dumps(normalized), "active", epoch))
            connection.execute("INSERT INTO rule_registry VALUES (?,?) ON CONFLICT(rule_name) DO UPDATE SET current_version=excluded.current_version",
                               (rule_name, version))
            self._bump(connection)
        return {"name": rule_name, "version": version, "replaced": replacing, "rule_epoch": epoch}

    def disable(self, rule_name, expected_version=None):
        name(rule_name)
        with self.store.atomic(writable=True) as connection:
            row = connection.execute("SELECT current_version FROM rule_registry WHERE rule_name=?",
                                     (rule_name,)).fetchone()
            if row is None:
                raise NotFound("unknown rule: " + rule_name)
            version = row["current_version"]
            if expected_version is not None and expected_version != version:
                raise Conflict("rule version changed; expected " + str(version))
            state = connection.execute("SELECT state FROM rule_versions WHERE rule_name=? AND version=?",
                                       (rule_name, version)).fetchone()
            if state["state"] != "active":
                raise Conflict("rule is already disabled")
            epoch = int(self.store.get("rule_epoch")) + 1
            connection.execute("UPDATE rule_versions SET state=?,rule_epoch=? WHERE rule_name=? AND version=?",
                               ("disabled", epoch, rule_name, version))
            connection.execute("DELETE FROM rule_registry WHERE rule_name=?", (rule_name,))
            self._bump(connection)
        return {"name": rule_name, "version": version, "rule_epoch": epoch}

    def describe(self, rule_name, version=None):
        name(rule_name)
        with self.store.read() as connection:
            if version is None:
                row = connection.execute("SELECT current_version FROM rule_registry WHERE rule_name=?",
                                         (rule_name,)).fetchone()
                if row is None:
                    raise NotFound("unknown active rule: " + rule_name)
                version = row["current_version"]
            record = connection.execute("SELECT * FROM rule_versions WHERE rule_name=? AND version=?",
                                        (rule_name, version)).fetchone()
            if record is None:
                raise NotFound("unknown rule version: " + rule_name + "#" + str(version))
            return {"rule": loads(record["body"]), "version": record["version"],
                    "state": record["state"], "rule_epoch": record["rule_epoch"]}

    def list(self, include_disabled=False):
        with self.store.read() as connection:
            rows = connection.execute("SELECT * FROM rule_versions ORDER BY rule_name,version").fetchall()
            output = []
            for row in rows:
                if row["state"] != "active" and not include_disabled:
                    continue
                output.append({"rule": loads(row["body"]), "version": row["version"],
                               "state": row["state"], "rule_epoch": row["rule_epoch"]})
            return output

    def history(self, rule_name):
        name(rule_name)
        with self.store.read() as connection:
            rows = connection.execute("SELECT * FROM rule_versions WHERE rule_name=? ORDER BY version",
                                      (rule_name,)).fetchall()
            if not rows:
                raise NotFound("unknown rule: " + rule_name)
            return [{"rule": loads(row["body"]), "version": row["version"],
                     "state": row["state"], "rule_epoch": row["rule_epoch"]} for row in rows]

    def explain(self, rule_name, version=None):
        """Return the static rule plus the collections its effects touch."""
        entry = self.describe(rule_name, version)
        rule = entry["rule"]
        return {**entry, "touches": {"source": rule["source"], "target": rule["target"]},
                "action": rule["action"], "events": rule["events"]}
