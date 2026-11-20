from .codec import dumps, loads
from .errors import Conflict, NotFound
from .indexes import refresh_document
from .merging import merge_patch
from .mvcc import latest_row
from .rules import build_effect, rule_matches
from .schema import validate_document

# Hard bounds: a single commit can never cascade without limit.
MAX_RULE_DEPTH = 32
MAX_DERIVED_OPS = 10000
# Deterministic intra-event ordering so folds are reproducible.
ACTION_ORDER = {"upsert": 0, "delete": 0, "patch": 1}


class TriggerEngine:
    """Runs active rules inside the committing transaction.

    The engine only touches the single IMMEDIATE connection owned by the
    committing batch, so user writes and every derived write share one
    atomic revision. Any raised exception rolls the whole transaction back.
    """

    def __init__(self, connection, store):
        self.connection = connection
        self.store = store

    def _active_rules(self):
        rows = self.connection.execute("""SELECT rv.body FROM rule_registry rr
            JOIN rule_versions rv ON rv.rule_name=rr.rule_name AND rv.version=rr.current_version
            WHERE rv.state='active' ORDER BY rv.rule_name, rv.version""").fetchall()
        return [loads(row["body"]) for row in rows]

    def _schema(self, collection):
        row = self.connection.execute("SELECT schema FROM collections WHERE name=?",
                                      (collection,)).fetchone()
        if row is None:
            raise NotFound("unknown collection: " + collection)
        return loads(row[0])

    def run(self, user_operations):
        """Fixed-point worklist propagation; returns durable derived operations.

        Collision policy within one commit:
        * a rule may never write a document the caller itself writes;
        * several rules may cooperatively touch one derived key, but only when
          every follow-up is a patch (folded in deterministic rule order);
        * two whole-document upserts/deletes on one key are ambiguous and rejected.
        """
        rules = self._active_rules()
        rules_by_source = {}
        for rule in rules:
            rules_by_source.setdefault(rule["source"], []).append(rule)

        # Entries are (event, collection, key, document, depth, cause).
        # User operations seed the queue at depth 0.
        queue = []
        for index, operation in enumerate(user_operations):
            event = "delete" if operation["document"] is None else "put"
            queue.append((event, operation["collection"], operation["key"],
                          operation["document"], 0, {"kind": "user", "index": index}))

        user_targets = {(op["collection"], op["key"]) for op in user_operations}
        # One folded derived write per target key, preserving first-write order.
        derived_by_target = {}
        target_order = []
        trace = []
        seen_edges = set()

        while queue:
            event, collection, key, document, depth, cause = queue.pop(0)
            if depth >= MAX_RULE_DEPTH:
                raise Conflict(
                    f"trigger cascade exceeded depth {MAX_RULE_DEPTH} at {collection}/{key}; "
                    "rule graph contains a cycle or is too deep")
            matched = [rule for rule in rules_by_source.get(collection, [])
                       if (rule["name"], event, collection, key, dumps(document)) not in seen_edges
                       and rule_matches(rule, event, document)]
            matched.sort(key=lambda item: (ACTION_ORDER[item["action"]], item["name"]))
            for rule in matched:
                edge_key = (rule["name"], event, collection, key, dumps(document))
                seen_edges.add(edge_key)
                effect = build_effect(rule, event, document, key)
                if len(target_order) >= MAX_DERIVED_OPS:
                    raise Conflict(
                        f"trigger fan-out exceeded {MAX_DERIVED_OPS} derived operations")
                target = (effect["collection"], effect["key"])
                if target in user_targets:
                    raise Conflict(
                        "rule " + rule["name"] + " targets " + effect["collection"] + "/"
                        + effect["key"] + " which the caller writes in the same commit")
                origin = {"rule": rule["name"], "source_collection": collection,
                          "source_key": key, "source_event": event, "depth": depth + 1,
                          "cause": cause}
                trace.append({"rule": rule["name"], "depth": depth + 1, "event": event,
                              "source": {"collection": collection, "key": key},
                              "effect": {"action": effect["action"],
                                         "collection": effect["collection"], "key": effect["key"]},
                              "cause": cause})
                if target not in derived_by_target:
                    durable = {"collection": effect["collection"], "key": effect["key"],
                               "document": effect["document"], "expected": None,
                               "expires_at": None, "action": effect["action"], "origin": origin}
                    derived_by_target[target] = durable
                    target_order.append(target)
                    propagated_document = effect["document"]
                else:
                    existing = derived_by_target[target]
                    if effect["action"] != "patch" or existing["action"] == "delete":
                        raise Conflict(
                            "rules " + existing["origin"]["rule"] + " and " + rule["name"]
                            + " both fully write " + effect["collection"] + "/" + effect["key"]
                            + "; only follow-up patches may fold onto one derived document")
                    # Fold the patch into the already-projected derived document.
                    existing["document"] = merge_patch(existing["document"], effect["document"])
                    existing.setdefault("folded", []).append(rule["name"])
                    propagated_document = existing["document"]
                next_event = "delete" if effect["action"] == "delete" else "put"
                queue.append((next_event, effect["collection"], effect["key"],
                              propagated_document, depth + 1,
                              {"kind": "rule", "rule": rule["name"],
                               "index": target_order.index(target)}))
        return [derived_by_target[target] for target in target_order], trace

    def _resolve(self, operation, head):
        """Turn one logical operation into its final (document, expires) state."""
        collection, key = operation["collection"], operation["key"]
        old = latest_row(self.connection, collection, key, head)
        current = old["rev"] if old is not None else 0
        expected = operation.get("expected")
        if expected is not None and expected != current:
            raise Conflict(
                f"revision conflict for {collection}/{key}: expected {expected}, found {current}")
        old_body = loads(old["body"]) if old is not None and old["body"] is not None else None
        old_expires = old["expires"] if old is not None else None
        document = operation["document"]
        if operation.get("action") == "patch":
            if old_body is None:
                raise NotFound("patch rule target does not exist: " + collection + "/" + key)
            document = merge_patch(old_body, document)
            operation = {**operation, "action": "upsert", "document": document}
        if document is not None:
            validate_document(document, self._schema(collection))
        expires = operation.get("expires_at")
        if expires is None and document is not None:
            expires = old_expires
        return operation, expires

    def apply(self, revision, head, operations):
        """Pre-flight every write, then materialize it against one revision."""
        finalized = []
        for operation in operations:
            final_op, expires = self._resolve(operation, head)
            finalized.append((final_op, expires))
        for final_op, expires in finalized:
            collection, key, document = final_op["collection"], final_op["key"], final_op["document"]
            body = dumps(document) if document is not None else None
            self.connection.execute("INSERT INTO versions VALUES (?,?,?,?,?)",
                                    (collection, key, revision, body, expires))
            refresh_document(self.connection, collection, key, document)
        return [final_op for final_op, _ in finalized]
