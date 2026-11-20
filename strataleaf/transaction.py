import math
from dataclasses import dataclass, field
from .codec import digest, dumps, key as checked_key, loads, name
from .errors import Conflict, InvalidDocument
from .triggers import TriggerEngine


@dataclass(frozen=True)
class CommitResult:
    revision: int
    count: int
    replayed: bool = False
    derived: int = 0
    trace: tuple = field(default_factory=tuple)


class WriteBatch:
    def __init__(self, store, request_key=None):
        self.store = store
        self.request_key = checked_key(request_key) if request_key is not None else None
        self.operations = []
        self.state = "open"

    def _append(self, operation):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        identity = (operation["collection"], operation["key"])
        if any((op["collection"], op["key"]) == identity for op in self.operations):
            raise InvalidDocument("one operation per document per batch")
        expected = operation["expected"]
        if expected is not None and (isinstance(expected, bool) or not isinstance(expected, int) or expected < 0):
            raise InvalidDocument("expected revision must be nonnegative or None")
        self.operations.append(operation)
        return self

    def put(self, collection, key, document, expected=None, expires_at=None):
        name(collection)
        checked_key(key)
        if not isinstance(document, dict):
            raise InvalidDocument("document must be an object")
        if expires_at is not None and (isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at)):
            raise InvalidDocument("expires_at must be a finite timestamp")
        return self._append({"collection": collection, "key": key, "document": loads(dumps(document)),
                             "expected": expected, "expires_at": expires_at})

    def delete(self, collection, key, expected=None):
        return self._append({"collection": name(collection), "key": checked_key(key), "document": None,
                             "expected": expected, "expires_at": None})

    def abort(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        self.operations.clear()
        self.state = "aborted"

    def commit(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        if not self.operations or len(self.operations) > 10000:
            raise InvalidDocument("batch must contain between 1 and 10000 operations")
        user_operations = [{**op, "action": "upsert" if op["document"] is not None else "delete",
                            "origin": None} for op in self.operations]
        fingerprint = digest(self.operations)
        try:
            with self.store.atomic(writable=True) as connection:
                if self.request_key is not None:
                    prior = connection.execute("SELECT * FROM receipts WHERE request_key=?", (self.request_key,)).fetchone()
                    if prior:
                        if prior["request_hash"] != fingerprint:
                            raise Conflict("request key was used with different operations")
                        self.state = "committed"
                        # The commit payload may have been reaped by retention, but
                        # receipts survive and remain the authoritative replay record.
                        logged = connection.execute("SELECT payload FROM commits WHERE rev=?",
                                                    (prior["rev"],)).fetchone()
                        if logged is not None:
                            payload = loads(logged[0])
                            derived, trace = payload.get("derived", 0), tuple(payload.get("trace", ()))
                        else:
                            derived, trace = 0, ()
                        return CommitResult(prior["rev"], prior["count"], True, derived, trace)
                head = int(self.store.get("head"))
                # Rule propagation runs on the same connection before any row is
                # written, so schema/unique/depth failures leave nothing behind.
                engine = TriggerEngine(connection, self.store)
                derived, trace = engine.run(user_operations)
                revision = head + 1
                all_operations = engine.apply(revision, head, user_operations + derived)
                payload = {"revision": revision, "operations": all_operations,
                           "user_count": len(self.operations), "derived": len(derived), "trace": trace}
                connection.execute("INSERT INTO commits VALUES (?,?,?,?)",
                                   (revision, dumps(payload), self.request_key, fingerprint))
                if self.request_key is not None:
                    connection.execute("INSERT INTO receipts VALUES (?,?,?,?)",
                                       (self.request_key, revision, fingerprint, len(self.operations)))
                self.store.set("head", revision)
        except BaseException:
            self.state = "aborted"
            raise
        self.state = "committed"
        return CommitResult(revision, len(self.operations), False, len(derived), tuple(trace))
