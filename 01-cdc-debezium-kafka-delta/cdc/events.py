"""Turns Debezium change events into rows ready to merge."""
from dataclasses import dataclass

COLUMNS = ("order_id", "store_id", "item_id", "quantity", "status")
UPSERT_OPS = {"c", "u", "r"}  # create, update, snapshot read


class MalformedEvent(ValueError):
    pass


@dataclass(frozen=True)
class Change:
    order_id: int
    deleted: bool
    row: dict


def parse_event(value):
    """Returns a Change, or None for a tombstone, which carries no information of its own."""
    if value is None:
        return None
    payload = value.get("payload", value)
    if payload is None:
        return None
    op = payload.get("op")
    if op == "d":
        before = payload.get("before") or {}
        if "order_id" not in before:
            raise MalformedEvent("delete event has no key in 'before'")
        return Change(before["order_id"], True, {})
    if op in UPSERT_OPS:
        after = payload.get("after") or {}
        missing = [column for column in COLUMNS if column not in after]
        if missing:
            raise MalformedEvent(f"'{op}' event is missing columns: {missing}")
        return Change(after["order_id"], False, {column: after[column] for column in COLUMNS})
    raise MalformedEvent(f"unknown op: {op!r}")


def latest_per_key(changes):
    """A batch can touch one key many times. Only its final state matters, and Kafka order gives that."""
    latest = {}
    for change in changes:
        latest[change.order_id] = change
    return list(latest.values())
