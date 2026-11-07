import pytest

from cdc.delta_sink import apply_changes, read_rows
from cdc.events import MalformedEvent, latest_per_key, parse_event


def row(order_id, quantity=1, status="new"):
    return {"order_id": order_id, "store_id": 10, "item_id": 100, "quantity": quantity, "status": status}


def event(op, before=None, after=None):
    return {"op": op, "before": before, "after": after}


def changes(*events):
    return [parse_event(e) for e in events]


def test_snapshot_create_and_update_are_upserts():
    for op in ("r", "c", "u"):
        change = parse_event(event(op, after=row(1)))
        assert not change.deleted and change.row["order_id"] == 1


def test_schema_wrapped_envelope_is_understood():
    assert parse_event({"schema": {}, "payload": event("c", after=row(7))}).order_id == 7


def test_delete_uses_before_image():
    change = parse_event(event("d", before=row(5)))
    assert change.deleted and change.order_id == 5


def test_tombstone_is_ignored():
    assert parse_event(None) is None


@pytest.mark.parametrize(
    "bad",
    [event("x", after=row(1)), event("c", after={"order_id": 1}), event("d", before={})],
)
def test_malformed_events_raise(bad):
    with pytest.raises(MalformedEvent):
        parse_event(bad)


def test_last_change_for_a_key_wins():
    final = latest_per_key(changes(event("c", after=row(1, 1)), event("u", after=row(1, 9)), event("c", after=row(2))))
    assert {c.order_id: c.row["quantity"] for c in final} == {1: 9, 2: 1}


def test_insert_update_delete_reach_delta(tmp_path):
    path = tmp_path / "orders"
    apply_changes(path, changes(event("r", after=row(1)), event("r", after=row(2)), event("r", after=row(3))))
    apply_changes(path, changes(event("u", after=row(1, 50, "shipped")), event("d", before=row(2)), event("c", after=row(4))))
    rows = {r["order_id"]: r for r in read_rows(path)}
    assert sorted(rows) == [1, 3, 4]
    assert rows[1]["quantity"] == 50 and rows[1]["status"] == "shipped"


def test_insert_then_delete_in_one_batch_leaves_no_row(tmp_path):
    path = tmp_path / "orders"
    apply_changes(path, changes(event("c", after=row(1)), event("d", before=row(1))))
    assert read_rows(path) == []


def test_replaying_a_batch_is_idempotent(tmp_path):
    path = tmp_path / "orders"
    batch = changes(event("c", after=row(1)), event("u", after=row(1, 8)), event("c", after=row(2)), event("d", before=row(2)))
    apply_changes(path, batch)
    first = read_rows(path)
    apply_changes(path, batch)
    assert read_rows(path) == first == [row(1, 8)]


def test_empty_batch_is_a_no_op(tmp_path):
    assert apply_changes(tmp_path / "orders", []) == {"upserted": 0, "deleted": 0}
