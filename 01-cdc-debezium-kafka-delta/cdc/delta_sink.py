"""Applies a batch of changes to a Delta table with MERGE."""
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .events import COLUMNS, latest_per_key

SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("store_id", pa.int64()),
        ("item_id", pa.int64()),
        ("quantity", pa.int64()),
        ("status", pa.string()),
    ]
)
BATCH_SCHEMA = SCHEMA.append(pa.field("_deleted", pa.bool_()))


def _ensure_table(path):
    if not (Path(path) / "_delta_log").exists():
        write_deltalake(str(path), SCHEMA.empty_table())
    return DeltaTable(str(path))


def apply_changes(path, changes):
    """Replaying the same events gives the same table, so at-least-once delivery from Kafka is safe."""
    table = _ensure_table(path)
    final = latest_per_key(changes)
    if not final:
        return {"upserted": 0, "deleted": 0}

    rows = [
        {**{column: change.row.get(column) for column in COLUMNS}, "order_id": change.order_id, "_deleted": change.deleted}
        for change in final
    ]
    assignments = {column: f"s.{column}" for column in COLUMNS}
    (
        table.merge(
            source=pa.Table.from_pylist(rows, schema=BATCH_SCHEMA),
            predicate="t.order_id = s.order_id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_delete(predicate="s._deleted = true")
        .when_matched_update(updates=assignments, predicate="s._deleted = false")
        .when_not_matched_insert(updates=assignments, predicate="s._deleted = false")
        .execute()
    )
    deleted = sum(change.deleted for change in final)
    return {"upserted": len(final) - deleted, "deleted": deleted}


def read_rows(path):
    rows = DeltaTable(str(path)).to_pyarrow_table().to_pylist()
    return sorted(rows, key=lambda row: row["order_id"])
