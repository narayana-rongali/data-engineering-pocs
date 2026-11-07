"""Runs the ADF pattern locally: Lookup the control table, ForEach source, incremental Copy, advance the watermark.

The real pipeline (infra/pipeline.arm.json) does the same with ADF activities.
"""
import sqlite3


def create_control_table(db, sources):
    db.execute(
        "CREATE TABLE IF NOT EXISTS source_control (source_id TEXT PRIMARY KEY, source_table TEXT, "
        "watermark_column TEXT, enabled INTEGER, last_watermark TEXT)"
    )
    for s in sources:
        db.execute(
            "INSERT INTO source_control VALUES (?, ?, ?, ?, '1900-01-01') "
            "ON CONFLICT(source_id) DO UPDATE SET enabled = excluded.enabled",
            (s["source_id"], f"{s['system']}_{s['source_table']}", s["watermark_column"], int(s["enabled"])),
        )


def run_once(db, landed):
    """One pipeline run. `landed` collects copied rows per source, standing in for the lake."""
    copied = {}
    sources = db.execute(
        "SELECT source_id, source_table, watermark_column, last_watermark FROM source_control WHERE enabled = 1"
    ).fetchall()
    for source_id, table, column, watermark in sources:
        # The upper bound is fixed before copying, so rows written during the copy are picked up next run.
        high = db.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
        if high is None or high <= watermark:
            copied[source_id] = 0
            continue
        rows = db.execute(
            f"SELECT * FROM {table} WHERE {column} > ? AND {column} <= ? ORDER BY {column}", (watermark, high)
        ).fetchall()
        landed.setdefault(source_id, []).extend(rows)
        # Advance only after the copy succeeds; a failure leaves the watermark alone and the next run retries.
        db.execute("UPDATE source_control SET last_watermark = ? WHERE source_id = ?", (high, source_id))
        copied[source_id] = len(rows)
    db.commit()
    return copied


def connect():
    return sqlite3.connect(":memory:")
