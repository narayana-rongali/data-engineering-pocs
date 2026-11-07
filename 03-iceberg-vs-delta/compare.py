"""Runs the same workload on Apache Iceberg and Delta Lake, and reports what actually happened."""
import argparse
import json
import os
import shutil
import statistics
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

DAYS = 5
SEED = 42
QUERY_REPEATS = 5


def make_batch(day_index, rows, seed=SEED):
    random = np.random.default_rng(seed + day_index)
    day = (date(2026, 1, 1) + timedelta(days=day_index)).isoformat()
    return pa.table(
        {
            "order_id": pa.array(np.arange(rows, dtype=np.int64) + day_index * rows, pa.int64()),
            "store_id": pa.array(random.integers(1, 500, rows), pa.int64()),
            "item_id": pa.array(random.integers(1, 5000, rows), pa.int64()),
            "amount": pa.array(random.random(rows) * 100, pa.float64()),
            "order_date": pa.array([day] * rows, pa.string()),
        }
    )


def timed(action):
    started = time.perf_counter()
    value = action()
    return value, time.perf_counter() - started


def folder_stats(path):
    files = [p for p in Path(path).rglob("*.parquet") if p.is_file()]
    return {"data_files": len(files), "data_mb": round(sum(p.stat().st_size for p in files) / 1e6, 2)}


def median_ms(action):
    return round(statistics.median(timed(action)[1] for _ in range(QUERY_REPEATS)) * 1000, 2)


def run_delta(root, batches, day):
    path = str(root / "delta_orders")
    write_seconds = 0.0
    for batch in batches:
        write_seconds += timed(lambda b=batch: write_deltalake(path, b, mode="append", partition_by=["order_date"]))[1]

    filtered = lambda: DeltaTable(path).to_pyarrow_dataset().to_table(filter=pc.field("order_date") == day)
    measures = {
        "write_seconds": round(write_seconds, 3),
        "total_rows": DeltaTable(path).to_pyarrow_table().num_rows,
        "filtered_rows": filtered().num_rows,
        "filtered_query_ms": median_ms(filtered),
        "time_travel_first_version_rows": DeltaTable(path, version=0).to_pyarrow_table().num_rows,
    }
    measures.update(folder_stats(path))

    extra = batches[0].append_column("channel", pa.array(["web"] * batches[0].num_rows, pa.string()))
    write_deltalake(path, extra, mode="append", schema_mode="merge", partition_by=["order_date"])
    evolved = DeltaTable(path).to_pyarrow_table()
    measures["rows_after_evolution"] = evolved.num_rows
    measures["old_rows_null_in_new_column"] = evolved["channel"].null_count
    return measures


def run_iceberg(root, batches, day):
    warehouse = root / "iceberg_wh"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{(root / 'catalog.db').as_posix()}",
        # Relative on purpose: PyIceberg mangles Windows file:// URIs, and this form works on every OS.
        warehouse=Path(os.path.relpath(warehouse)).as_posix(),
    )
    catalog.create_namespace("retail")
    schema = Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "store_id", LongType(), required=False),
        NestedField(3, "item_id", LongType(), required=False),
        NestedField(4, "amount", DoubleType(), required=False),
        NestedField(5, "order_date", StringType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=5, field_id=1000, transform=IdentityTransform(), name="order_date"))
    table = catalog.create_table("retail.orders", schema=schema, partition_spec=spec)

    write_seconds = 0.0
    for batch in batches:
        write_seconds += timed(lambda b=batch: table.append(b))[1]

    first_snapshot = table.history()[0].snapshot_id
    filtered = lambda: table.scan(row_filter=EqualTo("order_date", day)).to_arrow()
    measures = {
        "write_seconds": round(write_seconds, 3),
        "total_rows": table.scan().to_arrow().num_rows,
        "filtered_rows": filtered().num_rows,
        "filtered_query_ms": median_ms(filtered),
        "time_travel_first_version_rows": table.scan(snapshot_id=first_snapshot).to_arrow().num_rows,
    }
    measures.update(folder_stats(warehouse))

    with table.update_schema() as update:
        update.add_column("channel", StringType())
    extra = batches[0].append_column("channel", pa.array(["web"] * batches[0].num_rows, pa.string()))
    table.append(extra)
    evolved = table.scan().to_arrow()
    measures["rows_after_evolution"] = evolved.num_rows
    measures["old_rows_null_in_new_column"] = evolved["channel"].null_count
    return measures


def compare(rows_per_day, workdir):
    root = Path(workdir)
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    batches = [make_batch(i, rows_per_day) for i in range(DAYS)]
    day = batches[2]["order_date"][0].as_py()
    return {
        "rows_per_day": rows_per_day,
        "days": DAYS,
        "delta": run_delta(root, batches, day),
        "iceberg": run_iceberg(root, batches, day),
    }


def to_markdown(result):
    lines = [
        f"Workload: {result['days']} daily batches of {result['rows_per_day']:,} synthetic rows, single machine, Python engines.",
        "",
        "| Measure | Delta Lake (delta-rs) | Apache Iceberg (PyIceberg) |",
        "|---|---|---|",
    ]
    lines += [f"| {key.replace('_', ' ')} | {result['delta'][key]} | {result['iceberg'][key]} |" for key in result["delta"]]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows-per-day", type=int, default=1_000_000)
    parser.add_argument("--workdir", default="output/run")
    args = parser.parse_args()
    outcome = compare(args.rows_per_day, args.workdir)
    results = Path(__file__).parent / "results"
    results.mkdir(exist_ok=True)
    (results / "latest.json").write_text(json.dumps(outcome, indent=2))
    (results / "latest.md").write_text(to_markdown(outcome))
    print(to_markdown(outcome))
