import copy
import json
from pathlib import Path

import pytest

from framework.config import ConfigError, control_table_rows, load, validate
from framework.simulator import connect, create_control_table, run_once

ROOT = Path(__file__).parent.parent
SOURCES = load(ROOT / "config" / "sources.json")
PIPELINE = json.loads((ROOT / "infra" / "pipeline.arm.json").read_text())["resources"][0]["properties"]


def broken(**changes):
    source = copy.deepcopy(SOURCES[0])
    source.update(changes)
    return [source]


@pytest.mark.parametrize(
    "bad",
    [
        broken(source_table="sales; DROP TABLE x"),
        broken(watermark_column="updated at"),
        broken(key_columns=[]),
        broken(target_path="../outside"),
        broken(target_path="/absolute"),
        SOURCES[:1] + SOURCES[:1],
    ],
)
def test_bad_metadata_is_rejected(bad):
    with pytest.raises(ConfigError):
        validate(bad)


def test_control_table_sql_upserts_without_resetting_watermarks():
    sql = control_table_rows(SOURCES)
    assert sql.count("MERGE etl.source_control") == len(SOURCES)
    update_clause = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "last_watermark" not in update_clause


def make_db():
    db = connect()
    for source in SOURCES:
        db.execute(f"CREATE TABLE {source['system']}_{source['source_table']} (id INTEGER, updated_at TEXT)")
    create_control_table(db, SOURCES)
    return db


def test_incremental_runs_copy_each_row_once():
    db = make_db()
    db.executemany("INSERT INTO retailer_a_sales VALUES (?, ?)", [(1, "2026-01-01"), (2, "2026-01-02")])
    landed = {}
    assert run_once(db, landed)["retailer_a_sales"] == 2
    assert run_once(db, landed)["retailer_a_sales"] == 0
    db.execute("INSERT INTO retailer_a_sales VALUES (3, '2026-01-03')")
    assert run_once(db, landed)["retailer_a_sales"] == 1
    assert [row[0] for row in landed["retailer_a_sales"]] == [1, 2, 3]


def test_disabled_sources_are_skipped():
    db = make_db()
    db.execute("INSERT INTO retailer_c_inventory VALUES (1, '2026-01-01')")
    assert "retailer_c_inventory" not in run_once(db, {})


def test_onboarding_a_source_is_only_configuration():
    new_source = broken(source_id="retailer_d_sales", system="retailer_d")[0]
    db = make_db()
    db.execute("CREATE TABLE retailer_d_sales (id INTEGER, updated_at TEXT)")
    db.execute("INSERT INTO retailer_d_sales VALUES (1, '2026-01-01')")
    create_control_table(db, validate(SOURCES + [new_source]))
    assert run_once(db, {})["retailer_d_sales"] == 1


def test_pipeline_wiring():
    for_each = next(a for a in PIPELINE["activities"] if a["type"] == "ForEach")
    names = {a["name"]: a for a in for_each["typeProperties"]["activities"]}
    assert names["advance_watermark"]["dependsOn"][0] == {"activity": "copy_increment", "dependencyConditions": ["Succeeded"]}
    assert "lookup_sources" in for_each["typeProperties"]["items"]["value"]
    assert "enabled = 1" in PIPELINE["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]
