from pathlib import Path

import pytest
import sqlglot

from advisor.analyze import analyze, load, to_markdown
from sample.make_sample import write

SQL_DIR = Path(__file__).parent.parent / "sql"


@pytest.fixture(scope="module")
def report(tmp_path_factory):
    path = tmp_path_factory.mktemp("sample") / "history.csv"
    write(path)
    return {item["pattern"]: item for item in analyze(load(path))}


def test_each_problem_is_detected(report):
    assert "poor_pruning" in report["full_scan_report"]["issues"]
    assert "remote_spill" in report["spilling_join"]["issues"]
    assert {"repeated", "queued"} <= set(report["dashboard_tile"]["issues"])


def test_healthy_queries_are_not_flagged(report):
    assert "healthy_lookup" not in report


def test_report_is_ranked_by_cost(tmp_path):
    path = tmp_path / "history.csv"
    write(path)
    ranked = analyze(load(path))
    credits = [item["estimated_credits"] for item in ranked]
    assert credits == sorted(credits, reverse=True)
    assert "| Pattern |" in to_markdown(ranked)


def test_every_finding_has_advice(report):
    for item in report.values():
        assert len(item["advice"]) == len(item["issues"])


@pytest.mark.parametrize("path", sorted(SQL_DIR.glob("*.sql")), ids=lambda p: p.name)
def test_sql_parses_as_snowflake(path):
    statements = [s for s in sqlglot.parse(path.read_text(), read="snowflake") if s is not None]
    assert statements, path.name


@pytest.mark.parametrize("path", sorted(SQL_DIR.glob("0[1-5]*.sql")), ids=lambda p: p.name)
def test_diagnostic_queries_are_read_only(path):
    for statement in sqlglot.parse(path.read_text(), read="snowflake"):
        assert isinstance(statement, sqlglot.exp.Select), path.name
