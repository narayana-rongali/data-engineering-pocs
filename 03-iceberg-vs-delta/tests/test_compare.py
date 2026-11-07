import pytest

from compare import DAYS, compare, make_batch, to_markdown

ROWS = 200


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    return compare(ROWS, tmp_path_factory.mktemp("run") / "work")


@pytest.mark.parametrize("engine", ["delta", "iceberg"])
def test_formats_hold_the_same_data(result, engine):
    run = result[engine]
    assert run["total_rows"] == ROWS * DAYS
    assert run["filtered_rows"] == ROWS


@pytest.mark.parametrize("engine", ["delta", "iceberg"])
def test_time_travel_returns_the_first_commit(result, engine):
    assert result[engine]["time_travel_first_version_rows"] == ROWS


@pytest.mark.parametrize("engine", ["delta", "iceberg"])
def test_schema_evolution_keeps_old_rows_with_nulls(result, engine):
    run = result[engine]
    assert run["rows_after_evolution"] == ROWS * (DAYS + 1)
    assert run["old_rows_null_in_new_column"] == ROWS * DAYS


def test_batches_are_reproducible():
    assert make_batch(0, 50).equals(make_batch(0, 50))


def test_report_has_a_row_for_each_measure(result):
    assert to_markdown(result).count("\n|") >= len(result["delta"]) + 1
