import json
from pathlib import Path

from sink.batch_writer import FILE_SUFFIX, TEMP_SUFFIX, BatchWriter, LocalStore, Record, landing_name

DAY = "2026-01-15"
TEMPLATE = json.loads((Path(__file__).parent.parent / "infra" / "adf.arm.json").read_text())


def resources(kind):
    return [r for r in TEMPLATE["resources"] if r["type"].endswith(kind)]


def record(offset, partition=0):
    return Record(partition, offset, {"order_id": offset})


def test_batch_publishes_when_full(tmp_path):
    writer = BatchWriter(LocalStore(tmp_path), "orders", max_records=2)
    assert writer.add(record(10), DAY) is None
    name = writer.add(record(11), DAY)
    assert name == landing_name("orders", DAY, 0, 10, 11)
    lines = (tmp_path / name).read_text().splitlines()
    assert [json.loads(line)["order_id"] for line in lines] == [10, 11]


def test_flush_publishes_partial_batches_per_partition(tmp_path):
    writer = BatchWriter(LocalStore(tmp_path), "orders")
    writer.add(record(1, partition=0), DAY)
    writer.add(record(5, partition=1), DAY)
    assert len(writer.flush()) == 2 and not writer.buffers


def test_replay_overwrites_instead_of_duplicating(tmp_path):
    for _ in range(2):
        writer = BatchWriter(LocalStore(tmp_path), "orders")
        writer.add(record(1), DAY)
        writer.flush()
    assert len(list(tmp_path.rglob(f"*{FILE_SUFFIX}"))) == 1


def test_no_temp_file_is_left_behind(tmp_path):
    writer = BatchWriter(LocalStore(tmp_path), "orders")
    writer.add(record(1), DAY)
    writer.flush()
    assert not list(tmp_path.rglob(f"*{TEMP_SUFFIX}"))


def test_trigger_matches_what_the_writer_produces():
    """Contract test: if these drift apart, the pipeline silently never fires."""
    props = resources("/triggers")[0]["properties"]["typeProperties"]
    assert props["blobPathEndsWith"] == FILE_SUFFIX
    assert not f"x{TEMP_SUFFIX}".endswith(props["blobPathEndsWith"])
    assert props["blobPathBeginsWith"] == "/landing/blobs/orders/"
    # ADLS rename-after-upload raises BlobRenamed, so the trigger must listen for it.
    assert "Microsoft.Storage.BlobRenamed" in props["events"]


def test_trigger_targets_a_pipeline_that_exists_with_matching_parameters():
    pipeline = resources("/pipelines")[0]
    reference = resources("/triggers")[0]["properties"]["pipelines"][0]
    assert reference["pipelineReference"]["referenceName"] in pipeline["name"]
    assert set(reference["parameters"]) == set(pipeline["properties"]["parameters"])


def test_pipeline_uses_a_linked_service_that_exists():
    activity = resources("/pipelines")[0]["properties"]["activities"][0]
    assert activity["linkedServiceName"]["referenceName"] in resources("/linkedServices")[0]["name"]
    assert activity["policy"]["retry"] > 0


def test_every_template_parameter_is_used():
    text = json.dumps(TEMPLATE["resources"])
    for name in TEMPLATE["parameters"]:
        assert f"parameters('{name}')" in text, name
