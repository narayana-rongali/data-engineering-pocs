import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from ingest.handler import lambda_handler, validate_line

BUCKET = "lake-test"
KEY = "landing/orders/dt=2026-01-15/batch-001.jsonl"
POLICY = json.loads((Path(__file__).parent.parent / "infra" / "iam_policy.json").read_text())


def event(key=KEY):
    return {"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": key}}}]}


def line(**row):
    base = {"order_id": 1, "store_id": 10, "item_id": 100, "quantity": 2}
    base.update(row)
    return json.dumps(base)


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("GLUE_DATABASE", "retail")
    monkeypatch.setenv("GLUE_TABLE", "orders")
    with mock_aws():
        s3 = boto3.client("s3")
        glue = boto3.client("glue")
        s3.create_bucket(Bucket=BUCKET)
        glue.create_database(DatabaseInput={"Name": "retail"})
        glue.create_table(
            DatabaseName="retail",
            TableInput={
                "Name": "orders",
                "PartitionKeys": [{"Name": "dt", "Type": "string"}],
                "StorageDescriptor": {"Columns": [{"Name": "order_id", "Type": "bigint"}], "Location": f"s3://{BUCKET}/curated/orders/"},
            },
        )
        yield s3, glue


@pytest.mark.parametrize(
    "raw,reason",
    [("{oops", "not json"), (line(order_id="1"), "bad order_id"), (line(quantity=-1), "negative quantity"), (line(store_id=True), "bad store_id")],
)
def test_validation_reasons(raw, reason):
    assert validate_line(raw) == (None, reason)


def test_valid_rows_are_curated_and_bad_rows_quarantined(aws):
    s3, glue = aws
    s3.put_object(Bucket=BUCKET, Key=KEY, Body="\n".join([line(order_id=1), "{oops", line(order_id=2), line(quantity=-5)]))
    [result] = lambda_handler(event())
    assert (result["valid"], result["quarantined"], result["partition_created"]) == (2, 2, True)
    curated = s3.get_object(Bucket=BUCKET, Key="curated/orders/dt=2026-01-15/batch-001.jsonl")["Body"].read().decode()
    assert [json.loads(l)["order_id"] for l in curated.splitlines()] == [1, 2]
    quarantine = s3.get_object(Bucket=BUCKET, Key="quarantine/orders/dt=2026-01-15/batch-001.jsonl")["Body"].read().decode()
    assert {json.loads(l)["reason"] for l in quarantine.splitlines()} == {"not json", "negative quantity"}
    partition = glue.get_partition(DatabaseName="retail", TableName="orders", PartitionValues=["2026-01-15"])
    assert partition["Partition"]["StorageDescriptor"]["Location"].endswith("curated/orders/dt=2026-01-15/")


def test_duplicate_event_delivery_is_safe(aws):
    s3, glue = aws
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=line())
    first, second = lambda_handler(event())[0], lambda_handler(event())[0]
    assert first["partition_created"] and not second["partition_created"]
    assert len(glue.get_partitions(DatabaseName="retail", TableName="orders")["Partitions"]) == 1
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="curated/")["KeyCount"] == 1


def test_unexpected_keys_are_skipped(aws):
    assert "skipped" in lambda_handler(event("landing/orders/no-date/file.jsonl"))[0]


def test_url_encoded_keys_are_decoded(aws):
    s3, _ = aws
    key = "landing/orders/dt=2026-01-15/batch 002.jsonl"
    s3.put_object(Bucket=BUCKET, Key=key, Body=line())
    assert lambda_handler(event(key.replace(" ", "+")))[0]["valid"] == 1


def test_policy_grants_only_what_the_handler_uses():
    granted = {action for statement in POLICY["Statement"] for action in statement["Action"]}
    assert granted == {
        "s3:GetObject", "s3:PutObject",
        "glue:GetTable", "glue:GetPartition", "glue:CreatePartition",
        "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
    }
    assert all("*" not in action for action in granted)
