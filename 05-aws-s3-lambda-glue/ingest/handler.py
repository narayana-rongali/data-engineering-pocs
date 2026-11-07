"""S3-triggered Lambda: validate a landing file, route rows, and register the partition in Glue."""
import json
import os
import re
import urllib.parse

import boto3

REQUIRED = {"order_id": int, "store_id": int, "item_id": int, "quantity": int}
KEY_PATTERN = re.compile(r"^landing/orders/dt=(\d{4}-\d{2}-\d{2})/[^/]+\.jsonl$")


def validate_line(line):
    """Returns (row, None) or (None, reason)."""
    try:
        row = json.loads(line)
    except json.JSONDecodeError:
        return None, "not json"
    for field, kind in REQUIRED.items():
        if not isinstance(row.get(field), kind) or isinstance(row.get(field), bool):
            return None, f"bad {field}"
    if row["quantity"] < 0:
        return None, "negative quantity"
    return row, None


def ensure_partition(glue, database, table, day, location):
    """Idempotent: S3 can deliver the same event twice, and the second call must not fail."""
    try:
        glue.get_partition(DatabaseName=database, TableName=table, PartitionValues=[day])
        return False
    except glue.exceptions.EntityNotFoundException:
        pass
    storage = glue.get_table(DatabaseName=database, Name=table)["Table"]["StorageDescriptor"]
    try:
        glue.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={"Values": [day], "StorageDescriptor": {**storage, "Location": location}},
        )
    except glue.exceptions.AlreadyExistsException:
        return False
    return True


def process_object(s3, glue, bucket, key, database, table):
    match = KEY_PATTERN.match(key)
    if not match:
        return {"key": key, "skipped": "key does not match landing/orders/dt=YYYY-MM-DD/*.jsonl"}
    day = match.group(1)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    good, bad = [], []
    for number, line in enumerate(body.splitlines(), start=1):
        if not line.strip():
            continue
        row, reason = validate_line(line)
        if row:
            good.append(json.dumps(row, sort_keys=True))
        else:
            bad.append(json.dumps({"line": number, "reason": reason, "raw": line}))

    name = key.rsplit("/", 1)[1]
    # Same key on a retry, so reprocessing overwrites rather than duplicates.
    curated_key = f"curated/orders/dt={day}/{name}"
    s3.put_object(Bucket=bucket, Key=curated_key, Body="\n".join(good).encode("utf-8"))
    if bad:
        s3.put_object(Bucket=bucket, Key=f"quarantine/orders/dt={day}/{name}", Body="\n".join(bad).encode("utf-8"))
    created = ensure_partition(glue, database, table, day, f"s3://{bucket}/curated/orders/dt={day}/")
    return {"key": key, "valid": len(good), "quarantined": len(bad), "partition_created": created}


def lambda_handler(event, context=None):
    s3 = boto3.client("s3")
    glue = boto3.client("glue")
    database = os.environ["GLUE_DATABASE"]
    table = os.environ["GLUE_TABLE"]
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        results.append(process_object(s3, glue, bucket, key, database, table))
    print(json.dumps(results))
    return results
