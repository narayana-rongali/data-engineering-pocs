# CDC with Debezium, Kafka and Delta Lake

Streams row-level changes from SQL Server into a Delta table, instead of waiting for a nightly batch.

```
SQL Server (CDC) -> Debezium -> Kafka -> Python consumer -> Delta MERGE
```

## How it works
- **Capture:** Debezium reads the SQL Server change tables and publishes one event per row change.
- **Consume:** the consumer reads micro-batches and keeps only the last change per key in each batch.
- **Merge:** Delta MERGE applies inserts, updates and deletes in one transaction.
- **Delivery:** offsets are committed only after the merge succeeds. A crash can replay events, and a replay gives the same table.

## Run it
Needs Docker.

```bash
pip install -r requirements.txt
python scripts/e2e.py
```

Unit tests need no Docker: `pytest`.

## Limits
Single-partition demo with a fixed schema. Schema changes, dead-letter topics and multi-table routing are out of scope. Malformed events are logged and skipped, where production would send them to a dead-letter topic.
