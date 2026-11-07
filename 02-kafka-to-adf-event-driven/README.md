# Kafka to ADF, event-driven

Starts an Azure Data Factory pipeline when data arrives, instead of on a fixed schedule.

```
Kafka -> Python sink -> ADLS Gen2 -> ADF storage event trigger -> Databricks notebook -> Delta MERGE
```

ADF has no native Kafka source for streaming, so a sink bridges the two: it writes finished files to ADLS Gen2, and each file raises a storage event.

## Design decisions
- **No half-written files:** the sink uploads to `.tmp` and renames to `.jsonl`. The trigger only matches `.jsonl`.
- **Safe replays:** file names come from Kafka offsets, so a replay overwrites a file instead of duplicating it.
- **Idempotent load:** the notebook MERGEs on `order_id`, so re-running a file is harmless.
- **Contract tests:** tests fail if the trigger path and the writer's output drift apart. That drift would otherwise fail silently, because the pipeline just never fires.

## What has and has not been run
The sink logic and template structure are covered by `pytest`. **This has not been deployed to a real Azure subscription.** To deploy:

```bash
az deployment group create -g <resource-group> --template-file infra/adf.arm.json --parameters factoryName=<name> ...
```

Then grant the factory identity access to Databricks, and add the `storage_account` value to the notebook parameters.
