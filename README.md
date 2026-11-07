# Data engineering POCs

Small, self-contained builds by Narayana Rongali, each tested. All data is synthetic, and none of this comes from client work.

| POC | Stack | Proof |
|---|---|---|
| [CDC with Debezium and Kafka](01-cdc-debezium-kafka-delta) | SQL Server, Debezium, Kafka, Delta Lake | Unit tests, plus an end-to-end Docker run in CI |
| [Kafka to ADF, event-driven](02-kafka-to-adf-event-driven) | Kafka, ADLS Gen2, ADF, Databricks | Unit and contract tests |
| [Iceberg and Delta side by side](03-iceberg-vs-delta) | Apache Iceberg, Delta Lake | Benchmark results in the repo |
| [Metadata-driven ADF ingestion](04-metadata-driven-adf) | ADF, Azure SQL | Watermark logic tested on SQLite |
| [S3, Lambda and Glue ingestion](05-aws-s3-lambda-glue) | S3, Lambda, Glue, IAM, Terraform | Tests on moto, `terraform validate` in CI |
| [Snowflake optimization toolkit](06-snowflake-optimization) | Snowflake | Advisor tests, SQL syntax checks |

Each README says plainly what has been run and what has not. Nothing here has been deployed to a paid cloud account.

```bash
cd <poc-folder>
pip install -r requirements.txt
pytest
```
