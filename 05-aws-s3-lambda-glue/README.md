# S3, Lambda and Glue ingestion

Files landing in S3 are validated, split into clean and quarantined rows, and registered in the Glue Data Catalog so Athena can query them straight away.

```
S3 landing/ -> Lambda -> S3 curated/ and quarantine/ -> Glue partition -> Athena
```

## Design decisions
- **Idempotent:** S3 can deliver an event twice. A second run overwrites the same curated file, and partition creation tolerates "already exists".
- **Quarantine, don't drop:** bad rows keep their line number and the reason they failed, so they can be fixed and replayed.
- **Least privilege:** the IAM policy grants only the actions the handler calls, on specific prefixes. A test fails if the policy and the code drift apart.
- **Infrastructure as code:** `infra/main.tf` defines the bucket (public access blocked), Glue database and table, Lambda, IAM role and S3 notification.

## Run it
```bash
pip install -r requirements.txt
pytest
```
Tests run against `moto`, which emulates S3 and Glue locally. `terraform validate` runs in CI.

**This has not been deployed to a real AWS account.** To deploy: `terraform init && terraform apply -var bucket_name=<unique-name>`.
