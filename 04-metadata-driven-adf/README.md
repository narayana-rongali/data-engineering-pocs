# Metadata-driven ADF ingestion

One Azure Data Factory pipeline ingests any number of sources. Adding a source is a row of configuration, not a new pipeline.

```
Control table -> Lookup -> ForEach source -> incremental Copy -> advance watermark
```

## How it works
- **Configuration:** `config/sources.json` describes each source. `framework/config.py` validates it, rejecting non-identifiers, unsafe paths and duplicates, because these names end up inside SQL.
- **Control table:** the validated config is upserted into `etl.source_control`. Re-running the upsert never resets a watermark.
- **Pipeline:** `infra/pipeline.arm.json` looks up enabled sources and copies only rows above each watermark, up to a fixed upper bound.
- **Watermarks:** each watermark advances only after its copy succeeds. A failed copy is retried on the next run, with no gap and no duplicates.

## Run it
```bash
pytest
```
`framework/simulator.py` runs the same Lookup, ForEach, Copy and watermark logic on SQLite, so the behaviour is tested without Azure.

## Not included
The datasets and linked services the pipeline references (`ds_control_table`, `ds_source`, `ds_lake_parquet`, `ls_control_db`) are environment-specific and not part of the template. **This has not been deployed to a real Azure subscription.**
