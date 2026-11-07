# Iceberg and Delta side by side

Runs one workload on both open table formats and records what happens, rather than repeating marketing claims.

## The workload
- Five daily batches of synthetic orders, partitioned by `order_date`.
- A filtered read of one day, timed as the median of 5 runs.
- Time travel back to the first commit.
- Schema evolution: add a column, then append rows that use it.

## Run it
```bash
pip install -r requirements.txt
python compare.py --rows-per-day 1000000
pytest
```
Results are written to `results/latest.md`.

## What the numbers do and do not mean
They come from Python engines (delta-rs and PyIceberg) on one laptop with local files. They show both formats doing the same job correctly. They are **not** a Spark or cloud benchmark: catalog, file size tuning and object-storage latency all change real results.
