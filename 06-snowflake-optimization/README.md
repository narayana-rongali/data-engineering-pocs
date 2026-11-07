# Snowflake optimization toolkit

Finds where Snowflake time and credits go, and says what to do about it.

## What is here
| File | Answers |
|---|---|
| `sql/01_costliest_query_patterns.sql` | Which repeated query shapes cost the most warehouse time? |
| `sql/02_poor_partition_pruning.sql` | Which queries read almost the whole table? |
| `sql/03_spill_and_queueing.sql` | Which warehouses are undersized (spill) or overloaded (queueing)? |
| `sql/04_credits_by_warehouse.sql` | Where do credits go day by day? |
| `sql/05_clustering_health.sql` | Would a clustering key actually help pruning? |
| `sql/06_guardrails.sql` | Auto-suspend, statement timeouts, resource monitors and result caching. |
| `advisor/analyze.py` | Ranks query patterns from a `QUERY_HISTORY` export, with a fix for each issue. |

## Run the advisor
```bash
pip install -r requirements.txt
python sample/make_sample.py
python -m advisor.analyze sample/query_history_synthetic.csv
```
Export your own history with query 01's filters (all columns) to CSV and pass that file instead.

## What has and has not been run
The advisor and its tests run on **synthetic** data in the real `QUERY_HISTORY` column layout. The SQL is checked for valid Snowflake syntax with `sqlglot`, and diagnostic queries are checked to be read-only. **None of it has been run against a live Snowflake account here.**
