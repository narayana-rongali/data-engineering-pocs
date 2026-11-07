Workload: 5 daily batches of 1,000,000 synthetic rows, single machine, Python engines.

| Measure | Delta Lake (delta-rs) | Apache Iceberg (PyIceberg) |
|---|---|---|
| write seconds | 0.524 | 1.1 |
| total rows | 5000000 | 5000000 |
| filtered rows | 1000000 | 1000000 |
| filtered query ms | 26.68 | 33.39 |
| time travel first version rows | 1000000 | 1000000 |
| data files | 5 | 5 |
| data mb | 76.66 | 62.32 |
| rows after evolution | 6000000 | 6000000 |
| old rows null in new column | 5000000 | 5000000 |
