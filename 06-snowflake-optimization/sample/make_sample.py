"""Writes a SYNTHETIC query history in the ACCOUNT_USAGE.QUERY_HISTORY column layout, for trying the advisor."""
import csv
import random
from pathlib import Path

COLUMNS = [
    "QUERY_ID", "QUERY_PARAMETERIZED_HASH", "WAREHOUSE_NAME", "WAREHOUSE_SIZE", "TOTAL_ELAPSED_TIME",
    "PARTITIONS_SCANNED", "PARTITIONS_TOTAL", "BYTES_SPILLED_TO_REMOTE_STORAGE", "QUEUED_OVERLOAD_TIME",
]

# (hash, warehouse, size, runs, elapsed ms, scanned, total, spill bytes, queued ms)
PATTERNS = [
    ("full_scan_report", "BI_WH", "Medium", 40, 95_000, 9_800, 10_000, 0, 0),
    ("spilling_join", "TRANSFORM_WH", "Small", 12, 600_000, 2_000, 4_000, 8_000_000_000, 0),
    ("dashboard_tile", "BI_WH", "Medium", 400, 2_500, 20, 10_000, 0, 1_500),
    ("healthy_lookup", "BI_WH", "X-Small", 30, 800, 5, 10_000, 0, 0),
]


def write(path, seed=7):
    random.seed(seed)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        counter = 0
        for pattern, warehouse, size, runs, elapsed, scanned, total, spill, queued in PATTERNS:
            for _ in range(runs):
                counter += 1
                jitter = random.uniform(0.8, 1.2)
                writer.writerow([f"q{counter}", pattern, warehouse, size, int(elapsed * jitter), scanned, total, spill, queued])


if __name__ == "__main__":
    target = Path(__file__).parent / "query_history_synthetic.csv"
    write(target)
    print(f"wrote {target}")
