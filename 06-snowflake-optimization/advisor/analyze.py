"""Reads an export of ACCOUNT_USAGE.QUERY_HISTORY and turns it into a ranked list of fixes."""
import argparse
import csv
from collections import defaultdict
from statistics import median

SIZE_CREDITS_PER_HOUR = {"X-Small": 1, "Small": 2, "Medium": 4, "Large": 8, "X-Large": 16, "2X-Large": 32}

RULES = {
    "poor_pruning": "Reads more than 80% of a large table. Filter on the clustering key, avoid functions on filter columns, or reconsider the clustering key.",
    "remote_spill": "Spilled to remote storage. Reduce the data touched first; if it is still needed, use a larger warehouse for this query only.",
    "queued": "Waited for a free warehouse. Separate this workload or enable multi-cluster scaling.",
    "repeated": "Same query shape runs many times. Check that results caching applies, or materialize the result.",
}


def number(value):
    return float(value) if value not in ("", None) else 0.0


def load(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def findings_for(row):
    found = []
    total = number(row["PARTITIONS_TOTAL"])
    if total >= 1000 and number(row["PARTITIONS_SCANNED"]) / total > 0.8:
        found.append("poor_pruning")
    if number(row["BYTES_SPILLED_TO_REMOTE_STORAGE"]) > 0:
        found.append("remote_spill")
    if number(row["QUEUED_OVERLOAD_TIME"]) > 0:
        found.append("queued")
    return found


def estimated_credits(row):
    """Elapsed time on the warehouse size, as an upper bound for ranking. Real billing is per warehouse, not per query."""
    hours = number(row["TOTAL_ELAPSED_TIME"]) / 1000 / 3600
    return hours * SIZE_CREDITS_PER_HOUR.get(row["WAREHOUSE_SIZE"], 1)


def analyze(rows, repeat_threshold=50):
    by_pattern = defaultdict(list)
    for row in rows:
        by_pattern[row["QUERY_PARAMETERIZED_HASH"]].append(row)

    report = []
    for pattern, runs in by_pattern.items():
        issues = defaultdict(int)
        for run in runs:
            for issue in findings_for(run):
                issues[issue] += 1
        if len(runs) >= repeat_threshold:
            issues["repeated"] = len(runs)
        if not issues:
            continue
        report.append(
            {
                "pattern": pattern,
                "warehouse": runs[0]["WAREHOUSE_NAME"],
                "executions": len(runs),
                "median_seconds": round(median(number(r["TOTAL_ELAPSED_TIME"]) for r in runs) / 1000, 1),
                "estimated_credits": round(sum(estimated_credits(r) for r in runs), 2),
                "issues": dict(issues),
                "advice": [RULES[name] for name in issues],
            }
        )
    return sorted(report, key=lambda item: item["estimated_credits"], reverse=True)


def to_markdown(report, limit=10):
    lines = ["| Pattern | Warehouse | Runs | Median s | Est. credits | Issues |", "|---|---|---|---|---|---|"]
    for item in report[:limit]:
        issues = ", ".join(f"{name} ({count})" for name, count in item["issues"].items())
        lines.append(
            f"| {item['pattern']} | {item['warehouse']} | {item['executions']} | {item['median_seconds']} | {item['estimated_credits']} | {issues} |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("query_history_csv")
    parser.add_argument("--repeat-threshold", type=int, default=50)
    args = parser.parse_args()
    print(to_markdown(analyze(load(args.query_history_csv), args.repeat_threshold)))
