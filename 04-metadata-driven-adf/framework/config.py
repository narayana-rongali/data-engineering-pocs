"""Validates source metadata before it can reach the control table."""
import json
import re
from pathlib import Path

REQUIRED = ("source_id", "system", "source_table", "target_path", "watermark_column", "key_columns", "enabled")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    pass


def validate(sources):
    seen = set()
    for index, source in enumerate(sources):
        where = f"source #{index + 1}"
        missing = [field for field in REQUIRED if field not in source]
        if missing:
            raise ConfigError(f"{where} is missing {missing}")
        if source["source_id"] in seen:
            raise ConfigError(f"duplicate source_id {source['source_id']!r}")
        seen.add(source["source_id"])
        # Names end up inside SQL, so only plain identifiers are allowed.
        for field in ("system", "source_table", "watermark_column"):
            if not IDENTIFIER.match(source[field]):
                raise ConfigError(f"{where}: {field} {source[field]!r} is not a plain identifier")
        if not source["key_columns"] or not all(IDENTIFIER.match(c) for c in source["key_columns"]):
            raise ConfigError(f"{where}: key_columns must be a non-empty list of identifiers")
        if source["target_path"].startswith("/") or ".." in source["target_path"]:
            raise ConfigError(f"{where}: target_path must be relative and stay inside the lake")
    return sources


def load(path):
    return validate(json.loads(Path(path).read_text()))


def control_table_rows(sources):
    """SQL that upserts the control table. Watermarks are never reset by re-running it."""
    statements = []
    for s in sources:
        values = ", ".join(
            [
                f"'{s['source_id']}'",
                f"'{s['system']}'",
                f"'{s['source_table']}'",
                f"'{s['target_path']}'",
                f"'{s['watermark_column']}'",
                f"'{','.join(s['key_columns'])}'",
                "1" if s["enabled"] else "0",
            ]
        )
        statements.append(
            "MERGE etl.source_control AS t USING (SELECT "
            + values
            + ") AS s (source_id, system, source_table, target_path, watermark_column, key_columns, enabled) "
            "ON t.source_id = s.source_id "
            "WHEN MATCHED THEN UPDATE SET system = s.system, source_table = s.source_table, target_path = s.target_path, "
            "watermark_column = s.watermark_column, key_columns = s.key_columns, enabled = s.enabled "
            "WHEN NOT MATCHED THEN INSERT (source_id, system, source_table, target_path, watermark_column, key_columns, enabled, last_watermark) "
            "VALUES (s.source_id, s.system, s.source_table, s.target_path, s.watermark_column, s.key_columns, s.enabled, '1900-01-01');"
        )
    return "\n".join(statements) + "\n"
