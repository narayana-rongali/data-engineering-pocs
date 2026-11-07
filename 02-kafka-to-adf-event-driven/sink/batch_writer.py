"""Rolls Kafka messages into landing files. A finished file is what fires the ADF trigger."""
import json
from dataclasses import dataclass
from pathlib import Path

FILE_SUFFIX = ".jsonl"
TEMP_SUFFIX = ".tmp"


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    value: dict


def landing_name(topic, day, partition, first_offset, last_offset):
    """Named from its offsets, so a replay after a crash overwrites the same file instead of duplicating it."""
    return f"{topic}/date={day}/p{partition}-{first_offset:012d}-{last_offset:012d}{FILE_SUFFIX}"


class LocalStore:
    def __init__(self, root):
        self.root = Path(root)

    def publish(self, name, body):
        target = self.root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(TEMP_SUFFIX)
        temp.write_text(body, encoding="utf-8")
        temp.replace(target)


class AdlsStore:
    """Uploads to a temp name then renames, so the trigger can never see a half-written file."""

    def __init__(self, account_url, file_system, credential):
        from azure.storage.filedatalake import DataLakeServiceClient

        self.file_system = DataLakeServiceClient(account_url, credential=credential).get_file_system_client(file_system)

    def publish(self, name, body):
        temp = self.file_system.get_file_client(f"{name}{TEMP_SUFFIX}")
        temp.upload_data(body, overwrite=True)
        temp.rename_file(f"{self.file_system.file_system_name}/{name}")


class BatchWriter:
    def __init__(self, store, topic, max_records=1000):
        self.store = store
        self.topic = topic
        self.max_records = max_records
        self.buffers = {}

    def add(self, record, day):
        """Returns the published file name if this record filled a batch."""
        buffer = self.buffers.setdefault((record.partition, day), [])
        buffer.append(record)
        if len(buffer) >= self.max_records:
            return self._publish(record.partition, day)
        return None

    def flush(self):
        return [self._publish(partition, day) for partition, day in list(self.buffers)]

    def _publish(self, partition, day):
        records = self.buffers.pop((partition, day))
        name = landing_name(self.topic, day, partition, records[0].offset, records[-1].offset)
        body = "".join(json.dumps(record.value, sort_keys=True) + "\n" for record in records)
        self.store.publish(name, body)
        return name
