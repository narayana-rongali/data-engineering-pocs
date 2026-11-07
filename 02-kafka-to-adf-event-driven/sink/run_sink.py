"""Consumes a topic and publishes landing files. Offsets are committed only after a publish."""
import argparse
import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer

from .batch_writer import AdlsStore, BatchWriter, LocalStore, Record


def build_store(args):
    if args.local_dir:
        return LocalStore(args.local_dir)
    from azure.identity import DefaultAzureCredential

    return AdlsStore(os.environ["ADLS_ACCOUNT_URL"], os.environ["ADLS_FILE_SYSTEM"], DefaultAzureCredential())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--local-dir", help="Write to a folder instead of ADLS Gen2")
    parser.add_argument("--max-records", type=int, default=1000)
    args = parser.parse_args()

    writer = BatchWriter(build_store(args), args.topic, args.max_records)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id="adls-landing",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda raw: json.loads(raw),
    )
    while True:
        polled = consumer.poll(timeout_ms=5000)
        for messages in polled.values():
            for message in messages:
                day = datetime.fromtimestamp(message.timestamp / 1000, timezone.utc).date().isoformat()
                writer.add(Record(message.partition, message.offset, message.value), day)
        # Offsets only move once every polled record is in a published file, so nothing can be lost.
        if polled:
            print("published", writer.flush())
            consumer.commit()


if __name__ == "__main__":
    main()
