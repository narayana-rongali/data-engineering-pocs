"""Micro-batch consumer: read from Kafka, merge into Delta, then commit offsets."""
import argparse
import json
import time

from kafka import KafkaConsumer

from .delta_sink import apply_changes
from .events import MalformedEvent, parse_event


def decode(raw):
    return json.loads(raw) if raw else None


def run(bootstrap, topic, table_path, run_seconds=None, poll_ms=2000):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id="delta-sink",
        enable_auto_commit=False,  # Offsets move only after the merge succeeds.
        auto_offset_reset="earliest",
        value_deserializer=decode,
    )
    started = time.monotonic()
    totals = {"upserted": 0, "deleted": 0, "malformed": 0}
    try:
        while run_seconds is None or time.monotonic() - started < run_seconds:
            polled = consumer.poll(timeout_ms=poll_ms)
            messages = [message for batch in polled.values() for message in batch]
            if not messages:
                continue
            messages.sort(key=lambda message: (message.partition, message.offset))
            changes = []
            for message in messages:
                try:
                    change = parse_event(message.value)
                except MalformedEvent as error:
                    totals["malformed"] += 1
                    print(f"skipped offset {message.offset}: {error}")
                    continue
                if change:
                    changes.append(change)
            result = apply_changes(table_path, changes)
            consumer.commit()
            totals["upserted"] += result["upserted"]
            totals["deleted"] += result["deleted"]
            print(f"merged batch: {result}")
    finally:
        consumer.close()
    return totals


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--topic", default="shop.retail.dbo.orders")
    parser.add_argument("--table", default="output/orders_delta")
    parser.add_argument("--run-seconds", type=int)
    args = parser.parse_args()
    print(run(args.bootstrap, args.topic, args.table, args.run_seconds))
