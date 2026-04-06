from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka events and load them into BigQuery.")
    parser.add_argument("--once", action="store_true", help="Consume exactly 1 message and stop.")
    parser.add_argument("--count", type=int, help="Consume N messages and stop.")
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1.")
    if args.once and args.count is not None:
        parser.error("Use either --once or --count, not both.")
    return args


def build_row(event: dict) -> dict:
    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "event_domain": event.get("event_domain"),
        "event_timestamp": event.get("event_timestamp"),
        "customer_id": event.get("customer_id"),
        "account_id": event.get("account_id"),
        "source_system": event.get("source_system"),
        "trace_id": event.get("trace_id"),
        "payload_json": event.get("payload"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    args = parse_args()
    load_config()
    group_id = require_env("KAFKA_CONSUMER_GROUP_DW")
    topics = [
        require_env("KAFKA_TOPIC_MOBILE"),
        require_env("KAFKA_TOPIC_TRANSACTION"),
        require_env("KAFKA_TOPIC_CUSTOMER_SERVICE"),
    ]
    target_count = 1 if args.once else args.count
    consumed_count = 0

    project_id = require_env("BIGQUERY_PROJECT_ID")
    dataset = require_env("BIGQUERY_DATASET")
    table = require_env("BIGQUERY_TABLE_RAW_EVENTS")
    table_id = f"{project_id}.{dataset}.{table}"

    os.environ.setdefault(
        "GOOGLE_APPLICATION_CREDENTIALS",
        require_env("GOOGLE_APPLICATION_CREDENTIALS"),
    )

    client = bigquery.Client(project=project_id)
    consumer = build_consumer(group_id=group_id)
    consumer.subscribe(topics)

    print(f"[data_warehouse] listening to topics={topics}")
    print(f"[data_warehouse] target table={table_id}")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"[data_warehouse] consumer error: {message.error()}")
                continue

            event = deserialize_event(message.value())
            row = build_row(event)
            errors = client.insert_rows_json(table_id, [row])
            if errors:
                print(f"[data_warehouse] insert failed: {errors}")
            else:
                print(f"[data_warehouse] inserted event_id={row['event_id']}")
            consumed_count += 1
            if target_count is not None and consumed_count >= target_count:
                print(f"[data_warehouse] completed consumed_count={consumed_count}")
                break
    except KeyboardInterrupt:
        print("[data_warehouse] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
