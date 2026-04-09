from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import (
    build_consumer,
    build_event_envelope,
    build_producer,
    choose_message_key,
    deserialize_event,
    load_config,
    normalize_message_key,
    produce_json,
    require_env,
    topic_exists,
)


BOGUS_PROXY = "http://127.0.0.1:9"
INSERT_RETRY_ATTEMPTS = 3
INSERT_RETRY_DELAY_SECONDS = 2
DEFAULT_BIGQUERY_TIMEOUT_SECONDS = 10.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka events and load them into BigQuery.")
    parser.add_argument("--once", action="store_true", help="Consume exactly 1 message and stop.")
    parser.add_argument("--count", type=int, help="Consume N messages and stop.")
    parser.add_argument(
        "--group-id-suffix",
        help="Append a suffix to the consumer group id for isolated testing.",
    )
    parser.add_argument(
        "--offset-reset",
        choices=("earliest", "latest"),
        default="earliest",
        help="Where to start when the consumer group has no committed offset.",
    )
    parser.add_argument(
        "--insert-retry-attempts",
        type=int,
        default=INSERT_RETRY_ATTEMPTS,
        help="Number of BigQuery insert attempts before giving up.",
    )
    parser.add_argument(
        "--insert-retry-delay-seconds",
        type=float,
        default=INSERT_RETRY_DELAY_SECONDS,
        help="Delay between BigQuery insert retry attempts.",
    )
    parser.add_argument(
        "--bigquery-timeout-seconds",
        type=float,
        default=DEFAULT_BIGQUERY_TIMEOUT_SECONDS,
        help="HTTP timeout for BigQuery API requests.",
    )
    parser.add_argument(
        "--disable-bigquery-client-retry",
        action="store_true",
        help="Disable the BigQuery client library retry for faster failure during testing.",
    )
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1.")
    if args.once and args.count is not None:
        parser.error("Use either --once or --count, not both.")
    if args.insert_retry_attempts < 1:
        parser.error("--insert-retry-attempts must be at least 1.")
    if args.insert_retry_delay_seconds < 0:
        parser.error("--insert-retry-delay-seconds must be >= 0.")
    if args.bigquery_timeout_seconds <= 0:
        parser.error("--bigquery-timeout-seconds must be > 0.")
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


def insert_row(
    client: bigquery.Client,
    table_id: str,
    row: dict,
    *,
    timeout_seconds: float,
    disable_client_retry: bool,
) -> list[dict]:
    request_retry = None if disable_client_retry else bigquery.DEFAULT_RETRY
    table = client.get_table(table_id, retry=request_retry, timeout=timeout_seconds)
    return client.insert_rows(table=table, rows=[row], retry=request_retry, timeout=timeout_seconds)


def clear_bogus_proxy_settings() -> None:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        if os.environ.get(key) == BOGUS_PROXY:
            os.environ.pop(key, None)
            print(f"[data_warehouse] cleared bogus proxy setting: {key}")


def insert_row_with_retry(
    client: bigquery.Client,
    table_id: str,
    row: dict,
    *,
    attempt_count: int,
    delay_seconds: float,
    timeout_seconds: float,
    disable_client_retry: bool,
) -> list[dict]:
    last_error: Exception | None = None
    for attempt in range(1, attempt_count + 1):
        try:
            errors = insert_row(
                client,
                table_id,
                row,
                timeout_seconds=timeout_seconds,
                disable_client_retry=disable_client_retry,
            )
            if not errors:
                return []
            last_error = RuntimeError(str(errors))
            print(f"[data_warehouse] insert attempt={attempt} returned errors={errors}")
        except Exception as exc:
            last_error = exc
            print(f"[data_warehouse] insert attempt={attempt} raised error={exc}")

        if attempt < attempt_count:
            time.sleep(delay_seconds)

    if last_error is None:
        return [{"message": "Unknown insert failure"}]
    return [{"message": str(last_error)}]


def main() -> None:
    args = parse_args()
    load_config()
    clear_bogus_proxy_settings()
    group_id = require_env("KAFKA_CONSUMER_GROUP_DW")
    if args.group_id_suffix:
        group_id = f"{group_id}-{args.group_id_suffix}"
    topics = [
        require_env("KAFKA_TOPIC_MOBILE"),
        require_env("KAFKA_TOPIC_TRANSACTION"),
        require_env("KAFKA_TOPIC_CUSTOMER_SERVICE"),
    ]
    dlq_topic = require_env("KAFKA_TOPIC_DW_DEAD_LETTER")
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
    consumer = build_consumer(group_id=group_id, auto_offset_reset=args.offset_reset)
    producer = build_producer()
    dlq_topic_available = topic_exists(producer, dlq_topic)
    consumer.subscribe(topics)

    print(f"[data_warehouse] listening to topics={topics}")
    print(f"[data_warehouse] target table={table_id}")
    print(f"[data_warehouse] group_id={group_id} offset_reset={args.offset_reset}")
    print(f"[data_warehouse] dlq_topic={dlq_topic} available={dlq_topic_available}")
    print(
        "[data_warehouse] insert_retry_attempts="
        f"{args.insert_retry_attempts} insert_retry_delay_seconds={args.insert_retry_delay_seconds} "
        f"bigquery_timeout_seconds={args.bigquery_timeout_seconds} "
        f"disable_bigquery_client_retry={args.disable_bigquery_client_retry}"
    )
    try:
        client = bigquery.Client(project=project_id)
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"[data_warehouse] consumer error: {message.error()}")
                continue

            event = deserialize_event(message.value())
            row = build_row(event)
            errors = insert_row_with_retry(
                client,
                table_id,
                row,
                attempt_count=args.insert_retry_attempts,
                delay_seconds=args.insert_retry_delay_seconds,
                timeout_seconds=args.bigquery_timeout_seconds,
                disable_client_retry=args.disable_bigquery_client_retry,
            )
            if errors:
                print(f"[data_warehouse] insert failed: {errors}")
                if dlq_topic_available:
                    dlq_event = build_event_envelope(
                        event_type="warehouse_insert_failed",
                        event_domain="data_warehouse",
                        customer_id=event.get("customer_id"),
                        account_id=event.get("account_id"),
                        source_system="data-warehouse-consumer",
                        trace_id=event.get("trace_id"),
                        payload={
                            "source_event_id": event.get("event_id"),
                            "source_event_type": event.get("event_type"),
                            "source_topic": message.topic(),
                            "source_partition": message.partition(),
                            "source_offset": message.offset(),
                            "source_key": normalize_message_key(message.key()),
                            "target_table": table_id,
                            "insert_errors": errors,
                            "original_event": event,
                        },
                    )
                    remaining = produce_json(
                        producer,
                        dlq_topic,
                        dlq_event,
                        key=choose_message_key(dlq_event),
                    )
                    print(
                        f"[data_warehouse] published dlq_event_id={dlq_event['event_id']} "
                        f"to={dlq_topic} remaining={remaining}"
                    )
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
