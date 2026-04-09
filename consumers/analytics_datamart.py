from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import (
    build_consumer,
    deserialize_event,
    load_config,
    require_env,
)


DEFAULT_FLUSH_SECONDS = 30
DEFAULT_MAX_BUFFER = 500
INSERT_RETRY_ATTEMPTS = 3
INSERT_RETRY_DELAY_SECONDS = 2
DEFAULT_BIGQUERY_TIMEOUT_SECONDS = 10.0


@dataclass(slots=True, frozen=True)
class AggregateKey:
    window_start: datetime
    window_end: datetime
    event_domain: str
    event_type: str | None
    kpi_name: str
    transaction_type: str | None
    activity_action: str | None
    case_type: str | None
    case_priority: str | None
    status: str | None
    currency: str | None
    location: str | None


@dataclass(slots=True)
class AggregateValue:
    kpi_value: Decimal
    record_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Kafka events and build real-time analytics datamart KPIs in BigQuery."
    )
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
        "--flush-seconds",
        type=int,
        default=DEFAULT_FLUSH_SECONDS,
        help="Flush aggregated KPIs every N seconds.",
    )
    parser.add_argument(
        "--max-buffer",
        type=int,
        default=DEFAULT_MAX_BUFFER,
        help="Flush when aggregated KPI rows reach this size.",
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
    if args.flush_seconds < 5:
        parser.error("--flush-seconds must be at least 5 seconds.")
    if args.max_buffer < 1:
        parser.error("--max-buffer must be at least 1.")
    if args.insert_retry_attempts < 1:
        parser.error("--insert-retry-attempts must be at least 1.")
    if args.insert_retry_delay_seconds < 0:
        parser.error("--insert-retry-delay-seconds must be >= 0.")
    if args.bigquery_timeout_seconds <= 0:
        parser.error("--bigquery-timeout-seconds must be > 0.")
    return args


def parse_event_timestamp(raw_value: str | None) -> datetime:
    if not raw_value:
        return datetime.now(timezone.utc)
    value = raw_value
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def floor_to_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def window_bounds(event_timestamp: datetime) -> tuple[datetime, datetime]:
    window_start = floor_to_minute(event_timestamp)
    return window_start, window_start + timedelta(minutes=1)


def decimal_value(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def update_aggregate(
    aggregates: dict[AggregateKey, AggregateValue],
    key: AggregateKey,
    delta_value: Decimal,
    delta_count: int,
) -> None:
    current = aggregates.get(key)
    if current is None:
        aggregates[key] = AggregateValue(kpi_value=delta_value, record_count=delta_count)
        return
    current.kpi_value += delta_value
    current.record_count += delta_count


def update_from_event(aggregates: dict[AggregateKey, AggregateValue], event: dict[str, Any]) -> None:
    event_domain = event.get("event_domain", "unknown")
    event_type = event.get("event_type")
    payload = event.get("payload", {}) or {}
    event_timestamp = parse_event_timestamp(event.get("event_timestamp"))
    window_start, window_end = window_bounds(event_timestamp)

    if event_domain == "transaction":
        transaction_type = payload.get("transaction_type")
        status = payload.get("status")
        currency = payload.get("currency")
        location = payload.get("location")
        amount = decimal_value(payload.get("amount"))
        count_key = AggregateKey(
            window_start=window_start,
            window_end=window_end,
            event_domain=event_domain,
            event_type=event_type,
            kpi_name="transaction_count",
            transaction_type=transaction_type,
            activity_action=None,
            case_type=None,
            case_priority=None,
            status=status,
            currency=currency,
            location=location,
        )
        update_aggregate(aggregates, count_key, Decimal("1"), 1)
        amount_key = AggregateKey(
            window_start=window_start,
            window_end=window_end,
            event_domain=event_domain,
            event_type=event_type,
            kpi_name="transaction_amount_total",
            transaction_type=transaction_type,
            activity_action=None,
            case_type=None,
            case_priority=None,
            status=status,
            currency=currency,
            location=location,
        )
        update_aggregate(aggregates, amount_key, amount, 1)
        return

    if event_domain == "mobile_banking":
        action = payload.get("action")
        status = payload.get("status")
        location = payload.get("location")
        key = AggregateKey(
            window_start=window_start,
            window_end=window_end,
            event_domain=event_domain,
            event_type=event_type,
            kpi_name="mobile_activity_count",
            transaction_type=None,
            activity_action=action,
            case_type=None,
            case_priority=None,
            status=status,
            currency=None,
            location=location,
        )
        update_aggregate(aggregates, key, Decimal("1"), 1)
        return

    if event_domain == "customer_service":
        case_type = payload.get("case_type")
        priority = payload.get("priority")
        status = payload.get("status")
        key = AggregateKey(
            window_start=window_start,
            window_end=window_end,
            event_domain=event_domain,
            event_type=event_type,
            kpi_name="customer_service_case_count",
            transaction_type=None,
            activity_action=None,
            case_type=case_type,
            case_priority=priority,
            status=status,
            currency=None,
            location=None,
        )
        update_aggregate(aggregates, key, Decimal("1"), 1)


def build_rows(aggregates: dict[AggregateKey, AggregateValue]) -> list[dict[str, Any]]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for key, value in aggregates.items():
        rows.append(
            {
                "window_start": key.window_start.isoformat(),
                "window_end": key.window_end.isoformat(),
                "event_domain": key.event_domain,
                "event_type": key.event_type,
                "kpi_name": key.kpi_name,
                "kpi_value": float(value.kpi_value),
                "transaction_type": key.transaction_type,
                "activity_action": key.activity_action,
                "case_type": key.case_type,
                "case_priority": key.case_priority,
                "status": key.status,
                "currency": key.currency,
                "location": key.location,
                "record_count": value.record_count,
                "ingested_at": ingested_at,
            }
        )
    return rows


def insert_rows_with_retry(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict[str, Any]],
    *,
    attempt_count: int,
    delay_seconds: float,
    timeout_seconds: float,
    disable_client_retry: bool,
) -> list[dict]:
    request_retry = None if disable_client_retry else bigquery.DEFAULT_RETRY
    last_error: Exception | None = None
    for attempt in range(1, attempt_count + 1):
        try:
            errors = client.insert_rows_json(
                table_id,
                rows,
                retry=request_retry,
                timeout=timeout_seconds,
            )
            if not errors:
                return []
            last_error = RuntimeError(str(errors))
            print(f"[datamart] insert attempt={attempt} returned errors={errors}")
        except Exception as exc:
            last_error = exc
            print(f"[datamart] insert attempt={attempt} raised error={exc}")
        if attempt < attempt_count:
            time.sleep(delay_seconds)
    if last_error is None:
        return [{"message": "Unknown insert failure"}]
    return [{"message": str(last_error)}]


def main() -> None:
    args = parse_args()
    load_config()
    group_id = require_env("KAFKA_CONSUMER_GROUP_ANALYTICS_DATAMART")
    if args.group_id_suffix:
        group_id = f"{group_id}-{args.group_id_suffix}"
    topics = [
        require_env("KAFKA_TOPIC_MOBILE"),
        require_env("KAFKA_TOPIC_TRANSACTION"),
        require_env("KAFKA_TOPIC_CUSTOMER_SERVICE"),
    ]

    project_id = require_env("BIGQUERY_PROJECT_ID")
    dataset = require_env("BIGQUERY_DATASET")
    table = require_env("BIGQUERY_TABLE_REALTIME_KPI")
    table_id = f"{project_id}.{dataset}.{table}"

    os.environ.setdefault(
        "GOOGLE_APPLICATION_CREDENTIALS",
        require_env("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    consumer = build_consumer(group_id=group_id, auto_offset_reset=args.offset_reset)
    consumer.subscribe(topics)

    print(f"[datamart] listening to topics={topics}")
    print(f"[datamart] group_id={group_id} offset_reset={args.offset_reset}")
    print(f"[datamart] target table={table_id}")
    print(f"[datamart] flush_seconds={args.flush_seconds} max_buffer={args.max_buffer}")

    client = bigquery.Client(project=project_id)
    aggregates: dict[AggregateKey, AggregateValue] = {}
    target_count = 1 if args.once else args.count
    consumed_count = 0
    last_flush_time = time.time()

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                pass
            elif message.error():
                print(f"[datamart] consumer error: {message.error()}")
            else:
                event = deserialize_event(message.value())
                update_from_event(aggregates, event)
                consumed_count += 1

            should_flush = False
            if time.time() - last_flush_time >= args.flush_seconds:
                should_flush = True
            if len(aggregates) >= args.max_buffer:
                should_flush = True

            if should_flush and aggregates:
                rows = build_rows(aggregates)
                errors = insert_rows_with_retry(
                    client,
                    table_id,
                    rows,
                    attempt_count=args.insert_retry_attempts,
                    delay_seconds=args.insert_retry_delay_seconds,
                    timeout_seconds=args.bigquery_timeout_seconds,
                    disable_client_retry=args.disable_bigquery_client_retry,
                )
                if errors:
                    print(f"[datamart] insert failed: {errors}")
                else:
                    print(f"[datamart] inserted rows={len(rows)}")
                aggregates.clear()
                last_flush_time = time.time()

            if target_count is not None and consumed_count >= target_count:
                break
    except KeyboardInterrupt:
        print("[datamart] stopping")
    finally:
        if aggregates:
            rows = build_rows(aggregates)
            errors = insert_rows_with_retry(
                client,
                table_id,
                rows,
                attempt_count=args.insert_retry_attempts,
                delay_seconds=args.insert_retry_delay_seconds,
                timeout_seconds=args.bigquery_timeout_seconds,
                disable_client_retry=args.disable_bigquery_client_retry,
            )
            if errors:
                print(f"[datamart] final insert failed: {errors}")
            else:
                print(f"[datamart] final inserted rows={len(rows)}")
        consumer.close()


if __name__ == "__main__":
    main()
