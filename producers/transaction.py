from __future__ import annotations

import argparse
import os
import random
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from schemas.events import TransactionEvent
from utils.kafka_client import build_producer, load_config, require_env, serialize_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce transaction events to Kafka.")
    parser.add_argument("--once", action="store_true", help="Produce exactly 1 message and stop.")
    parser.add_argument("--count", type=int, help="Produce N messages and stop.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        help="Override PRODUCER_INTERVAL_SECONDS for this run.",
    )
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1.")
    if args.once and args.count is not None:
        parser.error("Use either --once or --count, not both.")
    return args


def generate_transaction_event() -> TransactionEvent:
    customer_no = random.randint(1000, 9999)
    amount = random.choice([50000, 125000, 500000, 1250000, 5000000, 15000000])
    transaction_type = random.choice(["transfer", "purchase", "atm_withdrawal"])
    payload = {
        "transaction_id": f"txn-{random.randint(100000, 999999)}",
        "transaction_type": transaction_type,
        "amount": float(amount),
        "currency": "IDR",
        "origin_account_id": f"acc-{customer_no}",
        "destination_account_id": f"acc-{random.randint(1000, 9999)}",
        "status": "success",
        "location": random.choice(["Jakarta", "Bandung", "Surabaya", "Medan"]),
    }
    return TransactionEvent(
        event_type=transaction_type,
        customer_id=f"cust-{customer_no}",
        account_id=f"acc-{customer_no}",
        payload=payload,
        source_system="transaction-producer",
    )


def main() -> None:
    args = parse_args()
    load_config()
    topic = require_env("KAFKA_TOPIC_TRANSACTION")
    interval_seconds = args.interval_seconds or float(os.getenv("PRODUCER_INTERVAL_SECONDS", "2"))
    target_count = 1 if args.once else args.count
    producer = build_producer()
    sent_count = 0

    print(f"[transaction] producing to topic={topic}")
    while True:
        event = generate_transaction_event().to_dict()
        producer.produce(topic, value=serialize_event(event))
        producer.poll(0)
        producer.flush()
        print(
            f"[transaction] sent event_id={event['event_id']} "
            f"type={event['event_type']} amount={event['payload']['amount']}"
        )
        sent_count += 1
        if target_count is not None and sent_count >= target_count:
            print(f"[transaction] completed sent_count={sent_count}")
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
