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

from schemas.events import MobileBankingEvent
from utils.kafka_client import build_producer, choose_message_key, load_config, produce_json, require_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce mobile banking events to Kafka.")
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


def generate_mobile_event() -> MobileBankingEvent:
    customer_no = random.randint(1000, 9999)
    action = random.choice(
        [
            "login",
            "logout",
            "balance_inquiry",
            "transfer_initiated",
            "bill_payment",
            "device_registered",
        ]
    )
    payload = {
        "channel": "mobile_app",
        "action": action,
        "device_id": f"device-{random.randint(100, 999)}",
        "ip_address": f"10.10.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "session_id": f"sess-{random.randint(10000, 99999)}",
        "status": random.choice(["success", "failed"]),
        "location": random.choice(["Jakarta", "Bandung", "Surabaya", "Semarang"]),
    }
    return MobileBankingEvent(
        event_type=action,
        customer_id=f"cust-{customer_no}",
        payload=payload,
        source_system="mobile-banking-producer",
    )


def main() -> None:
    args = parse_args()
    load_config()
    topic = require_env("KAFKA_TOPIC_MOBILE")
    interval_seconds = args.interval_seconds or float(os.getenv("PRODUCER_INTERVAL_SECONDS", "2"))
    target_count = 1 if args.once else args.count
    producer = build_producer()
    sent_count = 0

    print(f"[mobile] producing to topic={topic}")
    while True:
        event = generate_mobile_event().to_dict()
        message_key = choose_message_key(event)
        remaining = produce_json(producer, topic, event, key=message_key)
        print(
            f"[mobile] sent event_id={event['event_id']} type={event['event_type']} "
            f"key={message_key} remaining={remaining}"
        )
        sent_count += 1
        if target_count is not None and sent_count >= target_count:
            print(f"[mobile] completed sent_count={sent_count}")
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
