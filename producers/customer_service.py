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

from schemas.events import CustomerServiceEvent
from utils.kafka_client import build_producer, choose_message_key, load_config, produce_json, require_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce customer service events to Kafka.")
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


def generate_service_event() -> CustomerServiceEvent:
    customer_no = random.randint(1000, 9999)
    case_type = random.choice(
        [
            "complaint_opened",
            "complaint_closed",
            "dispute_opened",
            "dispute_resolved",
            "card_block_request",
            "profile_update_request",
        ]
    )
    payload = {
        "ticket_id": f"case-{random.randint(1000, 9999)}",
        "case_type": case_type,
        "priority": random.choice(["low", "medium", "high"]),
        "channel": random.choice(["call_center", "email", "branch", "chat"]),
        "status": random.choice(["open", "closed", "in_progress"]),
        "description": random.choice(
            [
                "mobile app transfer failed",
                "card swallowed by atm",
                "suspicious transaction inquiry",
                "profile update request",
            ]
        ),
    }
    return CustomerServiceEvent(
        event_type=case_type,
        customer_id=f"cust-{customer_no}",
        payload=payload,
        source_system="customer-service-producer",
    )


def main() -> None:
    args = parse_args()
    load_config()
    topic = require_env("KAFKA_TOPIC_CUSTOMER_SERVICE")
    interval_seconds = args.interval_seconds or float(os.getenv("PRODUCER_INTERVAL_SECONDS", "3"))
    target_count = 1 if args.once else args.count
    producer = build_producer()
    sent_count = 0

    print(f"[customer_service] producing to topic={topic}")
    while True:
        event = generate_service_event().to_dict()
        message_key = choose_message_key(event)
        remaining = produce_json(producer, topic, event, key=message_key)
        print(
            f"[customer_service] sent event_id={event['event_id']} type={event['event_type']} "
            f"key={message_key} remaining={remaining}"
        )
        sent_count += 1
        if target_count is not None and sent_count >= target_count:
            print(f"[customer_service] completed sent_count={sent_count}")
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
