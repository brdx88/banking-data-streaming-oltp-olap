from __future__ import annotations

import os
import random
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from schemas.events import CustomerServiceEvent
from utils.kafka_client import build_producer, load_config, require_env, serialize_event


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
    load_config()
    topic = require_env("KAFKA_TOPIC_CUSTOMER_SERVICE")
    interval_seconds = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "3"))
    producer = build_producer()

    print(f"[customer_service] producing to topic={topic}")
    while True:
        event = generate_service_event().to_dict()
        producer.produce(topic, value=serialize_event(event))
        producer.poll(0)
        producer.flush()
        print(f"[customer_service] sent event_id={event['event_id']} type={event['event_type']}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
