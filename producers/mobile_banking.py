from __future__ import annotations

import os
import random
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from schemas.events import MobileBankingEvent
from utils.kafka_client import build_producer, load_config, require_env, serialize_event


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
    load_config()
    topic = require_env("KAFKA_TOPIC_MOBILE")
    interval_seconds = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "2"))
    producer = build_producer()

    print(f"[mobile] producing to topic={topic}")
    while True:
        event = generate_mobile_event().to_dict()
        producer.produce(topic, value=serialize_event(event))
        producer.poll(0)
        producer.flush()
        print(f"[mobile] sent event_id={event['event_id']} type={event['event_type']}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
