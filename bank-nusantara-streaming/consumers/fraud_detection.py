from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env


HIGH_VALUE_THRESHOLD = 10_000_000


def is_suspicious(event: dict) -> tuple[bool, str]:
    if event.get("event_domain") != "transaction":
        return False, ""

    amount = float(event.get("payload", {}).get("amount", 0))
    if amount >= HIGH_VALUE_THRESHOLD:
        return True, f"high_value_transaction amount={amount}"
    return False, ""


def main() -> None:
    load_config()
    group_id = require_env("KAFKA_CONSUMER_GROUP_FRAUD")
    topic = require_env("KAFKA_TOPIC_TRANSACTION")
    consumer = build_consumer(group_id=group_id)
    consumer.subscribe([topic])

    print(f"[fraud] listening to topic={topic}")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"[fraud] consumer error: {message.error()}")
                continue

            event = deserialize_event(message.value())
            suspicious, reason = is_suspicious(event)
            if suspicious:
                print(
                    f"[fraud] alert event_id={event['event_id']} "
                    f"customer_id={event['customer_id']} reason={reason}"
                )
    except KeyboardInterrupt:
        print("[fraud] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
