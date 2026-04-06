from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env


HIGH_VALUE_THRESHOLD = 10_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume transaction events for fraud detection.")
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
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1.")
    if args.once and args.count is not None:
        parser.error("Use either --once or --count, not both.")
    return args


def is_suspicious(event: dict) -> tuple[bool, str]:
    if event.get("event_domain") != "transaction":
        return False, ""

    amount = float(event.get("payload", {}).get("amount", 0))
    if amount >= HIGH_VALUE_THRESHOLD:
        return True, f"high_value_transaction amount={amount}"
    return False, ""


def main() -> None:
    args = parse_args()
    load_config()
    group_id = require_env("KAFKA_CONSUMER_GROUP_FRAUD")
    if args.group_id_suffix:
        group_id = f"{group_id}-{args.group_id_suffix}"
    topic = require_env("KAFKA_TOPIC_TRANSACTION")
    target_count = 1 if args.once else args.count
    consumed_count = 0
    consumer = build_consumer(group_id=group_id, auto_offset_reset=args.offset_reset)
    consumer.subscribe([topic])

    print(f"[fraud] listening to topic={topic}")
    print(f"[fraud] group_id={group_id} offset_reset={args.offset_reset}")
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
            consumed_count += 1
            if target_count is not None and consumed_count >= target_count:
                print(f"[fraud] completed consumed_count={consumed_count}")
                break
    except KeyboardInterrupt:
        print("[fraud] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
