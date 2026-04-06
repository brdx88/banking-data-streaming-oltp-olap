from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume analytics events from Kafka topics.")
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


def main() -> None:
    args = parse_args()
    load_config()
    group_id = require_env("KAFKA_CONSUMER_GROUP_ANALYTICS")
    if args.group_id_suffix:
        group_id = f"{group_id}-{args.group_id_suffix}"
    topics = [
        require_env("KAFKA_TOPIC_MOBILE"),
        require_env("KAFKA_TOPIC_TRANSACTION"),
        require_env("KAFKA_TOPIC_CUSTOMER_SERVICE"),
    ]
    target_count = 1 if args.once else args.count
    consumed_count = 0
    counts = Counter()
    consumer = build_consumer(group_id=group_id, auto_offset_reset=args.offset_reset)
    consumer.subscribe(topics)

    print(f"[analytics] listening to topics={topics}")
    print(f"[analytics] group_id={group_id} offset_reset={args.offset_reset}")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"[analytics] consumer error: {message.error()}")
                continue

            event = deserialize_event(message.value())
            counts[event["event_type"]] += 1
            print(
                f"[analytics] event_type={event['event_type']} "
                f"domain={event['event_domain']} count={counts[event['event_type']]}"
            )
            consumed_count += 1
            if target_count is not None and consumed_count >= target_count:
                print(f"[analytics] completed consumed_count={consumed_count}")
                break
    except KeyboardInterrupt:
        print("[analytics] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
