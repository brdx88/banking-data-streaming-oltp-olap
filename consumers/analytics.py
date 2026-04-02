from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env


def main() -> None:
    load_config()
    group_id = require_env("KAFKA_CONSUMER_GROUP_ANALYTICS")
    topics = [
        require_env("KAFKA_TOPIC_MOBILE"),
        require_env("KAFKA_TOPIC_TRANSACTION"),
        require_env("KAFKA_TOPIC_CUSTOMER_SERVICE"),
    ]
    counts = Counter()
    consumer = build_consumer(group_id=group_id)
    consumer.subscribe(topics)

    print(f"[analytics] listening to topics={topics}")
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
    except KeyboardInterrupt:
        print("[analytics] stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
