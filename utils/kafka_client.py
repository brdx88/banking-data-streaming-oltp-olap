from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / "config" / "confluent.env"


def load_config() -> None:
    load_dotenv(ENV_PATH)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def kafka_common_config() -> dict[str, str]:
    load_config()
    return {
        "bootstrap.servers": require_env("BOOTSTRAP_SERVERS"),
        "security.protocol": require_env("SECURITY_PROTOCOL"),
        "sasl.mechanism": require_env("SASL_MECHANISM"),
        "sasl.username": require_env("SASL_USERNAME"),
        "sasl.password": require_env("SASL_PASSWORD"),
    }


def build_producer() -> Producer:
    return Producer(kafka_common_config())


def build_consumer(group_id: str, auto_offset_reset: str = "earliest") -> Consumer:
    config = kafka_common_config()
    config.update(
        {
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
        }
    )
    return Consumer(config)


def serialize_event(event: dict[str, Any]) -> str:
    return json.dumps(event)


def deserialize_event(raw_message: bytes | str | None) -> dict[str, Any]:
    if raw_message is None:
        raise ValueError("Received empty Kafka message.")
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    return json.loads(raw_message)


def normalize_message_key(key: str | bytes | None) -> str | None:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key.decode("utf-8")
    return key


def choose_message_key(event: dict[str, Any]) -> str:
    return (
        str(event.get("account_id") or "")
        or str(event.get("customer_id") or "")
        or str(event.get("event_id") or "")
        or str(uuid4())
    )


def produce_json(
    producer: Producer,
    topic: str,
    event: dict[str, Any],
    *,
    key: str | None = None,
    flush_timeout: float = 10.0,
) -> int:
    producer.produce(topic, key=key or choose_message_key(event), value=serialize_event(event))
    producer.poll(0)
    return producer.flush(flush_timeout)


def topic_exists(client: Producer, topic: str, timeout: float = 10.0) -> bool:
    metadata = client.list_topics(topic=topic, timeout=timeout)
    return topic in metadata.topics and metadata.topics[topic].error is None


def build_event_envelope(
    *,
    event_type: str,
    event_domain: str,
    payload: dict[str, Any],
    customer_id: str | None = None,
    account_id: str | None = None,
    source_system: str,
    trace_id: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_domain": event_domain,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "account_id": account_id,
        "source_system": source_system,
        "trace_id": trace_id,
        "payload": payload,
    }


def delivery_report(error: Exception | None, message: Any) -> None:
    if error is not None:
        print(f"[delivery] failed: {error}")
        return
    print(
        f"[delivery] topic={message.topic()} partition={message.partition()} "
        f"offset={message.offset()}"
    )
