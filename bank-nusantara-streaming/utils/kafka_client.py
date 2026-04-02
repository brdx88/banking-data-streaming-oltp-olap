from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

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


def delivery_report(error: Exception | None, message: Any) -> None:
    if error is not None:
        print(f"[delivery] failed: {error}")
        return
    print(
        f"[delivery] topic={message.topic()} partition={message.partition()} "
        f"offset={message.offset()}"
    )
