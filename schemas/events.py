from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class BaseEvent:
    event_type: str
    event_domain: str
    customer_id: str
    payload: dict[str, Any]
    account_id: str | None = None
    source_system: str = "python-simulator"
    trace_id: str | None = None
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MobileBankingEvent(BaseEvent):
    event_domain: str = field(default="mobile_banking", init=False)


@dataclass(slots=True)
class TransactionEvent(BaseEvent):
    event_domain: str = field(default="transaction", init=False)


@dataclass(slots=True)
class CustomerServiceEvent(BaseEvent):
    event_domain: str = field(default="customer_service", init=False)
