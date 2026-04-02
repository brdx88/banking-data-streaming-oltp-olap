# Data Contracts

## Event Envelope Standard

All emitted events should follow a common top-level envelope so every consumer can parse them consistently.

## Base Event Fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `event_id` | `str` | Yes | Unique identifier for the event |
| `event_type` | `str` | Yes | Specific business event name |
| `event_domain` | `str` | Yes | Domain such as `transaction` or `mobile_banking` |
| `event_timestamp` | `str` | Yes | ISO-8601 UTC timestamp |
| `customer_id` | `str` | Usually | Customer identifier |
| `account_id` | `str` | Optional | Account identifier if applicable |
| `source_system` | `str` | Yes | Producer source name |
| `trace_id` | `str` | Optional | Correlation / tracing identifier |
| `payload` | `dict` | Yes | Domain-specific body |

## Domain Payload Suggestions

### Mobile Banking Payload

```json
{
  "channel": "mobile_app",
  "action": "login",
  "device_id": "device-123",
  "ip_address": "10.10.10.10",
  "session_id": "sess-001",
  "status": "success",
  "location": "Jakarta"
}
```

### Transaction Payload

```json
{
  "transaction_id": "txn-001",
  "transaction_type": "transfer",
  "amount": 1250000.0,
  "currency": "IDR",
  "merchant_name": null,
  "origin_account_id": "acc-001",
  "destination_account_id": "acc-002",
  "status": "success",
  "location": "Bandung"
}
```

### Customer Service Payload

```json
{
  "ticket_id": "case-1001",
  "case_type": "complaint_opened",
  "priority": "medium",
  "channel": "call_center",
  "status": "open",
  "description": "mobile app transfer failed"
}
```

## Suggested Python Schema Approach

Use `dataclass` models in `schemas/events.py` for the first iteration:

- `BaseEvent`
- `MobileBankingEvent`
- `TransactionEvent`
- `CustomerServiceEvent`

Optional future upgrade:

- Move to `pydantic` for stricter validation
- Move to Avro or Protobuf once schema evolution becomes important

## BigQuery Table Design Suggestion

### Option A: One Raw Table Per Domain

- `raw_mobile_banking_events`
- `raw_transaction_events`
- `raw_customer_service_events`

Pros:

- Clear ownership and simpler schema management

Cons:

- Cross-domain analysis requires more joins or unions

### Option B: Unified Event Table

- `raw_events`

Recommended columns:

| Column | Type |
| --- | --- |
| `event_id` | STRING |
| `event_type` | STRING |
| `event_domain` | STRING |
| `event_timestamp` | TIMESTAMP |
| `customer_id` | STRING |
| `account_id` | STRING |
| `source_system` | STRING |
| `trace_id` | STRING |
| `payload_json` | JSON |
| `ingested_at` | TIMESTAMP |

Pros:

- Simpler ingestion path for the first version

Cons:

- Domain-specific analytics may require more JSON parsing

## Recommended Starting Choice

Start with:

- Kafka serialization: JSON
- Schema implementation: Python `dataclass`
- Warehouse ingestion: one unified raw table plus optional curated tables later
