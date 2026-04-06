# Bank Nusantara Streaming

Python event streaming simulation for a banking use case using Confluent Cloud and BigQuery.

Confluent Cloud target:

- Environment: `bank-nusantara-streaming`
- Cluster: `bank-nusantara-kafka`
- Topics:
  - `transaction-events`
  - `mobile-banking-activity`
  - `cs-interactions`

## Project Structure

```text
.
├── config/
│   ├── confluent.env
│   └── confluent.env.example
├── producers/
│   ├── mobile_banking.py
│   ├── transaction.py
│   └── customer_service.py
├── consumers/
│   ├── analytics.py
│   ├── fraud_detection.py
│   └── data_warehouse.py
├── schemas/
│   └── events.py
├── utils/
│   └── kafka_client.py
├── context/
│   ├── 00-project-overview.md
│   ├── 01-architecture-context.md
│   ├── 02-data-contracts.md
│   ├── 03-environment-and-ops.md
│   └── 04-implementation-roadmap.md
├── requirements.txt
└── README.md
```

## Setup

1. Create a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy the example config:

```bash
cp config/confluent.env.example config/confluent.env
```

4. Fill in Confluent Cloud and BigQuery credentials.

## Kafka Topics

- `transaction-events` for financial transactions
- `mobile-banking-activity` for digital channel events
- `cs-interactions` for customer service interactions

## Run Producers

```bash
python producers/transaction.py
python producers/mobile_banking.py
python producers/customer_service.py
```

For controlled testing:

```bash
python producers/transaction.py --once
python producers/transaction.py --count 3 --interval-seconds 1
python producers/mobile_banking.py --count 2
python producers/customer_service.py --once
```

## Run Consumers

```bash
python consumers/analytics.py
python consumers/fraud_detection.py
python consumers/data_warehouse.py
```

For controlled testing:

```bash
python consumers/analytics.py --once
python consumers/analytics.py --count 3
python consumers/fraud_detection.py --count 1
python consumers/data_warehouse.py --count 5
```

Notes:

- `--once` is equivalent to consuming or producing exactly 1 message.
- `--count N` stops after `N` messages have been processed.
- Producers still use `PRODUCER_INTERVAL_SECONDS` by default, but you can override it with `--interval-seconds`.
- Consumers using `--count` wait until enough messages are available in the subscribed topic or topics.

## Verified Smoke Tests

- Transaction producer successfully published to `transaction-events`
- Analytics consumer successfully read messages from Kafka
- Fraud detection consumer successfully flagged a high-value transaction
- Mobile banking producer successfully published to `mobile-banking-activity`
- Customer service producer successfully published to `cs-interactions`

## Current Scope

- JSON event production to Confluent Cloud
- Simple rule-based fraud screening
- Basic analytics logging
- BigQuery sink scaffold for raw event ingestion
