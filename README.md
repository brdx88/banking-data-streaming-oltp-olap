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

## Run Consumers

```bash
python consumers/analytics.py
python consumers/fraud_detection.py
python consumers/data_warehouse.py
```

## Current Scope

- JSON event production to Confluent Cloud
- Simple rule-based fraud screening
- Basic analytics logging
- BigQuery sink scaffold for raw event ingestion
