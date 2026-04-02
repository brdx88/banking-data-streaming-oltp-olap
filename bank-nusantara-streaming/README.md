# Bank Nusantara Streaming

Python event streaming simulation for a banking use case using Confluent Cloud and BigQuery.

## Project Structure

```text
bank-nusantara-streaming/
├── config/
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
