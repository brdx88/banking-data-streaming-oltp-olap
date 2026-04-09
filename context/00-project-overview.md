# Project Overview

## Objective

Build a Python-based event streaming simulation for a banking use case using:

- Python producers for synthetic event generation
- Python consumers for downstream processing
- Confluent Cloud free tier as the Kafka platform
- BigQuery as the OLAP / analytics destination

The project is intended to demonstrate a small but realistic event-driven data pipeline with banking-flavored use cases such as transaction monitoring, mobile banking activity tracking, customer service events, fraud detection, and analytical aggregation.

## Proposed Repository Structure

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
└── README.md
```

## Core Functional Scope

### Producers

- `mobile_banking.py`
  Simulates user activity from mobile channels such as login, logout, device changes, transfer initiation, bill payment, and balance inquiry.

- `transaction.py`
  Simulates financial transaction events such as transfers, purchases, withdrawals, deposits, and reversals.

- `customer_service.py`
  Simulates non-transactional service events such as complaints, dispute tickets, account updates, and support interactions.

### Consumers

- `analytics.py`
  Consumes multiple business events and produces derived operational metrics, simple aggregates, or dashboard-ready summaries.

- `analytics_datamart.py`
  Consumes multiple business events and writes real-time KPI aggregates into BigQuery for dashboard consumption.

- `fraud_detection.py`
  Applies rule-based anomaly checks to transaction and channel behavior events, then emits alert events or writes suspicious records.

- `data_warehouse.py`
  Loads curated event data into BigQuery for reporting, BI, and historical analysis.

## Target Outcomes

- Demonstrate Kafka-based decoupling between event producers and consumers
- Provide reusable event schemas and shared Kafka utilities
- Keep the project simple enough for a portfolio/demo, but structured enough to scale
- Support local development while using managed Confluent Cloud infrastructure
- Enable downstream OLAP analysis in BigQuery

## Design Principles

- Keep producers and consumers independently runnable
- Standardize event envelopes across all domains
- Prefer JSON serialization first for speed of implementation
- Separate raw events from derived analytics/fraud outputs
- Make local setup explicit and easy to follow
- Keep secrets only in `config/confluent.env` and never commit them

## Initial Assumptions

- Confluent Cloud free tier is sufficient for demo-scale throughput
- Python version target will be 3.10+ or 3.11+
- BigQuery ingestion can start with the official `google-cloud-bigquery` client
- The first iteration can use synchronous/simple consumer loops before introducing advanced orchestration
- Fraud detection will be rules-based first, not ML-based
