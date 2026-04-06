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

## BigQuery Setup

1. Place the BigQuery service account key in `config/secrets/`.
2. Keep `config/secrets/` and `config/confluent.env` out of git.
3. Set these values in `config/confluent.env`:

```env
GOOGLE_APPLICATION_CREDENTIALS=D:\path\to\config\secrets\bank-nusantara-bq-writer.json
BIGQUERY_PROJECT_ID=serious-music-469407-f1
BIGQUERY_DATASET=bank_nusantara_streaming
BIGQUERY_TABLE_RAW_EVENTS=raw_events
```

4. Create the BigQuery dataset if it does not exist yet:

```text
bank_nusantara_streaming
```

5. Create the raw events table using [sql/bigquery_raw_events.sql](/d:/OneDrive/Documents/07-Bank%20Negara%20Indonesia/09-Big%20Data%20Engineer/exploring_kafka_confluent_bigquery/sql/bigquery_raw_events.sql).

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

For isolated testing without replaying old backlog:

```bash
python consumers/analytics.py --count 1 --offset-reset latest --group-id-suffix test1
python consumers/fraud_detection.py --count 1 --offset-reset latest --group-id-suffix test1
python consumers/data_warehouse.py --count 1 --offset-reset latest --group-id-suffix test1
```

Notes:

- `--once` is equivalent to consuming or producing exactly 1 message.
- `--count N` stops after `N` messages have been processed.
- Producers still use `PRODUCER_INTERVAL_SECONDS` by default, but you can override it with `--interval-seconds`.
- Consumers using `--count` wait until enough messages are available in the subscribed topic or topics.
- `--group-id-suffix` helps isolate a test run from existing consumer offsets.
- `--offset-reset latest` is useful when you only want newly produced events in a test run.

## Verified Smoke Tests

- Transaction producer successfully published to `transaction-events`
- Analytics consumer successfully read messages from Kafka
- Fraud detection consumer successfully flagged a high-value transaction
- Mobile banking producer successfully published to `mobile-banking-activity`
- Customer service producer successfully published to `cs-interactions`
- Data warehouse consumer successfully inserted events into BigQuery `raw_events`

## End-To-End Demo Flow

1. Start a consumer with isolated offsets:

```bash
python consumers/data_warehouse.py --count 1 --offset-reset latest --group-id-suffix demo1
```

2. In a second terminal, produce one fresh event:

```bash
python producers/transaction.py --once
```

3. Verify the latest row in BigQuery:

```sql
SELECT *
FROM `serious-music-469407-f1.bank_nusantara_streaming.raw_events`
ORDER BY ingested_at DESC
LIMIT 10;
```

## Next BigQuery Test

After the dataset and table are created:

```bash
python consumers/data_warehouse.py --count 1
python producers/transaction.py --once
```

If BigQuery auth hangs for a long time, check for a bogus local proxy:

```powershell
echo $env:HTTP_PROXY
echo $env:HTTPS_PROXY
```

If you see `http://127.0.0.1:9`, clear it before retrying:

```powershell
Remove-Item Env:HTTP_PROXY -ErrorAction Ignore
Remove-Item Env:HTTPS_PROXY -ErrorAction Ignore
Remove-Item Env:http_proxy -ErrorAction Ignore
Remove-Item Env:https_proxy -ErrorAction Ignore
```

## Current Scope

- JSON event production to Confluent Cloud
- Simple rule-based fraud screening
- Basic analytics logging
- BigQuery sink scaffold for raw event ingestion
