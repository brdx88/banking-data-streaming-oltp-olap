# Testing Guide

## Goal

This guide contains repeatable smoke-test commands for the streaming pipeline:

- source topic publish
- analytics derived topic publish
- fraud alert topic publish
- data warehouse dead-letter topic publish

All commands assume you are in the repo root:

```powershell
cd "D:\OneDrive\Documents\07-Bank Negara Indonesia\09-Big Data Engineer\exploring_kafka_confluent_bigquery"
```

## 1. Verify Derived Topics Exist

```powershell
@'
from utils.kafka_client import build_producer, load_config, require_env, topic_exists

load_config()
producer = build_producer()
for env_name in ['KAFKA_TOPIC_ANALYTICS', 'KAFKA_TOPIC_FRAUD_ALERTS', 'KAFKA_TOPIC_DW_DEAD_LETTER']:
    topic = require_env(env_name)
    print(f'{env_name}={topic} exists={topic_exists(producer, topic)}')
'@ | python -
```

## 2. Analytics To `analytics-metrics`

Terminal 1:

```powershell
@'
import time
from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env, normalize_message_key

load_config()
consumer = build_consumer(
    group_id='analytics-metrics-verifier',
    auto_offset_reset='latest',
)
consumer.subscribe([require_env('KAFKA_TOPIC_ANALYTICS')])
print('[analytics-metrics-test] listening')
end_time = time.time() + 25
try:
    while time.time() < end_time:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'[analytics-metrics-test] error: {msg.error()}')
            continue
        event = deserialize_event(msg.value())
        print(f"[analytics-metrics-test] topic={msg.topic()} key={normalize_message_key(msg.key())} event_id={event['event_id']} type={event['event_type']} source_event_id={event['payload'].get('source_event_id')}")
        break
finally:
    consumer.close()
'@ | python -
```

Terminal 2:

```powershell
python consumers/analytics.py --count 1 --offset-reset latest --group-id-suffix analyticsdemo
```

Terminal 3:

```powershell
python producers/mobile_banking.py --once
```

## 3. Fraud Detection To `fraud-alerts`

Terminal 1:

```powershell
@'
import time
from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env, normalize_message_key

load_config()
consumer = build_consumer(
    group_id='fraud-alerts-verifier',
    auto_offset_reset='latest',
)
consumer.subscribe([require_env('KAFKA_TOPIC_FRAUD_ALERTS')])
print('[fraud-alerts-test] listening')
end_time = time.time() + 25
try:
    while time.time() < end_time:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'[fraud-alerts-test] error: {msg.error()}')
            continue
        event = deserialize_event(msg.value())
        print(f"[fraud-alerts-test] topic={msg.topic()} key={normalize_message_key(msg.key())} event_id={event['event_id']} type={event['event_type']} source_event_id={event['payload'].get('source_event_id')}")
        break
finally:
    consumer.close()
'@ | python -
```

Terminal 2:

```powershell
python consumers/fraud_detection.py --count 1 --offset-reset latest --group-id-suffix frauddemo
```

Terminal 3:

```powershell
@'
from schemas.events import TransactionEvent
from utils.kafka_client import build_producer, choose_message_key, load_config, produce_json, require_env

load_config()
topic = require_env('KAFKA_TOPIC_TRANSACTION')
event = TransactionEvent(
    event_type='transfer',
    customer_id='cust-fraud-topic-test',
    account_id='acc-fraud-topic-test',
    payload={
        'transaction_id': 'txn-fraud-topic-test',
        'transaction_type': 'transfer',
        'amount': 15000000.0,
        'currency': 'IDR',
        'origin_account_id': 'acc-fraud-topic-test',
        'destination_account_id': 'acc-destination',
        'status': 'success',
        'location': 'Jakarta',
    },
    source_system='transaction-fraud-topic-test',
).to_dict()
producer = build_producer()
message_key = choose_message_key(event)
remaining = produce_json(producer, topic, event, key=message_key)
print(f"[fraud-alert-trigger] sent {event['event_id']} key={message_key} remaining={remaining}")
'@ | python -
```

## 4. Data Warehouse To `dw-dead-letter`

This test is safe because it does not touch the real `raw_events` table. It only overrides the table name in the current terminal session.

Terminal 1:

```powershell
@'
import time
from utils.kafka_client import build_consumer, deserialize_event, load_config, require_env, normalize_message_key

load_config()
consumer = build_consumer(
    group_id='dw-dlq-verifier',
    auto_offset_reset='latest',
)
consumer.subscribe([require_env('KAFKA_TOPIC_DW_DEAD_LETTER')])
print('[dw-dlq-test] listening')
end_time = time.time() + 40
try:
    while time.time() < end_time:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'[dw-dlq-test] error: {msg.error()}')
            continue
        event = deserialize_event(msg.value())
        print(f"[dw-dlq-test] topic={msg.topic()} key={normalize_message_key(msg.key())} event_id={event['event_id']} type={event['event_type']} source_event_id={event['payload'].get('source_event_id')}")
        break
finally:
    consumer.close()
'@ | python -
```

Terminal 2:

```powershell
$env:BIGQUERY_TABLE_RAW_EVENTS='raw_events_does_not_exist'
python consumers/data_warehouse.py --count 1 --offset-reset latest --group-id-suffix dlqdemo --insert-retry-attempts 1 --insert-retry-delay-seconds 0 --bigquery-timeout-seconds 5 --disable-bigquery-client-retry
```

Terminal 3:

```powershell
python producers/transaction.py --once
```

After the test, close the terminal or clear the override:

```powershell
Remove-Item Env:BIGQUERY_TABLE_RAW_EVENTS -ErrorAction Ignore
```

## 5. Real-Time Datamart KPI Insert

Terminal 1:

```powershell
python consumers/analytics_datamart.py --count 3 --offset-reset latest --group-id-suffix dmtest
```

Terminal 2:

```powershell
python producers/transaction.py --count 3 --interval-seconds 1
```

Then verify in BigQuery:

```sql
SELECT *
FROM `serious-music-469407-f1.bank_nusantara_streaming.realtime_financial_kpis`
ORDER BY ingested_at DESC
LIMIT 20;
```
