# Environment And Ops

## Expected Config File

Path:

`config/confluent.env`

Confluent Cloud setup:

- Environment: `bank-nusantara-streaming`
- Cluster: `bank-nusantara-kafka`

Suggested variables:

```env
BOOTSTRAP_SERVERS=<confluent-bootstrap-server>
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
SASL_USERNAME=<confluent-api-key>
SASL_PASSWORD=<confluent-api-secret>

KAFKA_TOPIC_MOBILE=mobile-banking-activity
KAFKA_TOPIC_TRANSACTION=transaction-events
KAFKA_TOPIC_CUSTOMER_SERVICE=cs-interactions
KAFKA_TOPIC_ANALYTICS=analytics-metrics
KAFKA_TOPIC_FRAUD_ALERTS=fraud-alerts
KAFKA_TOPIC_DW_DEAD_LETTER=dw-dead-letter

KAFKA_CONSUMER_GROUP_ANALYTICS=bank-nusantara-analytics-group
KAFKA_CONSUMER_GROUP_FRAUD=bank-nusantara-fraud-group
KAFKA_CONSUMER_GROUP_DW=bank-nusantara-dw-group

GOOGLE_APPLICATION_CREDENTIALS=<absolute-path-to-service-account-json>
BIGQUERY_PROJECT_ID=<gcp-project-id>
BIGQUERY_DATASET=<dataset-name>
BIGQUERY_TABLE_RAW_EVENTS=raw_events
```

## Python Package Expectations

Likely dependencies:

- `confluent-kafka`
- `python-dotenv`
- `google-cloud-bigquery`
- `pandas` optional for debugging or batch transforms

Optional:

- `tenacity` for retry handling
- `pydantic` if validation becomes stricter

## Shared Utility Expectations

`utils/kafka_client.py` should ideally provide:

- producer factory
- consumer factory
- config loading helper
- JSON serialize / deserialize helpers
- delivery callback or error handling helper

## Logging Expectations

First version should log:

- producer startup and topic target
- event publish success or failure
- consumer subscription and partition assignment
- parsing errors
- BigQuery insert success or failure
- fraud alert generation

## Error Handling Strategy

### Producer Side

- Validate required fields before publish
- Log serialization or broker errors
- Optionally continue on non-fatal failures

### Consumer Side

- Skip malformed messages with clear logs
- Retry transient BigQuery failures
- Separate poison messages into dead-letter flow later if needed

## Confluent Cloud Free Tier Considerations

- Keep topic count small
- Keep partition count simple, usually 1 for the demo
- Avoid unnecessary high-frequency message generation
- Expect quota limits and design for demo-scale throughput

## Security Notes

- Never commit `config/confluent.env`
- Never hardcode API keys in producer or consumer scripts
- Use a dedicated GCP service account with minimum BigQuery permissions
- Sanitize logs if printing payloads that may contain sensitive fields
