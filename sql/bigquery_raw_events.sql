CREATE TABLE IF NOT EXISTS `serious-music-469407-f1.bank_nusantara_streaming.raw_events` (
  event_id STRING,
  event_type STRING,
  event_domain STRING,
  event_timestamp TIMESTAMP,
  customer_id STRING,
  account_id STRING,
  source_system STRING,
  trace_id STRING,
  payload_json JSON,
  ingested_at TIMESTAMP
);
