CREATE OR REPLACE VIEW `serious-music-469407-f1.bank_nusantara_streaming.curated_transaction_events` AS
SELECT
  event_id,
  event_timestamp,
  customer_id,
  account_id,
  event_type,
  source_system,
  trace_id,
  JSON_VALUE(payload_json, '$.transaction_id') AS transaction_id,
  JSON_VALUE(payload_json, '$.transaction_type') AS transaction_type,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount') AS NUMERIC) AS amount,
  JSON_VALUE(payload_json, '$.currency') AS currency,
  JSON_VALUE(payload_json, '$.origin_account_id') AS origin_account_id,
  JSON_VALUE(payload_json, '$.destination_account_id') AS destination_account_id,
  JSON_VALUE(payload_json, '$.status') AS transaction_status,
  JSON_VALUE(payload_json, '$.location') AS location,
  ingested_at
FROM `serious-music-469407-f1.bank_nusantara_streaming.raw_events`
WHERE event_domain = 'transaction';
