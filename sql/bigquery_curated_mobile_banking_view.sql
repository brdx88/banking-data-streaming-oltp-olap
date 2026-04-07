CREATE OR REPLACE VIEW `serious-music-469407-f1.bank_nusantara_streaming.curated_mobile_banking_events` AS
SELECT
  event_id,
  event_timestamp,
  customer_id,
  account_id,
  event_type,
  source_system,
  trace_id,
  JSON_VALUE(payload_json, '$.channel') AS channel,
  JSON_VALUE(payload_json, '$.action') AS action,
  JSON_VALUE(payload_json, '$.device_id') AS device_id,
  JSON_VALUE(payload_json, '$.ip_address') AS ip_address,
  JSON_VALUE(payload_json, '$.session_id') AS session_id,
  JSON_VALUE(payload_json, '$.status') AS activity_status,
  JSON_VALUE(payload_json, '$.location') AS location,
  ingested_at
FROM `serious-music-469407-f1.bank_nusantara_streaming.raw_events`
WHERE event_domain = 'mobile_banking';
