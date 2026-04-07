CREATE OR REPLACE VIEW `serious-music-469407-f1.bank_nusantara_streaming.curated_customer_service_events` AS
SELECT
  event_id,
  event_timestamp,
  customer_id,
  account_id,
  event_type,
  source_system,
  trace_id,
  JSON_VALUE(payload_json, '$.ticket_id') AS ticket_id,
  JSON_VALUE(payload_json, '$.case_type') AS case_type,
  JSON_VALUE(payload_json, '$.priority') AS priority,
  JSON_VALUE(payload_json, '$.channel') AS channel,
  JSON_VALUE(payload_json, '$.status') AS case_status,
  JSON_VALUE(payload_json, '$.description') AS description,
  ingested_at
FROM `serious-music-469407-f1.bank_nusantara_streaming.raw_events`
WHERE event_domain = 'customer_service';
