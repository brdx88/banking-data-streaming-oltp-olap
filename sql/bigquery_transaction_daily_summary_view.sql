CREATE OR REPLACE VIEW `serious-music-469407-f1.bank_nusantara_streaming.transaction_daily_summary` AS
SELECT
  DATE(event_timestamp) AS event_date,
  transaction_type,
  transaction_status,
  currency,
  location,
  COUNT(*) AS transaction_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  MAX(amount) AS max_amount
FROM `serious-music-469407-f1.bank_nusantara_streaming.curated_transaction_events`
GROUP BY
  event_date,
  transaction_type,
  transaction_status,
  currency,
  location;
