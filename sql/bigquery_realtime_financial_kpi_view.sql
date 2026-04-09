CREATE OR REPLACE VIEW `serious-music-469407-f1.bank_nusantara_streaming.realtime_financial_kpi_latest` AS
SELECT
  window_start,
  window_end,
  event_domain,
  kpi_name,
  transaction_type,
  activity_action,
  case_type,
  case_priority,
  status,
  currency,
  location,
  SUM(kpi_value) AS kpi_value,
  SUM(record_count) AS record_count
FROM `serious-music-469407-f1.bank_nusantara_streaming.realtime_financial_kpis`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY
  window_start,
  window_end,
  event_domain,
  kpi_name,
  transaction_type,
  activity_action,
  case_type,
  case_priority,
  status,
  currency,
  location;
