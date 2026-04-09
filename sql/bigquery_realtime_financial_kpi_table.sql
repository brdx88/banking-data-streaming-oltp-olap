CREATE TABLE IF NOT EXISTS `serious-music-469407-f1.bank_nusantara_streaming.realtime_financial_kpis` (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_domain STRING,
  event_type STRING,
  kpi_name STRING,
  kpi_value NUMERIC,
  transaction_type STRING,
  activity_action STRING,
  case_type STRING,
  case_priority STRING,
  status STRING,
  currency STRING,
  location STRING,
  record_count INT64,
  ingested_at TIMESTAMP
);
