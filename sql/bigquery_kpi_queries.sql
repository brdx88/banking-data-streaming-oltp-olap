-- KPI 1: transaction volume by type
SELECT
  transaction_type,
  COUNT(*) AS transaction_count,
  SUM(amount) AS total_amount
FROM `serious-music-469407-f1.bank_nusantara_streaming.curated_transaction_events`
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- KPI 2: daily transaction trend
SELECT
  event_date,
  SUM(transaction_count) AS total_transactions,
  SUM(total_amount) AS total_amount
FROM `serious-music-469407-f1.bank_nusantara_streaming.transaction_daily_summary`
GROUP BY event_date
ORDER BY event_date DESC;

-- KPI 3: mobile banking activity by action and status
SELECT
  action,
  activity_status,
  COUNT(*) AS event_count
FROM `serious-music-469407-f1.bank_nusantara_streaming.curated_mobile_banking_events`
GROUP BY action, activity_status
ORDER BY event_count DESC;

-- KPI 4: failed mobile banking activities
SELECT
  event_timestamp,
  customer_id,
  action,
  device_id,
  ip_address,
  location
FROM `serious-music-469407-f1.bank_nusantara_streaming.curated_mobile_banking_events`
WHERE activity_status = 'failed'
ORDER BY event_timestamp DESC
LIMIT 100;

-- KPI 5: customer service case volume by case type
SELECT
  case_type,
  priority,
  case_status,
  COUNT(*) AS case_count
FROM `serious-music-469407-f1.bank_nusantara_streaming.curated_customer_service_events`
GROUP BY case_type, priority, case_status
ORDER BY case_count DESC;

-- KPI 6: open customer service cases
SELECT
  event_timestamp,
  customer_id,
  ticket_id,
  case_type,
  priority,
  channel,
  description
FROM `serious-music-469407-f1.bank_nusantara_streaming.curated_customer_service_events`
WHERE case_status IN ('open', 'in_progress')
ORDER BY event_timestamp DESC
LIMIT 100;

-- KPI 7: real-time KPI snapshot (last 15 minutes)
SELECT
  window_start,
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
  event_domain,
  kpi_name,
  transaction_type,
  activity_action,
  case_type,
  case_priority,
  status,
  currency,
  location
ORDER BY window_start DESC;
