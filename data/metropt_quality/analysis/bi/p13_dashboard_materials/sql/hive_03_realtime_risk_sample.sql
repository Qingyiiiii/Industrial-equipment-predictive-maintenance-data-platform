USE metropt_quality;

SELECT
  event_id,
  event_time,
  operating_state,
  risk_score,
  risk_level,
  risk_reason,
  model_version
FROM dws_metropt_realtime_risk_events
ORDER BY event_time DESC
LIMIT 20;
