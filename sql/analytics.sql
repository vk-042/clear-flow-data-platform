-- PostgreSQL: count entities, not lifecycle events.
SELECT domain, status, COUNT(*) AS entity_count,
       SUM(amount_cents)::numeric / 100 AS face_value_usd
FROM entities
WHERE quality_issue = ''
GROUP BY domain, status;

-- Most recent anomaly per entity (review signal, not a fraud verdict).
SELECT DISTINCT ON (domain, entity_id)
       domain, entity_id, amount_cents, anomaly_score, model_version
FROM events WHERE is_anomaly = 1
ORDER BY domain, entity_id, sequence DESC;

-- Schema failures, duplicate deliveries, and conflicting records.
SELECT outcome, reason, COUNT(*) AS record_count
FROM raw_records GROUP BY outcome, reason;

-- Events arriving more than 5 minutes after their business timestamp.
SELECT domain, COUNT(*) AS late_events
FROM events
WHERE received_at::timestamptz - event_time::timestamptz > interval '5 minutes'
GROUP BY domain;
