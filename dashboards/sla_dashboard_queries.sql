-- SLA Dashboard — source queries.
--
-- Not a deployed Lakeview dashboard: hand-authoring the .lvdash.json widget
-- layout schema without being able to render/verify it here risks shipping
-- something that looks like a working dashboard but isn't. These queries
-- are the actually-verified part — each is plain SQL against tables this
-- project creates, ready to paste into a Databricks SQL editor or a new
-- Lakeview dashboard's dataset definitions (Dashboards > Create Dashboard >
-- add each as a dataset, then visualize).
--
-- Catalog is mdm_dq_demo throughout — update if you renamed it.
--
-- Gold tables below are queried under the `silver` schema, not `gold` —
-- pipeline.yml's `target: silver` currently routes every declared DLT table
-- into one schema regardless of its bronze_/silver_/gold_ name prefix.
-- Confirmed with a real run (`SHOW TABLES IN mdm_dq_demo.gold` returns
-- nothing). See src/governance/glossary_gate.py for the full explanation
-- and the fix this needs.

-- ============================================================
-- 1. Freshness SLA compliance (last 30 days), per entity/source
-- ============================================================
SELECT
    entity,
    source,
    DATE(checked_at) AS check_date,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_checks,
    ROUND(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS compliance_pct
FROM mdm_dq_demo.governance.freshness_check_log
WHERE checked_at >= current_date() - INTERVAL 30 DAY
GROUP BY entity, source, DATE(checked_at)
ORDER BY check_date DESC, entity, source;

-- ============================================================
-- 2. DQ pass / quarantine rate over time, per entity
-- ============================================================
SELECT 'customer' AS entity, DATE(_ingested_at) AS d,
       COUNT(*) AS total, AVG(dq_score) AS avg_dq_score,
       SUM(CASE WHEN dq_score < 0.85 THEN 1 ELSE 0 END) AS quarantined,
       ROUND(SUM(CASE WHEN dq_score < 0.85 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS quarantine_pct
FROM mdm_dq_demo.silver.silver_customer_dq_scored
WHERE _ingested_at >= current_date() - INTERVAL 30 DAY
GROUP BY DATE(_ingested_at)
UNION ALL
SELECT 'account' AS entity, DATE(_ingested_at) AS d,
       COUNT(*) AS total, AVG(dq_score) AS avg_dq_score,
       SUM(CASE WHEN dq_score < 0.85 THEN 1 ELSE 0 END) AS quarantined,
       ROUND(SUM(CASE WHEN dq_score < 0.85 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS quarantine_pct
FROM mdm_dq_demo.silver.silver_account_dq_scored
WHERE _ingested_at >= current_date() - INTERVAL 30 DAY
GROUP BY DATE(_ingested_at)
ORDER BY d DESC, entity;

-- ============================================================
-- 3. Match/merge outcomes over time, per entity
--    (dedup rate, avg confidence, review-status breakdown)
-- ============================================================
SELECT 'customer' AS entity, DATE(merged_at) AS d,
       COUNT(*) AS golden_records,
       SUM(source_record_count) AS source_records_consumed,
       ROUND(AVG(match_confidence), 3) AS avg_match_confidence,
       SUM(CASE WHEN review_status = 'auto_merged' THEN 1 ELSE 0 END) AS auto_merged,
       SUM(CASE WHEN review_status = 'needs_review' THEN 1 ELSE 0 END) AS needs_review,
       SUM(CASE WHEN review_status = 'single_source' THEN 1 ELSE 0 END) AS single_source
FROM mdm_dq_demo.silver.gold_customer_golden
WHERE merged_at >= current_date() - INTERVAL 30 DAY
GROUP BY DATE(merged_at)
UNION ALL
SELECT 'account' AS entity, DATE(merged_at) AS d,
       COUNT(*) AS golden_records,
       SUM(source_record_count) AS source_records_consumed,
       ROUND(AVG(match_confidence), 3) AS avg_match_confidence,
       SUM(CASE WHEN review_status = 'auto_merged' THEN 1 ELSE 0 END) AS auto_merged,
       SUM(CASE WHEN review_status = 'needs_review' THEN 1 ELSE 0 END) AS needs_review,
       SUM(CASE WHEN review_status = 'single_source' THEN 1 ELSE 0 END) AS single_source
FROM mdm_dq_demo.silver.gold_account_golden
WHERE merged_at >= current_date() - INTERVAL 30 DAY
GROUP BY DATE(merged_at)
ORDER BY d DESC, entity;

-- ============================================================
-- 4. Steward review queue backlog (current snapshot + age)
-- ============================================================
SELECT
    entity,
    record_type,
    COUNT(*) AS backlog_size,
    ROUND(AVG(DATEDIFF(HOUR, detected_at, current_timestamp())), 1) AS avg_age_hours,
    MAX(DATEDIFF(HOUR, detected_at, current_timestamp())) AS oldest_item_hours
FROM mdm_dq_demo.governance.steward_review_queue
GROUP BY entity, record_type
ORDER BY backlog_size DESC;

-- ============================================================
-- 5. Steward decision throughput (last 30 days)
-- ============================================================
SELECT entity, record_type, decision, DATE(decided_at) AS d, COUNT(*) AS decisions
FROM mdm_dq_demo.governance.steward_decisions
WHERE decided_at >= current_date() - INTERVAL 30 DAY
GROUP BY entity, record_type, decision, DATE(decided_at)
ORDER BY d DESC;

-- ============================================================
-- 6. Lakehouse Monitor drift/profile metrics
-- ============================================================
-- src/observability/drift_monitor.py registers a monitor per entity's Gold
-- table with output_schema_name = mdm_dq_demo.governance. Databricks names
-- the generated tables <table>_profile_metrics / <table>_drift_metrics.
-- Confirm exact names after the monitor's first refresh actually runs
-- (`SHOW TABLES IN mdm_dq_demo.governance LIKE '*_metrics'`), then query
-- e.g.:
--   SELECT * FROM mdm_dq_demo.governance.gold_customer_golden_drift_metrics
--   ORDER BY window.start DESC LIMIT 50;
