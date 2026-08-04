"""Anomaly gate: runs after the pipeline update completes, before the job is
considered successful. Flags volume anomalies (today's Gold row count vs.
a rolling baseline) and DQ quarantine-rate spikes — catching regressions
immediately rather than waiting for the next scheduled Lakehouse Monitoring
refresh.
"""
CATALOG = "mdm_dq_demo"
_Z_THRESHOLD = 3.0
_QUARANTINE_RATE_THRESHOLD = 0.15

history = spark.sql(f"""
    SELECT DATE(merged_at) AS d, COUNT(*) AS row_count
    FROM {CATALOG}.gold.gold_customer_golden
    WHERE merged_at >= current_date() - INTERVAL 8 DAY
    GROUP BY DATE(merged_at)
    ORDER BY d
""").toPandas()

if len(history) < 3:
    print("Volume anomaly check skipped: not enough history yet.")
else:
    today = history.iloc[-1]["row_count"]
    baseline = history.iloc[:-1]["row_count"]
    mean, std = baseline.mean(), baseline.std() or 1.0
    z = (today - mean) / std
    if abs(z) > _Z_THRESHOLD:
        raise RuntimeError(
            f"Anomaly gate failed: today's Gold row count {today} is {z:.1f} std devs from the "
            f"{len(baseline)}-day mean {mean:.0f} — halting before downstream notification."
        )
    print(f"Volume anomaly check passed: today's row count {today}, z-score {z:.2f}.")

quarantine_rate = spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {CATALOG}.silver.silver_customer_dq_quarantine
         WHERE _ingested_at >= current_date()) * 1.0 /
        GREATEST((SELECT COUNT(*) FROM {CATALOG}.silver.silver_customer_dq_scored
                  WHERE _ingested_at >= current_date()), 1) AS rate
""").collect()[0]["rate"]

if quarantine_rate > _QUARANTINE_RATE_THRESHOLD:
    raise RuntimeError(
        f"Anomaly gate failed: {quarantine_rate:.1%} of today's records were quarantined by the DQ gate "
        f"(threshold {_QUARANTINE_RATE_THRESHOLD:.0%}) — likely an upstream schema or data quality regression."
    )

print(f"Quarantine-rate check passed: {quarantine_rate:.1%} of today's records quarantined.")
