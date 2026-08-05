# Databricks notebook source
"""Weekly storage maintenance: VACUUM every managed Delta table in bronze/
silver/gold, removing files older than the retention window backing the
point-in-time restore procedure in docs/BACKUP_DR.md. Runs on its own
low-frequency schedule (resources/jobs.yml's maintenance job), not the
hourly pipeline job — VACUUM is a storage-cleanup operation, not part of
the data flow, and running it hourly would just be wasted compute.

RETAIN 720 HOURS (30 days) matches delta.deletedFileRetentionDuration set
on the Gold tables (src/gold/gold_golden_records*.py) — VACUUM must never
use a shorter window than that property or it can delete files a
concurrent long-running read still needs. Bronze/silver tables don't set
that property explicitly (no restore requirement documented for them), but
using the same 30-day constant everywhere keeps this script's one number
authoritative instead of one VACUUM call disagreeing with another.

Delta's own safety check (retentionDurationCheck, on by default) already
refuses VACUUM below 168 hours (7 days) — this script relies on that
default rather than disabling it, so a future edit that shrinks
_RETAIN_HOURS accidentally below 7 days fails loudly instead of silently
under-retaining.
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from pyspark.sql import SparkSession

# Explicit acquisition rather than relying on the injected notebook global:
# functions defined in an imported module (not the top-level executing
# notebook/pipeline-library file) don't automatically see that global.
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import functions as F

CATALOG = "mdm_dq_demo"
_RETAIN_HOURS = 720  # 30 days — keep in sync with the Gold tables' delta.deletedFileRetentionDuration.
_SCHEMAS = ("bronze", "silver", "gold")

# Same "degrade, don't crash the job" pattern as cost_monitor.py — a shared
# metastore already at its account-wide table quota shouldn't take down
# maintenance logging any more than it should take down cost visibility.
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.governance.vacuum_log (
            table_name STRING,
            status STRING,
            detail STRING,
            ran_at TIMESTAMP
        ) USING DELTA
    """)
    _log_table_available = True
except Exception as e:
    print(f"Vacuum maintenance: could not create/verify {CATALOG}.governance.vacuum_log ({e}). Skipping log writes this run.")
    _log_table_available = False

_results = []
for _schema in _SCHEMAS:
    try:
        _tables = [t.name for t in spark.catalog.listTables(f"{CATALOG}.{_schema}")]
    except Exception as e:
        print(f"Vacuum maintenance: could not list tables in {CATALOG}.{_schema} ({e}). Skipping this schema.")
        continue

    for _table in _tables:
        _full_name = f"{CATALOG}.{_schema}.{_table}"
        try:
            spark.sql(f"VACUUM {_full_name} RETAIN {_RETAIN_HOURS} HOURS")
            _results.append((_full_name, "ok", None))
        except Exception as e:
            print(f"Vacuum maintenance: VACUUM failed for {_full_name}: {e}")
            _results.append((_full_name, "failed", str(e)[:500]))

if _results and _log_table_available:
    (
        spark.createDataFrame(_results, ["table_name", "status", "detail"])
        .withColumn("ran_at", F.current_timestamp())
        .write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.governance.vacuum_log")
    )

_ok = sum(1 for _, status, _ in _results if status == "ok")
_failed = len(_results) - _ok
print(f"Vacuum maintenance complete: {_ok} table(s) vacuumed, {_failed} failed, retention {_RETAIN_HOURS}h.")
if _failed and _failed == len(_results):
    raise RuntimeError("Vacuum maintenance: every table failed — treating as a hard failure rather than a partial skip.")
