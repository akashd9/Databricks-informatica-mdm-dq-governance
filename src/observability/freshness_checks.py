# Databricks notebook source
"""Freshness gate: runs before the medallion pipeline starts. If a source
hasn't landed new files within its configured SLA window, the job halts
here rather than building a golden record on stale inputs. Every check
(pass or fail) is logged to governance.freshness_check_log — the SLA
dashboard (see dashboards/sla_dashboard_queries.sql) reads this table for
freshness compliance over time, which nothing else in the pipeline persists.
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# Explicit acquisition rather than relying on injected notebook globals:
# functions defined in an imported module (not the top-level executing
# notebook/pipeline-library file) don't automatically see them.
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.config_loader import load

_LOG_SCHEMA = StructType([
    StructField("entity", StringType()),
    StructField("source", StringType()),
    StructField("status", StringType()),
    StructField("detail", StringType()),
    StructField("age_hours", DoubleType()),
    StructField("sla_hours", DoubleType()),
])

CATALOG = "mdm_dq_demo"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.governance.freshness_check_log (
        entity STRING,
        source STRING,
        status STRING,       -- 'ok' | 'stale' | 'error'
        detail STRING,
        age_hours DOUBLE,
        sla_hours DOUBLE,
        checked_at TIMESTAMP
    ) USING DELTA
""")

_sources = load("sources.yml")["sources"] + load("account_sources.yml")["sources"]

_stale = []
_log_rows = []

for source in _sources:
    label = f"{source['entity']}/{source['name']}"
    sla = source["freshness_sla_hours"]

    try:
        files = dbutils.fs.ls(source["landing_path"])
    except Exception:
        _stale.append((label, "landing path not found"))
        _log_rows.append((source["entity"], source["name"], "error", "landing path not found", None, sla))
        continue

    if not files:
        _stale.append((label, "no files found"))
        _log_rows.append((source["entity"], source["name"], "error", "no files found", None, sla))
        continue

    latest_mtime = max(f.modificationTime for f in files) / 1000
    age_hours = (datetime.now(timezone.utc).timestamp() - latest_mtime) / 3600

    if age_hours > sla:
        detail = f"latest file is {age_hours:.1f}h old, SLA is {sla}h"
        _stale.append((label, detail))
        _log_rows.append((source["entity"], source["name"], "stale", detail, age_hours, sla))
    else:
        _log_rows.append((source["entity"], source["name"], "ok", None, age_hours, sla))

(
    spark.createDataFrame(_log_rows, _LOG_SCHEMA)
    .withColumn("checked_at", F.current_timestamp())
    .write.format("delta").mode("append")
    .saveAsTable(f"{CATALOG}.governance.freshness_check_log")
)

if _stale:
    raise RuntimeError(f"Freshness gate failed for sources: {_stale}")

print(f"Freshness gate passed for {len(_sources)} sources.")
