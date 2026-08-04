# Databricks notebook source
"""Cost monitoring for the pipeline's own compute. The DLT pipeline and its
gate tasks all run on Databricks compute tagged project=mdm-dq-medallion
(see resources/pipeline.yml's cluster custom_tags) — without watching that
spend, "integrated observability" would have a blind spot on its own cost
footprint. Reads system.billing.usage (a Unity Catalog system table that
must be enabled at the account level); degrades to a skip-and-warn rather
than failing the job if it isn't available or the schema doesn't match what
this script expects — cost visibility gracefully missing is a lot better
than an unrelated schema assumption taking down the whole pipeline run.

Two thresholds, not one: daily_budget_usd is a soft warning (printed, not
fatal); hard_limit_usd actually halts the job — a runaway-cost circuit
breaker, not just a dashboard number nobody reads until the invoice arrives.
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
from datetime import date, timedelta
from pyspark.sql import functions as F
from src.config_loader import load

CATALOG = "mdm_dq_demo"
_CONFIG = load("cost_monitoring.yml")
_LOOKBACK_DAYS = _CONFIG["lookback_days"]

# Same "degrade, don't crash the job" philosophy as the system.billing.usage
# access check below: a shared metastore can be at its account-wide table
# quota (hit for real running this pipeline — QUOTA_EXCEEDED.
# UC_RESOURCE_QUOTA_EXCEEDED, a metastore-wide limit shared with every other
# catalog in the workspace, not something this project can resolve on its
# own), and that shouldn't take down cost visibility's dependents any more
# than a missing system table should.
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.governance.cost_monitor_log (
            window_start DATE,
            window_end DATE,
            sku_name STRING,
            dbus DOUBLE,
            estimated_usd DOUBLE,
            checked_at TIMESTAMP
        ) USING DELTA
    """)
    _log_table_available = True
except Exception as e:
    print(f"Cost monitor: could not create/verify {CATALOG}.governance.cost_monitor_log ({e}). Skipping log writes this run.")
    _log_table_available = False

window_end = date.today()
window_start = window_end - timedelta(days=_LOOKBACK_DAYS)

try:
    usage_rows = spark.sql(f"""
        SELECT sku_name, SUM(usage_quantity) AS dbus
        FROM system.billing.usage
        WHERE usage_date >= DATE'{window_start}'
          AND (
                custom_tags['project'] = '{_CONFIG["project_tag"]}'
             OR usage_metadata.job_name LIKE '%{_CONFIG["project_tag"]}%'
          )
        GROUP BY sku_name
    """).collect()
except Exception as e:
    print(
        f"Cost monitor skipped: system.billing.usage not accessible ({e}). "
        f"Unity Catalog system tables may need enabling at the account level, "
        f"or the usage_metadata/custom_tags schema differs from what this script expects."
    )
    usage_rows = []

price_map = _CONFIG["dbu_price_usd"]
log_rows = []
total_usd = 0.0
for r in usage_rows:
    price = price_map.get(r["sku_name"], price_map["default"])
    dbus = r["dbus"] or 0.0
    estimated_usd = dbus * price
    total_usd += estimated_usd
    log_rows.append((window_start, window_end, r["sku_name"], dbus, estimated_usd))

if not log_rows:
    print("Cost monitor: no usage rows found for this project tag in the lookback window.")
else:
    if _log_table_available:
        (
            spark.createDataFrame(log_rows, ["window_start", "window_end", "sku_name", "dbus", "estimated_usd"])
            .withColumn("checked_at", F.current_timestamp())
            .write.format("delta").mode("append")
            .saveAsTable(f"{CATALOG}.governance.cost_monitor_log")
        )
    print(
        f"Cost monitor: ~${total_usd:.2f} estimated spend over the last {_LOOKBACK_DAYS} day(s) "
        f"across {len(log_rows)} SKU(s)."
    )

    if total_usd > _CONFIG["hard_limit_usd"]:
        raise RuntimeError(
            f"Cost monitor: estimated spend ${total_usd:.2f} exceeds the hard limit "
            f"${_CONFIG['hard_limit_usd']:.2f} — halting to avoid runaway compute cost."
        )
    if total_usd > _CONFIG["daily_budget_usd"]:
        print(
            f"WARNING: estimated spend ${total_usd:.2f} exceeds the daily budget "
            f"${_CONFIG['daily_budget_usd']:.2f} (soft threshold — not halting)."
        )
