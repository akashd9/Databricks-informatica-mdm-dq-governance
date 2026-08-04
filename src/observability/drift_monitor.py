"""Registers (idempotently) a Databricks Lakehouse Monitor on the Gold golden
record table for profile and drift metrics, then triggers a refresh as the
final job task. This makes drift detection a scheduled, queryable control
attached to the table itself, rather than a dashboard someone checks
manually after the fact.
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries

CATALOG = "mdm_dq_demo"
TABLE = f"{CATALOG}.gold.gold_customer_golden"

w = WorkspaceClient()

try:
    w.quality_monitors.get(table_name=TABLE)
except Exception:
    w.quality_monitors.create(
        table_name=TABLE,
        assets_dir=f"/Workspace/mdm-dq-medallion/monitoring/{CATALOG}",
        output_schema_name=f"{CATALOG}.governance",
        time_series=MonitorTimeSeries(timestamp_col="merged_at", granularities=["1 day"]),
        slicing_exprs=["surviving_source_system", "country_code"],
    )
    print(f"Created Lakehouse Monitor for {TABLE}.")

w.quality_monitors.run_refresh(table_name=TABLE)
print(f"Triggered drift/profile refresh for {TABLE}.")
