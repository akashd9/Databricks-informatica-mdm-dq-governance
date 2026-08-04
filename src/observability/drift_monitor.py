# Databricks notebook source
"""Registers (idempotently) a Databricks Lakehouse Monitor on each entity's
Gold table for profile and drift metrics, then triggers a refresh as the
final job task. This makes drift detection a scheduled, queryable control
attached to the table itself, rather than a dashboard someone checks
manually after the fact.
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries

CATALOG = "mdm_dq_demo"

# entity -> (gold table, slicing columns for the monitor)
_ENTITIES = {
    "customer": ("gold_customer_golden", ["surviving_source_system", "country_code"]),
    "account": ("gold_account_golden", ["surviving_source_system", "country_code"]),
}

w = WorkspaceClient()

for _entity, (_gold_table, _slicing_exprs) in _ENTITIES.items():
    # Gold tables actually live under `silver` — see the explanation in
    # src/governance/glossary_gate.py.
    table = f"{CATALOG}.silver.{_gold_table}"
    try:
        w.quality_monitors.get(table_name=table)
    except Exception:
        w.quality_monitors.create(
            table_name=table,
            assets_dir=f"/Workspace/mdm-dq-medallion/monitoring/{CATALOG}/{_entity}",
            output_schema_name=f"{CATALOG}.governance",
            time_series=MonitorTimeSeries(timestamp_col="merged_at", granularities=["1 day"]),
            slicing_exprs=_slicing_exprs,
        )
        print(f"Created Lakehouse Monitor for {table}.")

    w.quality_monitors.run_refresh(table_name=table)
    print(f"Triggered drift/profile refresh for {table}.")
