# Databricks notebook source
"""Governance gate: runs after the medallion pipeline, before the job is
considered successful. For every registered entity (see _ENTITIES below),
confirms (1) every published Gold column required by that entity's business
glossary is actually registered, and (2) Unity Catalog captured lineage from
Silver all the way through to Gold for this run. Raises — not just logs —
on any failure, so ungoverned or untraceable data never silently reaches
consumers. This is what turns "glossary and lineage exist alongside the
pipeline" (the stated gap) into "glossary and lineage are enforced as a gate
within it."
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
from src.config_loader import load

CATALOG = "mdm_dq_demo"

# entity -> (glossary config file, gold table name, upstream silver table the
# gold table's lineage must show)
_ENTITIES = {
    "customer": ("glossary_terms.yml", "gold_customer_golden", "silver_customer_match_groups"),
    "account": ("account_glossary_terms.yml", "gold_account_golden", "silver_account_match_groups"),
}

_registered = {r["column_name"] for r in spark.table(f"{CATALOG}.governance.business_glossary").collect()}
_total_terms = 0
_total_lineage_sources = 0

for _entity, (_config_file, _gold_table, _expected_upstream_table) in _ENTITIES.items():
    _required = load(_config_file)["required_glossary_mappings"]
    _total_terms += len(_required)
    _missing = [m["column"] for m in _required if m["column"] not in _registered]
    if _missing:
        raise ValueError(f"Glossary gate failed for {_entity}: no business glossary mapping for columns {_missing}")

    _lineage = spark.sql(f"""
        SELECT DISTINCT source_table_full_name
        FROM system.access.table_lineage
        WHERE target_table_full_name = '{CATALOG}.gold.{_gold_table}'
          AND event_time >= current_date() - INTERVAL 1 DAY
    """)
    _lineage_sources = {r["source_table_full_name"] for r in _lineage.collect()}
    _total_lineage_sources += len(_lineage_sources)

    _expected_upstream = f"{CATALOG}.silver.{_expected_upstream_table}"
    if not any(_expected_upstream in s for s in _lineage_sources):
        raise ValueError(
            f"Lineage gate failed for {_entity}: {CATALOG}.gold.{_gold_table} has no captured lineage from "
            f"{_expected_upstream} in the last 24h — Unity Catalog is not confirming this run's data flow."
        )

print(
    f"Governance gate passed for {len(_ENTITIES)} entities: {_total_terms} glossary terms checked, "
    f"lineage confirmed from {_total_lineage_sources} upstream tables total."
)
