"""Governance gate: runs after the medallion pipeline, before the job is
considered successful. Confirms (1) every published Gold column required by
the business glossary is actually registered, and (2) Unity Catalog captured
lineage from Silver all the way through to Gold for this run. Raises — not
just logs — on either failure, so ungoverned or untraceable data never
silently reaches consumers. This is what turns "glossary and lineage exist
alongside the pipeline" (the stated gap) into "glossary and lineage are
enforced as a gate within it."
"""
from src.config_loader import load

CATALOG = "mdm_dq_demo"

_required = load("glossary_terms.yml")["required_glossary_mappings"]
_registered = {r["column_name"] for r in spark.table(f"{CATALOG}.governance.business_glossary").collect()}
_missing = [m["column"] for m in _required if m["column"] not in _registered]
if _missing:
    raise ValueError(f"Glossary gate failed: no business glossary mapping for columns {_missing}")

_lineage = spark.sql(f"""
    SELECT DISTINCT source_table_full_name
    FROM system.access.table_lineage
    WHERE target_table_full_name = '{CATALOG}.gold.gold_customer_golden'
      AND event_time >= current_date() - INTERVAL 1 DAY
""")
_lineage_sources = {r["source_table_full_name"] for r in _lineage.collect()}

_expected_upstream = f"{CATALOG}.silver.silver_customer_match_groups"
if not any(_expected_upstream in s for s in _lineage_sources):
    raise ValueError(
        f"Lineage gate failed: {CATALOG}.gold.gold_customer_golden has no captured lineage from "
        f"{_expected_upstream} in the last 24h — Unity Catalog is not confirming this run's data flow."
    )

print(
    f"Governance gate passed: {len(_registered)} glossary terms registered, "
    f"lineage confirmed from {len(_lineage_sources)} upstream tables."
)
