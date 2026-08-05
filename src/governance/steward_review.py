"""Steward review workflow.

Two tables:
  governance.steward_review_queue — refreshed by this script (job task, runs
    after the pipeline). Unions every entity's DQ quarantine and
    needs_review (low-confidence match) records into one queue a steward
    works from, instead of a steward having to know which of N silver/gold
    tables to look in.
  governance.steward_decisions — stewards (or a review UI/notebook built on
    top of this) INSERT rows here recording approve/reject decisions. This
    script only reads it, never writes to it — decisions are a human action.

The DQ gates (src/quality/dq_gate.py, dq_gate_account.py) read APPROVED
dq_quarantine decisions via get_approved_override_ids() and route those
specific records into *_dq_passed on the next pipeline run — so an approved
override actually changes what reaches Gold, not just a note in a queue no
one reads. needs_review (low-confidence match) decisions are recorded here
for audit trail but don't yet re-drive match/merge automatically — splitting
an already-merged cluster is a bigger change than reversing a threshold
comparison, and out of scope for this pass (see README roadmap).
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

# entity -> (id_column, quarantine table, gold table)
_ENTITIES = {
    "customer": ("customer_id", "silver_customer_dq_quarantine", "gold_customer_golden"),
    "account": ("account_id", "silver_account_dq_quarantine", "gold_account_golden"),
}


def setup_tables():
    """Idempotent — creates the two governance tables if they don't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.governance.steward_decisions (
            decision_id STRING,
            entity STRING,
            record_type STRING,   -- 'dq_quarantine' | 'low_confidence_match'
            business_key STRING,  -- customer_id / account_id (dq_quarantine) or golden_id (low_confidence_match)
            decision STRING,      -- 'approved' | 'rejected'
            reviewer STRING,
            notes STRING,
            decided_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.governance.steward_review_queue (
            entity STRING,
            record_type STRING,
            business_key STRING,
            reason STRING,
            detected_at TIMESTAMP,
            queued_at TIMESTAMP
        ) USING DELTA
    """)


def refresh_queue():
    """Rebuilds the queue from current quarantine + needs_review state.
    Overwrite (not append) — the queue reflects "what needs review right
    now", not a historical log; steward_decisions is the historical log.
    """
    id_col_by_entity, quarantine_rows, review_rows = {}, [], []

    for entity, (id_col, quarantine_table, gold_table) in _ENTITIES.items():
        id_col_by_entity[entity] = id_col

        quarantine_rows.append(
            spark.sql(f"""
                SELECT
                    '{entity}' AS entity,
                    'dq_quarantine' AS record_type,
                    {id_col} AS business_key,
                    dq_issues AS reason,
                    _ingested_at AS detected_at,
                    current_timestamp() AS queued_at
                FROM {CATALOG}.silver.{quarantine_table}
            """)
        )
        review_rows.append(
            spark.sql(f"""
                SELECT
                    '{entity}' AS entity,
                    'low_confidence_match' AS record_type,
                    golden_id AS business_key,
                    concat('match_confidence=', round(match_confidence, 3)) AS reason,
                    merged_at AS detected_at,
                    current_timestamp() AS queued_at
                FROM {CATALOG}.gold.{gold_table}
                WHERE review_status = 'needs_review'
            """)
        )

    queue = quarantine_rows[0]
    for df in quarantine_rows[1:] + review_rows:
        queue = queue.unionByName(df)

    queue.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.governance.steward_review_queue")
    print(f"Steward review queue refreshed: {queue.count()} items pending review.")


def get_approved_override_ids(entity: str, record_type: str) -> list:
    """Business keys with an APPROVED decision for (entity, record_type).
    Called from dq_gate.py / dq_gate_account.py to promote steward-approved
    quarantined records into *_dq_passed on the next run. A plain spark.table
    read of a small control table — not part of the DLT dataset lineage,
    same pattern as reading any other reference/config table.

    Defensive: on a fresh environment where unity_catalog_setup.py hasn't
    run yet (so governance.steward_decisions doesn't exist), returns []
    rather than failing the whole pipeline over an empty review queue.
    """
    try:
        rows = spark.sql(f"""
            SELECT DISTINCT business_key
            FROM {CATALOG}.governance.steward_decisions
            WHERE entity = '{entity}' AND record_type = '{record_type}' AND decision = 'approved'
        """).collect()
        return [r["business_key"] for r in rows]
    except Exception:
        return []
