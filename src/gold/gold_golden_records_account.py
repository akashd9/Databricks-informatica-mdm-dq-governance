"""Gold gate for the account entity — reuses
src/mdm/survivorship.py::build_golden_records unchanged (it was already
generic, taking config as a parameter with no entity-specific assumptions);
only the config and output column selection differ from gold_golden_records.py
(customer).
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
import dlt
from pyspark.sql import functions as F
from src.config_loader import load
from src.mdm.survivorship import build_golden_records

_MATCH_CONFIG = load("account_match_rules.yml")
_DQ_CONFIG = load("account_dq_rules.yml")
_MIN_SCORE = _DQ_CONFIG["informatica_dq"]["min_dq_score_for_gold"]


@dlt.table(
    name="gold.gold_account_golden",
    comment="Curated, DQ-scored, de-duplicated golden account record.",
    # 30-day retention backs the point-in-time restore procedure in
    # docs/BACKUP_DR.md (RESTORE TABLE ... TO TIMESTAMP AS OF); the weekly
    # VACUUM task (src/governance/vacuum_maintenance.py) retains exactly
    # this same 720 hours so it never prunes files a restore inside the
    # window would need.
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 30 days",
    },
)
@dlt.expect_or_fail("has_golden_id", "golden_id IS NOT NULL")
@dlt.expect_or_fail("min_dq_score", f"dq_score >= {_MIN_SCORE}")
def gold_account_golden():
    dq_passed = dlt.read("silver.silver_account_dq_passed")
    match_groups = dlt.read("silver.silver_account_match_groups")
    golden = build_golden_records(dq_passed, match_groups, _MATCH_CONFIG)
    return golden.select(
        "golden_id",
        "account_name", "account_type", "industry", "annual_revenue", "owner_customer_id",
        "country_code", "postal_code",
        "dq_score", "dq_issues",
        "match_confidence", "source_record_count", "source_customer_ids",
        F.col("_source_system").alias("surviving_source_system"),
        "review_status", "merged_at",
    )
