"""Gold gate: builds the golden customer record from matched clusters and
enforces the last two hard gates before anything is published as "Gold" —
a non-null golden_id (proof match/merge actually ran) and a minimum DQ
score. Either failing condition fails the whole pipeline update via
expect_or_fail, rather than silently publishing a partial Gold table.
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

_MATCH_CONFIG = load("match_rules.yml")
_DQ_CONFIG = load("dq_rules.yml")
_MIN_SCORE = _DQ_CONFIG["informatica_dq"]["min_dq_score_for_gold"]


@dlt.table(
    name="gold_customer_golden",
    comment="Curated, DQ-scored, de-duplicated golden customer record — the consumption-ready source of truth.",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_fail("has_golden_id", "golden_id IS NOT NULL")
@dlt.expect_or_fail("min_dq_score", f"dq_score >= {_MIN_SCORE}")
def gold_customer_golden():
    dq_passed = dlt.read("silver_customer_dq_passed")
    match_groups = dlt.read("silver_customer_match_groups")
    golden = build_golden_records(dq_passed, match_groups, _MATCH_CONFIG)
    return golden.select(
        "golden_id",
        F.col("full_name").alias("customer_name"),
        "email", "tax_id", "address_line1", "country_code", "postal_code",
        "dq_score", "dq_issues",
        "match_confidence", "source_record_count", "source_customer_ids",
        F.col("_source_system").alias("surviving_source_system"),
        "review_status", "merged_at",
    )
