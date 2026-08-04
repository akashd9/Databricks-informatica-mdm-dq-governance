"""DQ gate — the enforced pre-merge quality checkpoint called out in the
problem statement. Every standardized record is scored (via Informatica DQ,
or the local fallback while that connectivity isn't configured), then split:
records below the minimum score are quarantined for stewardship and never
reach match/merge or Gold; only records that pass continue downstream.
"""
import dlt
from pyspark.sql import functions as F
from src.config_loader import load
from src.quality.dq_rules import InformaticaCloudDQClient, LocalDQFallback

_DQ_CONFIG = load("dq_rules.yml")
_MIN_SCORE = _DQ_CONFIG["informatica_dq"]["min_dq_score_for_gold"]
_MAPPING = _DQ_CONFIG["informatica_dq"]["mapping_name"]

_client = (
    InformaticaCloudDQClient(base_url=dbutils.secrets.get("informatica", "idmc_base_url"))
    if _DQ_CONFIG["informatica_dq"]["enabled"]
    else LocalDQFallback()
)


@dlt.table(
    name="silver_customer_dq_scored",
    comment="Every standardized customer record, scored by the Informatica DQ gate (or local fallback).",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({
    "has_customer_id": "customer_id IS NOT NULL",
    "has_name": "full_name IS NOT NULL",
})
def silver_customer_dq_scored():
    return _client.score(dlt.read("silver_customer_standardized"), _MAPPING)


@dlt.table(
    name="silver_customer_dq_quarantine",
    comment="Records below the minimum DQ score — held for stewardship review, never reach match/merge or Gold.",
    table_properties={"quality": "silver"},
)
def silver_customer_dq_quarantine():
    return dlt.read("silver_customer_dq_scored").filter(F.col("dq_score") < _MIN_SCORE)


@dlt.table(
    name="silver_customer_dq_passed",
    comment="Records that cleared the DQ gate and are eligible for MDM match/merge.",
    table_properties={"quality": "silver"},
)
def silver_customer_dq_passed():
    return dlt.read("silver_customer_dq_scored").filter(F.col("dq_score") >= F.lit(_MIN_SCORE))
