"""DQ gate for the account entity — identical pattern to dq_gate.py
(customer), reusing InformaticaCloudDQClient/LocalDQFallback and the steward
override mechanism unchanged; only the config differs
(config/account_dq_rules.yml).
"""
import dlt
from pyspark.sql import functions as F
from src.config_loader import load
from src.quality.dq_rules import InformaticaCloudDQClient, LocalDQFallback
from src.governance.steward_review import get_approved_override_ids

_DQ_CONFIG = load("account_dq_rules.yml")
_MIN_SCORE = _DQ_CONFIG["informatica_dq"]["min_dq_score_for_gold"]
_MAPPING = _DQ_CONFIG["informatica_dq"]["mapping_name"]

_client = (
    InformaticaCloudDQClient(base_url=dbutils.secrets.get("informatica", "idmc_base_url"))
    if _DQ_CONFIG["informatica_dq"]["enabled"]
    else LocalDQFallback(_DQ_CONFIG["rules"])
)


@dlt.table(
    name="silver_account_dq_scored",
    comment="Every standardized account record, scored by the Informatica DQ gate (or local fallback).",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({
    "has_account_id": "account_id IS NOT NULL",
    "has_account_name": "account_name IS NOT NULL",
})
def silver_account_dq_scored():
    return _client.score(dlt.read("silver_account_standardized"), _MAPPING)


def _approved_ids():
    return get_approved_override_ids("account", "dq_quarantine")


@dlt.table(
    name="silver_account_dq_quarantine",
    comment="Records below the minimum DQ score and not yet steward-approved — held for review, never reach Gold.",
    table_properties={"quality": "silver"},
)
def silver_account_dq_quarantine():
    below_threshold = dlt.read("silver_account_dq_scored").filter(F.col("dq_score") < _MIN_SCORE)
    approved = _approved_ids()
    if approved:
        return below_threshold.filter(~F.col("account_id").isin(approved))
    return below_threshold


@dlt.table(
    name="silver_account_dq_passed",
    comment="Records that cleared the DQ gate, plus any steward-approved override, eligible for MDM match/merge.",
    table_properties={"quality": "silver"},
)
def silver_account_dq_passed():
    scored = dlt.read("silver_account_dq_scored")
    passed_naturally = scored.filter(F.col("dq_score") >= F.lit(_MIN_SCORE))

    approved = _approved_ids()
    if not approved:
        return passed_naturally

    steward_approved = scored.filter((F.col("dq_score") < F.lit(_MIN_SCORE)) & F.col("account_id").isin(approved))
    return passed_naturally.unionByName(steward_approved)
