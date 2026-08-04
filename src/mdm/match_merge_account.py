"""MDM match/merge gate for the account entity — reuses
InformaticaMDMSaaSClient/LocalMatchMergeFallback from match_merge.py
(customer) unchanged; only the config differs (config/account_match_rules.yml).
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

import dlt
from src.config_loader import load
from src.mdm.match_merge import InformaticaMDMSaaSClient, LocalMatchMergeFallback


def _get_dbutils():
    # Lazy import: pyspark.dbutils only exists on real Databricks Runtime,
    # not the open-source pyspark package this module also needs to import
    # cleanly under (local tests, CI).
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils

    return DBUtils(SparkSession.builder.getOrCreate())


_MATCH_CONFIG = load("account_match_rules.yml")

_client = (
    InformaticaMDMSaaSClient(
        base_url=_get_dbutils().secrets.get("informatica", "mdm_base_url"),
        business_entity=_MATCH_CONFIG["informatica_mdm"]["business_entity"],
        rule_set=_MATCH_CONFIG["informatica_mdm"]["match_rule_set"],
    )
    if _MATCH_CONFIG["informatica_mdm"]["enabled"]
    else LocalMatchMergeFallback(_MATCH_CONFIG)
)


@dlt.table(
    name="silver_account_match_groups",
    comment="Cluster assignments from Informatica MDM (or local fallback) mapping DQ-passed account records to a golden_id.",
    table_properties={"quality": "silver"},
)
def silver_account_match_groups():
    return _client.match_merge(dlt.read("silver_account_dq_passed"))
