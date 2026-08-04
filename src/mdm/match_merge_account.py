"""MDM match/merge gate for the account entity — reuses
InformaticaMDMSaaSClient/LocalMatchMergeFallback from match_merge.py
(customer) unchanged; only the config differs (config/account_match_rules.yml).
"""
import dlt
from src.config_loader import load
from src.mdm.match_merge import InformaticaMDMSaaSClient, LocalMatchMergeFallback

_MATCH_CONFIG = load("account_match_rules.yml")

_client = (
    InformaticaMDMSaaSClient(
        base_url=dbutils.secrets.get("informatica", "mdm_base_url"),
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
