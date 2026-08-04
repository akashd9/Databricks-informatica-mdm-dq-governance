"""MDM match/merge gate for the customer entity. The reusable client classes
live in mdm_clients.py, not here — see that file's docstring for why (a real
"Found duplicate table" bug this split fixes). Informatica MDM SaaS's batch
match API is the primary path; a local fallback (blocking + pairwise
Jaro-Winkler scoring + union-find connected-components clustering) covers
dev/demo before that connectivity is configured. Both paths converge on the
same output contract: (golden_id, member_customer_id, match_confidence) — a
cluster assignment, not a merged record. The actual golden record is built
by survivorship.py from these clusters.
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

import dlt
from src.config_loader import load
from src.mdm.mdm_clients import InformaticaMDMSaaSClient, LocalMatchMergeFallback, _get_dbutils

_MATCH_CONFIG = load("match_rules.yml")

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
    name="silver_customer_match_groups",
    comment="Cluster assignments from Informatica MDM (or local fallback) mapping DQ-passed records to a golden_id.",
    table_properties={"quality": "silver"},
)
def silver_customer_match_groups():
    return _client.match_merge(dlt.read("silver_customer_dq_passed"))
