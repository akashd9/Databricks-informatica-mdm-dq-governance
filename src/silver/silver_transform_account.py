"""Silver layer for the account entity — same pattern as
silver_transform.py (customer): map each source's raw columns onto the
canonical schema, light standardization only, union into one stream. Kept as
a separate file rather than a generic loop over entities so each entity's
DLT tables stay simple, static, and independently readable, matching how
Databricks pipelines are conventionally organized.
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

_BRONZE_TABLES = {
    "erp": "bronze_erp_account",
    "crm": "bronze_crm_account",
}

_COLUMN_CONFIG = load("account_column_maps.yml")
_COLUMN_MAPS = _COLUMN_CONFIG["source_column_maps"]
_CANONICAL_COLUMNS = _COLUMN_CONFIG["canonical_columns"]


def _standardize(source_name: str, bronze_table: str):
    colmap = _COLUMN_MAPS[source_name]

    @dlt.table(
        name=f"silver_{source_name}_account_std",
        comment=f"Standardized {source_name} account records mapped to the canonical entity schema.",
        table_properties={"quality": "silver"},
    )
    def _std():
        df = dlt.read_stream(bronze_table)
        select_exprs = [
            F.trim(F.col(colmap[c])).alias(c) if c in colmap else F.lit(None).cast("string").alias(c)
            for c in _CANONICAL_COLUMNS
        ]
        return (
            df.select(*select_exprs, "_source_system", "_source_priority", "_ingested_at", "_source_file")
            .withColumn("account_name", F.initcap("account_name"))
            .withColumn("country_code", F.upper("country_code"))
        )

    return _std


for _name, _table in _BRONZE_TABLES.items():
    _standardize(_name, _table)


@dlt.table(
    name="silver_account_standardized",
    comment="Union of all source-standardized account records, prior to the DQ gate.",
    table_properties={"quality": "silver"},
)
def silver_account_standardized():
    frames = [dlt.read_stream(f"silver_{s}_account_std") for s in _BRONZE_TABLES]
    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f)
    return unioned
