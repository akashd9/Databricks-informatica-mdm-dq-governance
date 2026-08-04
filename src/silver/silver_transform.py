"""Silver layer: maps each source's raw column names onto the canonical
customer entity schema, applies light standardization (trim, casing), and
unions everything into one entity-resolved-ready stream. This is deliberately
"dumb" standardization only — validation and match-readiness scoring belong
to the DQ gate and MDM gate downstream, not here.
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
    "erp": "bronze_erp_customer",
    "crm": "bronze_crm_customer",
    "flatfile": "bronze_flatfile_customer",
    "partner_api": "bronze_partner_api_customer",
}

# Raw-to-canonical column mapping lives in config/column_maps.yml — shared
# with pilot/run_pilot_validation.py, which can't import this module
# directly since it depends on `dlt` (only available inside a pipeline run).
_COLUMN_CONFIG = load("column_maps.yml")
_COLUMN_MAPS = _COLUMN_CONFIG["source_column_maps"]
_CANONICAL_COLUMNS = _COLUMN_CONFIG["canonical_columns"]


def _standardize(source_name: str, bronze_table: str):
    colmap = _COLUMN_MAPS[source_name]

    @dlt.table(
        name=f"silver_{source_name}_customer_std",
        comment=f"Standardized {source_name} customer records mapped to the canonical entity schema.",
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
            .withColumn("full_name", F.initcap("full_name"))
            .withColumn("email", F.lower("email"))
            .withColumn("country_code", F.upper("country_code"))
            .withColumn("email_domain", F.regexp_extract("email", r"@(.+)$", 1))
        )

    return _std


for _name, _table in _BRONZE_TABLES.items():
    _standardize(_name, _table)


@dlt.table(
    name="silver_customer_standardized",
    comment="Union of all source-standardized customer records, prior to the DQ gate.",
    table_properties={"quality": "silver"},
)
def silver_customer_standardized():
    frames = [dlt.read_stream(f"silver_{s}_customer_std") for s in _BRONZE_TABLES]
    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f)
    return unioned
