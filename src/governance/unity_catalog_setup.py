"""One-time / idempotent Unity Catalog setup: catalog, schemas, the business
glossary registry table, and column-level tags on the Gold table. Tags do
double duty as PII/sensitivity classification and as the glossary link
surfaced in Catalog Explorer — governance metadata that lives ON the table
Unity Catalog already tracks lineage for, not in a separate wiki.

Run this once per environment (e.g. as a manual job / notebook run), not as
part of the scheduled pipeline.
"""
from pyspark.sql import functions as F
from src.config_loader import load

CATALOG = "mdm_dq_demo"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for _schema in ("bronze", "silver", "gold", "governance", "config"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.governance.business_glossary (
    entity STRING,
    column_name STRING,
    glossary_term STRING,
    steward STRING,
    registered_at TIMESTAMP
) USING DELTA
""")

_glossary_config = load("glossary_terms.yml")
_rows = [
    (_glossary_config["entity"], m["column"], m["glossary_term"], "data_governance_team")
    for m in _glossary_config["required_glossary_mappings"]
]
(
    spark.createDataFrame(_rows, ["entity", "column_name", "glossary_term", "steward"])
    .withColumn("registered_at", F.current_timestamp())
    .write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.governance.business_glossary")
)

# key -> (tag_key, tag_value | None). None-valued tags are plain classification flags.
_TAGGED_COLUMNS = {
    "email": [("pii", None), ("glossary", "Customer Contact Email")],
    "tax_id": [("pii", None), ("sensitive", None), ("glossary", "Tax Identification Number")],
    "customer_name": [("glossary", "Customer Legal Name")],
    "golden_id": [("glossary", "Customer Golden Record ID")],
    "dq_score": [("glossary", "Data Quality Score")],
}
for _column, _tags in _TAGGED_COLUMNS.items():
    for _key, _value in _tags:
        _clause = f"'{_key}' = '{_value}'" if _value else f"'{_key}'"
        spark.sql(f"""
            ALTER TABLE {CATALOG}.gold.gold_customer_golden
            ALTER COLUMN {_column} SET TAGS ({_clause})
        """)

print(f"Unity Catalog setup complete for {CATALOG}: schemas created, "
      f"{len(_rows)} glossary terms registered, tags applied to gold_customer_golden.")
