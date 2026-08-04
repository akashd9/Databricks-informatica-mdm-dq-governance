"""One-time / idempotent Unity Catalog setup: catalog, schemas, the business
glossary registry table, and column-level tags on each entity's Gold table.
Tags do double duty as PII/sensitivity classification and as the glossary
link surfaced in Catalog Explorer — governance metadata that lives ON the
table Unity Catalog already tracks lineage for, not in a separate wiki.

Loops over every registered entity (see _ENTITIES below) so adding a new
entity's governance setup is a config addition here, not new code — the
column/glossary-term mapping itself is read from each entity's own
glossary_terms.yml, not duplicated in this file.

Run this once per environment (e.g. as a manual job / notebook run), not as
part of the scheduled pipeline.
"""
from pyspark.sql import functions as F
from src.config_loader import load
from src.governance.steward_review import setup_tables as setup_steward_tables
from src.governance.glossary_submissions import setup_tables as setup_glossary_submission_tables

CATALOG = "mdm_dq_demo"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for _schema in ("bronze", "silver", "gold", "governance", "config"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

# Must exist before the pipeline's first run — dq_gate.py/dq_gate_account.py
# read steward_decisions on every run to apply approved overrides.
setup_steward_tables()
setup_glossary_submission_tables()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.governance.business_glossary (
    entity STRING,
    column_name STRING,
    glossary_term STRING,
    steward STRING,
    registered_at TIMESTAMP
) USING DELTA
""")

# entity -> (glossary config file, gold table name, {column: [extra classification tags]})
# Classification tags (pii, sensitive, ...) are supplementary to the
# glossary tag every mapped column always gets; not every entity needs any.
_ENTITIES = {
    "customer": ("glossary_terms.yml", "gold_customer_golden", {
        "email": ["pii"],
        "tax_id": ["pii", "sensitive"],
    }),
    "account": ("account_glossary_terms.yml", "gold_account_golden", {}),
}

_all_rows = []
for _entity, (_config_file, _gold_table, _classification_tags) in _ENTITIES.items():
    _glossary_config = load(_config_file)
    _all_rows.extend(
        (_glossary_config["entity"], m["column"], m["glossary_term"], "data_governance_team")
        for m in _glossary_config["required_glossary_mappings"]
    )

    for m in _glossary_config["required_glossary_mappings"]:
        _column = m["column"]
        _tags = [(t, None) for t in _classification_tags.get(_column, [])] + [("glossary", m["glossary_term"])]
        for _key, _value in _tags:
            _clause = f"'{_key}' = '{_value}'" if _value else f"'{_key}'"
            spark.sql(f"""
                ALTER TABLE {CATALOG}.gold.{_gold_table}
                ALTER COLUMN {_column} SET TAGS ({_clause})
            """)

# MERGE, not overwrite: self-service glossary submissions (see
# glossary_submissions.py) can add rows here too, keyed on the same
# (entity, column_name). An overwrite on every unity_catalog_setup.py re-run
# would silently wipe those out; MERGE lets YAML-defined and self-service
# terms coexist, re-running this is always safe.
(
    spark.createDataFrame(_all_rows, ["entity", "column_name", "glossary_term", "steward"])
    .withColumn("registered_at", F.current_timestamp())
    .createOrReplaceTempView("_yaml_glossary_terms")
)
spark.sql(f"""
    MERGE INTO {CATALOG}.governance.business_glossary AS target
    USING _yaml_glossary_terms AS source
    ON target.entity = source.entity AND target.column_name = source.column_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(
    f"Unity Catalog setup complete for {CATALOG}: schemas created, "
    f"{len(_all_rows)} glossary terms registered across {len(_ENTITIES)} entities, tags applied to Gold tables."
)
