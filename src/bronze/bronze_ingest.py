"""Bronze layer: one Autoloader-backed DLT table per source system. Nothing
is cleaned or conformed here — we just land raw records with provenance
columns (_source_system, _source_priority, _ingested_at) that every
downstream gate relies on for lineage and survivorship.
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from pyspark.sql import SparkSession

# Explicit acquisition rather than relying on the injected notebook global:
# functions defined in an imported module (not the top-level executing
# notebook/pipeline-library file) don't automatically see that global.
spark = SparkSession.builder.getOrCreate()
import dlt
from pyspark.sql import functions as F
from src.config_loader import load

# Each entity's sources.yml lists its own source systems (customer uses all
# 4; account uses just erp/crm — see config/account_sources.yml). Bronze
# table names are derived from source['entity'], so adding a new entity here
# only ever means adding a new sources.yml, never touching this loop.
_SOURCES = load("sources.yml")["sources"] + load("account_sources.yml")["sources"]


def _make_bronze_table(source: dict):
    @dlt.table(
        name=f"bronze_{source['name']}_{source['entity']}",
        comment=f"Raw {source['entity']} records ingested from {source['name']} via Autoloader.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source["format"])
            .option("cloudFiles.schemaLocation", f"{source['landing_path']}/_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source["landing_path"])
            .withColumn("_source_system", F.lit(source["name"]))
            .withColumn("_source_priority", F.lit(source["priority"]))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    return _bronze


for _source in _SOURCES:
    _make_bronze_table(_source)
