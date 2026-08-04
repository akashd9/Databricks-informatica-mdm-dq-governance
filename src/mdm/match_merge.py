"""MDM match/merge gate. Informatica MDM SaaS's batch match API is the
primary path; a local fallback (blocking + pairwise Jaro-Winkler scoring +
union-find connected-components clustering) covers dev/demo before that
connectivity is configured. Both paths converge on the same output
contract: (golden_id, member_customer_id, match_confidence) — a cluster
assignment, not a merged record. The actual golden record is built by
survivorship.py from these clusters.
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
import abc
import time
import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from src.config_loader import load

_MATCH_CONFIG = load("match_rules.yml")


def _get_dbutils():
    # Lazy import: pyspark.dbutils only exists on real Databricks Runtime,
    # not the open-source pyspark package this module also needs to import
    # cleanly under (local tests, CI).
    from pyspark.dbutils import DBUtils

    return DBUtils(spark)


class InformaticaMDMClient(abc.ABC):
    @abc.abstractmethod
    def match_merge(self, df: DataFrame) -> DataFrame:
        """Return match_groups: golden_id, member_customer_id, match_confidence."""


class InformaticaMDMSaaSClient(InformaticaMDMClient):
    """Calls Informatica MDM SaaS (Customer 360 / IDMC MDM) batch match API:
    stage the DQ-passed candidate set, trigger the configured match rule set,
    poll the job, read back match groups. golden_id here is Informatica's
    Base Object ID (BOID) for the consolidated record.
    """

    def __init__(self, base_url: str, business_entity: str, rule_set: str, secret_scope: str = "informatica"):
        self.base_url = base_url.rstrip("/")
        self.business_entity = business_entity
        self.rule_set = rule_set
        self.secret_scope = secret_scope

    def _headers(self) -> dict:
        return {"INFA-SESSION-ID": _get_dbutils().secrets.get(self.secret_scope, "session_token")}

    def match_merge(self, df: DataFrame) -> DataFrame:
        import requests

        staging_path = f"/Volumes/mdm_dq_demo/staging/informatica/mdm_batch/{int(time.time())}"
        df.write.format("delta").mode("overwrite").save(staging_path)

        resp = requests.post(
            f"{self.base_url}/mdm/batch/v1/match",
            headers=self._headers(),
            json={
                "businessEntity": self.business_entity,
                "matchRuleSet": self.rule_set,
                "sourcePath": staging_path,
            },
            timeout=30,
        )
        resp.raise_for_status()
        job_id = resp.json()["jobId"]

        status = "RUNNING"
        while status == "RUNNING":
            time.sleep(10)
            poll = requests.get(f"{self.base_url}/mdm/batch/v1/jobs/{job_id}", headers=self._headers(), timeout=30)
            poll.raise_for_status()
            status = poll.json()["status"]

        if status != "SUCCESS":
            raise RuntimeError(f"Informatica MDM match job {job_id} ended in {status}")

        return spark.read.format("delta").load(poll.json()["matchGroupsPath"])


class LocalMatchMergeFallback(InformaticaMDMClient):
    """Blocking on (country_code, postal_code prefix) + pairwise scoring
    across every attribute in match_rules.yml's `match_attributes` (each
    weighted, method jaro_winkler or exact) + connected-components
    clustering via a pure-Python union-find. Used in dev/demo when
    Informatica MDM isn't reachable. Requires `jellyfish` on the driver.

    Deliberately not GraphFrames: attaching a Maven library to a Lakeflow
    Declarative Pipeline's cluster (pipelines.PipelineLibrary.maven) is a
    Private Preview API as of this writing, not guaranteed available on
    every workspace/account tier. Collecting the post-blocking,
    post-threshold matched pairs to the driver and running union-find in
    plain Python — the same algorithm pilot/run_pilot_validation.py already
    validates against real measured precision/recall — avoids that
    dependency entirely. Assumes the matched-pairs graph fits in driver
    memory: reasonable at pilot/demo scale, worth revisiting (GraphFrames
    once GA, or a distributed union-find) at high production volume.
    """

    def __init__(self, config: dict):
        self.config = config

    def match_merge(self, df: DataFrame) -> DataFrame:
        import jellyfish
        import pandas as pd

        id_col = self.config["id_column"]
        attributes = self.config["match_attributes"]
        columns = [a["column"] for a in attributes]
        total_weight = sum(a["weight"] for a in attributes)

        blocked = df.withColumn(
            "block_key",
            F.concat_ws("|", F.col("country_code"), F.substring(F.col("postal_code"), 1, 3)),
        )

        left = blocked.select(
            F.col(id_col).alias("id_a"),
            *[F.col(c).alias(f"{c}_a") for c in columns],
            "block_key",
        )
        right = blocked.select(
            F.col(id_col).alias("id_b"),
            *[F.col(c).alias(f"{c}_b") for c in columns],
            "block_key",
        )
        pairs = left.join(right, on="block_key").filter(F.col("id_a") < F.col("id_b"))

        @F.pandas_udf(DoubleType())
        def jw_sim(a: pd.Series, b: pd.Series) -> pd.Series:
            return a.combine(b, lambda x, y: jellyfish.jaro_winkler_similarity(x or "", y or ""))

        scored = pairs
        weighted_terms = []
        for attr in attributes:
            col, method, weight = attr["column"], attr["method"], attr["weight"]
            sim_col = f"_sim_{col}"
            if method == "jaro_winkler":
                scored = scored.withColumn(sim_col, jw_sim(f"{col}_a", f"{col}_b"))
            elif method == "exact":
                scored = scored.withColumn(sim_col, (F.col(f"{col}_a") == F.col(f"{col}_b")).cast("double"))
            else:
                raise ValueError(f"Unknown match method: {method!r} (attribute {col!r})")
            weighted_terms.append(F.col(sim_col) * F.lit(weight))

        scored = (
            scored.withColumn("match_confidence", sum(weighted_terms) / F.lit(total_weight))
            .filter(F.col("match_confidence") >= self.config["match_threshold"])
            .select("id_a", "id_b", "match_confidence")
        )

        # Union-find clustering, driver-side. matched_pairs is already
        # filtered to above-threshold pairs within a blocking bucket, so
        # it's expected to be far smaller than the full record count.
        matched_pairs = scored.collect()
        all_ids = [row[id_col] for row in df.select(id_col).distinct().collect()]
        parent = {i: i for i in all_ids}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for row in matched_pairs:
            union(row["id_a"], row["id_b"])

        clusters_df = spark.createDataFrame(
            [(i, find(i)) for i in all_ids], ["member_customer_id", "golden_id"]
        )

        edge_conf = (
            scored.select(F.col("id_a").alias("id"), "match_confidence")
            .unionByName(scored.select(F.col("id_b").alias("id"), "match_confidence"))
            .groupBy("id")
            .agg(F.avg("match_confidence").alias("match_confidence"))
        )

        return (
            clusters_df.join(edge_conf.withColumnRenamed("id", "member_customer_id"), on="member_customer_id", how="left")
            .fillna({"match_confidence": 1.0})  # singleton cluster: no match pair, trivially "self-matched"
        )


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
