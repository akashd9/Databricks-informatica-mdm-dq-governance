"""Local DQ rule engine + Informatica Data Quality (IDMC Cloud Data Quality)
integration.

The local engine gives deterministic, fast checks usable in dev/demo or as a
cheap pre-check. InformaticaCloudDQClient wraps the same batch through an
external IDQ mapping for checks that should stay centrally governed
(standardization dictionaries, address validation, fuzzy dedup-readiness
scoring) instead of being reimplemented in Spark. Both implementations honor
the same contract: given a DataFrame, return it with dq_score (double) and
dq_issues (comma-separated string) columns added, so the DQ gate doesn't care
which one ran.
"""

from pyspark.sql import SparkSession

# Explicit acquisition rather than relying on the injected notebook global:
# functions defined in an imported module (not the top-level executing
# notebook/pipeline-library file) don't automatically see that global.
spark = SparkSession.builder.getOrCreate()
import abc
import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class InformaticaDQClient(abc.ABC):
    @abc.abstractmethod
    def score(self, df: DataFrame, mapping_name: str) -> DataFrame:
        """Return df with dq_score (double) and dq_issues (string) columns added."""


class InformaticaCloudDQClient(InformaticaDQClient):
    """Calls an Informatica IDMC Cloud Data Quality mapping task via its REST
    API. Batch pattern: stage the candidate batch where IDQ can read it,
    trigger the mapping task, poll for completion, read the scored output
    back. The session token is pulled from a Databricks secret scope — never
    hardcoded — and refreshed on every call since IDMC sessions expire.
    """

    def __init__(self, base_url: str, secret_scope: str = "informatica"):
        self.base_url = base_url.rstrip("/")
        self.secret_scope = secret_scope

    def _headers(self) -> dict:
        # Lazy import: pyspark.dbutils only exists on real Databricks
        # Runtime, not the open-source pyspark package this module also
        # needs to import cleanly under (local tests, CI).
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        return {"INFA-SESSION-ID": dbutils.secrets.get(self.secret_scope, "session_token")}

    def score(self, df: DataFrame, mapping_name: str) -> DataFrame:
        import requests

        staging_path = f"/Volumes/mdm_dq_demo/staging/informatica/{mapping_name}/{int(time.time())}"
        df.write.format("delta").mode("overwrite").save(staging_path)

        resp = requests.post(
            f"{self.base_url}/public/core/v3/mtt/{mapping_name}/run",
            headers=self._headers(),
            json={"sourceConnection": {"path": staging_path}},
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["runId"]

        status = "RUNNING"
        while status == "RUNNING":
            time.sleep(5)
            poll = requests.get(
                f"{self.base_url}/public/core/v3/mtt/runs/{run_id}", headers=self._headers(), timeout=30
            )
            poll.raise_for_status()
            status = poll.json()["status"]

        if status != "SUCCESS":
            raise RuntimeError(f"Informatica DQ mapping {mapping_name} run {run_id} ended in {status}")

        return spark.read.format("delta").load(poll.json()["outputPath"])


_SEVERITY_WEIGHTS = {"critical": 1.0, "warn": 0.5}


def _rule_pass_condition(rule: dict):
    column = F.col(rule["column"])
    rule_type = rule["type"]
    if rule_type == "not_null":
        return column.isNotNull()
    if rule_type == "regex":
        return column.isNotNull() & column.rlike(rule["pattern"])
    if rule_type == "allowed_values":
        return column.isin(rule["values"])
    raise ValueError(f"Unknown DQ rule type: {rule_type!r} (rule {rule['name']!r})")


class LocalDQFallback(InformaticaDQClient):
    """Used in dev/demo when Informatica connectivity isn't configured yet.
    Mirrors the IDQ mapping's output contract (dq_score, dq_issues) so
    downstream code is identical either way. Driven entirely by the
    declarative `rules` list in config/dq_rules.yml — adding, removing, or
    reweighting a check (via `severity`) is a config change, not a code
    change. `severity: critical` rules count double a `warn` rule's weight
    toward dq_score.
    """

    def __init__(self, rules: list[dict]):
        self.rules = rules

    def score(self, df: DataFrame, mapping_name: str) -> DataFrame:
        weights = [_SEVERITY_WEIGHTS[r["severity"]] for r in self.rules]
        total_weight = sum(weights)

        pass_conditions = [_rule_pass_condition(r) for r in self.rules]
        weighted_pass = [
            F.when(cond, F.lit(w)).otherwise(F.lit(0.0))
            for cond, w in zip(pass_conditions, weights)
        ]
        score_expr = sum(weighted_pass) / F.lit(total_weight)
        issue_exprs = [F.when(~cond, F.lit(r["name"])) for cond, r in zip(pass_conditions, self.rules)]

        return df.withColumn("dq_score", score_expr).withColumn(
            "dq_issues", F.concat_ws(",", *issue_exprs)
        )
