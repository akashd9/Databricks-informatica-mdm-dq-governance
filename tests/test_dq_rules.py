from pyspark.sql import SparkSession
from src.quality.dq_rules import LocalDQFallback

# Deliberately independent of config/dq_rules.yml so tuning that file (Phase 1
# threshold tuning) can't silently change what this test asserts.
RULES = [
    {"name": "customer_id_not_null", "column": "customer_id", "type": "not_null", "severity": "critical"},
    {"name": "name_not_null", "column": "full_name", "type": "not_null", "severity": "critical"},
    {"name": "email_format_valid", "column": "email", "type": "regex",
     "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "severity": "warn"},
    {"name": "tax_id_format", "column": "tax_id", "type": "regex",
     "pattern": r"^[A-Z0-9\-]{5,20}$", "severity": "warn"},
]
TOTAL_WEIGHT = 1.0 + 1.0 + 0.5 + 0.5  # 2 critical + 2 warn


def _spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_local_dq_fallback_scores_clean_record_as_1():
    spark = _spark()
    df = spark.createDataFrame(
        [("C1", "Jane Doe", "jane@acme.com", "TAX-12345")],
        ["customer_id", "full_name", "email", "tax_id"],
    )
    row = LocalDQFallback(RULES).score(df, mapping_name="unused").collect()[0]
    assert row["dq_score"] == 1.0
    assert row["dq_issues"] == ""


def test_local_dq_fallback_flags_missing_and_invalid_fields():
    spark = _spark()
    df = spark.createDataFrame(
        [("C2", None, "not-an-email", "???")],
        ["customer_id", "full_name", "email", "tax_id"],
    )
    row = LocalDQFallback(RULES).score(df, mapping_name="unused").collect()[0]
    # Only customer_id_not_null (weight 1.0) passes out of 3.0 total weight.
    assert abs(row["dq_score"] - (1.0 / TOTAL_WEIGHT)) < 1e-9
    assert "name_not_null" in row["dq_issues"]
    assert "email_format_valid" in row["dq_issues"]
    assert "tax_id_format" in row["dq_issues"]


def test_critical_failure_costs_more_than_warn_failure():
    spark = _spark()
    warn_only_fail = spark.createDataFrame(
        [("C3", "Jane Doe", "not-an-email", "TAX-12345")],
        ["customer_id", "full_name", "email", "tax_id"],
    )
    critical_fail = spark.createDataFrame(
        [("C4", None, "jane@acme.com", "TAX-12345")],
        ["customer_id", "full_name", "email", "tax_id"],
    )
    warn_score = LocalDQFallback(RULES).score(warn_only_fail, mapping_name="unused").collect()[0]["dq_score"]
    critical_score = LocalDQFallback(RULES).score(critical_fail, mapping_name="unused").collect()[0]["dq_score"]
    assert warn_score > critical_score
