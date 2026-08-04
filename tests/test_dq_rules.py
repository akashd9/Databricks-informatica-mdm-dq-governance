from pyspark.sql import SparkSession
from src.quality.dq_rules import LocalDQFallback


def _spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_local_dq_fallback_scores_clean_record_as_1():
    spark = _spark()
    df = spark.createDataFrame(
        [("C1", "Jane Doe", "jane@acme.com", "TAX-12345")],
        ["customer_id", "full_name", "email", "tax_id"],
    )
    row = LocalDQFallback().score(df, mapping_name="unused").collect()[0]
    assert row["dq_score"] == 1.0
    assert row["dq_issues"] == ""


def test_local_dq_fallback_flags_missing_and_invalid_fields():
    spark = _spark()
    df = spark.createDataFrame(
        [("C2", None, "not-an-email", "???")],
        ["customer_id", "full_name", "email", "tax_id"],
    )
    row = LocalDQFallback().score(df, mapping_name="unused").collect()[0]
    assert row["dq_score"] == 0.25
    assert "missing_name" in row["dq_issues"]
    assert "invalid_email" in row["dq_issues"]
    assert "invalid_tax_id" in row["dq_issues"]
