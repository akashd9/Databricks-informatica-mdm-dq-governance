from pyspark.sql import SparkSession
from src.mdm.survivorship import build_golden_records


def _spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_survivorship_picks_highest_priority_source_on_conflict():
    spark = _spark()
    dq_passed = spark.createDataFrame(
        [
            ("C1", "Jane Doe", "jane@acme.com", "TAX-1", "123 Main St", "US", "10001", "2024-01-01", "erp"),
            ("C2", "Jane D.", "jane.doe@acme.com", "TAX-1", "123 Main Street", "US", "10001", "2024-06-01", "crm"),
        ],
        ["customer_id", "full_name", "email", "tax_id", "address_line1", "country_code",
         "postal_code", "updated_at", "_source_system"],
    )
    match_groups = spark.createDataFrame(
        [("G1", "C1", 0.95), ("G1", "C2", 0.95)],
        ["golden_id", "member_customer_id", "match_confidence"],
    )
    config = {"survivorship": {"strategy": "source_priority_then_recency", "source_priority": ["erp", "crm"]}}

    row = build_golden_records(dq_passed, match_groups, config).collect()[0]

    assert row["golden_id"] == "G1"
    assert row["_source_system"] == "erp"  # erp outranks crm even though crm's record is newer
    assert row["source_record_count"] == 2


def test_survivorship_falls_back_to_recency_within_same_priority():
    spark = _spark()
    dq_passed = spark.createDataFrame(
        [
            ("C1", "Jane Doe", "jane@acme.com", "TAX-1", "123 Main St", "US", "10001", "2024-01-01", "erp"),
            ("C2", "Jane Doe", "jane@acme.com", "TAX-1", "123 Main St", "US", "10001", "2024-06-01", "erp"),
        ],
        ["customer_id", "full_name", "email", "tax_id", "address_line1", "country_code",
         "postal_code", "updated_at", "_source_system"],
    )
    match_groups = spark.createDataFrame(
        [("G1", "C1", 0.95), ("G1", "C2", 0.95)],
        ["golden_id", "member_customer_id", "match_confidence"],
    )
    config = {"survivorship": {"strategy": "source_priority_then_recency", "source_priority": ["erp"]}}

    row = build_golden_records(dq_passed, match_groups, config).collect()[0]

    assert row["customer_id"] == "C2"  # same source priority, most recently updated wins
