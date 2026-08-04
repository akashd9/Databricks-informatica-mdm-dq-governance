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
    config = {
        "id_column": "customer_id",
        "survivorship": {"strategy": "source_priority_then_recency", "source_priority": ["erp", "crm"]},
        "auto_merge_threshold": 0.93,
    }

    row = build_golden_records(dq_passed, match_groups, config).collect()[0]

    assert row["golden_id"] == "G1"
    assert row["_source_system"] == "erp"  # erp outranks crm even though crm's record is newer
    assert row["source_record_count"] == 2
    assert row["review_status"] == "auto_merged"  # 0.95 >= auto_merge_threshold 0.93


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
    config = {
        "id_column": "customer_id",
        "survivorship": {"strategy": "source_priority_then_recency", "source_priority": ["erp"]},
        "auto_merge_threshold": 0.99,
    }

    row = build_golden_records(dq_passed, match_groups, config).collect()[0]

    assert row["customer_id"] == "C2"  # same source priority, most recently updated wins
    assert row["review_status"] == "needs_review"  # 0.95 < auto_merge_threshold 0.99


def test_survivorship_flags_single_source_records_distinctly():
    spark = _spark()
    dq_passed = spark.createDataFrame(
        [("C1", "Jane Doe", "jane@acme.com", "TAX-1", "123 Main St", "US", "10001", "2024-01-01", "erp")],
        ["customer_id", "full_name", "email", "tax_id", "address_line1", "country_code",
         "postal_code", "updated_at", "_source_system"],
    )
    match_groups = spark.createDataFrame(
        [("G1", "C1", 1.0)],
        ["golden_id", "member_customer_id", "match_confidence"],
    )
    config = {
        "id_column": "customer_id",
        "survivorship": {"strategy": "source_priority_then_recency", "source_priority": ["erp"]},
        "auto_merge_threshold": 0.93,
    }

    row = build_golden_records(dq_passed, match_groups, config).collect()[0]

    # Not a match/merge outcome at all — nothing to steward-review here.
    assert row["review_status"] == "single_source"


def test_survivorship_generalizes_to_a_different_entitys_id_column():
    """Proves build_golden_records isn't secretly customer-specific — same
    logic, but the identifying column is account_id, as used by
    gold_golden_records_account.py."""
    spark = _spark()
    dq_passed = spark.createDataFrame(
        [
            ("A1", "Acme Corp", "Customer", "Manufacturing", "erp", "2024-01-01"),
            ("A2", "Acme Corp.", "Customer", "Manufacturing", "crm", "2024-02-01"),
        ],
        ["account_id", "account_name", "account_type", "industry", "_source_system", "updated_at"],
    )
    match_groups = spark.createDataFrame(
        [("G1", "A1", 0.9), ("G1", "A2", 0.9)],
        ["golden_id", "member_customer_id", "match_confidence"],
    )
    config = {
        "id_column": "account_id",
        "survivorship": {"strategy": "source_priority_then_recency", "source_priority": ["erp", "crm"]},
        "auto_merge_threshold": 0.85,
    }

    row = build_golden_records(dq_passed, match_groups, config).collect()[0]

    assert row["golden_id"] == "G1"
    assert row["account_id"] == "A1"  # erp outranks crm
    assert row["review_status"] == "auto_merged"
