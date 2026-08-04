"""Tests the pandas pilot-harness logic (pilot/run_pilot_validation.py).
Unlike the Spark-based tests, these need no Java/cluster, so they're real,
runnable coverage in any plain Python environment.
"""
import pandas as pd

from pilot.run_pilot_validation import score_dq, match_merge, evaluate_match_quality, UnionFind

RULES = {
    "rules": [
        {"name": "customer_id_not_null", "column": "customer_id", "type": "not_null", "severity": "critical"},
        {"name": "email_format_valid", "column": "email", "type": "regex",
         "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "severity": "warn"},
    ]
}

MATCH_CONFIG = {
    "match_attributes": [
        {"column": "full_name", "method": "jaro_winkler", "weight": 0.6},
        {"column": "tax_id", "method": "exact", "weight": 0.4},
    ],
    "match_threshold": 0.7,
}


def test_score_dq_weights_critical_and_warn_correctly():
    df = pd.DataFrame([
        {"customer_id": "C1", "email": "jane@acme.com"},   # passes both
        {"customer_id": None, "email": "not-an-email"},     # fails both
    ])
    scored = score_dq(df, RULES)
    # weight: critical=1.0, warn=0.5, total=1.5
    assert scored.loc[0, "dq_score"] == 1.0
    assert scored.loc[0, "dq_issues"] == ""
    assert scored.loc[1, "dq_score"] == 0.0
    assert "customer_id_not_null" in scored.loc[1, "dq_issues"]
    assert "email_format_valid" in scored.loc[1, "dq_issues"]


def test_union_find_merges_transitively():
    uf = UnionFind(["a", "b", "c", "d"])
    uf.union("a", "b")
    uf.union("b", "c")
    assert uf.find("a") == uf.find("c")
    assert uf.find("a") != uf.find("d")


def test_match_merge_clusters_near_duplicates_across_sources():
    df = pd.DataFrame([
        {"customer_id": "ERP-1", "full_name": "Jane Doe", "tax_id": "TAX-1",
         "country_code": "US", "postal_code": "10001"},
        {"customer_id": "CRM-1", "full_name": "Jane Doe", "tax_id": "TAX-1",
         "country_code": "US", "postal_code": "10099"},  # same block prefix "100"
        {"customer_id": "ERP-2", "full_name": "Bob Smith", "tax_id": "TAX-2",
         "country_code": "US", "postal_code": "99999"},  # different block, different person
    ])
    matched = match_merge(df, MATCH_CONFIG)
    jane_cluster = matched.loc[matched["customer_id"] == "ERP-1", "golden_id"].iloc[0]
    assert matched.loc[matched["customer_id"] == "CRM-1", "golden_id"].iloc[0] == jane_cluster
    assert matched.loc[matched["customer_id"] == "ERP-2", "golden_id"].iloc[0] != jane_cluster


def test_evaluate_match_quality_computes_precision_and_recall(tmp_path):
    # Two true customers (T1 has 2 records, T2 has 1); predicted clusters
    # correctly merge T1's pair but also wrongly merge one T2 record into it.
    df = pd.DataFrame([
        {"customer_id": "A", "golden_id": "G1"},
        {"customer_id": "B", "golden_id": "G1"},
        {"customer_id": "C", "golden_id": "G1"},  # false merge: C is really T2
    ])
    ground_truth = pd.DataFrame([
        {"source": "erp", "raw_id": "A", "true_customer_id": "T1"},
        {"source": "crm", "raw_id": "B", "true_customer_id": "T1"},
        {"source": "flatfile", "raw_id": "C", "true_customer_id": "T2"},
    ])
    gt_path = tmp_path / "ground_truth.csv"
    ground_truth.to_csv(gt_path, index=False)

    result = evaluate_match_quality(df, gt_path)

    # predicted pairs: (A,B) (A,C) (B,C) = 3; true pairs: (A,B) = 1
    # tp=1 (A,B), fp=2 ((A,C) and (B,C)), fn=0
    assert result["tp"] == 1
    assert result["fp"] == 2
    assert result["fn"] == 0
    assert abs(result["precision"] - (1 / 3)) < 1e-9
    assert result["recall"] == 1.0
