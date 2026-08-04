"""Pandas-only pilot validation harness — no Spark, no Java, no Databricks
cluster required. Runs the synthetic dataset from generate_pilot_dataset.py
through logic that mirrors src/quality/dq_rules.py's LocalDQFallback and
src/mdm/match_merge.py's LocalMatchMergeFallback (same config, same rule
semantics, reimplemented in pandas since those modules depend on a live
Spark/dlt context this script doesn't have). Reports DQ score distribution,
quarantine rate, and match/merge precision/recall against the known ground
truth, then suggests config threshold adjustments.

Usage: python pilot/generate_pilot_dataset.py && python pilot/run_pilot_validation.py
"""
import re
from pathlib import Path

import jellyfish
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DIR = Path(__file__).resolve().parent / "sample_data"
GROUND_TRUTH_PATH = Path(__file__).resolve().parent / "ground_truth.csv"

_SEVERITY_WEIGHTS = {"critical": 1.0, "warn": 0.5}


def _load_config(name):
    with open(REPO_ROOT / "config" / name) as f:
        return yaml.safe_load(f)


def load_and_standardize():
    column_config = _load_config("column_maps.yml")
    col_maps = column_config["source_column_maps"]
    canonical_cols = column_config["canonical_columns"]

    raw = {
        "erp": pd.read_parquet(SAMPLE_DIR / "erp" / "customers.parquet"),
        "crm": pd.read_json(SAMPLE_DIR / "crm" / "customers.json"),
        "flatfile": pd.read_csv(SAMPLE_DIR / "flatfile" / "customers.csv"),
        "partner_api": pd.read_json(SAMPLE_DIR / "api" / "customers.json"),
    }

    frames = []
    for source, df in raw.items():
        colmap = col_maps[source]
        std = pd.DataFrame(index=df.index)
        for canon_col in canonical_cols:
            raw_col = colmap.get(canon_col)
            std[canon_col] = df[raw_col] if raw_col in df.columns else None
        std["_source_system"] = source
        std["full_name"] = std["full_name"].astype("string").str.strip().str.title()
        std["email"] = std["email"].astype("string").str.strip().str.lower()
        std["country_code"] = std["country_code"].astype("string").str.strip().str.upper()
        frames.append(std)

    return pd.concat(frames, ignore_index=True)


def score_dq(df, dq_config):
    rules = dq_config["rules"]
    total_weight = sum(_SEVERITY_WEIGHTS[r["severity"]] for r in rules)

    scores = pd.Series(0.0, index=df.index)
    issues = pd.Series("", index=df.index)

    for rule in rules:
        col = df[rule["column"]]
        weight = _SEVERITY_WEIGHTS[rule["severity"]]

        if rule["type"] == "not_null":
            passed = col.notna()
        elif rule["type"] == "regex":
            pattern = re.compile(rule["pattern"])
            passed = col.apply(lambda v: bool(pattern.match(v)) if pd.notna(v) else False)
        elif rule["type"] == "allowed_values":
            passed = col.isin(rule["values"])
        else:
            raise ValueError(f"Unknown DQ rule type: {rule['type']!r}")

        scores += passed.astype(float) * weight
        issues = issues.where(passed, issues + rule["name"] + ",")

    df = df.copy()
    df["dq_score"] = scores / total_weight
    df["dq_issues"] = issues.str.rstrip(",")
    return df


class UnionFind:
    def __init__(self, ids):
        self.parent = {i: i for i in ids}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def match_merge(df, match_config):
    """NOTE: this harness's customer_id values are source-prefixed
    (ERP-0001, CRM-0001, ...) by construction in generate_pilot_dataset.py,
    so they're guaranteed globally unique here. The production code in
    src/mdm/match_merge.py and src/mdm/survivorship.py uses the *raw*
    customer_id from each source as-is with no such prefix — real ERP/CRM
    systems can plausibly assign colliding IDs independently. That's a real
    gap this pilot's clean synthetic IDs won't surface; see the caveat
    printed in main().
    """
    attributes = match_config["match_attributes"]
    total_weight = sum(a["weight"] for a in attributes)
    threshold = match_config["match_threshold"]

    df = df.reset_index(drop=True)
    df["block_key"] = df["country_code"].astype(str) + "|" + df["postal_code"].astype(str).str[:3]

    uf = UnionFind(df["customer_id"].tolist())
    pair_scores = {}

    for _, group in df.groupby("block_key"):
        rows = group.to_dict("records")
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                a, b = rows[i], rows[j]
                weighted_sum = 0.0
                for attr in attributes:
                    col, method, weight = attr["column"], attr["method"], attr["weight"]
                    va, vb = a.get(col), b.get(col)
                    if method == "jaro_winkler":
                        sim = jellyfish.jaro_winkler_similarity(va or "", vb or "")
                    elif method == "exact":
                        sim = 1.0 if (va == vb and va is not None) else 0.0
                    else:
                        raise ValueError(f"Unknown match method: {method!r}")
                    weighted_sum += sim * weight
                confidence = weighted_sum / total_weight
                if confidence >= threshold:
                    uf.union(a["customer_id"], b["customer_id"])
                    pair_scores[(a["customer_id"], b["customer_id"])] = confidence

    df["golden_id"] = df["customer_id"].apply(uf.find)

    member_scores = {}
    for (a, b), conf in pair_scores.items():
        member_scores.setdefault(a, []).append(conf)
        member_scores.setdefault(b, []).append(conf)
    df["match_confidence"] = df["customer_id"].map(
        lambda cid: (sum(member_scores[cid]) / len(member_scores[cid])) if cid in member_scores else 1.0
    )

    return df


def survivorship(df, match_config):
    priority = {s: i for i, s in enumerate(match_config["survivorship"]["source_priority"])}
    df = df.copy()
    df["_priority_rank"] = df["_source_system"].map(priority).fillna(len(priority))
    df = df.sort_values(["golden_id", "_priority_rank", "updated_at"], ascending=[True, True, False])
    survivors = df.groupby("golden_id", as_index=False).first()
    counts = df.groupby("golden_id").size().rename("source_record_count")
    return survivors.join(counts, on="golden_id")


def evaluate_match_quality(df, ground_truth_path):
    """Pairwise precision/recall of predicted golden_id clusters against the
    known true_customer_id clusters from generate_pilot_dataset.py."""
    truth = pd.read_csv(ground_truth_path)
    truth_map = dict(zip(truth["raw_id"], truth["true_customer_id"]))
    df = df.copy()
    df["true_id"] = df["customer_id"].map(truth_map)

    predicted_pairs, true_pairs = set(), set()
    for _, group in df.groupby("golden_id"):
        ids = sorted(group["customer_id"])
        predicted_pairs.update((ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids)))
    for _, group in df.groupby("true_id"):
        ids = sorted(group["customer_id"])
        true_pairs.update((ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids)))

    tp = len(predicted_pairs & true_pairs)
    fp = len(predicted_pairs - true_pairs)
    fn = len(true_pairs - predicted_pairs)
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn}


def main():
    dq_config = _load_config("dq_rules.yml")
    match_config = _load_config("match_rules.yml")

    standardized = load_and_standardize()
    print(f"Loaded {len(standardized)} standardized records from 4 sources.\n")

    scored = score_dq(standardized, dq_config)
    min_score = dq_config["informatica_dq"]["min_dq_score_for_gold"]
    passed = scored[scored["dq_score"] >= min_score]
    quarantined = scored[scored["dq_score"] < min_score]
    quarantine_rate = len(quarantined) / len(scored)

    print("=== DQ Gate ===")
    print(f"  min_dq_score_for_gold: {min_score}")
    print(f"  score distribution:\n{scored['dq_score'].describe().to_string()}")
    print(f"  quarantine rate: {quarantine_rate:.1%} ({len(quarantined)}/{len(scored)})\n")

    matched = match_merge(passed, match_config)
    dedup_rate = 1 - matched["golden_id"].nunique() / len(matched)

    print("=== MDM Match/Merge Gate ===")
    print(f"  match_threshold: {match_config['match_threshold']}")
    print(f"  DQ-passed records: {len(passed)}")
    print(f"  golden records produced: {matched['golden_id'].nunique()}")
    print(f"  dedup rate: {dedup_rate:.1%}")
    print(f"  avg match confidence: {matched['match_confidence'].mean():.3f}\n")

    quality = evaluate_match_quality(matched, GROUND_TRUTH_PATH)
    print("=== Match Quality vs. Ground Truth ===")
    print(f"  pairwise precision: {quality['precision']:.3f}  (of predicted-same-customer pairs, how many really are)")
    print(f"  pairwise recall:    {quality['recall']:.3f}  (of true duplicate pairs, how many we caught)")
    print(f"  pairwise f1:        {quality['f1']:.3f}")
    print(f"  tp={quality['tp']} fp={quality['fp']} fn={quality['fn']}\n")

    print("=== Tuning suggestions ===")
    suggestions = []
    if quarantine_rate > 0.15:
        suggestions.append(
            f"DQ quarantine rate ({quarantine_rate:.1%}) exceeds the 15% anomaly-gate threshold in "
            f"src/observability/anomaly_gate.py. Lower min_dq_score_for_gold or check whether a DQ "
            f"rule is too strict for this data."
        )
    if quality["precision"] < 0.90:
        suggestions.append(
            f"Precision ({quality['precision']:.3f}) is below 0.90 — match_threshold "
            f"({match_config['match_threshold']}) may be too low, merging distinct people. Raise it."
        )
    if quality["recall"] < 0.80:
        suggestions.append(
            f"Recall ({quality['recall']:.3f}) is below 0.80 — match_threshold may be too high, or "
            f"attribute weights may be under-weighting a reliable field (e.g. tax_id exact match). "
            f"Lower match_threshold or raise a high-signal attribute's weight."
        )
    if not suggestions:
        suggestions.append("Current thresholds look reasonable against this pilot dataset.")
    for s in suggestions:
        print(f"  - {s}")

    print(
        "\n=== Known limitation this pilot does NOT surface ===\n"
        "  This harness's customer_id values are source-prefixed (ERP-0001, CRM-0001, ...)\n"
        "  by construction, so they're guaranteed globally unique. The production pipeline\n"
        "  (src/mdm/match_merge.py, src/mdm/survivorship.py) uses each source's raw ID as-is\n"
        "  with no such prefix. Real ERP/CRM systems can independently assign colliding IDs.\n"
        "  Before pointing this at real source data, add a globally-unique key (e.g.\n"
        "  concat(_source_system, ':', customer_id)) as the match/merge and survivorship join\n"
        "  key, keeping raw customer_id as a separate business-key attribute."
    )


if __name__ == "__main__":
    main()
