"""Survivorship: given match clusters, pick the surviving source record's
field values for each golden_id using configured source-priority + recency
precedence, and roll up cluster metadata (member count, source ids,
confidence) onto it for auditability.

Entity-agnostic: which column on dq_passed identifies a record
(config["id_column"] — "customer_id" for customer, "account_id" for
account) varies per entity, but match_groups' own output column is always
literally "member_customer_id" regardless — that's match_merge.py's fixed
output contract, not a customer-specific name.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_golden_records(dq_passed: DataFrame, match_groups: DataFrame, config: dict) -> DataFrame:
    id_col = config["id_column"]
    priority = config["survivorship"]["source_priority"]
    priority_map = F.create_map(*[x for i, s in enumerate(priority) for x in (F.lit(s), F.lit(i))])

    joined = (
        dq_passed.join(match_groups, dq_passed[id_col] == match_groups["member_customer_id"])
        .withColumn("_priority_rank", priority_map[F.col("_source_system")])
    )

    survivor_window = Window.partitionBy("golden_id").orderBy(
        F.col("_priority_rank").asc_nulls_last(), F.col("updated_at").desc_nulls_last()
    )
    survivors = (
        joined.withColumn("_rank", F.row_number().over(survivor_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "match_confidence")  # drop the per-row value inherited from
        # match_groups via the join above — member_counts below computes the
        # real (aggregated) match_confidence for the cluster; keeping both
        # produces two same-named columns and an AMBIGUOUS_REFERENCE error
        # the moment anything downstream references match_confidence by name
        # (a real error hit running this pipeline for the first time).
    )

    member_counts = match_groups.groupBy("golden_id").agg(
        F.count("*").alias("source_record_count"),
        F.collect_set("member_customer_id").alias("source_customer_ids"),
        F.avg("match_confidence").alias("match_confidence"),
    )

    auto_merge_threshold = config["auto_merge_threshold"]
    return (
        survivors.join(member_counts, on="golden_id")
        .withColumn("merged_at", F.current_timestamp())
        .withColumn(
            "review_status",
            F.when(F.col("source_record_count") == 1, F.lit("single_source"))
            .when(F.col("match_confidence") >= auto_merge_threshold, F.lit("auto_merged"))
            .otherwise(F.lit("needs_review")),
        )
    )
