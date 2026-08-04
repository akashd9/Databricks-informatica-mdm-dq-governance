"""Survivorship: given match clusters, pick the surviving source record's
field values for each golden_id using configured source-priority + recency
precedence, and roll up cluster metadata (member count, source ids,
confidence) onto it for auditability.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_golden_records(dq_passed: DataFrame, match_groups: DataFrame, config: dict) -> DataFrame:
    priority = config["survivorship"]["source_priority"]
    priority_map = F.create_map(*[x for i, s in enumerate(priority) for x in (F.lit(s), F.lit(i))])

    joined = (
        dq_passed.join(match_groups, dq_passed["customer_id"] == match_groups["member_customer_id"])
        .withColumn("_priority_rank", priority_map[F.col("_source_system")])
    )

    survivor_window = Window.partitionBy("golden_id").orderBy(
        F.col("_priority_rank").asc_nulls_last(), F.col("updated_at").desc_nulls_last()
    )
    survivors = (
        joined.withColumn("_rank", F.row_number().over(survivor_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    member_counts = match_groups.groupBy("golden_id").agg(
        F.count("*").alias("source_record_count"),
        F.collect_set("member_customer_id").alias("source_customer_ids"),
        F.avg("match_confidence").alias("match_confidence"),
    )

    return survivors.join(member_counts, on="golden_id").withColumn("merged_at", F.current_timestamp())
