"""Self-service business glossary contribution: a steward can propose a
glossary term for a Gold column via governance.glossary_submissions without
needing engineering to add it to a YAML config and redeploy the pipeline.
Approved submissions get merged into governance.business_glossary — the
same table glossary_gate.py enforces coverage against — so a self-service
term counts toward the glossary gate exactly like a YAML-defined one.

Plain importable library (setup_tables, apply_approved_submissions) with no
top-level execution — see glossary_submissions_apply.py for the job-task
entry point, same split as steward_review.py / steward_review_refresh.py.
"""
CATALOG = "mdm_dq_demo"


def setup_tables():
    """Idempotent — creates the submissions table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.governance.glossary_submissions (
            submission_id STRING,
            entity STRING,
            column_name STRING,
            proposed_term STRING,
            proposed_definition STRING,
            submitted_by STRING,
            submitted_at TIMESTAMP,
            status STRING,        -- 'pending' | 'approved' | 'rejected'
            reviewed_by STRING,
            reviewed_at TIMESTAMP,
            review_notes STRING
        ) USING DELTA
    """)


def apply_approved_submissions():
    """MERGEs every approved submission into business_glossary. Safe to call
    repeatedly — re-approving or re-running never duplicates a row, and a
    submission that only just got approved is picked up on the next call.
    """
    spark.sql(f"""
        MERGE INTO {CATALOG}.governance.business_glossary AS target
        USING (
            SELECT
                entity,
                column_name,
                proposed_term AS glossary_term,
                submitted_by AS steward,
                reviewed_at AS registered_at
            FROM {CATALOG}.governance.glossary_submissions
            WHERE status = 'approved'
        ) AS source
        ON target.entity = source.entity AND target.column_name = source.column_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    applied = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CATALOG}.governance.glossary_submissions WHERE status = 'approved'
    """).collect()[0]["n"]
    print(f"Glossary submissions: {applied} approved submission(s) merged into business_glossary.")
