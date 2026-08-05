# Backup, Retention & Disaster Recovery

What's actually implemented, what a restore looks like, and what's
genuinely not covered yet — in that order, so this doc doesn't read as a
promise this project doesn't keep.

## What's implemented

**Delta time travel, 30-day window on Gold.** `src/gold/gold_golden_records.py`
and `gold_golden_records_account.py` set `delta.deletedFileRetentionDuration`
and `delta.logRetentionDuration` to `interval 30 days` in `table_properties`.
That's the actual backup mechanism for this project: every write to
`gold.gold_customer_golden` / `gold.gold_account_golden` is a new Delta
version, and any version (or any timestamp) inside the last 30 days is
queryable and restorable — no separate backup job, snapshot, or export ever
runs, because Delta's transaction log already *is* the backup.

Bronze and silver tables don't set this property explicitly (they inherit
Delta's default retention, 7 days) — there's no restore requirement
documented for intermediate layers today; if that changes, add the same two
properties to the relevant `@dlt.table(...)` call, the same way Gold does it.

**Weekly VACUUM, matched to the retention window.**
`src/governance/vacuum_maintenance.py` runs `VACUUM <table> RETAIN 720 HOURS`
(720h = 30 days) against every table in `bronze`, `silver`, and `gold`, on
its own weekly schedule (`resources/jobs.yml`'s
`mdm_dq_medallion_maintenance_job`, Sundays 03:00 UTC) — deliberately not
part of the hourly pipeline job, since VACUUM is storage cleanup, not data
flow. The 720-hour constant is the one number that has to stay in sync with
Gold's `delta.deletedFileRetentionDuration`; if you widen one, widen both,
or VACUUM will delete files a restore inside the (now-wider) window still
needs. Delta's own `retentionDurationCheck` (on by default, refuses VACUUM
below 168 hours / 7 days) is left enabled rather than disabled, specifically
so a future edit that shrinks the retention constant too far fails loudly
instead of silently under-retaining.

Results land in `governance.vacuum_log` (table_name, status, detail,
ran_at) — a failed VACUUM on one table doesn't fail the whole run (same
degrade-not-crash pattern as `cost_monitor.py` / `drift_monitor.py`); the
task only raises if *every* table's VACUUM failed that run.

## Restore runbook

**Scenario: a bad pipeline run corrupted or wrongly overwrote
`gold.gold_customer_golden` (or `_account_golden`), and you need last
Tuesday's data back.**

1. Find the version/timestamp to restore to:
   ```sql
   DESCRIBE HISTORY mdm_dq_demo.gold.gold_customer_golden;
   ```
   Look for the last `operationMetrics` timestamp before the bad run.

2. Inspect it before committing to anything — a read-only query, no risk:
   ```sql
   SELECT * FROM mdm_dq_demo.gold.gold_customer_golden
   TIMESTAMP AS OF '2026-07-28T00:00:00Z';
   -- or: VERSION AS OF 142
   ```

3. Restore in place (this is the operation — it rewrites the table's
   current state to match the chosen version, itself recorded as a new
   version, so the restore itself is undoable too):
   ```sql
   RESTORE TABLE mdm_dq_demo.gold.gold_customer_golden
   TO TIMESTAMP AS OF '2026-07-28T00:00:00Z';
   ```

4. If you'd rather not touch the live table until you're sure, clone
   instead of restoring — cheap (metadata-only copy on Delta), and lets you
   validate against the real table before deciding:
   ```sql
   CREATE TABLE mdm_dq_demo.gold.gold_customer_golden_recovery
   DEEP CLONE mdm_dq_demo.gold.gold_customer_golden
   TIMESTAMP AS OF '2026-07-28T00:00:00Z';
   ```

Same procedure applies to any table in the catalog — bronze/silver just
have a 7-day window instead of 30 by default, so act faster or widen their
retention property first if you need a longer look-back.

**Why 30 days and not longer:** every day of extra retention is extra
storage cost (deleted files stick around until VACUUM prunes them past the
window) for a project where nothing downstream has asked for longer. If a
real compliance retention requirement shows up (e.g. "must be able to
reconstruct Gold as of any date in the last 7 years"), that's a different,
much bigger design — periodic exports to cheap cold storage, not a wider
Delta retention window (which would make every table's storage footprint
grow roughly linearly with the window and defeat VACUUM's whole purpose).

## What's genuinely NOT covered

- **No cross-region / cross-workspace replication.** Everything above is
  Delta time travel *within this one metastore*. If the metastore's
  underlying storage (the S3 bucket backing this workspace's managed
  storage) or the metastore itself is lost, there is no second copy
  anywhere — this is a single point of failure. A real DR posture needs
  either cross-region bucket replication at the storage layer or a
  scheduled `DEEP CLONE` to a catalog in a second metastore/region; see
  `docs/multi-region-considerations.md` for why that's a real
  infrastructure decision (cost, region, ownership) this project
  deliberately didn't make unilaterally.
- **No automated restore testing.** The runbook above has been read through
  and the SQL is correct Delta syntax, but nothing in CI actually exercises
  a `RESTORE TABLE` against a real table on a schedule to prove the
  procedure still works as the schema evolves. A real production rollout
  should run this quarterly, not just trust the doc.
- **No backup of the non-Delta state**: Terraform state (catalog/schema/
  grant definitions — see `terraform/terraform.tfstate`, deliberately
  gitignored, see the repo's security notes on why) and the Databricks
  Asset Bundle deployment itself aren't covered by anything above. Losing
  the Terraform state doesn't lose data, but it does mean re-importing
  every resource by hand (`terraform import`) to regain management of the
  existing infrastructure — a real gap, not a hypothetical one, since it's
  exactly what `terraform/catalog.tf`'s comments already document having
  to do once.
- **No point-in-time recovery for Volumes** (raw landing/staging files).
  Delta time travel is a Delta-table feature; the raw files Autoloader
  reads from `landing`/`staging` Volumes have no equivalent versioning here.
  If a source file is deleted upstream before Autoloader ingests it,
  there's no recovery path through this project.
