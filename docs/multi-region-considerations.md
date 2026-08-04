# Multi-Region Governance — Design Considerations

Not implemented as live infrastructure, deliberately. Standing up a second
Unity Catalog metastore in another region is a real cloud-cost and
architecture decision — creating one silently, without a stakeholder
actually choosing the region, the data-residency driver, and who owns the
ongoing cost, would be the wrong kind of "helpful." This document is the
Phase 4 deliverable instead: what multi-region governance would actually
require for this project, so the decision is informed if/when it's made.

## Why you'd need this at all

Two distinct drivers, and they lead to different designs:

1. **Data residency / compliance** — GDPR, or a regulator requiring EU
   customer data to never leave the EU, for example. This means a *second,
   independent* metastore in-region, with its own storage, not a replica.
2. **Latency / availability** — serving Gold data to consumers physically
   far from `us-east-2` (this project's current metastore region, see
   `terraform/catalog.tf` and the discovery in this project's setup: the
   `lakehouse-demo` workspace's metastore is `metastore_aws_us_east_2`).
   This is closer to a replication/caching problem than a residency one.

This project's current design (`config/sources.yml`,
`config/account_sources.yml`) has no region awareness at all — landing
paths, secret scopes, and the catalog are all single-region by construction.

## What would actually need to change

### 1. A second metastore, not a second catalog
Unity Catalog metastores are region-scoped (one per region per Databricks
account, generally). Catalogs live *inside* a metastore. So multi-region
isn't "add another catalog" (that's what `config/account_*.yml` did for a
new *entity*, within the *same* region) — it's a second metastore, in a
second Databricks workspace, in a second region, each with:
- its own storage root / managed storage (or external location + storage
  credential, revisiting the AWS-IAM-role path this project deliberately
  avoided for the single-region case — see `terraform/volumes.tf`'s comment
  on why managed storage was chosen here),
- its own `informatica` secret scope (Informatica IDMC/MDM connectivity may
  also need a region-local pod, not just a region-local Databricks secret),
- its own copy of every schema/table this project creates.

### 2. Terraform: workspace-scoped, needs a provider alias per region
`terraform/providers.tf` currently configures one `databricks` provider
against one profile/workspace. A second region means a second workspace,
which means either:
- a second Terraform root module (simplest — copy `terraform/`, point it at
  the second workspace's profile, accept some duplication), or
- a single root module with two aliased `databricks` providers
  (`provider "databricks" { alias = "eu" ... }`) and every resource
  duplicated per alias — more DRY, meaningfully more complex, easy to get
  wrong on a `terraform destroy` across regions.

  For two regions, the first option is probably right. It only gets
  "obviously wrong" past 3-4 regions, where a proper module
  (`modules/mdm-catalog/` parameterized by region) pays for itself.

### 3. The golden record problem: is a customer global or regional?
This is the actual hard question, and the codebase doesn't answer it today
because it's never had more than one region to consider:

- **Regional golden records** (a customer matched/merged independently per
  region): simplest to build — literally "deploy this whole project again
  in region 2" — but a customer active in both regions gets two unrelated
  `golden_id`s, no cross-region view exists, and compliance reporting that
  spans regions can't reconcile them.
- **Global golden records, regional storage of underlying PII**: match/merge
  needs to run somewhere that can see records from both regions to decide
  they're the same person, but the regulatory driver (residency) is
  specifically about *not* moving that PII across the boundary. This
  usually gets solved with **Delta Sharing** (share non-PII match keys —
  hashed identifiers, not raw PII — across regions for matching, keep raw
  attributes region-local) or a **federated match** approach (each region
  computes local clusters; a lightweight cross-region reconciliation step
  matches on non-PII keys only). Both are materially more engineering than
  anything currently in `src/mdm/match_merge.py`.

**Recommendation if this becomes real**: start with regional golden records
(cheap, matches most compliance requirements which care about *storage*
location, not about having one global ID) and only build the cross-region
reconciliation layer if a concrete consumer need shows up — it's the kind
of complexity that's much easier to add later than to unwind.

### 4. Governance: one glossary, or one per region?
`config/glossary_terms.yml` / `config/account_glossary_terms.yml` and
`governance.business_glossary` should almost certainly stay **one global
glossary** even in a multi-region design — "Customer Golden Record ID" means
the same thing everywhere; there's no compliance reason to fork
definitions. This argues for the glossary (and glossary submissions/steward
decisions — `src/governance/steward_review.py`,
`src/governance/glossary_submissions.py`) living in a designated "home"
region and being read cross-region (via Delta Sharing or a periodic
export/sync), while `business_glossary` itself contains no customer PII —
only column/term metadata — so sharing it doesn't reopen the residency
question at all.

### 5. Observability: aggregate, but from where?
`src/observability/cost_monitor.py`, `drift_monitor.py`, and the SLA
dashboard queries (`dashboards/sla_dashboard_queries.sql`) all currently
assume one metastore. A multi-region rollout wants a **cross-region rollup**
(total spend, aggregate DQ pass rate) without needing PII to leave any
region — this is exactly the kind of thing System Tables' cross-workspace
visibility (when the workspaces share an account) or a small
metrics-only export per region (no raw records, just the aggregated numbers
these scripts already compute) can solve without touching the residency
boundary at all.

## What's genuinely NOT designed for yet

- No region field anywhere in the schema (`config/column_maps.yml`, Gold
  table columns) — `country_code` exists but isn't the same thing as "which
  region's metastore does this record live in."
- No secret-scope-per-region convention — `dbutils.secrets.get("informatica", ...)`
  is hardcoded to one scope name throughout `src/quality/dq_rules.py` and
  `src/mdm/match_merge.py`.
- No decision on regional vs. global golden records (see §3) — this is the
  one to resolve first; everything else follows from it.

## Suggested first step, if/when this is prioritized

Don't build the second metastore first. Answer §3 (regional vs. global
golden records) with whoever owns the compliance requirement, since that
decision changes whether "multi-region" is a Terraform exercise (regional
golden records — genuinely close to "deploy this project twice") or a
data-architecture project (global golden records — Delta Sharing / federated
matching, a multi-week design in itself).
