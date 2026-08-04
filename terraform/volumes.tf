# Managed Volumes — no storage credential or external location needed;
# Unity Catalog stores these under the metastore's own managed storage,
# matching how every other catalog in this workspace is already set up.
# Names here must stay in sync with config/sources.yml's landing_path values
# and the /Volumes/mdm_dq_demo/staging/informatica/... paths used in
# src/quality/dq_rules.py and src/mdm/match_merge.py.
locals {
  landing_volumes = ["erp", "crm", "flatfiles", "api"]
}

resource "databricks_volume" "landing" {
  for_each     = toset(local.landing_volumes)
  name         = each.value
  catalog_name = databricks_catalog.this.name
  schema_name  = databricks_schema.schemas["landing"].name
  volume_type  = "MANAGED"
  comment      = "Bronze landing zone for the ${each.value} source system."
}

resource "databricks_volume" "staging" {
  name         = "informatica"
  catalog_name = databricks_catalog.this.name
  schema_name  = databricks_schema.schemas["staging"].name
  volume_type  = "MANAGED"
  comment      = "Staging area for batches handed off to the Informatica DQ/MDM REST APIs."
}
