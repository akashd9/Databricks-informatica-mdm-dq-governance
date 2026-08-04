# This workspace's account has "Default Storage" enabled, which requires new
# catalogs be created via the UI (Catalog Explorer > Create Catalog, storage
# left as Default) — the API rejects catalog creation with an explicit
# storage_root. Once created there, `terraform import databricks_catalog.this
# <name>` adopts it so schemas/volumes/etc. below are still Terraform-managed.
#
# storage_root is ForceNew: it must be declared here matching the value the
# UI assigned (this metastore's shared managed-storage root — same value on
# every catalog in this metastore), or Terraform reads "unset in config" as
# "remove it" and plans a destroy+recreate. prevent_destroy exists as a
# backstop in case that ever happens again.
resource "databricks_catalog" "this" {
  name         = var.catalog_name
  comment      = "MDM/DQ medallion demo catalog: Bronze/Silver/Gold + governance + landing/staging Volumes."
  storage_root = "s3://dbstorage-prod-h7lk8/uc/66a4289c-028d-4a35-a93d-002bc1716981/9325dd5f-10c9-42e2-a7c9-f3374033eefb"

  lifecycle {
    prevent_destroy = true
  }
}

locals {
  schemas = ["bronze", "silver", "gold", "governance", "config", "landing", "staging"]
}

resource "databricks_schema" "schemas" {
  for_each     = toset(local.schemas)
  catalog_name = databricks_catalog.this.name
  name         = each.value
  comment      = "mdm-dq-medallion ${each.value} schema."
}
