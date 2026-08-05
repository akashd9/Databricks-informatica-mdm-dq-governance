# Least-privilege access model for the mdm-dq-medallion catalog.
#
# Four roles, mapped to the medallion layers they're allowed to touch:
#   data_engineers  — owns the pipeline: read/write on bronze/silver/gold,
#                      read on config/governance. The only role with access
#                      to raw landing/staging Volumes.
#   data_stewards   — works the review queue: read on silver (to see
#                      quarantine/match-confidence detail behind a queue
#                      item) and gold, read/write on governance (writes
#                      steward_decisions). No bronze access — stewards
#                      review conformed records, not raw source files.
#   data_analysts   — consumption only: read on gold, nothing else. Gold is
#                      the DQ-passed, matched, PII-masked layer this whole
#                      pipeline exists to produce — the only layer meant for
#                      general consumption.
#   pii_unmasked    — crosses the mask_pii_string function (see
#                      unity_catalog_setup.py) to see real email/tax_id
#                      values on Gold. Expected to be a small, audited
#                      subset of data_stewards, not its own broad grant.
#
# Grants go to individual users (the *_emails variables below), not groups.
# Unity Catalog privileges only resolve ACCOUNT-level principals — verified
# against this workspace with a real GRANT: a user email resolved
# immediately, `GRANT ... TO mdm_dq_data_engineers` (a workspace-local
# databricks_group, which is what this provider profile can create without
# account-admin API access) failed with PRINCIPAL_DOES_NOT_EXIST. Account
# groups would be the right long-term answer — create and manage them via
# the databricks provider's account-level config (a different host/auth
# than this workspace profile) once account-admin credentials are
# available — but until then, per-user grants are what actually works here.
#
# Empty by default so `terraform apply` succeeds with zero real users
# provisioned (this is a demo catalog, not a real team) — set the
# corresponding *_emails variable per environment to actually grant access.
# databricks_grants requires at least one `grant` block, so every resource
# below is count-gated on its principal set being non-empty rather than
# emitting a resource with zero grants.
locals {
  catalog_principals  = toset(concat(var.data_engineer_emails, var.data_steward_emails, var.data_analyst_emails))
  engineer_principals = toset(var.data_engineer_emails)
  steward_only        = toset(setsubtract(var.data_steward_emails, var.data_engineer_emails))
  analyst_only        = toset(setsubtract(var.data_analyst_emails, concat(var.data_engineer_emails, var.data_steward_emails)))
}

resource "databricks_grants" "catalog" {
  count   = length(local.catalog_principals) > 0 ? 1 : 0
  catalog = databricks_catalog.this.name

  dynamic "grant" {
    for_each = local.catalog_principals
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG"]
    }
  }
}

resource "databricks_grants" "bronze" {
  count  = length(local.engineer_principals) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["bronze"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
    }
  }
}

resource "databricks_grants" "silver" {
  count  = length(local.engineer_principals) + length(local.steward_only) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["silver"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
    }
  }
  dynamic "grant" {
    for_each = local.steward_only
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

resource "databricks_grants" "gold" {
  count  = length(local.engineer_principals) + length(local.steward_only) + length(local.analyst_only) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["gold"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
    }
  }
  dynamic "grant" {
    for_each = local.steward_only
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
  dynamic "grant" {
    for_each = local.analyst_only
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

resource "databricks_grants" "governance" {
  count  = length(local.engineer_principals) + length(local.steward_only) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["governance"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
  dynamic "grant" {
    for_each = local.steward_only
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
    }
  }
}

resource "databricks_grants" "config" {
  count  = length(local.engineer_principals) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["config"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

# Raw landing/staging Volumes: engineers only. These hold un-conformed
# source files (landing) and Informatica hand-off batches (staging) —
# neither has been through the DQ gate, so no other role touches them.
resource "databricks_grants" "landing_schema" {
  count  = length(local.engineer_principals) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["landing"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "READ_VOLUME", "WRITE_VOLUME"]
    }
  }
}

resource "databricks_grants" "staging_schema" {
  count  = length(local.engineer_principals) > 0 ? 1 : 0
  schema = "${databricks_catalog.this.name}.${databricks_schema.schemas["staging"].name}"

  dynamic "grant" {
    for_each = local.engineer_principals
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "READ_VOLUME", "WRITE_VOLUME"]
    }
  }
}

# Workspace-local groups — organizational scaffolding for workspace-level
# RBAC (job/pipeline run-as permissions, cluster policies) which DOES accept
# workspace-local groups, unlike Unity Catalog privileges above. Not used as
# UC grant principals; see the note at the top of this file.
resource "databricks_group" "data_engineers" {
  display_name = "mdm_dq_data_engineers"
}

resource "databricks_group" "data_stewards" {
  display_name = "mdm_dq_data_stewards"
}

resource "databricks_group" "data_analysts" {
  display_name = "mdm_dq_data_analysts"
}

resource "databricks_group" "pii_unmasked" {
  display_name = "mdm_dq_pii_unmasked"
}

data "databricks_user" "engineers" {
  for_each  = toset(var.data_engineer_emails)
  user_name = each.value
}
resource "databricks_group_member" "engineers" {
  for_each  = data.databricks_user.engineers
  group_id  = databricks_group.data_engineers.id
  member_id = each.value.id
}

data "databricks_user" "stewards" {
  for_each  = toset(var.data_steward_emails)
  user_name = each.value
}
resource "databricks_group_member" "stewards" {
  for_each  = data.databricks_user.stewards
  group_id  = databricks_group.data_stewards.id
  member_id = each.value.id
}

data "databricks_user" "analysts" {
  for_each  = toset(var.data_analyst_emails)
  user_name = each.value
}
resource "databricks_group_member" "analysts" {
  for_each  = data.databricks_user.analysts
  group_id  = databricks_group.data_analysts.id
  member_id = each.value.id
}

data "databricks_user" "pii_unmasked" {
  for_each  = toset(var.pii_unmasked_emails)
  user_name = each.value
}
resource "databricks_group_member" "pii_unmasked" {
  for_each  = data.databricks_user.pii_unmasked
  group_id  = databricks_group.pii_unmasked.id
  member_id = each.value.id
}
