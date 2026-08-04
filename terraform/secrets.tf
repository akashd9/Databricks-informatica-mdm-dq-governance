# Secret scope + key names must match what src/quality/dq_rules.py and
# src/mdm/match_merge.py read via dbutils.secrets.get("informatica", <key>).
# The values below are placeholders (see variables.tf) — populate real ones
# via TF_VAR_* env vars or a secrets-manager-backed data source, never a
# committed .tfvars file.
resource "databricks_secret_scope" "informatica" {
  name = "informatica"
}

resource "databricks_secret" "idmc_base_url" {
  key          = "idmc_base_url"
  string_value = var.informatica_idmc_base_url
  scope        = databricks_secret_scope.informatica.id
}

resource "databricks_secret" "mdm_base_url" {
  key          = "mdm_base_url"
  string_value = var.informatica_mdm_base_url
  scope        = databricks_secret_scope.informatica.id
}

resource "databricks_secret" "session_token" {
  key          = "session_token"
  string_value = var.informatica_session_token
  scope        = databricks_secret_scope.informatica.id
}
