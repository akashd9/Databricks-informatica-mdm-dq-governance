variable "databricks_profile" {
  description = "Named profile in ~/.databrickscfg to authenticate as (see `databricks auth login`). Matches this workspace's existing lakehouse-demo profile by default."
  type        = string
  default     = "lakehouse-demo"
}

variable "catalog_name" {
  description = "Unity Catalog catalog name for this project."
  type        = string
  default     = "mdm_dq_demo"
}

variable "informatica_idmc_base_url" {
  description = <<-EOT
    Informatica IDMC Cloud Data Quality API base URL. The default below is a
    *candidate* only — usw1.dm2-us.informaticacloud.com is this tenant's UI
    pod, not confirmed as the API base (IDMC's actual API host is normally
    returned by its login call and can differ per product). Verify before
    relying on it, and note src/quality/dq_rules.py's InformaticaCloudDQClient
    still assumes a session token already exists as a secret — it doesn't
    perform the login call itself yet.
  EOT
  type        = string
  default     = "https://usw1.dm2-us.informaticacloud.com"
}

variable "informatica_mdm_base_url" {
  description = "Informatica MDM SaaS base URL. Replace before applying."
  type        = string
  default     = "https://REPLACE_ME-mdm.informaticacloud.com"
}

variable "data_engineer_emails" {
  description = "Workspace users (must already exist) added to mdm_dq_data_engineers: read/write on bronze/silver/gold, read on config/governance, the only group with landing/staging Volume access. Empty by default."
  type        = list(string)
  default     = []
}

variable "data_steward_emails" {
  description = "Workspace users added to mdm_dq_data_stewards: read on silver/gold, read/write on governance (steward_decisions). Empty by default."
  type        = list(string)
  default     = []
}

variable "data_analyst_emails" {
  description = "Workspace users added to mdm_dq_data_analysts: read-only on gold. Empty by default."
  type        = list(string)
  default     = []
}

variable "pii_unmasked_emails" {
  description = "Workspace users added to mdm_dq_pii_unmasked: see real email/tax_id values on Gold instead of the masked default. Should be a small, audited subset of data_steward_emails. Empty by default."
  type        = list(string)
  default     = []
}

variable "informatica_session_token" {
  description = <<-EOT
    Informatica IDMC session token / API key. Never put a real value in a
    .tfvars file that gets committed — pass it via TF_VAR_informatica_session_token,
    -var on the CLI, or wire this variable to a secrets-manager data source
    (aws_secretsmanager_secret_version, azurerm_key_vault_secret, etc.).
  EOT
  type        = string
  sensitive   = true
  default     = "REPLACE_ME"
}
