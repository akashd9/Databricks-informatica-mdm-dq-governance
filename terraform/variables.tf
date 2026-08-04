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
  description = "Informatica IDMC Cloud Data Quality pod base URL, e.g. https://dmapisXX.informaticacloud.com. Replace before applying."
  type        = string
  default     = "https://REPLACE_ME.informaticacloud.com"
}

variable "informatica_mdm_base_url" {
  description = "Informatica MDM SaaS base URL. Replace before applying."
  type        = string
  default     = "https://REPLACE_ME-mdm.informaticacloud.com"
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
