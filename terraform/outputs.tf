output "catalog_name" {
  value = databricks_catalog.this.name
}

output "landing_volume_paths" {
  description = "Feed these straight into config/sources.yml's landing_path fields."
  value = {
    for name, vol in databricks_volume.landing :
    name => "/Volumes/${databricks_catalog.this.name}/landing/${vol.name}"
  }
}

output "informatica_staging_volume_path" {
  value = "/Volumes/${databricks_catalog.this.name}/staging/${databricks_volume.staging.name}"
}

output "informatica_secret_scope" {
  value = databricks_secret_scope.informatica.name
}

output "alert_webhook_destination_id" {
  description = "Copy into databricks.yml's alert_webhook_destinations variable (or pass via -var on `databricks bundle deploy`) to wire job failure alerts to this webhook. Null if alert_webhook_url wasn't set."
  value       = length(databricks_notification_destination.alerts) > 0 ? databricks_notification_destination.alerts[0].id : null
}
