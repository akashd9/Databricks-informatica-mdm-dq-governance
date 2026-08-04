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
