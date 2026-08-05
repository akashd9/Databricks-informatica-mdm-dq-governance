# Job failure alerting beyond email. resources/jobs.yml's webhook_notifications
# reference a notification destination by ID — Terraform and the bundle are
# separate tools/state here (see databricks.yml's alert_webhook_destinations
# variable comment), so after this creates the destination, copy the
# `alert_webhook_destination_id` output into that variable to actually wire
# it into the jobs.
#
# generic_webhook works with any endpoint that accepts a JSON POST (Slack
# incoming webhooks included) — count-gated on var.alert_webhook_url so
# `terraform apply` succeeds with none configured (this is a demo catalog,
# no real on-call webhook exists to point at by default).
resource "databricks_notification_destination" "alerts" {
  count        = var.alert_webhook_url != "" ? 1 : 0
  display_name = "mdm-dq-medallion-alerts"
  config {
    generic_webhook {
      url = var.alert_webhook_url
    }
  }
}
