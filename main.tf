# configure provider
provider "google" {
  credentials = file(var.credentials_file_path)
  project = var.project_id
  region  = var.region
}


# Create Cloud Functions
resource "google_cloudfunctions2_function" "function" {
  for_each = {
    traffic_data = {
      name = "df-ingest-traffic-data"
      data_type = "traffic"
      script_object = "${var.script_base_location}ingest-traffic_data.zip"
    }
    collision_data = {
      name = "df-ingest-crashes-data"
      data_type = "crashes"
      script_object = "${var.script_base_location}ingest-crashes_data.zip"
    }
    crashes_data = {
      name = "df-ingest-vehicles-data"
      data_type = "vehicles"
      script_object = "${var.script_base_location}ingest-vehicles_data.zip"
    }
    persons_data = {
      name = "df-ingest-persons-data"
      data_type = "persons"
      script_object = "${var.script_base_location}ingest-persons_data.zip"
    }
    weather_data = {
      name = "df-ingest-weather-data"
      data_type = "weather"
      script_object = "${var.script_base_location}ingest-weather_data.zip"
    }
  }

  name           = each.value.name
  location       = var.region
  description    = "Cloud Function to ingest ${each.value.data_type} data"

  build_config {
    runtime       = "python38"
    entry_point   = "execute"
    source {
      storage_source {
        bucket  = "df-code-bucket"
        object  = each.value.script_object
      }
    }
  }

  service_config {
    max_instance_count  = 1
    min_instance_count  = 1
    available_memory    = "2048M"
    timeout_seconds     = 1000
    ingress_settings   = "ALLOW_ALL"
    all_traffic_on_latest_revision = true
    service_account_email = var.service_account_email
  }
}

# Create Cloud Scheduler jobs for each Cloud Function
resource "google_cloud_scheduler_job" "invoke_cloud_function" {
  for_each = google_cloudfunctions2_function.function

  name           = each.key
  description    = format("Schedule the HTTPS trigger for cloud function '%s'", each.value.name)
  schedule       = "*/5 21-21 * * *"
  project        = google_cloudfunctions2_function.function[each.key].project
  region         = google_cloudfunctions2_function.function[each.key].location
  time_zone      = "America/New_York"

  http_target {
    uri               = google_cloudfunctions2_function.function[each.key].service_config[0].uri
    http_method       = "POST"
    oidc_token {
      audience         = "${google_cloudfunctions2_function.function[each.key].service_config[0].uri}/"
      service_account_email = var.service_account_email
    }
  }
}
