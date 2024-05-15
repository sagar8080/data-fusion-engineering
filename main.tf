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
      name           = "df-ingest-traffic-data"
      data_type      = "traffic"
      script_object  = "${var.script_base_location}ingest-traffic_data.zip"
      memory         = "2048M"
    }
    taxi_data = {
      name           = "df-ingest-taxi-data"
      data_type      = "traffic"
      script_object  = "${var.script_base_location}ingest-taxi_data.zip"
      memory         = "2048M"
    }
    collision_data = {
      name           = "df-ingest-crashes-data"
      data_type      = "crashes"
      script_object  = "${var.script_base_location}ingest-crashes_data.zip"
      memory         = "512M"
    }
    crashes_data = {
      name           = "df-ingest-vehicles-data"
      data_type      = "vehicles"
      script_object  = "${var.script_base_location}ingest-vehicles_data.zip"
      memory         = "512M"
    }
    persons_data = {
      name           = "df-ingest-persons-data"
      data_type      = "persons"
      script_object  = "${var.script_base_location}ingest-persons_data.zip"
      memory         = "512M"
    }
    weather_data = {
      name           = "df-ingest-weather-data"
      data_type      = "weather"
      script_object  = "${var.script_base_location}ingest-weather_data.zip"
      memory         = "512M"
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
        bucket  = var.code_bucket
        object  = each.value.script_object
      }
    }
  }

  service_config {
    max_instance_count  = 1
    min_instance_count  = 1
    available_memory    = each.value.memory
    timeout_seconds     = 1000
    ingress_settings    = "ALLOW_ALL"
    all_traffic_on_latest_revision = true
    service_account_email = var.service_account_email
  }
}


# Create Cloud Scheduler jobs for each Cloud Function
resource "google_cloud_scheduler_job" "invoke_cloud_function" {
  for_each = google_cloudfunctions2_function.function

  name           = each.key
  description    = format("Schedule the HTTPS trigger for cloud function '%s'", each.value.name)
  schedule       = var.cloud_function_schedule
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


resource "google_dataproc_cluster" "data_fusion" {
  name     = "data-fusion"
  region   = "us-east4"
  graceful_decommission_timeout = "120s"

  cluster_config {
    staging_bucket = "dataproc-staging-bucket"

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances    = 2
      machine_type     = "e2-standard-4"
      disk_config {
        boot_disk_type = "pd-balanced"
        boot_disk_size_gb = 50
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.0.35-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = var.service_account_email
      service_account_scopes = ["cloud-platform"]
    }
  }
}
