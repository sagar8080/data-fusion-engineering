resource "random_string" "bucket_suffix" {
  length  = 6
  special = false
  upper   = false
  lower   = true
}

variable "project_id" {
  default = "enter-your-project-id"
  description = "Project id of the GCP"
  type = string
}


variable "credentials_file_path" {
  description = "Location of the credentials to use."
  default     = "set-your-file-path"
}

variable "service_account_email" {
  default = "set-your-email"
}

variable "region" {
  default = "us-east4"
  description = "Default region for the GCP project"
  type = string
}

variable "script_bucket" {
  default = "df-code-bucket-${random_string.bucket_suffix.result}"
  description = "Default bucket to store the scripts for the project"
  type = string
}

variable "landing_bucket" {
  default = "df-landing-zone-${random_string.bucket_suffix.result}"
  description = "Default bucket to store the scripts for the project"
  type = string
}

variable "dataproc_bucket" {
  default = "df-dataproc-utils-${random_string.bucket_suffix.result}"
  description = "Default bucket to store the scripts for the project"
  type = string
}

variable "script_base_location" {
  type = string
  default = "scripts/"
}