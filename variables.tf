variable "project_id" {
  description = "Project id of the GCP"
  type = string
}

variable "credentials_file_path" {
  description = "Location of the credentials to use."
}

variable "service_account_email" {
  type = string
  description = "default desc"
}

variable "region" {
  default     = "us-east4"
  description = "Default region for the GCP project"
  type        = string
}

variable "code_bucket" {
  description = "Default bucket to store the scripts for the project"
  type        = string
}

variable "landing_bucket" {
  description = "Default bucket to store the scripts for the project"
  type        = string
}

variable "dataproc_bucket" {
  description = "Default bucket to store the scripts for the project"
  type        = string
}

variable "script_base_location" {
  type = string
  default = "scripts/"
}

variable "cloud_function_schedule" {
  default = "0 9-17 * * 1-5"
  description = "Default bucket to store the scripts for the project"
  type = string
}