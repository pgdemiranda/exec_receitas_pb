terraform {
  required_providers { google = {
    source = "hashicorp/google"
  version = "6.16.0" } }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_bigquery_dataset" "list_datasets" {
  count         = length(var.dataset_ids)
  dataset_id    = var.dataset_ids[count.index]
  location      = var.location
}

resource "google_bigquery_table" "tables" {
  count        = length(var.tables)
  dataset_id   = var.dataset_ids[0]
  table_id     = var.tables[count.index].table_id
  description  = var.tables[count.index].description
  depends_on = [google_bigquery_dataset.list_datasets] 
}

resource "google_storage_bucket" "prod_CSV_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}