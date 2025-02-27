variable "credentials" {
  description = "Credentials"
  default     = "./../creds/sound-bee-448014-m3-27183706c17d.json"
}

variable "project" {
  description = "Project"
  default     = "sound-bee-448014-m3"
}

variable "region" {
  description = "Project Region"
  default     = "SOUTHAMERICA-EAST1"
}

variable "location" {
  description = "Project Location"
  default     = "SOUTHAMERICA-EAST1"
}

variable "dataset_ids" {
  description = "List of BigQuery dataset IDs to create"
  type        = list(string)
  default     = ["prod", "data_warehouse"]
}

variable "tables" {
  description = "List of tables to create in a specific dataset"
  type = list(object({
    table_id    = string
    description = string
  }))
  default = [
    {
      table_id    = "exec_orcamentaria"
      description = "raw data from execução orçamentária data"
    },
    {
      table_id    = "unidade_gestora"
      description = "raw data from unidade gestora data"
    },
    {
        table_id = "unidade_orcamentaria"
        description = "raw data from unidade orçamentária data"
    }
  ]
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "prod_sound-bee-448014-m3-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}