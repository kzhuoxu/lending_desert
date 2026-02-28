variable "credentials" {
  description = "My Credentials"
  default     = "default-value"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "default-value"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "default-value"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "default-value"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "default-value"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "default-value"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}