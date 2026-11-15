variable "project" {
  description = "Name used as a prefix for every resource"
  type        = string
  default     = "flood-claims-lakehouse"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "athena_scan_limit_bytes" {
  description = "Largest scan a single Athena query may run (default 1 GB, about half a cent)"
  type        = number
  default     = 1073741824
}
