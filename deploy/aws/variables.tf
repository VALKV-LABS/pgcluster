variable "region" {
  type        = string
  description = "AWS region for the test fleet."
}

variable "allowed_cidr" {
  type        = string
  description = "The only range that can reach the admin API and databases. It has no default on purpose."

  validation {
    condition     = can(cidrhost(var.allowed_cidr, 0)) && var.allowed_cidr != "0.0.0.0/0"
    error_message = "The allowed_cidr must be a CIDR block narrower than the whole internet."
  }
}

variable "name" {
  type        = string
  default     = "pgcluster"
  description = "Prefix for resource names."
}

variable "host_count" {
  type        = number
  default     = 3
  description = "Hosts to launch."
}

variable "instance_type" {
  type        = string
  default     = "t4g.small"
  description = "Size of every VM. An arm64 type needs an arm64 image."
}

variable "volume_gb" {
  type        = number
  default     = 20
  description = "Root disk of every VM."
}

variable "ami_id" {
  type        = string
  default     = ""
  description = "Node image to boot. Left empty, the newest image from ami_owner is used."
}

variable "ami_owner" {
  type        = string
  default     = "self"
  description = "Account to find the newest node image in. `self` finds one you built with images/."
}
