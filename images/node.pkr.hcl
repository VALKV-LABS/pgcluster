# The node image: every Postgres major, the extensions, WAL-G and all binaries,
# pinned. Hosts and the pgcluster-admin VM both boot from it.

packer {
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = "~> 1.3"
    }
  }
}

variable "version" {
  type        = string
  description = "pgcluster release this image carries."
}

variable "arch" {
  type    = string
  default = "arm64"

  validation {
    condition     = contains(["arm64", "amd64"], var.arch)
    error_message = "The arch must be arm64 or amd64."
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "public" {
  type        = bool
  default     = false
  description = "Make the AMI public, as a release does."
}

variable "copy_regions" {
  type    = list(string)
  default = []
}

locals {
  instance_types = {
    arm64 = "t4g.small"
    amd64 = "t3.small"
  }
}

source "amazon-ebs" "node" {
  region        = var.region
  ami_name      = "pgcluster-node-${var.version}-${var.arch}"
  ami_regions   = var.copy_regions
  ami_groups    = var.public ? ["all"] : []
  instance_type = local.instance_types[var.arch]
  ssh_username  = "admin"

  # Debian's own account.
  source_ami_filter {
    owners      = ["136693071363"]
    most_recent = true
    filters = {
      name                = "debian-12-${var.arch}-*"
      virtualization-type = "hvm"
      root-device-type    = "ebs"
    }
  }

  tags = {
    Name    = "pgcluster-node"
    Version = var.version
  }
}

build {
  sources = ["source.amazon-ebs.node"]

  provisioner "file" {
    source      = "bin"
    destination = "/tmp/pgcluster-bin"
  }

  provisioner "file" {
    source      = "units"
    destination = "/tmp/pgcluster-units"
  }

  provisioner "shell" {
    script          = "provision.sh"
    execute_command = "sudo -E sh '{{ .Path }}'"
  }
}
