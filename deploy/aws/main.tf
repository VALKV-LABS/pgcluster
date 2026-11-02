data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "node" {
  count       = var.ami_id == "" ? 1 : 0
  most_recent = true
  owners      = [var.ami_owner]

  filter {
    name   = "name"
    values = ["pgcluster-node-*-${local.arch}"]
  }
}

data "aws_ec2_instance_type" "selected" {
  instance_type = var.instance_type
}

locals {
  arch      = contains(data.aws_ec2_instance_type.selected.supported_architectures, "arm64") ? "arm64" : "amd64"
  ami       = var.ami_id != "" ? var.ami_id : data.aws_ami.node[0].id
  vpc_cidr  = "10.60.0.0/16"
  admin_ip  = cidrhost(aws_subnet.fleet.cidr_block, 10)
  agent_url = "https://${local.admin_ip}:8444"
  # Offset 0 of each block is a proxy, and blocks start at 20000.
  proxy_port_low  = 20000
  proxy_port_high = 32760

  # Enrolment needs no admin key: each host has a token, and the admin VM only
  # ever sees its hash. The suffix lets a host verify the CA it is given.
  host_names = [for i in range(var.host_count) : "${var.name}-host-${i + 1}"]
  ca_hash    = sha256(tls_self_signed_cert.ca.cert_pem)
}

# ── Network ───────────────────────────────────────────────────────────────────

resource "aws_vpc" "fleet" {
  cidr_block           = local.vpc_cidr
  enable_dns_hostnames = true

  tags = { Name = var.name }
}

resource "aws_internet_gateway" "fleet" {
  vpc_id = aws_vpc.fleet.id

  tags = { Name = var.name }
}

resource "aws_subnet" "fleet" {
  vpc_id                  = aws_vpc.fleet.id
  cidr_block              = cidrsubnet(local.vpc_cidr, 8, 1)
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = { Name = var.name }
}

resource "aws_route_table" "fleet" {
  vpc_id = aws_vpc.fleet.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.fleet.id
  }

  tags = { Name = var.name }
}

resource "aws_route_table_association" "fleet" {
  subnet_id      = aws_subnet.fleet.id
  route_table_id = aws_route_table.fleet.id
}

resource "aws_security_group" "admin" {
  name_prefix = "${var.name}-admin-"
  description = "pgcluster-admin"
  vpc_id      = aws_vpc.fleet.id

  ingress {
    description = "Admin API"
    from_port   = 8443
    to_port     = 8443
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  ingress {
    description = "Agent API, from the fleet"
    from_port   = 8444
    to_port     = 8444
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "host" {
  name_prefix = "${var.name}-host-"
  description = "Hosts"
  vpc_id      = aws_vpc.fleet.id

  ingress {
    description = "Database proxies"
    from_port   = local.proxy_port_low
    to_port     = local.proxy_port_high
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  ingress {
    description = "Peers"
    from_port   = local.proxy_port_low
    to_port     = local.proxy_port_high
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

# ── Backups ───────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "backups" {
  bucket_prefix = "${var.name}-backups-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "backups" {
  bucket                  = aws_s3_bucket.backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "backups" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.backups.arn]
  }

  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.backups.arn}/*"]
  }
}

resource "aws_iam_role" "host" {
  name_prefix        = "${var.name}-host-"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "backups" {
  name_prefix = "backups-"
  role        = aws_iam_role.host.id
  policy      = data.aws_iam_policy_document.backups.json
}

resource "aws_iam_instance_profile" "host" {
  name_prefix = "${var.name}-host-"
  role        = aws_iam_role.host.name
}

# ── Generated secrets ─────────────────────────────────────────────────────────

resource "tls_private_key" "ca" {
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "tls_self_signed_cert" "ca" {
  private_key_pem       = tls_private_key.ca.private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 24 * 365 * 10
  allowed_uses          = ["cert_signing", "crl_signing"]

  subject {
    common_name = "pgcluster-admin-ca"
  }
}

resource "random_password" "api_key" {
  length  = 48
  special = false
}

resource "random_password" "metadata_db" {
  length  = 32
  special = false
}

resource "random_id" "secret_key" {
  byte_length = 32
}

resource "random_password" "enrollment" {
  count   = var.host_count
  length  = 48
  special = false
}

# ── VMs ───────────────────────────────────────────────────────────────────────

resource "aws_eip" "admin" {
  domain = "vpc"

  tags = { Name = "${var.name}-admin" }
}

resource "aws_eip_association" "admin" {
  allocation_id = aws_eip.admin.id
  instance_id   = aws_instance.admin.id
}

resource "aws_instance" "admin" {
  ami                    = local.ami
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.fleet.id
  private_ip             = local.admin_ip
  vpc_security_group_ids = [aws_security_group.admin.id]

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/templates/admin.sh.tftpl", {
    api_key        = random_password.api_key.result
    secret_key     = random_id.secret_key.b64_std
    metadata_db    = random_password.metadata_db.result
    ca_cert        = tls_self_signed_cert.ca.cert_pem
    ca_key         = tls_private_key.ca.private_key_pem_pkcs8
    server_names   = [local.admin_ip, aws_eip.admin.public_ip]
    host_names     = local.host_names
    token_hashes   = [for token in random_password.enrollment : sha256(token.result)]
    postgres_major = 18
  })

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = var.volume_gb
    encrypted   = true
  }

  tags = { Name = "${var.name}-admin" }
}

resource "aws_instance" "host" {
  count                  = var.host_count
  ami                    = local.ami
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.fleet.id
  vpc_security_group_ids = [aws_security_group.host.id]
  iam_instance_profile   = aws_iam_instance_profile.host.name

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/templates/host.sh.tftpl", {
    admin_url = local.agent_url
    token     = "${random_password.enrollment[count.index].result}.${local.ca_hash}"
  })

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = var.volume_gb
    encrypted   = true
  }

  tags = { Name = local.host_names[count.index] }

  depends_on = [aws_instance.admin]
}
