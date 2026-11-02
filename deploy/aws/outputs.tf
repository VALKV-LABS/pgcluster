output "admin_url" {
  description = "URL of the admin API."
  value       = "https://${aws_eip.admin.public_ip}:8443"
}

output "api_key" {
  description = "Key for the admin API."
  value       = random_password.api_key.result
  sensitive   = true
}

output "ca_cert" {
  description = "CA that signed the admin API's certificate. Save it for PGCLUSTER_ADMIN_CA."
  value       = tls_self_signed_cert.ca.cert_pem
}

output "backup_bucket" {
  description = "S3 bucket for backups."
  value       = aws_s3_bucket.backups.bucket
}
