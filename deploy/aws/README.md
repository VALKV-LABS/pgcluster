# AWS template

Launches a test fleet in your own account: a VPC, one `pgcluster-admin` VM with Postgres on the same VM, three hosts from the node image, and an S3 bucket for backups.

It is for testing, not production. `pgcluster-admin` is a single VM, and the default size is `t4g.small`. Four on-demand `t4g.small` VMs in `us-east-1` cost about $0.07 per hour, plus storage and the admin VM's public address.

## Use it

You need an AWS account, Terraform, and a node image. Until a release publishes a public one, build yours from [`images/`](../../images/README.md); the template finds the newest in your account. To use a published image, set `-var ami_owner=<account>`.

```sh
cd deploy/aws
terraform init
terraform apply -var region=us-east-1 -var allowed_cidr=203.0.113.4/32

terraform output -raw ca_cert > admin-ca.crt
export PGCLUSTER_ADMIN_URL=$(terraform output -raw admin_url)
export PGCLUSTER_ADMIN_KEY=$(terraform output -raw api_key)
export PGCLUSTER_ADMIN_CA=admin-ca.crt

pgcluster host list
pgcluster database create orders --allow 203.0.113.4/32 --wait
```

Hosts enrol within a few minutes of `apply`. `terraform destroy` removes everything, including the bucket and any backups in it.

## What to know

- **Your credentials stay in Terraform.** They never reach `pgcluster-admin` or the hosts. Hosts use an instance role limited to the backup bucket.
- **`allowed_cidr` has no default.** The admin API and databases accept only that range, and `0.0.0.0/0` is refused.
- **Enrolment needs no admin key on the hosts.** Terraform generates one single-use token per host, and loads their hashes into `pgcluster-admin` at boot. Tokens last an hour from the admin VM's first start.
- **Terraform state holds the API key, and each VM's user data holds secrets**, such as the CA key on the admin VM. The admin VM keeps only a hash of the API key. Treat the state file as a secret. VMs require IMDSv2.
