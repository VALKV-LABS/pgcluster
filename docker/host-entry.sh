#!/bin/sh
# Writes this host's enrolment token: the stack's secret and the hash of the CA.
# A real host gets its token from `pgcluster host add`, or from Terraform.
set -eu
printf '%s.%s' "${HOST_TOKEN_SECRET}" "$(sha256sum /ca/ca.crt | cut -d' ' -f1)" > /run/enrollment-token
chmod 600 /run/enrollment-token
# The stack's backup repository, a directory every host shares.
if [ -d /backups ]; then chown postgres:postgres /backups; fi
exec "$@"
