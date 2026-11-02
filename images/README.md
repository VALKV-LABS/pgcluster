# Node image

A Packer recipe for the image hosts and the `pgcluster-admin` VM boot from. It carries Postgres 16, 17 and 18, the extensions in [§9 of the spec](../docs/management-plane.md), WAL-G and all binaries, pinned.

Each release publishes it as a public AMI named `pgcluster-node-<version>-<arch>`. To build your own, put `pgcluster`, `vk-agent` and `pgcluster-admin`, built for the target architecture, in `images/bin/`, then:

```sh
cd images
packer init node.pkr.hcl
packer build -var version=dev -var arch=arm64 node.pkr.hcl
```

A host from the image runs nothing until it has config:

1. Write `/etc/vk-agent/host.toml`, setting `admin_url` and `enrollment_token_file`.
2. Save the token from `pgcluster host add` in that file.
3. Run `systemctl enable --now vk-agent-host`.

`make lint-deploy` validates the recipe.
