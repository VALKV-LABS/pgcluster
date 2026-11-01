# Contributing

Thanks for looking. [`todo.md`](todo.md) lists the planned work in order, and [`docs/management-plane.md`](docs/management-plane.md) describes where the project is heading. Please open an issue before starting significant work.

## Setup

You need Rust 1.85+, `protoc`, Docker with Compose, and `make`.

```sh
make setup-hooks   # once per clone: runs fmt and clippy before every push
make all           # format, lint, unit tests
make test-integ    # Docker integration tests
make test-e2e      # 3-node switchover and failover tests on Docker
```

## Pull requests

- Keep them small and focused, and say what changed and why.
- Tests ship with the change.
- A change users can see adds a line under "Unreleased" in [`CHANGELOG.md`](CHANGELOG.md), with the exact name of any setting, endpoint or command.
- `vk-agent` only executes commands. Decisions belong to the Raft leader.
- Never put secrets in logs or errors.

## Sign your commits

This project uses the [Developer Certificate of Origin](https://developercertificate.org/). It says you have the right to submit your contribution under the project's licence. To agree, sign off each commit:

```sh
git commit -s
```

That adds a `Signed-off-by: Your Name <you@example.com>` line, using your real name.

Contributions are licensed under [Apache-2.0](LICENSE).

## Security

Please report vulnerabilities privately, as described in [`SECURITY.md`](SECURITY.md), not in a public issue.

## Releasing

A release is made by pushing a tag `vX.Y.Z` that matches the workspace version in `Cargo.toml`. The tag runs CI, publishes `valkvlabs/pgcluster` and `valkvlabs/vk-agent` to Docker Hub for amd64 and arm64, and creates the GitHub release from the matching `CHANGELOG.md` section. Every push to `main` publishes the `edge` tag.
