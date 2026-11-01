# Component: TLS Manager (`tls_manager`)

## High-Level Function

pgcluster refuses to start without certificates, unless a development flag is set. The TLS manager loads them and hands the rest of the server its TLS configuration.

| Connection | Transport | Who is verified |
|---|---|---|
| pgcluster to pgcluster (Raft, topology) | gRPC over TLS | Both sides, by the cluster CA |
| pgcluster to vk-agent | gRPC over TLS | Both sides, by the cluster CA |
| Client to proxy | TLS, when the client asks for it | The proxy, by its certificate |
| Proxy to Postgres | TLS when `backend_tls = true` | Postgres, by the cluster CA and its hostname |

A gRPC connection with no client certificate, or with one from another CA, is rejected. So is a plaintext connection.

## Configuration

Every pgcluster instance and vk-agent has its own certificate from one cluster CA:

```toml
[tls]
ca_cert = "/etc/pgcluster/ca.crt"
cert    = "/etc/pgcluster/node.crt"
key     = "/etc/pgcluster/node.key"
```

All three are required, in both `pgcluster` and `vk-agent` configs. Certificates are valid as both server and client, because each gRPC peer is both.

A certificate's subject alternative name must match the address peers use: the `agent_addr`, `postgres_addr` or Raft peer address in the config.

For local development only, `insecure_plaintext = true` replaces all three and turns TLS off. Both binaries log a warning. The flag cannot be combined with certificate files, or with `backend_tls`.

## Generating certificates

`pgcluster certs` writes a CA and one certificate per name:

```sh
pgcluster certs --out /etc/pgcluster/certs pg1 pg2 pg3 10.0.0.5
```

This creates `ca.crt`, `ca.key`, and `<name>.crt` and `<name>.key` for each name. Private keys are readable only by their owner. A later run reuses the CA and keeps existing files, so it can add a node. Keep `ca.key` off the nodes.

Certificates from any other CA work too, such as Vault or cert-manager, as long as the names match.

`make start` runs the same command in a `certs` service, and the stack uses mutual TLS throughout.

## Proxy to Postgres

`[proxy] backend_tls = true` encrypts the connection to Postgres, which needs `ssl = on` and a certificate from the cluster CA. The proxy verifies that certificate and its hostname, and a server that declines TLS is an error, never a fallback to plain TCP.

It defaults to `false`. Clients authenticate to Postgres through the proxy, and with TLS on that leg Postgres offers SCRAM channel binding, which binds to a session the client is not part of. So SCRAM logins fail through the proxy with `backend_tls` on.

## Module layout

```
pgcluster/src/tls/
  mod.rs      # TlsManager: loads PEM files, builds rustls and tonic configs
  certs.rs    # CA and certificate generation for `pgcluster certs`
vk-agent/src/tls.rs   # Server TLS config for the agent
```

## Integration points

- `raft` uses `tonic_server_tls_config()` for its listener and `tonic_client_tls_config()` for peer connections.
- `agent_clients` uses `tonic_client_tls_config()` for connections to vk-agent.
- `proxy` uses `server_tls_acceptor()` for clients and `client_tls_connector()` for Postgres.
