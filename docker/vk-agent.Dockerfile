# Stage 1: builder
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /build
# Install build deps for tonic/prost codegen
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY pgcluster/Cargo.toml pgcluster/Cargo.toml
COPY pgcluster-admin/Cargo.toml pgcluster-admin/Cargo.toml
COPY vk-agent/Cargo.toml vk-agent/Cargo.toml
# Create stub lib files so cargo can build deps
RUN mkdir -p pgcluster/src pgcluster-admin/src vk-agent/src && \
    echo 'fn main() {}' > pgcluster/src/main.rs && \
    echo '' > pgcluster/src/lib.rs && \
    echo 'fn main() {}' > pgcluster-admin/src/main.rs && \
    echo '' > pgcluster-admin/src/lib.rs && \
    echo 'fn main() {}' > vk-agent/src/main.rs && \
    echo '' > vk-agent/src/lib.rs
# Compile dependencies against the stubs, as their own cached layer.
RUN cargo build --release --bin vk-agent
COPY proto ./proto
COPY pgcluster ./pgcluster
COPY vk-agent ./vk-agent
# COPY keeps old timestamps, so touch the sources or cargo keeps the stub binary.
RUN touch pgcluster/src/main.rs pgcluster/src/lib.rs vk-agent/src/main.rs vk-agent/src/lib.rs && \
    cargo build --release --bin vk-agent

# Stage 2: WAL-G, pinned and checked. The 22.04 build needs an older glibc than
# bookworm has, and carries libsodium for encryption.
FROM debian:bookworm-slim AS walg
ARG WALG_VERSION=v3.0.9
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    case "${TARGETARCH}" in \
      amd64) asset=amd64; sum=d6795c663894d836ba20840bffe8970aed81bbe25d3151bd4951f6f9c362def9 ;; \
      arm64) asset=aarch64; sum=072eb4a4a119f817e1fa888c64a776b01e3453b2e0d967a67d149adb18fde652 ;; \
      *) echo "unsupported architecture ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    curl -fsSL -o /wal-g "https://github.com/wal-g/wal-g/releases/download/${WALG_VERSION}/wal-g-pg-22.04-${asset}" && \
    echo "${sum}  /wal-g" | sha256sum -c - && \
    chmod 755 /wal-g

# Stage 3: runtime
# The Postgres image, so pg_ctl, pg_basebackup and pg_rewind match the server.
# It already has the postgres user, with uid 999.
FROM postgres:16-bookworm
COPY --from=builder /build/target/release/vk-agent /usr/local/bin/vk-agent
COPY --from=walg /wal-g /usr/local/bin/wal-g
USER postgres
EXPOSE 7001
ENTRYPOINT ["/usr/local/bin/vk-agent"]
CMD ["/etc/vk-agent/config.toml"]
