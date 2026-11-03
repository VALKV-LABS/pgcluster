# Stage 1: builder
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY pgcluster/Cargo.toml pgcluster/Cargo.toml
COPY pgcluster-admin/Cargo.toml pgcluster-admin/Cargo.toml
COPY vk-agent/Cargo.toml vk-agent/Cargo.toml
RUN mkdir -p pgcluster/src pgcluster-admin/src pgcluster-admin/migrations vk-agent/src && \
    echo 'fn main() {}' > pgcluster/src/main.rs && \
    echo '' > pgcluster/src/lib.rs && \
    echo 'fn main() {}' > pgcluster-admin/src/main.rs && \
    echo '' > pgcluster-admin/src/lib.rs && \
    echo 'fn main() {}' > vk-agent/src/main.rs && \
    echo '' > vk-agent/src/lib.rs
# Compile dependencies against the stubs, as their own cached layer.
RUN cargo build --release --bin pgcluster-admin
COPY pgcluster-admin ./pgcluster-admin
# COPY keeps old timestamps, so touch the sources or cargo keeps the stub binary.
RUN touch pgcluster-admin/src/main.rs pgcluster-admin/src/lib.rs && \
    cargo build --release --bin pgcluster-admin

# Stage 2: runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/pgcluster-admin /usr/local/bin/pgcluster-admin
EXPOSE 8443 8444
ENTRYPOINT ["/usr/local/bin/pgcluster-admin"]
CMD ["--config", "/etc/pgcluster-admin/config.toml"]
