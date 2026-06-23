# Stage 1: builder
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY pgcluster/Cargo.toml pgcluster/Cargo.toml
COPY vk-agent/Cargo.toml vk-agent/Cargo.toml
RUN mkdir -p pgcluster/src vk-agent/src && \
    echo 'fn main() {}' > pgcluster/src/main.rs && \
    echo '' > pgcluster/src/lib.rs && \
    echo 'fn main() {}' > vk-agent/src/main.rs && \
    echo '' > vk-agent/src/lib.rs
RUN cargo fetch
COPY proto ./proto
COPY pgcluster ./pgcluster
COPY vk-agent ./vk-agent
RUN cargo build --release --bin pgcluster

# Stage 2: runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/pgcluster /usr/local/bin/pgcluster
EXPOSE 5432 5433 8008 8009 7000 9190
ENTRYPOINT ["/usr/local/bin/pgcluster"]
CMD ["server", "--config", "/etc/pgcluster/config.toml"]
