# Stage 1: builder
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /build
# Install build deps for tonic/prost codegen
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY pgcluster/Cargo.toml pgcluster/Cargo.toml
COPY vk-agent/Cargo.toml vk-agent/Cargo.toml
# Create stub lib files so cargo can fetch deps
RUN mkdir -p pgcluster/src vk-agent/src && \
    echo 'fn main() {}' > pgcluster/src/main.rs && \
    echo '' > pgcluster/src/lib.rs && \
    echo 'fn main() {}' > vk-agent/src/main.rs && \
    echo '' > vk-agent/src/lib.rs
RUN cargo fetch
# Copy actual source
COPY proto ./proto
COPY pgcluster ./pgcluster
COPY vk-agent ./vk-agent
RUN cargo build --release --bin vk-agent

# Stage 2: runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates postgresql && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/vk-agent /usr/local/bin/vk-agent
RUN groupmod -g 999 postgres && usermod -u 999 -g 999 postgres
USER postgres
EXPOSE 7001
ENTRYPOINT ["/usr/local/bin/vk-agent"]
CMD ["/etc/vk-agent/config.toml"]
