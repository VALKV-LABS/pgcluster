# Test-runner image — compiles and runs cargo test inside Docker.
# Used by `make integ-up` and `make test-integ` via `docker compose run`.
FROM rust:1.95-slim-bookworm
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Cache dependency fetch separately from source compilation.
COPY Cargo.toml Cargo.lock ./
COPY pgcluster/Cargo.toml pgcluster/Cargo.toml
COPY vk-agent/Cargo.toml   vk-agent/Cargo.toml
RUN mkdir -p pgcluster/src vk-agent/src && \
    echo 'fn main() {}' > pgcluster/src/main.rs && \
    echo ''              > pgcluster/src/lib.rs  && \
    echo 'fn main() {}' > vk-agent/src/main.rs  && \
    echo ''              > vk-agent/src/lib.rs
RUN cargo fetch

# Copy full source and pre-compile all test + workspace binaries.
# This layer is rebuilt only when source changes.
COPY proto      ./proto
COPY pgcluster  ./pgcluster
COPY vk-agent   ./vk-agent
RUN cargo test --workspace --no-run

# At runtime: run unit tests AND #[ignore] integration tests, stream all output.
CMD ["cargo", "test", "--workspace", "--", "--include-ignored", "--nocapture"]
