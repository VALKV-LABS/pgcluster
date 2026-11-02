# The stack's host: vk-agent, pgcluster, each Postgres major and the extensions
# on the node image. Stage 3 builds the same list the node image installs.

# Stage 1: builder
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY pgcluster/Cargo.toml pgcluster/Cargo.toml
COPY pgcluster-admin/Cargo.toml pgcluster-admin/Cargo.toml
COPY vk-agent/Cargo.toml vk-agent/Cargo.toml
RUN mkdir -p pgcluster/src pgcluster-admin/src vk-agent/src && \
    echo 'fn main() {}' > pgcluster/src/main.rs && \
    echo '' > pgcluster/src/lib.rs && \
    echo 'fn main() {}' > pgcluster-admin/src/main.rs && \
    echo '' > pgcluster-admin/src/lib.rs && \
    echo 'fn main() {}' > vk-agent/src/main.rs && \
    echo '' > vk-agent/src/lib.rs
# Compile dependencies against the stubs, as their own cached layer.
RUN cargo build --release --bin vk-agent --bin pgcluster
COPY proto ./proto
COPY pgcluster ./pgcluster
COPY pgcluster-admin ./pgcluster-admin
COPY vk-agent ./vk-agent
# COPY keeps old timestamps, so touch the sources or cargo keeps the stub binary.
RUN touch pgcluster/src/main.rs pgcluster/src/lib.rs vk-agent/src/main.rs vk-agent/src/lib.rs && \
    cargo build --release --bin vk-agent --bin pgcluster

# Stage 2: Postgres, the extensions PGDG packages, and pg_tle, which it does not.
FROM postgres:18-bookworm AS postgres
ARG PG_TLE_VERSION=v1.5.3
ARG PG_TLE_SHA256=90cd505bd2b427911f16c738bafeb3b8029d7b30194df9518270d94e06fc791e
RUN apt-get update && \
    for v in 16 17 18; do \
      apt-get install -y --no-install-recommends \
        postgresql-$v postgresql-$v-pgvector postgresql-$v-postgis-3 postgresql-$v-cron \
        postgresql-server-dev-$v; \
    done && \
    apt-get install -y --no-install-recommends build-essential ca-certificates curl \
      libkrb5-dev libssl-dev flex bison && \
    mkdir /pg_tle && \
    curl -fsSL -o /pg_tle.tar.gz "https://github.com/aws/pg_tle/archive/refs/tags/${PG_TLE_VERSION}.tar.gz" && \
    echo "${PG_TLE_SHA256}  /pg_tle.tar.gz" | sha256sum -c - && \
    tar -xzf /pg_tle.tar.gz --strip-components=1 -C /pg_tle && \
    rm /pg_tle.tar.gz && \
    for v in 16 17 18; do \
      make -C /pg_tle clean USE_PGXS=1 PG_CONFIG=/usr/lib/postgresql/$v/bin/pg_config; \
      make -C /pg_tle install USE_PGXS=1 PG_CONFIG=/usr/lib/postgresql/$v/bin/pg_config; \
    done && \
    rm -rf /pg_tle && \
    for v in 16 17 18; do apt-get purge -y postgresql-server-dev-$v; done && \
    apt-get purge -y build-essential libkrb5-dev libssl-dev flex bison && \
    apt-get autoremove -y --purge && \
    apt-get install -y --no-install-recommends ca-certificates iproute2 systemd systemd-sysv && \
    rm -rf /var/lib/apt/lists/*

# Stage 3: runtime
FROM postgres AS runtime
COPY --from=builder /build/target/release/vk-agent /usr/local/bin/vk-agent
COPY --from=builder /build/target/release/pgcluster /usr/local/bin/pgcluster
ENTRYPOINT ["/usr/local/bin/vk-agent"]
CMD ["host", "/etc/vk-agent/host.toml"]
