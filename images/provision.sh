#!/bin/sh
# Installs what a node runs. Versions here match docker/host.Dockerfile and
# docker/vk-agent.Dockerfile, so a test on the local stack speaks for this image.
set -eu

PG_MAJORS="16 17 18"
PG_TLE_VERSION=v1.5.3
PG_TLE_SHA256=90cd505bd2b427911f16c738bafeb3b8029d7b30194df9518270d94e06fc791e
WALG_VERSION=v3.0.9

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends ca-certificates curl gnupg lsb-release postgresql-common

# Clusters are created per instance. The packages must not start one of their own.
mkdir -p /etc/postgresql-common
echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf
/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y

for v in $PG_MAJORS; do
  apt-get install -y --no-install-recommends \
    "postgresql-$v" "postgresql-$v-pgvector" "postgresql-$v-postgis-3" "postgresql-$v-cron" \
    "postgresql-server-dev-$v"
done

# pg_tle is not packaged.
apt-get install -y --no-install-recommends build-essential libkrb5-dev libssl-dev flex bison
mkdir /tmp/pg_tle
curl -fsSL -o /tmp/pg_tle.tar.gz \
  "https://github.com/aws/pg_tle/archive/refs/tags/${PG_TLE_VERSION}.tar.gz"
echo "${PG_TLE_SHA256}  /tmp/pg_tle.tar.gz" | sha256sum -c -
tar -xzf /tmp/pg_tle.tar.gz --strip-components=1 -C /tmp/pg_tle
for v in $PG_MAJORS; do
  make -C /tmp/pg_tle clean USE_PGXS=1 "PG_CONFIG=/usr/lib/postgresql/$v/bin/pg_config"
  make -C /tmp/pg_tle install USE_PGXS=1 "PG_CONFIG=/usr/lib/postgresql/$v/bin/pg_config"
done
rm -rf /tmp/pg_tle /tmp/pg_tle.tar.gz
for v in $PG_MAJORS; do apt-get purge -y "postgresql-server-dev-$v"; done
apt-get purge -y build-essential libkrb5-dev libssl-dev flex bison
apt-get autoremove -y --purge

# WAL-G, pinned and checked.
case "$(dpkg --print-architecture)" in
  amd64) asset=amd64; sum=d6795c663894d836ba20840bffe8970aed81bbe25d3151bd4951f6f9c362def9 ;;
  arm64) asset=aarch64; sum=072eb4a4a119f817e1fa888c64a776b01e3453b2e0d967a67d149adb18fde652 ;;
  *) echo "unsupported architecture" >&2; exit 1 ;;
esac
curl -fsSL -o /usr/local/bin/wal-g \
  "https://github.com/wal-g/wal-g/releases/download/${WALG_VERSION}/wal-g-pg-22.04-${asset}"
echo "${sum}  /usr/local/bin/wal-g" | sha256sum -c -
chmod 755 /usr/local/bin/wal-g

install -m 755 /tmp/pgcluster-bin/pgcluster /tmp/pgcluster-bin/vk-agent \
  /tmp/pgcluster-bin/pgcluster-admin /usr/local/bin/
install -m 644 /tmp/pgcluster-units/*.service /etc/systemd/system/
rm -rf /tmp/pgcluster-bin /tmp/pgcluster-units

useradd --system --home-dir /var/lib/pgcluster-admin --create-home --shell /usr/sbin/nologin \
  pgcluster-admin
mkdir -p /etc/vk-agent /etc/pgcluster-admin /var/lib/pgcluster/instances /var/lib/vk-agent-host

# Tokens and host identities belong to one boot's instance, not the image.
apt-get clean
rm -rf /var/lib/apt/lists/*
