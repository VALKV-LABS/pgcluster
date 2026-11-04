//! Chooses hosts for nodes. A host never takes more than it has, keeps memory
//! back for the OS and the page cache, and a three-node database gets one host
//! per failure domain.

use serde::Deserialize;
use sqlx::{FromRow, PgConnection};

use crate::model::{choose_hosts, Resources};
use crate::reconciler::ONLINE_WINDOW_SECONDS;

/// Room for rounding, so that a request for exactly what is free fits.
const EPSILON: f64 = 1e-9;

/// How much of each host the scheduler hands out, and which ports.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SchedulerConfig {
    /// Memory kept for the OS, `vk-agent host` and each instance's own agents.
    pub os_reserved_memory_gb: f64,
    /// Share of the remaining memory kept for the page cache shared by every
    /// instance, which Postgres leans on beyond `shared_buffers`.
    pub page_cache_share: f64,
    /// Share of the disk kept free for logs and the OS.
    pub storage_reserve_share: f64,
    /// The first port of the first block, and the highest a block may start
    /// at. Each instance takes ten ports from its `port_base`.
    pub port_base_start: i32,
    pub port_base_end: i32,
}

/// Ports each instance takes, from its `port_base`.
pub const PORT_BLOCK: i32 = 10;

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            os_reserved_memory_gb: 1.0,
            page_cache_share: 0.2,
            storage_reserve_share: 0.1,
            port_base_start: 20000,
            port_base_end: 32760,
        }
    }
}

impl SchedulerConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if !(0.0..1.0).contains(&self.page_cache_share)
            || !(0.0..1.0).contains(&self.storage_reserve_share)
        {
            anyhow::bail!("[scheduler] shares must be at least 0 and below 1");
        }
        if self.os_reserved_memory_gb < 0.0 {
            anyhow::bail!("[scheduler] os_reserved_memory_gb cannot be negative");
        }
        if self.port_base_start < 1024
            || self.port_base_end < self.port_base_start
            || self.port_base_end > 65535 - PORT_BLOCK
        {
            anyhow::bail!(
                "[scheduler] ports need 1024 <= port_base_start <= port_base_end <= {}",
                65535 - PORT_BLOCK
            );
        }
        Ok(())
    }

    /// How many instances fit a host's port range.
    pub fn port_blocks(&self) -> i64 {
        i64::from((self.port_base_end - self.port_base_start) / PORT_BLOCK + 1)
    }

    /// What can be handed out from a host of this size.
    pub fn allocatable(&self, capacity: &Resources) -> Resources {
        let memory = (capacity.memory_gb - self.os_reserved_memory_gb).max(0.0);
        Resources {
            vcpu: capacity.vcpu,
            memory_gb: memory * (1.0 - self.page_cache_share),
            storage_gb: capacity.storage_gb * (1.0 - self.storage_reserve_share),
        }
    }
}

pub fn fits(free: &Resources, request: &Resources) -> bool {
    request.vcpu <= free.vcpu + EPSILON
        && request.memory_gb <= free.memory_gb + EPSILON
        && request.storage_gb <= free.storage_gb + EPSILON
}

fn minus(a: &Resources, b: &Resources) -> Resources {
    Resources {
        vcpu: a.vcpu - b.vcpu,
        memory_gb: a.memory_gb - b.memory_gb,
        storage_gb: a.storage_gb - b.storage_gb,
    }
}

pub fn describe(r: &Resources) -> String {
    format!(
        "{} vCPU, {} GB memory and {} GB storage",
        round(r.vcpu),
        round(r.memory_gb),
        round(r.storage_gb)
    )
}

fn round(value: f64) -> f64 {
    (value.max(0.0) * 10.0).floor() / 10.0
}

/// A host, and what it has to give.
#[derive(Debug, Clone, FromRow)]
pub struct HostLoad {
    pub id: String,
    pub name: String,
    pub address: Option<String>,
    pub failure_domain: Option<String>,
    pub capacity: Option<sqlx::types::Json<Resources>>,
    pub used_vcpu: f64,
    pub used_memory_gb: f64,
    pub used_storage_gb: f64,
    pub instances: i64,
    pub online: bool,
    pub draining: bool,
}

impl HostLoad {
    pub fn used(&self) -> Resources {
        Resources {
            vcpu: self.used_vcpu,
            memory_gb: self.used_memory_gb,
            storage_gb: self.used_storage_gb,
        }
    }

    /// What is left after reserves and every instance on it, or nothing if it
    /// has not said how large it is.
    pub fn free(&self, config: &SchedulerConfig) -> Option<Resources> {
        let capacity = &self.capacity.as_ref()?.0;
        Some(minus(&config.allocatable(capacity), &self.used()))
    }

    pub fn domain(&self) -> String {
        self.failure_domain
            .clone()
            .unwrap_or_else(|| self.id.clone())
    }

    /// Whether a node of this size could land here now.
    pub fn has_room(&self, config: &SchedulerConfig, request: &Resources) -> bool {
        self.instances < config.port_blocks()
            && self.free(config).is_some_and(|free| fits(&free, request))
    }
}

/// Every instance on a host counts, including one on its way out: it runs
/// until the host removes it.
const HOST_LOAD_QUERY: &str = "SELECT h.id, h.name, h.address, h.failure_domain, h.capacity, \
        coalesce(u.vcpu, 0) AS used_vcpu, coalesce(u.memory_gb, 0) AS used_memory_gb, \
        coalesce(u.storage_gb, 0) AS used_storage_gb, coalesce(u.instances, 0) AS instances, \
        coalesce(h.last_seen_at > now() - make_interval(secs => $1), false) AS online, \
        h.draining \
    FROM hosts h LEFT JOIN LATERAL ( \
        SELECT sum((d.spec->'resources'->>'vcpu')::float8) AS vcpu, \
               sum((d.spec->'resources'->>'memory_gb')::float8) AS memory_gb, \
               sum((d.spec->'resources'->>'storage_gb')::float8) AS storage_gb, \
               count(*) AS instances \
        FROM nodes n JOIN databases d ON d.id = n.database_id \
        WHERE n.host_id = h.id) u ON true";

/// Every host, with what it runs. `lock` holds the hosts until the transaction
/// ends, so two placements cannot both take the last of one.
pub async fn host_loads(conn: &mut PgConnection, lock: bool) -> Result<Vec<HostLoad>, sqlx::Error> {
    let lock = if lock { " FOR UPDATE OF h" } else { "" };
    sqlx::query_as::<_, HostLoad>(&format!("{HOST_LOAD_QUERY} ORDER BY h.id{lock}"))
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .fetch_all(conn)
        .await
}

/// Where a database's nodes go, or why they cannot go anywhere.
#[derive(Debug)]
pub enum Placement {
    Placed(Vec<HostLoad>),
    /// Too few hosts, or failure domains, to hold the nodes at all. More hosts
    /// may still come.
    TooFewHosts,
    /// There are hosts enough, but not enough of them have room.
    NoRoom(String),
}

/// Choose `count` hosts with room for `request` from `hosts`, in separate
/// failure domains. Hosts with the most free memory come first, which spreads
/// databases out.
pub fn place(
    config: &SchedulerConfig,
    hosts: &[HostLoad],
    count: usize,
    request: &Resources,
) -> Placement {
    let mut by_free: Vec<&HostLoad> = hosts.iter().collect();
    let free_memory = |h: &HostLoad| h.free(config).map_or(f64::MIN, |f| f.memory_gb);
    by_free.sort_by(|a, b| free_memory(b).total_cmp(&free_memory(a)));
    let roomy: Vec<&HostLoad> = by_free
        .iter()
        .copied()
        .filter(|h| h.has_room(config, request))
        .collect();
    if let Some(chosen) = choose_hosts(roomy, count, |h| h.domain()) {
        return Placement::Placed(chosen.into_iter().cloned().collect());
    }
    if choose_hosts(by_free.clone(), count, |h| h.domain()).is_none() {
        return Placement::TooFewHosts;
    }
    Placement::NoRoom(no_room(config, &by_free, count, request))
}

fn no_room(
    config: &SchedulerConfig,
    hosts: &[&HostLoad],
    count: usize,
    request: &Resources,
) -> String {
    let nodes = if count == 1 {
        "one host has".to_string()
    } else {
        format!("{count} hosts in separate failure domains have")
    };
    let largest = hosts
        .iter()
        .filter_map(|h| h.free(config))
        .max_by(|a, b| a.memory_gb.total_cmp(&b.memory_gb));
    let full = hosts
        .iter()
        .filter(|h| h.instances >= config.port_blocks())
        .count();
    let mut message = format!("no {nodes} room for {}", describe(request));
    if let Some(largest) = largest {
        message.push_str(&format!(
            ". The most any host has free is {}",
            describe(&largest)
        ));
    }
    if full > 0 {
        message.push_str(&format!(". {full} host(s) have no free port block"));
    }
    message
}

/// Whether a request can ever be met by hosts of these sizes, even empty.
pub fn too_large(
    config: &SchedulerConfig,
    hosts: &[HostLoad],
    request: &Resources,
) -> Option<String> {
    let sizes: Vec<Resources> = hosts
        .iter()
        .filter_map(|h| h.capacity.as_ref().map(|c| config.allocatable(&c.0)))
        .collect();
    if sizes.is_empty() || sizes.iter().any(|size| fits(size, request)) {
        return None;
    }
    let largest = sizes
        .iter()
        .max_by(|a, b| a.memory_gb.total_cmp(&b.memory_gb))
        .expect("not empty");
    Some(format!(
        "no host is large enough for {}. The largest offers {} after reserves",
        describe(request),
        describe(largest)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resources(vcpu: f64, memory_gb: f64, storage_gb: f64) -> Resources {
        Resources {
            vcpu,
            memory_gb,
            storage_gb,
        }
    }

    fn host(id: &str, domain: &str, used: Resources) -> HostLoad {
        HostLoad {
            id: id.into(),
            name: id.into(),
            address: Some("10.0.0.1".into()),
            failure_domain: Some(domain.into()),
            capacity: Some(sqlx::types::Json(resources(4.0, 9.0, 100.0))),
            used_vcpu: used.vcpu,
            used_memory_gb: used.memory_gb,
            used_storage_gb: used.storage_gb,
            instances: 0,
            online: true,
            draining: false,
        }
    }

    fn empty(id: &str, domain: &str) -> HostLoad {
        host(id, domain, resources(0.0, 0.0, 0.0))
    }

    #[test]
    fn memory_is_kept_back_for_the_os_and_the_page_cache() {
        let config = SchedulerConfig::default();
        let given = config.allocatable(&resources(4.0, 9.0, 100.0));
        assert_eq!(given.vcpu, 4.0);
        assert!((given.memory_gb - 6.4).abs() < 1e-9, "{given:?}");
        assert!((given.storage_gb - 90.0).abs() < 1e-9, "{given:?}");
    }

    #[test]
    fn a_host_is_never_overcommitted() {
        let config = SchedulerConfig::default();
        let busy = host("a", "r1", resources(3.0, 1.0, 10.0));
        let request = resources(2.0, 1.0, 10.0);
        assert!(!busy.has_room(&config, &request));
        let placement = place(&config, &[busy], 1, &request);
        let Placement::NoRoom(message) = placement else {
            panic!("{placement:?}");
        };
        assert!(message.contains("2 vCPU"), "{message}");
        assert!(message.contains("1 vCPU"), "{message}");
    }

    #[test]
    fn exactly_what_is_free_fits() {
        let config = SchedulerConfig::default();
        let request = resources(4.0, 6.4, 90.0);
        assert!(empty("a", "r1").has_room(&config, &request));
    }

    #[test]
    fn three_nodes_need_room_in_three_domains() {
        let config = SchedulerConfig::default();
        let request = resources(1.0, 1.0, 10.0);
        let hosts = [
            empty("a", "r1"),
            host("b", "r2", resources(4.0, 0.0, 0.0)),
            empty("c", "r2"),
            empty("d", "r3"),
        ];
        let Placement::Placed(chosen) = place(&config, &hosts, 3, &request) else {
            panic!("expected a placement");
        };
        let mut ids: Vec<&str> = chosen.iter().map(|h| h.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, ["a", "c", "d"]);

        let crowded = [
            empty("a", "r1"),
            host("b", "r2", resources(4.0, 0.0, 0.0)),
            empty("d", "r3"),
        ];
        assert!(matches!(
            place(&config, &crowded, 3, &request),
            Placement::NoRoom(_)
        ));
        assert!(matches!(
            place(&config, &crowded[..2], 3, &request),
            Placement::TooFewHosts
        ));
    }

    #[test]
    fn the_emptiest_host_comes_first() {
        let config = SchedulerConfig::default();
        let hosts = [host("a", "r1", resources(0.0, 4.0, 0.0)), empty("b", "r2")];
        let Placement::Placed(chosen) = place(&config, &hosts, 1, &resources(1.0, 1.0, 1.0)) else {
            panic!("expected a placement");
        };
        assert_eq!(chosen[0].id, "b");
    }

    #[test]
    fn a_host_with_every_port_block_taken_is_full() {
        let config = SchedulerConfig {
            port_base_start: 20000,
            port_base_end: 20010,
            ..Default::default()
        };
        assert_eq!(config.port_blocks(), 2);
        let mut full = empty("a", "r1");
        full.instances = 2;
        let request = resources(0.1, 0.1, 0.1);
        assert!(!full.has_room(&config, &request));
        let Placement::NoRoom(message) = place(&config, &[full], 1, &request) else {
            panic!("expected no room");
        };
        assert!(message.contains("port block"), "{message}");
    }

    #[test]
    fn a_request_larger_than_any_host_is_impossible() {
        let config = SchedulerConfig::default();
        let hosts = [empty("a", "r1")];
        let message = too_large(&config, &hosts, &resources(8.0, 1.0, 1.0)).unwrap();
        assert!(message.contains("no host is large enough"), "{message}");
        assert!(too_large(&config, &hosts, &resources(1.0, 1.0, 1.0)).is_none());
        assert!(too_large(&config, &[], &resources(64.0, 1.0, 1.0)).is_none());
    }

    #[test]
    fn a_host_that_has_not_reported_has_no_room() {
        let config = SchedulerConfig::default();
        let mut silent = empty("a", "r1");
        silent.capacity = None;
        assert!(!silent.has_room(&config, &resources(0.1, 0.1, 0.1)));
    }

    #[test]
    fn bad_settings_are_refused() {
        let bad = |change: fn(&mut SchedulerConfig)| {
            let mut config = SchedulerConfig::default();
            change(&mut config);
            config.validate().is_err()
        };
        assert!(SchedulerConfig::default().validate().is_ok());
        assert!(bad(|c| c.page_cache_share = 1.0));
        assert!(bad(|c| c.port_base_start = 80));
        assert!(bad(|c| c.port_base_end = 65530));
        assert!(bad(|c| c.port_base_end = c.port_base_start - 10));
    }
}
