//! What this machine has to offer.

use std::net::UdpSocket;

use anyhow::{Context, Result};
use nix::sys::statvfs::statvfs;

use super::config::HostConfig;
use super::model::Resources;

/// The Postgres majors `pgcluster-admin` offers.
const MAJORS: &[&str] = &["16", "17", "18"];

/// Each major installed here, and its full version, from `postgres --version`.
/// A package upgrade changes this before any running server restarts.
pub fn postgres_versions(config: &HostConfig) -> std::collections::BTreeMap<String, String> {
    MAJORS
        .iter()
        .filter_map(|major| {
            let output = std::process::Command::new(format!("{}/postgres", config.pg_bin(major)))
                .arg("--version")
                .output()
                .ok()?;
            let version = super::instance::release(&String::from_utf8_lossy(&output.stdout))?;
            Some((major.to_string(), version))
        })
        .collect()
}

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

pub fn measure(instances_dir: &str) -> Result<Resources> {
    let vcpu = std::thread::available_parallelism()
        .map(|n| n.get() as f64)
        .unwrap_or(1.0);
    let meminfo = std::fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
    let disk = statvfs(instances_dir).context("read filesystem size")?;
    let storage_gb = disk.blocks() as f64 * disk.fragment_size() as f64 / GIB;
    Ok(Resources {
        vcpu,
        memory_gb: parse_mem_total(&meminfo).context("find MemTotal")?,
        storage_gb,
    })
}

fn parse_mem_total(meminfo: &str) -> Option<f64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024.0 / GIB)
}

/// The local address this host uses to reach `admin_url`. A UDP connect sends
/// nothing: it only makes the kernel choose a route.
pub fn detect_address(admin_url: &str) -> Result<String> {
    let authority = admin_url
        .trim_start_matches("https://")
        .split('/')
        .next()
        .context("admin_url has no host")?;
    let target = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:443")
    };
    let socket = UdpSocket::bind("0.0.0.0:0").context("open a socket")?;
    socket
        .connect(&target)
        .with_context(|| format!("find a route to {target}"))?;
    Ok(socket
        .local_addr()
        .context("read local address")?
        .ip()
        .to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_is_read_from_meminfo() {
        let total = parse_mem_total("MemFree: 1 kB\nMemTotal:  8388608 kB\n").unwrap();
        assert!((total - 8.0).abs() < 0.001);
    }

    #[test]
    fn the_address_comes_from_the_route() {
        assert_eq!(
            detect_address("https://127.0.0.1:8444").unwrap(),
            "127.0.0.1"
        );
    }
}
