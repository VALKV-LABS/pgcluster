//! Per-instance disk quotas. CPU and memory limits do nothing for disk, which is
//! the likeliest noisy neighbour, so each instance's directory gets an XFS
//! project quota of its `storage_gb`. Raising it is how storage grows online.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use super::config::QuotaKind;
use super::fsutil::write_private_if_changed;

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

static DETECTED: std::sync::OnceLock<Option<QuotaFs>> = std::sync::OnceLock::new();

/// Decide once, at start, where quotas apply.
pub fn init(kind: QuotaKind, instances_dir: &str) -> Result<()> {
    let found = detect(kind, instances_dir)?;
    match &found {
        Some(fs) => tracing::info!(mount = fs.mount_point, "instances get XFS project quotas"),
        None => tracing::warn!(
            "instances get no disk quota: {instances_dir} is not on XFS mounted with prjquota"
        ),
    }
    DETECTED.set(found).ok();
    Ok(())
}

/// Where quotas apply, if anywhere.
pub fn detected() -> Option<&'static QuotaFs> {
    DETECTED.get().and_then(Option::as_ref)
}

/// What a quota on an instance was last set to.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct Applied {
    project: u32,
    bytes: u64,
}

/// A filesystem that can hold per-directory quotas.
#[derive(Debug, Clone, PartialEq)]
pub struct QuotaFs {
    pub mount_point: String,
}

/// Where quotas apply, given the host's setting. `auto` uses XFS project quotas
/// when the filesystem has them on, and none otherwise.
pub fn detect(kind: QuotaKind, instances_dir: &str) -> Result<Option<QuotaFs>> {
    if kind == QuotaKind::None {
        return Ok(None);
    }
    let mounts = std::fs::read_to_string("/proc/self/mounts").context("read /proc/self/mounts")?;
    let found = project_quota_mount(&mounts, instances_dir);
    match (kind, found) {
        (_, Some(fs)) => Ok(Some(fs)),
        (QuotaKind::Xfs, None) => {
            bail!("disk_quota = \"xfs\", but {instances_dir} is not on XFS mounted with prjquota")
        }
        _ => Ok(None),
    }
}

/// The mount holding `path`, if it is XFS with project quotas on.
fn project_quota_mount(mounts: &str, path: &str) -> Option<QuotaFs> {
    let mount = mounts
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let (_, point, fstype, options) = (
                fields.next()?,
                fields.next()?,
                fields.next()?,
                fields.next()?,
            );
            let inside = path == point
                || point == "/"
                || path.starts_with(&format!("{}/", point.trim_end_matches('/')));
            inside.then_some((point, fstype, options))
        })
        .max_by_key(|(point, ..)| point.len())?;
    let (point, fstype, options) = mount;
    let quotas = options
        .split(',')
        .any(|o| o == "prjquota" || o == "pquota" || o == "pqnoenforce");
    (fstype == "xfs" && quotas).then(|| QuotaFs {
        mount_point: point.to_string(),
    })
}

/// A stable project ID for an instance. Collisions among a host's instances
/// are as likely as a 31-bit hash colliding.
pub fn project_id(instance_id: &str) -> u32 {
    // FNV-1a, which needs nothing outside this file.
    let mut hash: u32 = 0x811c_9dc5;
    for byte in instance_id.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    (hash & 0x7fff_ffff).max(1)
}

pub fn bytes(storage_gb: f64) -> u64 {
    (storage_gb * GIB) as u64
}

async fn xfs_quota(fs: &QuotaFs, command: &str) -> Result<()> {
    let output = Command::new("xfs_quota")
        .args(["-x", "-c", command, &fs.mount_point])
        .output()
        .await
        .context("run xfs_quota")?;
    if !output.status.success() {
        bail!(
            "xfs_quota {command:?} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Hold `dir` to `storage_gb`. Tagging the directory walks its whole tree, so it
/// happens once, and later calls only move the limit.
pub async fn apply(fs: &QuotaFs, dir: &Path, instance_id: &str, storage_gb: f64) -> Result<()> {
    let record = dir.join("quota.json");
    let wanted = Applied {
        project: project_id(instance_id),
        bytes: bytes(storage_gb),
    };
    let current: Option<Applied> = std::fs::read_to_string(&record)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok());
    if current.as_ref() == Some(&wanted) {
        return Ok(());
    }
    if current.as_ref().map(|c| c.project) != Some(wanted.project) {
        xfs_quota(
            fs,
            &format!("project -s -p {} {}", dir.display(), wanted.project),
        )
        .await?;
    }
    xfs_quota(
        fs,
        &format!("limit -p bhard={} {}", wanted.bytes, wanted.project),
    )
    .await?;
    write_private_if_changed(&record, &serde_json::to_string(&wanted)?, None)?;
    tracing::info!(instance_id, storage_gb, "set the disk quota");
    Ok(())
}

/// Lift the limit of an instance that is going away.
pub async fn remove(fs: &QuotaFs, instance_id: &str) -> Result<()> {
    xfs_quota(fs, &format!("limit -p bhard=0 {}", project_id(instance_id))).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTS: &str = "\
/dev/root / ext4 rw,relatime 0 0
/dev/nvme1n1 /var/lib/pgcluster xfs rw,relatime,prjquota 0 0
/dev/nvme2n1 /data xfs rw,relatime 0 0
";

    #[test]
    fn an_xfs_mount_with_project_quotas_is_found() {
        let fs = project_quota_mount(MOUNTS, "/var/lib/pgcluster/instances").unwrap();
        assert_eq!(fs.mount_point, "/var/lib/pgcluster");
    }

    #[test]
    fn other_filesystems_hold_no_quotas() {
        assert!(project_quota_mount(MOUNTS, "/data/instances").is_none());
        assert!(project_quota_mount(MOUNTS, "/srv/instances").is_none());
        assert!(project_quota_mount(MOUNTS, "/var/lib/pgclusterx").is_none());
    }

    #[test]
    fn project_ids_are_stable_and_never_zero() {
        assert_eq!(project_id("db-1-n1"), project_id("db-1-n1"));
        assert_ne!(project_id("db-1-n1"), project_id("db-1-n2"));
        assert!(project_id("") > 0);
        assert!(project_id("db-1-n1") <= 0x7fff_ffff);
    }

    #[test]
    fn storage_becomes_bytes() {
        assert_eq!(bytes(1.0), 1024 * 1024 * 1024);
        assert_eq!(bytes(0.5), 512 * 1024 * 1024);
    }
}
