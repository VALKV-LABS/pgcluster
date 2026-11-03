//! Seeds this node from the primary: a base backup, then Postgres as a standby.

use std::path::Path;

use anyhow::{bail, Context, Result};
use tokio::process::Command;

use crate::process::PgCtl;
use crate::server::agent::SeedRequest;
use crate::standby::configure_standby;

/// Replace `data_dir` with a base backup of the primary and start as a standby.
///
/// Refuses to touch a node that has data unless `force` is set, because that
/// data may be the only copy of something.
pub async fn seed(pg_ctl: &PgCtl, data_dir: &Path, req: &SeedRequest) -> Result<()> {
    let running = pg_ctl.is_running().await.unwrap_or(false);
    if !req.force && (running || has_data(data_dir).await?) {
        bail!(
            "{} has data; seed with force to replace it",
            data_dir.display()
        );
    }
    if running {
        pg_ctl.stop_fast().await?;
    }
    clear_contents(data_dir).await?;

    base_backup(pg_ctl, data_dir, req).await?;

    let conninfo = format!(
        "host={} port={} user={} password={}",
        req.primary_host, req.primary_port, req.user, req.password
    );
    configure_standby(data_dir, &conninfo, &req.slot_name, &req.application_name).await?;

    // Postgres refuses a data directory that others can read.
    set_owner_only(data_dir).await?;
    pg_ctl.start().await
}

/// Rewind this node's timeline to the primary's, and start as a standby.
///
/// Leaves the data alone if pg_rewind fails, so the caller can decide to seed.
/// pg_rewind itself recovers a data directory that was not shut down cleanly.
pub async fn rewind(pg_ctl: &PgCtl, data_dir: &Path, req: &SeedRequest) -> Result<()> {
    if pg_ctl.is_running().await.unwrap_or(false) {
        pg_ctl.stop_fast().await?;
    }

    let source = format!(
        "host={} port={} user={} dbname=postgres",
        req.primary_host, req.primary_port, req.user
    );
    let output = Command::new(pg_ctl.tool("pg_rewind"))
        .arg("--target-pgdata")
        .arg(data_dir)
        .arg("--source-server")
        .arg(&source)
        .env("PGPASSWORD", &req.password)
        .output()
        .await
        .context("spawn pg_rewind")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        bail!("pg_rewind failed: {}", stderr.trim());
    }
    // pg_rewind compares timeline IDs. This node accepted writes as a primary,
    // so it cannot truly match: two nodes that promoted independently can share
    // an ID with different histories, and rejoining would keep the diverged
    // commits. Fail, so the caller seeds it instead.
    if stdout.contains("no rewind required") || stderr.contains("no rewind required") {
        bail!("pg_rewind found the same timeline ID on a node that diverged");
    }

    let conninfo = format!("{source} password={}", req.password);
    configure_standby(data_dir, &conninfo, &req.slot_name, &req.application_name).await?;
    pg_ctl.start().await
}

async fn base_backup(pg_ctl: &PgCtl, data_dir: &Path, req: &SeedRequest) -> Result<()> {
    let mut command = Command::new(pg_ctl.tool("pg_basebackup"));
    command
        .arg("--host")
        .arg(&req.primary_host)
        .arg("--port")
        .arg(req.primary_port.to_string())
        .arg("--username")
        .arg(&req.user)
        .arg("--pgdata")
        .arg(data_dir)
        .args(["--wal-method=stream", "--checkpoint=fast", "--no-password"])
        .env("PGPASSWORD", &req.password);
    if !req.slot_name.is_empty() {
        command.arg("--slot").arg(&req.slot_name);
    }

    let output = command.output().await.context("spawn pg_basebackup")?;
    if !output.status.success() {
        bail!(
            "pg_basebackup failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

pub async fn has_data(data_dir: &Path) -> Result<bool> {
    tokio::fs::try_exists(data_dir.join("PG_VERSION"))
        .await
        .context("check data directory")
}

/// Empty `data_dir`, keeping the directory itself, which may be a mount point.
pub(crate) async fn clear_contents(data_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(data_dir)
        .await
        .with_context(|| format!("create {}", data_dir.display()))?;
    let mut entries = tokio::fs::read_dir(data_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?;
        } else {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) async fn set_owner_only(data_dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700))
        .await
        .context("restrict data directory")
}

#[cfg(not(unix))]
pub(crate) async fn set_owner_only(_data_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Stdio;
    use tempfile::TempDir;

    /// Write an executable script from a child process. Tests run in parallel,
    /// and a write handle open here can leak into a process another test is
    /// forking, which makes running the script fail with ETXTBSY.
    fn write_script(path: &Path, contents: &str) {
        let mut writer = std::process::Command::new("sh")
            .args(["-c", "cat > \"$1\" && chmod 755 \"$1\"", "sh"])
            .arg(path)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = writer.stdin.take().unwrap();
        stdin.write_all(contents.as_bytes()).unwrap();
        drop(stdin);
        assert!(writer.wait().unwrap().success());
    }

    /// Fake pg_ctl and pg_basebackup that log their arguments. pg_ctl reports
    /// `status_code` for `status`, and pg_basebackup exits with `backup_code`.
    fn fake_tools(status_code: u32, backup_code: u32) -> (TempDir, PgCtl, std::path::PathBuf) {
        let bin = tempfile::tempdir().unwrap();
        let data = bin.path().join("data");
        let log = bin.path().join("calls.log");
        let script = |name: &str, body: String| {
            write_script(&bin.path().join(name), &format!("#!/bin/sh\n{body}\n"));
        };
        script(
            "pg_ctl",
            format!(
                "echo \"pg_ctl $1\" >> {log}\nif [ \"$1\" = status ]; then exit {status_code}; fi",
                log = log.display()
            ),
        );
        script(
            "pg_basebackup",
            format!(
                "echo \"pg_basebackup $* PGPASSWORD=$PGPASSWORD\" >> {log}\n\
                 mkdir -p {data}/global && echo 16 > {data}/PG_VERSION\nexit {backup_code}",
                log = log.display(),
                data = data.display()
            ),
        );
        let pg_ctl = PgCtl::new(
            bin.path().join("pg_ctl").to_str().unwrap(),
            data.to_str().unwrap(),
        );
        (bin, pg_ctl, data)
    }

    #[tokio::test]
    async fn rewind_stops_postgres_and_restarts_it_as_a_standby() {
        let (bin, pg_ctl, data) = fake_tools(0, 0);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("kept"), "x").unwrap();
        let script = bin.path().join("pg_rewind");
        let log = bin.path().join("calls.log");
        write_script(
            &script,
            &format!(
                "#!/bin/sh\necho \"pg_rewind $* PGPASSWORD=$PGPASSWORD\" >> {}\n",
                log.display()
            ),
        );

        rewind(&pg_ctl, &data, &request(false)).await.unwrap();

        let calls = calls(&bin);
        assert!(calls.contains("pg_ctl stop"), "{calls}");
        assert!(calls.contains("host=pg1 port=5432"), "{calls}");
        assert!(calls.contains("PGPASSWORD=secret"), "{calls}");
        assert!(calls.contains("pg_ctl start"), "{calls}");
        assert!(data.join("kept").exists(), "a rewind keeps the data");
        assert!(data.join("standby.signal").exists());
    }

    #[tokio::test]
    async fn no_rewind_required_counts_as_failure_on_a_diverged_node() {
        let (bin, pg_ctl, data) = fake_tools(3, 0);
        std::fs::create_dir_all(&data).unwrap();
        let script = bin.path().join("pg_rewind");
        write_script(&script, "#!/bin/sh\necho 'no rewind required' >&2\n");

        let err = rewind(&pg_ctl, &data, &request(false)).await.unwrap_err();

        assert!(err.to_string().contains("same timeline"), "{err}");
        assert!(!data.join("standby.signal").exists());
        assert!(!std::fs::read_to_string(bin.path().join("calls.log"))
            .unwrap()
            .contains("pg_ctl start"));
    }

    #[tokio::test]
    async fn a_failed_rewind_leaves_postgres_stopped_and_the_data_alone() {
        let (bin, pg_ctl, data) = fake_tools(3, 0);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("kept"), "x").unwrap();
        let script = bin.path().join("pg_rewind");
        write_script(&script, "#!/bin/sh\necho diverged >&2\nexit 1\n");

        let err = rewind(&pg_ctl, &data, &request(false)).await.unwrap_err();

        assert!(err.to_string().contains("diverged"), "{err}");
        assert!(data.join("kept").exists());
        assert!(!data.join("standby.signal").exists());
        assert!(!std::fs::read_to_string(bin.path().join("calls.log"))
            .unwrap()
            .contains("pg_ctl start"));
    }

    fn request(force: bool) -> SeedRequest {
        SeedRequest {
            primary_host: "pg1".into(),
            primary_port: 5432,
            user: "replicator".into(),
            password: "secret".into(),
            slot_name: "pgcluster_pg4".into(),
            application_name: "pg4".into(),
            force,
        }
    }

    fn calls(bin: &TempDir) -> String {
        std::fs::read_to_string(bin.path().join("calls.log")).unwrap()
    }

    #[tokio::test]
    async fn an_empty_node_is_seeded_and_started_as_a_standby() {
        let (bin, pg_ctl, data) = fake_tools(3, 0);

        seed(&pg_ctl, &data, &request(false)).await.unwrap();

        let calls = calls(&bin);
        assert!(calls.contains("--host pg1 --port 5432"), "{calls}");
        assert!(calls.contains("--slot pgcluster_pg4"), "{calls}");
        assert!(calls.contains("PGPASSWORD=secret"), "{calls}");
        assert!(calls.contains("pg_ctl start"), "{calls}");

        assert!(data.join("standby.signal").exists());
        let conf = std::fs::read_to_string(data.join("postgresql.auto.conf")).unwrap();
        assert!(conf.contains("application_name=pg4"), "{conf}");
        assert!(conf.contains("primary_slot_name"), "{conf}");
        assert!(
            !conf.contains("secret"),
            "the password must stay out of {conf}"
        );

        let mode = std::fs::metadata(&data).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[tokio::test]
    async fn a_node_with_data_refuses_without_force() {
        let (bin, pg_ctl, data) = fake_tools(3, 0);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("PG_VERSION"), "16").unwrap();
        std::fs::write(data.join("precious"), "x").unwrap();

        let err = seed(&pg_ctl, &data, &request(false)).await.unwrap_err();

        assert!(err.to_string().contains("force"), "{err}");
        assert!(data.join("precious").exists());
        assert!(!calls(&bin).contains("pg_basebackup"));
    }

    #[tokio::test]
    async fn a_running_node_refuses_without_force() {
        let (_bin, pg_ctl, data) = fake_tools(0, 0);
        std::fs::create_dir_all(&data).unwrap();
        assert!(seed(&pg_ctl, &data, &request(false)).await.is_err());
    }

    #[tokio::test]
    async fn force_stops_postgres_and_replaces_the_data() {
        let (bin, pg_ctl, data) = fake_tools(0, 0);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("old"), "x").unwrap();

        seed(&pg_ctl, &data, &request(true)).await.unwrap();

        assert!(calls(&bin).contains("pg_ctl stop"));
        assert!(!data.join("old").exists());
        assert!(data.join("PG_VERSION").exists());
    }

    #[tokio::test]
    async fn a_failed_backup_does_not_start_postgres() {
        let (bin, pg_ctl, data) = fake_tools(3, 1);

        assert!(seed(&pg_ctl, &data, &request(false)).await.is_err());
        assert!(!calls(&bin).contains("pg_ctl start"));
    }
}
