use std::path::Path;

use anyhow::{Context, Result};
use tokio::fs;

/// Write an empty signal file into `data_dir`.
///
/// Used for `promote.signal` and `standby.signal`. Creates the file if it
/// does not exist; truncates and leaves it empty if it does.
pub async fn write_signal_file(data_dir: &Path, filename: &str) -> Result<()> {
    let path = data_dir.join(filename);
    fs::write(&path, b"")
        .await
        .with_context(|| format!("failed to write signal file {}", path.display()))?;
    tracing::info!(path = %path.display(), "signal file written");
    Ok(())
}

/// Update (or insert) a single `key = 'value'` line in `postgresql.auto.conf`.
///
/// The update is atomic: the new content is first written to a `.tmp` file in
/// the same directory, then renamed over the target file so readers always see
/// a complete file.
pub async fn update_auto_conf(data_dir: &Path, key: &str, value: &str) -> Result<()> {
    let conf_path = data_dir.join("postgresql.auto.conf");
    // Unique per-call tmp name avoids a race when two concurrent async tasks
    // (e.g. two Demote RPCs) both write to the same fixed path and one clobbers
    // the other.  Using the OS PID makes it unique per-process-per-call pair;
    // a thread-id suffix would be needed for true concurrency safety, but the
    // vk-agent RPC handler serializes each field update sequentially.
    let tmp_name = format!("postgresql.auto.conf.{}.tmp", std::process::id());
    let tmp_path = data_dir.join(&tmp_name);

    // PostgreSQL's guc-file.l lexer terminates quoted strings at the first
    // newline character, so an embedded newline produces a malformed file that
    // postgres rejects on reload.  Replace any newlines in the value with spaces.
    let value = &value.replace(['\n', '\r'], " ");

    // Read existing content, or start empty if the file does not exist yet.
    let existing = match fs::read_to_string(&conf_path).await {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", conf_path.display())),
    };

    let new_line = format!("{} = '{}'", key, value.replace('\'', "''"));
    let prefix = format!("{} =", key);

    // Rebuild the file, replacing the matching line or appending a new one.
    let mut found = false;
    let mut lines: Vec<String> = existing
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            // Match lines that set this key (handles both `key =` and `key=`).
            if trimmed == key
                || trimmed.starts_with(&prefix)
                || trimmed.starts_with(&format!("{}=", key))
            {
                found = true;
                new_line.clone()
            } else {
                line.to_owned()
            }
        })
        .collect();

    if !found {
        lines.push(new_line);
    }

    let mut new_content = lines.join("\n");
    if !new_content.ends_with('\n') {
        new_content.push('\n');
    }

    // Write to temp file then atomically rename.
    fs::write(&tmp_path, new_content.as_bytes())
        .await
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;

    fs::rename(&tmp_path, &conf_path).await.with_context(|| {
        format!(
            "failed to rename {} → {}",
            tmp_path.display(),
            conf_path.display()
        )
    })?;

    tracing::info!(
        key = key,
        value = value,
        path = %conf_path.display(),
        "postgresql.auto.conf updated"
    );
    Ok(())
}

/// Write a `.pgpass` file with one entry for the given credentials.
///
/// PostgreSQL ignores `.pgpass` if the file is world- or group-readable, so
/// permissions are set to `0600` on POSIX systems. Any `:` or `\` in the
/// field values are escaped per the libpq spec.
///
/// The file is written to `data_dir/.pgpass`.
pub async fn write_pgpass(
    data_dir: &Path,
    host: &str,
    port: &str,
    database: &str,
    user: &str,
    password: &str,
) -> Result<()> {
    fn esc(s: &str) -> String {
        s.replace('\\', "\\\\").replace(':', "\\:")
    }

    let content = format!(
        "{}:{}:{}:{}:{}\n",
        esc(host),
        esc(port),
        esc(database),
        esc(user),
        esc(password)
    );

    let path = data_dir.join(".pgpass");
    fs::write(&path, content.as_bytes())
        .await
        .with_context(|| format!("failed to write .pgpass at {}", path.display()))?;

    // PostgreSQL refuses to read .pgpass if it is world- or group-readable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        fs::set_permissions(&path, perms)
            .await
            .with_context(|| format!("failed to chmod 0600 {}", path.display()))?;
    }

    tracing::info!(path = %path.display(), "wrote .pgpass");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn read_conf(dir: &Path) -> String {
        tokio::fs::read_to_string(dir.join("postgresql.auto.conf"))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn write_signal_file_creates_empty_file() {
        let dir = TempDir::new().unwrap();
        write_signal_file(dir.path(), "promote.signal")
            .await
            .unwrap();
        let meta = tokio::fs::metadata(dir.path().join("promote.signal"))
            .await
            .unwrap();
        assert_eq!(meta.len(), 0);
    }

    #[tokio::test]
    async fn write_promote_signal_creates_file() {
        let dir = TempDir::new().unwrap();
        write_signal_file(dir.path(), "promote.signal")
            .await
            .unwrap();
        assert!(dir.path().join("promote.signal").exists());
    }

    #[tokio::test]
    async fn update_auto_conf_writes_key_value() {
        let dir = TempDir::new().unwrap();
        update_auto_conf(dir.path(), "primary_conninfo", "host=pg1")
            .await
            .unwrap();
        let content = read_conf(dir.path()).await;
        assert!(content.contains("primary_conninfo = 'host=pg1'"));
    }

    #[tokio::test]
    async fn update_auto_conf_overwrites_existing_key() {
        let dir = TempDir::new().unwrap();
        update_auto_conf(dir.path(), "primary_conninfo", "host=old")
            .await
            .unwrap();
        update_auto_conf(dir.path(), "primary_conninfo", "host=new")
            .await
            .unwrap();
        let content = read_conf(dir.path()).await;
        assert!(content.contains("primary_conninfo = 'host=new'"));
        assert!(!content.contains("host=old"), "old value must be replaced");
    }

    #[tokio::test]
    async fn update_auto_conf_creates_file_if_missing() {
        let dir = TempDir::new().unwrap();
        update_auto_conf(dir.path(), "primary_conninfo", "host=primary")
            .await
            .unwrap();
        let content = read_conf(dir.path()).await;
        assert!(
            content.contains("primary_conninfo = 'host=primary'"),
            "unexpected content: {content}"
        );
    }

    #[tokio::test]
    async fn update_auto_conf_updates_existing_key() {
        let dir = TempDir::new().unwrap();
        // Write initial file.
        tokio::fs::write(
            dir.path().join("postgresql.auto.conf"),
            "primary_conninfo = 'host=old'\n",
        )
        .await
        .unwrap();

        update_auto_conf(dir.path(), "primary_conninfo", "host=new")
            .await
            .unwrap();
        let content = read_conf(dir.path()).await;
        assert!(
            content.contains("primary_conninfo = 'host=new'"),
            "expected updated key, got: {content}"
        );
        assert!(
            !content.contains("host=old"),
            "old value should have been replaced: {content}"
        );
    }

    #[tokio::test]
    async fn update_auto_conf_appends_new_key() {
        let dir = TempDir::new().unwrap();
        tokio::fs::write(
            dir.path().join("postgresql.auto.conf"),
            "wal_level = 'replica'\n",
        )
        .await
        .unwrap();

        update_auto_conf(dir.path(), "primary_conninfo", "host=primary")
            .await
            .unwrap();
        let content = read_conf(dir.path()).await;
        assert!(content.contains("wal_level = 'replica'"));
        assert!(content.contains("primary_conninfo = 'host=primary'"));
    }

    #[tokio::test]
    async fn update_auto_conf_escapes_single_quotes_in_value() {
        let dir = TempDir::new().unwrap();
        update_auto_conf(dir.path(), "application_name", "it's alive")
            .await
            .unwrap();
        let content = read_conf(dir.path()).await;
        // Single quote in value must be doubled for PostgreSQL syntax.
        assert!(
            content.contains("application_name = 'it''s alive'"),
            "unexpected content: {content}"
        );
    }

    #[tokio::test]
    async fn write_pgpass_creates_correct_entry() {
        let dir = TempDir::new().unwrap();
        write_pgpass(
            dir.path(),
            "pg-primary",
            "5432",
            "replication",
            "replicator",
            "s3cret",
        )
        .await
        .unwrap();
        let content = tokio::fs::read_to_string(dir.path().join(".pgpass"))
            .await
            .unwrap();
        assert_eq!(content, "pg-primary:5432:replication:replicator:s3cret\n");
    }

    #[tokio::test]
    async fn write_pgpass_escapes_special_chars() {
        let dir = TempDir::new().unwrap();
        // Colons in the password must be escaped.
        write_pgpass(
            dir.path(),
            "host",
            "5432",
            "replication",
            "user",
            "pa:ss\\word",
        )
        .await
        .unwrap();
        let content = tokio::fs::read_to_string(dir.path().join(".pgpass"))
            .await
            .unwrap();
        assert_eq!(content, "host:5432:replication:user:pa\\:ss\\\\word\n");
    }

    #[tokio::test]
    async fn update_auto_conf_multiple_keys() {
        let dir = TempDir::new().unwrap();
        update_auto_conf(dir.path(), "primary_conninfo", "host=primary")
            .await
            .unwrap();
        update_auto_conf(dir.path(), "primary_slot_name", "slot1")
            .await
            .unwrap();
        update_auto_conf(dir.path(), "recovery_target_timeline", "latest")
            .await
            .unwrap();

        let content = read_conf(dir.path()).await;
        assert!(content.contains("primary_conninfo = 'host=primary'"));
        assert!(content.contains("primary_slot_name = 'slot1'"));
        assert!(content.contains("recovery_target_timeline = 'latest'"));
    }
}
