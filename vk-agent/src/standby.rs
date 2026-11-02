//! Configures a data directory as a standby of a given primary.

use std::path::Path;

use anyhow::{Context, Result};

use crate::files::{update_auto_conf, write_pgpass, write_signal_file};

/// Write `standby.signal`, `primary_conninfo` and `primary_slot_name`.
///
/// `application_name` becomes the name `synchronous_standby_names` knows this
/// node by. If `conninfo` carries a password, it moves to `PGDATA/.pgpass`, so
/// that credentials are never stored in plaintext in the config file.
pub async fn configure_standby(
    data_dir: &Path,
    conninfo: &str,
    slot_name: &str,
    application_name: &str,
) -> Result<()> {
    write_signal_file(data_dir, "standby.signal").await?;

    if !conninfo.is_empty() {
        let (mut stored, maybe_password) = extract_conninfo_password(conninfo);
        if !application_name.is_empty() {
            stored = format!("{stored} application_name={application_name}");
        }

        if let Some(password) = maybe_password {
            let host = conninfo_value(conninfo, "host").unwrap_or_else(|| "*".to_string());
            let port = conninfo_value(conninfo, "port").unwrap_or_else(|| "5432".to_string());
            let user = conninfo_value(conninfo, "user").unwrap_or_else(|| "*".to_string());

            write_pgpass(data_dir, &host, &port, "replication", &user, &password)
                .await
                .context("write_pgpass failed")?;

            let passfile = data_dir.join(".pgpass");
            stored = format!("{stored} passfile={}", passfile.display());
        }

        update_auto_conf(data_dir, "primary_conninfo", &stored).await?;
    }

    if !slot_name.is_empty() {
        update_auto_conf(data_dir, "primary_slot_name", slot_name).await?;
    }
    Ok(())
}

/// Split `password=<value>` out of a libpq keyword/value conninfo string.
///
/// Returns `(conninfo_without_password, Some(password))` when a `password`
/// keyword is present, or `(original, None)` otherwise.
///
/// This handles the plain-token format we generate internally
/// (`key=value` pairs separated by whitespace). Values may be surrounded by
/// single quotes; the returned password has those quotes stripped.
fn extract_conninfo_password(conninfo: &str) -> (String, Option<String>) {
    let mut password: Option<String> = None;
    let filtered: Vec<&str> = conninfo
        .split_whitespace()
        .filter(|token| {
            if let Some(raw) = token.strip_prefix("password=") {
                let pw = raw.trim_matches('\'').to_string();
                password = Some(pw);
                false
            } else {
                true
            }
        })
        .collect();
    (filtered.join(" "), password)
}

/// Extract a single keyword value from a libpq conninfo string.
///
/// Returns `Some(value)` for the first `key=value` or `key='value'` token
/// that matches `key`, stripping surrounding single quotes from the value.
fn conninfo_value(conninfo: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    conninfo.split_whitespace().find_map(|token| {
        token
            .strip_prefix(&prefix)
            .map(|v| v.trim_matches('\'').to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_password_removes_token_and_returns_value() {
        let conninfo = "host=pg-primary port=5432 user=replicator password=s3cr3t";
        let (new, pw) = extract_conninfo_password(conninfo);
        assert_eq!(pw, Some("s3cr3t".to_string()));
        assert!(!new.contains("password="), "password must be stripped");
        assert!(new.contains("host=pg-primary"));
        assert!(new.contains("port=5432"));
        assert!(new.contains("user=replicator"));
    }

    #[test]
    fn extract_password_no_password_returns_none() {
        let conninfo = "host=pg-primary port=5432 user=replicator";
        let (new, pw) = extract_conninfo_password(conninfo);
        assert_eq!(pw, None);
        assert_eq!(new, conninfo);
    }

    #[test]
    fn extract_password_strips_surrounding_quotes() {
        // Single-quoted unspaced value (e.g. password='p@ss!').
        let conninfo = "host=pg-primary password='p@ss!'";
        let (_, pw) = extract_conninfo_password(conninfo);
        assert_eq!(pw, Some("p@ss!".to_string()));
    }

    #[test]
    fn conninfo_value_finds_host() {
        let conninfo = "host=pg-primary port=5432 user=replicator";
        assert_eq!(
            conninfo_value(conninfo, "host"),
            Some("pg-primary".to_string())
        );
    }

    #[test]
    fn conninfo_value_missing_key_returns_none() {
        let conninfo = "host=pg-primary port=5432";
        assert_eq!(conninfo_value(conninfo, "user"), None);
    }
}
