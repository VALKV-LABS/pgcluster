//! Renders WAL-G's settings from `[backup]`. With the agent's `walg` module, this
//! is all that knows about WAL-G: the backup RPCs name no tool, so another
//! engine would replace these two files.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use crate::config::BackupConfig;

/// The settings an agent needs to reach this cluster's repository.
///
/// Environment credentials are included when set, because Postgres runs
/// `wal-g wal-push` with no access to pgcluster's environment. Instance roles
/// need none.
pub fn settings(config: &BackupConfig) -> HashMap<String, String> {
    let mut settings = HashMap::new();
    settings.insert(
        "WALG_DELTA_MAX_STEPS".to_string(),
        config.delta_steps.to_string(),
    );
    settings.insert("WALG_COMPRESSION_METHOD".to_string(), "zstd".to_string());

    if let Some(s3) = &config.s3 {
        let prefix = s3.prefix.trim_matches('/');
        settings.insert(
            "WALG_S3_PREFIX".to_string(),
            format!("s3://{}/{prefix}", s3.bucket),
        );
        if let Some(region) = &s3.region {
            settings.insert("AWS_REGION".to_string(), region.clone());
        }
        if let Some(endpoint) = &s3.endpoint {
            settings.insert("AWS_ENDPOINT".to_string(), endpoint.clone());
        }
        if s3.path_style {
            settings.insert("AWS_S3_FORCE_PATH_STYLE".to_string(), "true".to_string());
        }
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            let value = crate::credentials::get(name);
            if !value.is_empty() {
                settings.insert(name.to_string(), value);
            }
        }
    }
    if let Some(posix) = &config.posix {
        settings.insert("WALG_FILE_PREFIX".to_string(), posix.path.clone());
    }

    if let Some(env) = &config.encryption_key_env {
        let key = crate::credentials::get(env);
        if !key.is_empty() {
            settings.insert("WALG_LIBSODIUM_KEY".to_string(), key);
            settings.insert(
                "WALG_LIBSODIUM_KEY_TRANSFORM".to_string(),
                "base64".to_string(),
            );
        }
    }
    settings
}

/// Identifies a set of settings, so the leader can tell when a node is behind.
pub fn version(settings: &HashMap<String, String>) -> String {
    let ordered: BTreeMap<_, _> = settings.iter().collect();
    let mut hasher = DefaultHasher::new();
    ordered.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3Config;

    fn config() -> BackupConfig {
        toml::from_str("[posix]\npath = \"/backups\"").unwrap()
    }

    #[test]
    fn posix_uses_the_file_prefix() {
        let s = settings(&config());
        assert_eq!(s["WALG_FILE_PREFIX"], "/backups");
        assert_eq!(s["WALG_DELTA_MAX_STEPS"], "6");
        assert!(!s.contains_key("WALG_S3_PREFIX"));
    }

    #[test]
    fn s3_uses_the_bucket_and_prefix() {
        let cfg = BackupConfig {
            posix: None,
            s3: Some(S3Config {
                bucket: "b".into(),
                prefix: "/backups/one/".into(),
                region: Some("us-east-1".into()),
                endpoint: Some("http://minio:9000".into()),
                path_style: true,
            }),
            ..config()
        };
        let s = settings(&cfg);
        assert_eq!(s["WALG_S3_PREFIX"], "s3://b/backups/one");
        assert_eq!(s["AWS_ENDPOINT"], "http://minio:9000");
        assert_eq!(s["AWS_S3_FORCE_PATH_STYLE"], "true");
    }

    #[test]
    fn version_tracks_content_not_order() {
        let a = HashMap::from([("X".to_string(), "1".to_string()), ("Y".into(), "2".into())]);
        let b = HashMap::from([("Y".to_string(), "2".to_string()), ("X".into(), "1".into())]);
        let c = HashMap::from([("X".to_string(), "9".to_string()), ("Y".into(), "2".into())]);
        assert_eq!(version(&a), version(&b));
        assert_ne!(version(&a), version(&c));
    }
}
