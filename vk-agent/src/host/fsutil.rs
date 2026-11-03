use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{chown, OpenOptionsExt};
use std::path::Path;

use anyhow::{Context, Result};

use super::runner::RunAs;

/// Replace a file atomically, readable only by its owner, so a crash leaves the
/// old file or the new one and nobody else can read either.
pub fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("create {}", tmp.display()))?;
    file.write_all(contents)
        .with_context(|| format!("write {}", tmp.display()))?;
    file.sync_all().context("sync")?;
    std::fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))
}

/// Replace a file only if its contents differ. Returns whether they did.
pub fn write_private_if_changed(
    path: &Path,
    contents: &str,
    run_as: Option<RunAs>,
) -> Result<bool> {
    if std::fs::read_to_string(path).ok().as_deref() == Some(contents) {
        return Ok(false);
    }
    write_private(path, contents.as_bytes())?;
    give_to(path, run_as)?;
    Ok(true)
}

/// Hand a path to the user instances run as.
pub fn give_to(path: &Path, run_as: Option<RunAs>) -> Result<()> {
    if let Some(user) = run_as {
        chown(path, Some(user.uid.as_raw()), Some(user.gid.as_raw()))
            .with_context(|| format!("chown {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn private_files_are_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        write_private(&path, b"x").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn a_second_write_replaces_the_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        write_private(&path, b"one").unwrap();
        write_private(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
    }

    #[test]
    fn unchanged_contents_are_not_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        assert!(write_private_if_changed(&path, "a", None).unwrap());
        assert!(!write_private_if_changed(&path, "a", None).unwrap());
        assert!(write_private_if_changed(&path, "b", None).unwrap());
    }
}
