//! Passwords `pgcluster` holds, by the name of the environment variable that
//! carries them.
//!
//! With `[cluster] secrets_file` set they come from that file, which SIGHUP reads
//! again, so a password changes with no restart and no client disconnected.
//! Otherwise they come from the environment. A value named `<NAME>_NEXT` is the
//! password about to replace `<NAME>`: while the role changes over, callers that
//! open new connections fall back to it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use anyhow::{Context, Result};

static FILE: RwLock<Option<HashMap<String, String>>> = RwLock::new(None);
static GENERATION: AtomicU64 = AtomicU64::new(0);

fn parse(text: &str) -> HashMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

/// Read `path` in place of what was read before.
pub fn load(path: &str) -> Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    *FILE.write().expect("credentials lock") = Some(parse(&text));
    bump();
    Ok(())
}

/// Tell holders of connections built from these values that they may be stale.
pub fn bump() {
    GENERATION.fetch_add(1, Ordering::SeqCst);
}

/// Rises whenever a password or certificate reloads.
pub fn generation() -> u64 {
    GENERATION.load(Ordering::SeqCst)
}

/// The password called `name`, empty if there is none.
pub fn get(name: &str) -> String {
    let file = FILE.read().expect("credentials lock");
    match file.as_ref() {
        Some(values) => values.get(name).cloned().unwrap_or_default(),
        None => std::env::var(name).unwrap_or_default(),
    }
}

/// The password about to replace `name`, during a rotation.
pub fn next(name: &str) -> Option<String> {
    Some(get(&format!("{name}_NEXT"))).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_may_hold_an_equals_sign() {
        let values = parse("A=1\nB=x=y\n\nnot a line\n");
        assert_eq!(values["A"], "1");
        assert_eq!(values["B"], "x=y");
        assert_eq!(values.len(), 2);
    }
}
