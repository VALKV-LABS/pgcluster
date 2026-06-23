/// BM-0 exit criterion: `pgcluster --version` and `vk-agent --version` must
/// print a version string and exit 0.
use std::process::Command;

#[test]
#[ignore = "requires built binaries: run `make build` then `make test`"]
fn pgcluster_version_exits_zero() {
    let out = Command::new(env!("CARGO_BIN_EXE_pgcluster"))
        .arg("--version")
        .output()
        .expect("failed to run pgcluster");
    assert!(
        out.status.success(),
        "pgcluster --version exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("pgcluster"),
        "expected 'pgcluster' in version output, got: {stdout}"
    );
}

#[test]
#[ignore = "requires built binaries: run `make build` then `make test`"]
fn vk_agent_version_exits_zero() {
    // vk-agent is in the workspace but a different package; find it via the
    // workspace root's target directory relative to this manifest.
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().expect("workspace root");
    let bin = workspace_root.join("target/debug/vk-agent");
    let out = Command::new(&bin)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("failed to run vk-agent at {}: {e}", bin.display()));
    assert!(
        out.status.success(),
        "vk-agent --version exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("vk-agent"),
        "expected 'vk-agent' in version output, got: {stdout}"
    );
}
