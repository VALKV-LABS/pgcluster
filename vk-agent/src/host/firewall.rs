//! Keeps an instance's ports to the machines that should reach them: clients on
//! the allowlist for the proxy, and the database's own nodes for everything else.
//!
//! Each instance has its own nftables table. A script replaces the table in one
//! transaction, so no change opens a gap.

use std::net::IpAddr;
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use ipnet::IpNet;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::config::FirewallKind;

/// What one instance's table has to say.
pub struct Rules<'a> {
    pub instance_id: &'a str,
    pub proxy_port: u16,
    /// Postgres, Raft and `vk-agent`, which only the database's nodes may reach.
    pub peer_ports: [u16; 3],
    pub peers: &'a [IpAddr],
    pub allowed_cidrs: &'a [IpNet],
}

/// Instance IDs can outrun what nftables accepts as a name.
fn table_name(instance_id: &str) -> String {
    let hash = hex::encode(Sha256::digest(instance_id.as_bytes()));
    format!("pgcluster_{}", &hash[..16])
}

fn set(items: impl Iterator<Item = String>) -> Option<String> {
    let items: Vec<String> = items.collect();
    (!items.is_empty()).then(|| format!("{{ {} }}", items.join(", ")))
}

fn accept_sources(ports: &str, v4: Option<String>, v6: Option<String>) -> String {
    let mut lines = String::new();
    if let Some(sources) = v4 {
        lines.push_str(&format!(
            "    tcp dport {ports} ip saddr {sources} accept\n"
        ));
    }
    if let Some(sources) = v6 {
        lines.push_str(&format!(
            "    tcp dport {ports} ip6 saddr {sources} accept\n"
        ));
    }
    lines
}

/// The script that replaces this instance's table. It is pure, so a change can
/// be detected by comparing scripts.
pub fn render(rules: &Rules<'_>) -> String {
    let table = table_name(rules.instance_id);
    let [postgres, raft, agent] = rules.peer_ports;
    let peer_ports = format!("{{ {postgres}, {raft}, {agent} }}");
    let proxy = rules.proxy_port.to_string();

    let peers_v4 = set(rules
        .peers
        .iter()
        .filter(|ip| ip.is_ipv4())
        .map(ToString::to_string));
    let peers_v6 = set(rules
        .peers
        .iter()
        .filter(|ip| ip.is_ipv6())
        .map(ToString::to_string));
    let clients_v4 = set(rules
        .allowed_cidrs
        .iter()
        .filter(|c| matches!(c, IpNet::V4(_)))
        .map(ToString::to_string));
    let clients_v6 = set(rules
        .allowed_cidrs
        .iter()
        .filter(|c| matches!(c, IpNet::V6(_)))
        .map(ToString::to_string));

    format!(
        "add table inet {table}\n\
         delete table inet {table}\n\
         table inet {table} {{\n\
         \x20 chain input {{\n\
         \x20   type filter hook input priority 0; policy accept;\n\
         \x20   iif \"lo\" accept\n\
         {peers}\
         \x20   tcp dport {peer_ports} drop\n\
         \x20   tcp dport {proxy} ct state established,related accept\n\
         {clients}\
         \x20   tcp dport {proxy} drop\n\
         \x20 }}\n\
         }}\n",
        peers = accept_sources(&peer_ports, peers_v4, peers_v6),
        clients = accept_sources(&proxy, clients_v4, clients_v6),
    )
}

async fn nft(script: &str) -> Result<()> {
    let mut child = Command::new("nft")
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("run nft")?;
    let mut stdin = child.stdin.take().context("open nft's input")?;
    stdin
        .write_all(script.as_bytes())
        .await
        .context("write to nft")?;
    drop(stdin);
    let output = child.wait_with_output().await.context("wait for nft")?;
    if !output.status.success() {
        bail!(
            "nft failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Make the host's rules for this instance match `rules`.
pub async fn apply(kind: FirewallKind, rules: &Rules<'_>) -> Result<()> {
    match kind {
        FirewallKind::None => Ok(()),
        FirewallKind::Nftables => nft(&render(rules)).await,
    }
}

/// Forget this instance's rules. Doing this again changes nothing.
pub async fn remove(kind: FirewallKind, instance_id: &str) -> Result<()> {
    if kind == FirewallKind::None {
        return Ok(());
    }
    let table = table_name(instance_id);
    nft(&format!(
        "add table inet {table}\ndelete table inet {table}\n"
    ))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script(peers: &[&str], cidrs: &[&str]) -> String {
        let peers: Vec<IpAddr> = peers.iter().map(|p| p.parse().unwrap()).collect();
        let cidrs: Vec<IpNet> = cidrs.iter().map(|c| c.parse().unwrap()).collect();
        render(&Rules {
            instance_id: "db-1-n1",
            proxy_port: 20000,
            peer_ports: [20002, 20003, 20004],
            peers: &peers,
            allowed_cidrs: &cidrs,
        })
    }

    #[test]
    fn peer_ports_accept_only_the_databases_nodes() {
        let text = script(&["10.0.0.2", "10.0.0.3"], &[]);
        assert!(
            text.contains(
                "tcp dport { 20002, 20003, 20004 } ip saddr { 10.0.0.2, 10.0.0.3 } accept"
            ),
            "{text}"
        );
        let accept = text.find("saddr { 10.0.0.2").unwrap();
        let drop = text.find("tcp dport { 20002, 20003, 20004 } drop").unwrap();
        assert!(accept < drop, "{text}");
    }

    #[test]
    fn an_empty_allowlist_leaves_only_the_drop() {
        let text = script(&["10.0.0.2"], &[]);
        assert!(!text.contains("tcp dport 20000 ip saddr"), "{text}");
        assert!(text.contains("tcp dport 20000 drop"), "{text}");
    }

    #[test]
    fn each_family_gets_its_own_rule() {
        let text = script(&["10.0.0.2"], &["198.51.100.0/24", "2001:db8::/32"]);
        assert!(
            text.contains("tcp dport 20000 ip saddr { 198.51.100.0/24 } accept"),
            "{text}"
        );
        assert!(
            text.contains("tcp dport 20000 ip6 saddr { 2001:db8::/32 } accept"),
            "{text}"
        );
    }

    #[test]
    fn the_table_is_replaced_in_one_script_and_named_safely() {
        let text = script(&[], &[]);
        let table = table_name("db-1-n1");
        assert!(text.starts_with(&format!("add table inet {table}\n")));
        assert!(text.contains(&format!("delete table inet {table}\n")));
        assert!(table.len() <= 32);
    }

    #[test]
    fn connected_clients_survive_an_allowlist_change() {
        let text = script(&[], &["198.51.100.0/24"]);
        let established = text.find("tcp dport 20000 ct state").unwrap();
        let drop = text.find("tcp dport 20000 drop").unwrap();
        assert!(established < drop);
    }
}
