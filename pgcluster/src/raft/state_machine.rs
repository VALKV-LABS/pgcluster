use std::sync::{Arc, Mutex};

use openraft::{
    AnyError, EntryPayload, ErrorSubject, ErrorVerb, LogId, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::raft::{
    commands::TopologyCommand,
    topology::{ClusterTopology, FailoverEvent},
    types::{NodeId, RaftTypeConfig, TopologyResponse},
};

// ── Error helpers ─────────────────────────────────────────────────────────────

fn snap_write_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            AnyError::new(e),
        ),
    }
}

fn snap_read_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            AnyError::new(e),
        ),
    }
}

// ── Snapshot payload ──────────────────────────────────────────────────────────

/// Full state-machine snapshot: membership + topology, serialised as JSON.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateMachineSnapshot {
    pub meta: StoredMembership<NodeId, openraft::BasicNode>,
    pub topology: ClusterTopology,
}

// ── State machine ─────────────────────────────────────────────────────────────

/// The Raft state machine that applies [`TopologyCommand`] entries to a
/// [`ClusterTopology`].
///
/// All fields are wrapped in `Arc` so the struct can be cheaply cloned and
/// shared with the topology watch forwarder task.
#[derive(Clone)]
pub struct TopologyStateMachine {
    state: Arc<Mutex<ClusterTopology>>,
    last_log: Arc<Mutex<Option<LogId<NodeId>>>>,
    membership: Arc<Mutex<StoredMembership<NodeId, openraft::BasicNode>>>,
    /// Sends topology snapshots to external subscribers (proxy router, etc.).
    change_tx: Arc<tokio::sync::watch::Sender<Arc<ClusterTopology>>>,
}

impl TopologyStateMachine {
    /// Create a new, empty state machine.
    ///
    /// Returns the machine and a watch receiver that fires on every topology
    /// change.
    pub fn new() -> (Self, tokio::sync::watch::Receiver<Arc<ClusterTopology>>) {
        let topology = ClusterTopology::default();
        let (tx, rx) = tokio::sync::watch::channel(Arc::new(topology.clone()));
        let sm = Self {
            state: Arc::new(Mutex::new(topology)),
            last_log: Arc::new(Mutex::new(None)),
            membership: Arc::new(Mutex::new(StoredMembership::default())),
            change_tx: Arc::new(tx),
        };
        (sm, rx)
    }

    /// Return a snapshot of the current topology (cheap `Arc` clone).
    pub fn current_topology(&self) -> Arc<ClusterTopology> {
        Arc::clone(&*self.change_tx.borrow())
    }

    // ── Public methods called by PgClusterStorage ─────────────────────────────

    /// Return the last applied log id and current membership.
    pub async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let last = *self.last_log.lock().unwrap();
        let mem = self.membership.lock().unwrap().clone();
        Ok((last, mem))
    }

    /// Apply a slice of log entries and return per-entry responses.
    pub async fn apply_entries(
        &mut self,
        entries: &[openraft::Entry<RaftTypeConfig>],
    ) -> Result<Vec<TopologyResponse>, StorageError<NodeId>> {
        let mut responses = Vec::new();

        for entry in entries {
            *self.last_log.lock().unwrap() = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {}

                EntryPayload::Normal(cmd) => {
                    let mut topo = self.state.lock().unwrap();
                    Self::apply_command(&mut topo, cmd);
                    let snap = Arc::new(topo.clone());
                    drop(topo);
                    let _ = self.change_tx.send(snap);
                }

                EntryPayload::Membership(m) => {
                    *self.membership.lock().unwrap() =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                }
            }

            let version = self.state.lock().unwrap().version;
            responses.push(TopologyResponse {
                applied_version: version,
            });
        }

        Ok(responses)
    }

    /// Build a serialised snapshot of the current state.
    pub async fn build_snapshot_data(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<RaftTypeConfig>, StorageError<NodeId>> {
        let topology = self.state.lock().unwrap().clone();
        let membership = self.membership.lock().unwrap().clone();
        let last_log = *self.last_log.lock().unwrap();

        let snap = StateMachineSnapshot {
            meta: membership.clone(),
            topology,
        };
        let data = serde_json::to_vec(&snap).map_err(|e| snap_write_err(&e))?;

        let snapshot_id = match &last_log {
            Some(lid) => format!("snapshot-{}-{}", lid.leader_id, lid.index),
            None => "snapshot-empty".to_string(),
        };

        let meta = SnapshotMeta {
            last_log_id: last_log,
            last_membership: membership,
            snapshot_id,
        };

        Ok(openraft::storage::Snapshot {
            meta,
            snapshot: Box::new(std::io::Cursor::new(data)),
        })
    }

    /// Begin receiving an incoming snapshot (returns an empty write buffer).
    pub async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    /// Install a snapshot received from the leader.
    pub async fn install_snapshot_from(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let snap: StateMachineSnapshot =
            serde_json::from_slice(&data).map_err(|e| snap_read_err(&e))?;

        *self.state.lock().unwrap() = snap.topology.clone();
        *self.membership.lock().unwrap() = snap.meta;
        *self.last_log.lock().unwrap() = meta.last_log_id;

        let _ = self.change_tx.send(Arc::new(snap.topology));
        Ok(())
    }

    /// Return the current snapshot if one exists.
    pub async fn get_current_snapshot_data(
        &mut self,
    ) -> Result<Option<openraft::storage::Snapshot<RaftTypeConfig>>, StorageError<NodeId>> {
        let last_log = *self.last_log.lock().unwrap();
        let Some(last_log_id) = last_log else {
            return Ok(None);
        };

        let topology = self.state.lock().unwrap().clone();
        let membership = self.membership.lock().unwrap().clone();

        let snap = StateMachineSnapshot {
            meta: membership.clone(),
            topology,
        };
        let data = serde_json::to_vec(&snap).map_err(|e| snap_write_err(&e))?;
        let snapshot_id = format!("snapshot-{}-{}", last_log_id.leader_id, last_log_id.index);

        let meta = SnapshotMeta {
            last_log_id: Some(last_log_id),
            last_membership: membership,
            snapshot_id,
        };

        Ok(Some(openraft::storage::Snapshot {
            meta,
            snapshot: Box::new(std::io::Cursor::new(data)),
        }))
    }

    // ── Command application ───────────────────────────────────────────────────

    pub(crate) fn apply_command(topology: &mut ClusterTopology, cmd: &TopologyCommand) {
        use crate::raft::topology::NodeRole;

        topology.version += 1;
        topology.last_changed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        match cmd {
            TopologyCommand::SetPrimary {
                node_id,
                at_lsn,
                new_timeline: _,
            } => {
                if !topology.primary_node_id.is_empty() && &topology.primary_node_id != node_id {
                    topology
                        .node_roles
                        .insert(topology.primary_node_id.clone(), NodeRole::Replica);
                }
                topology.primary_node_id = node_id.clone();
                topology
                    .node_roles
                    .insert(node_id.clone(), NodeRole::Primary);
                topology.last_flush_lsns.insert(node_id.clone(), *at_lsn);
            }

            TopologyCommand::MarkOffline { node_id } => {
                topology
                    .node_roles
                    .insert(node_id.clone(), NodeRole::Offline);
            }

            TopologyCommand::MarkReplica { node_id, flush_lsn } => {
                topology
                    .node_roles
                    .insert(node_id.clone(), NodeRole::Replica);
                topology.last_flush_lsns.insert(node_id.clone(), *flush_lsn);
            }

            TopologyCommand::MarkMaintenance { node_id } => {
                topology
                    .node_roles
                    .insert(node_id.clone(), NodeRole::Maintenance);
            }

            TopologyCommand::UpdatePrimaryConninfo { node_id, conninfo } => {
                topology
                    .primary_conninfos
                    .insert(node_id.clone(), conninfo.clone());
            }

            TopologyCommand::UpdateFlushLsn {
                node_id,
                flush_lsn,
                replay_lsn,
            } => {
                topology.last_flush_lsns.insert(node_id.clone(), *flush_lsn);
                topology
                    .last_replay_lsns
                    .insert(node_id.clone(), *replay_lsn);
                if !topology.primary_node_id.is_empty() && node_id != &topology.primary_node_id {
                    let primary_lsn = topology
                        .last_flush_lsns
                        .get(&topology.primary_node_id)
                        .copied()
                        .unwrap_or(0);
                    topology
                        .replica_lag_bytes
                        .insert(node_id.clone(), primary_lsn.saturating_sub(*flush_lsn));
                }
            }

            TopologyCommand::AddNode(node_cfg) => {
                topology
                    .node_roles
                    .entry(node_cfg.node_id.clone())
                    .or_insert(NodeRole::Unknown);
                topology
                    .node_configs
                    .insert(node_cfg.node_id.clone(), node_cfg.clone());
            }

            TopologyCommand::RemoveNode { node_id } => {
                topology.node_roles.remove(node_id);
                topology.node_configs.remove(node_id);
                topology.last_flush_lsns.remove(node_id);
                topology.last_replay_lsns.remove(node_id);
                topology.replica_lag_bytes.remove(node_id);
            }

            TopologyCommand::RecordFailover {
                old_primary,
                new_primary,
                triggered_at,
                duration_ms,
                reason,
            } => {
                topology.record_failover(FailoverEvent {
                    old_primary: old_primary.clone(),
                    new_primary: new_primary.clone(),
                    triggered_at: *triggered_at,
                    duration_ms: *duration_ms,
                    reason: reason.clone(),
                });
            }

            TopologyCommand::SetReplicationSlot { node_id, slot_name } => {
                topology
                    .replica_slots
                    .insert(node_id.clone(), slot_name.clone());
            }

            TopologyCommand::AddBackupManifest(manifest) => {
                topology.backups.push(manifest.clone());
                topology.backups.sort_by(|a, b| b.completed_at.cmp(&a.completed_at));
                topology.backups.truncate(1_000);
            }

            TopologyCommand::RemoveBackupManifest { backup_id } => {
                topology.backups.retain(|b| &b.backup_id != backup_id);
            }
        }
    }
}

// ── RaftSnapshotBuilder ───────────────────────────────────────────────────────

// openraft 0.9 uses native async traits — no #[async_trait].
// TopologyStateMachine is used as PgClusterStorage::SnapshotBuilder via Clone.
impl openraft::RaftSnapshotBuilder<RaftTypeConfig> for TopologyStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<RaftTypeConfig>, StorageError<NodeId>> {
        self.build_snapshot_data().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{
        commands::TopologyCommand,
        topology::{NodeConfig, NodeRole},
    };
    use std::collections::HashMap;

    fn make_node_cfg(id: &str) -> crate::raft::topology::NodeConfig {
        NodeConfig {
            node_id: id.into(),
            agent_addr: format!("127.0.0.1:700{}", id.len()),
            postgres_addr: format!("127.0.0.1:543{}", id.len()),
            priority: 100,
            tags: HashMap::new(),
        }
    }

    fn apply(t: &mut ClusterTopology, cmd: TopologyCommand) {
        TopologyStateMachine::apply_command(t, &cmd);
    }

    #[test]
    fn apply_set_primary_updates_topology() {
        let mut t = ClusterTopology::default();
        apply(
            &mut t,
            TopologyCommand::SetPrimary {
                node_id: "pg1".into(),
                at_lsn: 100,
                new_timeline: 1,
            },
        );
        assert_eq!(t.primary_node_id, "pg1");
        assert_eq!(t.node_roles.get("pg1"), Some(&NodeRole::Primary));
        assert_eq!(t.last_flush_lsns.get("pg1"), Some(&100));
        assert_eq!(t.version, 1);
    }

    #[test]
    fn apply_mark_offline_changes_role() {
        let mut t = ClusterTopology::default();
        apply(
            &mut t,
            TopologyCommand::MarkReplica {
                node_id: "pg2".into(),
                flush_lsn: 50,
            },
        );
        apply(
            &mut t,
            TopologyCommand::MarkOffline {
                node_id: "pg2".into(),
            },
        );
        assert_eq!(t.node_roles.get("pg2"), Some(&NodeRole::Offline));
    }

    #[test]
    fn apply_mark_replica_tracks_lsn() {
        let mut t = ClusterTopology::default();
        apply(
            &mut t,
            TopologyCommand::MarkReplica {
                node_id: "pg3".into(),
                flush_lsn: 999,
            },
        );
        assert_eq!(t.node_roles.get("pg3"), Some(&NodeRole::Replica));
        assert_eq!(t.last_flush_lsns.get("pg3"), Some(&999));
    }

    #[test]
    fn apply_update_flush_lsn() {
        let mut t = ClusterTopology::default();
        apply(
            &mut t,
            TopologyCommand::SetPrimary {
                node_id: "pg1".into(),
                at_lsn: 1000,
                new_timeline: 1,
            },
        );
        apply(
            &mut t,
            TopologyCommand::UpdateFlushLsn {
                node_id: "pg2".into(),
                flush_lsn: 900,
                replay_lsn: 890,
            },
        );
        assert_eq!(t.last_flush_lsns.get("pg2"), Some(&900));
        assert_eq!(t.last_replay_lsns.get("pg2"), Some(&890));
        assert_eq!(t.replica_lag_bytes.get("pg2"), Some(&100u64));
    }

    #[test]
    fn apply_add_then_remove_node() {
        let mut t = ClusterTopology::default();
        apply(&mut t, TopologyCommand::AddNode(make_node_cfg("pg4")));
        assert!(t.node_configs.contains_key("pg4"));
        apply(
            &mut t,
            TopologyCommand::RemoveNode {
                node_id: "pg4".into(),
            },
        );
        assert!(!t.node_configs.contains_key("pg4"));
        assert!(!t.node_roles.contains_key("pg4"));
    }

    #[test]
    fn apply_sequence_of_commands_is_idempotent() {
        let mut t = ClusterTopology::default();
        for _ in 0..3 {
            apply(
                &mut t,
                TopologyCommand::SetPrimary {
                    node_id: "pg1".into(),
                    at_lsn: 100,
                    new_timeline: 1,
                },
            );
        }
        assert_eq!(t.primary_node_id, "pg1");
        let primaries: Vec<_> = t
            .node_roles
            .iter()
            .filter(|(_, r)| **r == NodeRole::Primary)
            .collect();
        assert_eq!(primaries.len(), 1, "expected exactly one primary");
    }
}
