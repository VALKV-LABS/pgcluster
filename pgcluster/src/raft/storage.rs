use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::{
    storage::{RaftLogReader, RaftStorage, Snapshot},
    AnyError, ErrorSubject, ErrorVerb, LogId, LogState, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use sled::Db;

use super::state_machine::TopologyStateMachine;
use crate::raft::types::{NodeId, RaftTypeConfig, TopologyResponse};

// ── helpers ───────────────────────────────────────────────────────────────────

fn io_err_read<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(e)),
    }
}

fn io_err_write<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(e)),
    }
}

// ── SledLogStorage ────────────────────────────────────────────────────────────

/// Raft log storage backed by [`sled`], an embedded key-value store.
///
/// Layout inside the sled database:
///
/// * Tree `"raft_logs"` — key: 8-byte big-endian log index → value: JSON
///   serialised `openraft::Entry<RaftTypeConfig>`.
/// * Tree `"raft_meta"` — small key-value pairs:
///   - `"vote"`   → JSON `Vote<NodeId>`
///   - `"purged"` → JSON `LogId<NodeId>` (highest purged entry)
#[derive(Clone)]
pub struct SledLogStorage {
    db: Arc<Db>,
}

// The openraft RaftStorage trait dictates `Result<_, StorageError<NodeId>>` — we cannot
// reduce the size of openraft's error type, so allow this lint for the whole impl.
#[allow(clippy::result_large_err)]
impl SledLogStorage {
    /// Open (or create) the sled database at `path`.
    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db: Arc::new(db) })
    }

    const LOGS_TREE: &'static str = "raft_logs";
    const META_TREE: &'static str = "raft_meta";
    const VOTE_KEY: &'static [u8] = b"vote";
    const PURGED_KEY: &'static [u8] = b"purged";

    fn logs_tree(&self) -> Result<sled::Tree, StorageError<NodeId>> {
        self.db
            .open_tree(Self::LOGS_TREE)
            .map_err(|e| io_err_read(&e))
    }

    fn meta_tree(&self) -> Result<sled::Tree, StorageError<NodeId>> {
        self.db
            .open_tree(Self::META_TREE)
            .map_err(|e| io_err_read(&e))
    }

    fn idx_key(index: u64) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }

    fn decode_entry(raw: &[u8]) -> Result<openraft::Entry<RaftTypeConfig>, StorageError<NodeId>> {
        serde_json::from_slice(raw).map_err(|e| io_err_read(&e))
    }

    fn encode_entry(
        entry: &openraft::Entry<RaftTypeConfig>,
    ) -> Result<Vec<u8>, StorageError<NodeId>> {
        serde_json::to_vec(entry).map_err(|e| io_err_write(&e))
    }

    fn decode_vote(raw: &[u8]) -> Result<Vote<NodeId>, StorageError<NodeId>> {
        serde_json::from_slice(raw).map_err(|e| io_err_read(&e))
    }

    fn encode_vote(vote: &Vote<NodeId>) -> Result<Vec<u8>, StorageError<NodeId>> {
        serde_json::to_vec(vote).map_err(|e| io_err_write(&e))
    }

    fn decode_log_id(raw: &[u8]) -> Result<LogId<NodeId>, StorageError<NodeId>> {
        serde_json::from_slice(raw).map_err(|e| io_err_read(&e))
    }

    fn encode_log_id(lid: &LogId<NodeId>) -> Result<Vec<u8>, StorageError<NodeId>> {
        serde_json::to_vec(lid).map_err(|e| io_err_write(&e))
    }

    fn to_key_bound(b: Bound<&u64>) -> Bound<Vec<u8>> {
        match b {
            Bound::Included(&n) => Bound::Included(Self::idx_key(n)),
            Bound::Excluded(&n) => Bound::Excluded(Self::idx_key(n)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    // ── public log operation helpers (called by PgClusterStorage) ─────────────

    /// Scan log entries within `range`.  Named `get_entries_range` to avoid
    /// colliding with the `RaftLogReader::try_get_log_entries` trait method.
    pub async fn get_entries_range<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<RaftTypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Send,
    {
        let tree = self.logs_tree()?;
        let start = Self::to_key_bound(range.start_bound());
        let end = Self::to_key_bound(range.end_bound());

        let mut entries = Vec::new();
        for item in tree.range((start, end)) {
            let (_, v) = item.map_err(|e| io_err_read(&e))?;
            entries.push(Self::decode_entry(&v)?);
        }
        Ok(entries)
    }

    pub async fn get_log_state(
        &mut self,
    ) -> Result<LogState<RaftTypeConfig>, StorageError<NodeId>> {
        let meta = self.meta_tree()?;
        let logs = self.logs_tree()?;

        let last_purged_log_id = meta
            .get(Self::PURGED_KEY)
            .map_err(|e| io_err_read(&e))?
            .map(|v| Self::decode_log_id(&v))
            .transpose()?;

        let last_log_id = logs
            .last()
            .map_err(|e| io_err_read(&e))?
            .map(|(_, v)| Self::decode_entry(&v))
            .transpose()?
            .map(|e| e.log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    pub async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let meta = self.meta_tree()?;
        let bytes = Self::encode_vote(vote)?;
        meta.insert(Self::VOTE_KEY, bytes.as_slice())
            .map_err(|e| io_err_write(&e))?;
        meta.flush().map_err(|e| io_err_write(&e))?;
        Ok(())
    }

    pub async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let meta = self.meta_tree()?;
        meta.get(Self::VOTE_KEY)
            .map_err(|e| io_err_read(&e))?
            .map(|v| Self::decode_vote(&v))
            .transpose()
    }

    pub async fn append_entries<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<RaftTypeConfig>> + Send,
    {
        let tree = self.logs_tree()?;
        let mut batch = sled::Batch::default();

        for entry in entries {
            let key = Self::idx_key(entry.log_id.index);
            let value = Self::encode_entry(&entry)?;
            batch.insert(key.as_slice(), value.as_slice());
        }

        tree.apply_batch(batch).map_err(|e| io_err_write(&e))?;
        tree.flush().map_err(|e| io_err_write(&e))?;
        Ok(())
    }

    pub async fn truncate_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let tree = self.logs_tree()?;
        let start = Self::idx_key(log_id.index);
        let keys: Vec<sled::IVec> = tree
            .range(start.as_slice()..)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();

        for key in keys {
            tree.remove(key).map_err(|e| io_err_write(&e))?;
        }
        tree.flush().map_err(|e| io_err_write(&e))?;
        Ok(())
    }

    pub async fn purge_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let tree = self.logs_tree()?;
        let meta = self.meta_tree()?;

        let end = Self::idx_key(log_id.index);
        let keys: Vec<sled::IVec> = tree
            .range(..=end.as_slice())
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();

        for key in keys {
            tree.remove(key).map_err(|e| io_err_write(&e))?;
        }

        let purge_bytes = Self::encode_log_id(&log_id)?;
        meta.insert(Self::PURGED_KEY, purge_bytes.as_slice())
            .map_err(|e| io_err_write(&e))?;

        tree.flush().map_err(|e| io_err_write(&e))?;
        meta.flush().map_err(|e| io_err_write(&e))?;
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
impl RaftLogReader<RaftTypeConfig> for SledLogStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<RaftTypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Send,
    {
        self.get_entries_range(range).await
    }
}

// ── PgClusterStorage ──────────────────────────────────────────────────────────

/// Combined Raft storage: log via [`SledLogStorage`] + state machine via
/// [`TopologyStateMachine`].
///
/// Implements the openraft 0.9 v1 `RaftStorage` trait, which is then wrapped
/// with `openraft::storage::Adaptor::new(storage)` to produce the sealed v2
/// `(RaftLogStorage, RaftStateMachine)` pair that `Raft::new` requires.
pub struct PgClusterStorage {
    pub log: SledLogStorage,
    pub sm: TopologyStateMachine,
}

impl PgClusterStorage {
    pub fn new(log: SledLogStorage, sm: TopologyStateMachine) -> Self {
        Self { log, sm }
    }
}

#[allow(clippy::result_large_err)]
impl RaftLogReader<RaftTypeConfig> for PgClusterStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<RaftTypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Send,
    {
        self.log.get_entries_range(range).await
    }
}

#[allow(clippy::result_large_err)]
impl RaftStorage<RaftTypeConfig> for PgClusterStorage {
    // SledLogStorage is the log reader returned by get_log_reader().
    type LogReader = SledLogStorage;
    // TopologyStateMachine is the snapshot builder returned by get_snapshot_builder().
    type SnapshotBuilder = TopologyStateMachine;

    async fn get_log_state(&mut self) -> Result<LogState<RaftTypeConfig>, StorageError<NodeId>> {
        self.log.get_log_state().await
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log.save_vote(vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.log.read_vote().await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        self.sm.applied_state().await
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.log.truncate_since(log_id).await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log.purge_upto(log_id).await
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<RaftTypeConfig>> + Send,
    {
        self.log.append_entries(entries).await
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[openraft::Entry<RaftTypeConfig>],
    ) -> Result<Vec<TopologyResponse>, StorageError<NodeId>> {
        self.sm.apply_entries(entries).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.log.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.sm.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        self.sm.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.sm.install_snapshot_from(meta, snapshot).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftTypeConfig>>, StorageError<NodeId>> {
        self.sm.get_current_snapshot_data().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{state_machine::StateMachineSnapshot, topology::ClusterTopology};
    use openraft::Vote;

    #[test]
    fn snapshot_roundtrip() {
        let snap = StateMachineSnapshot {
            meta: openraft::StoredMembership::default(),
            topology: ClusterTopology {
                primary_node_id: "pg1".into(),
                version: 5,
                ..Default::default()
            },
        };
        let encoded = serde_json::to_vec(&snap).expect("serialize");
        let decoded: StateMachineSnapshot = serde_json::from_slice(&encoded).expect("deserialize");
        assert_eq!(decoded.topology.primary_node_id, "pg1");
        assert_eq!(decoded.topology.version, 5);
    }

    #[tokio::test]
    async fn log_entry_survives_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let vote = Vote::<NodeId> {
            leader_id: openraft::LeaderId {
                term: 2,
                node_id: 1,
            },
            committed: false,
        };

        {
            let mut storage = SledLogStorage::open(dir.path()).expect("open");
            storage.save_vote(&vote).await.expect("save_vote");
        }

        {
            let mut storage = SledLogStorage::open(dir.path()).expect("reopen");
            let read_back = storage.read_vote().await.expect("read_vote");
            assert_eq!(read_back, Some(vote), "vote should survive restart");
        }
    }
}
