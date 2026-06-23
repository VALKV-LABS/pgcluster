//! Per-connection session state.
//!
//! Tracks transaction status, session variables, and routing hints.

use std::collections::HashMap;

use super::protocol::StatementIntent;

// ── TxnState ─────────────────────────────────────────────────────────────────

/// The current transaction state of a client connection, mirroring the
/// `ReadyForQuery` status byte sent by the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnState {
    /// No open transaction — free to route to any backend.
    Idle,
    /// Inside an open transaction block — must stay on the same backend.
    InTransaction,
    /// Inside a failed transaction block (error occurred) — must stay on
    /// the same backend until `ROLLBACK`.
    Failed,
}

// ── SessionState ──────────────────────────────────────────────────────────────

/// All per-connection state that influences routing and protocol behaviour.
#[allow(dead_code)]
#[derive(Debug)]
pub struct SessionState {
    /// Current transaction state (updated from each ReadyForQuery 'Z' message).
    pub txn_state: TxnState,

    /// Whether the current transaction was opened in read-only mode
    /// (`BEGIN READ ONLY` or `SET TRANSACTION READ ONLY`).
    pub is_read_only: bool,

    /// Database name from the startup message.
    pub database: String,

    /// User name from the startup message.
    pub user: String,

    /// `application_name` from the startup message (may be empty).
    pub application_name: String,

    /// Session-level SET variables seen since connection start.
    ///
    /// These are forwarded to the backend but also tracked here so that when
    /// a backend connection is returned to the pool and later reassigned, we
    /// can replay the SET commands.
    pub set_vars: HashMap<String, String>,

    /// Shard key supplied by the client via `SET pgcluster.shard_key = '...'`.
    ///
    /// Reserved for the M5-A coordinator; `None` in regular proxy mode.
    pub shard_key: Option<String>,

    /// The node ID of the backend this session is currently pinned to.
    /// `None` while in `Idle` state (no active backend).
    pub pinned_backend: Option<String>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            txn_state: TxnState::Idle,
            is_read_only: false,
            database: String::new(),
            user: String::new(),
            application_name: String::new(),
            set_vars: HashMap::new(),
            shard_key: None,
            pinned_backend: None,
        }
    }
}

impl SessionState {
    /// Update transaction state from the status byte in a `ReadyForQuery` ('Z') message.
    ///
    /// `b'I'` → `Idle`, `b'T'` → `InTransaction`, `b'E'` → `Failed`.
    ///
    /// When the session returns to Idle the backend pin is released.
    pub fn update_from_ready_for_query(&mut self, status_byte: u8) {
        match status_byte {
            b'I' => {
                self.txn_state = TxnState::Idle;
                self.is_read_only = false;
                self.pinned_backend = None;
            }
            b'T' => {
                self.txn_state = TxnState::InTransaction;
            }
            b'E' => {
                self.txn_state = TxnState::Failed;
            }
            _ => {
                tracing::warn!(
                    byte = status_byte,
                    "unknown ReadyForQuery status byte; treating as Idle"
                );
                self.txn_state = TxnState::Idle;
                self.pinned_backend = None;
            }
        }
    }

    /// Returns `true` if the next statement must be sent to the primary.
    ///
    /// This is the case when:
    /// - We are inside a write transaction (not read-only).
    /// - We are in a failed transaction block (must ROLLBACK on primary).
    /// - The current statement intent is `Write` or `SetLocal`.
    ///
    /// Note: this does not take the *current* statement intent into account —
    /// that is handled by the router. This method only reflects sticky-routing
    /// requirements from the session state.
    pub fn needs_primary(&self) -> bool {
        match self.txn_state {
            TxnState::Idle => false,
            TxnState::InTransaction => !self.is_read_only,
            TxnState::Failed => true,
        }
    }

    /// Determine whether a statement with the given intent should go to the
    /// primary, updating session state as appropriate.
    pub fn route_intent(&mut self, intent: &StatementIntent) -> RouteTarget {
        match intent {
            StatementIntent::Write => {
                // Write statements always go to the primary.
                self.is_read_only = false;
                RouteTarget::Primary
            }
            StatementIntent::SetLocal => {
                // SET commands are session-affine; route to primary to be safe.
                RouteTarget::Primary
            }
            StatementIntent::Read => {
                if self.needs_primary() {
                    // Inside a write transaction: stay on primary.
                    RouteTarget::Primary
                } else {
                    RouteTarget::ReplicaOrPrimary
                }
            }
        }
    }
}

/// Where to send the next statement.
#[derive(Debug, PartialEq, Eq)]
pub enum RouteTarget {
    /// Must go to the current primary.
    Primary,
    /// Can go to a replica; fall back to primary if no replica available.
    ReplicaOrPrimary,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_state_transitions() {
        let mut s = SessionState::default();
        assert_eq!(s.txn_state, TxnState::Idle);

        // Client opens a transaction
        s.update_from_ready_for_query(b'T');
        assert_eq!(s.txn_state, TxnState::InTransaction);

        // Transaction commits/rolls back
        s.update_from_ready_for_query(b'I');
        assert_eq!(s.txn_state, TxnState::Idle);
    }

    #[test]
    fn txn_state_failed_transition() {
        let mut s = SessionState::default();
        s.update_from_ready_for_query(b'T');
        s.update_from_ready_for_query(b'E');
        assert_eq!(s.txn_state, TxnState::Failed);
        // After rollback
        s.update_from_ready_for_query(b'I');
        assert_eq!(s.txn_state, TxnState::Idle);
    }

    #[test]
    fn sticky_routing_while_in_txn() {
        let mut s = SessionState::default();

        // Before any transaction: a read can go to replica
        let target = s.route_intent(&StatementIntent::Read);
        assert_eq!(target, RouteTarget::ReplicaOrPrimary);

        // Open a write transaction (backend returns 'T')
        s.update_from_ready_for_query(b'T');

        // Now a read inside a write txn must stay on primary
        let target = s.route_intent(&StatementIntent::Read);
        assert_eq!(target, RouteTarget::Primary);
    }

    #[test]
    fn needs_primary_for_write_intent() {
        let mut s = SessionState::default();
        // Idle + read-only == no primary needed
        assert!(!s.needs_primary());

        // Begin write transaction
        s.update_from_ready_for_query(b'T');
        s.is_read_only = false;
        assert!(s.needs_primary());

        // Begin read-only transaction
        s.is_read_only = true;
        assert!(!s.needs_primary());
    }

    #[test]
    fn needs_primary_when_failed() {
        let mut s = SessionState::default();
        s.update_from_ready_for_query(b'E');
        assert!(s.needs_primary());
    }

    #[test]
    fn backend_pin_cleared_on_idle() {
        let mut s = SessionState::default();
        s.pinned_backend = Some("pg1".to_string());
        s.update_from_ready_for_query(b'I');
        assert!(s.pinned_backend.is_none());
    }

    #[test]
    fn txn_state_transitions_commit() {
        let mut s = SessionState::default();
        s.update_from_ready_for_query(b'T');
        assert_eq!(s.txn_state, TxnState::InTransaction);
        s.update_from_ready_for_query(b'I'); // commit → Idle
        assert_eq!(s.txn_state, TxnState::Idle);
    }

    #[test]
    fn txn_state_transitions_rollback() {
        let mut s = SessionState::default();
        s.update_from_ready_for_query(b'T');
        s.update_from_ready_for_query(b'E'); // error
        s.update_from_ready_for_query(b'I'); // rollback → Idle
        assert_eq!(s.txn_state, TxnState::Idle);
    }

    #[test]
    fn txn_state_stays_in_failed() {
        let mut s = SessionState::default();
        s.update_from_ready_for_query(b'E');
        assert_eq!(s.txn_state, TxnState::Failed);
        // Another statement while failed — still Failed until rollback
        s.update_from_ready_for_query(b'E');
        assert_eq!(s.txn_state, TxnState::Failed);
    }
}
