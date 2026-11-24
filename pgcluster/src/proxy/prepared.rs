//! Named prepared statements across pooled backends.
//!
//! The proxy hands a backend back to the pool when a transaction ends, so a
//! statement a client prepared may live on a backend the client no longer
//! holds. As PgBouncer does, the proxy keeps each client's statements itself.
//! On the backend it names each statement after a hash of its text and
//! parameter types, so clients that prepare the same query share it. It sends
//! `Parse` again to a backend that lacks a statement, and answers a `Parse` or
//! `Close` it did not need to send. Replies are matched to requests in order,
//! so the client sees exactly the replies it expects, in the order it expects
//! them.

use std::collections::{HashMap, VecDeque};

use sha2::{Digest, Sha256};

use super::protocol::FrontendMessage;

// ── Backend side ──────────────────────────────────────────────────────────────

/// Statements prepared on one backend connection, by backend name.
#[derive(Debug, Default)]
pub struct BackendStatements {
    /// Backend name → `clock` at its last use.
    last_used: HashMap<String, u64>,
    clock: u64,
    /// `clock` at the last `ReadyForQuery`. Statements used after it belong
    /// to the requests in flight, and closing one would break them.
    pipeline_start: u64,
}

impl BackendStatements {
    fn contains(&self, name: &str) -> bool {
        self.last_used.contains_key(name)
    }

    fn touch(&mut self, name: &str) {
        self.clock += 1;
        self.last_used.insert(name.to_owned(), self.clock);
    }

    fn remove(&mut self, name: &str) {
        self.last_used.remove(name);
    }

    fn end_pipeline(&mut self) {
        self.pipeline_start = self.clock;
    }

    /// The least recently used statement that no request in flight uses.
    fn evictable(&self) -> Option<String> {
        self.last_used
            .iter()
            .filter(|(_, &used)| used <= self.pipeline_start)
            .min_by_key(|(_, &used)| used)
            .map(|(name, _)| name.clone())
    }

    pub fn len(&self) -> usize {
        self.last_used.len()
    }

    pub fn is_empty(&self) -> bool {
        self.last_used.is_empty()
    }
}

// ── Client side ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Statement {
    backend_name: String,
    /// The client's `Parse`, renamed for the backend.
    parse: Vec<u8>,
}

/// A reply the proxy waits for, in the order it sent the requests.
#[derive(Debug)]
enum Expect {
    /// A client request whose reply ends with one of these message types.
    Reply(&'static [u8]),
    /// A `Parse` under a backend name. `client_name` is set when the client
    /// sent it; otherwise the proxy did, and its `ParseComplete` is dropped.
    Parse {
        backend_name: String,
        client_name: Option<String>,
    },
    /// A `Close` the proxy sent to make room. Its reply is dropped.
    Evict { backend_name: String },
    /// A reply the proxy writes itself, in its place in the order.
    Synthetic(u8),
    /// `Sync`: ends at `ReadyForQuery`, and an error skips to it.
    Sync,
    /// A simple query: ends at `ReadyForQuery`. `discard` marks `DISCARD ALL`.
    Query { discard: bool, failed: bool },
}

/// One client's prepared statements, and the replies its backend owes.
#[derive(Debug)]
pub struct ClientStatements {
    /// Statements kept on one backend. `0` turns tracking off, and names pass
    /// through to the backend unchanged.
    limit: usize,
    by_name: HashMap<String, Statement>,
    expected: VecDeque<Expect>,
}

impl ClientStatements {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            by_name: HashMap::new(),
            expected: VecDeque::new(),
        }
    }

    fn enabled(&self) -> bool {
        self.limit > 0
    }

    /// Translate one extended-protocol message for a backend holding
    /// `backend`. Returns the bytes to send, which may be none.
    pub fn outgoing(
        &mut self,
        msg: &FrontendMessage,
        raw: &[u8],
        backend: &mut BackendStatements,
    ) -> Vec<u8> {
        match msg {
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } if self.enabled() && !name.is_empty() => {
                let backend_name = backend_name(query, param_types);
                let parse = encode_parse(&backend_name, query, param_types);
                self.by_name.insert(
                    name.clone(),
                    Statement {
                        backend_name: backend_name.clone(),
                        parse: parse.clone(),
                    },
                );
                if backend.contains(&backend_name) {
                    backend.touch(&backend_name);
                    self.expected.push_back(Expect::Synthetic(b'1'));
                    return Vec::new();
                }
                let mut out = self.make_room(backend);
                backend.touch(&backend_name);
                self.expected.push_back(Expect::Parse {
                    backend_name,
                    client_name: Some(name.clone()),
                });
                out.extend_from_slice(&parse);
                out
            }
            FrontendMessage::Parse { .. } => {
                self.expected.push_back(Expect::Reply(b"1"));
                raw.to_vec()
            }
            FrontendMessage::Bind { statement, .. } => {
                let (mut out, renamed) = self.ensure(statement, backend);
                self.expected.push_back(Expect::Reply(b"2"));
                match renamed {
                    Some(name) => out.extend_from_slice(&rename_bind(raw, &name)),
                    None => out.extend_from_slice(raw),
                }
                out
            }
            FrontendMessage::Describe { kind: b'S', name } => {
                let (mut out, renamed) = self.ensure(name, backend);
                self.expected.push_back(Expect::Reply(b"Tn"));
                match renamed {
                    Some(name) => out.extend_from_slice(&encode_named(b'D', b'S', &name)),
                    None => out.extend_from_slice(raw),
                }
                out
            }
            FrontendMessage::Describe { .. } => {
                self.expected.push_back(Expect::Reply(b"Tn"));
                raw.to_vec()
            }
            FrontendMessage::Execute { .. } => {
                self.expected.push_back(Expect::Reply(b"CIs"));
                raw.to_vec()
            }
            FrontendMessage::Close { kind: b'S', name }
                if self.enabled() && self.by_name.remove(name).is_some() =>
            {
                // The statement stays on the backend for the next client.
                self.expected.push_back(Expect::Synthetic(b'3'));
                Vec::new()
            }
            FrontendMessage::Close { .. } => {
                self.expected.push_back(Expect::Reply(b"3"));
                raw.to_vec()
            }
            FrontendMessage::Sync => {
                self.expected.push_back(Expect::Sync);
                raw.to_vec()
            }
            _ => raw.to_vec(),
        }
    }

    /// Note a simple query about to go to the backend.
    pub fn query(&mut self, sql: &str) {
        self.expected.push_back(Expect::Query {
            discard: self.enabled() && is_discard_all(sql),
            failed: false,
        });
    }

    /// The command tag for a simple query the proxy answers itself, having
    /// applied it. `DEALLOCATE` of a statement the proxy keeps only forgets
    /// it for this client, because the backend's copy serves others.
    pub fn answer_locally(&mut self, sql: &str) -> Option<&'static str> {
        if !self.enabled() || !self.expected.is_empty() {
            return None;
        }
        match parse_deallocate(sql)? {
            Deallocate::All => {
                self.by_name.clear();
                Some("DEALLOCATE ALL")
            }
            Deallocate::Name(name) => self.by_name.remove(&name).map(|_| "DEALLOCATE"),
        }
    }

    /// The next reply the proxy writes itself, if its turn has come.
    pub fn next_synthetic(&mut self) -> Option<u8> {
        match self.expected.front() {
            Some(Expect::Synthetic(kind)) => {
                let kind = *kind;
                self.expected.pop_front();
                Some(kind)
            }
            _ => None,
        }
    }

    /// Account for one backend message of type `kind`. Returns whether it
    /// goes on to the client.
    pub fn on_reply(&mut self, kind: u8, backend: &mut BackendStatements) -> bool {
        match kind {
            b'E' => {
                if let Some(Expect::Query { failed, .. }) = self.expected.front_mut() {
                    *failed = true;
                    return true;
                }
                // The backend skips everything up to the next Sync.
                while let Some(front) = self.expected.front() {
                    if matches!(front, Expect::Sync | Expect::Query { .. }) {
                        break;
                    }
                    let skipped = self.expected.pop_front().expect("front exists");
                    self.undo(skipped, backend);
                }
                true
            }
            b'Z' => {
                while let Some(front) = self.expected.pop_front() {
                    match front {
                        Expect::Sync => break,
                        Expect::Query { discard, failed } => {
                            if discard && !failed {
                                backend.last_used.clear();
                                self.by_name.clear();
                            }
                            break;
                        }
                        other => self.undo(other, backend),
                    }
                }
                backend.end_pipeline();
                true
            }
            _ => match self.expected.front() {
                Some(Expect::Reply(ends)) if ends.contains(&kind) => {
                    self.expected.pop_front();
                    true
                }
                Some(Expect::Parse { client_name, .. }) if kind == b'1' => {
                    let forward = client_name.is_some();
                    self.expected.pop_front();
                    forward
                }
                Some(Expect::Evict { .. }) if kind == b'3' => {
                    self.expected.pop_front();
                    false
                }
                _ => true,
            },
        }
    }

    /// Reverse what was assumed when a request was sent that the backend
    /// then skipped or refused.
    fn undo(&mut self, expect: Expect, backend: &mut BackendStatements) {
        match expect {
            Expect::Parse {
                backend_name,
                client_name,
            } => {
                backend.remove(&backend_name);
                if let Some(name) = client_name {
                    self.by_name.remove(&name);
                }
            }
            Expect::Evict { backend_name } => backend.touch(&backend_name),
            _ => {}
        }
    }

    /// Make sure the backend holds the client's statement `name`. Returns the
    /// bytes to send first, and the backend name when the statement is one
    /// the proxy keeps.
    fn ensure(&mut self, name: &str, backend: &mut BackendStatements) -> (Vec<u8>, Option<String>) {
        if !self.enabled() || name.is_empty() {
            return (Vec::new(), None);
        }
        let Some(statement) = self.by_name.get(name).cloned() else {
            return (Vec::new(), None);
        };
        if backend.contains(&statement.backend_name) {
            backend.touch(&statement.backend_name);
            return (Vec::new(), Some(statement.backend_name));
        }
        let mut out = self.make_room(backend);
        backend.touch(&statement.backend_name);
        self.expected.push_back(Expect::Parse {
            backend_name: statement.backend_name.clone(),
            client_name: None,
        });
        out.extend_from_slice(&statement.parse);
        (out, Some(statement.backend_name))
    }

    /// Close least recently used statements until there is room for one more.
    fn make_room(&mut self, backend: &mut BackendStatements) -> Vec<u8> {
        let mut out = Vec::new();
        while backend.len() >= self.limit {
            let Some(name) = backend.evictable() else {
                break;
            };
            backend.remove(&name);
            out.extend_from_slice(&encode_named(b'C', b'S', &name));
            self.expected
                .push_back(Expect::Evict { backend_name: name });
        }
        out
    }
}

// ── Simple-query statements ───────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
enum Deallocate {
    All,
    Name(String),
}

/// Words of a single statement, without a trailing semicolon.
fn words(sql: &str) -> Vec<&str> {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect()
}

fn is_discard_all(sql: &str) -> bool {
    let words = words(sql);
    words.len() == 2
        && words[0].eq_ignore_ascii_case("DISCARD")
        && words[1].eq_ignore_ascii_case("ALL")
}

/// `DEALLOCATE [PREPARE] { name | ALL }`, with SQL's identifier rules.
fn parse_deallocate(sql: &str) -> Option<Deallocate> {
    let words = words(sql);
    let rest = match words.as_slice() {
        [first, rest @ ..] if first.eq_ignore_ascii_case("DEALLOCATE") => rest,
        _ => return None,
    };
    let target = match rest {
        [prepare, target] if prepare.eq_ignore_ascii_case("PREPARE") => *target,
        [target] => *target,
        _ => return None,
    };
    if target.eq_ignore_ascii_case("ALL") {
        return Some(Deallocate::All);
    }
    if let Some(quoted) = target.strip_prefix('"').and_then(|t| t.strip_suffix('"')) {
        return Some(Deallocate::Name(quoted.replace("\"\"", "\"")));
    }
    Some(Deallocate::Name(target.to_lowercase()))
}

// ── Encoding ──────────────────────────────────────────────────────────────────

/// The backend name for a query: the same text and types give the same name.
pub fn backend_name(query: &str, param_types: &[u32]) -> String {
    let mut hash = Sha256::new();
    hash.update(query.as_bytes());
    for oid in param_types {
        hash.update(oid.to_be_bytes());
    }
    let digest = hash.finalize();
    let hex: String = digest[..12].iter().map(|b| format!("{b:02x}")).collect();
    format!("pgcluster_{hex}")
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(5 + body.len());
    msg.push(kind);
    msg.extend_from_slice(&(4 + body.len() as u32).to_be_bytes());
    msg.extend_from_slice(body);
    msg
}

fn encode_parse(name: &str, query: &str, param_types: &[u32]) -> Vec<u8> {
    let mut body = Vec::with_capacity(name.len() + query.len() + 4 + 4 * param_types.len());
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(query.as_bytes());
    body.push(0);
    body.extend_from_slice(&(param_types.len() as u16).to_be_bytes());
    for oid in param_types {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    frame(b'P', &body)
}

/// A `Describe` or `Close` of statement or portal `name`.
fn encode_named(kind: u8, target: u8, name: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(name.len() + 2);
    body.push(target);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    frame(kind, &body)
}

/// A server message with no body, such as `ParseComplete`.
pub fn encode_empty(kind: u8) -> Vec<u8> {
    frame(kind, &[])
}

/// `raw` is a `Bind` that already parsed, so both names are terminated.
fn rename_bind(raw: &[u8], statement: &str) -> Vec<u8> {
    let payload = &raw[5..];
    let portal_end = payload.iter().position(|&b| b == 0).expect("portal name") + 1;
    let statement_end = portal_end
        + payload[portal_end..]
            .iter()
            .position(|&b| b == 0)
            .expect("statement name")
        + 1;
    let mut body = Vec::with_capacity(payload.len() + statement.len());
    body.extend_from_slice(&payload[..portal_end]);
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(&payload[statement_end..]);
    frame(b'B', &body)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(name: &str, query: &str) -> (FrontendMessage, Vec<u8>) {
        let msg = FrontendMessage::Parse {
            name: name.into(),
            query: query.into(),
            param_types: vec![],
        };
        (msg, encode_parse(name, query, &[]))
    }

    fn bind(statement: &str) -> (FrontendMessage, Vec<u8>) {
        // portal "", statement, no formats, no params, no result formats
        let mut body = vec![0];
        body.extend_from_slice(statement.as_bytes());
        body.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
        let msg = FrontendMessage::Bind {
            portal: String::new(),
            statement: statement.into(),
            params: vec![],
        };
        (msg, frame(b'B', &body))
    }

    fn sync() -> (FrontendMessage, Vec<u8>) {
        (FrontendMessage::Sync, frame(b'S', &[]))
    }

    fn send(
        client: &mut ClientStatements,
        backend: &mut BackendStatements,
        (msg, raw): (FrontendMessage, Vec<u8>),
    ) -> Vec<u8> {
        client.outgoing(&msg, &raw, backend)
    }

    /// Message types in a byte stream of framed messages.
    fn kinds(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < bytes.len() {
            out.push(bytes[pos]);
            let len = u32::from_be_bytes(bytes[pos + 1..pos + 5].try_into().unwrap()) as usize;
            pos += 1 + len;
        }
        out
    }

    /// Feed backend replies; return what reaches the client, synthetic
    /// replies included.
    fn replies(
        client: &mut ClientStatements,
        backend: &mut BackendStatements,
        from: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        for &kind in from {
            while let Some(k) = client.next_synthetic() {
                out.push(k);
            }
            if client.on_reply(kind, backend) {
                out.push(kind);
            }
        }
        while let Some(k) = client.next_synthetic() {
            out.push(k);
        }
        out
    }

    #[test]
    fn same_query_gets_same_backend_name() {
        assert_eq!(backend_name("SELECT 1", &[]), backend_name("SELECT 1", &[]));
        assert_ne!(
            backend_name("SELECT $1", &[23]),
            backend_name("SELECT $1", &[25])
        );
    }

    #[test]
    fn parse_is_renamed_for_the_backend() {
        let mut client = ClientStatements::new(10);
        let mut backend = BackendStatements::default();
        let sent = send(&mut client, &mut backend, parse("s1", "SELECT 1"));
        assert_eq!(
            sent,
            encode_parse(&backend_name("SELECT 1", &[]), "SELECT 1", &[])
        );
        assert!(backend.contains(&backend_name("SELECT 1", &[])));
    }

    #[test]
    fn statement_is_prepared_again_on_a_new_backend() {
        let mut client = ClientStatements::new(10);
        let mut first = BackendStatements::default();
        send(&mut client, &mut first, parse("s1", "SELECT 1"));
        send(&mut client, &mut first, sync());
        assert_eq!(replies(&mut client, &mut first, b"1Z"), b"1Z");

        // The client is now on another backend.
        let mut second = BackendStatements::default();
        let sent = send(&mut client, &mut second, bind("s1"));
        let name = backend_name("SELECT 1", &[]);
        assert_eq!(kinds(&sent), b"PB");
        assert!(sent.starts_with(&encode_parse(&name, "SELECT 1", &[])));
        send(&mut client, &mut second, sync());
        // The injected ParseComplete is dropped.
        assert_eq!(replies(&mut client, &mut second, b"12Z"), b"2Z");
    }

    #[test]
    fn parse_of_a_statement_the_backend_holds_is_answered_locally() {
        let mut backend = BackendStatements::default();
        let mut a = ClientStatements::new(10);
        send(&mut a, &mut backend, parse("x", "SELECT 1"));
        send(&mut a, &mut backend, sync());
        replies(&mut a, &mut backend, b"1Z");

        let mut b = ClientStatements::new(10);
        assert!(send(&mut b, &mut backend, parse("y", "SELECT 1")).is_empty());
        let sent = send(&mut b, &mut backend, bind("y"));
        assert_eq!(kinds(&sent), b"B");
        send(&mut b, &mut backend, sync());
        // The synthetic ParseComplete comes before the BindComplete.
        assert_eq!(replies(&mut b, &mut backend, b"2Z"), b"12Z");
    }

    #[test]
    fn synthetic_reply_waits_for_earlier_replies() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(10);
        send(&mut client, &mut backend, parse("x", "SELECT 1"));
        send(&mut client, &mut backend, sync());
        replies(&mut client, &mut backend, b"1Z");

        // Bind, Execute, then a Parse the backend already holds.
        send(&mut client, &mut backend, bind("x"));
        let exec = FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        client.outgoing(&exec, &frame(b'E', &[0, 0, 0, 0, 0]), &mut backend);
        send(&mut client, &mut backend, parse("x2", "SELECT 1"));
        send(&mut client, &mut backend, sync());
        assert_eq!(replies(&mut client, &mut backend, b"2DCZ"), b"2DC1Z");
    }

    #[test]
    fn close_is_answered_locally_and_keeps_the_backend_copy() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(10);
        send(&mut client, &mut backend, parse("x", "SELECT 1"));
        let close = FrontendMessage::Close {
            kind: b'S',
            name: "x".into(),
        };
        assert!(client
            .outgoing(&close, &encode_named(b'C', b'S', "x"), &mut backend)
            .is_empty());
        send(&mut client, &mut backend, sync());
        assert_eq!(replies(&mut client, &mut backend, b"1Z"), b"13Z");
        assert_eq!(backend.len(), 1);
    }

    #[test]
    fn unknown_statement_passes_through() {
        let mut client = ClientStatements::new(10);
        let mut backend = BackendStatements::default();
        let (msg, raw) = bind("nope");
        assert_eq!(client.outgoing(&msg, &raw, &mut backend), raw);
    }

    #[test]
    fn failed_parse_is_forgotten() {
        let mut client = ClientStatements::new(10);
        let mut backend = BackendStatements::default();
        send(&mut client, &mut backend, parse("bad", "SELEC 1"));
        send(&mut client, &mut backend, bind("bad"));
        send(&mut client, &mut backend, sync());
        assert_eq!(replies(&mut client, &mut backend, b"EZ"), b"EZ");
        assert!(backend.is_empty());
        let (msg, raw) = bind("bad");
        assert_eq!(client.outgoing(&msg, &raw, &mut backend), raw);
    }

    #[test]
    fn error_skips_synthetic_replies_up_to_sync() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(10);
        send(&mut client, &mut backend, parse("x", "SELECT 1"));
        send(&mut client, &mut backend, sync());
        replies(&mut client, &mut backend, b"1Z");

        send(&mut client, &mut backend, bind("missing"));
        send(&mut client, &mut backend, parse("y", "SELECT 1"));
        send(&mut client, &mut backend, sync());
        assert_eq!(replies(&mut client, &mut backend, b"EZ"), b"EZ");
    }

    #[test]
    fn least_recently_used_statement_is_closed_for_room() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(2);
        for (name, query) in [("a", "SELECT 1"), ("b", "SELECT 2")] {
            send(&mut client, &mut backend, parse(name, query));
            send(&mut client, &mut backend, sync());
            replies(&mut client, &mut backend, b"1Z");
        }
        let sent = send(&mut client, &mut backend, parse("c", "SELECT 3"));
        assert_eq!(kinds(&sent), b"CP");
        assert!(sent.starts_with(&encode_named(b'C', b'S', &backend_name("SELECT 1", &[]))));
        send(&mut client, &mut backend, sync());
        // The CloseComplete for the eviction is dropped.
        assert_eq!(replies(&mut client, &mut backend, b"31Z"), b"1Z");
        assert_eq!(backend.len(), 2);
    }

    #[test]
    fn statements_in_flight_are_not_evicted() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(1);
        send(&mut client, &mut backend, parse("a", "SELECT 1"));
        let sent = send(&mut client, &mut backend, parse("b", "SELECT 2"));
        assert_eq!(kinds(&sent), b"P");
        assert_eq!(backend.len(), 2);
    }

    #[test]
    fn discard_all_forgets_everything_once_it_succeeds() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(10);
        send(&mut client, &mut backend, parse("x", "SELECT 1"));
        send(&mut client, &mut backend, sync());
        replies(&mut client, &mut backend, b"1Z");

        client.query("DISCARD ALL");
        replies(&mut client, &mut backend, b"EZ");
        assert_eq!(backend.len(), 1);

        client.query("discard all;");
        replies(&mut client, &mut backend, b"CZ");
        assert!(backend.is_empty());
        let (msg, raw) = bind("x");
        assert_eq!(client.outgoing(&msg, &raw, &mut backend), raw);
    }

    #[test]
    fn deallocate_is_answered_locally() {
        let mut backend = BackendStatements::default();
        let mut client = ClientStatements::new(10);
        send(&mut client, &mut backend, parse("x", "SELECT 1"));
        send(&mut client, &mut backend, parse("y", "SELECT 2"));
        // Not while replies are owed.
        assert_eq!(client.answer_locally("DEALLOCATE x"), None);
        send(&mut client, &mut backend, sync());
        replies(&mut client, &mut backend, b"11Z");

        assert_eq!(client.answer_locally("DEALLOCATE x"), Some("DEALLOCATE"));
        assert_eq!(client.answer_locally("DEALLOCATE x"), None);
        assert_eq!(
            client.answer_locally("deallocate prepare all"),
            Some("DEALLOCATE ALL")
        );
        assert_eq!(backend.len(), 2);
    }

    #[test]
    fn deallocate_follows_identifier_rules() {
        assert_eq!(
            parse_deallocate("DEALLOCATE Foo"),
            Some(Deallocate::Name("foo".into()))
        );
        assert_eq!(
            parse_deallocate("DEALLOCATE PREPARE \"Foo\";"),
            Some(Deallocate::Name("Foo".into()))
        );
        assert_eq!(parse_deallocate("SELECT 1"), None);
    }

    #[test]
    fn tracking_off_passes_names_through() {
        let mut client = ClientStatements::new(0);
        let mut backend = BackendStatements::default();
        let (msg, raw) = parse("s1", "SELECT 1");
        assert_eq!(client.outgoing(&msg, &raw, &mut backend), raw);
        assert_eq!(client.answer_locally("DEALLOCATE s1"), None);
    }
}
