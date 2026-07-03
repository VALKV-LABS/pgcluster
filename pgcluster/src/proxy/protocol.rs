//! PostgreSQL wire protocol v3 — message framing and classification.
//!
//! This module handles:
//! - Startup message / SSL request parsing
//! - Regular frontend message parsing
//! - Writing common server responses (ErrorResponse, ReadyForQuery)
//! - Statement classification for routing decisions

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// ── Protocol constants ────────────────────────────────────────────────────────

/// Protocol version 3.0 (major=3, minor=0)
pub const PROTOCOL_V3: u32 = 196608; // 3 << 16 | 0

/// Magic bytes for an SSL request message
pub const SSL_REQUEST_CODE: u32 = 80877103; // 0x04D2162F

/// Magic bytes for a cancel request
pub const CANCEL_REQUEST_CODE: u32 = 80877102; // 0x04D2162E

// ── Frontend messages ─────────────────────────────────────────────────────────

/// All message types a Postgres client can send.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum FrontendMessage {
    /// Initial handshake (before authentication)
    StartupMessage {
        params: HashMap<String, String>,
    },
    /// Client is requesting TLS upgrade
    SslRequest,
    /// Simple query protocol
    Query(String),
    /// Extended query: Parse
    Parse {
        name: String,
        query: String,
        param_types: Vec<u32>,
    },
    /// Extended query: Bind
    Bind {
        portal: String,
        statement: String,
        params: Vec<Vec<u8>>,
    },
    /// Extended query: Execute
    Execute {
        portal: String,
        max_rows: i32,
    },
    /// Extended query: Describe — `kind` is b'S' (statement) or b'P' (portal)
    Describe {
        kind: u8,
        name: String,
    },
    /// Extended query: Sync (flush pipeline and return to idle)
    Sync,
    /// Flush buffered output without returning to idle
    Flush,
    /// Extended query: Close
    Close {
        kind: u8,
        name: String,
    },
    /// Cancel a running query (identified by pid + secret)
    CancelRequest {
        pid: u32,
        secret: u32,
    },
    /// Client is closing the connection
    Terminate,
    /// COPY sub-protocol
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
    /// Password / SASL auth response
    PasswordMessage(Vec<u8>),
}

// ── Statement classification ─────────────────────────────────────────────────

/// Routing intent derived from a SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementIntent {
    /// Statement must go to the primary (INSERT, UPDATE, DELETE, DDL, …)
    Write,
    /// Statement can go to a replica (SELECT, TABLE, VALUES, WITH … SELECT)
    Read,
    /// Session-scoped SET — must go to the same backend as the current session
    SetLocal,
}

/// Classify a SQL statement into a routing intent.
///
/// The classification is purely syntactic (no parsing) and is intentionally
/// conservative: when in doubt, we return `Write` so the statement goes to the
/// primary.
pub fn classify_statement(sql: &str) -> StatementIntent {
    // Work on the first non-whitespace word
    let upper = sql.trim_start().to_uppercase();

    if upper.starts_with("SELECT") || upper.starts_with("TABLE") || upper.starts_with("VALUES") {
        return StatementIntent::Read;
    }

    // WITH … SELECT is a read; WITH … INSERT/UPDATE/DELETE is a write.
    if upper.starts_with("WITH") {
        // Scan for the first non-CTE DML keyword after the WITH clause.
        // A simple heuristic: look for SELECT that appears before INSERT/UPDATE/DELETE.
        let has_select = upper.contains("SELECT");
        let has_write = upper.contains("INSERT")
            || upper.contains("UPDATE")
            || upper.contains("DELETE")
            || upper.contains("MERGE");
        if has_select && !has_write {
            return StatementIntent::Read;
        }
        return StatementIntent::Write;
    }

    if upper.starts_with("SET") {
        return StatementIntent::SetLocal;
    }

    // "BEGIN READ ONLY" is a read-only transaction opener
    if upper.starts_with("BEGIN READ ONLY") {
        return StatementIntent::Read;
    }

    // Everything else (INSERT, UPDATE, DELETE, DDL, BEGIN, CALL, COMMIT, …)
    StatementIntent::Write
}

// ── Reading messages ──────────────────────────────────────────────────────────

/// Read the very first message on a new connection.
///
/// This is special because it has no type byte — just a 4-byte length followed
/// by a 4-byte protocol version / request code, then a payload.
pub async fn read_startup_message(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<FrontendMessage> {
    // Read total length (includes the 4 bytes of the length field itself)
    let total_len = stream.read_u32().await.context("read startup length")?;

    if total_len < 8 {
        bail!("startup message too short: {total_len}");
    }

    let code = stream.read_u32().await.context("read startup code")?;

    match code {
        SSL_REQUEST_CODE => {
            // The entire message is exactly 8 bytes (\x00\x00\x00\x08 + code)
            Ok(FrontendMessage::SslRequest)
        }

        CANCEL_REQUEST_CODE => {
            let pid = stream.read_u32().await.context("read cancel pid")?;
            let secret = stream.read_u32().await.context("read cancel secret")?;
            Ok(FrontendMessage::CancelRequest { pid, secret })
        }

        PROTOCOL_V3 => {
            // Normal startup: read remaining bytes as null-terminated key=value pairs
            let payload_len = (total_len - 8) as usize;
            let mut buf = vec![0u8; payload_len];
            stream
                .read_exact(&mut buf)
                .await
                .context("read startup payload")?;

            let params = parse_startup_params(&buf)?;
            Ok(FrontendMessage::StartupMessage { params })
        }

        other => bail!("unknown startup code: 0x{other:08X}"),
    }
}

/// Parse the null-terminated key=value pairs from a startup payload.
///
/// Format: `key\0value\0key\0value\0\0`
fn parse_startup_params(buf: &[u8]) -> Result<HashMap<String, String>> {
    let mut params = HashMap::new();
    let mut iter = buf.split(|&b| b == 0);

    loop {
        let key_bytes = match iter.next() {
            Some(b) if !b.is_empty() => b,
            _ => break,
        };
        let val_bytes = iter.next().context("startup param value missing")?;

        let key = std::str::from_utf8(key_bytes).context("startup param key not UTF-8")?;
        let val = std::str::from_utf8(val_bytes).context("startup param value not UTF-8")?;
        params.insert(key.to_owned(), val.to_owned());
    }

    Ok(params)
}

/// Read a regular (post-startup) frontend message.
///
/// Format: `[type:u8][length:u32 (includes itself)][payload]`
pub async fn read_frontend_message(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<FrontendMessage> {
    let msg_type = stream.read_u8().await.context("read message type")?;
    let length = stream.read_u32().await.context("read message length")?;

    // `length` includes the 4-byte length field but NOT the type byte.
    if length < 4 {
        bail!("message length {length} too small");
    }
    let payload_len = (length - 4) as usize;

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .context("read message payload")?;
    }

    parse_frontend_message(msg_type, &payload)
}

fn parse_frontend_message(msg_type: u8, payload: &[u8]) -> Result<FrontendMessage> {
    match msg_type {
        b'Q' => {
            // Simple Query: null-terminated string
            let sql = read_cstring(payload, 0).context("Query string")?;
            Ok(FrontendMessage::Query(sql))
        }

        b'P' => {
            // Parse: name\0 query\0 param_count:u16 param_types...
            let (name, pos) = read_cstring_with_pos(payload, 0)?;
            let (query, pos) = read_cstring_with_pos(payload, pos)?;

            let count = read_u16_be(payload, pos)? as usize;
            let mut param_types = Vec::with_capacity(count);
            let mut p = pos + 2;
            for _ in 0..count {
                param_types.push(read_u32_be(payload, p)?);
                p += 4;
            }
            Ok(FrontendMessage::Parse {
                name,
                query,
                param_types,
            })
        }

        b'B' => {
            // Bind: portal\0 statement\0 … params …
            let (portal, pos) = read_cstring_with_pos(payload, 0)?;
            let (statement, pos) = read_cstring_with_pos(payload, pos)?;

            // Format codes (skip them for our purpose — we're a proxy)
            let fmt_count = read_u16_be(payload, pos)? as usize;
            let mut p = pos + 2 + fmt_count * 2;

            let param_count = read_u16_be(payload, p)? as usize;
            p += 2;

            let mut params = Vec::with_capacity(param_count);
            for _ in 0..param_count {
                let len = read_i32_be(payload, p)?;
                p += 4;
                if len < 0 {
                    // NULL
                    params.push(vec![]);
                } else {
                    let end = p + len as usize;
                    params.push(payload[p..end].to_vec());
                    p = end;
                }
            }

            Ok(FrontendMessage::Bind {
                portal,
                statement,
                params,
            })
        }

        b'E' => {
            // Execute: portal\0 max_rows:i32
            let (portal, pos) = read_cstring_with_pos(payload, 0)?;
            let max_rows = read_i32_be(payload, pos)?;
            Ok(FrontendMessage::Execute { portal, max_rows })
        }

        b'D' => {
            // Describe: kind:u8 name\0
            if payload.is_empty() {
                bail!("Describe: empty payload");
            }
            let kind = payload[0];
            let name = read_cstring(payload, 1).context("Describe name")?;
            Ok(FrontendMessage::Describe { kind, name })
        }

        b'S' => Ok(FrontendMessage::Sync),

        b'H' => Ok(FrontendMessage::Flush),

        b'C' => {
            // Close: kind:u8 name\0
            if payload.is_empty() {
                bail!("Close: empty payload");
            }
            let kind = payload[0];
            let name = read_cstring(payload, 1).context("Close name")?;
            Ok(FrontendMessage::Close { kind, name })
        }

        b'X' => Ok(FrontendMessage::Terminate),

        b'd' => Ok(FrontendMessage::CopyData(payload.to_vec())),

        b'c' => Ok(FrontendMessage::CopyDone),

        b'f' => {
            let msg = read_cstring(payload, 0).context("CopyFail message")?;
            Ok(FrontendMessage::CopyFail(msg))
        }

        b'p' => Ok(FrontendMessage::PasswordMessage(payload.to_vec())),

        other => bail!("unknown frontend message type: 0x{other:02X}"),
    }
}

// ── Writing server messages ───────────────────────────────────────────────────

/// Write an `ErrorResponse` ('E') server message.
///
/// Fields written:
/// - `S` Severity
/// - `C` SQLSTATE code
/// - `M` Message
pub async fn write_error_response(
    stream: &mut (impl AsyncWrite + Unpin),
    severity: &str,
    code: &str,
    message: &str,
) -> Result<()> {
    let mut body: Vec<u8> = Vec::new();

    // Severity
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);

    // SQLSTATE code
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);

    // Message
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);

    // Terminator
    body.push(0);

    write_message(stream, b'E', &body).await
}

/// Write a `ReadyForQuery` ('Z') server message.
///
/// `txn_status` must be one of:
/// - `b'I'` — Idle (no transaction)
/// - `b'T'` — In a transaction block
/// - `b'E'` — In a failed transaction block
pub async fn write_ready_for_query(
    stream: &mut (impl AsyncWrite + Unpin),
    txn_status: u8,
) -> Result<()> {
    write_message(stream, b'Z', &[txn_status]).await
}

/// Write a framed server message: `[type:u8][length:u32 BE][body]`.
///
/// `length` = 4 (length field itself) + body.len()
pub async fn write_message(
    stream: &mut (impl AsyncWrite + Unpin),
    msg_type: u8,
    body: &[u8],
) -> Result<()> {
    let length = (4u32 + body.len() as u32).to_be_bytes();

    let mut buf = Vec::with_capacity(1 + 4 + body.len());
    buf.push(msg_type);
    buf.extend_from_slice(&length);
    buf.extend_from_slice(body);

    stream.write_all(&buf).await.context("write message")?;
    stream.flush().await.context("flush after write_message")
}

// ── Byte-level helpers ────────────────────────────────────────────────────────

fn read_cstring(buf: &[u8], offset: usize) -> Result<String> {
    let (s, _) = read_cstring_with_pos(buf, offset)?;
    Ok(s)
}

fn read_cstring_with_pos(buf: &[u8], offset: usize) -> Result<(String, usize)> {
    let slice = &buf[offset..];
    let end = slice
        .iter()
        .position(|&b| b == 0)
        .with_context(|| format!("missing null terminator at offset {offset}"))?;
    let s = std::str::from_utf8(&slice[..end])
        .context("string is not valid UTF-8")?
        .to_owned();
    Ok((s, offset + end + 1))
}

fn read_u16_be(buf: &[u8], offset: usize) -> Result<u16> {
    buf.get(offset..offset + 2)
        .map(|b| u16::from_be_bytes([b[0], b[1]]))
        .context("read u16")
}

fn read_u32_be(buf: &[u8], offset: usize) -> Result<u32> {
    buf.get(offset..offset + 4)
        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .context("read u32")
}

fn read_i32_be(buf: &[u8], offset: usize) -> Result<i32> {
    buf.get(offset..offset + 4)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .context("read i32")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_select_is_read() {
        assert_eq!(classify_statement("SELECT 1"), StatementIntent::Read);
        assert_eq!(
            classify_statement("SELECT id FROM users WHERE id = 1"),
            StatementIntent::Read
        );
    }

    #[test]
    fn classify_insert_is_write() {
        assert_eq!(
            classify_statement("INSERT INTO t VALUES (1)"),
            StatementIntent::Write
        );
    }

    #[test]
    fn classify_begin_read_only_is_read() {
        assert_eq!(classify_statement("BEGIN READ ONLY"), StatementIntent::Read);
        assert_eq!(classify_statement("begin read only"), StatementIntent::Read);
    }

    #[test]
    fn classify_begin_is_write() {
        assert_eq!(classify_statement("BEGIN"), StatementIntent::Write);
        assert_eq!(
            classify_statement("BEGIN ISOLATION LEVEL SERIALIZABLE"),
            StatementIntent::Write
        );
    }

    #[test]
    fn classify_update_is_write() {
        assert_eq!(
            classify_statement("UPDATE users SET name='x' WHERE id=1"),
            StatementIntent::Write
        );
    }

    #[test]
    fn classify_ddl_is_write() {
        assert_eq!(
            classify_statement("CREATE TABLE foo (id INT)"),
            StatementIntent::Write
        );
        assert_eq!(classify_statement("DROP TABLE foo"), StatementIntent::Write);
        assert_eq!(
            classify_statement("ALTER TABLE foo ADD COLUMN bar TEXT"),
            StatementIntent::Write
        );
    }

    #[test]
    fn classify_set_is_set_local() {
        assert_eq!(
            classify_statement("SET search_path TO public"),
            StatementIntent::SetLocal
        );
        assert_eq!(
            classify_statement("set timezone = 'UTC'"),
            StatementIntent::SetLocal
        );
    }

    #[test]
    fn classify_with_leading_whitespace() {
        // Multiple spaces + newline before SELECT
        assert_eq!(
            classify_statement("   \n  SELECT * FROM foo"),
            StatementIntent::Read
        );
        assert_eq!(classify_statement("\t\t  SELECT 1"), StatementIntent::Read);
    }

    #[test]
    fn classify_with_cte_select_is_read() {
        assert_eq!(
            classify_statement("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            StatementIntent::Read
        );
    }

    #[test]
    fn classify_with_cte_insert_is_write() {
        assert_eq!(
            classify_statement("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"),
            StatementIntent::Write
        );
    }

    #[test]
    fn classify_table_is_read() {
        assert_eq!(classify_statement("TABLE users"), StatementIntent::Read);
    }

    #[test]
    fn classify_values_is_read() {
        assert_eq!(classify_statement("VALUES (1, 2)"), StatementIntent::Read);
    }

    #[tokio::test]
    async fn write_error_response_is_parseable() {
        let mut buf = Vec::new();
        write_error_response(&mut buf, "ERROR", "42601", "syntax error")
            .await
            .unwrap();

        // Type byte = 'E'
        assert_eq!(buf[0], b'E');
        // Length field: 4 bytes
        let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(length as usize, buf.len() - 1);
    }

    #[tokio::test]
    async fn write_ready_for_query_format() {
        let mut buf = Vec::new();
        write_ready_for_query(&mut buf, b'I').await.unwrap();

        // 'Z' + length(4+1=5) + 'I'
        assert_eq!(buf.len(), 6);
        assert_eq!(buf[0], b'Z');
        assert_eq!(buf[5], b'I');
    }
}
