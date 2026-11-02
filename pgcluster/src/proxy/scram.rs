//! SCRAM-SHA-256 on both sides of the proxy.
//!
//! The proxy is the SCRAM server to the client, using the verifier Postgres
//! stores for the role. A client's proof reveals its `ClientKey`, which lets
//! the proxy act as the SCRAM client to any backend, as PgBouncer and Supavisor
//! do. It never sees the password, and the keys live only for the session.
//!
//! Channel binding (`SCRAM-SHA-256-PLUS`) is not offered to clients: the proxy
//! terminates their TLS, and a client that insists with
//! `channel_binding=require` is refused rather than given a false assurance.

use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::protocol::write_message;

const MECHANISM: &str = "SCRAM-SHA-256";
const NONCE_BYTES: usize = 18;
/// Longer than any auth or startup-reply message the proxy reads.
const MAX_FRAME: usize = 1024 * 1024;

const AUTH_OK: i32 = 0;
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

type Key = [u8; 32];

fn hmac(key: &[u8], message: &[u8]) -> Key {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> Key {
    Sha256::digest(data).into()
}

fn xor(a: &Key, b: &Key) -> Key {
    let mut out = [0u8; 32];
    for (o, (x, y)) in out.iter_mut().zip(a.iter().zip(b.iter())) {
        *o = x ^ y;
    }
    out
}

/// Compare in constant time, so a failed proof reveals nothing about the key.
fn keys_equal(a: &Key, b: &Key) -> bool {
    a.iter().zip(b.iter()).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

fn key_from_base64(text: &str) -> Result<Key> {
    let bytes = BASE64.decode(text).context("decode key")?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("a SCRAM key must be 32 bytes"))
}

fn nonce() -> String {
    let mut bytes = [0u8; NONCE_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}

// ── Verifier ──────────────────────────────────────────────────────────────────

/// Postgres's Hi() from RFC 5802, which only a client with the password needs.
fn salted_password(password: &str, salt: &[u8], iterations: u32) -> Key {
    let mut salted = salt.to_vec();
    salted.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac(password.as_bytes(), &salted);
    let mut out = u;
    for _ in 1..iterations {
        u = hmac(password.as_bytes(), &u);
        out = xor(&out, &u);
    }
    out
}

/// The text Postgres stores for `password`, which `CREATE ROLE` accepts as is.
pub fn make_verifier(password: &str, salt: &[u8], iterations: u32) -> String {
    let salted = salted_password(password, salt, iterations);
    let stored_key = sha256(&hmac(&salted, b"Client Key"));
    let server_key = hmac(&salted, b"Server Key");
    format!(
        "SCRAM-SHA-256${iterations}:{}${}:{}",
        BASE64.encode(salt),
        BASE64.encode(stored_key),
        BASE64.encode(server_key)
    )
}

/// A role's stored `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`.
#[derive(Clone)]
pub struct Verifier {
    iterations: u32,
    salt: Vec<u8>,
    stored_key: Key,
    server_key: Key,
}

impl Verifier {
    /// `None` for anything else, such as an MD5 password or no password.
    pub fn parse(stored: &str) -> Option<Self> {
        let rest = stored.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = rest.split_once('$')?;
        let (iterations, salt) = params.split_once(':')?;
        let (stored_key, server_key) = keys.split_once(':')?;
        Some(Self {
            iterations: iterations.parse().ok()?,
            salt: BASE64.decode(salt).ok()?,
            stored_key: key_from_base64(stored_key).ok()?,
            server_key: key_from_base64(server_key).ok()?,
        })
    }

    /// A verifier no proof can satisfy, so that an unknown role fails exactly as
    /// a wrong password does, and the salt stays the same between attempts.
    pub fn mock(role: &str, secret: &Key) -> Self {
        Self {
            iterations: 4096,
            salt: hmac(secret, role.as_bytes())[..16].to_vec(),
            stored_key: hmac(secret, b"stored"),
            server_key: hmac(secret, b"server"),
        }
    }
}

/// What the proxy needs to log in to a backend as this client.
#[derive(Clone)]
pub struct Credentials {
    client_key: Key,
    verifier: Verifier,
}

// ── Frames ────────────────────────────────────────────────────────────────────

/// Read one `[type:u8][length:i32][body]` message.
pub async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> Result<(u8, Vec<u8>)> {
    let kind = stream.read_u8().await.context("read message type")?;
    let length = stream.read_u32().await.context("read message length")? as usize;
    ensure!(
        (4..=MAX_FRAME).contains(&length),
        "bad message length {length}"
    );
    let mut body = vec![0u8; length - 4];
    stream
        .read_exact(&mut body)
        .await
        .context("read message body")?;
    Ok((kind, body))
}

fn encode_frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(kind);
    frame.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

fn auth_message(code: i32, data: &[u8]) -> Vec<u8> {
    let mut body = code.to_be_bytes().to_vec();
    body.extend_from_slice(data);
    body
}

/// The value of `key=` among the comma-separated attributes of a SCRAM message.
fn attribute(message: &str, key: char) -> Option<&str> {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(key)?.strip_prefix('='))
}

// ── Server side: the client ───────────────────────────────────────────────────

/// Why a client's login did not complete.
#[derive(Debug)]
pub enum ClientAuthError {
    /// The proof was wrong: a wrong password, or a mock verifier.
    Rejected,
    Protocol(anyhow::Error),
}

impl From<anyhow::Error> for ClientAuthError {
    fn from(e: anyhow::Error) -> Self {
        Self::Protocol(e)
    }
}

/// Run the server side of SCRAM with a client, up to and excluding its final
/// `AuthenticationOk`. Returns the credentials and the server's final message,
/// which proves to the client that the proxy knows the verifier.
pub async fn authenticate_client<S>(
    client: &mut S,
    verifier: Verifier,
) -> Result<Credentials, ClientAuthError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mechanisms = format!("{MECHANISM}\0\0");
    write_message(
        client,
        b'R',
        &auth_message(AUTH_SASL, mechanisms.as_bytes()),
    )
    .await
    .context("send SASL mechanisms")?;

    // SASLInitialResponse: mechanism\0, length, client-first-message.
    let (kind, body) = read_frame(client).await?;
    if kind != b'p' {
        return Err(anyhow!("expected SASLInitialResponse, got {kind:#x}").into());
    }
    let (client_first, gs2_header) = parse_initial_response(&body)?;

    let client_first_bare = client_first
        .strip_prefix(&gs2_header)
        .ok_or_else(|| anyhow!("malformed client-first-message"))?;
    let client_nonce = attribute(client_first_bare, 'r')
        .ok_or_else(|| anyhow!("client-first-message has no nonce"))?;
    let combined_nonce = format!("{client_nonce}{}", nonce());
    let server_first = format!(
        "r={combined_nonce},s={},i={}",
        BASE64.encode(&verifier.salt),
        verifier.iterations
    );
    write_message(
        client,
        b'R',
        &auth_message(AUTH_SASL_CONTINUE, server_first.as_bytes()),
    )
    .await
    .context("send server-first-message")?;

    // SASLResponse: client-final-message.
    let (kind, body) = read_frame(client).await?;
    if kind != b'p' {
        return Err(anyhow!("expected SASLResponse, got {kind:#x}").into());
    }
    let client_final =
        std::str::from_utf8(&body).map_err(|_| anyhow!("client-final-message is not UTF-8"))?;
    let (without_proof, proof) = client_final
        .rsplit_once(",p=")
        .ok_or_else(|| anyhow!("client-final-message has no proof"))?;

    if attribute(without_proof, 'r') != Some(combined_nonce.as_str()) {
        return Err(anyhow!("client-final-message has the wrong nonce").into());
    }
    if attribute(without_proof, 'c') != Some(BASE64.encode(&gs2_header).as_str()) {
        return Err(anyhow!("channel binding does not match the GS2 header").into());
    }

    let auth = format!("{client_first_bare},{server_first},{without_proof}");
    let proof = key_from_base64(proof)?;
    let client_key = xor(&proof, &hmac(&verifier.stored_key, auth.as_bytes()));
    if !keys_equal(&sha256(&client_key), &verifier.stored_key) {
        return Err(ClientAuthError::Rejected);
    }

    let signature = BASE64.encode(hmac(&verifier.server_key, auth.as_bytes()));
    let server_final = format!("v={signature}");
    write_message(
        client,
        b'R',
        &auth_message(AUTH_SASL_FINAL, server_final.as_bytes()),
    )
    .await
    .context("send server-final-message")?;
    write_message(client, b'R', &AUTH_OK.to_be_bytes())
        .await
        .context("send AuthenticationOk")?;

    Ok(Credentials {
        client_key,
        verifier,
    })
}

/// Split a SASLInitialResponse into the client-first-message and its GS2 header.
fn parse_initial_response(body: &[u8]) -> Result<(String, String)> {
    let end = body
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| anyhow!("SASLInitialResponse has no mechanism"))?;
    let mechanism = std::str::from_utf8(&body[..end]).context("mechanism is not UTF-8")?;
    ensure!(mechanism == MECHANISM, "unsupported mechanism {mechanism}");

    let data = body
        .get(end + 5..)
        .ok_or_else(|| anyhow!("SASLInitialResponse is truncated"))?;
    let client_first = std::str::from_utf8(data).context("client-first-message is not UTF-8")?;

    // "n,," means no channel binding. "y,," means the client supports it but
    // saw that this server does not. "p=" asks for a binding never offered.
    let mut parts = client_first.splitn(3, ',');
    let (flag, authzid) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    ensure!(
        (flag == "n" || flag == "y") && parts.next().is_some(),
        "unsupported GS2 header"
    );
    let gs2_header = format!("{flag},{authzid},");
    Ok((client_first.to_string(), gs2_header))
}

// ── Client side: the backend ──────────────────────────────────────────────────

/// Why a backend login failed.
#[derive(Debug)]
pub enum BackendLoginError {
    /// Postgres sent this ErrorResponse frame, to pass on to the client.
    Refused(Vec<u8>),
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for BackendLoginError {
    fn from(e: anyhow::Error) -> Self {
        Self::Failed(e)
    }
}

impl std::fmt::Display for BackendLoginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(_) => write!(f, "backend refused the login"),
            Self::Failed(e) => write!(f, "{e:#}"),
        }
    }
}

/// Log in to a fresh backend connection as the client. Returns everything the
/// backend sent after `AuthenticationOk`, through its `ReadyForQuery`.
pub async fn login_to_backend<S>(
    backend: &mut S,
    startup: &[u8],
    credentials: &Credentials,
) -> Result<Vec<u8>, BackendLoginError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    backend
        .write_all(startup)
        .await
        .context("send startup message")?;
    backend.flush().await.context("flush startup message")?;

    let client_nonce = nonce();
    // The username comes from the startup message, so it stays empty here.
    let client_first_bare = format!("n=,r={client_nonce}");
    let verifier = &credentials.verifier;
    let mut auth = String::new();

    loop {
        let (kind, body) = read_frame(backend).await?;
        match kind {
            b'E' => return Err(BackendLoginError::Refused(encode_frame(kind, &body))),
            b'R' => {}
            other => return Err(anyhow!("unexpected message {other:#x} in login").into()),
        }
        ensure_body(&body)?;
        let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        let data = &body[4..];
        match code {
            AUTH_OK => break,
            AUTH_SASL => {
                let offered = String::from_utf8_lossy(data);
                if !offered.split('\0').any(|m| m == MECHANISM) {
                    return Err(anyhow!("backend does not offer {MECHANISM}").into());
                }
                let client_first = format!("n,,{client_first_bare}");
                let mut response = format!("{MECHANISM}\0").into_bytes();
                response.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
                response.extend_from_slice(client_first.as_bytes());
                send(backend, &response).await?;
            }
            AUTH_SASL_CONTINUE => {
                let server_first = std::str::from_utf8(data).context("server-first not UTF-8")?;
                let server_nonce = attribute(server_first, 'r')
                    .filter(|n| n.starts_with(&client_nonce))
                    .ok_or_else(|| anyhow!("backend sent the wrong nonce"))?;
                check_same_verifier(server_first, verifier)?;

                let without_proof = format!("c=biws,r={server_nonce}");
                auth = format!("{client_first_bare},{server_first},{without_proof}");
                let signature = hmac(&verifier.stored_key, auth.as_bytes());
                let proof = BASE64.encode(xor(&credentials.client_key, &signature));
                send(backend, format!("{without_proof},p={proof}").as_bytes()).await?;
            }
            AUTH_SASL_FINAL => {
                let server_final = std::str::from_utf8(data).context("server-final not UTF-8")?;
                let signature = attribute(server_final, 'v')
                    .ok_or_else(|| anyhow!("backend sent no server signature"))
                    .and_then(key_from_base64)?;
                let expected = hmac(&verifier.server_key, auth.as_bytes());
                if !keys_equal(&signature, &expected) {
                    return Err(anyhow!("backend failed to prove it knows the role").into());
                }
            }
            other => {
                return Err(anyhow!(
                    "backend asked for auth method {other}, and the proxy needs SCRAM"
                )
                .into());
            }
        }
    }

    // ParameterStatus and BackendKeyData follow, through ReadyForQuery.
    let mut rest = Vec::new();
    loop {
        let (kind, body) = read_frame(backend).await?;
        let frame = encode_frame(kind, &body);
        if kind == b'E' {
            return Err(BackendLoginError::Refused(frame));
        }
        rest.extend_from_slice(&frame);
        if kind == b'Z' {
            return Ok(rest);
        }
    }
}

fn ensure_body(body: &[u8]) -> Result<()> {
    if body.len() < 4 {
        bail!("authentication message is too short");
    }
    Ok(())
}

/// The keys only work against the verifier they came from, so a backend that
/// stores another one means the password changed mid-session.
fn check_same_verifier(server_first: &str, verifier: &Verifier) -> Result<()> {
    let salt = attribute(server_first, 's').context("server-first has no salt")?;
    let iterations = attribute(server_first, 'i').context("server-first has no iterations")?;
    if BASE64.decode(salt).ok().as_ref() != Some(&verifier.salt)
        || iterations.parse().ok() != Some(verifier.iterations)
    {
        bail!("the role's password changed");
    }
    Ok(())
}

async fn send<S: AsyncWrite + Unpin>(backend: &mut S, body: &[u8]) -> Result<()> {
    write_message(backend, b'p', body)
        .await
        .context("send SASL response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    const SALT: &[u8] = b"0123456789abcdef";
    const ITERATIONS: u32 = 16;

    fn stored(password: &str) -> String {
        make_verifier(password, SALT, ITERATIONS)
    }

    /// A client that knows the password, as libpq does. Returns the server's
    /// reply codes in order.
    async fn scram_client<S>(stream: &mut S, password: &str, flag: &str) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (_, mechanisms) = read_frame(stream).await?;
        assert_eq!(&mechanisms[..4], &AUTH_SASL.to_be_bytes());
        assert!(String::from_utf8_lossy(&mechanisms[4..]).contains(MECHANISM));

        let bare = format!("n=,r={}", nonce());
        let gs2 = format!("{flag},,");
        let first = format!("{gs2}{bare}");
        let mut body = format!("{MECHANISM}\0").into_bytes();
        body.extend_from_slice(&(first.len() as i32).to_be_bytes());
        body.extend_from_slice(first.as_bytes());
        write_message(stream, b'p', &body).await?;

        let (_, reply) = read_frame(stream).await?;
        let server_first = std::str::from_utf8(&reply[4..])?.to_string();
        let without_proof = format!(
            "c={},r={}",
            BASE64.encode(&gs2),
            attribute(&server_first, 'r').unwrap()
        );
        let auth = format!("{bare},{server_first},{without_proof}");
        let salted = salted_password(password, SALT, ITERATIONS);
        let client_key = hmac(&salted, b"Client Key");
        let signature = hmac(&sha256(&client_key), auth.as_bytes());
        let proof = BASE64.encode(xor(&client_key, &signature));
        write_message(
            stream,
            b'p',
            format!("{without_proof},p={proof}").as_bytes(),
        )
        .await?;
        // A rejected client gets neither of the final messages.
        let _ = read_frame(stream).await;
        let _ = read_frame(stream).await;
        Ok(())
    }

    async fn run(
        stored_as: &str,
        password: &str,
        flag: &'static str,
    ) -> Result<Credentials, ClientAuthError> {
        let (mut client, mut proxy) = duplex(4096);
        let password = password.to_string();
        let task = tokio::spawn(async move { scram_client(&mut client, &password, flag).await });
        let result = authenticate_client(&mut proxy, Verifier::parse(stored_as).unwrap()).await;
        drop(proxy);
        task.await.unwrap().unwrap();
        result
    }

    #[test]
    fn verifiers_parse_and_other_forms_do_not() {
        assert!(Verifier::parse(&stored("secret")).is_some());
        assert!(Verifier::parse("md5abcdef").is_none());
        assert!(Verifier::parse("").is_none());
    }

    #[tokio::test]
    async fn the_right_password_is_accepted() {
        assert!(run(&stored("secret"), "secret", "n").await.is_ok());
    }

    #[tokio::test]
    async fn a_tls_client_that_saw_no_channel_binding_is_accepted() {
        assert!(run(&stored("secret"), "secret", "y").await.is_ok());
    }

    #[tokio::test]
    async fn a_wrong_password_is_rejected() {
        assert!(matches!(
            run(&stored("secret"), "wrong", "n").await,
            Err(ClientAuthError::Rejected)
        ));
    }

    #[tokio::test]
    async fn a_mock_verifier_rejects_every_password() {
        let (mut client, mut proxy) = duplex(4096);
        let task = tokio::spawn(async move { scram_client(&mut client, "anything", "n").await });
        let result = authenticate_client(&mut proxy, Verifier::mock("nobody", &[7; 32])).await;
        drop(proxy);
        task.await.unwrap().unwrap();
        assert!(matches!(result, Err(ClientAuthError::Rejected)));
    }

    #[test]
    fn a_mock_salt_is_stable_for_a_role() {
        let secret = [7; 32];
        assert_eq!(
            Verifier::mock("a", &secret).salt,
            Verifier::mock("a", &secret).salt
        );
        assert_ne!(
            Verifier::mock("a", &secret).salt,
            Verifier::mock("b", &secret).salt
        );
    }

    /// A fake Postgres that verifies the proxy's proof as Postgres does.
    async fn fake_backend<S>(stream: &mut S, stored_as: &str) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let verifier = Verifier::parse(stored_as).unwrap();
        let mut startup = [0u8; 7];
        stream.read_exact(&mut startup).await?;

        let mechanisms = format!("SCRAM-SHA-256-PLUS\0{MECHANISM}\0\0");
        write_message(
            stream,
            b'R',
            &auth_message(AUTH_SASL, mechanisms.as_bytes()),
        )
        .await?;

        let (_, body) = read_frame(stream).await?;
        let (first, gs2) = parse_initial_response(&body)?;
        assert_eq!(gs2, "n,,");
        let bare = first.strip_prefix(&gs2).unwrap().to_string();
        let server_nonce = format!("{}server", attribute(&bare, 'r').unwrap());
        let server_first = format!(
            "r={server_nonce},s={},i={}",
            BASE64.encode(&verifier.salt),
            verifier.iterations
        );
        write_message(
            stream,
            b'R',
            &auth_message(AUTH_SASL_CONTINUE, server_first.as_bytes()),
        )
        .await?;

        let (_, body) = read_frame(stream).await?;
        let client_final = std::str::from_utf8(&body)?;
        let (without_proof, proof) = client_final.rsplit_once(",p=").unwrap();
        let auth = format!("{bare},{server_first},{without_proof}");
        let key = xor(
            &key_from_base64(proof)?,
            &hmac(&verifier.stored_key, auth.as_bytes()),
        );
        assert!(keys_equal(&sha256(&key), &verifier.stored_key), "bad proof");

        let signature = BASE64.encode(hmac(&verifier.server_key, auth.as_bytes()));
        write_message(
            stream,
            b'R',
            &auth_message(AUTH_SASL_FINAL, format!("v={signature}").as_bytes()),
        )
        .await?;
        write_message(stream, b'R', &AUTH_OK.to_be_bytes()).await?;
        write_message(stream, b'S', b"a\0b\0").await?;
        write_message(stream, b'Z', b"I").await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_key_from_a_client_logs_in_to_a_backend() {
        let stored_as = stored("secret");
        let credentials = run(&stored_as, "secret", "n").await.ok().unwrap();

        let (mut proxy, mut backend) = duplex(4096);
        let task = tokio::spawn(async move { fake_backend(&mut backend, &stored_as).await });
        let rest = login_to_backend(&mut proxy, b"startup", &credentials)
            .await
            .unwrap();
        task.await.unwrap().unwrap();

        assert_eq!(rest[0], b'S');
        assert_eq!(*rest.last().unwrap(), b'I');
    }

    #[tokio::test]
    async fn a_refused_login_returns_the_error_to_pass_on() {
        let credentials = run(&stored("secret"), "secret", "n").await.ok().unwrap();
        let (mut proxy, mut backend) = duplex(4096);
        tokio::spawn(async move {
            let mut startup = [0u8; 7];
            backend.read_exact(&mut startup).await.unwrap();
            write_message(&mut backend, b'E', b"SFATAL\0\0")
                .await
                .unwrap();
        });
        let result = login_to_backend(&mut proxy, b"startup", &credentials).await;
        assert!(matches!(result, Err(BackendLoginError::Refused(f)) if f[0] == b'E'));
    }

    #[tokio::test]
    async fn a_changed_password_is_detected() {
        let credentials = run(&stored("secret"), "secret", "n").await.ok().unwrap();
        let server_first = format!("r=x,s={},i=9999", BASE64.encode(SALT));
        assert!(check_same_verifier(&server_first, &credentials.verifier).is_err());
    }
}
