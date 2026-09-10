//! Provider-neutral callback and cryptographic host capabilities.
use pi_ai_core::{BoxFuture, CancellationToken, SecretString};
use std::{error::Error, fmt, net::SocketAddr, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AuthHostErrorKind {
    Cancelled,
    Closed,
    InvalidInput,
    LimitExceeded,
    Io,
    Runtime,
    Crypto,
}
#[derive(Clone)]
pub struct AuthHostError {
    kind: AuthHostErrorKind,
    source: Option<Arc<dyn Error + Send + Sync>>,
}
impl AuthHostError {
    pub fn new(kind: AuthHostErrorKind) -> Self {
        Self { kind, source: None }
    }
    pub fn with_source(
        kind: AuthHostErrorKind,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            source: Some(Arc::new(source)),
        }
    }
    pub fn kind(&self) -> AuthHostErrorKind {
        self.kind
    }
}
impl fmt::Debug for AuthHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthHostError")
            .field("kind", &self.kind)
            .finish()
    }
}
impl fmt::Display for AuthHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("authentication host operation failed")
    }
}
impl Error for AuthHostError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|s| s as _)
    }
}

/// Native limits are a finite host admission domain, not OAuth protocol policy.
#[derive(Clone, Copy, Debug)]
pub struct CallbackLimits {
    pub max_connections: usize,
    pub max_request_head_bytes: usize,
    pub max_response_bytes: usize,
}
impl Default for CallbackLimits {
    fn default() -> Self {
        Self {
            max_connections: 32,
            max_request_head_bytes: 16_384,
            max_response_bytes: 65_536,
        }
    }
}
#[derive(Clone)]
pub struct CallbackBind {
    pub host: String,
    pub port: u16,
    pub limits: CallbackLimits,
}
impl fmt::Debug for CallbackBind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CallbackBind([REDACTED])")
    }
}
/// A transient response. The host supplies length/connection headers.
pub struct CallbackResponse {
    pub status: u16,
    pub content_type: String,
    pub no_store: bool,
    pub body: String,
}
impl fmt::Debug for CallbackResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CallbackResponse([REDACTED])")
    }
}
pub trait CallbackReply: Send {
    /// Consumes the one-shot response owner; completion acknowledges write and shutdown.
    fn send<'a>(
        self: Box<Self>,
        response: CallbackResponse,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AuthHostError>>;
}
pub struct CallbackRequest {
    pub method: String,
    pub target: String,
    pub reply: Box<dyn CallbackReply>,
}
impl fmt::Debug for CallbackRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CallbackRequest([REDACTED])")
    }
}
pub trait CallbackSession: Send {
    fn local_addr(&self) -> SocketAddr;
    /// Cancelling/dropping this wait does not close the listener.
    fn next<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CallbackRequest, AuthHostError>>;
    /// Definitively releases the listener and every accepted connection. Drop initiates cleanup.
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), AuthHostError>>;
}
pub trait CallbackListener: Send + Sync {
    fn bind<'a>(
        &'a self,
        config: CallbackBind,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CallbackSession>, AuthHostError>>;
}
/// Entropy and SHA256 are supplied by a trusted host; no OS access on construction.
pub trait OAuthCrypto: Send + Sync {
    fn fill_random(&self, bytes: &mut [u8]) -> Result<(), AuthHostError>;
    fn sha256(&self, bytes: &[u8]) -> Result<[u8; 32], AuthHostError>;
}
pub struct Pkce {
    pub verifier: SecretString,
    pub challenge: String,
}
impl fmt::Debug for Pkce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Pkce([REDACTED])")
    }
}
/// Matches pinned WebCrypto PKCE: 32 entropy bytes and unpadded base64url.
pub fn generate_pkce(crypto: &dyn OAuthCrypto) -> Result<Pkce, AuthHostError> {
    let mut bytes = [0; 32];
    crypto.fill_random(&mut bytes)?;
    let verifier = base64url(&bytes);
    let challenge = base64url(&crypto.sha256(verifier.as_bytes())?);
    Ok(Pkce {
        verifier: SecretString::new(verifier),
        challenge,
    })
}
// Encoding is deliberately tiny and deterministic; crypto is never implemented here.
fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = *chunk.get(1).unwrap_or(&0);
        let c = *chunk.get(2).unwrap_or(&0);
        out.push(ALPHABET[(a >> 2) as usize] as char);
        out.push(ALPHABET[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b & 15) << 2) | (c >> 6)) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(c & 63) as usize] as char);
        }
    }
    out
}
