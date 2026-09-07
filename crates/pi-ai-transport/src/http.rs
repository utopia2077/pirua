use std::{error::Error, fmt};

use pi_ai_core::{BoxFuture, RequestEndpoint, RequestRuntime};

/// Stable categories; never copy native errors or wire payloads into diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    Cancelled,
    Timeout,
    Connection,
    InvalidRequest,
    InvalidProxy,
    UnsupportedProxy,
    Protocol,
    LimitExceeded,
    RetryDelayExceeded,
    Closed,
    RuntimeUnavailable,
}

/// Redacted failure. Source chains, when supplied, must themselves be safe.
pub struct TransportError {
    kind: TransportErrorKind,
}

impl TransportError {
    pub const fn new(kind: TransportErrorKind) -> Self {
        Self { kind }
    }
    pub const fn kind(&self) -> TransportErrorKind {
        self.kind
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportError")
            .field("kind", &self.kind)
            .finish()
    }
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Transport failure: {:?}", self.kind)
    }
}
impl Error for TransportError {}

/// Ordered transient headers, retaining repeated response fields. Values are
/// exposed only by explicit wire methods. Request overrides use `set`/`remove`.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct HttpHeaders {
    wire: Vec<(String, String)>,
    fetch: Option<Vec<(String, String)>>,
}
impl HttpHeaders {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn append(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), TransportError> {
        let name = name.into();
        let value = value.into();
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
            || value.bytes().any(|c| c == b'\r' || c == b'\n' || c == 0)
        {
            return Err(TransportError::new(TransportErrorKind::InvalidRequest));
        }
        let name = name.to_ascii_lowercase();
        if let Some(fetch) = &mut self.fetch {
            fetch.push((name.clone(), value.trim_matches([' ', '\t']).to_owned()));
        }
        self.wire.push((name, value));
        Ok(())
    }
    pub fn set(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), TransportError> {
        let mut checked = Self::new();
        checked.append(name, value)?;
        let (name, value) = checked.wire.remove(0);
        self.remove(&name);
        if let Some(fetch) = &mut self.fetch {
            fetch.push((name.clone(), value.trim_matches([' ', '\t']).to_owned()));
        }
        self.wire.push((name, value));
        Ok(())
    }
    pub fn remove(&mut self, name: &str) {
        self.wire.retain(|(key, _)| !key.eq_ignore_ascii_case(name));
        if let Some(fetch) = &mut self.fetch {
            fetch.retain(|(key, _)| !key.eq_ignore_ascii_case(name));
        }
    }
    pub fn get_for_wire(&self, name: &str) -> Option<&str> {
        self.wire
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
    /// Fetch Headers.get projection. Raw repeated fields remain untouched.
    /// Cookie joins with semicolons; all other fields (including Set-Cookie)
    /// join with commas. Constructed values normalize HTTP edge whitespace;
    /// explicitly supplied response-field values are already admitted and unchanged.
    pub fn get_for_fetch(&self, name: &str) -> Option<String> {
        let values: Vec<_> = self
            .iter_for_fetch()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
            .collect();
        (!values.is_empty()).then(|| {
            values.join(if name.eq_ignore_ascii_case("cookie") {
                "; "
            } else {
                ", "
            })
        })
    }
    pub fn iter_for_wire(&self) -> impl Iterator<Item = (&str, &str)> {
        self.wire.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    /// Attach actual admitted Fetch response field data without replacing the
    /// raw repeated fields. Names and per-name cardinality must agree; field
    /// order may differ because a native raw HeaderMap groups duplicate names.
    /// This is explicit header data, never an origin/mode flag. Values must be
    /// Latin-1 byte strings. The supplied view's raw fields are consumed verbatim.
    pub fn with_fetch_response_fields(mut self, fields: Self) -> Result<Self, TransportError> {
        fn counts(fields: &[(String, String)]) -> std::collections::BTreeMap<&str, usize> {
            let mut counts = std::collections::BTreeMap::new();
            for (name, _) in fields {
                *counts.entry(name.as_str()).or_default() += 1;
            }
            counts
        }
        if counts(&self.wire) != counts(&fields.wire)
            || fields
                .wire
                .iter()
                .any(|(_, value)| value.chars().any(|c| u32::from(c) > 255))
        {
            return Err(TransportError::new(TransportErrorKind::Protocol));
        }
        self.fetch = Some(fields.wire);
        Ok(self)
    }
    /// Ordered repeated semantic fields. Existing constructors retain their
    /// constructed-value normalization; observed fields never get re-trimmed.
    pub fn iter_for_fetch(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fetch
            .as_ref()
            .unwrap_or(&self.wire)
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str(),
                    if self.fetch.is_some() {
                        value.as_str()
                    } else {
                        value.trim_matches([' ', '\t'])
                    },
                )
            })
    }
}
impl fmt::Debug for HttpHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HttpHeaders([REDACTED])")
    }
}

/// Replayable bytes for one logical HTTP request. A retry borrows the same value.
pub struct HttpRequest {
    pub endpoint: RequestEndpoint,
    pub method: String,
    pub headers: HttpHeaders,
    pub body: Vec<u8>,
    /// Already selected by explicit host environment configuration.
    pub proxy: Option<crate::ProxyUrl>,
}
impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HttpRequest([REDACTED])")
    }
}

/// Response acquisition does not consume or replay the body.
pub struct HttpResponse {
    pub status: u16,
    /// Lossless HTTP/1 reason byte string: every octet maps to U+0000..U+00FF.
    /// Empty/custom values are retained; HTTP/2 is empty. Fetch adapters must
    /// decode the recovered octets as UTF-8 with replacement at their boundary.
    /// Never include this untrusted field in diagnostics.
    pub status_text: String,
    pub headers: HttpHeaders,
    pub body: Box<dyn ResponseBody>,
}
impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("wire", &"[REDACTED]")
            .finish()
    }
}

/// An incremental owned body. Dropping a pending next future abandons that read;
/// closing/dropping the body releases the connection. It never restarts a request.
pub trait ResponseBody: Send {
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>>;
    fn close(&mut self);
}

/// Exactly one wire attempt; automatic retries and redirects are forbidden.
/// Implementations must release outstanding I/O when the returned future drops.
pub trait RawHttpClient: Send + Sync {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>>;
    /// One raw-network Fetch hop. Implementations may add protocol-dependent
    /// network defaults after negotiation. No redirects or retries are allowed.
    /// Existing injected clients keep exactly their original send behavior.
    fn send_fetch_hop<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.send(request, runtime)
    }
}

/// Bounded collection for adapter-owned error parsing; never formats raw bytes.
pub async fn read_body_limited(
    body: &mut dyn ResponseBody,
    runtime: &RequestRuntime,
    limit: usize,
) -> Result<Vec<u8>, TransportError> {
    let mut bytes = Vec::new();
    loop {
        let result = crate::policy::cancel_wait(body.next(runtime), &runtime.cancellation).await;
        match result {
            Ok(Some(chunk)) if chunk.len() <= limit.saturating_sub(bytes.len()) => {
                bytes.extend_from_slice(&chunk)
            }
            Ok(Some(_)) => {
                body.close();
                return Err(TransportError::new(TransportErrorKind::LimitExceeded));
            }
            Ok(None) => {
                body.close();
                return Ok(bytes);
            }
            Err(error) => {
                body.close();
                return Err(error);
            }
        }
    }
}
