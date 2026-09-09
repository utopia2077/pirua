//! Actual finite Fetch input values, before a custom or network consumer.
use crate::{HttpHeaders, HttpRequest, HttpResponse, ProxyUrl, TransportError, TransportErrorKind};
use pi_ai_core::{BoxFuture, RequestEndpoint, RequestRuntime};
use std::fmt;

/// Ordinary own string properties. Exact replacement retains its first slot;
/// array-index keys enumerate first, as Object.entries does. No HTTP validation
/// or case folding occurs until a consumer is selected.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct FetchHeaderRecord(Vec<(String, String)>);
impl FetchHeaderRecord {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, name: String, value: String) {
        if let Some((_, old)) = self.0.iter_mut().find(|(key, _)| key == &name) {
            *old = value;
        } else {
            self.0.push((name, value));
            self.0.sort_by_key(|(key, _)| {
                key.parse::<u32>()
                    .ok()
                    .filter(|index| *index != u32::MAX && index.to_string() == *key)
                    .map_or((1, 0), |index| (0, index))
            });
        }
    }
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}
impl fmt::Debug for FetchHeaderRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FetchHeaderRecord([REDACTED])")
    }
}

#[derive(Clone, Debug)]
pub enum FetchHeaders {
    Record(FetchHeaderRecord),
    Constructed(HttpHeaders),
}
#[derive(Clone)]
pub enum FetchBody {
    Text(String),
    Bytes(Vec<u8>),
}
impl fmt::Debug for FetchBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FetchBody([REDACTED])")
    }
}

/// A trusted replayable input, not the arbitrary JS Request/RequestInit union.
/// Empty legacy byte bodies keep the existing GET/HEAD absent-body convention.
pub struct FetchRequest {
    pub endpoint: RequestEndpoint,
    pub method: String,
    pub headers: FetchHeaders,
    pub body: FetchBody,
    pub proxy: Option<ProxyUrl>,
}
impl fmt::Debug for FetchRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FetchRequest([REDACTED])")
    }
}
impl FetchRequest {
    pub fn from_http(request: &HttpRequest) -> Result<Self, TransportError> {
        Ok(Self {
            endpoint: RequestEndpoint::new_resolved(request.endpoint.expose_for_transport())
                .map_err(|_| TransportError::new(TransportErrorKind::InvalidRequest))?,
            method: request.method.clone(),
            headers: FetchHeaders::Constructed(request.headers.clone()),
            body: FetchBody::Bytes(request.body.clone()),
            proxy: request.proxy.clone(),
        })
    }
    pub(crate) fn network_request(&self) -> Result<HttpRequest, TransportError> {
        let invalid = || TransportError::new(TransportErrorKind::InvalidRequest);
        let mut headers = match &self.headers {
            FetchHeaders::Constructed(headers) => headers.clone(),
            FetchHeaders::Record(record) => {
                let mut headers = HttpHeaders::new();
                for (name, value) in record.iter() {
                    if value.chars().any(|c| u32::from(c) > 255) {
                        return Err(invalid());
                    }
                    headers.append(name, value.trim_matches([' ', '\t', '\r', '\n']))?;
                }
                headers
            }
        };
        let body = match &self.body {
            FetchBody::Bytes(bytes) => bytes.clone(),
            FetchBody::Text(text) => {
                if matches!(self.method.to_ascii_uppercase().as_str(), "GET" | "HEAD") {
                    return Err(invalid());
                }
                if headers.get_for_wire("content-type").is_none() {
                    headers.append("content-type", "text/plain;charset=UTF-8")?;
                }
                text.as_bytes().to_vec()
            }
        };
        Ok(HttpRequest {
            endpoint: RequestEndpoint::new_resolved(self.endpoint.expose_for_transport())
                .map_err(|_| invalid())?,
            method: self.method.clone(),
            headers,
            body,
            proxy: self.proxy.clone(),
        })
    }
}

/// Explicit configured custom Fetch. Input is observed before network
/// preparation; returned responses are preconstructed and never redirected or
/// decoded again. Errors retain their closed kind and gain a custom-rejection
/// stage annotation. SDK acquisition owners apply any source-specific wrapper;
/// direct callers do not inherit an SDK's Connection classification.
/// The transport owns cancellation, timeout and selected retry.
pub trait CustomFetch: Send + Sync {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>>;
}
