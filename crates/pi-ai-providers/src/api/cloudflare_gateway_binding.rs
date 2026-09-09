//! Configured, pre-authenticated gateway capability over finite logical Fetch.
use super::wire_json::WirePayload;
use pi_ai_core::{BoxFuture, RequestRuntime};
use pi_ai_transport::{
    CustomFetch, FetchBody, FetchHeaderRecord, FetchHeaders, FetchRequest, HttpResponse,
    HttpTransport, RetryJitter, TransportError, TransportErrorKind,
};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeSet, fmt, sync::Arc};
use url::Url;

pub const CLOUDFLARE_GATEWAY_BINDING_AUTH_SENTINEL: &str = "cloudflare-gateway-binding";

/// Trusted bounded synchronous selection; credentials/discovery belong to host.
pub trait AiGatewayBinding: Send + Sync {
    fn gateway(&self, id: &str) -> Result<Box<dyn AiGatewayBindingGateway>, TransportError>;
}
pub trait AiGatewayBindingGateway: Send + Sync {
    fn run<'a>(
        &'a self,
        data: &'a AiGatewayUniversalRequestLike,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>>;
}
pub struct AiGatewayUniversalRequestLike {
    pub provider: String,
    pub endpoint: String,
    pub headers: FetchHeaderRecord,
    pub query: WirePayload,
}
impl fmt::Debug for AiGatewayUniversalRequestLike {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AiGatewayUniversalRequestLike([REDACTED])")
    }
}
pub struct GatewayBindingFetchOptions {
    pub binding: Arc<dyn AiGatewayBinding>,
    pub base_url: String,
    pub gateway: String,
}
impl fmt::Debug for GatewayBindingFetchOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GatewayBindingFetchOptions([REDACTED])")
    }
}
pub struct GatewayBindingFetch {
    binding: Arc<dyn AiGatewayBinding>,
    origin: String,
    path: String,
    gateway: String,
}
impl fmt::Debug for GatewayBindingFetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GatewayBindingFetch([REDACTED])")
    }
}
fn invalid() -> TransportError {
    TransportError::new(TransportErrorKind::InvalidRequest)
}

pub fn create_gateway_binding_fetch(
    options: GatewayBindingFetchOptions,
) -> Result<GatewayBindingFetch, TransportError> {
    let base = Url::parse(&options.base_url).map_err(|_| invalid())?;
    let path = if base.path().ends_with('/') {
        base.path().to_owned()
    } else {
        format!("{}/", base.path())
    };
    Ok(GatewayBindingFetch {
        binding: options.binding,
        origin: base.origin().ascii_serialization(),
        path,
        gateway: options.gateway,
    })
}
impl GatewayBindingFetch {
    /// No raw operation, redirect, network defaults, or second content decoder.
    /// Retry, if explicitly selected by a caller, uses the supplied jitter port.
    pub fn into_transport(self, jitter: Arc<dyn RetryJitter>) -> HttpTransport {
        HttpTransport::new_custom(Arc::new(self), jitter)
    }
    fn prepare(
        &self,
        request: &FetchRequest,
    ) -> Result<AiGatewayUniversalRequestLike, TransportError> {
        let parsed = Url::parse(request.endpoint.expose_for_transport()).map_err(|_| invalid())?;
        if parsed.origin().ascii_serialization() != self.origin
            || !parsed.path().starts_with(&self.path)
            || request.method.to_uppercase() != "POST"
        {
            return Err(invalid());
        }
        let rest = &parsed.path()[self.path.len()..];
        let (provider, tail) = rest
            .split_once('/')
            .filter(|(provider, _)| !provider.is_empty())
            .ok_or_else(invalid)?;
        // URL.search omits an empty query separator, and never includes fragment.
        let endpoint = match parsed.query().filter(|query| !query.is_empty()) {
            Some(query) => format!("{tail}?{query}"),
            None => tail.to_owned(),
        };
        let text = match &request.body {
            FetchBody::Text(text) => std::borrow::Cow::Borrowed(text.as_str()),
            FetchBody::Bytes(bytes) => {
                let bytes = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
                String::from_utf8_lossy(bytes)
            }
        };
        let mut deserializer = serde_json::Deserializer::from_str(&text);
        // This explicit binding instance mirrors JSON.parse's lack of a fixed
        // depth-128 cap. Other decoders retain their defaults, even when unified.
        deserializer.disable_recursion_limit();
        let mut query = Value::deserialize(&mut deserializer).map_err(|_| invalid())?;
        deserializer.end().map_err(|_| invalid())?;
        normalize_json_parse(&mut query)?;
        let mut headers = FetchHeaderRecord::new();
        let mut add = |name: &str, value: &str| {
            let name = name.to_lowercase();
            if !matches!(
                name.as_str(),
                "content-length" | "host" | "cf-aig-authorization" | "__proto__"
            ) {
                headers.insert(name, value.to_owned());
            }
        };
        match &request.headers {
            FetchHeaders::Record(record) => {
                for (name, value) in record.iter() {
                    add(name, value);
                }
            }
            FetchHeaders::Constructed(constructed) => {
                let names: BTreeSet<_> =
                    constructed.iter_for_fetch().map(|(name, _)| name).collect();
                for name in names {
                    let value = if name == "set-cookie" {
                        constructed
                            .iter_for_fetch()
                            .filter(|(key, _)| *key == name)
                            .last()
                            .map(|(_, value)| value.to_owned())
                    } else {
                        constructed.get_for_fetch(name)
                    }
                    .ok_or_else(invalid)?;
                    if value.chars().any(|c| u32::from(c) > 255) {
                        return Err(invalid());
                    }
                    add(name, &value);
                }
            }
        }
        Ok(AiGatewayUniversalRequestLike {
            provider: provider.into(),
            endpoint,
            headers,
            query: WirePayload::new(query),
        })
    }
}
fn normalize_json_parse(value: &mut Value) -> Result<(), TransportError> {
    match value {
        Value::Number(number) => {
            *number = serde_json::Number::from_f64(number.as_f64().ok_or_else(invalid)?)
                .ok_or_else(invalid)?;
        }
        Value::Array(values) => {
            for value in values {
                normalize_json_parse(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                normalize_json_parse(value)?;
            }
            // JSON.parse creates ordinary own properties: numeric indices
            // enumerate first, while other keys retain their original slots.
            // serde already replaces duplicate exact keys in their first slot.
            let mut entries: Vec<_> = std::mem::take(values).into_iter().collect();
            entries.sort_by_key(|(key, _)| {
                key.parse::<u32>()
                    .ok()
                    .filter(|index| *index != u32::MAX && index.to_string() == *key)
                    .map_or((1, 0), |index| (0, index))
            });
            values.extend(entries);
        }
        _ => {}
    }
    Ok(())
}
impl CustomFetch for GatewayBindingFetch {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            if runtime.cancellation.is_cancelled() {
                return Err(TransportError::new(TransportErrorKind::Cancelled));
            }
            let data = self.prepare(request)?;
            let gateway = self.binding.gateway(&self.gateway)?;
            gateway.run(&data, runtime).await
        })
    }
}
