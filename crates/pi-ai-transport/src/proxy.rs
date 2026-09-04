use crate::{TransportError, TransportErrorKind};
use std::{collections::BTreeMap, fmt};
use url::Url;

#[derive(Clone)]
pub struct ProxyUrl(Url);
impl ProxyUrl {
    pub fn parse(value: &str) -> Result<Self, TransportError> {
        let url =
            Url::parse(value).map_err(|_| TransportError::new(TransportErrorKind::InvalidProxy))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(TransportError::new(TransportErrorKind::UnsupportedProxy));
        }
        if url.host_str().is_none() {
            return Err(TransportError::new(TransportErrorKind::InvalidProxy));
        }
        Ok(Self(url))
    }
    pub fn expose_for_transport(&self) -> &str {
        self.0.as_str()
    }
}
impl fmt::Debug for ProxyUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProxyUrl([REDACTED])")
    }
}

/// Explicit environment snapshot. Resolution never consults ambient process state.
#[derive(Clone, Default)]
pub struct ProxyEnvironment {
    pub scoped: BTreeMap<String, String>,
    pub ambient: BTreeMap<String, String>,
}
impl fmt::Debug for ProxyEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProxyEnvironment([REDACTED])")
    }
}
impl ProxyEnvironment {
    fn get(&self, key: &str) -> &str {
        let upper = key.to_ascii_uppercase();
        [
            self.scoped.get(key),
            self.scoped.get(&upper),
            self.ambient.get(key),
            self.ambient.get(&upper),
        ]
        .into_iter()
        .flatten()
        .find(|s| !s.is_empty())
        .map(String::as_str)
        .unwrap_or("")
    }
    /// `websocket_as_http` reproduces Codex's explicit ws/wss proxy rewrite.
    pub fn resolve(
        &self,
        target: &str,
        websocket_as_http: bool,
    ) -> Result<Option<ProxyUrl>, TransportError> {
        let Ok(url) = Url::parse(target) else {
            return Ok(None);
        };
        let Some(host) = url.host_str() else {
            return Ok(None);
        };
        let scheme = match (websocket_as_http, url.scheme()) {
            (true, "ws") => "http",
            (true, "wss") => "https",
            (_, s) => s,
        };
        let port = url.port_or_known_default().unwrap_or(0);
        let no_proxy = self.get("no_proxy").to_ascii_lowercase();
        if no_proxy == "*" {
            return Ok(None);
        }
        for item in no_proxy
            .split(|c: char| c == ',' || crate::is_ecma_whitespace(c))
            .filter(|s| !s.is_empty())
        {
            let (pattern, required_port) = item
                .rsplit_once(':')
                .filter(|(_, p)| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
                .map_or((item, None), |(h, p)| {
                    (h, Some(p.parse::<u64>().unwrap_or(u64::MAX)))
                });
            if required_port.is_some_and(|p| p > 0 && p != u64::from(port)) {
                continue;
            }
            let excluded = if pattern.starts_with(['.', '*']) {
                host.ends_with(pattern.strip_prefix('*').unwrap_or(pattern))
            } else {
                host == pattern
            };
            if excluded {
                return Ok(None);
            }
        }
        let specific = self.get(&format!("{scheme}_proxy"));
        let value = if specific.is_empty() {
            self.get("all_proxy")
        } else {
            specific
        };
        if value.is_empty() {
            return Ok(None);
        }
        ProxyUrl::parse(&if value.contains("://") {
            value.to_owned()
        } else {
            format!("{scheme}://{value}")
        })
        .map(Some)
    }
}
