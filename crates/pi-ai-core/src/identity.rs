//! Explicit calling-application identity, applied below configured headers.
use crate::Presence;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// User-Agent presentation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UserAgentIdentity {
    /// A named standard presentation.
    Mode(UserAgentMode),
    /// Exact caller-owned header value.
    Custom {
        /// Custom User-Agent value.
        custom: String,
    },
}
/// Standard User-Agent modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserAgentMode {
    /// Product, version and host runtime.
    Detailed,
    /// Minimal product identity.
    Short,
}
/// Optional provider attribution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAttribution {
    /// Application name.
    pub name: String,
    /// Application URL.
    pub url: String,
    /// Optional application categories.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub categories: Option<Vec<String>>,
}
/// Calling application identity; absent fields retain Pi defaults.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientIdentity {
    /// User-Agent presentation.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub user_agent: Option<UserAgentIdentity>,
    /// Application version for detailed presentation.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub version: Option<String>,
    /// Provider attribution; explicit null disables an inherited attribution.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub attribution: Presence<ClientAttribution>,
}
/// Merges defaults with request fields individually.
pub fn merge_client_identity(
    base: Option<&ClientIdentity>,
    request: Option<&ClientIdentity>,
) -> ClientIdentity {
    let base = base.cloned().unwrap_or_default();
    let request = request.cloned().unwrap_or_default();
    ClientIdentity {
        user_agent: request.user_agent.or(base.user_agent),
        version: request.version.or(base.version),
        attribution: if request.attribution.is_missing() {
            base.attribution
        } else {
            request.attribution
        },
    }
}
/// Formats an explicit User-Agent; the native runtime describes this Rust implementation.
pub fn format_user_agent(identity: Option<&ClientIdentity>) -> String {
    match identity.and_then(|identity| identity.user_agent.as_ref()) {
        Some(UserAgentIdentity::Custom { custom }) => custom.clone(),
        Some(UserAgentIdentity::Mode(UserAgentMode::Short)) => "pi".into(),
        _ => format!(
            "pi/{} ({}; rust; {})",
            identity
                .and_then(|identity| identity.version.as_deref())
                .unwrap_or("1.4.0"),
            std::env::consts::OS,
            std::env::consts::ARCH
        ),
    }
}
/// Identity defaults for an open provider/endpoint, before model and request overrides.
pub fn get_identity_headers(
    provider: &str,
    base_url: &str,
    identity: Option<&ClientIdentity>,
) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::from([("User-Agent".into(), format_user_agent(identity))]);
    if let Some(Presence::Value(attribution)) = identity.map(|identity| &identity.attribution) {
        if provider == "openrouter" || base_url.contains("openrouter.ai") {
            headers.insert("HTTP-Referer".into(), attribution.url.clone());
            headers.insert("X-OpenRouter-Title".into(), attribution.name.clone());
            if let Some(categories) = attribution
                .categories
                .as_ref()
                .filter(|categories| !categories.is_empty())
            {
                headers.insert("X-OpenRouter-Categories".into(), categories.join(","));
            }
        } else if provider == "nvidia"
            || base_url.split('/').nth(2) == Some("integrate.api.nvidia.com")
        {
            let mut chars = attribution.name.chars();
            let origin = chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default();
            headers.insert("X-BILLING-INVOKE-ORIGIN".into(), origin);
        } else if matches!(provider, "opencode" | "opencode-go")
            || base_url.split('/').nth(2) == Some("opencode.ai")
        {
            headers.insert("x-opencode-client".into(), attribution.name.clone());
        }
    }
    headers
}
