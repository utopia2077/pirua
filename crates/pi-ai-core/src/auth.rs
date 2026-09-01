//! Serializable authentication and credential DTOs without store or login behavior.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::id::ProviderId;
use crate::json::{JsonValue, SafeUnsigned};
use crate::options::{ProviderEnv, ProviderHeaders};

const OAUTH_RESERVED_EXTRA_KEYS: [&str; 4] = ["type", "refresh", "access", "expires"];

/// Error returned when OAuth extension data shadows a reserved credential field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OAuthExtraError {
    key: String,
}

impl fmt::Display for OAuthExtraError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "OAuth extension key {:?} is reserved", self.key)
    }
}

impl std::error::Error for OAuthExtraError {}

/// Provider-specific OAuth extension fields that cannot shadow canonical keys.
#[derive(Clone, Default, PartialEq)]
pub struct OAuthExtraFields(BTreeMap<String, JsonValue>);

impl OAuthExtraFields {
    /// Validates and creates provider-specific OAuth extension fields.
    pub fn new(values: BTreeMap<String, JsonValue>) -> Result<Self, OAuthExtraError> {
        if let Some(key) = values
            .keys()
            .find(|key| OAUTH_RESERVED_EXTRA_KEYS.contains(&key.as_str()))
        {
            return Err(OAuthExtraError { key: key.clone() });
        }
        Ok(Self(values))
    }

    /// Returns the validated provider-specific fields.
    pub const fn as_map(&self) -> &BTreeMap<String, JsonValue> {
        &self.0
    }
}

impl fmt::Debug for OAuthExtraFields {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.0.keys().map(String::as_str).collect();
        formatter
            .debug_tuple("OAuthExtraFields")
            .field(&keys)
            .finish()
    }
}

impl Serialize for OAuthExtraFields {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OAuthExtraFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = crate::json::deserialize_btree_map_reject_duplicates(deserializer)?;
        Self::new(values).map_err(serde::de::Error::custom)
    }
}

/// Secret string whose debug representation never reveals its contents.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    /// Wraps a secret for controlled serialization and redacted debugging.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the secret only to an authorized transient runtime boundary.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([REDACTED])")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self)
    }
}

/// Request authentication resolved for a single model call.
///
/// This DTO is transient and must not enter durable Harness state.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelAuth {
    /// Optional provider API key or bearer token.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub api_key: Option<SecretString>,
    /// Optional raw transient request headers.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub headers: Option<ProviderHeaders>,
    /// Optional credential-selected base URL.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub base_url: Option<String>,
}

impl fmt::Debug for ModelAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelAuth")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field("headers", &self.headers)
            .field("base_url", &self.base_url.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Literal API-key credential tag.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ApiKeyCredentialType {
    /// The `api_key` credential type.
    #[default]
    #[serde(rename = "api_key")]
    ApiKey,
}

/// Stored API-key credential.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiKeyCredential {
    /// Literal `api_key` tag.
    #[serde(rename = "type")]
    pub credential_type: ApiKeyCredentialType,
    /// Optional stored key.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub key: Option<SecretString>,
    /// Optional provider-scoped environment values.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub env: Option<ProviderEnv>,
}

/// OAuth token data plus explicit provider-specific JSON extension fields.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct OAuthCredentials {
    /// Refresh token.
    pub refresh: SecretString,
    /// Access token.
    pub access: SecretString,
    /// Unix expiry timestamp in milliseconds.
    pub expires: SafeUnsigned,
    /// Provider-specific credential fields preserved verbatim.
    #[serde(flatten)]
    pub extra: OAuthExtraFields,
}

impl fmt::Debug for OAuthCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthCredentials")
            .field("refresh", &"[REDACTED]")
            .field("access", &"[REDACTED]")
            .field("expires", &self.expires)
            .field("extra", &self.extra)
            .finish()
    }
}

/// Literal OAuth credential tag.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum OAuthCredentialType {
    /// The `oauth` credential type.
    #[default]
    #[serde(rename = "oauth")]
    OAuth,
}

/// Stored canonical OAuth credential.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthCredential {
    /// Literal `oauth` tag.
    #[serde(rename = "type")]
    pub credential_type: OAuthCredentialType,
    /// OAuth token fields and provider extensions.
    #[serde(flatten)]
    pub credentials: OAuthCredentials,
}

/// Closed stored credential union.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Credential {
    /// API-key credential.
    ApiKey(ApiKeyCredential),
    /// OAuth credential.
    OAuth(OAuthCredential),
}

/// Closed authentication method.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthType {
    /// API-key or ambient authentication.
    #[serde(rename = "api_key")]
    ApiKey,
    /// OAuth authentication.
    #[serde(rename = "oauth")]
    OAuth,
}

/// Non-secret credential metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialInfo {
    /// Provider that owns the credential.
    pub provider_id: ProviderId,
    /// Stored credential type.
    #[serde(rename = "type")]
    pub auth_type: AuthType,
}

/// Result of resolving transient request authentication.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthResult {
    /// Resolved request authentication.
    pub auth: ModelAuth,
    /// Optional resolved provider environment.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub env: Option<ProviderEnv>,
    /// Optional human-readable source label.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub source: Option<String>,
}

/// Non-secret authentication availability result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthCheck {
    /// Optional human-readable source label.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub source: Option<String>,
    /// Available authentication type.
    #[serde(rename = "type")]
    pub auth_type: AuthType,
}

/// Link shown with an authentication instruction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthInfoLink {
    /// Link target.
    pub url: String,
    /// Optional display label.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub label: Option<String>,
}

/// Selectable authentication prompt option.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSelectOption {
    /// Value returned when selected.
    pub id: String,
    /// Human-readable label.
    pub label: String,
    /// Optional explanatory text.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
}

/// Authentication prompt data without a process-local cancellation handle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum AuthPrompt {
    /// Plain-text input prompt.
    #[serde(rename = "text")]
    Text {
        /// Prompt message.
        message: String,
        /// Optional input placeholder.
        #[serde(
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        placeholder: Option<String>,
    },
    /// Secret input prompt.
    #[serde(rename = "secret")]
    Secret {
        /// Prompt message.
        message: String,
        /// Optional input placeholder.
        #[serde(
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        placeholder: Option<String>,
    },
    /// Select-one prompt.
    #[serde(rename = "select")]
    Select {
        /// Prompt message.
        message: String,
        /// Available options.
        options: Vec<AuthSelectOption>,
    },
    /// Manual authorization-code prompt.
    #[serde(rename = "manual_code")]
    ManualCode {
        /// Prompt message.
        message: String,
        /// Optional input placeholder.
        #[serde(
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        placeholder: Option<String>,
    },
}

/// Authentication progress event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthEvent {
    /// Informational message and optional links.
    Info {
        /// Event message.
        message: String,
        /// Related links.
        #[serde(
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        links: Option<Vec<AuthInfoLink>>,
    },
    /// Authorization URL.
    AuthUrl {
        /// Browser authorization URL.
        url: String,
        /// Optional user instructions.
        #[serde(
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        instructions: Option<String>,
    },
    /// Device-code authorization data.
    DeviceCode {
        /// User-facing code.
        #[serde(rename = "userCode")]
        user_code: String,
        /// Verification page URI.
        #[serde(rename = "verificationUri")]
        verification_uri: String,
        /// Suggested polling interval in seconds.
        #[serde(
            rename = "intervalSeconds",
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        interval_seconds: Option<SafeUnsigned>,
        /// Device-code expiry in seconds.
        #[serde(
            rename = "expiresInSeconds",
            default,
            deserialize_with = "crate::json::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        expires_in_seconds: Option<SafeUnsigned>,
    },
    /// Progress message.
    Progress {
        /// Event message.
        message: String,
    },
}

/// Closed error code used by the Models runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ModelsErrorCode {
    /// Model source failed.
    #[serde(rename = "model_source")]
    ModelSource,
    /// Model data failed validation.
    #[serde(rename = "model_validation")]
    ModelValidation,
    /// Provider selection or capability failure.
    #[serde(rename = "provider")]
    Provider,
    /// Stream setup or execution failure.
    #[serde(rename = "stream")]
    Stream,
    /// API-key or credential-store failure.
    #[serde(rename = "auth")]
    Auth,
    /// OAuth login, refresh, or derivation failure.
    #[serde(rename = "oauth")]
    OAuth,
}
