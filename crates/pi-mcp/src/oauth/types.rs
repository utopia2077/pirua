use crate::{BoxFuture, Object, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub token_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_token: Option<String>,
}
impl OAuthTokens {
    pub(crate) fn parse(mut value: Value) -> Result<Self> {
        for key in ["scope", "refresh_token", "id_token", "expires_in"] {
            if value
                .get(key)
                .is_some_and(|v| v.is_null() || v.as_str() == Some(""))
            {
                value.as_object_mut().unwrap().remove(key);
            }
        }
        if let Some(expiry) = value.get("expires_in").and_then(Value::as_str) {
            value["expires_in"] = Value::from(
                expiry
                    .parse::<f64>()
                    .map_err(|_| crate::Error::invalid("invalid token expiry"))?,
            );
        }
        let result: Self = serde_json::from_value(value)?;
        if result.access_token.is_empty() || result.token_type.is_empty() {
            return Err(crate::Error::invalid("empty OAuth token"));
        }
        Ok(result)
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ClientInformation {
    pub client_id: String,
    #[serde(
        default,
        deserialize_with = "optional_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_secret: Option<String>,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProtectedResourceMetadata {
    pub resource: String,
    #[serde(default, deserialize_with = "null_vec")]
    pub authorization_servers: Vec<String>,
    #[serde(default, deserialize_with = "null_vec")]
    pub scopes_supported: Vec<String>,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthorizationServerMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub response_types_supported: Vec<String>,
    #[serde(default)]
    pub registration_endpoint: Option<String>,
    #[serde(default, deserialize_with = "null_vec")]
    pub token_endpoint_auth_methods_supported: Vec<String>,
    #[serde(default)]
    pub code_challenge_methods_supported: Option<Vec<String>>,
    #[serde(default)]
    pub authorization_response_iss_parameter_supported: bool,
    #[serde(flatten)]
    pub extra: Object,
}
#[derive(Clone, Debug, Default)]
pub struct OAuthChallenge {
    pub resource_metadata_url: Option<url::Url>,
    pub scope: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveryState {
    pub authorization_server_url: String,
    pub authorization_server_metadata: Option<AuthorizationServerMetadata>,
    pub resource_metadata: Option<ProtectedResourceMetadata>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthState {
    pub server_url: String,
    pub client_information: Option<ClientInformation>,
    pub tokens: Option<OAuthTokens>,
    pub tokens_expire_at_ms: Option<u64>,
    pub code_verifier: Option<String>,
    pub oauth_state: Option<String>,
    pub discovery: Option<DiscoveryState>,
}
impl OAuthState {
    pub fn new(server_url: String) -> Self {
        Self {
            server_url,
            client_information: None,
            tokens: None,
            tokens_expire_at_ms: None,
            code_verifier: None,
            oauth_state: None,
            discovery: None,
        }
    }
}
pub trait OAuthStateStore: Send + Sync {
    fn load(&self) -> BoxFuture<'_, Result<Option<OAuthState>>>;
    fn save(&self, state: OAuthState) -> BoxFuture<'_, Result<()>>;
}
#[derive(Default)]
pub struct MemoryOAuthStateStore(Mutex<Option<OAuthState>>);
impl OAuthStateStore for MemoryOAuthStateStore {
    fn load(&self) -> BoxFuture<'_, Result<Option<OAuthState>>> {
        Box::pin(async { Ok(self.0.lock().unwrap().clone()) })
    }
    fn save(&self, state: OAuthState) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            *self.0.lock().unwrap() = Some(state);
            Ok(())
        })
    }
}

/// Optional HTTPS client ID metadata document, as supported by the target MCP
/// OAuth subset. The host chooses when the authorization server supports it.
#[derive(Clone, Debug)]
pub struct ClientMetadataDocument {
    pub url: url::Url,
    pub redirect_url: url::Url,
}

pub type RedirectHandler =
    std::sync::Arc<dyn Fn(url::Url) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Optional host authentication for token requests, e.g. private_key_jwt.
pub trait TokenAuthenticator: Send + Sync {
    fn apply<'a>(
        &'a self,
        headers: &'a mut reqwest::header::HeaderMap,
        params: &'a mut Vec<(String, String)>,
        endpoint: &'a url::Url,
        metadata: Option<&'a AuthorizationServerMetadata>,
    ) -> BoxFuture<'a, Result<()>>;
}

fn null_vec<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error> {
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}
fn optional_string<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.filter(|value| !value.is_empty()))
}
