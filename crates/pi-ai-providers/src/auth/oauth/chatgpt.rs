//! Sign in with ChatGPT direct Responses-token authorization at Pi v1.4.0.
use super::{common::*, page::page};
use crate::auth::{OAuthServices, checked_oauth_expiry};
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthPrompt, AuthProviderError, BoxFuture, CancellationToken,
    JsonValue, ModelAuth, OAuthAuth, OAuthCredential, OAuthCredentialType, OAuthCredentials,
    OAuthExtraFields, SecretString,
};
use pi_ai_transport::{CallbackBind, CallbackSession, generate_pkce};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    task::Poll,
};
use url::Url;
const AUTHORIZE_URL: &str = "https://auth.openai.com/api/accounts/authorize";
const TOKEN_URL: &str = "https://auth.openai.com/api/accounts/oauth/token";
const REDIRECT: &str = "http://127.0.0.1:1455/auth/callback";
const RESOURCE: &str = "https://api.openai.com/v1";
const DIRECT_SCOPE: &str = "chatgpt.tokens.use.direct";

/// Public-client dynamic-registration flow. Tokens are sent directly to api.openai.com.
#[derive(Clone)]
pub struct OpenAIChatGPTOAuth {
    services: OAuthServices,
}
impl OpenAIChatGPTOAuth {
    /// Captures explicit host capabilities without invoking them.
    pub fn new(services: OAuthServices) -> Self {
        Self { services }
    }
    async fn request(
        &self,
        body: String,
        client_id: &str,
        require_id_token: bool,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let response = post(
            &self.services,
            TOKEN_URL,
            &[
                ("accept", "application/json"),
                ("content-type", "application/x-www-form-urlencoded"),
            ],
            body,
            c,
            None,
            TokenRead::ChatGPT,
        )
        .await?;
        if !response.ok() {
            return Err(OAuthFlowError::http(
                OAuthFlowErrorKind::Exchange,
                response.status,
            ));
        }
        let value = response.json()?;
        if !value.is_object() {
            return Err(invalid());
        }
        let access = nonempty(&value, "access_token")?;
        let refresh = nonempty(&value, "refresh_token")?;
        let scope = nonempty(&value, "scope")?;
        let expires = value
            .get("expires_in")
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v > 0.0)
            .ok_or_else(invalid)?;
        if require_id_token {
            nonempty(&value, "id_token")?;
        }
        let scopes = scope
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if !scopes.iter().any(|scope| scope == DIRECT_SCOPE) {
            return Err(invalid());
        }
        Ok(OAuthCredential {
            credential_type: OAuthCredentialType::OAuth,
            credentials: OAuthCredentials {
                access: SecretString::new(access),
                refresh: SecretString::new(refresh),
                expires: checked_oauth_expiry(self.services.clock.now_unix_ms(), expires, 180_000)
                    .map_err(host)?,
                extra: OAuthExtraFields::new(BTreeMap::from([
                    ("clientId".into(), JsonValue::String(client_id.into())),
                    (
                        "scopes".into(),
                        JsonValue::Array(scopes.into_iter().map(JsonValue::String).collect()),
                    ),
                ]))
                .map_err(host)?,
            },
        })
    }
    fn random_value(&self) -> Result<String> {
        let mut bytes = [0; 32];
        self.services.crypto.fill_random(&mut bytes).map_err(host)?;
        // Identical unpadded base64url representation to randomBytes(32).
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let a = chunk[0];
            let b = *chunk.get(1).unwrap_or(&0);
            let d = *chunk.get(2).unwrap_or(&0);
            out.push(alphabet[(a >> 2) as usize] as char);
            out.push(alphabet[(((a & 3) << 4) | (b >> 4)) as usize] as char);
            if chunk.len() > 1 {
                out.push(alphabet[(((b & 15) << 2) | (d >> 6)) as usize] as char);
            }
            if chunk.len() > 2 {
                out.push(alphabet[(d & 63) as usize] as char);
            }
        }
        Ok(out)
    }
    async fn login_flow(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let device = self
            .services
            .device_id
            .as_deref()
            .filter(|id| valid_uuid(id))
            .ok_or_else(invalid)?;
        let host_id = format!("urn:uuid:{}", device.to_ascii_lowercase());
        let pkce = generate_pkce(self.services.crypto.as_ref()).map_err(host)?;
        let state = self.random_value()?;
        let nonce = self.random_value()?;
        // Listener ownership is mandatory, including when manual callback input is used.
        let mut session = self
            .services
            .callbacks
            .bind(
                CallbackBind {
                    host: self.services.callback_host.clone(),
                    port: 1455,
                    limits: self.services.callback_limits,
                },
                c,
            )
            .await
            .map_err(host)?;
        let manual = CancelOnDrop(CancellationToken::new());
        let authorization_url = format!(
            "{AUTHORIZE_URL}?{}",
            form(&[
                ("client_id", "dynamic_agent_client"),
                (
                    "agent_name_hint",
                    self.services.agent_name.as_deref().unwrap_or("Pi")
                ),
                ("ext_agent_host_id", &host_id),
                ("response_type", "code"),
                ("redirect_uri", REDIRECT),
                ("resource", RESOURCE),
                (
                    "scope",
                    "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct"
                ),
                ("state", &state),
                ("code_challenge", &pkce.challenge),
                ("code_challenge_method", "S256"),
                ("nonce", &nonce),
            ])
        );
        let result=async {
            i.notify(AuthEvent::AuthUrl{url:authorization_url,instructions:Some("Complete sign-in in your browser. If the callback does not complete, paste the final redirect URL here.".into())}).map_err(host)?;
            let (code,client_id)=authorize(i,c,&state,session.as_mut(),&manual.0).await?;
            i.notify(AuthEvent::Progress{message:"Exchanging authorization code for tokens...".into()}).map_err(host)?;
            self.request(form(&[("grant_type","authorization_code"),("client_id",&client_id),("code",&code),("code_verifier",pkce.verifier.expose_secret()),("redirect_uri",REDIRECT),("resource",RESOURCE)]),&client_id,true,c).await
        }.await;
        manual.0.cancel();
        // Definitively release both the listener and spare accepted browser connections.
        let closed = session.close().await.map_err(host);
        match result {
            Ok(credential) => {
                closed?;
                Ok(credential)
            }
            Err(error) => Err(error),
        }
    }
}
fn valid_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(index, b)| {
            if [8, 13, 18, 23].contains(&index) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}
fn callback(url: &Url, state: &str) -> Result<(String, String)> {
    let code = query(url, "code")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| OAuthFlowError::new(OAuthFlowErrorKind::MissingCode))?;
    let actual = query(url, "state")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| OAuthFlowError::new(OAuthFlowErrorKind::MissingState))?;
    if actual != state {
        return Err(OAuthFlowError::new(OAuthFlowErrorKind::StateMismatch));
    }
    let client_id = query(url, "client_id")
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .ok_or_else(invalid)?;
    Ok((code, client_id))
}
fn manual_callback(input: &str, state: &str) -> Result<(String, String)> {
    let url = Url::parse(input.trim()).map_err(|_| invalid())?;
    let expected = Url::parse(REDIRECT).expect("constant redirect");
    if url.origin() != expected.origin() || url.path() != expected.path() {
        return Err(invalid());
    }
    if query(&url, "error").is_some_and(|error| !error.is_empty()) {
        return Err(OAuthFlowError::new(OAuthFlowErrorKind::AuthorizationDenied));
    }
    callback(&url, state)
}
async fn authorize(
    i: &dyn AuthInteraction,
    c: &CancellationToken,
    state: &str,
    session: &mut dyn CallbackSession,
    manual_cancel: &CancellationToken,
) -> Result<(String, String)> {
    let mut manual = i.prompt(
        AuthPrompt::ManualCode {
            message: "Complete login in your browser, or paste the final redirect URL here:".into(),
            placeholder: Some(REDIRECT.into()),
        },
        manual_cancel,
    );
    loop {
        enum Choice {
            Manual(std::result::Result<String, pi_ai_core::AuthInteractionError>),
            Request(
                std::result::Result<
                    pi_ai_transport::CallbackRequest,
                    pi_ai_transport::AuthHostError,
                >,
            ),
            Cancelled,
        }
        let choice = {
            let mut next = session.next(c);
            let mut cancel = Box::pin(c.cancelled());
            poll_fn(|cx| {
                if cancel.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Choice::Cancelled);
                }
                if let Poll::Ready(value) = manual.as_mut().poll(cx) {
                    return Poll::Ready(Choice::Manual(value));
                }
                next.as_mut().poll(cx).map(Choice::Request)
            })
            .await
        };
        match choice {
            Choice::Cancelled => return Err(cancelled()),
            Choice::Manual(value) => return manual_callback(&value.map_err(host)?, state),
            Choice::Request(request) => {
                let request = request.map_err(host)?;
                let url = Url::parse(REDIRECT)
                    .expect("constant redirect")
                    .join(&request.target)
                    .map_err(|_| invalid());
                let (response, result, terminal) = match url {
                    Err(error) => (
                        page(
                            500,
                            false,
                            "Internal error while processing the callback.",
                            None,
                            false,
                        ),
                        Err(error),
                        false,
                    ),
                    Ok(url) if url.path() != "/auth/callback" => (
                        page(404, false, "Callback route not found.", None, false),
                        Err(invalid()),
                        false,
                    ),
                    Ok(url) if query(&url, "error").is_some_and(|e| !e.is_empty()) => (
                        page(400, false, "ChatGPT was not connected.", None, false),
                        Err(OAuthFlowError::new(OAuthFlowErrorKind::AuthorizationDenied)),
                        true,
                    ),
                    Ok(url) => match callback(&url, state) {
                        Ok(result) => (
                            page(
                                200,
                                true,
                                "ChatGPT authentication completed. You can close this window.",
                                None,
                                false,
                            ),
                            Ok(result),
                            true,
                        ),
                        Err(error) => (
                            page(400, false, &error.to_string(), None, false),
                            Err(error),
                            false,
                        ),
                    },
                };
                acknowledge(request.reply, response, c).await?;
                if terminal {
                    return result;
                }
            }
        }
    }
}
impl OAuthAuth for OpenAIChatGPTOAuth {
    fn name(&self) -> &str {
        "OpenAI (ChatGPT subscription)"
    }
    fn is_subscription(&self) -> bool {
        true
    }
    fn login_label(&self) -> Option<&str> {
        Some("Sign in with ChatGPT")
    }
    fn login<'a>(
        &'a self,
        i: &'a dyn AuthInteraction,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move { self.login_flow(i, c).await.map_err(provider) })
    }
    fn refresh<'a>(
        &'a self,
        v: &'a OAuthCredential,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            let client = v
                .credentials
                .extra
                .as_map()
                .get("clientId")
                .and_then(|v| {
                    if let JsonValue::String(v) = v {
                        Some(v.as_str())
                    } else {
                        None
                    }
                })
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| provider(invalid()))?;
            self.request(
                form(&[
                    ("grant_type", "refresh_token"),
                    ("client_id", client),
                    ("refresh_token", v.credentials.refresh.expose_secret()),
                    ("resource", RESOURCE),
                ]),
                client,
                false,
                c,
            )
            .await
            .map_err(provider)
        })
    }
    fn to_auth<'a>(
        &'a self,
        v: &'a OAuthCredential,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move {
            Ok(ModelAuth {
                api_key: Some(v.credentials.access.clone()),
                ..Default::default()
            })
        })
    }
}

fn nonempty<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(invalid)
}
