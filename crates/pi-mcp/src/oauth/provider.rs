use super::{
    discovery::{discover_authorization_server, secure_endpoint},
    *,
};
use crate::{
    BoxFuture, CancellationToken, Error, Object, Result,
    transport::{AuthProvider, UnauthorizedContext},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use reqwest::{
    Client,
    header::{HeaderMap, HeaderValue},
};
use ring::{
    digest,
    rand::{SecureRandom, SystemRandom},
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, watch};
use url::Url;

#[derive(Clone)]
pub struct OAuthOptions {
    pub server_url: Url,
    pub redirect_url: Url,
    pub client_metadata: Object,
    pub client_information: Option<ClientInformation>,
    pub client_metadata_document: Option<ClientMetadataDocument>,
    pub store: Arc<dyn OAuthStateStore>,
    pub client: Option<Client>,
    pub on_redirect: Option<RedirectHandler>,
    pub token_authenticator: Option<Arc<dyn TokenAuthenticator>>,
}
impl OAuthOptions {
    pub fn new(server_url: Url, redirect_url: Url, client_metadata: Object) -> Self {
        Self {
            server_url,
            redirect_url,
            client_metadata,
            client_information: None,
            client_metadata_document: None,
            store: Arc::new(MemoryOAuthStateStore::default()),
            client: None,
            on_redirect: None,
            token_authenticator: None,
        }
    }
}
#[derive(Clone, Default)]
pub struct FlowOptions {
    pub authorization_code: Option<String>,
    pub issuer: Option<String>,
    pub scope: Option<String>,
    pub resource_metadata_url: Option<Url>,
    pub authorization_server_metadata_url: Option<Url>,
    pub skip_refresh: bool,
    pub cancellation: CancellationToken,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationResult {
    Authorized,
    Redirect(Url),
}
type RecoveryReceiver = watch::Receiver<Option<Result<()>>>;
struct Inner {
    options: OAuthOptions,
    client: Client,
    writes: Mutex<()>,
    recovery: Mutex<Option<RecoveryReceiver>>,
}
#[derive(Clone)]
pub struct OAuthProvider(Arc<Inner>);
impl OAuthProvider {
    pub fn new(mut options: OAuthOptions) -> Result<Self> {
        secure_endpoint(options.server_url.clone())?;
        options
            .client_metadata
            .entry("redirect_uris")
            .or_insert_with(|| json!([options.redirect_url.as_str()]));
        options
            .client_metadata
            .entry("grant_types")
            .or_insert_with(|| json!(["authorization_code", "refresh_token"]));
        options
            .client_metadata
            .entry("response_types")
            .or_insert_with(|| json!(["code"]));
        let auth_method = if options
            .client_information
            .as_ref()
            .is_some_and(|info| info.client_secret.is_some())
        {
            "client_secret_post"
        } else {
            "none"
        };
        options
            .client_metadata
            .entry("token_endpoint_auth_method")
            .or_insert_with(|| json!(auth_method));
        if let Some(document) = &options.client_metadata_document
            && (document.url.scheme() != "https" || document.url.path() == "/")
        {
            return Err(Error::invalid("invalid OAuth client metadata document URL"));
        }
        let client = match &options.client {
            Some(client) => client.clone(),
            None => Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        };
        Ok(Self(Arc::new(Inner {
            options,
            client,
            writes: Mutex::new(()),
            recovery: Mutex::new(None),
        })))
    }
    async fn load_unlocked(&self) -> Result<OAuthState> {
        let state = self.0.options.store.load().await?;
        Ok(state
            .filter(|state| state.server_url == self.0.options.server_url.as_str())
            .unwrap_or_else(|| OAuthState::new(self.0.options.server_url.to_string())))
    }
    pub async fn state(&self) -> Result<OAuthState> {
        let _guard = self.0.writes.lock().await;
        self.load_unlocked().await
    }
    async fn update(&self, update: impl FnOnce(&mut OAuthState)) -> Result<()> {
        let _guard = self.0.writes.lock().await;
        let mut state = self.load_unlocked().await?;
        update(&mut state);
        self.0.options.store.save(state).await
    }
    pub async fn save_tokens(&self, tokens: OAuthTokens) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let expiry = tokens
            .expires_in
            .map(|seconds| now.saturating_add((seconds.max(0.0) * 1000.0) as u64));
        self.update(|state| {
            state.tokens = Some(tokens);
            state.tokens_expire_at_ms = expiry;
        })
        .await
    }
    pub async fn invalidate(&self) -> Result<()> {
        self.update(|state| *state = OAuthState::new(self.0.options.server_url.to_string()))
            .await
    }
    /// Run one user-driven OAuth flow. Redirect is returned to the host and can
    /// also be forwarded through `on_redirect`; no browser or callback listener
    /// is opened implicitly.
    pub async fn authorize(&self, options: FlowOptions) -> Result<AuthorizationResult> {
        match self.run_flow(&options).await {
            Err(Error::OAuth { code, .. })
                if ["invalid_client", "unauthorized_client"].contains(&code.as_str()) =>
            {
                self.invalidate().await?;
                self.run_flow(&options).await
            }
            Err(Error::OAuth { code, .. }) if code == "invalid_grant" => {
                self.update(|state| {
                    state.tokens = None;
                    state.tokens_expire_at_ms = None;
                })
                .await?;
                self.run_flow(&options).await
            }
            result => result,
        }
    }
    async fn run_flow(&self, options: &FlowOptions) -> Result<AuthorizationResult> {
        if options.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let state = self.state().await?;
        let discovered = if options.authorization_server_metadata_url.is_none()
            && let Some(mut discovered) = state.discovery
        {
            if discovered.authorization_server_metadata.is_none() {
                let issuer = parse_url(&discovered.authorization_server_url)?;
                discovered.authorization_server_metadata =
                    discover_authorization_server(&self.0.client, &issuer, &options.cancellation)
                        .await?;
            }
            discovered
        } else {
            discover_server_info(
                &self.0.client,
                &self.0.options.server_url,
                options.resource_metadata_url.as_ref(),
                options.authorization_server_metadata_url.as_ref(),
                &options.cancellation,
            )
            .await?
        };
        if options.authorization_server_metadata_url.is_none() {
            self.update(|state| state.discovery = Some(discovered.clone()))
                .await?;
        }
        let metadata = discovered.authorization_server_metadata.as_ref();
        let issuer = parse_url(&discovered.authorization_server_url)?;
        let resource = select_resource(
            &self.0.options.server_url,
            discovered.resource_metadata.as_ref(),
        )?;
        let scope = options
            .scope
            .clone()
            .filter(|scope| !scope.is_empty())
            .or_else(|| {
                discovered
                    .resource_metadata
                    .as_ref()
                    .map(|resource| resource.scopes_supported.join(" "))
                    .filter(|scope| !scope.is_empty())
            })
            .or_else(|| {
                self.0
                    .options
                    .client_metadata
                    .get("scope")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        let stored = self
            .0
            .options
            .client_information
            .clone()
            .or(self.state().await?.client_information);
        let document = if stored.is_none() {
            self.0.options.client_metadata_document.as_ref()
        } else {
            None
        };
        let client = if let Some(client) = stored {
            client
        } else if let Some(document) = document {
            ClientInformation {
                client_id: document.url.to_string(),
                client_secret: None,
                extra: Object::new(),
            }
        } else {
            if options.authorization_code.is_some() {
                return Err(Error::invalid(
                    "OAuth client information missing during code exchange",
                ));
            }
            let client = self
                .register(&issuer, metadata, scope.as_deref(), &options.cancellation)
                .await?;
            self.update(|state| state.client_information = Some(client.clone()))
                .await?;
            client
        };
        let redirect = document
            .map(|document| &document.redirect_url)
            .unwrap_or(&self.0.options.redirect_url);
        if let Some(code) = &options.authorization_code {
            if (options.issuer.is_some()
                || metadata.is_some_and(|metadata| {
                    metadata.authorization_response_iss_parameter_supported
                }))
                && options.issuer.as_deref() != metadata.map(|metadata| metadata.issuer.as_str())
            {
                return Err(Error::OAuth {
                    code: "issuer_mismatch".into(),
                    message: "authorization response issuer does not match server".into(),
                });
            }
            let verifier = self
                .state()
                .await?
                .code_verifier
                .ok_or_else(|| Error::invalid("OAuth PKCE verifier missing"))?;
            let params = vec![
                ("grant_type".into(), "authorization_code".into()),
                ("code".into(), code.clone()),
                ("code_verifier".into(), verifier),
                ("redirect_uri".into(), redirect.to_string()),
            ];
            let mut tokens = self
                .token_request(
                    &issuer,
                    metadata,
                    &client,
                    resource.as_deref(),
                    params,
                    &options.cancellation,
                )
                .await?;
            if tokens.scope.is_none() {
                tokens.scope = scope;
            }
            self.save_tokens(tokens).await?;
            return Ok(AuthorizationResult::Authorized);
        }
        let existing = if options.skip_refresh {
            None
        } else {
            self.state().await?.tokens
        };
        if let Some(existing) = existing
            && let Some(refresh) = &existing.refresh_token
        {
            let params = vec![
                ("grant_type".into(), "refresh_token".into()),
                ("refresh_token".into(), refresh.clone()),
            ];
            match self
                .token_request(
                    &issuer,
                    metadata,
                    &client,
                    resource.as_deref(),
                    params,
                    &options.cancellation,
                )
                .await
            {
                Ok(mut tokens) => {
                    if tokens.refresh_token.is_none() {
                        tokens.refresh_token = Some(refresh.clone());
                    }
                    if tokens.scope.is_none() {
                        tokens.scope = existing.scope;
                    }
                    self.save_tokens(tokens).await?;
                    return Ok(AuthorizationResult::Authorized);
                }
                Err(error @ (Error::Cancelled | Error::OAuth { .. })) => {
                    if !matches!(&error, Error::OAuth { code, .. } if code == "server_error") {
                        return Err(error);
                    }
                }
                Err(_) => {}
            }
        }
        if metadata.is_some_and(|metadata| {
            !metadata
                .response_types_supported
                .iter()
                .any(|kind| kind == "code")
        }) {
            return Err(Error::invalid(
                "OAuth server does not support code response",
            ));
        }
        if metadata
            .and_then(|metadata| metadata.code_challenge_methods_supported.as_ref())
            .is_some_and(|methods| !methods.iter().any(|method| method == "S256"))
        {
            return Err(Error::invalid("OAuth server does not support PKCE S256"));
        }
        let verifier = random_string()?;
        let challenge =
            URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, verifier.as_bytes()).as_ref());
        let oauth_state = self.state().await?.oauth_state.unwrap_or(random_string()?);
        self.update(|state| {
            state.code_verifier = Some(verifier);
            state.oauth_state = Some(oauth_state.clone());
        })
        .await?;
        let mut authorization = endpoint(
            metadata.map(|metadata| metadata.authorization_endpoint.as_str()),
            &issuer,
            "/authorize",
        )?;
        {
            let mut generated = vec![
                ("response_type", "code"),
                ("client_id", client.client_id.as_str()),
                ("code_challenge", challenge.as_str()),
                ("code_challenge_method", "S256"),
                ("redirect_uri", redirect.as_str()),
                ("state", oauth_state.as_str()),
            ];
            if let Some(scope) = &scope {
                generated.push(("scope", scope.as_str()));
                if scope
                    .split_whitespace()
                    .any(|scope| scope == "offline_access")
                {
                    generated.push(("prompt", "consent"));
                }
            }
            if let Some(resource) = &resource {
                generated.push(("resource", resource.as_str()));
            }
            // Match URLSearchParams.set in the pinned target: replace the first
            // value and remove duplicates for each generated parameter, keeping
            // unrelated query pairs (and unsupplied optional parameters).
            let mut query: Vec<(String, String)> =
                authorization.query_pairs().into_owned().collect();
            for (name, value) in generated {
                let mut replaced = false;
                query.retain_mut(|(key, current)| {
                    if key != name {
                        return true;
                    }
                    if replaced {
                        return false;
                    }
                    *current = value.into();
                    replaced = true;
                    true
                });
                if !replaced {
                    query.push((name.into(), value.into()));
                }
            }
            authorization.query_pairs_mut().clear().extend_pairs(query);
        }
        if options.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(handler) = &self.0.options.on_redirect {
            handler(authorization.clone()).await?;
        }
        Ok(AuthorizationResult::Redirect(authorization))
    }
    async fn register(
        &self,
        issuer: &Url,
        metadata: Option<&AuthorizationServerMetadata>,
        scope: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<ClientInformation> {
        if metadata.is_some_and(|metadata| metadata.registration_endpoint.is_none()) {
            return Err(Error::invalid(
                "OAuth server does not support dynamic registration",
            ));
        }
        let endpoint = endpoint(
            metadata.and_then(|metadata| metadata.registration_endpoint.as_deref()),
            issuer,
            "/register",
        )?;
        let mut body = self.0.options.client_metadata.clone();
        if !body.contains_key("application_type") {
            let native = body
                .get("redirect_uris")
                .and_then(Value::as_array)
                .is_some_and(|uris| {
                    uris.iter()
                        .filter_map(Value::as_str)
                        .filter_map(|uri| Url::parse(uri).ok())
                        .any(|uri| {
                            !["http", "https"].contains(&uri.scheme())
                                || matches!(
                                    uri.host_str(),
                                    Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
                                )
                        })
                });
            body.insert(
                "application_type".into(),
                json!(if native { "native" } else { "web" }),
            );
        }
        if let Some(scope) = scope {
            body.insert("scope".into(), json!(scope));
        }
        let response = tokio::select! { _ = cancellation.cancelled() => return Err(Error::Cancelled), response = self.0.client.post(endpoint).header("accept","application/json").json(&body).send() => response? };
        let status = response.status().as_u16();
        let bytes = crate::transport::http::read_bounded(
            response,
            crate::DEFAULT_MAX_MESSAGE_BYTES,
            cancellation,
            cancellation,
        )
        .await?;
        if !(200..300).contains(&status) {
            return Err(Error::OAuth {
                code: "registration_failed".into(),
                message: format!("OAuth registration HTTP {status}"),
            });
        }
        let info: ClientInformation = serde_json::from_slice(&bytes)?;
        if info.client_id.is_empty() {
            return Err(Error::invalid("empty OAuth client ID"));
        }
        Ok(info)
    }
    async fn token_request(
        &self,
        issuer: &Url,
        metadata: Option<&AuthorizationServerMetadata>,
        client: &ClientInformation,
        resource: Option<&str>,
        mut params: Vec<(String, String)>,
        cancellation: &CancellationToken,
    ) -> Result<OAuthTokens> {
        let endpoint = endpoint(
            metadata.map(|metadata| metadata.token_endpoint.as_str()),
            issuer,
            "/token",
        )?;
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("application/json"));
        if let Some(resource) = resource {
            params.push(("resource".into(), resource.into()));
        }
        if let Some(authenticator) = &self.0.options.token_authenticator {
            authenticator
                .apply(&mut headers, &mut params, &endpoint, metadata)
                .await?;
        } else {
            let supported = metadata
                .map(|metadata| metadata.token_endpoint_auth_methods_supported.as_slice())
                .unwrap_or_default();
            let hint = client
                .extra
                .get("token_endpoint_auth_method")
                .and_then(Value::as_str);
            let method = hint
                .filter(|method| {
                    ["client_secret_basic", "client_secret_post", "none"].contains(method)
                        && (supported.is_empty()
                            || supported.iter().any(|supported| supported == method))
                })
                .unwrap_or_else(|| {
                    if supported.is_empty() {
                        if client.client_secret.is_some() {
                            "client_secret_basic"
                        } else {
                            "none"
                        }
                    } else if client.client_secret.is_some()
                        && supported
                            .iter()
                            .any(|method| method == "client_secret_basic")
                    {
                        "client_secret_basic"
                    } else if client.client_secret.is_some()
                        && supported
                            .iter()
                            .any(|method| method == "client_secret_post")
                    {
                        "client_secret_post"
                    } else if supported.iter().any(|method| method == "none") {
                        "none"
                    } else if client.client_secret.is_some() {
                        "client_secret_post"
                    } else {
                        "none"
                    }
                });
            if method == "client_secret_basic" {
                let secret = client
                    .client_secret
                    .as_deref()
                    .ok_or_else(|| Error::invalid("client_secret_basic requires secret"))?;
                let encoded = STANDARD.encode(format!("{}:{secret}", client.client_id));
                let mut value = HeaderValue::from_str(&format!("Basic {encoded}"))
                    .map_err(|_| Error::invalid("invalid client auth"))?;
                value.set_sensitive(true);
                headers.insert("authorization", value);
            } else {
                params.push(("client_id".into(), client.client_id.clone()));
                if method == "client_secret_post"
                    && let Some(secret) = &client.client_secret
                {
                    params.push(("client_secret".into(), secret.clone()));
                }
            }
        }
        let response = tokio::select! { _ = cancellation.cancelled() => return Err(Error::Cancelled), response = self.0.client.post(endpoint).headers(headers).form(&params).send() => response? };
        let status = response.status().as_u16();
        let bytes = crate::transport::http::read_bounded(
            response,
            crate::DEFAULT_MAX_MESSAGE_BYTES,
            cancellation,
            cancellation,
        )
        .await?;
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if let Some(code) = value.get("error").and_then(Value::as_str) {
            return Err(Error::OAuth {
                code: code.into(),
                message: value
                    .get("error_description")
                    .and_then(Value::as_str)
                    .unwrap_or(code)
                    .into(),
            });
        }
        if !(200..300).contains(&status) {
            return Err(Error::OAuth {
                code: "server_error".into(),
                message: format!("OAuth token HTTP {status}"),
            });
        }
        OAuthTokens::parse(value)
    }
    async fn recover(&self, context: UnauthorizedContext) -> Result<()> {
        if context.server_url != self.0.options.server_url {
            return Err(Error::invalid(
                "OAuth provider belongs to another MCP server",
            ));
        }
        let challenge = parse_www_authenticate(context.challenge.as_deref());
        let step_up = challenge.error.as_deref() == Some("insufficient_scope");
        let mut recovery = self.0.recovery.lock().await;
        let mut receiver = if let Some(receiver) = recovery.as_ref() {
            receiver.clone()
        } else {
            let current = self.state().await?.tokens;
            if !step_up
                && context.rejected_token.is_some()
                && current.as_ref().is_some_and(|tokens| {
                    Some(&tokens.access_token) != context.rejected_token.as_ref()
                })
            {
                return Ok(());
            }
            let (sender, receiver) = watch::channel(None);
            *recovery = Some(receiver.clone());
            let provider = self.clone();
            tokio::spawn(async move {
                let options = FlowOptions {
                    resource_metadata_url: challenge.resource_metadata_url,
                    scope: if step_up {
                        step_up_scope(
                            current.and_then(|tokens| tokens.scope).as_deref(),
                            challenge.scope.as_deref(),
                        )
                    } else {
                        challenge.scope
                    },
                    skip_refresh: step_up,
                    ..FlowOptions::default()
                };
                let result = match provider.authorize(options).await {
                    Ok(AuthorizationResult::Authorized) => Ok(()),
                    Ok(AuthorizationResult::Redirect(url)) => Err(Error::AuthorizationRequired {
                        authorization_url: url.to_string(),
                    }),
                    Err(error) => Err(error),
                };
                sender.send_replace(Some(result));
                *provider.0.recovery.lock().await = None;
            });
            receiver
        };
        drop(recovery);
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            receiver.changed().await.map_err(|_| Error::Closed)?;
        }
    }
}
impl AuthProvider for OAuthProvider {
    fn validate_server(&self, server: &Url) -> Result<()> {
        if server == &self.0.options.server_url {
            Ok(())
        } else {
            Err(Error::invalid(
                "OAuth provider belongs to another MCP server",
            ))
        }
    }
    fn token(&self) -> BoxFuture<'_, Result<Option<String>>> {
        Box::pin(async { Ok(self.state().await?.tokens.map(|tokens| tokens.access_token)) })
    }
    fn on_unauthorized(&self, context: UnauthorizedContext) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.recover(context))
    }
}
fn parse_url(value: &str) -> Result<Url> {
    Url::parse(value).map_err(|_| Error::invalid("invalid OAuth URL"))
}
fn endpoint(configured: Option<&str>, issuer: &Url, fallback: &str) -> Result<Url> {
    secure_endpoint(match configured {
        Some(value) => parse_url(value)?,
        None => issuer
            .join(fallback)
            .map_err(|_| Error::invalid("invalid OAuth issuer"))?,
    })
}
fn random_string() -> Result<String> {
    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| Error::invalid("OS random source unavailable"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
pub fn step_up_scope(granted: Option<&str>, challenged: Option<&str>) -> Option<String> {
    let challenged = challenged.filter(|scope| !scope.is_empty())?;
    let mut seen = HashSet::new();
    Some(
        granted
            .into_iter()
            .chain(Some(challenged))
            .flat_map(str::split_whitespace)
            .filter(|scope| seen.insert(*scope))
            .collect::<Vec<_>>()
            .join(" "),
    )
}
