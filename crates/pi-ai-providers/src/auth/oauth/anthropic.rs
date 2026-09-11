use super::{
    browser::{self, BrowserConfig, BrowserKind},
    common::*,
};
use crate::auth::OAuthServices;
use pi_ai_core::{
    AuthInteraction, AuthProviderError, BoxFuture, CancellationToken, ModelAuth, OAuthAuth,
    OAuthCredential,
};
use pi_ai_transport::generate_pkce;
use std::time::Duration;
const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const REDIRECT: &str = "http://localhost:53692/callback";
#[derive(Clone)]
pub struct AnthropicOAuth {
    services: OAuthServices,
}
impl AnthropicOAuth {
    pub fn new(services: OAuthServices) -> Self {
        Self { services }
    }
    async fn exchange(
        &self,
        code: String,
        state: String,
        verifier: String,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let body = serde_json::json!({"grant_type":"authorization_code","client_id":CLIENT_ID,"code":code,"state":state,"redirect_uri":REDIRECT,"code_verifier":verifier});
        self.request(body, c).await
    }
    async fn request(
        &self,
        body: serde_json::Value,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let response = post(
            &self.services,
            TOKEN_URL,
            &[
                ("Content-Type", "application/json"),
                ("Accept", "application/json"),
            ],
            body.to_string(),
            c,
            Some(Duration::from_secs(30)),
            TokenRead::Anthropic,
        )
        .await?;
        if !response.ok() {
            return Err(OAuthFlowError::http(
                OAuthFlowErrorKind::Exchange,
                response.status,
            ));
        }
        token(&response.json()?, self.services.clock.now_unix_ms(), 300000)
    }
    async fn login_flow(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let pkce = generate_pkce(self.services.crypto.as_ref()).map_err(host)?;
        let verifier = pkce.verifier.expose_secret().to_owned();
        let authorize_url = format!(
            "https://claude.ai/oauth/authorize?{}",
            form(&[
                ("code", "true"),
                ("client_id", CLIENT_ID),
                ("response_type", "code"),
                ("redirect_uri", REDIRECT),
                (
                    "scope",
                    "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"
                ),
                ("code_challenge", &pkce.challenge),
                ("code_challenge_method", "S256"),
                ("state", &verifier)
            ])
        );
        browser::login(
            &self.services,
            i,
            c,
            BrowserConfig {
                kind: BrowserKind::Anthropic,
                state: verifier.clone(),
                authorize_url,
            },
            |code, state| Box::pin(self.exchange(code, state, verifier, c)),
        )
        .await
    }
}
impl OAuthAuth for AnthropicOAuth {
    fn name(&self) -> &str {
        "Anthropic (Claude Pro/Max)"
    }
    fn is_subscription(&self) -> bool {
        true
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
            self.request(serde_json::json!({"grant_type":"refresh_token","client_id":CLIENT_ID,"refresh_token":v.credentials.refresh.expose_secret()}),c).await.map_err(provider)
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
