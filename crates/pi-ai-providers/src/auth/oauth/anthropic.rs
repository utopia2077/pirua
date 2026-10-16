use super::{browser, common::*};
use crate::auth::OAuthServices;
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthPrompt, AuthProviderError, AuthSelectOption, BoxFuture,
    CancellationToken, ModelAuth, OAuthAuth, OAuthCredential,
};
use pi_ai_transport::generate_pkce;
use std::time::Duration;
const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
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
        redirect: String,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let body = serde_json::json!({"grant_type":"authorization_code","client_id":CLIENT_ID,"code":code,"state":state,"redirect_uri":redirect,"code_verifier":verifier});
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
        let method = i
            .prompt(
                AuthPrompt::Select {
                    message: "Select Anthropic login method:".into(),
                    options: vec![
                        AuthSelectOption {
                            id: "browser".into(),
                            label: "Browser login (default)".into(),
                            description: None,
                        },
                        AuthSelectOption {
                            id: "copy_code".into(),
                            label: "Copy code login (headless)".into(),
                            description: None,
                        },
                    ],
                },
                c,
            )
            .await
            .map_err(host)?;
        if !matches!(method.as_str(), "browser" | "copy_code") {
            return Err(OAuthFlowError::new(OAuthFlowErrorKind::UnknownMethod));
        }
        let pkce = generate_pkce(self.services.crypto.as_ref()).map_err(host)?;
        let verifier = pkce.verifier.expose_secret().to_owned();
        if method == "copy_code" {
            let redirect = "https://platform.claude.com/oauth/code/callback";
            let url = format!(
                "https://claude.ai/oauth/authorize?{}",
                form(&[
                    ("code", "true"),
                    ("client_id", CLIENT_ID),
                    ("response_type", "code"),
                    ("redirect_uri", redirect),
                    (
                        "scope",
                        "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"
                    ),
                    ("code_challenge", &pkce.challenge),
                    ("code_challenge_method", "S256"),
                    ("state", &verifier)
                ])
            );
            i.notify(AuthEvent::AuthUrl{url,instructions:Some("Complete login in your browser, then copy the code Anthropic shows and paste it here.".into())}).map_err(host)?;
            let input = i
                .prompt(
                    AuthPrompt::ManualCode {
                        message: "Paste the code Anthropic shows after you sign in:".into(),
                        placeholder: Some("code#state".into()),
                    },
                    c,
                )
                .await
                .map_err(host)?;
            let (code, state) = parse_input(&input, true);
            if state
                .as_deref()
                .is_some_and(|state| !state.is_empty() && state != verifier)
            {
                return Err(OAuthFlowError::new(OAuthFlowErrorKind::StateMismatch));
            }
            let code = code
                .filter(|code| !code.is_empty())
                .ok_or_else(|| OAuthFlowError::new(OAuthFlowErrorKind::MissingCode))?;
            i.notify(AuthEvent::Progress {
                message: "Exchanging authorization code for tokens...".into(),
            })
            .map_err(host)?;
            self.exchange(
                code,
                state.unwrap_or_else(|| verifier.clone()),
                verifier,
                redirect.into(),
                c,
            )
            .await
        } else {
            browser::login(
                &self.services,
                i,
                c,
                verifier.clone(),
                pkce.challenge,
                |code, state, redirect| Box::pin(self.exchange(code, state, verifier, redirect, c)),
            )
            .await
        }
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
