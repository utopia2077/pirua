use super::{common::*, page::page};
use crate::auth::OAuthServices;
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthPrompt, AuthProviderError, BoxFuture, CancellationToken,
    ModelAuth, OAuthAuth, OAuthCredential, OAuthCredentialType, OAuthCredentials, OAuthExtraFields,
    SafeUnsigned, SecretString,
};
use pi_ai_transport::{
    CallbackBind, CallbackReply, CallbackRequest, CallbackSession, generate_pkce,
};
use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};
#[derive(Clone)]
pub struct OpenRouterOAuth {
    services: OAuthServices,
}
impl OpenRouterOAuth {
    pub fn new(services: OAuthServices) -> Self {
        Self { services }
    }
    async fn exchange(
        &self,
        code: &str,
        verifier: &str,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let response=post(&self.services,"https://openrouter.ai/api/v1/auth/keys",&[("accept","application/json"),("content-type","application/json")],serde_json::json!({"code":code,"code_verifier":verifier,"code_challenge_method":"S256"}).to_string(),c,Some(Duration::from_secs(30)),TokenRead::OpenRouter).await.map_err(|error| {
            if error.kind()==OAuthFlowErrorKind::Timeout {
                error.with_callback_message("OpenRouter OAuth token exchange timed out".into())
            } else {error}
        })?;
        if !response.ok() {
            let value = response.json().unwrap_or(serde_json::Value::Null);
            let detail = error_detail(&value).filter(|detail| !detail.is_empty());
            let message = format!(
                "OpenRouter OAuth key exchange failed (HTTP {}){}",
                response.status,
                detail.map(|d| format!(": {d}")).unwrap_or_default()
            );
            return Err(
                OAuthFlowError::http(OAuthFlowErrorKind::Exchange, response.status)
                    .with_callback_message(message),
            );
        }
        let value = response.json().map_err(|e| {
            e.with_callback_message("OpenRouter OAuth returned invalid JSON".into())
        })?;
        let key = value
            .get("key")
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                invalid()
                    .with_callback_message("OpenRouter OAuth response carries no \"key\"".into())
            })?;
        Ok(OAuthCredential {
            credential_type: OAuthCredentialType::OAuth,
            credentials: OAuthCredentials {
                access: SecretString::new(key),
                refresh: SecretString::new(""),
                expires: SafeUnsigned::new(9_007_199_254_740_991).map_err(host)?,
                extra: OAuthExtraFields::default(),
            },
        })
    }
    async fn login_flow(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let pkce = generate_pkce(self.services.crypto.as_ref()).map_err(host)?;
        let mut bytes = [0; 16];
        self.services.crypto.fill_random(&mut bytes).map_err(host)?;
        bytes[6] = (bytes[6] & 15) | 64;
        bytes[8] = (bytes[8] & 63) | 128;
        let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let path = format!(
            "/oauth/callback/{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..]
        );
        let mut session = self
            .services
            .callbacks
            .bind(
                CallbackBind {
                    host: self.services.callback_host.clone(),
                    port: 0,
                    limits: self.services.callback_limits,
                },
                c,
            )
            .await
            .map_err(host)?;
        let callback = format!(
            "http://{}:{}{}",
            self.services.callback_host,
            session.local_addr().port(),
            path
        );
        let manual_cancel = CancelOnDrop(CancellationToken::new());
        let timer_cancel = CancelOnDrop(CancellationToken::new());
        let result = self
            .authorize(
                i,
                c,
                session.as_mut(),
                &path,
                &callback,
                pkce.verifier.expose_secret(),
                &pkce.challenge,
                &manual_cancel.0,
                &timer_cancel.0,
            )
            .await;
        manual_cancel.0.cancel();
        timer_cancel.0.cancel();
        let closed = session.close().await.map_err(host);
        match result {
            Err(error) => Err(error),
            Ok(value) => {
                closed?;
                Ok(value)
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn authorize(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
        session: &mut dyn CallbackSession,
        path: &str,
        callback: &str,
        verifier: &str,
        challenge: &str,
        manual_cancel: &CancellationToken,
        timer_cancel: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let mut timeout = self
            .services
            .clock
            .sleep(Duration::from_secs(300), timer_cancel);
        i.notify(AuthEvent::Progress {
            message: format!("Listening for OpenRouter OAuth callback on {callback}"),
        })
        .map_err(host)?;
        i.notify(AuthEvent::AuthUrl{url:format!("https://openrouter.ai/auth?{}",form(&[("callback_url",callback),("code_challenge",challenge),("code_challenge_method","S256")])),instructions:Some("Complete sign-in in your browser. If the browser is on another machine, paste the final redirect URL here.".into())}).map_err(host)?;
        let mut manual=Some(i.prompt(AuthPrompt::ManualCode{message:"Complete sign-in in your browser, or paste the authorization code / redirect URL here:".into(),placeholder:Some(callback.into())},manual_cancel));
        let mut responses = Responses::new(self.services.callback_limits.max_connections);
        let mut exchange: Option<BoxFuture<'_, Result<OAuthCredential>>> = None;
        let mut primary_reply: Option<Box<dyn CallbackReply>> = None;
        let mut primary_ack: Option<
            BoxFuture<'_, std::result::Result<(), pi_ai_transport::AuthHostError>>,
        > = None;
        let mut primary_result: Option<Result<OAuthCredential>> = None;
        let mut manual_error = None;
        let mut claimed = false;
        loop {
            enum Event {
                Cancelled,
                Timeout,
                Manual(std::result::Result<String, pi_ai_core::AuthInteractionError>),
                Request(std::result::Result<CallbackRequest, pi_ai_transport::AuthHostError>),
                Exchange(Result<OAuthCredential>),
                Ack(std::result::Result<(), pi_ai_transport::AuthHostError>),
            }
            let event = {
                let mut next = session.next(c);
                let mut cancel = Box::pin(c.cancelled());
                poll_fn(|cx| {
                    if cancel.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(Event::Cancelled);
                    }
                    if timeout.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(Event::Timeout);
                    }
                    responses.poll(cx);
                    if let Some(manual) = manual.as_mut()
                        && let Poll::Ready(value) = manual.as_mut().poll(cx)
                    {
                        return Poll::Ready(Event::Manual(value));
                    }
                    if let Poll::Ready(value) = next.as_mut().poll(cx) {
                        return Poll::Ready(Event::Request(value));
                    }
                    if let Some(ack) = primary_ack.as_mut()
                        && let Poll::Ready(value) = ack.as_mut().poll(cx)
                    {
                        return Poll::Ready(Event::Ack(value));
                    }
                    if let Some(exchange) = exchange.as_mut()
                        && let Poll::Ready(value) = exchange.as_mut().poll(cx)
                    {
                        return Poll::Ready(Event::Exchange(value));
                    }
                    Poll::Pending
                })
                .await
            };
            match event {
                Event::Cancelled => {
                    timer_cancel.cancel();
                    let _ = session.close().await;
                    return Err(cancelled());
                }
                Event::Timeout => {
                    timer_cancel.cancel();
                    let _ = session.close().await;
                    return Err(OAuthFlowError::new(OAuthFlowErrorKind::Timeout));
                }
                Event::Manual(value) => {
                    manual = None;
                    if claimed {
                        if let Err(error) = value {
                            manual_error = Some(host(error));
                        }
                        continue;
                    }
                    timer_cancel.cancel();
                    session.close().await.map_err(host)?;
                    let value = value.map_err(host)?;
                    let (code, _) = parse_input(&value, false);
                    let code = code
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| OAuthFlowError::new(OAuthFlowErrorKind::MissingCode))?;
                    i.notify(AuthEvent::Progress {
                        message: "Exchanging authorization code for an API key...".into(),
                    })
                    .map_err(host)?;
                    return self.exchange(&code, verifier, c).await;
                }
                Event::Exchange(result) => {
                    drop(exchange.take());
                    let response = match &result {
                        Ok(_) => page(
                            200,
                            true,
                            "Signed in to OpenRouter. You may now close this page.",
                            None,
                            true,
                        ),
                        Err(error) => page(
                            502,
                            false,
                            "OpenRouter key exchange failed.",
                            Some(&error.callback_message()),
                            true,
                        ),
                    };
                    let reply = primary_reply.take().ok_or_else(invalid)?;
                    primary_ack = Some(reply.send(response, c));
                    primary_result = Some(result);
                }
                Event::Ack(ack) => {
                    drop(primary_ack.take());
                    timer_cancel.cancel();
                    let closed = session.close().await.map_err(host);
                    ack.map_err(host)?;
                    closed?;
                    return match primary_result.take().ok_or_else(invalid)? {
                        Err(error) => Err(error),
                        Ok(value) => {
                            if let Some(error) = manual_error {
                                Err(error)
                            } else {
                                Ok(value)
                            }
                        }
                    };
                }
                Event::Request(request) => {
                    let request = request.map_err(host)?;
                    let url = callback_url(&request.target)?;
                    if request.method != "GET" || url.path() != path {
                        responses
                            .start(
                                request.reply,
                                page(404, false, "OAuth callback route not found.", None, true),
                                c,
                            )
                            .await?;
                        continue;
                    }
                    if claimed {
                        responses
                            .start(
                                request.reply,
                                page(
                                    409,
                                    false,
                                    "This OAuth callback has already been used.",
                                    None,
                                    true,
                                ),
                                c,
                            )
                            .await?;
                        continue;
                    }
                    if let Some(error) = query(&url, "error").filter(|s| !s.is_empty()) {
                        let details = query(&url, "error_description").unwrap_or(error);
                        primary_ack = Some(request.reply.send(
                            page(
                                400,
                                false,
                                "OpenRouter authorization was denied.",
                                Some(&details),
                                true,
                            ),
                            c,
                        ));
                        primary_result = Some(Err(OAuthFlowError::new(
                            OAuthFlowErrorKind::AuthorizationDenied,
                        )));
                        claimed = true;
                        continue;
                    }
                    let Some(code) = query(&url, "code").filter(|s| !s.is_empty()) else {
                        responses
                            .start(
                                request.reply,
                                page(
                                    400,
                                    false,
                                    "OpenRouter returned no authorization code.",
                                    None,
                                    true,
                                ),
                                c,
                            )
                            .await?;
                        continue;
                    };
                    claimed = true;
                    primary_reply = Some(request.reply);
                    exchange = Some(Box::pin(
                        async move { self.exchange(&code, verifier, c).await },
                    ));
                }
            }
        }
    }
}

fn error_detail(value: &serde_json::Value) -> Option<&str> {
    for key in ["error_description", "message", "error"] {
        if let Some(text) = value.get(key).and_then(serde_json::Value::as_str) {
            return Some(text);
        }
    }
    value
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(serde_json::Value::as_str)
}

impl OAuthAuth for OpenRouterOAuth {
    fn name(&self) -> &str {
        "OpenRouter OAuth"
    }
    fn login_label(&self) -> Option<&str> {
        Some("Sign in with OpenRouter")
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
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move { Ok(v.clone()) })
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
