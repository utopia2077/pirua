use super::{common::*, page::page};
use crate::auth::OAuthServices;
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthPrompt, BoxFuture, CancellationToken, OAuthCredential,
};
use pi_ai_transport::{CallbackBind, CallbackRequest, CallbackResponse, CallbackSession};
use std::{
    future::{Future, poll_fn},
    task::Poll,
};
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum BrowserKind {
    Anthropic,
    Codex,
}
pub(super) struct BrowserConfig {
    pub kind: BrowserKind,
    pub state: String,
    pub authorize_url: String,
}
impl BrowserConfig {
    fn redirect(&self) -> &'static str {
        match self.kind {
            BrowserKind::Anthropic => "http://localhost:53692/callback",
            BrowserKind::Codex => "http://localhost:1455/auth/callback",
        }
    }
    fn path(&self) -> &'static str {
        match self.kind {
            BrowserKind::Anthropic => "/callback",
            BrowserKind::Codex => "/auth/callback",
        }
    }
}
pub(super) async fn login<'a>(
    s: &'a OAuthServices,
    i: &'a dyn AuthInteraction,
    c: &'a CancellationToken,
    config: BrowserConfig,
    exchange: impl FnOnce(String, String) -> BoxFuture<'a, Result<OAuthCredential>>,
) -> Result<OAuthCredential> {
    let binding = s
        .callbacks
        .bind(
            CallbackBind {
                host: s.callback_host.clone(),
                port: if config.kind == BrowserKind::Anthropic {
                    53692
                } else {
                    1455
                },
                limits: s.callback_limits,
            },
            c,
        )
        .await;
    let mut session = match binding {
        Ok(session) => Some(session),
        Err(error) if config.kind == BrowserKind::Codex && !c.is_cancelled() => {
            let _ = error;
            None
        }
        Err(error) => return Err(host(error)),
    };
    let manual_cancel = CancelOnDrop(CancellationToken::new());
    let result = authorize(
        i,
        c,
        &config,
        session
            .as_deref_mut()
            .map(|s| s as &mut dyn CallbackSession),
        &manual_cancel.0,
        s.callback_limits.max_connections,
    )
    .await;
    match result {
        Ok((code, state)) => {
            if config.kind == BrowserKind::Anthropic
                && let Err(error) = i.notify(AuthEvent::Progress {
                    message: "Exchanging authorization code for tokens...".into(),
                })
            {
                manual_cancel.0.cancel();
                if let Some(session) = &mut session {
                    let _ = session.close().await;
                }
                return Err(host(error));
            }
            let mut pending = exchange(code, state);
            // JavaScript starts fetch when the returned async exchange is invoked, then
            // runs finally before observing its promise. Preserve that ordering explicitly.
            let first = poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
            manual_cancel.0.cancel();
            if let Some(session) = &mut session {
                session.close().await.map_err(host)?;
            }
            match first {
                Poll::Ready(result) => result,
                Poll::Pending => pending.await,
            }
        }
        Err(error) => {
            manual_cancel.0.cancel();
            if let Some(session) = &mut session {
                let _ = session.close().await;
            }
            Err(error)
        }
    }
}
async fn authorize(
    i: &dyn AuthInteraction,
    c: &CancellationToken,
    config: &BrowserConfig,
    mut session: Option<&mut dyn CallbackSession>,
    manual_cancel: &CancellationToken,
    response_limit: usize,
) -> Result<(String, String)> {
    let mut responses = Responses::new(response_limit);
    i.notify(AuthEvent::AuthUrl{url:config.authorize_url.clone(),instructions:Some(if config.kind==BrowserKind::Anthropic{"Complete login in your browser. If the browser is on another machine, paste the final redirect URL here."}else{"A browser window should open. Complete login to finish."}.into())}).map_err(host)?;
    let mut manual=i.prompt(AuthPrompt::ManualCode{message:"Complete login in your browser, or paste the authorization code / redirect URL here:".into(),placeholder:Some(config.redirect().into())},manual_cancel);
    loop {
        enum Choice {
            Manual(std::result::Result<String, pi_ai_core::AuthInteractionError>),
            Request(std::result::Result<CallbackRequest, pi_ai_transport::AuthHostError>),
            Cancelled,
        }
        let choice = {
            let mut next = session.as_mut().map(|session| session.next(c));
            let mut cancelled = Box::pin(c.cancelled());
            poll_fn(|cx| {
                if cancelled.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Choice::Cancelled);
                }
                responses.poll(cx);
                if let Poll::Ready(value) = manual.as_mut().poll(cx) {
                    return Poll::Ready(Choice::Manual(value));
                }
                match next.as_mut() {
                    Some(next) => next.as_mut().poll(cx).map(Choice::Request),
                    None => Poll::Pending,
                }
            })
            .await
        };
        match choice {
            Choice::Cancelled => return Err(cancelled()),
            Choice::Manual(value) => {
                let value = value.map_err(host)?;
                let (code, state) = parse_input(&value, true);
                if state
                    .as_ref()
                    .is_some_and(|s| !s.is_empty() && s != &config.state)
                {
                    return Err(OAuthFlowError::new(OAuthFlowErrorKind::StateMismatch));
                }
                let code = code
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| OAuthFlowError::new(OAuthFlowErrorKind::MissingCode))?;
                let state = state.unwrap_or_else(|| config.state.clone());
                if config.kind == BrowserKind::Anthropic && state.is_empty() {
                    return Err(OAuthFlowError::new(OAuthFlowErrorKind::MissingState));
                }
                return Ok((code, state));
            }
            Choice::Request(request) => {
                let request = request.map_err(host)?;
                let url = callback_url(&request.target);
                let (response, code) = match url {
                    Err(_) if config.kind == BrowserKind::Anthropic => (
                        CallbackResponse {
                            status: 500,
                            content_type: "text/plain; charset=utf-8".into(),
                            no_store: false,
                            body: "Internal error".into(),
                        },
                        None,
                    ),
                    Err(_) => (
                        page(
                            500,
                            false,
                            "Internal error while processing OAuth callback.",
                            None,
                            false,
                        ),
                        None,
                    ),
                    Ok(url) if url.path() != config.path() => (
                        page(404, false, "Callback route not found.", None, false),
                        None,
                    ),
                    Ok(url) => {
                        let code = query(&url, "code");
                        let state = query(&url, "state");
                        let error = query(&url, "error");
                        if config.kind == BrowserKind::Anthropic
                            && error.as_ref().is_some_and(|s| !s.is_empty())
                        {
                            (
                                page(
                                    400,
                                    false,
                                    "Anthropic authentication did not complete.",
                                    error.as_ref().map(|s| format!("Error: {s}")).as_deref(),
                                    false,
                                ),
                                None,
                            )
                        } else if config.kind == BrowserKind::Anthropic
                            && (code.as_ref().is_none_or(String::is_empty)
                                || state.as_ref().is_none_or(String::is_empty))
                        {
                            (
                                page(400, false, "Missing code or state parameter.", None, false),
                                None,
                            )
                        } else if state.as_deref() != Some(config.state.as_str()) {
                            (page(400, false, "State mismatch.", None, false), None)
                        } else if code.as_ref().is_none_or(String::is_empty) {
                            (
                                page(400, false, "Missing authorization code.", None, false),
                                None,
                            )
                        } else {
                            (
                                page(
                                    200,
                                    true,
                                    if config.kind == BrowserKind::Anthropic {
                                        "Anthropic authentication completed. You can close this window."
                                    } else {
                                        "OpenAI authentication completed. You can close this window."
                                    },
                                    None,
                                    false,
                                ),
                                Some((code.unwrap_or_default(), state.unwrap_or_default())),
                            )
                        }
                    }
                };
                if let Some(code) = code {
                    acknowledge(request.reply, response, c).await?;
                    // Manual rejection already available when the callback settles wins in
                    // the source; a later successful manual value cannot replace this code.
                    let manual_result = poll_fn(|cx| Poll::Ready(manual.as_mut().poll(cx))).await;
                    if let Poll::Ready(Err(error)) = manual_result {
                        return Err(host(error));
                    }
                    return Ok(code);
                } else {
                    responses.start(request.reply, response, c).await?;
                }
            }
        }
    }
}
