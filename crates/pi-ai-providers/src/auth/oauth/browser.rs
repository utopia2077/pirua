//! Anthropic callback server, fallback binding and concurrent manual completion.
use super::{common::*, page::page};
use crate::auth::OAuthServices;
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthPrompt, BoxFuture, CancellationToken, OAuthCredential,
};
use pi_ai_transport::{CallbackBind, CallbackSession};
use std::{
    future::{Future, poll_fn},
    task::Poll,
};

pub(super) async fn login<'a>(
    s: &'a OAuthServices,
    i: &'a dyn AuthInteraction,
    c: &'a CancellationToken,
    state: String,
    challenge: String,
    exchange: impl FnOnce(String, String, String) -> BoxFuture<'a, Result<OAuthCredential>>,
) -> Result<OAuthCredential> {
    let mut session = None;
    for port in [53692, 0] {
        if c.is_cancelled() {
            return Err(cancelled());
        }
        match s
            .callbacks
            .bind(
                CallbackBind {
                    host: s.callback_host.clone(),
                    port,
                    limits: s.callback_limits,
                },
                c,
            )
            .await
        {
            Ok(bound) => {
                session = Some(bound);
                break;
            }
            Err(_) if !c.is_cancelled() => {}
            Err(error) => return Err(host(error)),
        }
    }
    let redirect = format!(
        "http://localhost:{}/callback",
        session
            .as_ref()
            .map_or(53692, |session| session.local_addr().port())
    );
    let url = format!(
        "https://claude.ai/oauth/authorize?{}",
        form(&[
            ("code", "true"),
            ("client_id", "9d1c250a-e61b-44d9-88ed-5944d1962f5e"),
            ("response_type", "code"),
            ("redirect_uri", &redirect),
            (
                "scope",
                "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"
            ),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("state", &state)
        ])
    );
    let manual = CancelOnDrop(CancellationToken::new());
    let result=async {
        i.notify(AuthEvent::AuthUrl{url,instructions:Some("Complete login in your browser. If the browser is on another machine, paste the final redirect URL here.".into())}).map_err(host)?;
        let (code,actual)=authorize(i,c,&state,&redirect,session.as_deref_mut().map(|session|session as &mut dyn CallbackSession),&manual.0,s.callback_limits.max_connections).await?;
        i.notify(AuthEvent::Progress{message:"Exchanging authorization code for tokens...".into()}).map_err(host)?;
        exchange(code,actual,redirect).await
    }.await;
    manual.0.cancel();
    if let Some(session) = session.as_mut() {
        let closed = session.close().await.map_err(host);
        if result.is_ok() {
            closed?;
        }
    }
    result
}
async fn authorize(
    i: &dyn AuthInteraction,
    c: &CancellationToken,
    state: &str,
    redirect: &str,
    mut session: Option<&mut dyn CallbackSession>,
    manual_cancel: &CancellationToken,
    response_limit: usize,
) -> Result<(String, String)> {
    let mut manual=i.prompt(AuthPrompt::ManualCode{message:"Complete login in your browser, or paste the authorization code / redirect URL here:".into(),placeholder:Some(redirect.into())},manual_cancel);
    let mut responses = Responses::new(response_limit);
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
            let mut next = session.as_mut().map(|session| session.next(c));
            let mut cancel = Box::pin(c.cancelled());
            poll_fn(|cx| {
                if cancel.as_mut().poll(cx).is_ready() {
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
                let (code, actual) = parse_input(&value.map_err(host)?, true);
                if actual
                    .as_deref()
                    .is_some_and(|actual| !actual.is_empty() && actual != state)
                {
                    return Err(OAuthFlowError::new(OAuthFlowErrorKind::StateMismatch));
                }
                let code = code
                    .filter(|code| !code.is_empty())
                    .ok_or_else(|| OAuthFlowError::new(OAuthFlowErrorKind::MissingCode))?;
                return Ok((code, actual.unwrap_or_else(|| state.into())));
            }
            Choice::Request(request) => {
                let request = request.map_err(host)?;
                let url = url::Url::parse(redirect)
                    .expect("internal redirect")
                    .join(&request.target)
                    .map_err(|_| invalid());
                let (response, code) = match url {
                    Err(_) => (
                        page(
                            500,
                            false,
                            "Internal error while processing the callback.",
                            None,
                            false,
                        ),
                        None,
                    ),
                    Ok(url) if url.path() != "/callback" => (
                        page(404, false, "Callback route not found.", None, false),
                        None,
                    ),
                    Ok(url) => {
                        let code = query(&url, "code").filter(|code| !code.is_empty());
                        let actual = query(&url, "state").filter(|state| !state.is_empty());
                        if query(&url, "error").is_some_and(|error| !error.is_empty()) {
                            (
                                page(
                                    400,
                                    false,
                                    "Anthropic authentication did not complete.",
                                    None,
                                    false,
                                ),
                                None,
                            )
                        } else if code.is_none() || actual.is_none() {
                            (
                                page(400, false, "Missing code or state parameter.", None, false),
                                None,
                            )
                        } else if actual.as_deref() != Some(state) {
                            (page(400, false, "State mismatch.", None, false), None)
                        } else {
                            (
                                page(
                                    200,
                                    true,
                                    "Anthropic authentication completed. You can close this window.",
                                    None,
                                    false,
                                ),
                                code,
                            )
                        }
                    }
                };
                if let Some(code) = code {
                    acknowledge(request.reply, response, c).await?;
                    return Ok((code, state.into()));
                }
                responses.start(request.reply, response, c).await?;
            }
        }
    }
}
