use super::*;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ApiCode {
    Other,
    ConnectionLimit,
    PreviousMissing,
}
#[derive(Clone, Debug)]
pub(super) enum Failure {
    Setup(&'static str),
    Transport(TransportErrorKind),
    TransportText(ResponsesError, u16),
    Protocol(ResponsesError),
    Api(ApiCode, ResponsesError),
    Reducer(ResponsesError),
    Http(ResponsesError),
    Delay { requested: f64, limit: u64 },
    Limit,
}
impl Failure {
    pub fn setup(kind: &'static str) -> Self {
        Self::Setup(kind)
    }
    pub fn flags(&self) -> ResponsesError {
        match self {
            Self::Reducer(e) if e.kind == ResponsesErrorKind::MissingTerminal => ResponsesError {
                retryable: true,
                ..*e
            },
            Self::TransportText(e, _)
            | Self::Protocol(e)
            | Self::Api(_, e)
            | Self::Reducer(e)
            | Self::Http(e) => *e,
            Self::Transport(TransportErrorKind::InvalidRequest) => {
                ResponsesError::new(ResponsesErrorKind::Provider)
            }
            Self::Transport(_) => ResponsesError {
                kind: ResponsesErrorKind::Provider,
                retryable: true,
                context_overflow: false,
            },
            _ => ResponsesError::new(ResponsesErrorKind::Provider),
        }
    }
    pub fn diagnostic(&self) -> DiagnosticErrorInfo {
        let (name, code) = match self {
            Self::TransportText(_, code) => (
                "WebSocketCloseError",
                Some(DiagnosticCode::Number(JsonNumber::Unsigned(
                    SafeUnsigned::new(u64::from(*code)).expect("u16"),
                ))),
            ),
            Self::Setup("payload_null") => ("TypeError", None),
            _ => ("Error", None),
        };
        DiagnosticErrorInfo {
            name: Some(name.into()),
            message: "WebSocket transport failed".into(),
            stack: None,
            code,
        }
    }
    pub fn non_transport(&self) -> bool {
        matches!(self, Self::Api(..) | Self::Protocol(..) | Self::Limit)
    }
}
impl From<TransportError> for Failure {
    fn from(e: TransportError) -> Self {
        if e.kind() == TransportErrorKind::LimitExceeded {
            Self::Limit
        } else {
            Self::Transport(e.kind())
        }
    }
}
impl From<ResponsesError> for Failure {
    fn from(e: ResponsesError) -> Self {
        Self::Reducer(e)
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(kind) => write!(f, "Codex setup {kind}"),
            Self::Transport(kind) => write!(f, "Codex transport {kind:?}"),
            Self::TransportText(_, code) => write!(f, "Codex WebSocket closed {code}"),
            _ => f.write_str("Codex adapter failure"),
        }
    }
}
impl std::error::Error for Failure {}
pub(super) fn empty_message(r: &TextStreamRequest) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: vec![],
        api: r.resolved.model.api.clone(),
        provider: r.resolved.model.provider.clone(),
        model: r.resolved.model.id.clone(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: StopReason::Pending,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(r.options.runtime.clock.now_unix_ms())
            .unwrap_or(SafeUnsigned::MAX),
    }
}
pub(super) fn classify(text: &str, template: &AssistantMessage) -> ResponsesError {
    let mut message = template.clone();
    message.stop_reason = StopReason::Error;
    message.error_message = Some(text.to_owned());
    ResponsesError {
        kind: ResponsesErrorKind::Provider,
        retryable: is_retryable_assistant_error(&message),
        context_overflow: is_context_overflow(&message, None),
    }
}
pub(super) fn publish_failure(
    r: &TextStreamRequest,
    p: &AssistantStreamProducer,
    mut output: AssistantMessage,
    error: Failure,
) {
    let aborted = r.options.runtime.cancellation.is_cancelled();
    let flags = error.flags();
    output.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    output.error_message = Some(if aborted {
        REQUEST_CANCELLED_MESSAGE.to_owned()
    } else {
        match (flags.context_overflow, flags.retryable) {
            (true, true) => "prompt is too long; server error".into(),
            (true, false) => "prompt is too long".into(),
            (false, true) => "server error".into(),
            _ => match error {
                Failure::Delay { requested, limit } => format!(
                    "Server requested {}s retry delay (max: {}s)",
                    (requested / 1000.0).ceil(),
                    (limit as f64 / 1000.0).ceil()
                ),
                _ => "Codex request failed".into(),
            },
        }
    });
    let _ = p.push(AssistantMessageEvent::Error {
        reason: if aborted {
            ErrorReason::Aborted
        } else {
            ErrorReason::Error
        },
        error: Box::new(output),
    });
}
