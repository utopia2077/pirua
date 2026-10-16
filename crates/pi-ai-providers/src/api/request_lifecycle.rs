//! Request-local composition of existing provider service boundaries.
#[cfg(any(
    feature = "wire-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-anthropic-messages",
    feature = "wire-google-genai",
))]
mod payload {
    use super::super::{runtime::AdapterServices, wire_json::WirePayload};
    use pi_ai_core::{
        BoxFuture, ModelsError, ModelsErrorCode, RequestLifecycleInvocation,
        RequestPayloadTransform, TextStreamRequest,
    };

    pub(crate) fn bind(services: &mut AdapterServices, invocation: RequestLifecycleInvocation) {
        services.request_payload = invocation.payload;
        services.response_recorder = Some(invocation.recorder);
    }

    // Construct the host future at the call site, just like captured middleware.
    // The family's unchanged cancellation race owns whether that future is polled.
    pub(crate) fn transform<'a>(
        handler: &'a dyn RequestPayloadTransform,
        request: &'a TextStreamRequest,
        payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        let future = handler.transform(&request.resolved.model, payload.as_value_mut());
        Box::pin(async move {
            future
                .await
                .map(|value| value.map(WirePayload::new))
                .map_err(|_| ModelsError::new(ModelsErrorCode::Provider))
        })
    }
}
#[cfg(any(
    feature = "wire-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-anthropic-messages",
    feature = "wire-google-genai",
))]
pub(super) use payload::{bind, transform};
#[cfg(any(
    feature = "wire-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-anthropic-messages",
))]
mod response {
    use super::super::runtime::{AdapterResponseHead, AdapterServices};
    use pi_ai_core::{RequestResponseHead, RequestResponseHeaderValue};
    pub(crate) fn record(services: &AdapterServices, head: &AdapterResponseHead) {
        if let Some(recorder) = &services.response_recorder {
            recorder.record(RequestResponseHead {
                status: head.status,
                headers: head
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.to_owned(),
                            match value {
                                super::super::runtime::ResponseHeaderValue::Text(value) => {
                                    RequestResponseHeaderValue::Text(value.clone())
                                }
                                super::super::runtime::ResponseHeaderValue::Number(value) => {
                                    RequestResponseHeaderValue::Number(*value)
                                }
                            },
                        )
                    })
                    .collect(),
            });
        }
    }

    /// Attempt only observation projection; failure must not enter family error/retry handling.
    pub(crate) fn observe_only(
        services: &AdapterServices,
        status: u16,
        headers: &pi_ai_transport::HttpHeaders,
    ) {
        if services.response_recorder.is_some()
            && let Ok(head) = AdapterResponseHead::from_http(status, headers)
        {
            record(services, &head);
        }
    }
}
#[cfg(any(
    feature = "wire-openai-responses",
    feature = "wire-openai-completions",
    feature = "wire-anthropic-messages",
))]
pub(super) use response::{observe_only, record};
