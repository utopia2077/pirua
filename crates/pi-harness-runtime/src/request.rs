//! One admitted provider request, with request-local Harness hook adaptation.

use std::{
    future::poll_fn,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    task::Poll,
};

use pi_agent_schema::{
    AfterResponseEvent, AgentHarnessStreamOptions, AgentThinkingLevel, BeforePayloadEvent,
    BoxFuture, ErrorContext, HarnessFault, HarnessRuntimeError, LaneId, OperationId,
    ResponseHeaders, SessionId, SettledAssistantMessage,
};
use pi_ai_core::{
    AssistantMessageEvent, Context, JsonValue, Model, ModelsError, ModelsErrorCode, ModelsRegistry,
    ModelsRequestOverrides, ProviderHeaders, RequestLifecycleOptions, RequestPayloadTransform,
    RequestResponseHeaderValue, RequestRuntime, SafeUnsigned, SimpleStreamOptions,
    SimpleStreamOptionsWire, StreamOptionsWire, ThinkingLevel,
};

/// Captured, owner-gated hooks and streaming publication for one request.
pub trait RequestCallbacks: Send + Sync {
    fn has_payload_handlers(&self) -> bool;
    fn before_payload(
        &self,
        event: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<JsonValue>, HarnessRuntimeError>>;
    fn after_response(
        &self,
        event: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>>;
    fn stream_event(
        &self,
        event: AssistantMessageEvent,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
}

/// Fully assembled inputs captured by the request's owning effect.
pub struct RequestInput {
    pub models: Arc<ModelsRegistry>,
    pub model: Model,
    pub context: Context,
    pub thinking: AgentThinkingLevel,
    pub options: AgentHarnessStreamOptions,
    pub runtime: RequestRuntime,
    pub lane: LaneId,
    pub run_id: OperationId,
    pub session_id: SessionId,
    pub callbacks: Arc<dyn RequestCallbacks>,
}

struct PayloadBridge {
    callbacks: Arc<dyn RequestCallbacks>,
    lane: LaneId,
    run_id: OperationId,
    // Models normalizes adapter errors in-band. Keep the actual runtime error
    // in this invocation only, so trusted faults and close retain their channel.
    error: Arc<Mutex<Option<HarnessRuntimeError>>>,
}

fn conversion_fault() -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(HarnessFault::new(
        ErrorContext::new("request payload cannot be represented exactly")
            .expect("fixed bounded error context"),
    ))
}

fn callback_panic() -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(HarnessFault::new(
        ErrorContext::new("trusted request callback panicked")
            .expect("fixed bounded error context"),
    ))
}

async fn invoke<'a, T>(
    callback: impl FnOnce() -> BoxFuture<'a, Result<T, HarnessRuntimeError>>,
) -> Result<T, HarnessRuntimeError> {
    let mut future = catch_unwind(AssertUnwindSafe(callback)).map_err(|_| callback_panic())?;
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(_) => Poll::Ready(Err(callback_panic())),
        },
    )
    .await
}

impl RequestPayloadTransform for PayloadBridge {
    fn transform<'a>(
        &'a self,
        model: &'a Model,
        payload: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Result<Option<serde_json::Value>, ModelsError>> {
        Box::pin(async move {
            let result = async {
                // Value's integer visitor preserves u64/i64, so JsonValue rejects
                // unsafe integers before any floating-point conversion occurs.
                let payload =
                    serde_json::from_value(payload.clone()).map_err(|_| conversion_fault())?;
                let replacement = invoke(|| {
                    self.callbacks.before_payload(BeforePayloadEvent {
                        lane: self.lane.clone(),
                        run_id: self.run_id.clone(),
                        model: model.clone(),
                        payload,
                    })
                })
                .await?;
                replacement
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| conversion_fault())
            }
            .await;
            result.map_err(|error| {
                let mut carrier = self.error.lock().unwrap_or_else(|e| e.into_inner());
                if carrier.is_none() {
                    *carrier = Some(error);
                }
                ModelsError::new(ModelsErrorCode::Stream)
            })
        })
    }
}

/// Executes exactly one Models request and returns its post-hook settled message.
/// Stream terminal events are progress observations; the owner publishes Harness
/// message_end from this return value, after the response pipeline completes.
pub async fn run_request(
    input: RequestInput,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    let cancellation = input.runtime.cancellation.clone();
    let error = Arc::new(Mutex::new(None));
    let handlers = catch_unwind(AssertUnwindSafe(|| input.callbacks.has_payload_handlers()))
        .map_err(|_| callback_panic())?;
    let payload = handlers.then(|| {
        Arc::new(PayloadBridge {
            callbacks: input.callbacks.clone(),
            lane: input.lane.clone(),
            run_id: input.run_id.clone(),
            error: error.clone(),
        }) as Arc<dyn RequestPayloadTransform>
    });
    let headers = input
        .options
        .headers
        .as_ref()
        .map(|headers| {
            headers
                .as_map()
                .iter()
                .map(|(name, value)| (name.clone(), Some(value.clone())))
                .collect()
        })
        .unwrap_or_default();
    let reasoning = match input.thinking {
        AgentThinkingLevel::Off => None,
        AgentThinkingLevel::Minimal => Some(ThinkingLevel::Minimal),
        AgentThinkingLevel::Low => Some(ThinkingLevel::Low),
        AgentThinkingLevel::Medium => Some(ThinkingLevel::Medium),
        AgentThinkingLevel::High => Some(ThinkingLevel::High),
        AgentThinkingLevel::ExtraHigh => Some(ThinkingLevel::ExtraHigh),
        AgentThinkingLevel::Max => Some(ThinkingLevel::Max),
    };
    let options = SimpleStreamOptions {
        runtime: input.runtime,
        wire: SimpleStreamOptionsWire {
            stream: StreamOptionsWire {
                transport: input.options.transport,
                timeout_ms: input.options.timeout_ms,
                max_retries: input.options.max_retries,
                max_retry_delay_ms: input.options.max_retry_delay_ms,
                metadata: input.options.metadata,
                cache_retention: input.options.cache_retention,
                session_id: Some(input.session_id.as_str().to_owned()),
                ..Default::default()
            },
            reasoning,
            deferred: input.options.deferred,
            ..Default::default()
        },
    };
    let mut observed = input.models.stream_simple_with_lifecycle(
        input.model,
        input.context,
        ModelsRequestOverrides {
            headers: ProviderHeaders::new(headers),
            ..Default::default()
        },
        options,
        RequestLifecycleOptions { payload },
    );
    while let Some(event) = observed.stream.next().await {
        let callback_error = error.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(error) = callback_error {
            return Err(error);
        }
        invoke(|| input.callbacks.stream_event(event)).await?;
    }
    let result = observed.stream.result().await;
    let callback_error = error.lock().unwrap_or_else(|e| e.into_inner()).take();
    if let Some(error) = callback_error {
        return Err(error);
    }
    // Models' validated terminal stream guarantees this invariant. It also
    // supplies setup failures with the known request identity, without recovery
    // defaults or a second provider invocation.
    let message = SettledAssistantMessage::new(result).map_err(|_| {
        HarnessRuntimeError::Fault(HarnessFault::new(
            ErrorContext::new("validated request stream returned an unsettled message")
                .expect("fixed bounded error context"),
        ))
    })?;
    if cancellation.is_cancelled() {
        return Ok(message);
    }
    let head = observed.metadata.snapshot();
    let status = head
        .as_ref()
        .map(|head| SafeUnsigned::new(u64::from(head.status)).expect("u16 is JavaScript-safe"));
    let headers = head.and_then(|head| {
        ResponseHeaders::sanitize_entries(head.headers.into_iter().map(|(name, value)| {
            let value = match value {
                RequestResponseHeaderValue::Text(value) => value,
                RequestResponseHeaderValue::Number(value) => {
                    ryu_js::Buffer::new().format_finite(value.get()).to_owned()
                }
            };
            (name, value)
        }))
    });
    // The owner performs its serialized start check inside this callback. Once
    // started it may finish after operation abort; do not race it with the token.
    invoke(|| {
        input.callbacks.after_response(AfterResponseEvent {
            lane: input.lane,
            run_id: input.run_id,
            status,
            headers,
            message,
        })
    })
    .await
}
