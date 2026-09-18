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

fn capture_fault() -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(HarnessFault::new(
        ErrorContext::new("captured request registration or model mismatch")
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
    run_request_inner(input, None, None).await
}

/// Owner-only route: captured registration survives listener reentry and delayed spawn.
pub(crate) async fn run_captured_request(
    input: RequestInput,
    captured: pi_ai_core::CapturedTextDispatch,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    if &input.model != captured.model() {
        return Err(capture_fault());
    }
    run_request_inner(input, Some(captured), None).await
}

async fn run_request_inner(
    input: RequestInput,
    captured: Option<pi_ai_core::CapturedTextDispatch>,
    summary: Option<SummaryRequestSettings>,
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
    let (summary_cap, summary_budgets) = match &summary {
        Some(settings) => {
            let (cap, budgets) = summary_wire_budget(&input, settings)?;
            (Some(cap), budgets)
        }
        None => (None, None),
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
                session_id: Some(summary.as_ref().map_or_else(
                    || input.session_id.as_str().to_owned(),
                    |s| s.routing_key.clone(),
                )),
                max_tokens: summary_cap,
                ..Default::default()
            },
            reasoning,
            thinking_budgets: summary_budgets,
            deferred: if summary.is_some() {
                Some(pi_ai_core::DeferredRequest::Enabled(false))
            } else {
                input.options.deferred
            },
            ..Default::default()
        },
    };
    let overrides = ModelsRequestOverrides {
        headers: ProviderHeaders::new(headers),
        ..Default::default()
    };
    let lifecycle = RequestLifecycleOptions { payload };
    let observed = match captured {
        Some(captured) => input
            .models
            .stream_simple_captured_with_lifecycle(
                captured,
                input.context,
                overrides,
                options,
                lifecycle,
            )
            .map_err(|_| capture_fault())?,
        None => input.models.stream_simple_with_lifecycle(
            input.model,
            input.context,
            overrides,
            options,
            lifecycle,
        ),
    };
    consume_observed(
        observed,
        input.callbacks,
        input.lane,
        input.run_id,
        cancellation,
        error,
    )
    .await
}

async fn consume_observed(
    mut observed: pi_ai_core::ObservedAssistantStream,
    callbacks: Arc<dyn RequestCallbacks>,
    lane: LaneId,
    run_id: OperationId,
    cancellation: pi_ai_core::CancellationToken,
    error: Arc<Mutex<Option<HarnessRuntimeError>>>,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    while let Some(event) = observed.stream.next().await {
        let callback_error = error.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(error) = callback_error {
            return Err(error);
        }
        invoke(|| callbacks.stream_event(event)).await?;
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
        callbacks.after_response(AfterResponseEvent {
            lane,
            run_id,
            status,
            headers,
            message,
        })
    })
    .await
}

/// Captured inputs for one deferred fetch; no prompt or context conversion is needed.
pub struct DeferredRequestInput {
    pub models: Arc<ModelsRegistry>,
    pub model: Model,
    pub handle: pi_ai_core::DeferredHandle,
    pub options: AgentHarnessStreamOptions,
    pub runtime: RequestRuntime,
    pub lane: LaneId,
    pub run_id: OperationId,
    pub callbacks: Arc<dyn RequestCallbacks>,
}

/// Captured best-effort cancellation inputs. The owner supplies a close-only signal.
pub struct DeferredCancelInput {
    pub models: Arc<ModelsRegistry>,
    pub model: Model,
    pub handle: pi_ai_core::DeferredHandle,
    pub options: AgentHarnessStreamOptions,
    pub runtime: RequestRuntime,
}

fn deferred_headers(options: &AgentHarnessStreamOptions) -> ModelsRequestOverrides {
    ModelsRequestOverrides {
        headers: ProviderHeaders::new(
            options
                .headers
                .as_ref()
                .map(|headers| {
                    headers
                        .as_map()
                        .iter()
                        .map(|(name, value)| (name.clone(), Some(value.clone())))
                        .collect()
                })
                .unwrap_or_default(),
        ),
        ..Default::default()
    }
}

/// Fetch one captured deferred response through the existing payload/response pipeline.
pub async fn run_deferred_request(
    input: DeferredRequestInput,
    captured: pi_ai_core::CapturedTextDispatch,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    if &input.model != captured.model() {
        return Err(capture_fault());
    }
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
    let overrides = deferred_headers(&input.options);
    let options = pi_ai_core::DeferredFetchOptions {
        runtime: input.runtime,
        wire: pi_ai_core::DeferredFetchOptionsWire {
            timeout_ms: input.options.timeout_ms,
            max_retries: input.options.max_retries,
            max_retry_delay_ms: input.options.max_retry_delay_ms,
            wait: Some(SafeUnsigned::new(0).expect("zero is safe")),
        },
    };
    let observed = input
        .models
        .fetch_deferred_captured_with_lifecycle(
            captured,
            input.handle,
            overrides,
            options,
            RequestLifecycleOptions { payload },
        )
        .map_err(|_| capture_fault())?;
    consume_observed(
        observed,
        input.callbacks,
        input.lane,
        input.run_id,
        cancellation,
        error,
    )
    .await
}

fn cancel_provider_panic() -> pi_ai_core::AuthResolutionError {
    pi_ai_core::AuthResolutionError::Models(ModelsError::new(ModelsErrorCode::Provider))
}

/// Cancel a captured deferred response without hooks, state mutation or assistant events.
/// Provider failures, including panic, remain in the inner best-effort error channel.
pub async fn cancel_deferred_request(
    input: DeferredCancelInput,
    captured: pi_ai_core::CapturedTextDispatch,
) -> Result<Result<(), pi_ai_core::AuthResolutionError>, HarnessRuntimeError> {
    if &input.model != captured.model() {
        return Err(capture_fault());
    }
    let overrides = deferred_headers(&input.options);
    let options = pi_ai_core::DeferredCancelOptions {
        runtime: input.runtime,
        wire: pi_ai_core::DeferredCancelOptionsWire {
            timeout_ms: input.options.timeout_ms,
            max_retries: input.options.max_retries,
            max_retry_delay_ms: input.options.max_retry_delay_ms,
        },
    };
    let mut future = match catch_unwind(AssertUnwindSafe(|| {
        input
            .models
            .cancel_deferred_captured(captured, input.handle, overrides, options)
    })) {
        Ok(result) => result.map_err(|_| capture_fault())?,
        Err(_) => return Ok(Err(cancel_provider_panic())),
    };
    let result = poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(_) => Poll::Ready(Err(cancel_provider_panic())),
        },
    )
    .await;
    if catch_unwind(AssertUnwindSafe(|| drop(future))).is_err() {
        return Ok(Err(cancel_provider_panic()));
    }
    Ok(result)
}

/// Process-local summary request settings; never changes the captured model identity.
pub struct SummaryRequestSettings {
    /// Dedicated provider routing identity for this nested request.
    pub routing_key: String,
    /// Original desired output budget, before scalar-context wire compensation.
    pub max_tokens: SafeUnsigned,
    /// Logical complete prompt units before cut-created surrogate removal. When
    /// present, binds exactly one text User message and the supplied system prompt.
    pub logical_prompt_utf16_units: Option<SafeUnsigned>,
}

fn summary_wire_budget(
    input: &RequestInput,
    settings: &SummaryRequestSettings,
) -> Result<(SafeUnsigned, Option<pi_ai_core::ThinkingBudgets>), HarnessRuntimeError> {
    let Some(logical) = settings.logical_prompt_utf16_units else {
        return Ok((settings.max_tokens, None));
    };
    let [pi_ai_core::Message::User(user)] = input.context.messages.as_slice() else {
        return Err(conversion_fault());
    };
    let text = match &user.content {
        pi_ai_core::UserContent::Text(text) => text,
        pi_ai_core::UserContent::Blocks(blocks) => {
            let [pi_ai_core::InputContent::Text(text)] = blocks.as_slice() else {
                return Err(conversion_fault());
            };
            &text.text
        }
    };
    let clean_units = u64::try_from(text.encode_utf16().count()).map_err(|_| conversion_fault())?;
    if logical.get() < clean_units {
        return Err(conversion_fault());
    }
    // No normalization means the original selected provider path already sees
    // exactly the original estimate; preserve its ordinary options untouched.
    if logical.get() == clean_units {
        return Ok((settings.max_tokens, None));
    }
    let system_tokens = input
        .context
        .system_prompt
        .as_deref()
        .map(pi_ai_core::estimate_text_tokens)
        .transpose()
        .map_err(|_| conversion_fault())?
        .map_or(0, SafeUnsigned::get);
    let tokens = logical
        .get()
        .div_ceil(4)
        .checked_add(system_tokens)
        .filter(|n| *n <= SafeUnsigned::MAX.get())
        .ok_or_else(conversion_fault)?;
    let clamp = |cap: u64| {
        if input.model.context_window.get() == 0 {
            cap.max(1)
        } else {
            cap.min(
                input
                    .model
                    .context_window
                    .get()
                    .saturating_sub(tokens)
                    .saturating_sub(4096)
                    .max(1),
            )
        }
    };
    let compat = match input.model.typed_compat() {
        Ok(compat) => compat,
        // Let the actual captured provider retain its existing invalid-compat
        // error channel; this scalar correction must not invent a default mode.
        Err(_) => return Ok((settings.max_tokens, None)),
    };
    let adaptive = matches!(compat, Some(pi_ai_core::TypedModelCompat::AnthropicMessages(ref compat)) if compat.force_adaptive_thinking == Some(true));
    let initial = clamp(settings.max_tokens.get());
    if input.model.api.as_str() != pi_ai_core::ApiId::ANTHROPIC_MESSAGES
        || input.thinking == AgentThinkingLevel::Off
        || adaptive
    {
        return Ok((
            SafeUnsigned::new(initial).map_err(|_| conversion_fault())?,
            None,
        ));
    }
    // Exact pinned simple-options + non-adaptive Anthropic order: clamp,
    // thinking expansion/model ceiling, second clamp, effective answer-room budget.
    let budget = match input.thinking {
        AgentThinkingLevel::Minimal => 1024,
        AgentThinkingLevel::Low => 2048,
        AgentThinkingLevel::Medium => 8192,
        AgentThinkingLevel::High | AgentThinkingLevel::ExtraHigh | AgentThinkingLevel::Max => 16384,
        AgentThinkingLevel::Off => unreachable!("off returned above"),
    };
    let adjusted_cap = (initial + budget).min(input.model.max_tokens.get());
    let adjusted_budget = if adjusted_cap <= budget {
        budget.min(adjusted_cap.saturating_sub(1024))
    } else {
        budget
    };
    let final_cap = clamp(adjusted_cap);
    let effective = adjusted_budget.min(final_cap.saturating_sub(1024));
    let mut budgets = pi_ai_core::ThinkingBudgets::default();
    let value = Some(SafeUnsigned::new(effective).map_err(|_| conversion_fault())?);
    match input.thinking {
        AgentThinkingLevel::Minimal => budgets.minimal = value,
        AgentThinkingLevel::Low => budgets.low = value,
        AgentThinkingLevel::Medium => budgets.medium = value,
        AgentThinkingLevel::High | AgentThinkingLevel::ExtraHigh | AgentThinkingLevel::Max => {
            budgets.high = value
        }
        AgentThinkingLevel::Off => unreachable!("off returned above"),
    }
    Ok((
        SafeUnsigned::new(final_cap - effective).map_err(|_| conversion_fault())?,
        Some(budgets),
    ))
}

/// Seed isolation before the owner's before_request hook; its allowed patches win.
pub fn seed_summary_options(mut options: AgentHarnessStreamOptions) -> AgentHarnessStreamOptions {
    options.cache_retention = Some(pi_ai_core::CacheRetention::None);
    options
}

struct SummaryCallbacks(Arc<dyn RequestCallbacks>);
impl RequestCallbacks for SummaryCallbacks {
    fn has_payload_handlers(&self) -> bool {
        self.0.has_payload_handlers()
    }
    fn before_payload(
        &self,
        event: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<JsonValue>, HarnessRuntimeError>> {
        self.0.before_payload(event)
    }
    fn after_response(
        &self,
        event: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>> {
        self.0.after_response(event)
    }
    fn stream_event(
        &self,
        _event: AssistantMessageEvent,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async { Ok(()) })
    }
}

// Registry workers own producer handles. Dropping a panicked worker settles the
// same stream through its existing setup-error path; no second request is made.
struct SummarySpawner(Arc<dyn pi_ai_core::TaskSpawner>);
impl pi_ai_core::TaskSpawner for SummarySpawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), pi_ai_core::SpawnError> {
        self.0.spawn(Box::pin(async move {
            let mut task = Some(task);
            poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| {
                    task.as_mut().expect("live task").as_mut().poll(cx)
                })) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(())) | Err(_) => {
                        drop(task.take());
                        Poll::Ready(())
                    }
                }
            })
            .await;
        }))
    }
}

/// Executes one owner-admitted nested summary request, with no retries or public
/// assistant stream publication. The owner supplies the summary context/system prompt,
/// post-hook effective options, distinct routing key and captured registration.
pub async fn run_summary_request(
    mut input: RequestInput,
    captured: pi_ai_core::CapturedTextDispatch,
    settings: SummaryRequestSettings,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    if &input.model != captured.model() || settings.routing_key.is_empty() {
        return Err(capture_fault());
    }
    if !input.model.reasoning {
        input.thinking = AgentThinkingLevel::Off;
    }
    input.runtime.spawner = Arc::new(SummarySpawner(input.runtime.spawner));
    input.context.tools = None;
    input.callbacks = Arc::new(SummaryCallbacks(input.callbacks));
    run_request_inner(input, Some(captured), Some(settings)).await
}
