//! Configured Bedrock ConverseStream: typed logical payload, signed Smithy
//! attempts and incremental binary response reduction. No discovery or registry.
mod build;
mod config;
mod invocation;
mod options;
mod payload;
mod reducer;
mod response;
mod schema;
mod sdk;
mod signing;
use crate::api::runtime::AdapterServices;
pub use config::*;
pub use options::*;
pub use payload::*;
use pi_ai_core::*;
use pi_ai_transport::{
    SmithyEventStreamLimits, SmithyRetryClass, SmithyRetryDisposition, SmithyRetryError,
    TransportError,
};
use std::{fmt, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub struct BedrockLimits {
    pub eventstream: SmithyEventStreamLimits,
    pub max_content_blocks: usize,
    pub max_content_bytes: usize,
    pub max_tool_argument_bytes: usize,
}
impl Default for BedrockLimits {
    fn default() -> Self {
        Self {
            eventstream: SmithyEventStreamLimits::default(),
            max_content_blocks: 4096,
            max_content_bytes: 16 * 1024 * 1024,
            max_tool_argument_bytes: 262144,
        }
    }
}
#[derive(Clone)]
pub struct BedrockApi {
    pub services: AdapterServices<BedrockPayload>,
    pub defaults: BedrockOptions,
    pub limits: BedrockLimits,
    configuration: Arc<dyn BedrockConfigProvider>,
    entropy: Arc<dyn BedrockEntropy>,
    api: ApiId,
}
impl fmt::Debug for BedrockApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockApi([REDACTED])")
    }
}
impl BedrockApi {
    pub fn new(
        services: AdapterServices<BedrockPayload>,
        configuration: Arc<dyn BedrockConfigProvider>,
        entropy: Arc<dyn BedrockEntropy>,
    ) -> Self {
        Self {
            services,
            defaults: BedrockOptions::default(),
            limits: BedrockLimits::default(),
            configuration,
            entropy,
            api: "bedrock-converse-stream".parse().expect("API id"),
        }
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: BedrockOptions,
    ) -> AssistantStream {
        self.start(request, Ok(options))
    }
    fn start(
        &self,
        request: TextStreamRequest,
        options: Result<BedrockOptions, Failure>,
    ) -> AssistantStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api.clone(),
            provider: request.resolved.model.provider.clone(),
            model: request.resolved.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        if request.options.runtime.cancellation.is_cancelled() {
            return stream;
        }
        let reducer = reducer::Reducer::new(&request, self.limits);
        if request.resolved.model.api != self.api {
            reducer::error(&request, &producer, reducer, Failure::local());
            return stream;
        }
        let api = self.clone();
        let worker = producer.clone();
        let spawner = request.options.runtime.spawner.clone();
        if spawner
            .spawn(Box::pin(async move {
                let mut reducer = reducer;
                let result = match options {
                    Ok(options) => {
                        invocation::run(&api, &request, &options, &worker, &mut reducer).await
                    }
                    Err(e) => Err(e),
                };
                if let Err(failure) = result {
                    reducer::error(&request, &worker, reducer, failure);
                }
            }))
            .is_err()
        {
            let _ = producer.fail_setup(StreamSetupFailure::Spawn);
        }
        stream
    }
}
impl ApiAdapter for BedrockApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let mut options = BedrockOptions {
            reasoning: request.options.wire.reasoning,
            thinking_budgets: request.options.wire.thinking_budgets.clone(),
            tool_choice: request.options.wire.tool_choice.map(|v| {
                BedrockToolChoice::Mode(match v {
                    ToolChoice::Auto => BedrockToolMode::Auto,
                    ToolChoice::None => BedrockToolMode::None,
                })
            }),
            ..Default::default()
        };
        let mut wire = crate::api::simple_options::build_base_options(
            &request.resolved.model,
            &request.context,
            &request.options.wire.stream,
        )
        .map_err(|_| Failure::local());
        let mut simple_failure = None;
        if let (Ok(wire), Some(level)) = (&mut wire, options.reasoning)
            && build::claude(&request.resolved.model)
            && !build::adaptive(&request.resolved.model)
        {
            let adjusted = crate::api::simple_options::adjust_max_tokens_for_thinking(
                wire.max_tokens,
                request.resolved.model.max_tokens,
                level,
                options.thinking_budgets.as_ref(),
            );
            match crate::api::simple_options::clamp_max_tokens_to_context(
                &request.resolved.model,
                &request.context,
                adjusted.max_tokens,
            ) {
                Ok(max) => {
                    wire.max_tokens = Some(max);
                    let budget = SafeUnsigned::new(
                        adjusted
                            .thinking_budget
                            .get()
                            .min(max.get().saturating_sub(1024)),
                    )
                    .expect("bounded budget");
                    let budgets = options.thinking_budgets.get_or_insert_default();
                    match level {
                        ThinkingLevel::Minimal => budgets.minimal = Some(budget),
                        ThinkingLevel::Low => budgets.low = Some(budget),
                        ThinkingLevel::Medium => budgets.medium = Some(budget),
                        _ => budgets.high = Some(budget),
                    }
                }
                Err(_) => simple_failure = Some(Failure::local()),
            }
        }
        if let Some(error) = simple_failure {
            wire = Err(error);
        }
        let (wire, options) = match wire {
            Ok(wire) => (wire, Ok(options)),
            Err(e) => (request.options.wire.stream.clone(), Err(e)),
        };
        self.start(
            TextStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: StreamOptions {
                    wire,
                    runtime: request.options.runtime,
                },
            },
            options,
        )
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async {
            Err(ModelsError::with_source(
                ModelsErrorCode::Stream,
                CapabilityError::Unsupported,
            ))
        })
    }
}
struct Failure {
    status: Option<u16>,
    code: Option<String>,
    request_id: Option<String>,
    class: SmithyRetryClass,
    safe: &'static str,
}
impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockFailure([REDACTED])")
    }
}
impl Failure {
    fn local() -> Self {
        Self {
            status: None,
            code: None,
            request_id: None,
            class: SmithyRetryClass::ClientError,
            safe: "Bedrock request failed",
        }
    }
    fn leaf(error: BedrockLeafError) -> Self {
        Self {
            class: error.retry_class,
            safe: "Bedrock configured operation failed",
            ..Self::local()
        }
    }
    fn transport(error: TransportError) -> Self {
        Self {
            safe: if error.smithy_assistant_retry() == Some(true) {
                "server error"
            } else {
                "Bedrock request failed"
            },
            class: if error.smithy_retry() == Some(SmithyRetryDisposition::Transient) {
                SmithyRetryClass::Transient
            } else {
                SmithyRetryClass::ClientError
            },
            ..Self::local()
        }
    }
    fn message(
        status: Option<u16>,
        name: Option<&str>,
        message: &str,
        template: &AssistantMessage,
    ) -> Self {
        let mut probe = template.clone();
        probe.stop_reason = StopReason::Error;
        probe.error_message = Some(message.into());
        let retry = is_retryable_assistant_error(&probe);
        let overflow = is_context_overflow(&probe, None);
        let safe = match (overflow, retry) {
            (true, true) => "prompt is too long; server error",
            (true, false) => "prompt is too long",
            (false, true) => "server error",
            _ => "Bedrock request failed",
        };
        let class = SmithyRetryError {
            name: name.unwrap_or("Error"),
            message,
            code: None,
            status,
            retryable_trait: name == Some("ModelNotReadyException"),
            throttling_trait: false,
            clock_skew_corrected: false,
            type_error: false,
            cause: None,
        }
        .class();
        Self {
            status,
            code: name
                .filter(|n| n.ends_with("Exception"))
                .and_then(reducer::diagnostic),
            request_id: None,
            class,
            safe,
        }
    }
}
