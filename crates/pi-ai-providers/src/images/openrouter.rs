//! OpenRouter's pinned chat-completions image wire, with one HTTP retry owner.
use super::{ImageAdapterServices, wire_request, wire_response};
use crate::api::{runtime::AdapterResponseHead, wire_json::WirePayload};
use pi_ai_core::{
    AssistantImages, BoxFuture, ImageGenerationRequest, ImagesApiAdapter, ImagesStopReason,
    SafeUnsigned,
};
use pi_ai_transport::{ProviderAttemptFailure, ProviderRetryError, RetryProfile};

/// Configured adapter only: does not resolve credentials, register, or fetch catalogs.
#[derive(Clone, Debug)]
pub struct OpenRouterImagesApi {
    pub services: ImageAdapterServices,
}
impl OpenRouterImagesApi {
    pub fn new(services: ImageAdapterServices) -> Self {
        Self { services }
    }
}
impl ImagesApiAdapter for OpenRouterImagesApi {
    fn generate<'a>(&'a self, request: ImageGenerationRequest) -> BoxFuture<'a, AssistantImages> {
        Box::pin(generate_images(request, &self.services))
    }
}

/// Executes a resolved image request, retaining partial output on reduction failure.
pub async fn generate_images(
    request: ImageGenerationRequest,
    services: &ImageAdapterServices,
) -> AssistantImages {
    let model = &request.resolved.model;
    let mut output = AssistantImages {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        output: Vec::new(),
        response_id: None,
        usage: None,
        stop_reason: ImagesStopReason::Stop,
        error_message: None,
        timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
            .unwrap_or(SafeUnsigned::MAX),
    };
    if let Err(error) = run(&request, services, &mut output).await {
        // Deliberately catch-only: a successful response observer may cancel
        // its token and still allow the already decoded result to finish.
        let aborted = request.options.runtime.cancellation.is_cancelled();
        output.stop_reason = if aborted {
            ImagesStopReason::Aborted
        } else {
            ImagesStopReason::Error
        };
        output.error_message = Some(if aborted {
            pi_ai_core::REQUEST_CANCELLED_MESSAGE.to_owned()
        } else {
            error.message().to_owned()
        });
    }
    output
}
async fn run(
    request: &ImageGenerationRequest,
    services: &ImageAdapterServices,
    output: &mut AssistantImages,
) -> Result<(), Failure> {
    if request.resolved.model.api.as_str() != "openrouter-images" {
        return Err(Failure::Api);
    }
    let key = request
        .auth
        .api_key
        .as_ref()
        .map(|v| v.expose_secret())
        .filter(|s| !s.is_empty())
        .ok_or(Failure::Auth)?;
    // Construction observes invalid OPENAI_CUSTOM_HEADERS before onPayload.
    let defaults = wire_request::default_headers(request, services)?;
    let mut payload = WirePayload::new(wire_request::payload(request));
    if let Some(middleware) = &services.payload
        && let Some(replacement) = middleware
            .on_payload(request, &mut payload)
            .await
            .map_err(|_| Failure::Payload)?
    {
        payload = replacement;
    }
    let (http, streaming) =
        wire_request::build(request, services, key, defaults, payload.into_value())?;
    let (response, value) = services
        .transport
        .retry_provider_request(
            &request.options.runtime,
            RetryProfile::SharedProvider {
                max_retries: request
                    .options
                    .wire
                    .max_retries
                    .map(SafeUnsigned::get)
                    .unwrap_or(0),
                max_retry_delay_ms: request
                    .options
                    .wire
                    .max_retry_delay_ms
                    .map(SafeUnsigned::get),
            },
            || wire_response::attempt(&http, request, services, streaming),
        )
        .await
        .map_err(|error| match error {
            ProviderRetryError::Attempt(
                ProviderAttemptFailure::Provider { error, .. }
                | ProviderAttemptFailure::Other(error),
            ) => error,
            ProviderRetryError::Cancelled { .. } => Failure::Transport,
            ProviderRetryError::Policy { .. } => Failure::Retry,
        })?;
    if let Some(middleware) = &services.response {
        let head = AdapterResponseHead::from_http(response.status, &response.headers)
            .map_err(|_| Failure::Response)?;
        middleware
            .on_response(request, &head)
            .await
            .map_err(|_| Failure::Response)?;
    }
    wire_response::reduce(value, &request.resolved.model, output)
}

/// Fixed redacted projections of the source's setup/SDK/normalization failures.
#[derive(Clone, Copy, Debug)]
pub(super) enum Failure {
    Api,
    Auth,
    Headers,
    Endpoint,
    Payload,
    Response,
    Decode,
    Wire,
    Usage,
    Transport,
    Http,
    Retry,
}
impl Failure {
    pub(super) fn message(self) -> &'static str {
        match self {
            Self::Api => "Image API mismatch",
            Self::Auth => "OpenRouter image authentication is missing",
            Self::Headers => "OpenRouter image headers are invalid",
            Self::Endpoint => "OpenRouter image endpoint is invalid",
            Self::Payload => "OpenRouter image payload failed",
            Self::Response => "OpenRouter image response observer failed",
            Self::Decode => "OpenRouter image response decoding failed",
            Self::Wire => "OpenRouter image response is invalid",
            Self::Usage => "OpenRouter image usage is invalid",
            Self::Transport => "OpenRouter image transport failed",
            Self::Http => "OpenRouter image provider request failed",
            Self::Retry => "OpenRouter image retry policy failed",
        }
    }
}
