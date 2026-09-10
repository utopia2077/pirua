use super::{ImageAdapterServices, openrouter::Failure, wire_request::truthy};
use crate::api::{openai_sdk_strings::trim, wire_json::project_wire_safe_unsigned};
use pi_ai_core::*;
use pi_ai_transport::{
    FetchRequest, HttpResponse, ProviderAttemptFailure, RetryProfile, TimeoutProfile,
    TransportErrorKind, read_body_limited,
};
use serde_json::Value;
use std::time::Duration;

pub(super) fn attempt<'a>(
    http: &'a FetchRequest,
    request: &'a ImageGenerationRequest,
    services: &'a ImageAdapterServices,
    streaming: bool,
) -> BoxFuture<'a, Result<(HttpResponse, Value), ProviderAttemptFailure<Failure>>> {
    Box::pin(async move {
        let runtime = &request.options.runtime;
        let mut response = services
            .transport
            .send_fetch_request(
                http,
                runtime,
                RetryProfile::None,
                TimeoutProfile::Headers(Duration::from_millis(
                    request
                        .options
                        .wire
                        .timeout_ms
                        .map(SafeUnsigned::get)
                        .unwrap_or(600_000),
                )),
            )
            .await
            .map_err(|e| {
                let kind = if e.is_custom_fetch_rejection()
                    && !matches!(
                        e.kind(),
                        TransportErrorKind::Cancelled | TransportErrorKind::Timeout
                    ) {
                    TransportErrorKind::Connection
                } else {
                    e.kind()
                };
                if matches!(
                    kind,
                    TransportErrorKind::Connection | TransportErrorKind::Timeout
                ) {
                    ProviderAttemptFailure::Provider {
                        status: None,
                        headers: None,
                        error: Failure::Transport,
                    }
                } else {
                    ProviderAttemptFailure::Other(Failure::Transport)
                }
            })?;
        if !(200..300).contains(&response.status) {
            // SDK response.text errors still form an HTTP error, except local
            // cancellation/resource admission which retains its owning failure.
            if let Err(e) = read_body_limited(
                response.body.as_mut(),
                runtime,
                services.max_error_body_bytes,
            )
            .await
                && matches!(
                    e.kind(),
                    TransportErrorKind::Cancelled | TransportErrorKind::LimitExceeded
                )
            {
                return Err(ProviderAttemptFailure::Other(Failure::Transport));
            }
            return Err(ProviderAttemptFailure::Provider {
                status: Some(response.status),
                headers: Some(response.headers.clone()),
                error: Failure::Http,
            });
        }
        let result = if streaming {
            // The SDK supplies a Stream object without reading. The image reducer
            // then finds no choices. Drop closes the retained response body.
            Value::Object(Default::default())
        } else if response.status == 204 {
            Value::Null
        } else {
            let content_type = response
                .headers
                .get_for_fetch("content-type")
                .unwrap_or_default();
            let media = trim(content_type.split(';').next().unwrap_or_default());
            let json = media.contains("application/json") || media.ends_with("+json");
            if json && response.headers.get_for_fetch("content-length").as_deref() == Some("0") {
                Value::Null
            } else {
                let bytes = read_body_limited(
                    response.body.as_mut(),
                    runtime,
                    services.max_response_body_bytes,
                )
                .await
                .map_err(|_| ProviderAttemptFailure::Other(Failure::Decode))?;
                let text = String::from_utf8_lossy(&bytes);
                let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
                if json {
                    serde_json::from_str(text)
                        .map_err(|_| ProviderAttemptFailure::Other(Failure::Decode))?
                } else {
                    Value::String(text.to_owned())
                }
            }
        };
        Ok((response, result))
    })
}
fn count(value: Option<&Value>) -> Result<f64, Failure> {
    match value.filter(|v| truthy(v)) {
        None => Ok(0.0),
        Some(value) => value
            .as_f64()
            .filter(|n| n.is_finite())
            .ok_or(Failure::Usage),
    }
}
fn safe(value: f64) -> Result<SafeUnsigned, Failure> {
    project_wire_safe_unsigned(&Value::from(value)).map_err(|_| Failure::Usage)
}
fn cost(value: f64) -> Result<FiniteF64, Failure> {
    FiniteF64::new(value).map_err(|_| Failure::Usage)
}
fn usage(value: &Value, model: &ImagesModel) -> Result<Usage, Failure> {
    let prompt = count(value.get("prompt_tokens"))?;
    let cached = count(
        value
            .get("prompt_tokens_details")
            .and_then(|v| v.get("cached_tokens")),
    )?;
    let write = count(
        value
            .get("prompt_tokens_details")
            .and_then(|v| v.get("cache_write_tokens")),
    )?;
    let read = if write > 0.0 {
        (cached - write).max(0.0)
    } else {
        cached
    };
    let input = (prompt - read - write).max(0.0);
    let output = count(value.get("completion_tokens"))?;
    let ci = (model.cost.input.get() / 1_000_000.0) * input;
    let co = (model.cost.output.get() / 1_000_000.0) * output;
    let cr = (model.cost.cache_read.get() / 1_000_000.0) * read;
    let cw = (model.cost.cache_write.get() / 1_000_000.0) * write;
    Ok(Usage {
        input: safe(input)?,
        output: safe(output)?,
        cache_read: safe(read)?,
        cache_write: safe(write)?,
        cache_write_1h: None,
        reasoning: None,
        total_tokens: safe(input + output + read + write)?,
        cost: UsageCost {
            input: cost(ci)?,
            output: cost(co)?,
            cache_read: cost(cr)?,
            cache_write: cost(cw)?,
            total: cost(ci + co + cr + cw)?,
        },
    })
}
pub(super) fn reduce(
    value: Value,
    model: &ImagesModel,
    output: &mut AssistantImages,
) -> Result<(), Failure> {
    if value.is_null() {
        return Err(Failure::Wire);
    }
    if let Some(id) = value.get("id") {
        output.response_id =
            Some(ResponseId::new(id.as_str().ok_or(Failure::Wire)?).map_err(|_| Failure::Wire)?);
    }
    if let Some(raw) = value.get("usage").filter(|v| truthy(v)) {
        output.usage = Some(usage(raw, model)?);
    }
    let choices = value
        .get("choices")
        .filter(|v| !v.is_null())
        .ok_or(Failure::Wire)?;
    // JS indexing accepts objects and primitive non-null values too.
    let choice = match choices {
        Value::Array(v) => v.first(),
        Value::Object(v) => v.get("0"),
        Value::String(v) if !v.is_empty() => return Err(Failure::Wire),
        _ => None,
    };
    let Some(choice) = choice.filter(|v| truthy(v)) else {
        return Ok(());
    };
    let message = choice
        .get("message")
        .filter(|v| !v.is_null())
        .ok_or(Failure::Wire)?;
    if let Some(text) = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        output.output.push(InputContent::Text(TextContent {
            content_type: TextContentType::Text,
            text: text.to_owned(),
            text_signature: None,
        }));
    }
    match message.get("images") {
        None | Some(Value::Null) => {}
        Some(Value::String(_)) => {} // character strings have no image_url
        Some(Value::Array(images)) => {
            for image in images {
                if image.is_null() {
                    return Err(Failure::Wire);
                }
                let Some(url) = image.get("image_url") else {
                    continue;
                };
                let url = if let Some(url) = url.as_str() {
                    Some(url)
                } else {
                    match url.get("url") {
                        None | Some(Value::Null) => None,
                        Some(v) => Some(v.as_str().ok_or(Failure::Wire)?),
                    }
                };
                if let Some(url) = url
                    && let Some((mime, data)) = data_url(url)
                {
                    output.output.push(InputContent::Image(ImageContent {
                        content_type: ImageContentType::Image,
                        mime_type: mime.to_owned(),
                        data: data.to_owned(),
                    }));
                }
            }
        }
        _ => return Err(Failure::Wire),
    }
    Ok(())
}
fn data_url(url: &str) -> Option<(&str, &str)> {
    let (mime, rest) = url.strip_prefix("data:")?.split_once(';')?;
    let data = rest.strip_prefix("base64,")?;
    (!mime.is_empty() && !data.is_empty() && !data.contains(['\n', '\r', '\u{2028}', '\u{2029}']))
        .then_some((mime, data))
}
