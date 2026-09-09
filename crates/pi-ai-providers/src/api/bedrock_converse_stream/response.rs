use super::{Failure, build::whitespace, payload::BedrockValue, sdk};
use pi_ai_core::{AssistantMessage, RequestRuntime};
use pi_ai_transport::{
    ResponseBody, SmithyEventStreamDecoder, SmithyEventStreamLimits, SmithyEventStreamMessage,
    SmithyHeaderValue,
};
use serde_json::Value;

pub(super) struct BodyOwner(pub Box<dyn ResponseBody>);
impl Drop for BodyOwner {
    fn drop(&mut self) {
        self.0.close();
    }
}
pub(super) struct Reader {
    body: Option<BodyOwner>,
    decoder: SmithyEventStreamDecoder,
    eof: bool,
}
pub(super) enum Event {
    Modeled(String, BedrockValue),
    Initial,
}
impl Reader {
    pub fn new(body: Box<dyn ResponseBody>, limits: SmithyEventStreamLimits) -> Self {
        Self {
            body: Some(BodyOwner(body)),
            decoder: SmithyEventStreamDecoder::new(limits),
            eof: false,
        }
    }
    async fn frame(
        &mut self,
        runtime: &RequestRuntime,
    ) -> Result<Option<SmithyEventStreamMessage>, Failure> {
        loop {
            if let Some(frame) = self.decoder.next_frame().map_err(Failure::transport)? {
                return Ok(Some(frame));
            }
            if self.eof {
                return Ok(None);
            }
            let mut body = self.body.take().ok_or_else(Failure::local)?;
            let chunk =
                crate::api::runtime::cancellable(body.0.next(runtime), &runtime.cancellation)
                    .await
                    .map_err(|_| Failure::local())?
                    .map_err(Failure::transport)?;
            if let Some(chunk) = chunk {
                self.decoder.push(&chunk).map_err(Failure::transport)?;
                self.body = Some(body);
            } else {
                self.eof = true;
                self.decoder.finish();
            }
        }
    }
    pub async fn next(
        &mut self,
        runtime: &RequestRuntime,
        template: &AssistantMessage,
        priming: bool,
    ) -> Result<Option<Event>, Failure> {
        while let Some(frame) = self.frame(runtime).await? {
            let ty = string_header(&frame, ":message-type").ok_or_else(Failure::local)?;
            if ty == "error" {
                let code = string_header(&frame, ":error-code").ok_or_else(Failure::local)?;
                let message = string_header(&frame, ":error-message").ok_or_else(Failure::local)?;
                return Err(Failure::message(
                    None,
                    Some(code),
                    if message.is_empty() {
                        "UnknownError"
                    } else {
                        message
                    },
                    template,
                ));
            }
            let exception = ty == "exception";
            if ty != "event" && !exception {
                return Err(Failure::local());
            }
            let key = if exception {
                ":exception-type"
            } else {
                ":event-type"
            };
            let Some(SmithyHeaderValue::String(kind)) = frame.get_header(key) else {
                if exception || frame.get_header(key).is_none() {
                    return Err(Failure::local());
                }
                continue;
            };
            if matches!(
                kind.as_str(),
                "__type"
                    | "__proto__"
                    | "constructor"
                    | "toString"
                    | "valueOf"
                    | "hasOwnProperty"
                    | "isPrototypeOf"
                    | "propertyIsEnumerable"
                    | "toLocaleString"
                    | "__defineGetter__"
                    | "__defineSetter__"
                    | "__lookupGetter__"
                    | "__lookupSetter__"
            ) {
                return Err(Failure::local());
            }
            if kind == "initial-response" && !exception {
                let value = parse(&frame.payload, template, priming)?;
                sdk::deserialize(value, "ConverseStreamResponse").map_err(|_| Failure::local())?;
                return Ok(Some(Event::Initial));
            }
            let shape = match kind.as_str() {
                "messageStart" => "MessageStartEvent",
                "contentBlockStart" => "ContentBlockStartEvent",
                "contentBlockDelta" => "ContentBlockDeltaEvent",
                "contentBlockStop" => "ContentBlockStopEvent",
                "messageStop" => "MessageStopEvent",
                "metadata" => "ConverseStreamMetadataEvent",
                "internalServerException" => "InternalServerException",
                "modelStreamErrorException" => "ModelStreamErrorException",
                "validationException" => "ValidationException",
                "throttlingException" => "ThrottlingException",
                "serviceUnavailableException" => "ServiceUnavailableException",
                _ => {
                    if exception {
                        return Err(Failure::message(
                            None,
                            Some(kind),
                            &String::from_utf8_lossy(&frame.payload),
                            template,
                        ));
                    }
                    continue;
                }
            };
            let value = sdk::deserialize(parse(&frame.payload, template, priming)?, shape)
                .map_err(|_| Failure::local())?;
            if exception {
                let message = if priming {
                    value
                        .get("message")
                        .and_then(BedrockValue::as_str)
                        .unwrap_or("undefined")
                        .to_owned()
                } else {
                    crate::api::wire_json::stringify_wire_json(&value.to_json())
                        .map_err(|_| Failure::local())?
                };
                return Err(Failure::message(None, None, &message, template));
            }
            return Ok(Some(Event::Modeled(kind.clone(), value)));
        }
        Ok(None)
    }
}
fn string_header<'a>(frame: &'a SmithyEventStreamMessage, name: &str) -> Option<&'a str> {
    match frame.get_header(name) {
        Some(SmithyHeaderValue::String(v)) => Some(v),
        _ => None,
    }
}
fn parse(bytes: &[u8], template: &AssistantMessage, priming: bool) -> Result<Value, Failure> {
    if bytes.is_empty() {
        return Ok(serde_json::json!({}));
    }
    let text = String::from_utf8_lossy(bytes);
    serde_json::from_str(&text).map_err(|error| {
        // Only the unresolved SDK send attaches response-body fallback. Later
        // iterator SyntaxErrors expose parser facts, never the full body.
        let observation = if priming {
            truncate(text.trim_matches(whitespace))
        } else {
            parser_observation(&text, &error)
        };
        Failure::message(None, None, &observation, template)
    })
}
/// The pinned parser's observable classifier input: a bounded unexpected-token
/// preview or diagnostic position. This never becomes displayed exception text.
fn parser_observation(text: &str, error: &serde_json::Error) -> String {
    let category = error.to_string();
    let line_start = if error.line() > 1 {
        text.match_indices('\n')
            .nth(error.line() - 2)
            .map_or(0, |(offset, _)| offset + 1)
    } else {
        0
    };
    let byte_position = line_start
        .saturating_add(error.column().saturating_sub(usize::from(!error.is_eof())))
        .min(text.len());
    let byte_position = (0..=byte_position)
        .rev()
        .find(|i| text.is_char_boundary(*i))
        .unwrap_or(0);
    let position = text[..byte_position].encode_utf16().count();
    if !error.is_eof()
        && (category.starts_with("expected value") || category.starts_with("expected ident"))
    {
        let length = text.encode_utf16().count();
        let (start, end) = if length <= 20 {
            (0, length)
        } else {
            (
                position.saturating_sub(10),
                position.saturating_add(10).min(length),
            )
        };
        return String::from_utf16_lossy(
            &text
                .encode_utf16()
                .skip(start)
                .take(end - start)
                .collect::<Vec<_>>(),
        );
    }
    if error.is_eof() {
        let ending = text.trim_end_matches([' ', '\t', '\r', '\n']);
        let unfinished_value = category.starts_with("EOF while parsing a value")
            && !ending.ends_with(['-', '+', '.', 'e', 'E']);
        let empty_array = category.starts_with("EOF while parsing a list") && ending.ends_with('[');
        if unfinished_value || empty_array {
            return String::new();
        }
    }
    // V8 diagnostic locations count CR, LF and CRLF; serde_json's byte
    // location above is used only to locate the source token.
    let (mut line, mut column, mut carriage_return) = (1usize, 1usize, false);
    for ch in text[..byte_position].chars() {
        match ch {
            '\r' => {
                line += 1;
                column = 1;
                carriage_return = true;
            }
            '\n' => {
                if !carriage_return {
                    line += 1;
                }
                column = 1;
                carriage_return = false;
            }
            _ => {
                column += ch.len_utf16();
                carriage_return = false;
            }
        }
    }
    format!("JSON parser position {position} line {line} column {column}")
}
pub(super) fn truncate(text: &str) -> String {
    let units = text.encode_utf16().collect::<Vec<_>>();
    if units.len() <= 4000 {
        return text.to_owned();
    }
    format!(
        "{}... [truncated {} chars]",
        String::from_utf16_lossy(&units[..4000]),
        units.len() - 4000
    )
}
