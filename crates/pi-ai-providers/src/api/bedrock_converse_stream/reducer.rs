use super::{BedrockLimits, Failure, build::whitespace, payload::BedrockValue as V, sdk};
use pi_ai_core::*;
use serde_json::{Value, json};
fn invalid() -> Failure {
    Failure::local()
}
fn encoded_size(n: usize) -> Result<usize, Failure> {
    n.checked_add(2)
        .and_then(|n| (n / 3).checked_mul(4))
        .ok_or_else(invalid)
}
fn truthy(v: &V) -> bool {
    match v {
        V::Null => false,
        V::Bool(v) => *v,
        V::Number(v) => v.get() != 0.0,
        V::String(v) => !v.is_empty(),
        _ => true,
    }
}
fn string(v: Option<&V>) -> Result<&str, Failure> {
    v.and_then(V::as_str).ok_or_else(invalid)
}
fn arguments(value: &str) -> Result<JsonObject, Failure> {
    match parse_streaming_json(Some(value)) {
        JsonValue::Object(v) => Ok(v),
        _ => Err(invalid()),
    }
}
enum BlockIndex {
    Null,
    Bool(bool),
    Number(FiniteF64),
    String(String),
    Opaque,
}
impl BlockIndex {
    fn capture(v: &V) -> Self {
        match v {
            V::Null => Self::Null,
            V::Bool(b) => Self::Bool(*b),
            V::Number(n) => Self::Number(*n),
            V::String(s) => Self::String(s.clone()),
            _ => Self::Opaque,
        }
    }
    fn matches(&self, v: &V) -> bool {
        match (self, v) {
            (Self::Null, V::Null) => true,
            (Self::Bool(a), V::Bool(b)) => a == b,
            (Self::Number(a), V::Number(b)) => a == b,
            (Self::String(a), V::String(b)) => a == b,
            _ => false,
        }
    }
    fn bytes(&self) -> usize {
        if let Self::String(s) = self {
            s.len()
        } else {
            0
        }
    }
}
#[derive(Default)]
struct Scratch {
    index: Option<BlockIndex>,
    partial: String,
    redacted: Vec<u8>,
    arguments_bytes: usize,
}
pub(super) struct Reducer {
    pub partial: AssistantMessage,
    scratch: Vec<Scratch>,
    limits: BedrockLimits,
}
impl Reducer {
    pub fn new(request: &TextStreamRequest, limits: BedrockLimits) -> Self {
        Self {
            partial: empty(request),
            scratch: Vec::new(),
            limits,
        }
    }
    pub fn snapshot(&self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        let mut snapshot = self.partial.clone();
        for (block, scratch) in snapshot.content.iter_mut().zip(&self.scratch) {
            if let AssistantContent::Thinking(t) = block
                && t.redacted == Some(true)
                && !scratch.redacted.is_empty()
            {
                t.thinking_signature = Some(sdk::base64_encode(&scratch.redacted));
            }
        }
        producer
            .publish_cancellation_snapshot(snapshot)
            .map_err(|_| invalid())
    }
    fn retained_bytes(&self) -> Result<usize, Failure> {
        let mut size = 0usize;
        for (block, scratch) in self.partial.content.iter().zip(&self.scratch) {
            let bytes = match block {
                AssistantContent::Text(t) => t.text.len(),
                AssistantContent::Thinking(t) => t
                    .thinking
                    .len()
                    .saturating_add(t.thinking_signature.as_ref().map_or(0, String::len))
                    .saturating_add(scratch.redacted.len())
                    .saturating_add(encoded_size(scratch.redacted.len())?),
                AssistantContent::ToolCall(t) => {
                    t.id.as_str()
                        .len()
                        .saturating_add(t.name.as_str().len())
                        .saturating_add(scratch.partial.len())
                        .saturating_add(scratch.arguments_bytes)
                }
            };
            size = size
                .checked_add(bytes)
                .and_then(|n| n.checked_add(scratch.index.as_ref().map_or(0, BlockIndex::bytes)))
                .ok_or_else(invalid)?;
        }
        Ok(size)
    }
    fn admit(&self, additional: usize) -> Result<(), Failure> {
        if additional
            > self
                .limits
                .max_content_bytes
                .saturating_sub(self.retained_bytes()?)
        {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    fn bounds(&self) -> Result<(), Failure> {
        if self.partial.content.len() > self.limits.max_content_blocks
            || self.retained_bytes()? > self.limits.max_content_bytes
            || self
                .scratch
                .iter()
                .any(|s| s.partial.len() > self.limits.max_tool_argument_bytes)
        {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    fn push(
        &self,
        producer: &AssistantStreamProducer,
        event: AssistantMessageEvent,
    ) -> Result<(), Failure> {
        self.bounds()?;
        self.snapshot(producer)?;
        producer.push(event).map_err(|_| invalid())
    }
    fn partial(&self) -> Box<AssistantMessage> {
        Box::new(self.partial.clone())
    }
    fn idx(index: usize) -> SafeUnsigned {
        SafeUnsigned::new(index as u64).expect("bounded content index")
    }
    fn find(&self, event: &V) -> Option<usize> {
        let index = event.get("contentBlockIndex");
        self.scratch
            .iter()
            .position(|b| match (b.index.as_ref(), index) {
                (None, None) => true,
                (Some(a), Some(b)) => a.matches(b),
                _ => false,
            })
    }
    fn add(&mut self, block: AssistantContent, index: Option<&V>) -> Result<usize, Failure> {
        if self.partial.content.len() >= self.limits.max_content_blocks {
            return Err(invalid());
        }
        let arguments_bytes = if matches!(&block, AssistantContent::ToolCall(_)) {
            2
        } else {
            0
        };
        let charge = match &block {
            AssistantContent::ToolCall(t) => {
                t.id.as_str()
                    .len()
                    .saturating_add(t.name.as_str().len())
                    .saturating_add(arguments_bytes)
            }
            _ => 0,
        };
        self.admit(charge.saturating_add(index.and_then(V::as_str).map_or(0, str::len)))?;
        let i = self.partial.content.len();
        self.partial.content.push(block);
        self.scratch.push(Scratch {
            index: index.map(BlockIndex::capture),
            arguments_bytes,
            ..Default::default()
        });
        Ok(i)
    }
    pub fn consume(
        &mut self,
        kind: &str,
        event: &V,
        model: &Model,
        producer: &AssistantStreamProducer,
    ) -> Result<(), Failure> {
        if !truthy(event) {
            return Ok(());
        }
        match kind {
            "messageStart" => {
                if string(event.get("role"))? != "assistant" {
                    return Err(invalid());
                }
                self.push(
                    producer,
                    AssistantMessageEvent::Start {
                        partial: self.partial(),
                    },
                )?;
            }
            "contentBlockStart" => {
                if let Some(tool) = event
                    .get("start")
                    .and_then(|s| s.get("toolUse"))
                    .filter(|v| truthy(v))
                {
                    let id = string(tool.get("toolUseId"))?
                        .parse()
                        .map_err(|_| invalid())?;
                    let name = string(tool.get("name"))?.parse().map_err(|_| invalid())?;
                    let index = self.add(
                        AssistantContent::ToolCall(ToolCall {
                            content_type: ToolCallType::ToolCall,
                            id,
                            name,
                            arguments: JsonObject::default(),
                            thought_signature: None,
                            namespace: None,
                        }),
                        event.get("contentBlockIndex"),
                    )?;
                    self.push(
                        producer,
                        AssistantMessageEvent::ToolCallStart {
                            content_index: Self::idx(index),
                            partial: self.partial(),
                        },
                    )?;
                }
            }
            "contentBlockDelta" => {
                let Some(delta) = event.get("delta") else {
                    return Ok(());
                };
                let index = self.find(event);
                if let Some(text) = delta.get("text") {
                    let text = string(Some(text))?.to_owned();
                    let index = if let Some(index) = index {
                        index
                    } else {
                        let index = self.add(
                            AssistantContent::Text(TextContent {
                                content_type: TextContentType::Text,
                                text: String::new(),
                                text_signature: None,
                            }),
                            event.get("contentBlockIndex"),
                        )?;
                        self.push(
                            producer,
                            AssistantMessageEvent::TextStart {
                                content_index: Self::idx(index),
                                partial: self.partial(),
                            },
                        )?;
                        index
                    };
                    if matches!(&self.partial.content[index], AssistantContent::Text(_)) {
                        self.admit(text.len())?;
                    }
                    if let AssistantContent::Text(block) = &mut self.partial.content[index] {
                        block.text.push_str(&text);
                        self.push(
                            producer,
                            AssistantMessageEvent::TextDelta {
                                content_index: Self::idx(index),
                                delta: text,
                                partial: self.partial(),
                            },
                        )?;
                    }
                } else if let Some(tool) = delta.get("toolUse").filter(|v| truthy(v))
                    && let Some(index) = index
                    && matches!(self.partial.content[index], AssistantContent::ToolCall(_))
                {
                    let delta = tool
                        .get("input")
                        .filter(|v| truthy(v))
                        .map(|v| string(Some(v)))
                        .transpose()?
                        .unwrap_or("")
                        .to_owned();
                    if delta.len()
                        > self
                            .limits
                            .max_tool_argument_bytes
                            .saturating_sub(self.scratch[index].partial.len())
                    {
                        return Err(invalid());
                    }
                    self.admit(delta.len())?;
                    let mut candidate = self.scratch[index].partial.clone();
                    candidate.push_str(&delta);
                    let args = arguments(&candidate)?;
                    let arguments_bytes = serde_json::to_vec(&args).map_err(|_| invalid())?.len();
                    self.admit(
                        delta
                            .len()
                            .saturating_add(arguments_bytes)
                            .saturating_sub(self.scratch[index].arguments_bytes),
                    )?;
                    self.scratch[index].partial = candidate;
                    self.scratch[index].arguments_bytes = arguments_bytes;
                    if let AssistantContent::ToolCall(tool) = &mut self.partial.content[index] {
                        tool.arguments = args;
                    }
                    self.push(
                        producer,
                        AssistantMessageEvent::ToolCallDelta {
                            content_index: Self::idx(index),
                            delta,
                            partial: self.partial(),
                        },
                    )?;
                } else if let Some(reason) = delta.get("reasoningContent").filter(|v| truthy(v)) {
                    let index = if let Some(index) = index {
                        index
                    } else {
                        let index = self.add(
                            AssistantContent::Thinking(ThinkingContent {
                                content_type: ThinkingContentType::Thinking,
                                thinking: String::new(),
                                thinking_signature: Some(String::new()),
                                redacted: None,
                            }),
                            event.get("contentBlockIndex"),
                        )?;
                        self.push(
                            producer,
                            AssistantMessageEvent::ThinkingStart {
                                content_index: Self::idx(index),
                                partial: self.partial(),
                            },
                        )?;
                        index
                    };
                    if !matches!(self.partial.content[index], AssistantContent::Thinking(_)) {
                        return Ok(());
                    }
                    if let Some(text) = reason.get("text").filter(|v| truthy(v)) {
                        let delta = string(Some(text))?.to_owned();
                        self.admit(delta.len())?;
                        if let AssistantContent::Thinking(block) = &mut self.partial.content[index]
                        {
                            block.thinking.push_str(&delta);
                        }
                        self.push(
                            producer,
                            AssistantMessageEvent::ThinkingDelta {
                                content_index: Self::idx(index),
                                delta,
                                partial: self.partial(),
                            },
                        )?;
                    }
                    if let Some(signature) = reason.get("signature").filter(|v| truthy(v))
                        && matches!(&self.partial.content[index],AssistantContent::Thinking(t)if t.redacted!=Some(true))
                    {
                        let signature = string(Some(signature))?;
                        self.admit(signature.len())?;
                        if let AssistantContent::Thinking(block) = &mut self.partial.content[index]
                        {
                            block
                                .thinking_signature
                                .get_or_insert_default()
                                .push_str(signature);
                        }
                    }
                    if let Some(bytes) = reason
                        .get("redactedContent")
                        .and_then(V::as_bytes)
                        .filter(|b| !b.is_empty())
                    {
                        let first = matches!(&self.partial.content[index],AssistantContent::Thinking(t)if t.redacted!=Some(true));
                        let old = self.scratch[index].redacted.len();
                        let total = old.checked_add(bytes.len()).ok_or_else(invalid)?;
                        let expansion = encoded_size(total)?.saturating_sub(encoded_size(old)?);
                        let released = if first {
                            match &self.partial.content[index] {
                                AssistantContent::Thinking(t) => {
                                    t.thinking_signature.as_ref().map_or(0, String::len)
                                }
                                _ => 0,
                            }
                        } else {
                            0
                        };
                        self.admit(
                            bytes
                                .len()
                                .saturating_add(expansion)
                                .saturating_add(if first { 20 } else { 0 })
                                .saturating_sub(released),
                        )?;
                        if first {
                            if let AssistantContent::Thinking(block) =
                                &mut self.partial.content[index]
                            {
                                block.redacted = Some(true);
                                block.thinking_signature = Some(String::new());
                                block.thinking.push_str("[Reasoning redacted]");
                            }
                            self.push(
                                producer,
                                AssistantMessageEvent::ThinkingDelta {
                                    content_index: Self::idx(index),
                                    delta: "[Reasoning redacted]".into(),
                                    partial: self.partial(),
                                },
                            )?;
                        }
                        if bytes.len()
                            > self
                                .limits
                                .max_content_bytes
                                .saturating_sub(self.scratch[index].redacted.len())
                        {
                            return Err(invalid());
                        }
                        self.scratch[index].redacted.extend_from_slice(bytes);
                    }
                    self.bounds()?;
                    self.snapshot(producer)?;
                }
            }
            "contentBlockStop" => {
                if let Some(index) = self.find(event) {
                    self.scratch[index].index = None;
                    self.flush(index);
                    let event = match &self.partial.content[index] {
                        AssistantContent::Text(t) => AssistantMessageEvent::TextEnd {
                            content_index: Self::idx(index),
                            content: t.text.clone(),
                            partial: self.partial(),
                        },
                        AssistantContent::Thinking(t) => AssistantMessageEvent::ThinkingEnd {
                            content_index: Self::idx(index),
                            content: t.thinking.clone(),
                            partial: self.partial(),
                        },
                        AssistantContent::ToolCall(t) => {
                            let mut tool = t.clone();
                            tool.arguments = arguments(&self.scratch[index].partial)?;
                            self.partial.content[index] = AssistantContent::ToolCall(tool.clone());
                            self.scratch[index].partial.clear();
                            AssistantMessageEvent::ToolCallEnd {
                                content_index: Self::idx(index),
                                tool_call: tool,
                                partial: self.partial(),
                            }
                        }
                    };
                    self.push(producer, event)?;
                }
            }
            "messageStop" => {
                let reason = event
                    .get("stopReason")
                    .map(|v| string(Some(v)))
                    .transpose()?;
                self.partial.raw_stop_reason = reason
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse().map_err(|_| invalid()))
                    .transpose()?;
                self.partial.stop_reason = match reason {
                    Some("end_turn" | "stop_sequence") => StopReason::Stop,
                    Some("max_tokens" | "model_context_window_exceeded") => StopReason::Length,
                    Some("tool_use") => StopReason::ToolUse,
                    _ => StopReason::Error,
                };
                if self.partial.stop_reason == StopReason::Error {
                    self.partial.error_message =
                        Some("Bedrock provider stopped with an unsuccessful reason".into());
                }
                self.snapshot(producer)?;
            }
            "metadata" => {
                if let Some(usage) = event.get("usage").filter(|v| truthy(v)) {
                    let count = |name| -> Result<SafeUnsigned, Failure> {
                        let value = usage
                            .get(name)
                            .filter(|v| truthy(v))
                            .map(V::to_json)
                            .unwrap_or(json!(0));
                        crate::api::wire_json::project_wire_safe_unsigned(&value)
                            .map_err(|_| invalid())
                    };
                    self.partial.usage.input = count("inputTokens")?;
                    self.partial.usage.output = count("outputTokens")?;
                    self.partial.usage.cache_read = count("cacheReadInputTokens")?;
                    self.partial.usage.cache_write = count("cacheWriteInputTokens")?;
                    self.partial.usage.total_tokens =
                        if usage.get("totalTokens").is_some_and(truthy) {
                            count("totalTokens")?
                        } else {
                            SafeUnsigned::new(
                                self.partial
                                    .usage
                                    .input
                                    .get()
                                    .checked_add(self.partial.usage.output.get())
                                    .ok_or_else(invalid)?,
                            )
                            .map_err(|_| invalid())?
                        };
                    calculate_cost(model, &mut self.partial.usage).map_err(|_| invalid())?;
                    self.snapshot(producer)?;
                }
            }
            "internalServerException"
            | "modelStreamErrorException"
            | "validationException"
            | "throttlingException"
            | "serviceUnavailableException" => {
                return Err(Failure::message(
                    None,
                    None,
                    &crate::api::wire_json::stringify_wire_json(&event.to_json())
                        .map_err(|_| invalid())?,
                    &self.partial,
                ));
            }
            _ => {}
        }
        Ok(())
    }
    fn flush(&mut self, index: usize) {
        if let AssistantContent::Thinking(t) = &mut self.partial.content[index]
            && t.redacted == Some(true)
            && !self.scratch[index].redacted.is_empty()
        {
            t.thinking_signature = Some(sdk::base64_encode(&self.scratch[index].redacted));
            self.scratch[index].redacted.clear();
        }
    }
    pub fn finalize(&mut self) {
        for i in 0..self.partial.content.len() {
            self.flush(i);
            self.scratch[i].index = None;
            self.scratch[i].partial.clear();
        }
    }
    pub fn finish(&mut self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        self.finalize();
        let reason = match self.partial.stop_reason {
            StopReason::Stop => DoneReason::Stop,
            StopReason::Length => DoneReason::Length,
            StopReason::ToolUse => DoneReason::ToolUse,
            StopReason::Pending => {
                return Err(Failure::message(
                    None,
                    None,
                    "Bedrock stream ended without a stop reason",
                    &self.partial,
                ));
            }
            _ => {
                return Err(Failure::message(
                    None,
                    None,
                    &self.partial.raw_stop_reason.as_ref().map_or_else(
                        || "An unknown error occurred".into(),
                        |r| format!("Provider stopped with: {}", r.as_str()),
                    ),
                    &self.partial,
                ));
            }
        };
        producer
            .push(AssistantMessageEvent::Done {
                reason,
                message: self.partial(),
            })
            .map_err(|_| invalid())
    }
}
pub(super) fn diagnostic(value: &str) -> Option<String> {
    let value = value.trim_matches(whitespace);
    (!value.is_empty() && value.encode_utf16().count() <= 200).then(|| value.to_owned())
}
pub(super) fn empty(request: &TextStreamRequest) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: vec![],
        api: request.resolved.model.api.clone(),
        provider: request.resolved.model.provider.clone(),
        model: request.resolved.model.id.clone(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: StopReason::Pending,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
            .unwrap_or(SafeUnsigned::MAX),
    }
}
pub(super) fn error(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut reducer: Reducer,
    failure: Failure,
) {
    reducer.finalize();
    let mut partial = reducer.partial;
    let aborted = request.options.runtime.cancellation.is_cancelled();
    partial.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    partial.error_message = Some(
        if aborted {
            REQUEST_CANCELLED_MESSAGE
        } else {
            failure.safe
        }
        .into(),
    );
    if !aborted {
        let mut details = serde_json::Map::new();
        if let Some(status) = failure.status {
            details.insert("status".into(), json!(status));
        }
        if let Some(code) = failure.code {
            details.insert("errorCode".into(), json!(code));
        }
        if let Some(id) = failure.request_id {
            details.insert("requestId".into(), json!(id));
        }
        if !details.is_empty() {
            partial.diagnostics = Some(vec![AssistantMessageDiagnostic {
                diagnostic_type: "bedrock_response_failure".parse().expect("diagnostic type"),
                timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
                    .unwrap_or(SafeUnsigned::MAX),
                error: None,
                details: Some(
                    serde_json::from_value(Value::Object(details))
                        .expect("bounded diagnostic values"),
                ),
            }]);
        }
    }
    let _ = producer.push(AssistantMessageEvent::Error {
        reason: if aborted {
            ErrorReason::Aborted
        } else {
            ErrorReason::Error
        },
        error: Box::new(partial),
    });
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use std::{sync::Arc, time::Duration};
    struct ClockPort;
    impl Clock for ClockPort {
        fn now_unix_ms(&self) -> u64 {
            1767323045000
        }
        fn sleep<'a>(
            &'a self,
            _: Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(async { Ok(()) })
        }
    }
    fn setup(
        limit: usize,
    ) -> (
        Reducer,
        Model,
        AssistantMessageEventStream,
        AssistantStreamProducer,
    ) {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../../conformance/provider-traces/bedrock-converse-stream/family.json"
        ))
        .unwrap();
        let row = &corpus["cases"][0];
        let model: Model = serde_json::from_value(row["model"].clone()).unwrap();
        let partial = serde_json::from_value(row["events"][0]["partial"].clone()).unwrap();
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            clock: Arc::new(ClockPort),
            cancellation: CancellationToken::new(),
        });
        (
            Reducer {
                partial,
                scratch: vec![],
                limits: BedrockLimits {
                    max_content_bytes: limit,
                    ..Default::default()
                },
            },
            model,
            stream,
            producer,
        )
    }
    #[test]
    fn over_limit_delta_does_not_enter_retained_terminal_content() {
        let (mut r, m, _stream, p) = setup(3);
        r.consume(
            "messageStart",
            &V::from_json(json!({"role":"assistant"})),
            &m,
            &p,
        )
        .unwrap();
        r.consume(
            "contentBlockDelta",
            &V::from_json(json!({"contentBlockIndex":0,"delta":{"text":"ab"}})),
            &m,
            &p,
        )
        .unwrap();
        assert!(
            r.consume(
                "contentBlockDelta",
                &V::from_json(json!({"contentBlockIndex":0,"delta":{"text":"cd"}})),
                &m,
                &p
            )
            .is_err()
        );
        assert!(matches!(&r.partial.content[0],AssistantContent::Text(t)if t.text=="ab"));
        assert!(r.bounds().is_ok());
    }
    #[test]
    fn completed_tool_arguments_remain_charged() {
        let (mut r, m, _stream, p) = setup(64);
        r.consume(
            "messageStart",
            &V::from_json(json!({"role":"assistant"})),
            &m,
            &p,
        )
        .unwrap();
        for (kind, event) in [
            (
                "contentBlockStart",
                json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"i","name":"n"}}}),
            ),
            (
                "contentBlockDelta",
                json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"{\"x\":\"abcdefgh\"}"}}}),
            ),
            ("contentBlockStop", json!({"contentBlockIndex":0})),
        ] {
            r.consume(kind, &V::from_json(event), &m, &p).unwrap();
        }
        r.limits.max_content_bytes = 3;
        assert!(r.bounds().is_err());
    }
    #[test]
    fn redacted_replacement_releases_old_signature_and_charges_snapshot_expansion() {
        let (mut r, m, _stream, p) = setup(26);
        r.consume(
            "messageStart",
            &V::from_json(json!({"role":"assistant"})),
            &m,
            &p,
        )
        .unwrap();
        r.consume(
            "contentBlockDelta",
            &V::from_json(
                json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"signature":"old"}}}),
            ),
            &m,
            &p,
        )
        .unwrap();
        let mut event = V::from_json(
            json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"redactedContent":null}}}),
        );
        *event
            .get_mut("delta")
            .unwrap()
            .get_mut("reasoningContent")
            .unwrap()
            .get_mut("redactedContent")
            .unwrap() = V::Bytes(vec![1, 2]);
        r.consume("contentBlockDelta", &event, &m, &p).unwrap();
        assert_eq!(r.retained_bytes().unwrap(), 26);
        assert!(r.consume("contentBlockDelta", &event, &m, &p).is_err());
        assert_eq!(r.retained_bytes().unwrap(), 26);
        r.finalize();
        assert_eq!(r.retained_bytes().unwrap(), 24);
    }
    #[test]
    fn string_index_is_admitted_before_retention() {
        let (mut r, m, _stream, p) = setup(2);
        assert!(
            r.consume(
                "contentBlockDelta",
                &V::from_json(json!({"contentBlockIndex":"long","delta":{"text":"a"}})),
                &m,
                &p
            )
            .is_err()
        );
        assert_eq!(r.partial.content.len(), 0);
        assert_eq!(r.retained_bytes().unwrap(), 0);
    }
}
