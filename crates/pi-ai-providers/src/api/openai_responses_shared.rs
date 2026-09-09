//! Responses-family conversion and incremental event reduction.
//!
//! HTTP, initial Start and final Done/Error events belong to the outer adapter.
//! Wire JSON and replay signatures are transient; diagnostics retain categories only.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::task::Poll;

use pi_ai_core::{
    AssistantContent, AssistantMessage, AssistantMessageEvent, AssistantStreamProducer, BoxFuture,
    CancellationToken, Context, InputContent, JsonObject, JsonValue, LiteralOne, Message, Model,
    ModelInput, Presence, RawStopReason, ResponseId, SafeUnsigned, StopReason, TextContent,
    TextContentType, TextSignaturePhase, TextSignatureV1, ThinkingContent, ThinkingContentType,
    Tool, ToolCall, ToolCallId, ToolCallIdNormalizer, ToolCallType, ToolName, Usage,
    calculate_cost, is_context_overflow, is_retryable_assistant_error, parse_streaming_json,
    short_hash, transform_messages,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::wire_json::{WireJsonError, project_wire_safe_unsigned, stringify_wire_json};

use super::constrained_sampling::{
    ConstrainedSamplingError, GrammarToolInputJsonBuffer, append_grammar_tool_input_json_delta,
    get_grammar_tool_input, get_wire_json_schema_tool_parameters,
    resolve_grammar_constrained_sampling, resolve_json_schema_strict_sampling,
};

/// Failure categories without provider-controlled text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponsesErrorKind {
    /// Invalid wire shape, identifier or safe numeric projection.
    InvalidWire,
    /// Missing terminal response at end-of-stream.
    MissingTerminal,
    /// Caller cancellation won the next-event wait.
    Cancelled,
    /// The supplied producer was already settled or rejected its identity.
    Producer,
    /// Provider returned an error event or failed response.
    Provider,
    /// Message transformation, signature or tool conversion failed.
    Conversion,
    /// Bounded wire or accumulated argument input exceeded its limit.
    ResourceLimit,
    /// Trusted service-tier policy failed.
    Policy,
}

/// Redacted failure with independent retry/overflow observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponsesError {
    /// Closed failure kind.
    pub kind: ResponsesErrorKind,
    /// Retry classifier observation made before discarding raw provider error text.
    pub retryable: bool,
    /// Overflow classifier observation made before discarding raw provider error text.
    pub context_overflow: bool,
}

impl ResponsesError {
    /// Creates a category-only error, suitable for transport event-source adaptation.
    pub const fn new(kind: ResponsesErrorKind) -> Self {
        Self {
            kind,
            retryable: false,
            context_overflow: false,
        }
    }
}

impl fmt::Display for ResponsesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ResponsesErrorKind::InvalidWire => "invalid Responses wire event",
            ResponsesErrorKind::MissingTerminal => {
                "Responses stream ended before a terminal response event"
            }
            ResponsesErrorKind::Cancelled => "Responses request cancelled",
            ResponsesErrorKind::Producer => "Responses producer rejected publication",
            ResponsesErrorKind::Provider => "Responses provider failure",
            ResponsesErrorKind::Conversion => "Responses conversion failed",
            ResponsesErrorKind::ResourceLimit => "Responses resource limit",
            ResponsesErrorKind::Policy => "Responses service-tier policy failed",
        })
    }
}
impl std::error::Error for ResponsesError {}

/// Incremental decoded wire-event source. Dropping next cancels that local wait.
/// A source must release its own transport resources when dropped by the adapter.
pub trait ResponsesEventSource: Send {
    /// Receives one complete SDK-wire event, end-of-stream, or a redacted error.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Value>, ResponsesError>>;
}

/// Closed service-tier values in OpenAI 6.40.0 Responses create parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResponsesServiceTier {
    /// Automatic tier.
    Auto,
    /// Default tier.
    Default,
    /// Flexible processing.
    Flex,
    /// Scale tier.
    Scale,
    /// Priority processing.
    Priority,
}

/// Trusted borrowed price adjustment, independent of tier resolution.
pub trait ResponsesServiceTierPricing: Send + Sync {
    /// Adjusts detached usage after ordinary model cost calculation.
    fn apply(
        &self,
        usage: &mut Usage,
        tier: Presence<ResponsesServiceTier>,
    ) -> Result<(), ResponsesError>;
}

/// Trusted borrowed tier policy; no serialized/foreign callback crosses this port.
pub trait ResponsesServiceTierPolicy: Send + Sync {
    /// Optional pricing capability. Without it, the resolver is not invoked.
    fn pricing(&self) -> Option<&dyn ResponsesServiceTierPricing> {
        None
    }
    /// Resolves response/request tiers; default is the pinned nullish precedence.
    fn resolve(
        &self,
        response: Presence<ResponsesServiceTier>,
        request: Presence<ResponsesServiceTier>,
    ) -> Presence<ResponsesServiceTier> {
        match response {
            Presence::Value(_) => response,
            _ => request,
        }
    }
}

/// Shared streaming controls, separate from HTTP options and terminal ownership.
#[derive(Default)]
pub struct OpenAIResponsesStreamOptions<'a> {
    /// Requested service tier, preserving omission and explicit null.
    pub service_tier: Presence<ResponsesServiceTier>,
    /// Grammar tool-name to string-property projection.
    pub grammar_tool_input_properties: Option<&'a BTreeMap<String, String>>,
    /// Optional borrowed resolver/pricing capability.
    pub service_tier_policy: Option<&'a dyn ResponsesServiceTierPolicy>,
}

/// Tool wire projection controls. Omitted strict defaults to false; null is retained.
#[derive(Clone, Default)]
pub struct ConvertResponsesToolsOptions {
    /// Explicit strict flag or null; Missing selects false.
    pub strict: Presence<bool>,
    /// Omitted means supported.
    pub supports_strict_mode: Option<bool>,
    /// Omitted means unsupported.
    pub supports_openai_grammar_tools: Option<bool>,
    /// Only explicit true emits defer_loading.
    pub defer_loading: Option<bool>,
}

/// Deferred tool-definition replay shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponsesDeferredToolsMode {
    /// Developer additional_tools item.
    AdditionalTools,
    /// Client tool-search call/output pair.
    ToolSearch,
}

/// Borrowed transcript conversion controls.
#[derive(Default)]
pub struct ConvertResponsesMessagesOptions<'a> {
    /// Omitted means include a nonempty system prompt.
    pub include_system_prompt: Option<bool>,
    /// Grammar tool-name to string-property projection.
    pub grammar_tool_input_properties: Option<&'a BTreeMap<String, String>>,
    /// Active definitions eligible at addedToolNames markers.
    pub deferred_tools: Option<&'a BTreeMap<String, Tool>>,
    /// Deferred replay mechanism.
    pub deferred_tools_mode: Option<ResponsesDeferredToolsMode>,
    /// Options for emitted deferred definitions.
    pub tool_options: Option<&'a ConvertResponsesToolsOptions>,
    /// Injected synthetic tool-result timestamp; omitted selects zero.
    /// Timestamps are not included in Responses wire items.
    pub synthetic_result_timestamp: Option<SafeUnsigned>,
}

/// Converts tools into function/custom wire objects without changing declarations.
pub fn convert_responses_tools(
    tools: &[Tool],
    options: &ConvertResponsesToolsOptions,
) -> Result<Vec<Value>, ResponsesError> {
    let supports_strict = options.supports_strict_mode.unwrap_or(true);
    tools.iter().map(|tool| {
        let grammar = resolve_grammar_constrained_sampling(
            tool, options.supports_openai_grammar_tools.unwrap_or(false),
        ).map_err(helper_error)?;
        let mut wire = if let Some(grammar) = grammar {
            json!({
                "type":"custom", "name":tool.name, "description":tool.description,
                "format":{"type":"grammar","syntax":grammar.format,"definition":grammar.definition}
            })
        } else {
            let constrained = resolve_json_schema_strict_sampling(tool, supports_strict)
                .map_err(helper_error)?;
            let strict = constrained.map(Presence::Value).unwrap_or_else(|| match options.strict {
                Presence::Missing => Presence::Value(false),
                _ => options.strict.clone(),
            });
            let parameters = get_wire_json_schema_tool_parameters(
                tool, Some(matches!(strict, Presence::Value(true))),
            ).map_err(helper_error)?;
            let mut wire = json!({
                "type":"function", "name":tool.name, "description":tool.description,
                "parameters":parameters.as_value()
            });
            if supports_strict {
                wire["strict"] = serde_json::to_value(strict)
                    .map_err(|_| ResponsesError::new(ResponsesErrorKind::Conversion))?;
            }
            wire
        };
        if options.defer_loading == Some(true) {
            wire["defer_loading"] = Value::Bool(true);
        }
        Ok(wire)
    }).collect()
}

fn helper_error(error: ConstrainedSamplingError) -> ResponsesError {
    ResponsesError::new(if error == ConstrainedSamplingError::ResourceLimit {
        ResponsesErrorKind::ResourceLimit
    } else {
        ResponsesErrorKind::Conversion
    })
}

fn normalize_part(part: &str) -> String {
    part.encode_utf16()
        .map(|unit| match unit {
            48..=57 | 65..=90 | 97..=122 | 45 | 95 => {
                char::from_u32(u32::from(unit)).unwrap_or('_')
            }
            _ => '_',
        })
        .take(64)
        .collect::<String>()
        .trim_end_matches('_')
        .to_owned()
}

struct IdNormalizer<'a> {
    allowed: &'a BTreeSet<String>,
    invalid: bool,
}
impl ToolCallIdNormalizer for IdNormalizer<'_> {
    fn normalize_tool_call_id(
        &mut self,
        id: &ToolCallId,
        model: &Model,
        source: &AssistantMessage,
    ) -> ToolCallId {
        let mut parts = id.as_str().split('|');
        let call = parts.next().unwrap_or_default();
        let item = parts.next();
        let normalized = if self.allowed.contains(model.provider.as_str())
            && let Some(item) = item
        {
            let mut item = if source.provider != model.provider || source.api != model.api {
                format!("fc_{}", short_hash(item))
            } else {
                normalize_part(item)
            };
            if !item.starts_with("fc_") {
                item = normalize_part(&format!("fc_{item}"));
            }
            format!("{}|{item}", normalize_part(call))
        } else {
            normalize_part(id.as_str())
        };
        ToolCallId::new(normalized).unwrap_or_else(|_| {
            self.invalid = true;
            id.clone()
        })
    }
}

fn image_wire(image: &pi_ai_core::ImageContent) -> Value {
    json!({"type":"input_image","detail":"auto","image_url":format!("data:{};base64,{}",image.mime_type,image.data)})
}

fn input_wire(block: &InputContent) -> Value {
    match block {
        InputContent::Text(text) => json!({"type":"input_text","text":text.text}),
        InputContent::Image(image) => image_wire(image),
    }
}

fn tool_output(model: &Model, content: &[InputContent]) -> Value {
    let text = content
        .iter()
        .filter_map(|block| match block {
            InputContent::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let images: Vec<_> = content
        .iter()
        .filter_map(|block| match block {
            InputContent::Image(image) => Some(image),
            _ => None,
        })
        .collect();
    if images.is_empty() || !model.input.contains(&ModelInput::Image) {
        return Value::String(if !text.is_empty() {
            text
        } else if images.is_empty() {
            "(no tool output)".to_owned()
        } else {
            "(see attached image)".to_owned()
        });
    }
    let mut result = Vec::new();
    if !text.is_empty() {
        result.push(json!({"type":"input_text","text":text}));
    }
    result.extend(images.into_iter().map(image_wire));
    Value::Array(result)
}

fn text_signature(signature: Option<&str>) -> Option<(String, Option<String>)> {
    let signature = signature.filter(|value| !value.is_empty())?;
    if signature.starts_with('{')
        && let Ok(value) = serde_json::from_str::<Value>(signature)
        && value["v"] == 1
        && let Some(id) = value["id"].as_str()
    {
        let phase = value["phase"]
            .as_str()
            .filter(|phase| matches!(*phase, "commentary" | "final_answer"))
            .map(str::to_owned);
        return Some((id.to_owned(), phase));
    }
    Some((signature.to_owned(), None))
}

/// Converts a detached transcript, using the accepted transform/repair policy first.
pub fn convert_responses_messages(
    model: &Model,
    context: &Context,
    allowed_tool_call_providers: &BTreeSet<String>,
    options: &ConvertResponsesMessagesOptions<'_>,
) -> Result<Vec<Value>, ResponsesError> {
    let mut normalizer = IdNormalizer {
        allowed: allowed_tool_call_providers,
        invalid: false,
    };
    let timestamp = options.synthetic_result_timestamp.unwrap_or(
        SafeUnsigned::new(0).map_err(|_| ResponsesError::new(ResponsesErrorKind::Conversion))?,
    );
    let transformed =
        transform_messages(&context.messages, model, timestamp, Some(&mut normalizer))
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::Conversion))?;
    if normalizer.invalid {
        return Err(ResponsesError::new(ResponsesErrorKind::Conversion));
    }
    let mut messages = Vec::new();
    let mut loaded = BTreeSet::new();
    if options.include_system_prompt.unwrap_or(true)
        && let Some(prompt) = context
            .system_prompt
            .as_deref()
            .filter(|value| !value.is_empty())
    {
        let supports_developer = model
            .compat
            .as_ref()
            .and_then(|compat| compat.0.as_map().get("supportsDeveloperRole"))
            != Some(&JsonValue::Bool(false));
        messages.push(json!({"role":if model.reasoning && supports_developer {"developer"} else {"system"},"content":prompt}));
    }
    let mut message_index = 0;
    for message in transformed {
        match message {
            Message::User(message) => {
                let content: Vec<_> = match &message.content {
                    pi_ai_core::UserContent::Text(text) => {
                        vec![json!({"type":"input_text","text":text})]
                    }
                    pi_ai_core::UserContent::Blocks(blocks) => {
                        blocks.iter().map(input_wire).collect()
                    }
                };
                if content.is_empty() {
                    continue;
                }
                messages.push(json!({"role":"user","content":content}));
            }
            Message::Assistant(message) => {
                let same_provider_api =
                    message.provider == model.provider && message.api == model.api;
                let same_model = same_provider_api && message.model == model.id;
                let different_model = same_provider_api && message.model != model.id;
                let mut converted = Vec::new();
                let mut text_index = 0;
                for block in &message.content {
                    match block {
                        AssistantContent::Thinking(thinking) => {
                            if let Some(signature) = thinking
                                .thinking_signature
                                .as_deref()
                                .filter(|value| !value.is_empty())
                            {
                                converted.push(serde_json::from_str(signature).map_err(|_| {
                                    ResponsesError::new(ResponsesErrorKind::Conversion)
                                })?);
                            }
                        }
                        AssistantContent::Text(text) => {
                            let signature = text_signature(text.text_signature.as_deref());
                            let id = match signature
                                .as_ref()
                                .map(|(id, _)| id)
                                .filter(|id| !id.is_empty())
                            {
                                Some(id) if id.encode_utf16().count() > 64 => {
                                    format!("msg_{}", short_hash(id))
                                }
                                Some(id) => id.clone(),
                                None if text_index == 0 => format!("msg_pi_{message_index}"),
                                None => format!("msg_pi_{message_index}_{text_index}"),
                            };
                            text_index += 1;
                            let mut item = json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text.text,"annotations":[]}],"status":"completed","id":id});
                            if let Some((_, Some(phase))) = signature {
                                item["phase"] = Value::String(phase);
                            }
                            converted.push(item);
                        }
                        AssistantContent::ToolCall(call) => {
                            let mut parts = call.id.as_str().split('|');
                            let call_id = parts.next().unwrap_or_default();
                            let mut item_id = parts.next();
                            let property = options
                                .grammar_tool_input_properties
                                .and_then(|map| map.get(call.name.as_str()));
                            if (different_model && item_id.is_some_and(|id| id.starts_with("fc_")))
                                || (property.is_none()
                                    && !item_id.is_some_and(|id| id.starts_with("fc_")))
                            {
                                item_id = None;
                            }
                            let mut item = if let Some(property) = property {
                                json!({"type":"custom_tool_call","call_id":call_id,"name":call.name,"input":get_grammar_tool_input(call.name.as_str(),&call.arguments,property).map_err(|_|ResponsesError::new(ResponsesErrorKind::Conversion))?})
                            } else {
                                let arguments =
                                    serde_json::to_value(&call.arguments).map_err(|_| {
                                        ResponsesError::new(ResponsesErrorKind::Conversion)
                                    })?;
                                json!({"type":"function_call","call_id":call_id,"name":call.name,"arguments":stringify_wire_json(&arguments).map_err(wire_json_error)?})
                            };
                            if let Some(id) = item_id {
                                item["id"] = Value::String(id.to_owned());
                            }
                            if (same_model
                                || options
                                    .deferred_tools
                                    .is_some_and(|map| map.contains_key(call.name.as_str())))
                                && let Some(namespace) = &call.namespace
                            {
                                item["namespace"] = Value::String(namespace.clone());
                            }
                            converted.push(item);
                        }
                    }
                }
                if converted.is_empty() {
                    continue;
                }
                messages.extend(converted);
            }
            Message::ToolResult(message) => {
                let call_id = message
                    .tool_call_id
                    .as_str()
                    .split('|')
                    .next()
                    .unwrap_or_default();
                let custom = options
                    .grammar_tool_input_properties
                    .is_some_and(|map| map.contains_key(message.tool_name.as_str()));
                messages.push(json!({"type":if custom {"custom_tool_call_output"} else {"function_call_output"},"call_id":call_id,"output":tool_output(model,&message.content)}));
                let mut tools = Vec::new();
                for name in message.added_tool_names.iter().flatten() {
                    if let Some(tool) = options
                        .deferred_tools
                        .and_then(|map| map.get(name.as_str()))
                        && loaded.insert(name.clone())
                    {
                        tools.push(tool.clone());
                    }
                }
                if !tools.is_empty() {
                    let tool_options = options.tool_options.cloned().unwrap_or_default();
                    match options.deferred_tools_mode {
                        Some(ResponsesDeferredToolsMode::AdditionalTools) => messages.push(json!({"type":"additional_tools","role":"developer","tools":convert_responses_tools(&tools,&tool_options)?})),
                        Some(ResponsesDeferredToolsMode::ToolSearch) => {
                            let names = tools.iter().map(|tool|tool.name.as_str()).collect::<Vec<_>>();
                            let id = format!("pi_tool_load_{}",short_hash(&format!("{}:{}",message.tool_call_id.as_str(),names.join(","))));
                            messages.push(json!({"type":"tool_search_call","call_id":id,"execution":"client","status":"completed","arguments":{"query":names.join(" "),"limit":names.len()}}));
                            messages.push(json!({"type":"tool_search_output","call_id":id,"execution":"client","status":"completed","tools":convert_responses_tools(&tools,&ConvertResponsesToolsOptions {defer_loading:Some(true),..tool_options})?}));
                        }
                        None => {}
                    }
                }
            }
        }
        message_index += 1;
    }
    Ok(messages)
}

fn wire_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ResponsesError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ResponsesError::new(ResponsesErrorKind::InvalidWire))
}

fn output_index(event: &Value) -> Result<Option<u64>, ResponsesError> {
    event
        .get("output_index")
        .map(|value| {
            project_wire_safe_unsigned(value)
                .map(SafeUnsigned::get)
                .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))
        })
        .transpose()
}

enum SlotKind {
    Thinking,
    Text,
    Function(String),
    Custom {
        property: String,
        buffer: GrammarToolInputJsonBuffer,
    },
}
struct Slot {
    index: usize,
    kind: SlotKind,
}

struct Processor<'a, 'b> {
    output: &'a mut AssistantMessage,
    producer: &'a AssistantStreamProducer,
    model: &'a Model,
    options: &'a OpenAIResponsesStreamOptions<'b>,
    slots: BTreeMap<Option<u64>, Slot>,
    reasoning: BTreeMap<String, usize>,
    terminal: bool,
    cancellation: &'a CancellationToken,
}

enum Emission {
    Start,
    Delta(String),
    End,
}

impl Processor<'_, '_> {
    fn snapshot(&self) -> Result<(), ResponsesError> {
        if self.cancellation.is_cancelled() {
            return Err(ResponsesError::new(ResponsesErrorKind::Cancelled));
        }
        self.producer
            .publish_cancellation_snapshot(self.output.clone())
            .map_err(|_| {
                ResponsesError::new(if self.cancellation.is_cancelled() {
                    ResponsesErrorKind::Cancelled
                } else {
                    ResponsesErrorKind::Producer
                })
            })
    }

    fn emit(&self, index: usize, emission: Emission) -> Result<(), ResponsesError> {
        self.snapshot()?;
        let content_index = SafeUnsigned::new(index as u64)
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
        let partial = Box::new(self.output.clone());
        let event = match (&self.output.content[index], emission) {
            (AssistantContent::Thinking(_), Emission::Start) => {
                AssistantMessageEvent::ThinkingStart {
                    content_index,
                    partial,
                }
            }
            (AssistantContent::Thinking(_), Emission::Delta(delta)) => {
                AssistantMessageEvent::ThinkingDelta {
                    content_index,
                    delta,
                    partial,
                }
            }
            (AssistantContent::Thinking(block), Emission::End) => {
                AssistantMessageEvent::ThinkingEnd {
                    content_index,
                    content: block.thinking.clone(),
                    partial,
                }
            }
            (AssistantContent::Text(_), Emission::Start) => AssistantMessageEvent::TextStart {
                content_index,
                partial,
            },
            (AssistantContent::Text(_), Emission::Delta(delta)) => {
                AssistantMessageEvent::TextDelta {
                    content_index,
                    delta,
                    partial,
                }
            }
            (AssistantContent::Text(block), Emission::End) => AssistantMessageEvent::TextEnd {
                content_index,
                content: block.text.clone(),
                partial,
            },
            (AssistantContent::ToolCall(_), Emission::Start) => {
                AssistantMessageEvent::ToolCallStart {
                    content_index,
                    partial,
                }
            }
            (AssistantContent::ToolCall(_), Emission::Delta(delta)) => {
                AssistantMessageEvent::ToolCallDelta {
                    content_index,
                    delta,
                    partial,
                }
            }
            (AssistantContent::ToolCall(block), Emission::End) => {
                AssistantMessageEvent::ToolCallEnd {
                    content_index,
                    tool_call: block.clone(),
                    partial,
                }
            }
        };
        self.producer
            .push(event)
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::Producer))
    }

    fn phase(&mut self, item: &Value) {
        if item["type"] == "message" && item["phase"] == "final_answer" {
            self.output.stop_reason = StopReason::Stop;
        }
    }

    fn create_slot(&mut self, key: Option<u64>, item: &Value) -> Result<(), ResponsesError> {
        let kind = wire_string(item, "type")?;
        self.phase(item);
        let (content, kind) = match kind {
            "reasoning" => (
                AssistantContent::Thinking(ThinkingContent {
                    content_type: ThinkingContentType::Thinking,
                    thinking: String::new(),
                    thinking_signature: None,
                    redacted: None,
                }),
                SlotKind::Thinking,
            ),
            "message" => (
                AssistantContent::Text(TextContent {
                    content_type: TextContentType::Text,
                    text: String::new(),
                    text_signature: None,
                }),
                SlotKind::Text,
            ),
            "function_call" | "custom_tool_call" => {
                let name = wire_string(item, "name")?;
                let mut arguments = JsonObject::default();
                let custom = kind == "custom_tool_call";
                let slot_kind = if custom {
                    let property = self
                        .options
                        .grammar_tool_input_properties
                        .and_then(|map| map.get(name))
                        .map(String::as_str)
                        .unwrap_or("input")
                        .to_owned();
                    arguments = JsonObject::new(BTreeMap::from([(
                        property.clone(),
                        JsonValue::String(item["input"].as_str().unwrap_or_default().to_owned()),
                    )]));
                    SlotKind::Custom {
                        property,
                        buffer: GrammarToolInputJsonBuffer::default(),
                    }
                } else {
                    SlotKind::Function(item["arguments"].as_str().unwrap_or_default().to_owned())
                };
                let call = ToolCall {
                    content_type: ToolCallType::ToolCall,
                    id: ToolCallId::new(format!(
                        "{}|{}",
                        wire_string(item, "call_id")?,
                        match item.get("id") {
                            None => "undefined",
                            Some(value) => value
                                .as_str()
                                .ok_or(ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
                        }
                    ))
                    .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
                    name: ToolName::new(name)
                        .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
                    arguments,
                    thought_signature: None,
                    namespace: item
                        .get("namespace")
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or(ResponsesError::new(ResponsesErrorKind::InvalidWire))
                        })
                        .transpose()?,
                };
                (AssistantContent::ToolCall(call), slot_kind)
            }
            _ => return Ok(()),
        };
        let index = self.output.content.len();
        self.output.content.push(content);
        self.slots.insert(key, Slot { index, kind });
        self.emit(index, Emission::Start)
    }

    fn custom_input(
        &mut self,
        key: Option<u64>,
        next: &str,
        close: bool,
    ) -> Result<(), ResponsesError> {
        let Some(slot) = self.slots.get_mut(&key) else {
            return Ok(());
        };
        let SlotKind::Custom { property, buffer } = &mut slot.kind else {
            return Ok(());
        };
        let delta = append_grammar_tool_input_json_delta(buffer, property, next, close)
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
        let index = slot.index;
        if let AssistantContent::ToolCall(call) = &mut self.output.content[index] {
            call.arguments = JsonObject::new(BTreeMap::from([(
                property.clone(),
                JsonValue::String(next.to_owned()),
            )]));
        }
        if let Some(delta) = delta {
            self.emit(index, Emission::Delta(delta))?;
        }
        Ok(())
    }

    fn finish_item(&mut self, key: Option<u64>, item: &Value) -> Result<(), ResponsesError> {
        self.phase(item);
        if !self.slots.contains_key(&key) {
            self.create_slot(key, item)?;
        }
        let Some(slot) = self.slots.get(&key) else {
            return Ok(());
        };
        let index = slot.index;
        match (wire_string(item, "type")?, &slot.kind) {
            ("reasoning", SlotKind::Thinking) => {
                let summary = joined_text(item.get("summary"), "\n\n")?;
                let content = joined_text(item.get("content"), "\n\n")?;
                if let AssistantContent::Thinking(block) = &mut self.output.content[index] {
                    if !summary.is_empty() {
                        block.thinking = summary;
                    } else if !content.is_empty() {
                        block.thinking = content;
                    }
                    block.thinking_signature =
                        Some(stringify_wire_json(item).map_err(wire_json_error)?);
                }
                self.reasoning
                    .insert(wire_string(item, "id")?.to_owned(), index);
            }
            ("message", SlotKind::Text) => {
                let content = joined_text(item.get("content"), "")?;
                let signature = TextSignatureV1 {
                    v: LiteralOne,
                    id: wire_string(item, "id")?.to_owned(),
                    phase: match item["phase"].as_str() {
                        Some("commentary") => Some(TextSignaturePhase::Commentary),
                        Some("final_answer") => Some(TextSignaturePhase::FinalAnswer),
                        _ => None,
                    },
                };
                if let AssistantContent::Text(block) = &mut self.output.content[index] {
                    block.text = content;
                    block.text_signature = Some(
                        serde_json::to_string(&signature)
                            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
                    );
                }
            }
            ("function_call", SlotKind::Function(partial)) => {
                let arguments = item["arguments"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .unwrap_or(if partial.is_empty() { "{}" } else { partial });
                let arguments = parse_arguments(arguments)?;
                if let AssistantContent::ToolCall(block) = &mut self.output.content[index] {
                    block.arguments = arguments;
                }
            }
            ("custom_tool_call", SlotKind::Custom { property, .. }) => {
                let current = match &self.output.content[index] {
                    AssistantContent::ToolCall(call) => match call.arguments.as_map().get(property)
                    {
                        Some(JsonValue::String(value)) => value.as_str(),
                        _ => "",
                    },
                    _ => "",
                };
                let next = item["input"].as_str().unwrap_or(current).to_owned();
                self.custom_input(key, &next, true)?;
            }
            _ => return Ok(()),
        }
        if let AssistantContent::ToolCall(block) = &mut self.output.content[index]
            && let Some(namespace) = item.get("namespace")
        {
            block.namespace = Some(
                namespace
                    .as_str()
                    .ok_or(ResponsesError::new(ResponsesErrorKind::InvalidWire))?
                    .to_owned(),
            );
        }
        self.emit(index, Emission::End)?;
        self.slots.remove(&key);
        Ok(())
    }

    fn finalize(&mut self, response: &Value) -> Result<(), ResponsesError> {
        if !response.is_object() {
            return Err(ResponsesError::new(ResponsesErrorKind::InvalidWire));
        }
        self.terminal = true;
        if let Some(items) = response["output"].as_array() {
            for item in items {
                if item["type"] != "reasoning" {
                    continue;
                }
                let Some(encrypted) = item["encrypted_content"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                else {
                    continue;
                };
                let Some(index) = item["id"].as_str().and_then(|id| self.reasoning.get(id)) else {
                    continue;
                };
                if let AssistantContent::Thinking(block) = &mut self.output.content[*index]
                    && let Some(signature) = &block.thinking_signature
                {
                    let mut stored: Value = serde_json::from_str(signature)
                        .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
                    if !stored["encrypted_content"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty())
                    {
                        stored["encrypted_content"] = Value::String(encrypted.to_owned());
                        block.thinking_signature =
                            Some(stringify_wire_json(&stored).map_err(wire_json_error)?);
                    }
                }
            }
        }
        if let Some(id) = response["id"].as_str().filter(|id| !id.is_empty()) {
            self.output.response_id = Some(
                ResponseId::new(id)
                    .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
            );
        }
        let mut usage = self.output.usage.clone();
        if response.get("usage").is_some_and(|value| !value.is_null()) {
            let raw = &response["usage"];
            let cached = wire_count(&raw["input_tokens_details"]["cached_tokens"])?;
            let written = wire_count(&raw["input_tokens_details"]["cache_write_tokens"])?;
            let input = wire_count(&raw["input_tokens"])?;
            usage = Usage::zero();
            usage.input = SafeUnsigned::new(
                input
                    .get()
                    .saturating_sub(cached.get())
                    .saturating_sub(written.get()),
            )
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
            usage.cache_read = cached;
            usage.cache_write = written;
            usage.output = wire_count(&raw["output_tokens"])?;
            usage.reasoning = Some(wire_count(
                &raw["output_tokens_details"]["reasoning_tokens"],
            )?);
            usage.total_tokens = wire_count(&raw["total_tokens"])?;
        }
        calculate_cost(self.model, &mut usage)
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
        self.output.usage = usage;
        // Upstream stores usage before calling the trusted pricing hook. Publish
        // it before that hook can fail or synchronously cancel the request.
        self.snapshot()?;
        if let Some(policy) = self.options.service_tier_policy
            && let Some(pricing) = policy.pricing()
        {
            let tier = match response.get("service_tier") {
                None => Presence::Missing,
                Some(Value::Null) => Presence::Null,
                Some(value) => Presence::Value(
                    serde_json::from_value(value.clone())
                        .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
                ),
            };
            let applied = pricing.apply(
                &mut self.output.usage,
                policy.resolve(tier, self.options.service_tier.clone()),
            );
            self.snapshot()?;
            applied?;
        }
        // Source finalization stores accounting and invokes pricing before it
        // rejects unknown stop status. Keep those safe observations, but do not
        // publish an unvalidated status or reason identifier.
        let status = response_status(response)?;
        let reason = response["incomplete_details"]["reason"]
            .as_str()
            .filter(|value| !value.is_empty());
        if let Some(reason) = reason {
            validate_reason_identifier(reason)?;
        }
        self.output.raw_stop_reason = if status.is_empty() {
            None
        } else {
            Some(
                RawStopReason::new(if let Some(reason) = reason {
                    format!("{status}.{reason}")
                } else {
                    status.to_owned()
                })
                .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
            )
        };
        self.output.error_message = None;
        self.output.stop_reason = match status {
            "" | "completed" | "in_progress" | "queued" => StopReason::Stop,
            "incomplete" if reason == Some("max_output_tokens") => StopReason::Length,
            "incomplete" => {
                self.output.error_message = Some(match reason {
                    Some(reason) => format!("Response incomplete: {reason}"),
                    None => "Response incomplete without a provider reason".to_owned(),
                });
                StopReason::Error
            }
            "failed" | "cancelled" => {
                self.output.error_message = Some("Responses provider failure".to_owned());
                StopReason::Error
            }
            _ => return Err(ResponsesError::new(ResponsesErrorKind::InvalidWire)),
        };
        if self.output.stop_reason == StopReason::Stop
            && self
                .output
                .content
                .iter()
                .any(|block| matches!(block, AssistantContent::ToolCall(_)))
        {
            self.output.stop_reason = StopReason::ToolUse;
        }
        self.snapshot()
    }

    fn event(&mut self, event: Value) -> Result<(), ResponsesError> {
        let kind = wire_string(&event, "type")?;
        match kind {
            "response.created" => {
                self.output.response_id = Some(
                    ResponseId::new(wire_string(&event["response"], "id")?)
                        .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))?,
                );
            }
            "response.output_item.added" => {
                self.create_slot(output_index(&event)?, &event["item"])?
            }
            "response.output_item.done" => {
                self.finish_item(output_index(&event)?, &event["item"])?
            }
            "response.completed" | "response.incomplete" => self.finalize(&event["response"])?,
            "error" | "response.failed" => {
                let raw = if kind == "error" {
                    format!(
                        "Error Code {}: {}",
                        event["code"].as_str().unwrap_or("unknown"),
                        event["message"].as_str().unwrap_or_default()
                    )
                } else if event["response"]["error"].is_object() {
                    format!(
                        "{}: {}",
                        event["response"]["error"]["code"]
                            .as_str()
                            .unwrap_or("unknown"),
                        event["response"]["error"]["message"]
                            .as_str()
                            .unwrap_or_default()
                    )
                } else if let Some(reason) = event["response"]["incomplete_details"]["reason"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                {
                    format!("incomplete: {reason}")
                } else {
                    "Unknown error (no error details in response)".to_owned()
                };
                if kind == "response.failed" {
                    self.terminal = true;
                    let status = response_status(&event["response"])?;
                    self.output.raw_stop_reason =
                        if status.is_empty() {
                            None
                        } else {
                            Some(RawStopReason::new(status).map_err(|_| {
                                ResponsesError::new(ResponsesErrorKind::InvalidWire)
                            })?)
                        };
                }
                let mut classified = self.output.clone();
                classified.stop_reason = StopReason::Error;
                classified.error_message = Some(raw);
                let error = ResponsesError {
                    kind: ResponsesErrorKind::Provider,
                    retryable: is_retryable_assistant_error(&classified),
                    context_overflow: is_context_overflow(&classified, None),
                };
                self.snapshot()?;
                return Err(error);
            }
            "response.reasoning_summary_text.delta"
            | "response.reasoning_text.delta"
            | "response.reasoning_summary_part.done"
            | "response.output_text.delta"
            | "response.refusal.delta" => {
                let key = output_index(&event)?;
                let Some(slot) = self.slots.get(&key) else {
                    return Ok(());
                };
                let thinking = matches!(
                    kind,
                    "response.reasoning_summary_text.delta"
                        | "response.reasoning_text.delta"
                        | "response.reasoning_summary_part.done"
                );
                if !matches!(
                    (&slot.kind, thinking),
                    (SlotKind::Thinking, true) | (SlotKind::Text, false)
                ) {
                    return Ok(());
                }
                let index = slot.index;
                let delta = if kind == "response.reasoning_summary_part.done" {
                    "\n\n"
                } else {
                    wire_string(&event, "delta")?
                };
                match &mut self.output.content[index] {
                    AssistantContent::Thinking(block) => block.thinking.push_str(delta),
                    AssistantContent::Text(block) => block.text.push_str(delta),
                    _ => return Err(ResponsesError::new(ResponsesErrorKind::InvalidWire)),
                }
                self.emit(index, Emission::Delta(delta.to_owned()))?;
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                let key = output_index(&event)?;
                let Some(slot) = self.slots.get_mut(&key) else {
                    return Ok(());
                };
                let SlotKind::Function(partial) = &mut slot.kind else {
                    return Ok(());
                };
                let next = if kind.ends_with(".delta") {
                    format!("{}{}", partial, wire_string(&event, "delta")?)
                } else {
                    wire_string(&event, "arguments")?.to_owned()
                };
                let arguments = parse_arguments(&next)?;
                let delta = if kind.ends_with(".delta") {
                    Some(wire_string(&event, "delta")?.to_owned())
                } else {
                    next.strip_prefix(partial.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned)
                };
                *partial = next;
                let index = slot.index;
                if let AssistantContent::ToolCall(block) = &mut self.output.content[index] {
                    block.arguments = arguments;
                }
                if let Some(delta) = delta {
                    self.emit(index, Emission::Delta(delta))?;
                }
            }
            "response.custom_tool_call_input.delta" | "response.custom_tool_call_input.done" => {
                let key = output_index(&event)?;
                let Some(slot) = self.slots.get(&key) else {
                    return Ok(());
                };
                let SlotKind::Custom { property, .. } = &slot.kind else {
                    return Ok(());
                };
                let next = if kind.ends_with(".delta") {
                    let old = match &self.output.content[slot.index] {
                        AssistantContent::ToolCall(call) => {
                            match call.arguments.as_map().get(property) {
                                Some(JsonValue::String(value)) => value.as_str(),
                                _ => "",
                            }
                        }
                        _ => "",
                    };
                    format!("{}{}", old, wire_string(&event, "delta")?)
                } else {
                    wire_string(&event, "input")?.to_owned()
                };
                self.custom_input(key, &next, kind.ends_with(".done"))?;
            }
            _ => {}
        }
        self.snapshot()
    }
}

fn joined_text(content: Option<&Value>, separator: &str) -> Result<String, ResponsesError> {
    let Some(content) = content.filter(|value| !value.is_null()) else {
        return Ok(String::new());
    };
    let array = content
        .as_array()
        .ok_or(ResponsesError::new(ResponsesErrorKind::InvalidWire))?;
    Ok(array
        .iter()
        .map(|part| {
            part["text"]
                .as_str()
                .or_else(|| part["refusal"].as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(separator))
}

fn parse_arguments(input: &str) -> Result<JsonObject, ResponsesError> {
    if input.len() > 256 * 1024 {
        return Err(ResponsesError::new(ResponsesErrorKind::ResourceLimit));
    }
    match parse_streaming_json(Some(input)) {
        JsonValue::Object(object) => Ok(object),
        _ => Err(ResponsesError::new(ResponsesErrorKind::InvalidWire)),
    }
}

fn wire_count(value: &Value) -> Result<SafeUnsigned, ResponsesError> {
    if value.is_null() {
        SafeUnsigned::new(0).map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))
    } else {
        project_wire_safe_unsigned(value)
            .map_err(|_| ResponsesError::new(ResponsesErrorKind::InvalidWire))
    }
}

fn wire_json_error(error: WireJsonError) -> ResponsesError {
    ResponsesError::new(match error {
        WireJsonError::DepthLimit | WireJsonError::ByteLimit => ResponsesErrorKind::ResourceLimit,
        WireJsonError::InvalidNumber => ResponsesErrorKind::InvalidWire,
    })
}

/// Reduces one event at a time, publishing owned cancellation snapshots before waits.
/// Completion updates output but emits no terminal. Partial output survives failure.
pub async fn process_responses_stream(
    source: &mut dyn ResponsesEventSource,
    output: &mut AssistantMessage,
    producer: &AssistantStreamProducer,
    model: &Model,
    options: &OpenAIResponsesStreamOptions<'_>,
    cancellation: &CancellationToken,
) -> Result<(), ResponsesError> {
    let mut processor = Processor {
        output,
        producer,
        model,
        options,
        slots: BTreeMap::new(),
        reasoning: BTreeMap::new(),
        terminal: false,
        cancellation,
    };
    loop {
        processor.snapshot()?;
        let mut next = source.next();
        let mut cancelled = Box::pin(cancellation.cancelled());
        let event = poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(ResponsesError::new(ResponsesErrorKind::Cancelled)));
            }
            next.as_mut().poll(cx)
        })
        .await?;
        if cancellation.is_cancelled() {
            return Err(ResponsesError::new(ResponsesErrorKind::Cancelled));
        }
        let Some(event) = event else {
            break;
        };
        validate_wire(&event)?;
        processor.event(event)?;
    }
    if !processor.terminal {
        return Err(ResponsesError::new(ResponsesErrorKind::MissingTerminal));
    }
    Ok(())
}

fn response_status(response: &Value) -> Result<&str, ResponsesError> {
    match response.get("status") {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(value))
            if matches!(
                value.as_str(),
                "" | "completed" | "incomplete" | "failed" | "cancelled" | "in_progress" | "queued"
            ) =>
        {
            Ok(value)
        }
        _ => Err(ResponsesError::new(ResponsesErrorKind::InvalidWire)),
    }
}

fn validate_reason_identifier(value: &str) -> Result<(), ResponsesError> {
    if value.len() > 256
        || !value
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
    {
        Err(ResponsesError::new(ResponsesErrorKind::InvalidWire))
    } else {
        Ok(())
    }
}

fn validate_wire(value: &Value) -> Result<(), ResponsesError> {
    fn depth(value: &Value, level: usize) -> Result<(), ResponsesError> {
        if level > 128 {
            return Err(ResponsesError::new(ResponsesErrorKind::ResourceLimit));
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    depth(value, level + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    depth(value, level + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    struct Budget(usize);
    impl io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| io::Error::other("wire limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    depth(value, 0)?;
    serde_json::to_writer(Budget(1024 * 1024), value)
        .map_err(|_| ResponsesError::new(ResponsesErrorKind::ResourceLimit))
}
