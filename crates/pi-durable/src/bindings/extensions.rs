//! Authenticated durable.v1 bindings. Every callback retains weak runtime and
//! attachment custody. The peer has no host document/task/model action surface.
use crate::*;
use base64::Engine;
use pi_ai::{JsonObject, Message, UserContent};
use pi_extension_protocol::{
    host::{self, HostConnection},
    profiles::{NativeError, Registry, Stage, ValidatedPayload},
    wire::v1_1::CapabilityRef,
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

pub const HOOK_METHODS: [&str; 7] = [
    "before_request",
    "after_response",
    "on_yield",
    "after_tools",
    "before_tool",
    "after_tool",
    "before_compact",
];

/// Host-selected tool metadata. This is a native registration, not peer data.
/// The attachment replaces execute with its authenticated remote binding.
#[derive(Clone)]
pub enum ExtensionRegistration {
    Hook(CapabilityRef),
    Tool {
        capability: CapabilityRef,
        tool: Box<ToolRegistration>,
    },
}
/// An opaque, process-local grant scope. Numeric conversation IDs alone do not
/// identify a runtime/store. No runtime lease or credential crosses the wire.
pub fn grant_scope(runtime: &Durable, conversation: ConversationId) -> String {
    format!(
        "durable:{}:conversation:{}",
        runtime.binding_id(),
        conversation
    )
}
struct State {
    connection: HostConnection,
    runtime: WeakDurable,
    conversation: ConversationId,
    generation: u64,
    live: AtomicBool,
    retired: pi_ai::CancellationToken,
}
/// Installs one uniquely named native extension in the existing runtime registry.
/// Last public handle drop retires callbacks and its connection. Closures retain
/// only `Weak<State>`; an in-flight request cannot postpone public retirement.
pub struct DurableExtensionAttachment {
    state: Arc<State>,
    name: String,
}
impl DurableExtensionAttachment {
    pub async fn attach(
        runtime: &Durable,
        conversation: ConversationId,
        connection: HostConnection,
        registrations: Vec<ExtensionRegistration>,
    ) -> Result<Self> {
        runtime.check_open()?;
        if runtime.conversation(conversation).await?.is_none() {
            return Err(Error::request("extension conversation does not exist"));
        }
        if !connection.current() {
            return Err(Error::request("extension connection is closed"));
        }
        let generation = connection.grant().wire().connection_generation;
        let scope = grant_scope(runtime, conversation);
        let mut seen = BTreeSet::new();
        for registration in &registrations {
            let (cap, execution) = match registration {
                ExtensionRegistration::Hook(c) => (c, 1),
                ExtensionRegistration::Tool { capability, .. } => (capability, 2),
            };
            if !seen.insert((cap.name.clone(), cap.handler_id.clone())) {
                return Err(Error::request("duplicate Durable extension registration"));
            }
            let approved = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference() == cap)
                .ok_or_else(|| Error::request("missing authenticated Durable grant"))?
                .approved();
            if approved.profile() != "durable.v1"
                || approved.kind() != "durable"
                || approved.scope() != scope
                || approved.execution() != execution
            {
                return Err(Error::request(
                    "Durable extension grant scope/profile/execution mismatch",
                ));
            }
            match registration {
                ExtensionRegistration::Hook(c) if !HOOK_METHODS.contains(&c.name.as_str()) => {
                    return Err(Error::request("unsupported Durable extension hook"));
                }
                ExtensionRegistration::Tool { capability, tool }
                    if capability.name != format!("tool:{}", tool.name())
                        || approved.replay()
                            != if tool.replay == ReplayPolicy::Safe {
                                1
                            } else {
                                2
                            } =>
                {
                    return Err(Error::request(
                        "Durable tool declaration/replay grant mismatch",
                    ));
                }
                _ => (),
            }
        }
        let state = Arc::new(State {
            connection,
            runtime: runtime.downgrade(),
            conversation,
            generation,
            live: AtomicBool::new(true),
            retired: pi_ai::CancellationToken::new(),
        });
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let name = format!(
            "durable-native:{}:{}:{}",
            runtime.binding_id(),
            generation,
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut extension = Extension {
            name: name.clone(),
            ..Default::default()
        };
        for registration in registrations {
            match registration {
                ExtensionRegistration::Hook(cap) => {
                    install_hook(&mut extension.hooks, Arc::downgrade(&state), cap)?
                }
                ExtensionRegistration::Tool {
                    capability,
                    mut tool,
                } => {
                    let weak = Arc::downgrade(&state);
                    tool.execute = Arc::new(move |invocation| {
                        let weak = weak.clone();
                        let cap = capability.clone();
                        Box::pin(
                            async move { attachment_state(&weak)?.tool(cap, invocation).await },
                        )
                    });
                    extension.tools.push(*tool);
                }
            }
        }
        runtime.registry().install(extension)?;
        Ok(Self { state, name })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn close(&self) {
        if self.state.live.swap(false, Ordering::AcqRel) {
            self.state.retired.cancel();
            self.state.connection.close();
            if let Ok(runtime) = self.state.runtime.upgrade() {
                let _ = runtime.registry().uninstall(&self.name);
            }
        }
    }
}
impl Drop for DurableExtensionAttachment {
    fn drop(&mut self) {
        self.close();
    }
}
fn attachment_state(weak: &Weak<State>) -> Result<Arc<State>> {
    let state = weak
        .upgrade()
        .ok_or_else(|| Error::request("Durable extension attachment retired"))?;
    state.check()?;
    Ok(state)
}
impl State {
    fn check(&self) -> Result<()> {
        self.runtime.upgrade()?.check_open()?;
        if !self.live.load(Ordering::Acquire)
            || !self.connection.current()
            || self.connection.grant().wire().connection_generation != self.generation
        {
            return Err(Error::request("Durable extension binding is stale"));
        }
        Ok(())
    }
    fn check_hook(&self, api: &HookApi) -> Result<()> {
        self.check()?;
        api.check_live()?;
        if api.binding_id() != self.runtime.binding_id()
            || api.conversation_id() != self.conversation
        {
            return Err(Error::request("foreign Durable hook invocation"));
        }
        Ok(())
    }
    fn check_tool(&self, api: &ToolExecutionApi) -> Result<()> {
        self.check()?;
        api.check_live()?;
        if api.binding_id() != self.runtime.binding_id()
            || api.identity().conversation_id != self.conversation
        {
            return Err(Error::request("foreign Durable tool invocation"));
        }
        Ok(())
    }
    async fn hook(
        self: Arc<Self>,
        cap: CapabilityRef,
        api: HookApi,
        mut input: Value,
    ) -> Result<Value> {
        self.check_hook(&api)?;
        input["context"] = json!({"conversationId":api.conversation_id(),"taskId":api.task_id()});
        let expected = input.clone();
        let method = cap.name.clone();
        let check = self.clone();
        let check_api = api.clone();
        let validator: host::Validator = Arc::new(move |p| {
            check
                .check_hook(&check_api)
                .map_err(|_| NativeError::Rejected)?;
            if p.stage == Stage::Request {
                if p.fields != expected {
                    return Err(NativeError::Rejected);
                }
                return Ok(());
            }
            validate_terminal(p)
        });
        let registry = Registry::durable().map_err(protocol)?;
        let payload =
            host::bound(&registry, &format!("durable.input:{method}"), &input).map_err(protocol)?;
        let signal = api.signal();
        let retired = self.retired.clone();
        let result = self
            .connection
            .request(
                cap,
                method,
                payload,
                validator,
                Box::pin(async move {
                    tokio::select! { _=signal.cancelled()=>(), _=retired.cancelled()=>() }
                }),
                |_| Err(host::Error::Protocol),
            )
            .await
            .map_err(protocol)?;
        self.check_hook(&api)?;
        Ok(result)
    }
    async fn tool(
        self: Arc<Self>,
        cap: CapabilityRef,
        invocation: ToolInvocation,
    ) -> Result<ToolExecutionResult> {
        self.check_tool(&invocation.api)?;
        let api = invocation.api;
        let options = progress_options(api.progress_options())?;
        let identity = api.identity();
        let input = json!({"context":{"conversationId":identity.conversation_id,"taskId":identity.invocation.task_id},"call":invocation.call,"progress":options});
        let expected = input.clone();
        let validate = self.clone();
        let validate_api = api.clone();
        let enabled = options["enabled"] == true;
        let byte_limit = self
            .connection
            .grant()
            .wire()
            .limits
            .as_ref()
            .ok_or_else(|| Error::validation("missing extension limits"))?
            .aggregate_decoded_bytes as usize;
        let validator: host::Validator = Arc::new(move |p| {
            validate
                .check_tool(&validate_api)
                .map_err(|_| NativeError::Rejected)?;
            match p.stage {
                Stage::Request => {
                    if p.fields == expected {
                        Ok(())
                    } else {
                        Err(NativeError::Rejected)
                    }
                }
                Stage::Progress => {
                    if !enabled {
                        return Err(NativeError::Rejected);
                    }
                    decode_progress(p.fields.clone(), byte_limit)
                        .map(|_| ())
                        .map_err(|_| NativeError::Rejected)
                }
                Stage::Terminal => validate_terminal(p),
            }
        });
        let registry = Registry::durable().map_err(protocol)?;
        let payload =
            host::bound(&registry, "durable.input:tool.call", &input).map_err(protocol)?;
        let signal = api.signal();
        let retired = self.retired.clone();
        let update_api = api.clone();
        let update_state = self.clone();
        let failure = Arc::new(Mutex::new(None));
        let fault = failure.clone();
        let result = self
            .connection
            .request_with_progress_ack(
                cap,
                "tool.call".into(),
                payload,
                validator,
                Box::pin(async move {
                    tokio::select! { _=signal.cancelled()=>(), _=retired.cancelled()=>() }
                }),
                move |value| {
                    let api = update_api.clone();
                    let state = update_state.clone();
                    let fault = fault.clone();
                    async move {
                        let result = async {
                            state.check_tool(&api)?;
                            api.report_progress(decode_progress(value, byte_limit)?)
                                .await
                        }
                        .await;
                        result.map_err(|error| {
                            *fault.lock().unwrap() = Some(error);
                            host::Error::HostValidation
                        })
                    }
                },
            )
            .await;
        if let Some(error) = failure.lock().unwrap().take() {
            return Err(error);
        }
        let result = result.map_err(protocol)?;
        self.check_tool(&api)?;
        Ok(decode::<ResultWire>(result)?.result)
    }
}
fn protocol(error: impl std::fmt::Display) -> Error {
    Error::request(format!("Durable extension: {error}"))
}
fn decode<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(Error::from)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Unit {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagesWire {
    messages: Option<Vec<Message>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct YieldWire {
    r#continue: Option<UserContent>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BeforeWire {
    arguments: Option<JsonObject>,
    block: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AfterWire {
    result: Option<ToolExecutionResult>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultWire {
    result: ToolExecutionResult,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactWire {
    decision: Option<DecisionWire>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum DecisionWire {
    Decline,
    Summary { summary: String },
}
fn validate_terminal(payload: &ValidatedPayload) -> std::result::Result<(), NativeError> {
    if payload.stage != Stage::Terminal {
        return Err(NativeError::Rejected);
    }
    let result = match payload.method.as_str() {
        "before_request" => decode::<MessagesWire>(payload.fields.clone()).map(|_| ()),
        "after_response" | "after_tools" => decode::<Unit>(payload.fields.clone()).map(|_| ()),
        "on_yield" => decode::<YieldWire>(payload.fields.clone()).map(|_| ()),
        "before_tool" => decode::<BeforeWire>(payload.fields.clone()).map(|_| ()),
        "after_tool" => decode::<AfterWire>(payload.fields.clone()).map(|_| ()),
        "before_compact" => decode::<CompactWire>(payload.fields.clone()).map(|_| ()),
        "tool.call" => decode::<ResultWire>(payload.fields.clone()).map(|_| ()),
        _ => return Err(NativeError::Rejected),
    };
    result.map_err(|_| NativeError::Rejected)
}
fn install_hook(hooks: &mut Hooks, weak: Weak<State>, cap: CapabilityRef) -> Result<()> {
    match cap.name.as_str() {
        "before_request" => {
            hooks.before_request = Some(Arc::new(move |messages, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    let r = attachment_state(&weak)?
                        .hook(cap, api, json!({"messages":messages}))
                        .await?;
                    Ok(decode::<MessagesWire>(r)?.messages)
                })
            }))
        }
        "after_response" => {
            hooks.after_response = Some(Arc::new(move |message, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    attachment_state(&weak)?
                        .hook(cap, api, json!({"message":message}))
                        .await?;
                    Ok(())
                })
            }))
        }
        "on_yield" => {
            hooks.on_yield = Some(Arc::new(move |answer, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    let r = attachment_state(&weak)?
                        .hook(cap, api, json!({"answer":answer}))
                        .await?;
                    Ok(decode::<YieldWire>(r)?.r#continue)
                })
            }))
        }
        "after_tools" => {
            hooks.after_tools = Some(Arc::new(move |assistant, results, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    attachment_state(&weak)?
                        .hook(cap, api, json!({"assistant":assistant,"results":results}))
                        .await?;
                    Ok(())
                })
            }))
        }
        "before_tool" => {
            hooks.before_tool = Some(Arc::new(move |call, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    let r = decode::<BeforeWire>(
                        attachment_state(&weak)?
                            .hook(cap, api, json!({"call":call}))
                            .await?,
                    )?;
                    Ok(
                        (r.arguments.is_some() || r.block.is_some()).then_some(BeforeToolResult {
                            arguments: r.arguments,
                            block: r.block,
                        }),
                    )
                })
            }))
        }
        "after_tool" => {
            hooks.after_tool = Some(Arc::new(move |call, result, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    let r = attachment_state(&weak)?
                        .hook(cap, api, json!({"call":call,"result":result}))
                        .await?;
                    Ok(decode::<AfterWire>(r)?.result)
                })
            }))
        }
        "before_compact" => {
            hooks.before_compact = Some(Arc::new(move |c, api| {
                let weak = weak.clone();
                let cap = cap.clone();
                Box::pin(async move {
                    let mut input = json!({"reason":c.reason,"entries":c.entries,"messages":c.messages,"firstKept":c.first_kept});
                    if let Some(instructions) = c.instructions {
                        input["instructions"] = json!(instructions)
                    }
                    let r = attachment_state(&weak)?.hook(cap, api, input).await?;
                    Ok(decode::<CompactWire>(r)?.decision.map(|d| match d {
                        DecisionWire::Decline => CompactDecision::Decline,
                        DecisionWire::Summary { summary } => CompactDecision::Summary(summary),
                    }))
                })
            }))
        }
        _ => return Err(Error::request("unknown Durable hook")),
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum ChunkWire {
    Text { text: String },
    Bytes { base64: String },
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum ProgressWire {
    Output {
        chunk: ChunkWire,
        skipped: Option<OutputSkip>,
    },
    Details {
        value: pi_ai::JsonValue,
    },
    Diagnostic {
        diagnostic: ToolDiagnostic,
    },
}
fn decode_progress(value: Value, limit: usize) -> Result<ToolProgress> {
    Ok(match decode::<ProgressWire>(value)? {
        ProgressWire::Output { chunk, skipped } => {
            let chunk = match chunk {
                ChunkWire::Text { text } => OutputChunk::Text(text),
                ChunkWire::Bytes { base64 } => {
                    if base64.len().saturating_add(3) / 4 * 3 > limit.saturating_add(2) {
                        return Err(Error::validation("decoded progress exceeds byte limit"));
                    }
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(&base64)
                        .map_err(|_| Error::validation("invalid progress base64"))?;
                    if bytes.len() > limit
                        || base64::engine::general_purpose::STANDARD.encode(&bytes) != base64
                    {
                        return Err(Error::validation(
                            "noncanonical or oversized progress base64",
                        ));
                    }
                    OutputChunk::Bytes(bytes)
                }
            };
            Ok::<_, Error>(ToolProgress::Output { chunk, skipped })?
        }
        ProgressWire::Details { value } => ToolProgress::Details { value },
        ProgressWire::Diagnostic { diagnostic } => ToolProgress::Diagnostic { diagnostic },
    })
}
fn count(value: usize) -> Result<Value> {
    let value =
        u64::try_from(value).map_err(|_| Error::validation("progress count out of range"))?;
    if value > 9_007_199_254_740_991 {
        return Err(Error::validation("progress count is not a safe integer"));
    }
    Ok(json!(value))
}
fn progress_options(options: ToolProgressOptions) -> Result<Value> {
    let limits = options.output_limits;
    let mut out = json!({"enabled":options.enabled,"outputLimits":{"maxBytes":count(limits.max_bytes)?,"maxLines":count(limits.max_lines)?,"retain":limits.retain}});
    if let Some(w) = options.output_window {
        if !w.min_interval_ms.is_finite() || w.min_interval_ms < 0.0 {
            return Err(Error::validation(
                "outputWindow.minIntervalMs must be finite and nonnegative",
            ));
        }
        out["outputWindow"] = json!({"maxBytes":count(w.max_bytes)?,"maxLines":count(w.max_lines)?,"minIntervalMs":w.min_interval_ms,"bytesPerSecond":count(w.bytes_per_second)?});
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn window_preserves_fractional_interval_and_rejects_invalid_host_numbers() {
        for interval in [0.0, 0.125, 1.5] {
            let value = progress_options(ToolProgressOptions {
                enabled: true,
                output_limits: OutputLimits {
                    retain: OutputRetention::Tail,
                    ..Default::default()
                },
                output_window: Some(OutputWindow {
                    max_bytes: 100,
                    max_lines: 2,
                    min_interval_ms: interval,
                    bytes_per_second: 1000,
                }),
            })
            .unwrap();
            assert_eq!(value["outputWindow"]["minIntervalMs"], json!(interval));
        }
        for interval in [-0.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                progress_options(ToolProgressOptions {
                    enabled: true,
                    output_limits: Default::default(),
                    output_window: Some(OutputWindow {
                        max_bytes: 100,
                        max_lines: 2,
                        min_interval_ms: interval,
                        bytes_per_second: 1000
                    })
                })
                .is_err()
            );
        }
    }
    #[test]
    fn terminal_and_progress_native_codecs_preserve_null_and_closed_fields() {
        let value =
            decode::<ResultWire>(json!({"result":{"details":null,"structuredOutput":null}}))
                .unwrap()
                .result;
        assert!(matches!(value.details, pi_ai::Presence::Null));
        assert!(matches!(value.structured_output, pi_ai::Presence::Null));
        assert!(decode::<ResultWire>(json!({"result":{"durationMs":1}})).is_err());
        assert!(decode_progress(json!({"type":"details","value":null}), 10).is_ok());
        assert!(
            decode_progress(
                json!({"type":"output","chunk":{"type":"bytes","base64":"YQ"}}),
                10
            )
            .is_err()
        );
        assert!(
            decode_progress(
                json!({"type":"output","chunk":{"type":"bytes","base64":"YQ=="}}),
                0
            )
            .is_err()
        );
    }
}
