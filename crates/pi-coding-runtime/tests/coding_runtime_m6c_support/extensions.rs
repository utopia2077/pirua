//! Authenticated deterministic peer fixture derived from the accepted M5 tests.
use super::*;
use pi_agent::schema::{
    AgentHarnessTool, AgentToolResult, Cancellation, ErrorContext, ToolExecutionError,
    ToolProgressSink,
};
use pi_ai::{JsonValue, ToolCallId};
use pi_coding_runtime::{CodingExtensionAdapter, ExtensionRegistration, ExtensionRegistrationKind};
use pi_extension_protocol::profiles::wire_v1_1 as dto;
use pi_extension_protocol::{
    activation::{AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs},
    profiles::{ApprovedProfile, Exposure, ProfileApproval},
    wire::{framing, v1_1},
};
use pi_harness_runtime::extensions::{
    profiles::connection::ProfileConnection,
    tool_driver::{EchoRouteTiming, OwnerLimits, SingleToolOwner},
    transport::{
        ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig, RetainedOrderedByteChannel,
    },
};
use prost::Message as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicU64},
    },
    task::{Poll, Waker},
};
struct EchoTool;

impl EchoTool {
    fn descriptor() -> &'static pi_agent::schema::AgentTool {
        static DESCRIPTOR: OnceLock<pi_agent::schema::AgentTool> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| {
            serde_json::from_value(echo_catalog()["entries"][0]["agentTool"].clone())
                .expect("echo descriptor")
        })
    }

    fn error(message: &'static str) -> ToolExecutionError {
        ToolExecutionError::new(ErrorContext::new(message).expect("bounded tool error"))
    }
}

impl AgentHarnessTool<pi_coding_runtime::ExecutionToolContext> for EchoTool {
    fn descriptor(&self) -> &pi_agent::schema::AgentTool {
        Self::descriptor()
    }

    fn prepare_arguments(&self, args: JsonValue) -> Result<pi_ai::JsonObject, ToolExecutionError> {
        match args {
            JsonValue::Object(object) => Ok(object),
            _ => Err(Self::error("echo arguments must be an object")),
        }
    }

    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        _: pi_ai::JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a pi_coding_runtime::ExecutionToolContext,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async {
            Err(Self::error(
                "echo is remote-only in the assembled extension witness",
            ))
        })
    }
}

fn profile(kind: &str, name: &str) -> ApprovedProfile {
    // Real provider history includes the earlier echo call and result. The
    // authenticated host must explicitly approve these dynamic JSON leaves.
    let leaves = if kind == "provider" {
        let arguments =
            pi_extension_protocol::jcs::admit_schema(
                &serde_json::to_vec(
                    &echo_catalog()["entries"][0]["selected_minor_schema_sets"]["1.0"]["arguments"]
                        ["schema"],
                )
                .unwrap(),
                Default::default(),
            )
            .unwrap();
        let details = pi_extension_protocol::jcs::admit_schema(
            br#"{"type":"object","properties":{},"additionalProperties":false}"#,
            Default::default(),
        )
        .unwrap();
        BTreeMap::from([
            (
                "provider.context.v1.messages[].content[].arguments".into(),
                arguments,
            ),
            ("provider.context.v1.messages[].details".into(), details),
        ])
    } else {
        BTreeMap::new()
    };
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: kind.into(),
        name: name.into(),
        profile: format!("{kind}.v1"),
        handler: format!("{kind}-{name}-handler"),
        minors: BTreeSet::from([0]),
        replay: if kind == "provider" { 2 } else { 1 },
        execution: if kind == "provider" { 2 } else { 1 },
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(leaves, BTreeSet::new()).expect("exposure"),
    })
    .expect("approved profile")
}

struct Mono(AtomicU64);
impl MonotonicClock for Mono {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, deadline_ms: u64) -> pi_ai::BoxFuture<'_, ()> {
        Box::pin(std::future::poll_fn(move |_| {
            if self.now_ms() >= deadline_ms {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    }
}

struct Peer {
    input: Mutex<VecDeque<u8>>,
    writes: Mutex<Vec<Vec<u8>>>,
    closed: AtomicBool,
    frame_bound: usize,
    tool_invocations: AtomicU64,
    progress_items: AtomicU64,
    terminal_items: AtomicU64,
    max_frame_bytes: AtomicU64,
    requests: Mutex<Vec<Value>>,
    progress_frames: Mutex<Vec<Value>>,
    credits: Mutex<Vec<Value>>,
}
impl Peer {
    fn new_with_frame_bound(profiles: &[ApprovedProfile], frame_bound: usize) -> Arc<Self> {
        let supported_profiles = profiles
            .iter()
            .map(|profile| v1_1::ProfileRange {
                name: profile.profile().into(),
                major: 1,
                min_minor: 0,
                max_minor: 0,
            })
            .collect();
        let capabilities = profiles
            .iter()
            .map(|profile| v1_1::CapabilityDeclaration {
                kind: profile.kind().into(),
                name: profile.name().into(),
                profile: Some(v1_1::ProfileRange {
                    name: profile.profile().into(),
                    major: 1,
                    min_minor: 0,
                    max_minor: 0,
                }),
                handler_id: profile.handler().into(),
                replay: profile.replay(),
                execution: profile.execution(),
                required: true,
                supported_descriptor_digests: vec![v1_1::SupportedProfileDigest {
                    minor: 0,
                    descriptor_digest_sha256:
                        pi_extension_protocol::descriptor::canonical_profile_digest(
                            profile.profile(),
                            1,
                            0,
                        )
                        .expect("profile digest")
                        .to_vec(),
                }],
                ..Default::default()
            })
            .collect();
        let peer = Arc::new(Self {
            input: Mutex::new(VecDeque::new()),
            writes: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            frame_bound,
            tool_invocations: AtomicU64::new(0),
            progress_items: AtomicU64::new(0),
            terminal_items: AtomicU64::new(0),
            max_frame_bytes: AtomicU64::new(0),
            requests: Mutex::new(Vec::new()),
            progress_frames: Mutex::new(Vec::new()),
            credits: Mutex::new(Vec::new()),
        });
        peer.push(v1_1::envelope::Frame::Hello(v1_1::Hello {
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 0,
            }),
            supported_profiles,
        }));
        peer.push(v1_1::envelope::Frame::Manifest(v1_1::Manifest {
            declared_extension_id: "peer-a".into(),
            extension_version: "m5".into(),
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 0,
            }),
            capabilities,
            permissions: vec![],
            ..Default::default()
        }));
        peer.push(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
        peer
    }

    fn new_tool(profiles: &[ApprovedProfile]) -> Arc<Self> {
        Self::new_tool_with_frame_bound(profiles, 8192)
    }

    fn new_tool_with_frame_bound(profiles: &[ApprovedProfile], frame_bound: usize) -> Arc<Self> {
        let peer = Self::new_with_frame_bound(profiles, frame_bound);
        peer.enable_tool(0);
        peer
    }

    fn tool_invocations(&self) -> u64 {
        self.tool_invocations.load(Ordering::SeqCst)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn progress_items(&self) -> u64 {
        self.progress_items.load(Ordering::SeqCst)
    }

    fn terminal_items(&self) -> u64 {
        self.terminal_items.load(Ordering::SeqCst)
    }

    fn max_frame_bytes(&self) -> u64 {
        self.max_frame_bytes.load(Ordering::SeqCst)
    }

    fn enable_tool(&self, minor: u32) {
        let raw = self
            .input
            .lock()
            .expect("peer input")
            .drain(..)
            .collect::<Vec<_>>();
        let mut reader = framing::FrameReader::<v1_1::Envelope>::new(
            NonZeroUsize::new(self.frame_bound).expect("reader"),
        );
        let mut at = 0;
        let catalog = echo_catalog();
        let set = &catalog["entries"][0]["selected_minor_schema_sets"][format!("1.{minor}")];
        while at < raw.len() {
            let read = reader.read_one(&raw[at..]).expect("handshake frame");
            at += read.consumed;
            let mut envelope = read.frame.expect("handshake envelope").envelope;
            match envelope.frame.as_mut().expect("handshake arm") {
                v1_1::envelope::Frame::Hello(hello) => {
                    hello.supported_profiles.push(v1_1::ProfileRange {
                        name: "tool.v1".into(),
                        major: 1,
                        min_minor: minor,
                        max_minor: minor,
                    });
                }
                v1_1::envelope::Frame::Manifest(manifest) => {
                    manifest.capabilities.push(v1_1::CapabilityDeclaration {
                        kind: "tool".into(),
                        name: "echo".into(),
                        profile: Some(v1_1::ProfileRange {
                            name: "tool.v1".into(),
                            major: 1,
                            min_minor: minor,
                            max_minor: minor,
                        }),
                        handler_id: "echo-handler".into(),
                        replay: 1,
                        execution: 2,
                        required: true,
                        supported_descriptor_digests: vec![v1_1::SupportedProfileDigest {
                            minor,
                            descriptor_digest_sha256:
                                pi_extension_protocol::descriptor::canonical_profile_digest(
                                    "tool.v1", 1, minor,
                                )
                                .expect("tool profile digest")
                                .to_vec(),
                        }],
                        tool_schema_sets: vec![v1_1::ToolSchemaSet {
                            tool_name: "echo".into(),
                            minor,
                            arguments: Some(v1_1::DynamicProjectionDigest {
                                projection_id: set["arguments"]["projection_id"]
                                    .as_str()
                                    .expect("argument projection")
                                    .into(),
                                schema_digest_sha256: hex(set["arguments"]["schema_digest_sha256"]
                                    .as_str()
                                    .expect("argument digest")),
                            }),
                            progress: Some(v1_1::DynamicProjectionDigest {
                                projection_id: set["progress"]["projection_id"]
                                    .as_str()
                                    .expect("progress projection")
                                    .into(),
                                schema_digest_sha256: hex(set["progress"]["schema_digest_sha256"]
                                    .as_str()
                                    .expect("progress digest")),
                            }),
                            raw_result: Some(v1_1::DynamicProjectionDigest {
                                projection_id: set["raw_result"]["projection_id"]
                                    .as_str()
                                    .expect("result projection")
                                    .into(),
                                schema_digest_sha256: hex(
                                    set["raw_result"]["schema_digest_sha256"]
                                        .as_str()
                                        .expect("result digest"),
                                ),
                            }),
                        }],
                        ..Default::default()
                    });
                }
                _ => {}
            }
            self.input.lock().expect("peer input").extend(
                framing::encode(
                    &envelope,
                    NonZeroUsize::new(self.frame_bound).expect("frame bound"),
                )
                .expect("encode handshake")
                .bytes,
            );
        }
    }

    fn respond_to_tool(&self, request: &v1_1::Request) {
        let invoke =
            v1_1::InvokeToolRequest::decode(request.payload.as_slice()).expect("tool request");
        let args = wire_value(
            invoke
                .arguments
                .expect("tool arguments")
                .value
                .expect("tool argument value"),
        );
        let minor = request
            .capability_ref
            .as_ref()
            .expect("tool capability")
            .profile
            .as_ref()
            .expect("tool profile")
            .minor;
        let set = &echo_catalog()["entries"][0]["selected_minor_schema_sets"][format!("1.{minor}")];
        let progress_raw = json!({
            "content": [{"type": "text", "text": format!("progress: {}", args["text"]) }],
            "details": {}
        });
        let progress = v1_1::SchemaBoundValue {
            projection_id: set["progress"]["projection_id"]
                .as_str()
                .expect("progress projection")
                .into(),
            schema_digest_sha256: hex(set["progress"]["schema_digest_sha256"]
                .as_str()
                .expect("progress digest")),
            value: Some(
                pi_extension_protocol::profiles::json_wire(&progress_raw).expect("progress value"),
            ),
        };
        let result_raw = json!({
            "content": [{"type": "text", "text": args["text"] }],
            "details": {}
        });
        let result = v1_1::SchemaBoundValue {
            projection_id: set["raw_result"]["projection_id"]
                .as_str()
                .expect("result projection")
                .into(),
            schema_digest_sha256: hex(set["raw_result"]["schema_digest_sha256"]
                .as_str()
                .expect("result digest")),
            value: Some(
                pi_extension_protocol::profiles::json_wire(&result_raw).expect("result value"),
            ),
        };
        self.progress_items.fetch_add(1, Ordering::SeqCst);
        let body_bytes = self.push(v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
            request_id: request.request_id,
            sequence: 0,
            payload: v1_1::ToolProgress {
                event: Some(progress),
                percent_complete: None,
            }
            .encode_to_vec(),
        }));
        self.progress_frames.lock().unwrap().push(json!({"request_id":request.request_id,"sequence":0,"body_bytes":body_bytes,"payload":progress_raw}));
        self.terminal_items.fetch_add(1, Ordering::SeqCst);
        self.push(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
            request_id: request.request_id,
            terminal: Some(v1_1::stream_end::Terminal::Result(
                v1_1::InvokeToolResult {
                    raw_result: Some(result),
                }
                .encode_to_vec(),
            )),
        }));
    }

    fn push(&self, frame: v1_1::envelope::Frame) -> usize {
        let encoded = framing::encode(
            &v1_1::Envelope { frame: Some(frame) },
            NonZeroUsize::new(self.frame_bound).expect("frame bound"),
        )
        .expect("encode handshake");
        let body_bytes = encoded.body_len;
        self.input.lock().expect("peer input").extend(encoded.bytes);
        body_bytes
    }
}
impl OrderedByteChannel for Peer {
    fn max_read_scratch_bytes(&self) -> usize {
        8192
    }
    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> pi_ai::BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(async move {
            loop {
                if self.closed.load(Ordering::SeqCst) {
                    return Ok(None);
                }
                let count = {
                    let mut input = self.input.lock().expect("peer input");
                    if input.is_empty() {
                        None
                    } else {
                        let count = buffer.len().min(input.len());
                        for destination in buffer.iter_mut().take(count) {
                            *destination = input.pop_front().expect("counted byte");
                        }
                        Some(count)
                    }
                };
                if let Some(count) = count {
                    return Ok(Some(count));
                }
                tokio::task::yield_now().await;
            }
        })
    }
    fn write_frame(
        &self,
        frame: pi_extension_protocol::wire::framing::EncodedFrame,
    ) -> pi_ai::BoxFuture<'_, Result<(), ChannelError>> {
        self.max_frame_bytes
            .fetch_max(frame.bytes.len() as u64, Ordering::SeqCst);
        let frame_index = {
            let mut writes = self.writes.lock().expect("peer writes");
            let index = writes.len();
            writes.push(frame.bytes.clone());
            index
        };
        let envelope =
            framing::FrameReader::<v1_1::Envelope>::new(NonZeroUsize::new(8192).expect("reader"))
                .read_one(&frame.bytes)
                .expect("host frame")
                .frame
                .expect("host envelope")
                .envelope;
        if let Some(v1_1::envelope::Frame::Credit(credit)) = &envelope.frame {
            self.credits.lock().unwrap().push(json!({"frame_index":frame_index,"request_id":credit.request_id,"additional_bytes":credit.additional_bytes}));
        }
        if let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame {
            eprintln!("M6C peer request {}", request.method);
            let cap = request.capability_ref.as_ref().unwrap();
            let mut observation = json!({"method":request.method,"capability":{"kind":cap.kind,"name":cap.name,"principal":cap.authenticated_extension_id,"handler":cap.handler_id},"request_id":request.request_id,"frame_index":frame_index});
            if request.method == "tool.invoke" {
                let invoke = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
                observation["input"] = wire_value(invoke.arguments.unwrap().value.unwrap());
                self.tool_invocations.fetch_add(1, Ordering::SeqCst);
                self.respond_to_tool(&request);
            } else if request.method == "before_run" {
                let invoke = dto::InvokeHookRequest::decode(request.payload.as_slice()).unwrap();
                let input = wire_value(invoke.input.unwrap().value.unwrap());
                observation["input"] = json!({"name":input["name"],"run_id":input["event"]["runId"],"prompt":input["event"]["prompt"].as_array().unwrap().iter().map(|message|message_json(&serde_json::from_value::<AgentMessage>(message.clone()).unwrap())).collect::<Vec<_>>()});
                self.push(v1_1::envelope::Frame::Response(v1_1::Response {
                    request_id: request.request_id,
                    terminal: Some(v1_1::response::Terminal::Result(
                        dto::InvokeHookResult { output: None }.encode_to_vec(),
                    )),
                }));
            } else if request.method == "provider.invoke" {
                let invoke =
                    dto::InvokeProviderRequest::decode(request.payload.as_slice()).unwrap();
                // Match the existing profile PayloadBudget accounting against
                // the actual received DTO, including the encoded request body.
                let decoded_charge = request.payload.len()
                    + [
                        &invoke.model,
                        &invoke.context,
                        &invoke.tools,
                        &invoke.options,
                    ]
                    .iter()
                    .map(|field| {
                        decoded_value_bytes(field.as_ref().unwrap().value.as_ref().unwrap())
                    })
                    .sum::<usize>();
                let model = wire_value(invoke.model.unwrap().value.unwrap());
                let context = serde_json::from_value::<pi_ai::Context>(wire_value(
                    invoke.context.unwrap().value.unwrap(),
                ))
                .unwrap();
                observation["input"] = json!({"model":{"id":model["id"],"provider":model["provider"],"name":model["name"]},"context":context_json(&context),"auth_mode":invoke.auth_mode,"encoded_bytes":request.payload.len(),"decoded_bytes":decoded_charge});
                let registry = pi_extension_protocol::profiles::Registry::bundled().unwrap();
                let message = json!({"role":"assistant","content":[{"type":"text","text":"remote completion"}],"api":model["api"],"provider":model["provider"],"model":model["id"],"usage":{"input":3,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":5,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":7});
                let body = dto::InvokeProviderResult {
                    assistant_message: Some(
                        pi_harness_runtime::extensions::profiles::bound(
                            &registry,
                            "provider.assistant_message.v1",
                            &message,
                        )
                        .unwrap(),
                    ),
                    usage: Some(
                        pi_harness_runtime::extensions::profiles::bound(
                            &registry,
                            "provider.usage.v1",
                            &message["usage"],
                        )
                        .unwrap(),
                    ),
                }
                .encode_to_vec();
                self.push(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                    request_id: request.request_id,
                    terminal: Some(v1_1::stream_end::Terminal::Result(body)),
                }));
            } else {
                panic!("unexpected peer method {}", request.method);
            }
            self.requests.lock().unwrap().push(observation);
        }
        Box::pin(async { Ok(()) })
    }
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

impl RetainedOrderedByteChannel for Peer {
    fn take_notifications(&self) -> [Option<Waker>; 2] {
        [None, None]
    }

    fn close_retained(&self) -> [Option<Waker>; 2] {
        self.close();
        [None, None]
    }
}

fn hex(value: &str) -> Vec<u8> {
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).expect("hex"))
        .collect()
}

fn echo_catalog() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../pi-extension-protocol/schema/fixtures/host-catalog.json"
    ))
    .expect("echo catalog")
}

fn wire_value(value: v1_1::WireValue) -> serde_json::Value {
    use v1_1::wire_value::Value;
    match value.value.expect("wire value") {
        Value::NullValue(_) => serde_json::Value::Null,
        Value::BoolValue(value) => serde_json::Value::Bool(value),
        Value::IntegerValue(value) => serde_json::Value::from(value),
        Value::FloatValue(value) => serde_json::Value::from(value),
        Value::TextValue(value) => serde_json::Value::String(value),
        Value::ListValue(value) => {
            serde_json::Value::Array(value.items.into_iter().map(wire_value).collect())
        }
        Value::MapValue(value) => serde_json::Value::Object(
            value
                .entries
                .into_iter()
                .map(|entry| (entry.key, wire_value(entry.value.expect("map value"))))
                .collect(),
        ),
        Value::BytesValue(_) => panic!("echo fixture does not use byte values"),
    }
}
// pi-extension-protocol::profiles::PayloadBudget::charge owns this logical
// accounting. Read concrete DTO sizes on this host; do not assume encoded size.
fn decoded_value_bytes(value: &v1_1::WireValue) -> usize {
    use v1_1::wire_value::Value as W;
    std::mem::size_of::<v1_1::WireValue>()
        + match value.value.as_ref().unwrap() {
            W::TextValue(text) => text.len(),
            W::ListValue(list) => list.items.iter().map(decoded_value_bytes).sum(),
            W::MapValue(map) => map
                .entries
                .iter()
                .map(|entry| {
                    std::mem::size_of::<v1_1::WireMapEntry>()
                        + entry.key.len()
                        + decoded_value_bytes(entry.value.as_ref().unwrap())
                })
                .sum(),
            W::BytesValue(_) => panic!("the provider fixture has no byte values"),
            _ => 0,
        }
}
fn host_with_tool_generation_and_frame(
    profiles: Vec<ApprovedProfile>,
    generation: u64,
    tool: Option<ApprovedTool>,
    frame_bytes: u64,
) -> HostAuthority {
    HostAuthority::new(
        "peer-a".into(),
        generation,
        0,
        0,
        v1_1::ProtocolLimits {
            pre_handshake_frame_bytes: frame_bytes,
            frame_bytes,
            // Full provider history charges 9,963 logical decoded DTO bytes.
            aggregate_decoded_bytes: 16_384,
            value_depth: 32,
            value_elements: 1024,
            string_bytes: 4096,
            bytes_length: 4096,
            in_flight_requests: 8,
            blocking_queue_bytes: 4096,
            progress_queue_bytes: 4096,
            passive_queue_bytes: 4096,
            telemetry_queue_bytes: 4096,
            nesting_depth: 8,
            requests_per_second: 32,
            handshake_ms: 1000,
            cancellation_grace_ms: 100,
            max_stream_credit_bytes: 4096,
        },
        tool.into_iter().collect(),
        vec![],
    )
    .expect("host")
    .with_profiles(profiles)
    .expect("profiles")
}

fn echo_approval() -> ApprovedTool {
    let entry = &echo_catalog()["entries"][0];
    let agent_tool = &entry["agentTool"];
    let descriptor = pi_extension_protocol::activation::AgentTool {
        name: agent_tool["name"].as_str().expect("tool name").into(),
        label: agent_tool["label"].as_str().expect("tool label").into(),
        description: agent_tool["description"]
            .as_str()
            .expect("tool description")
            .into(),
        parameters: agent_tool["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let schemas = [0_u32, 1]
        .into_iter()
        .map(|minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).expect("arguments"),
                progress: serde_json::to_vec(&set["progress"]["schema"]).expect("progress"),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).expect("result"),
            }
        })
        .collect();
    ApprovedTool::new("peer-a".into(), descriptor, schemas, Default::default())
        .expect("echo approval")
}

fn reservation() -> pi_extension_protocol::profile_live::Reservations {
    pi_extension_protocol::profile_live::Reservations {
        blocking_bytes: 4096,
        control_bytes: 65_536,
        terminal_bytes: 65_536,
        progress_bytes: 4096,
        passive_bytes: 4096,
        telemetry_bytes: 4096,
    }
}

fn config() -> PumpConfig {
    PumpConfig {
        max_read_chunk: NonZeroUsize::new(1024).expect("read bound"),
        write_timeout_ms: NonZeroU64::new(1000).expect("write timeout"),
    }
}

fn capability(connection: &ProfileConnection, kind: &str, name: &str) -> v1_1::CapabilityRef {
    connection
        .grant()
        .profiles()
        .iter()
        .find(|profile| profile.reference().kind == kind && profile.reference().name == name)
        .expect("granted capability")
        .reference()
        .clone()
}

fn tool_response(model: &Model) -> AssistantMessage {
    let mut response = coding_runtime_m6b_support::assistant(model, pi_ai::StopReason::ToolUse, "");
    response.content = vec![pi_ai::AssistantContent::ToolCall(pi_ai::ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("m6c-echo").unwrap(),
        name: ToolName::new("echo").unwrap(),
        arguments: serde_json::from_value(json!({"text":"extension echo"})).unwrap(),
        thought_signature: None,
        namespace: None,
    })];
    response
}

pub(super) async fn participation() -> Value {
    let f = fixture().await;
    *f.adapter.responses.lock().unwrap() = VecDeque::from([
        tool_response(&f.model),
        coding_runtime_m6b_support::assistant(&f.model, pi_ai::StopReason::Stop, "echo settled"),
    ]);
    let h = builder(&f, f.session.clone())
        .first_attachment()
        .build()
        .await
        .unwrap();
    let trace = Arc::new(Trace::default());
    trace.attach(&h);
    let selected = vec![
        profile("hook", "before_run"),
        profile("provider", "openrouter"),
    ];
    let peer = Peer::new_tool(&selected);
    let owner = SingleToolOwner::new(
        Arc::new(TokioSpawner),
        OwnerLimits {
            outbound_bytes: NonZeroUsize::new(8192).unwrap(),
            receipt_bytes: NonZeroUsize::new(8192).unwrap(),
            write_timeout_ms: NonZeroU64::new(1000).unwrap(),
        },
    );
    let adapter = CodingExtensionAdapter::connect_tool_retained(
        &h,
        pi_coding_runtime::ExtensionRetainedToolInput {
            host: host_with_tool_generation_and_frame(selected, 7, Some(echo_approval()), 8192),
            owner,
            channel: peer.clone(),
            clock: Arc::new(Mono(AtomicU64::new(0))),
            config: config(),
            reservations: reservation(),
            remaining_ms: 10_000,
        },
    )
    .await
    .unwrap();
    let grant = adapter.grant().wire();
    let granted = json!({"principal":grant.authenticated_extension_id,"generation":grant.connection_generation,"ready":adapter.connection().current(),"frame_bytes":grant.limits.as_ref().unwrap().frame_bytes,"aggregate_decoded_bytes":grant.limits.as_ref().unwrap().aggregate_decoded_bytes});
    let provider = capability(adapter.connection(), "provider", "openrouter");
    let hook = capability(adapter.connection(), "hook", "before_run");
    let mut remote = model("remote-offline");
    remote.provider = ProviderId::new("openrouter").unwrap();
    remote.name = "remote catalog model".into();
    let attachment=h.attach_extensions(adapter,vec![ExtensionRegistration::Provider{capability:provider,models:vec![remote.clone()],model_policy:Default::default(),payload_schema:Some(pi_extension_protocol::jcs::admit_schema(br#"{"type":"object","properties":{"model":{"type":"object"},"context":{"type":"object"},"tools":{"type":"array","items":{"type":"object"},"maxItems":0},"options":{"type":"object"}},"required":["model","context","tools","options"],"additionalProperties":false}"#,Default::default()).unwrap())},ExtensionRegistration::echo_tool(Arc::new(EchoTool),EchoRouteTiming{method_limit_ms:NonZeroU64::new(1000).unwrap(),connection_deadline_ms:10_000}),ExtensionRegistration::hook(hook)]).await.unwrap();
    let registration_order = attachment
        .registration_order()
        .iter()
        .map(|kind| match kind {
            ExtensionRegistrationKind::Provider => "provider",
            ExtensionRegistrationKind::EchoTool => "echo_tool",
            ExtensionRegistrationKind::Hook => "hook",
        })
        .collect::<Vec<_>>();
    let hook_count = h
        .harness()
        .registered_hook_count(pi_agent::schema::HookName::BeforeRun);
    let catalog = f.registry.get_model(&remote.provider, &remote.id).unwrap();
    assert_eq!(catalog, remote);
    h.harness()
        .set_active_tools(vec![ToolName::new("echo").unwrap()])
        .await
        .unwrap();
    prompt(h.harness().as_ref(), "extension tool operation").await;
    assert_eq!(peer.tool_invocations(), 1);
    assert_eq!(peer.progress_items(), 1);
    assert_eq!(peer.terminal_items(), 1);
    h.harness().set_active_tools(vec![]).await.unwrap();
    h.switch_model(remote.provider.clone(), remote.id.clone())
        .await
        .unwrap();
    prompt(h.harness().as_ref(), "extension provider operation").await;
    let session = snapshot(f.session.as_ref()).await;
    let leaf = f.session.get_leaf_id().await.unwrap().unwrap();
    let Entry::Message(entry) = f.session.get_entry(leaf).await.unwrap().unwrap() else {
        panic!("remote provider must publish a message leaf")
    };
    let AgentMessage::BuiltIn(Message::Assistant(message)) = &entry.message else {
        panic!("remote provider must publish an assistant leaf")
    };
    let provider_terminal = json!({"entry_id":entry.id,"provider":message.provider,"model":message.model,
        "usage_total":message.usage.total_tokens,"message":message_json(&entry.message)});
    let requests = peer.requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .map(|r| r["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["before_run", "tool.invoke", "before_run", "provider.invoke"]
    );
    let current_before = attachment.connection().current();
    let connection = attachment.connection().clone();
    attachment.close();
    let current_after = connection.current();
    let hooks_after = h
        .harness()
        .registered_hook_count(pi_agent::schema::HookName::BeforeRun);
    let mut obs = trace.base(&f, entries_len(&session));
    obs["provider_calls"] = json!(
        f.adapter.calls.load(Ordering::SeqCst)
            + requests
                .iter()
                .filter(|r| r["method"] == "provider.invoke")
                .count()
    );
    obs["extension"] = json!({"grant":granted,"registration_order":registration_order,"hook_count":hook_count,
        "catalog":{"provider":catalog.provider,"id":catalog.id,"name":catalog.name,"no_auth":f.registry.get_provider(&catalog.provider).unwrap().auth().is_no_auth()},
        "peer_requests":requests,"progress_items":peer.progress_items(),"terminal_items":peer.terminal_items(),"tool_invocations":peer.tool_invocations(),
        "max_frame_bytes":peer.max_frame_bytes(),"connection_before_close":current_before,"connection_after_close":current_after,"hooks_after_close":hooks_after,"peer_closed":peer.is_closed(),"session":session,"provider_terminal":provider_terminal,"progress_frames":peer.progress_frames.lock().unwrap().clone(),"credits":peer.credits.lock().unwrap().clone()});
    obs["fixture_boundary"] = json!(
        "host authenticates peer-a and supplies grants/channel; deterministic provider, echo and hook frames only; JS loading, transport authentication, live credentials/network and same-family hook identity remain unverified; RoutedTool currently consumes protocol progress and returns credit without forwarding Harness ToolUpdate (observed limitation for final assembly review)"
    );
    h.harness().close().await.unwrap();
    result("extension.participation_ordering", &trace, obs)
}
