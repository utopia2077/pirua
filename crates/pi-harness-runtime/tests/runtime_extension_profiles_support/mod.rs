#![allow(dead_code)]
use runtime_extension_profiles_alloc::TestAlloc;

#[global_allocator]
static GLOBAL: TestAlloc = TestAlloc;

use pi_extension_protocol::{
    activation::*,
    profiles::{self, wire_v1_1 as dto},
    wire::{
        framing::{self, EncodedFrame, FrameReader},
        v1_1,
    },
};
use pi_harness_runtime::extensions::{profiles::connection::ProfileConnection, transport::*};
use prost::Message;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
};
pub const HOOKS: [&str; 11] = [
    "before_run",
    "before_resume",
    "before_run_end",
    "transform_context",
    "before_request",
    "before_payload",
    "after_response",
    "before_tool",
    "after_tool",
    "before_compaction",
    "before_navigation",
];
pub fn profile(profile: &str, name: &str) -> profiles::ApprovedProfile {
    let empty = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{},"additionalProperties":false}"#,
        Default::default(),
    )
    .unwrap();
    let null = pi_extension_protocol::jcs::admit_schema(br#"{"type":"null"}"#, Default::default())
        .unwrap();
    let mut leaves=BTreeMap::from([("hook.input:before_tool.event.args".into(),empty.clone()),("hook.input:after_tool.event.args".into(),empty.clone()),("provider.tools.v1[].parameters".into(),empty.clone()),("provider.assistant_event.v1.toolCall.arguments".into(),empty),("hook.input:before_payload.event.payload".into(),pi_extension_protocol::jcs::admit_schema(br#"{"type":"object","properties":{"request":{"type":"string","enum":["actual faux input","native replacement"]}},"required":["request"],"additionalProperties":false}"#,Default::default()).unwrap()),("hook.output:before_payload.result.payload".into(),pi_extension_protocol::jcs::admit_schema(br#"{"type":"object","properties":{"request":{"type":"string","enum":["actual faux input","native replacement"]}},"required":["request"],"additionalProperties":false}"#,Default::default()).unwrap()),("hook.input:after_tool.event.details".into(),null)]);
    let golden: Value = serde_json::from_str(include_str!(
        "../../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    for (part, value) in [
        ("compat", &golden["model"]["compat"]),
        ("samplingParams", &golden["model"]["samplingParams"]),
    ] {
        let schema = pi_extension_protocol::jcs::admit_schema(
            &serde_json::to_vec(&exact_schema(value)).unwrap(),
            Default::default(),
        )
        .unwrap();
        for root in [
            "provider.model.v1",
            "hook.input:before_request.event.model",
            "hook.input:before_payload.event.model",
        ] {
            leaves.insert(format!("{root}.{part}"), schema.clone());
        }
    }
    profiles::ApprovedProfile::new(profiles::ProfileApproval {
        principal: "peer-a".into(),
        kind: profile.trim_end_matches(".v1").into(),
        name: name.into(),
        profile: profile.into(),
        handler: "remote-handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: if profile == "provider.v1" {
            2
        } else if profile.ends_with("sink.v1") {
            3
        } else {
            1
        },
        scope: if profile == "hook.v1" || profile == "provider.v1" {
            "main".into()
        } else {
            "allowed".into()
        },
        permissions: match profile {
            "resource.v1" => BTreeSet::from([("resource.provide".into(), "allowed".into())]),
            "telemetry-sink.v1" => BTreeSet::from([("telemetry.receive".into(), "allowed".into())]),
            _ => BTreeSet::new(),
        },
        exposure: profiles::Exposure::new(leaves, BTreeSet::new()).unwrap(),
    })
    .unwrap()
}
pub struct Mono(pub AtomicU64);
impl MonotonicClock for Mono {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, deadline: u64) -> pi_ai_core::BoxFuture<'_, ()> {
        Box::pin(std::future::poll_fn(move |_| {
            if self.now_ms() >= deadline {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    }
}
pub struct Peer {
    pub input: Mutex<VecDeque<u8>>,
    pub requests: Mutex<Vec<v1_1::Request>>,
    pub responses: Mutex<Vec<v1_1::Response>>,
    pub items: Mutex<Vec<v1_1::StreamItem>>,
    pub ends: Mutex<Vec<v1_1::StreamEnd>>,
    pub closed: AtomicBool,
    pub core: u32,
    pub selected: Vec<profiles::ApprovedProfile>,
    pub results: Mutex<BTreeMap<String, Option<Value>>>,
    pub malformed: AtomicBool,
    pub hold: AtomicBool,
    pub hold_provider: AtomicBool,
    pub hold_tools: AtomicBool,
}
impl Peer {
    pub fn new(core: u32, minor: u32, selected: Vec<profiles::ApprovedProfile>) -> Arc<Self> {
        let profiles = selected
            .iter()
            .map(|p| {
                (
                    p.profile().to_owned(),
                    v1_1::ProfileRange {
                        name: p.profile().into(),
                        major: 1,
                        min_minor: minor,
                        max_minor: minor,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect();
        let capabilities = selected
            .iter()
            .map(|p| v1_1::CapabilityDeclaration {
                kind: p.kind().into(),
                name: p.name().into(),
                profile: Some(v1_1::ProfileRange {
                    name: p.profile().into(),
                    major: 1,
                    min_minor: minor,
                    max_minor: minor,
                }),
                handler_id: p.handler().into(),
                replay: p.replay(),
                execution: p.execution(),
                required: true,
                supported_descriptor_digests: vec![v1_1::SupportedProfileDigest {
                    minor,
                    descriptor_digest_sha256:
                        pi_extension_protocol::descriptor::canonical_profile_digest(
                            p.profile(),
                            1,
                            minor,
                        )
                        .unwrap()
                        .to_vec(),
                }],
                tool_schema_sets: vec![],
                ..Default::default()
            })
            .collect();
        let peer = Arc::new(Self {
            input: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(Vec::new()),
            items: Mutex::new(Vec::new()),
            ends: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            core,
            selected,
            results: Mutex::new(BTreeMap::new()),
            malformed: AtomicBool::new(false),
            hold: AtomicBool::new(false),
            hold_provider: AtomicBool::new(false),
            hold_tools: AtomicBool::new(false),
        });
        peer.push(v1_1::envelope::Frame::Hello(v1_1::Hello {
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: core,
                max_minor: core,
            }),
            supported_profiles: profiles,
        }));
        peer.push(v1_1::envelope::Frame::Manifest(v1_1::Manifest {
            declared_extension_id: "claimed".into(),
            extension_version: "1".into(),
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: core,
                max_minor: core,
            }),
            capabilities,
            permissions: peer
                .selected
                .iter()
                .flat_map(|profile| {
                    profile
                        .permissions()
                        .iter()
                        .map(|(name, scope)| v1_1::PermissionRequest {
                            name: name.clone(),
                            scope: scope.clone(),
                            required: true,
                        })
                })
                .collect(),
            ..Default::default()
        }));
        peer.push(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
        peer
    }
    pub fn push(&self, frame: v1_1::envelope::Frame) {
        let encoded = framing::encode(
            &v1_1::Envelope { frame: Some(frame) },
            NonZeroUsize::new(262144).unwrap(),
        )
        .unwrap();
        self.input.lock().unwrap().extend(encoded.bytes);
    }
    fn terminal(&self, request: &v1_1::Request) {
        let registry = profiles::Registry::bundled().unwrap();
        if request.method == "tool.invoke" {
            self.tool_request(request);
        } else if matches!(
            request.method.as_str(),
            "resource.list" | "resource.read" | "telemetry.emit_batch" | "telemetry.flush"
        ) {
            let phase = self
                .results
                .lock()
                .unwrap()
                .get("resource_phase")
                .and_then(|v| v.as_ref())
                .and_then(Value::as_u64)
                .unwrap_or(1);
            let text = if phase == 1 { "hello" } else { "world" };
            let digest = hex(if phase == 1 {
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            } else {
                "486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7"
            });
            let result=match request.method.as_str(){
                "resource.list"=>dto::ResourceListResult{revision:format!("revision-{phase}"),resources:vec![
                    dto::ResourceManifest{id:"skill-id".into(),version:phase.to_string(),kind:"skill".into(),digest_sha256:digest.clone(),metadata:Some(pi_harness_runtime::extensions::profiles::bound(&registry,"resource.metadata:skill",&json!({"name":"remote-skill","description":"Remote native skill","filePath":"/remote/skill.md"})).unwrap())},
                    dto::ResourceManifest{id:"template-id".into(),version:phase.to_string(),kind:"prompt_template".into(),digest_sha256:digest.clone(),metadata:Some(pi_harness_runtime::extensions::profiles::bound(&registry,"resource.metadata:prompt_template",&json!({"name":"remote-template","description":"Remote template"})).unwrap())}
                ]}.encode_to_vec(),
                "resource.read"=>dto::ResourceReadResult{digest_sha256:digest,value:Some(dto::resource_read_result::Value::Text(text.into()))}.encode_to_vec(),
                "telemetry.emit_batch"=>{let batch=dto::TelemetryEmitBatchRequest::decode(request.payload.as_slice()).unwrap();dto::TelemetryEmitBatchResult{accepted:batch.records.len() as u64+u64::from(self.malformed.load(Ordering::SeqCst)),dropped:0}.encode_to_vec()},
                _=>{let flush=dto::TelemetryFlushRequest::decode(request.payload.as_slice()).unwrap();dto::TelemetryFlushResult{flushed_through:flush.through_sequence}.encode_to_vec()},
            };
            self.push(v1_1::envelope::Frame::Response(v1_1::Response {
                request_id: request.request_id,
                terminal: Some(v1_1::response::Terminal::Result(result)),
            }));
        } else if request.method == "provider.invoke" {
            let invoke = dto::InvokeProviderRequest::decode(request.payload.as_slice()).unwrap();
            let model = wire_value(invoke.model.unwrap().value.unwrap());
            let mut message = json!({"role":"assistant","content":[{"type":"text","text":"remote completion"}],"api":model["api"],"provider":model["provider"],"model":model["id"],"usage":{"input":3,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":5,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":7});
            if self
                .results
                .lock()
                .unwrap()
                .get("provider_mode")
                .and_then(|v| v.as_ref())
                .and_then(Value::as_str)
                == Some("deferred")
                && request
                    .effect_key
                    .as_ref()
                    .is_some_and(|key| key.effect_kind == "assistant")
            {
                message["stopReason"] = json!("deferred");
                message["deferred"] = json!({"provider":model["provider"],"api":model["api"],"modelId":model["id"],"id":"native-source-handle"});
            }
            let mut output = pi_harness_runtime::extensions::profiles::bound(
                &registry,
                "provider.assistant_message.v1",
                &message,
            )
            .unwrap();
            if self.malformed.load(Ordering::SeqCst) {
                output.projection_id = "forged".into();
            }
            let result = dto::InvokeProviderResult {
                assistant_message: Some(output),
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
                terminal: Some(v1_1::stream_end::Terminal::Result(result)),
            }));
            if self
                .results
                .lock()
                .unwrap()
                .get("loss_after_terminal")
                .and_then(|v| v.as_ref())
                .and_then(Value::as_bool)
                == Some(true)
            {
                self.closed.store(true, Ordering::SeqCst);
            }
        } else {
            let output = self
                .results
                .lock()
                .unwrap()
                .get(&request.method)
                .cloned()
                .flatten()
                .map(|result| {
                    pi_harness_runtime::extensions::profiles::bound(
                        &registry,
                        &format!("hook.output:{}", request.method),
                        &json!({"name":request.method,"result":result}),
                    )
                    .unwrap()
                });
            let result = if request.method == "before_resume" {
                dto::BeforeResumeUnitResult {}.encode_to_vec()
            } else {
                dto::InvokeHookResult { output }.encode_to_vec()
            };
            self.push(v1_1::envelope::Frame::Response(v1_1::Response {
                request_id: request.request_id,
                terminal: Some(v1_1::response::Terminal::Result(result)),
            }));
        }
    }
    pub fn release(&self) {
        self.hold.store(false, Ordering::SeqCst);
        self.hold_provider.store(false, Ordering::SeqCst);
        self.hold_tools.store(false, Ordering::SeqCst);
        let request = self.requests.lock().unwrap().last().unwrap().clone();
        self.terminal(&request);
    }
}
impl OrderedByteChannel for Peer {
    fn max_read_scratch_bytes(&self) -> usize {
        4096
    }
    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> pi_ai_core::BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(std::future::poll_fn(move |_| {
            let mut input = self.input.lock().unwrap();
            if input.is_empty() {
                return if self.closed.load(Ordering::SeqCst) {
                    Poll::Ready(Ok(None))
                } else {
                    Poll::Pending
                };
            }
            let size = buffer.len().min(input.len());
            for value in &mut buffer[..size] {
                *value = input.pop_front().unwrap();
            }
            Poll::Ready(Ok(Some(size)))
        }))
    }
    fn write_frame(
        &self,
        frame: EncodedFrame,
    ) -> pi_ai_core::BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(async move {
            if self.closed.load(Ordering::SeqCst) {
                return Err(ChannelError::Closed);
            }
            let mut reader = FrameReader::<v1_1::Envelope>::new(NonZeroUsize::new(262144).unwrap());
            let envelope = reader
                .read_one(&frame.bytes)
                .unwrap()
                .frame
                .unwrap()
                .envelope;
            match envelope.frame.unwrap() {
                v1_1::envelope::Frame::Request(request) => {
                    self.requests.lock().unwrap().push(request.clone());
                    if !self.hold.load(Ordering::SeqCst)
                        && !(self.hold_provider.load(Ordering::SeqCst)
                            && request.method == "provider.invoke")
                        && !(self.hold_tools.load(Ordering::SeqCst)
                            && request.method == "tool.invoke")
                    {
                        self.terminal(&request);
                    }
                }
                v1_1::envelope::Frame::Response(response) => {
                    let id = response.request_id;
                    self.responses.lock().unwrap().push(response);
                    let request = self
                        .requests
                        .lock()
                        .unwrap()
                        .iter()
                        .rev()
                        .find(|request| {
                            request.method == "tool.invoke"
                                && request.request_id
                                    + self
                                        .results
                                        .lock()
                                        .unwrap()
                                        .get("peer_child_offset")
                                        .and_then(|v| v.as_ref())
                                        .and_then(Value::as_u64)
                                        .unwrap_or(1)
                                    == id
                        })
                        .cloned();
                    if let Some(request) = request {
                        self.tool_terminal(&request);
                    }
                }
                v1_1::envelope::Frame::StreamItem(item) => self.items.lock().unwrap().push(item),
                v1_1::envelope::Frame::StreamEnd(end) => self.ends.lock().unwrap().push(end),
                v1_1::envelope::Frame::Cancel(_) => {}
                _ => {}
            }
            Ok(())
        })
    }
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
pub fn wire_value(value: dto::WireValue) -> Value {
    use dto::wire_value::Value as W;
    match value.value.unwrap() {
        W::NullValue(_) => Value::Null,
        W::BoolValue(v) => Value::Bool(v),
        W::IntegerValue(v) => Value::from(v),
        W::FloatValue(v) => Value::from(v),
        W::TextValue(v) => Value::String(v),
        W::ListValue(v) => Value::Array(v.items.into_iter().map(wire_value).collect()),
        W::MapValue(v) => Value::Object(
            v.entries
                .into_iter()
                .map(|entry| (entry.key, wire_value(entry.value.unwrap())))
                .collect(),
        ),
        W::BytesValue(_) => panic!("not a native JSON value"),
    }
}

pub fn connection(
    setup: &super::runtime_r1_r4_support::Setup,
    core: u32,
    minor: u32,
    profiles: Vec<profiles::ApprovedProfile>,
) -> (Arc<ProfileConnection>, Arc<Peer>, Arc<Mono>) {
    let peer = Peer::new(core, minor, profiles.clone());
    let clock = Arc::new(Mono(AtomicU64::new(0)));
    let limits = v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 262144,
        frame_bytes: 262144,
        aggregate_decoded_bytes: 1048576,
        value_depth: 64,
        value_elements: 16384,
        string_bytes: 262144,
        bytes_length: 262144,
        in_flight_requests: 32,
        blocking_queue_bytes: 1048576,
        progress_queue_bytes: 1048576,
        passive_queue_bytes: 1048576,
        telemetry_queue_bytes: 1048576,
        nesting_depth: 8,
        requests_per_second: 100,
        handshake_ms: 1000,
        cancellation_grace_ms: 20,
        max_stream_credit_bytes: 1048576,
    };
    let permissions = profiles
        .iter()
        .flat_map(|profile| {
            profile
                .permissions()
                .iter()
                .map(|(name, scope)| v1_1::PermissionGrant {
                    name: name.clone(),
                    scope: scope.clone(),
                })
        })
        .collect();
    let host = HostAuthority::new("peer-a".into(), 7, core, 1, limits, vec![], permissions)
        .unwrap()
        .with_profiles(profiles)
        .unwrap();
    let reservations = pi_extension_protocol::profile_live::Reservations {
        blocking_bytes: 1048576,
        control_bytes: 8388608,
        terminal_bytes: 8388608,
        progress_bytes: 1048576,
        passive_bytes: 1048576,
        telemetry_bytes: 1048576,
    };
    let connection = setup
        .pump
        .drive(ProfileConnection::connect(
            host,
            peer.clone(),
            clock.clone(),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(1024).unwrap(),
                write_timeout_ms: NonZeroU64::new(500).unwrap(),
            },
            reservations,
            10000,
        ))
        .unwrap();
    (connection, peer, clock)
}

pub fn exact_schema(value: &Value) -> Value {
    match value {
        Value::Null => json!({"type":"null"}),
        Value::Bool(_) => json!({"type":"boolean","enum":[value]}),
        Value::String(_) => json!({"type":"string","enum":[value]}),
        Value::Number(_) => json!({"type":"number","enum":[value]}),
        Value::Object(map) => {
            json!({"type":"object","properties":map.iter().map(|(k,v)|(k.clone(),exact_schema(v))).collect::<serde_json::Map<_,_>>(),"required":map.keys().cloned().collect::<Vec<_>>(),"additionalProperties":false})
        }
        Value::Array(values) => {
            json!({"type":"array","items":values.first().map(exact_schema).unwrap_or(json!({"type":"null"})),"minItems":values.len(),"maxItems":values.len()})
        }
    }
}

pub fn hex(value: &str) -> Vec<u8> {
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
        .collect()
}
pub fn echo_catalog() -> Value {
    serde_json::from_str(include_str!(
        "../../../pi-extension-protocol/schema/fixtures/host-catalog.json"
    ))
    .unwrap()
}
pub fn echo_approval() -> ApprovedTool {
    let catalog = echo_catalog();
    let entry = &catalog["entries"][0];
    let descriptor = AgentTool {
        name: "echo".into(),
        label: entry["agentTool"]["label"].as_str().unwrap().into(),
        description: entry["agentTool"]["description"].as_str().unwrap().into(),
        parameters: entry["agentTool"]["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let schemas = [0, 1]
        .into_iter()
        .map(|minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    ApprovedTool::new("peer-a".into(), descriptor, schemas, Default::default()).unwrap()
}
pub fn echo_hook(name: &str) -> profiles::ApprovedProfile {
    let catalog = echo_catalog();
    let schema = pi_extension_protocol::jcs::admit_schema(
        &serde_json::to_vec(&catalog["entries"][0]["agentTool"]["parameters"]).unwrap(),
        Default::default(),
    )
    .unwrap();
    let empty = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{},"additionalProperties":false}"#,
        Default::default(),
    )
    .unwrap();
    let exposure = profiles::Exposure::new(
        BTreeMap::from([
            ("hook.input:before_tool.event.args".into(), schema.clone()),
            ("hook.input:after_tool.event.args".into(), schema),
            ("hook.input:after_tool.event.details".into(), empty),
        ]),
        BTreeSet::new(),
    )
    .unwrap();
    profiles::ApprovedProfile::new(profiles::ProfileApproval {
        principal: "peer-a".into(),
        kind: "hook".into(),
        name: name.into(),
        profile: "hook.v1".into(),
        handler: "remote-handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure,
    })
    .unwrap()
}
pub fn actions_profile() -> profiles::ApprovedProfile {
    profiles::ApprovedProfile::new(profiles::ProfileApproval {
        principal: "peer-a".into(),
        kind: "host-action".into(),
        name: "actions".into(),
        profile: "host-action.v1".into(),
        handler: "native-actions".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: "main".into(),
        permissions: BTreeSet::from([("queue.next_run".into(), "main".into())]),
        exposure: profiles::Exposure::new(BTreeMap::new(), BTreeSet::new()).unwrap(),
    })
    .unwrap()
}
impl Peer {
    pub fn enable_tool(&self, minor: u32) {
        let raw = self.input.lock().unwrap().drain(..).collect::<Vec<_>>();
        let mut reader = FrameReader::<v1_1::Envelope>::new(NonZeroUsize::new(262144).unwrap());
        let mut at = 0;
        let catalog = echo_catalog();
        let set = &catalog["entries"][0]["selected_minor_schema_sets"][format!("1.{minor}")];
        while at < raw.len() {
            let read = reader.read_one(&raw[at..]).unwrap();
            at += read.consumed;
            let mut envelope = read.frame.unwrap().envelope;
            match envelope.frame.as_mut().unwrap() {
                v1_1::envelope::Frame::Hello(hello) => {
                    hello.supported_profiles.push(v1_1::ProfileRange {
                        name: "tool.v1".into(),
                        major: 1,
                        min_minor: minor,
                        max_minor: minor,
                    })
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
                                .unwrap()
                                .to_vec(),
                        }],
                        tool_schema_sets: vec![v1_1::ToolSchemaSet {
                            tool_name: "echo".into(),
                            minor,
                            arguments: Some(v1_1::DynamicProjectionDigest {
                                projection_id: set["arguments"]["projection_id"]
                                    .as_str()
                                    .unwrap()
                                    .into(),
                                schema_digest_sha256: hex(set["arguments"]["schema_digest_sha256"]
                                    .as_str()
                                    .unwrap()),
                            }),
                            progress: Some(v1_1::DynamicProjectionDigest {
                                projection_id: set["progress"]["projection_id"]
                                    .as_str()
                                    .unwrap()
                                    .into(),
                                schema_digest_sha256: hex(set["progress"]["schema_digest_sha256"]
                                    .as_str()
                                    .unwrap()),
                            }),
                            raw_result: Some(v1_1::DynamicProjectionDigest {
                                projection_id: set["raw_result"]["projection_id"]
                                    .as_str()
                                    .unwrap()
                                    .into(),
                                schema_digest_sha256: hex(
                                    set["raw_result"]["schema_digest_sha256"].as_str().unwrap(),
                                ),
                            }),
                        }],
                        ..Default::default()
                    })
                }
                _ => {}
            }
            self.input.lock().unwrap().extend(
                framing::encode(&envelope, NonZeroUsize::new(262144).unwrap())
                    .unwrap()
                    .bytes,
            );
        }
    }
    fn tool_terminal(&self, request: &v1_1::Request) {
        let invoke = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
        let args = wire_value(invoke.arguments.unwrap().value.unwrap());
        let minor = request
            .capability_ref
            .as_ref()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .minor;
        let catalog = echo_catalog();
        let set = &catalog["entries"][0]["selected_minor_schema_sets"][format!("1.{minor}")]["raw_result"];
        let raw = json!({"content":[{"type":"text","text":args["text"]}],"details":{}});
        let value = v1_1::SchemaBoundValue {
            projection_id: set["projection_id"].as_str().unwrap().into(),
            schema_digest_sha256: hex(set["schema_digest_sha256"].as_str().unwrap()),
            value: Some(profiles::json_wire(&raw).unwrap()),
        };
        self.push(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
            request_id: request.request_id,
            terminal: Some(v1_1::stream_end::Terminal::Result(
                v1_1::InvokeToolResult {
                    raw_result: Some(value),
                }
                .encode_to_vec(),
            )),
        }));
    }
    fn tool_request(&self, request: &v1_1::Request) {
        let Some(capability) = self
            .selected
            .iter()
            .find(|profile| profile.profile() == "host-action.v1")
        else {
            self.tool_terminal(request);
            return;
        };
        let minor = request
            .capability_ref
            .as_ref()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .minor;
        let cap = v1_1::CapabilityRef {
            authenticated_extension_id: "peer-a".into(),
            kind: "host-action".into(),
            name: capability.name().into(),
            profile: Some(v1_1::ProfileVersion {
                name: "host-action.v1".into(),
                major: 1,
                minor,
            }),
            schema_digest_sha256: pi_extension_protocol::descriptor::canonical_profile_digest(
                "host-action.v1",
                1,
                minor,
            )
            .unwrap()
            .to_vec(),
            handler_id: Some(capability.handler().into()),
        };
        let item = pi_harness_runtime::extensions::profiles::bound(
            &profiles::Registry::bundled().unwrap(),
            "host.queue_item.v1",
            &json!({"role":"user","content":"actual peer Tool-parent queue","timestamp":7}),
        )
        .unwrap();
        self.push(v1_1::envelope::Frame::Request(v1_1::Request {
            request_id: request.request_id
                + self
                    .results
                    .lock()
                    .unwrap()
                    .get("peer_child_offset")
                    .and_then(|v| v.as_ref())
                    .and_then(Value::as_u64)
                    .unwrap_or(1),
            parent_request_id: Some(request.request_id),
            capability_ref: Some(cap),
            effect_key: None,
            method: "queue.next_run".into(),
            deadline_after_ms: Some(300),
            initial_stream_credit_bytes: Some(0),
            payload: dto::QueueNextRunRequest {
                lane: "main".into(),
                item: Some(item),
            }
            .encode_to_vec(),
        }));
    }
}

impl Peer {
    pub fn release_request(&self, id: u64) {
        self.hold.store(false, Ordering::SeqCst);
        self.hold_tools.store(false, Ordering::SeqCst);
        self.hold_provider.store(false, Ordering::SeqCst);
        let request = self
            .requests
            .lock()
            .unwrap()
            .iter()
            .find(|request| request.request_id == id)
            .unwrap()
            .clone();
        self.terminal(&request);
    }
}

pub fn passive_event_profile() -> profiles::ApprovedProfile {
    profiles::ApprovedProfile::new(profiles::ProfileApproval {
        principal: "peer-a".into(),
        kind: "event-sink".into(),
        name: "events".into(),
        profile: "event-sink.v1".into(),
        handler: "native-events".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 3,
        scope: "allowed".into(),
        permissions: BTreeSet::from([("event.receive".into(), "allowed".into())]),
        exposure: profiles::Exposure::new(BTreeMap::new(), BTreeSet::new()).unwrap(),
    })
    .unwrap()
}
pub fn service_host(core: u32, profiles: Vec<profiles::ApprovedProfile>) -> HostAuthority {
    let limits = v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 262144,
        frame_bytes: 262144,
        aggregate_decoded_bytes: 1048576,
        value_depth: 64,
        value_elements: 16384,
        string_bytes: 262144,
        bytes_length: 262144,
        in_flight_requests: 32,
        blocking_queue_bytes: 1048576,
        progress_queue_bytes: 1048576,
        passive_queue_bytes: 1048576,
        telemetry_queue_bytes: 1048576,
        nesting_depth: 8,
        requests_per_second: 100,
        handshake_ms: 1000,
        cancellation_grace_ms: 20,
        max_stream_credit_bytes: 1048576,
    };
    let permissions = profiles
        .iter()
        .flat_map(|p| {
            p.permissions()
                .iter()
                .map(|(name, scope)| v1_1::PermissionGrant {
                    name: name.clone(),
                    scope: scope.clone(),
                })
        })
        .collect();
    HostAuthority::new("peer-a".into(), 7, core, 1, limits, vec![], permissions)
        .unwrap()
        .with_profiles(profiles)
        .unwrap()
}
pub fn reservations() -> pi_extension_protocol::profile_live::Reservations {
    pi_extension_protocol::profile_live::Reservations {
        blocking_bytes: 1048576,
        control_bytes: 8388608,
        terminal_bytes: 8388608,
        progress_bytes: 1048576,
        passive_bytes: 1048576,
        telemetry_bytes: 1048576,
    }
}
pub fn request(
    id: u64,
    parent: Option<u64>,
    cap: dto::CapabilityRef,
    method: &str,
    payload: Vec<u8>,
    credit: u64,
) -> v1_1::envelope::Frame {
    v1_1::envelope::Frame::Request(v1_1::Request {
        request_id: id,
        parent_request_id: parent,
        capability_ref: Some(cap),
        effect_key: None,
        method: method.into(),
        deadline_after_ms: Some(500),
        initial_stream_credit_bytes: Some(credit),
        payload,
    })
}

pub fn all_actions_profile(resource: bool) -> profiles::ApprovedProfile {
    let scope = if resource { "allowed" } else { "main" };
    let permissions = if resource {
        vec!["resource.read"]
    } else {
        vec![
            "conversation.read",
            "queue.steer",
            "queue.follow_up",
            "queue.next_run",
            "operation.abort",
        ]
    };
    profiles::ApprovedProfile::new(profiles::ProfileApproval {
        principal: "peer-a".into(),
        kind: "host-action".into(),
        name: if resource {
            "actions-resource".into()
        } else {
            "actions".into()
        },
        profile: "host-action.v1".into(),
        handler: "native-actions".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: scope.into(),
        permissions: permissions
            .into_iter()
            .map(|name| (name.into(), scope.into()))
            .collect(),
        exposure: profiles::Exposure::new(BTreeMap::new(), BTreeSet::new()).unwrap(),
    })
    .unwrap()
}

pub mod storage;

pub mod process;

pub fn accepted_profiles() -> Vec<profiles::ApprovedProfile> {
    let corpus: Value = serde_json::from_str(include_str!("profiles-runtime-v1.json")).unwrap();
    let leaves = corpus["exposure"]["open_leaves"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, schema)| {
            (
                key.clone(),
                pi_extension_protocol::jcs::admit_schema(
                    &serde_json::to_vec(schema).unwrap(),
                    Default::default(),
                )
                .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let exposure = profiles::Exposure::new(leaves, BTreeSet::new()).unwrap();
    let permissions = corpus["exposure"]["permission_scopes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            (
                pair[0].as_str().unwrap().to_owned(),
                pair[1].as_str().unwrap().to_owned(),
            )
        })
        .collect::<BTreeSet<_>>();
    let entries = HOOKS
        .into_iter()
        .map(|name| ("hook.v1", name, "main"))
        .chain([
            ("provider.v1", "remote", "main"),
            ("resource.v1", "resources", "allowed"),
            ("host-action.v1", "actions", "main"),
            ("host-action.v1", "actions-resource", "allowed"),
            ("event-sink.v1", "events", "captured_scope"),
            ("telemetry-sink.v1", "metrics", "allowed"),
        ]);
    entries
        .map(|(profile, name, scope)| {
            profiles::ApprovedProfile::new(profiles::ProfileApproval {
                principal: "peer-a".into(),
                kind: profile.trim_end_matches(".v1").into(),
                name: name.into(),
                profile: profile.into(),
                handler: "qualified_handler".into(),
                minors: BTreeSet::from([0, 1]),
                replay: 1,
                execution: if profile == "provider.v1" {
                    2
                } else if profile.ends_with("sink.v1") {
                    3
                } else {
                    1
                },
                scope: scope.into(),
                permissions: permissions
                    .iter()
                    .filter(|(permission, permission_scope)| {
                        permission_scope == scope
                            && match profile {
                                "resource.v1" => permission == "resource.provide",
                                "host-action.v1" => [
                                    "conversation.read",
                                    "resource.read",
                                    "queue.steer",
                                    "queue.follow_up",
                                    "queue.next_run",
                                    "operation.abort",
                                ]
                                .contains(&permission.as_str()),
                                "event-sink.v1" => permission == "event.receive",
                                "telemetry-sink.v1" => permission == "telemetry.receive",
                                _ => false,
                            }
                    })
                    .cloned()
                    .collect(),
                exposure: exposure.clone(),
            })
            .unwrap()
        })
        .collect()
}

// New receipt-lifetime fixtures wrap the unchanged peer and keep its old inputs exact.
#[derive(Clone, Copy, Debug)]
pub enum ReceiptScenario {
    Healthy = 0,
    LaterNotFound = 1,
    InvalidRead = 2,
    DuplicateManifest = 3,
    HoldLaterRead = 4,
    PanicLaterRead = 5,
    ListNotFound = 6,
    TelemetryError = 7,
    TelemetryWrongAck = 8,
}
pub struct ReceiptPeer {
    pub peer: Arc<Peer>,
    pub scenario: std::sync::atomic::AtomicUsize,
}
impl ReceiptPeer {
    pub fn set(&self, scenario: ReceiptScenario) {
        self.scenario.store(scenario as usize, Ordering::SeqCst);
    }
    fn handle(&self, request: &v1_1::Request) -> bool {
        let scenario = self.scenario.load(Ordering::SeqCst);
        let later_read = request.method == "resource.read"
            && dto::ResourceReadRequest::decode(request.payload.as_slice())
                .unwrap()
                .id
                == "template-id";
        if later_read && scenario == ReceiptScenario::PanicLaterRead as usize {
            panic!("actual resource consumer unwind fixture");
        }
        if later_read && scenario == ReceiptScenario::HoldLaterRead as usize {
            self.peer.requests.lock().unwrap().push(request.clone());
            return true;
        }
        let error = later_read && scenario == ReceiptScenario::LaterNotFound as usize
            || request.method == "resource.list"
                && scenario == ReceiptScenario::ListNotFound as usize
            || request.method.starts_with("telemetry.")
                && scenario == ReceiptScenario::TelemetryError as usize;
        let result = if error {
            let code = v1_1::ErrorCode::NotFound;
            Some(v1_1::response::Terminal::Error(v1_1::TypedError {
                code: code as i32,
                safe_message: pi_extension_protocol::profile_live::safe_error(code).into(),
            }))
        } else if later_read && scenario == ReceiptScenario::InvalidRead as usize {
            Some(v1_1::response::Terminal::Result(
                dto::ResourceReadResult {
                    digest_sha256: vec![0; 32],
                    value: Some(dto::resource_read_result::Value::Text("world".into())),
                }
                .encode_to_vec(),
            ))
        } else if request.method == "resource.list"
            && scenario == ReceiptScenario::DuplicateManifest as usize
        {
            let manifest = dto::ResourceManifest {
                id: "skill-id".into(),
                version: "2".into(),
                kind: "skill".into(),
                digest_sha256: hex("486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7"),
                metadata: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        &profiles::Registry::bundled().unwrap(),
                        "resource.metadata:skill",
                        &json!({"name":"remote-skill","description":"Remote native skill","filePath":"/remote/skill.md"}),
                    )
                    .unwrap(),
                ),
            };
            Some(v1_1::response::Terminal::Result(
                dto::ResourceListResult {
                    revision: "revision-2".into(),
                    resources: vec![manifest.clone(), manifest],
                }
                .encode_to_vec(),
            ))
        } else if request.method == "telemetry.emit_batch"
            && scenario == ReceiptScenario::TelemetryWrongAck as usize
        {
            Some(v1_1::response::Terminal::Result(
                dto::TelemetryEmitBatchResult {
                    accepted: 0,
                    dropped: 0,
                }
                .encode_to_vec(),
            ))
        } else {
            None
        };
        if let Some(terminal) = result {
            self.peer.requests.lock().unwrap().push(request.clone());
            self.peer
                .push(v1_1::envelope::Frame::Response(v1_1::Response {
                    request_id: request.request_id,
                    terminal: Some(terminal),
                }));
            true
        } else {
            false
        }
    }
}
impl OrderedByteChannel for ReceiptPeer {
    fn max_read_scratch_bytes(&self) -> usize {
        self.peer.max_read_scratch_bytes()
    }
    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> pi_ai_core::BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        self.peer.read_chunk(buffer)
    }
    fn write_frame(
        &self,
        frame: EncodedFrame,
    ) -> pi_ai_core::BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(async move {
            let mut reader = FrameReader::<v1_1::Envelope>::new(NonZeroUsize::new(262144).unwrap());
            let envelope = reader
                .read_one(&frame.bytes)
                .unwrap()
                .frame
                .unwrap()
                .envelope;
            if let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame
                && self.handle(&request)
            {
                return Ok(());
            }
            self.peer.write_frame(frame).await
        })
    }
    fn close(&self) {
        self.peer.close();
    }
}
pub fn receipt_connection(
    setup: &super::runtime_r1_r4_support::Setup,
    core: u32,
    minor: u32,
    profiles: Vec<profiles::ApprovedProfile>,
) -> (Arc<ProfileConnection>, Arc<ReceiptPeer>) {
    let peer = Arc::new(ReceiptPeer {
        peer: Peer::new(core, minor, profiles.clone()),
        scenario: std::sync::atomic::AtomicUsize::new(0),
    });
    let connection = setup
        .pump
        .drive(ProfileConnection::connect(
            service_host(core, profiles),
            peer.clone(),
            Arc::new(Mono(AtomicU64::new(0))),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(1024).unwrap(),
                write_timeout_ms: NonZeroU64::new(500).unwrap(),
            },
            pi_extension_protocol::profile_live::Reservations {
                blocking_bytes: 1048576,
                control_bytes: 8388608,
                terminal_bytes: 8388608,
                progress_bytes: 1048576,
                passive_bytes: 1048576,
                telemetry_bytes: 1048576,
            },
            10000,
        ))
        .unwrap();
    (connection, peer)
}
