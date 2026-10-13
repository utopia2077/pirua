#![cfg(feature = "extensions")]

use pi_agent::schema::{
    AgentHarness, AgentHarnessTool, AgentLane, AgentMessage, AgentToolResult, Cancellation,
    ErrorContext, HarnessRuntimeError, IdGenerator, SessionCreateOptions, SessionRepo, SessionTree,
    StorageError, ToolExecutionError, ToolProgressSink,
};
use pi_agent_session::{
    context::SummaryRenderer,
    memory::{MemoryStorage, StorageClock},
    repo::MemorySessionRepo,
};
use pi_ai::{
    ApiAdapter, ApiId, AssistantContent, AssistantMessage, AssistantMessageEvent, AssistantStream,
    AssistantStreamInit, BoxFuture, CapabilityError, Clock, DeferredCancelRequest,
    DeferredFetchRequest, DoneReason, JsonValue, Message, Model, ModelBaseUrl, ModelCost, ModelId,
    ModelInput, ModelsError, ModelsRegistry, ModelsRegistryOptions, Provider, ProviderAuth,
    ProviderId, PublicModelBaseUrl, RequestLifecycleAdapter, RequestLifecycleInvocation,
    SafeUnsigned, SimpleStreamRequest, StreamOptionsWire, TaskSpawner, TextStreamRequest, ToolCall,
    ToolCallId, ToolCallType, ToolName, Usage, UserContent, UserMessage,
    create_assistant_message_event_stream,
};
use pi_coding_runtime::{
    CodingExtensionAdapter, CodingHarnessBuilder, CodingSettingsSnapshot, CompactionSettings,
    EchoConflictPolicy, ExecutionEnv, ExtensionRegistration, LocalExecutionEnv, ModelRef,
    QueueMode, Resources, ThinkingLevel,
};
use pi_extension_protocol::{
    activation::{AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs},
    profiles::{ApprovedProfile, Exposure, ProfileApproval},
    wire::{framing, v1_1},
};
use pi_harness_runtime::{
    extensions::{
        profiles::connection::ProfileConnection,
        tool_driver::{EchoRouteTiming, OwnerLimits, SingleToolOwner},
        transport::{
            ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig,
            RetainedOrderedByteChannel,
        },
    },
    owner::RuntimeFactory,
};
use prost::Message as _;
use serde_json::json;
use std::sync::OnceLock;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};

struct Ids(AtomicU64);
impl Default for Ids {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        format!("m5-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct AppClock;
impl Clock for AppClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        cancellation: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<(), pi_ai::Cancelled>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(pi_ai::Cancelled)
            } else {
                Ok(())
            }
        })
    }
}
impl StorageClock for AppClock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(SafeUnsigned::new(7).expect("bounded clock"))
    }
}

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

struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, task: pi_ai::BoxFuture<'static, ()>) -> Result<(), pi_ai::SpawnError> {
        tokio::spawn(task);
        Ok(())
    }
}

struct AuthContext;
impl pi_ai::AuthContext for AuthContext {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<Option<String>, pi_ai::AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<bool, pi_ai::AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}

struct Summary;
impl SummaryRenderer for Summary {
    fn render(
        &self,
        summary: pi_agent_session::context::ContextSummary,
    ) -> pi_agent::schema::BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            Ok(AgentMessage::BuiltIn(Message::User(UserMessage {
                role: Default::default(),
                content: UserContent::Text(summary.text),
                timestamp: summary.timestamp,
            })))
        })
    }
}

struct EchoApiAdapter {
    model: Model,
    responses: Mutex<VecDeque<AssistantMessage>>,
}

impl EchoApiAdapter {
    fn new(model: &Model) -> Arc<Self> {
        let tool_response = AssistantMessage {
            role: Default::default(),
            content: vec![AssistantContent::ToolCall(ToolCall {
                content_type: ToolCallType::ToolCall,
                id: ToolCallId::new("assembled-echo-call").expect("tool call ID"),
                name: ToolName::new("echo").expect("tool name"),
                arguments: serde_json::from_value(json!({"text": "assembled echo"}))
                    .expect("tool arguments"),
                thought_signature: None,
                namespace: None,
            })],
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: Usage::zero(),
            stop_reason: pi_ai::StopReason::ToolUse,
            deferred: None,
            error_message: None,
            raw_stop_reason: None,
            end_turn: None,
            timestamp: SafeUnsigned::new(7).expect("timestamp"),
        };
        let stop_response = AssistantMessage {
            role: Default::default(),
            content: Vec::new(),
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: Usage::zero(),
            stop_reason: pi_ai::StopReason::Stop,
            deferred: None,
            error_message: None,
            raw_stop_reason: None,
            end_turn: None,
            timestamp: SafeUnsigned::new(7).expect("timestamp"),
        };
        Arc::new(Self {
            model: model.clone(),
            responses: Mutex::new(VecDeque::from([tool_response, stop_response])),
        })
    }

    fn start(
        &self,
        clock: Arc<dyn Clock>,
        cancellation: pi_ai::CancellationToken,
    ) -> AssistantStream {
        let response = self
            .responses
            .lock()
            .expect("provider responses")
            .pop_front()
            .unwrap_or_else(|| panic!("fixture provider exhausted"));
        let reason = match response.stop_reason {
            pi_ai::StopReason::ToolUse => DoneReason::ToolUse,
            _ => DoneReason::Stop,
        };
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock,
            cancellation,
        });
        tokio::spawn(async move {
            producer
                .push(AssistantMessageEvent::Done {
                    reason,
                    message: Box::new(response),
                })
                .expect("fixture response");
        });
        stream
    }
}

impl ApiAdapter for EchoApiAdapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }

    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.start(
            request.options.runtime.clock.clone(),
            request.options.runtime.cancellation.clone(),
        )
    }

    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.start(
            request.options.runtime.clock.clone(),
            request.options.runtime.cancellation.clone(),
        )
    }

    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }

    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }

    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
}

impl RequestLifecycleAdapter for EchoApiAdapter {
    fn stream_with_lifecycle(
        &self,
        request: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream(request)
    }

    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream_simple(request)
    }
}

struct FixtureProvider {
    id: ProviderId,
    models: Vec<Model>,
    model_calls: Arc<AtomicU64>,
    adapter: Option<Arc<dyn ApiAdapter>>,
}
impl Provider for FixtureProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn name(&self) -> &str {
        "m5-fixture"
    }
    fn auth(&self) -> &ProviderAuth {
        static AUTH: std::sync::OnceLock<ProviderAuth> = std::sync::OnceLock::new();
        AUTH.get_or_init(ProviderAuth::no_auth)
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.model_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.models.clone().into())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (self
            .adapter
            .as_ref()
            .is_some_and(|adapter| adapter.api_id() == api))
        .then(|| self.adapter.clone().expect("adapter"))
    }
}

fn model(id: &str) -> Model {
    Model {
        id: ModelId::new(id).expect("model ID"),
        name: id.to_owned(),
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("API"),
        provider: ProviderId::new("openai").expect("provider"),
        base_url: ModelBaseUrl::Public(
            PublicModelBaseUrl::new("https://example.invalid").expect("public endpoint"),
        ),
        reasoning: false,
        thinking_level_map: None,
        input: vec![ModelInput::Text],
        cost: ModelCost {
            input: 0.0.try_into().expect("finite"),
            output: 0.0.try_into().expect("finite"),
            cache_read: 0.0.try_into().expect("finite"),
            cache_write: 0.0.try_into().expect("finite"),
            tiers: None,
        },
        context_window: SafeUnsigned::new(1024).expect("context"),
        max_tokens: SafeUnsigned::new(128).expect("max"),
        sampling_params: None,
        headers: None,
        compat: None,
    }
}

fn settings(model: &Model) -> CodingSettingsSnapshot {
    CodingSettingsSnapshot::new(
        ModelRef::new(model.provider.clone(), model.id.clone()),
        ThinkingLevel::Off,
        vec![ToolName::new("read").expect("tool")],
        pi_ai::RetryPolicy {
            enabled: false,
            max_retries: pi_ai::MaxRetries::new(0).expect("retries"),
            base_delay_ms: SafeUnsigned::new(0).expect("delay"),
        },
        CompactionSettings {
            enabled: false,
            reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
            keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
        },
        StreamOptionsWire::default(),
        QueueMode::All,
        QueueMode::All,
    )
}

fn profile(kind: &str, name: &str) -> ApprovedProfile {
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
        exposure: Exposure::new(BTreeMap::new(), BTreeSet::new()).expect("exposure"),
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
}
impl Peer {
    fn new(profiles: &[ApprovedProfile]) -> Arc<Self> {
        Self::new_with_frame_bound(profiles, 8192)
    }

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
        self.push(v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
            request_id: request.request_id,
            sequence: 0,
            payload: v1_1::ToolProgress {
                event: Some(progress),
                percent_complete: None,
            }
            .encode_to_vec(),
        }));
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

    fn push_oversized_stream_end(&self, request_id: u64, payload_bytes: usize) {
        let frame = v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
            request_id,
            terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                code: v1_1::ErrorCode::HandlerError as i32,
                safe_message: "x".repeat(payload_bytes),
            })),
        });
        let encoded = framing::encode(
            &v1_1::Envelope { frame: Some(frame) },
            NonZeroUsize::new(8192).expect("oversized frame bound"),
        )
        .expect("encode oversized frame");
        assert!(encoded.body_len > self.frame_bound);
        self.input.lock().expect("peer input").extend(encoded.bytes);
    }

    fn push(&self, frame: v1_1::envelope::Frame) {
        let encoded = framing::encode(
            &v1_1::Envelope { frame: Some(frame) },
            NonZeroUsize::new(self.frame_bound).expect("frame bound"),
        )
        .expect("encode handshake");
        self.input.lock().expect("peer input").extend(encoded.bytes);
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
        self.writes
            .lock()
            .expect("peer writes")
            .push(frame.bytes.clone());
        let envelope =
            framing::FrameReader::<v1_1::Envelope>::new(NonZeroUsize::new(8192).expect("reader"))
                .read_one(&frame.bytes)
                .expect("host frame")
                .frame
                .expect("host envelope")
                .envelope;
        if let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame
            && request.method == "tool.invoke"
        {
            self.tool_invocations.fetch_add(1, Ordering::SeqCst);
            self.respond_to_tool(&request);
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
        "../../pi-extension-protocol/schema/fixtures/host-catalog.json"
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

struct Fixture {
    model: Model,
    factory: Arc<RuntimeFactory<pi_coding_runtime::ExecutionToolContext, MemoryStorage>>,
    session: Arc<pi_agent_session::repo::MemorySession>,
    registry: Arc<ModelsRegistry>,
    environment: Arc<dyn ExecutionEnv>,
    provider_model_calls: Arc<AtomicU64>,
}

async fn fixture() -> Fixture {
    let clock = Arc::new(AppClock);
    let spawner = Arc::new(Spawner);
    let ids = Arc::new(Ids::default());
    let model = model("m5-model");
    let provider_model_calls = Arc::new(AtomicU64::new(0));
    let adapter = EchoApiAdapter::new(&model);
    let registry = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(pi_ai::InMemoryCredentialStore::new()),
        models_store: Box::new(pi_ai::InMemoryModelsStore::new()),
        auth_context: Arc::new(AuthContext),
        clock: clock.clone(),
        spawner: spawner.clone(),
        model_header_policy: Default::default(),
    }));
    registry
        .set_provider(Arc::new(FixtureProvider {
            id: model.provider.clone(),
            models: vec![model.clone()],
            model_calls: provider_model_calls.clone(),
            adapter: Some(adapter),
        }))
        .await
        .expect("initial model catalog");
    let repo = MemorySessionRepo::new(
        Default::default(),
        Default::default(),
        ids.clone(),
        clock.clone(),
        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
    )
    .expect("session repo");
    let session = repo
        .create(SessionCreateOptions {
            id: Some("m5-session".parse().expect("session ID")),
            ..Default::default()
        })
        .await
        .expect("session");
    let environment: Arc<dyn ExecutionEnv> = Arc::new(LocalExecutionEnv::new(std::env::temp_dir()));
    let factory = Arc::new(RuntimeFactory::new(
        clock,
        spawner,
        ids,
        Arc::new(pi_coding_runtime::ExecutionToolContext {
            env: environment.clone(),
        }),
        Arc::new(Summary),
    ));
    Fixture {
        model,
        factory,
        session,
        registry,
        environment,
        provider_model_calls,
    }
}

fn host(profiles: Vec<ApprovedProfile>) -> HostAuthority {
    host_with_tool_generation_and_frame(profiles, 7, None, 8192)
}

fn host_with_tool_generation(
    profiles: Vec<ApprovedProfile>,
    generation: u64,
    tool: Option<ApprovedTool>,
) -> HostAuthority {
    host_with_tool_generation_and_frame(profiles, generation, tool, 8192)
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
            aggregate_decoded_bytes: 8192,
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

async fn coding_harness(
    fixture: &Fixture,
) -> pi_coding_runtime::CodingHarness<pi_agent_session::memory::MemoryStorage> {
    CodingHarnessBuilder::new(
        fixture.factory.clone(),
        fixture.session.clone(),
        fixture.registry.clone(),
        fixture.environment.clone(),
        Resources::default(),
        settings(&fixture.model),
    )
    .first_attachment()
    .build()
    .await
    .expect("coding harness")
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

#[test]
fn assembled_missing_grant_refuses_before_harness_registration() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let provider_calls_before = fixture.provider_model_calls.load(Ordering::SeqCst);
            let stats_before = fixture.session.get_stats().await.expect("stats before");
            let leaf_before = fixture.session.get_leaf_id().await.expect("leaf before");
            let peer = Peer::new(&[]);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(vec![]),
                    channel: peer.clone(),
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let missing = v1_1::CapabilityRef {
                authenticated_extension_id: "peer-a".into(),
                kind: "hook".into(),
                name: "before_run".into(),
                profile: Some(v1_1::ProfileVersion {
                    name: "hook.v1".into(),
                    major: 1,
                    minor: 0,
                }),
                handler_id: Some("missing".into()),
                ..Default::default()
            };
            let result = harness
                .attach_extensions(adapter, vec![ExtensionRegistration::hook(missing)])
                .await;
            assert!(matches!(
                result,
                Err(pi_coding_runtime::CodingExtensionError::MissingGrant { .. })
            ));
            assert_eq!(
                harness
                    .harness()
                    .registered_hook_count(pi_agent::schema::HookName::BeforeRun),
                0
            );
            assert_eq!(
                fixture.provider_model_calls.load(Ordering::SeqCst),
                provider_calls_before,
                "missing grant must not call the provider catalog"
            );
            assert_eq!(
                fixture.session.get_stats().await.expect("stats after"),
                stats_before,
                "missing grant must not publish a session event"
            );
            assert_eq!(
                fixture.session.get_leaf_id().await.expect("leaf after"),
                leaf_before,
                "missing grant must not add a durable entry"
            );
            assert_eq!(
                peer.tool_invocations(),
                0,
                "missing grant must not issue an assembled tool request"
            );
            assert!(peer.writes.lock().expect("writes").len() >= 2);
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_ordered_provider_and_hook_registration_is_deterministic() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run"), profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap_hook = capability(adapter.connection(), "hook", "before_run");
            let cap_provider = capability(adapter.connection(), "provider", "openai");
            let mut alternate = fixture.model.clone();
            alternate.id = pi_ai::ModelId::new("m5-selected").expect("selected model");
            let attachment = harness
                .attach_extensions(
                    adapter,
                    vec![
                        ExtensionRegistration::provider(
                            cap_provider,
                            vec![fixture.model.clone(), alternate.clone()],
                        ),
                        ExtensionRegistration::hook(cap_hook),
                    ],
                )
                .await
                .expect("attach ordered profiles");
            assert_eq!(
                attachment.registration_order(),
                &[
                    pi_coding_runtime::ExtensionRegistrationKind::Provider,
                    pi_coding_runtime::ExtensionRegistrationKind::Hook
                ]
            );
            assert_eq!(
                harness
                    .harness()
                    .registered_hook_count(pi_agent::schema::HookName::BeforeRun),
                1
            );
            assert!(
                fixture
                    .registry
                    .get_provider(&fixture.model.provider)
                    .expect("provider")
                    .auth()
                    .is_no_auth()
            );
            harness
                .switch_model(alternate.provider.clone(), alternate.id.clone())
                .await
                .expect("select catalog model");
            assert_eq!(
                harness
                    .harness()
                    .get_model()
                    .await
                    .expect("model")
                    .expect("selected")
                    .id,
                alternate.id
            );
            attachment.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_late_hook_refusal_rolls_back_prior_provider_mutation() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run"), profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let connection = adapter.connection().clone();
            let hook = capability(&connection, "hook", "before_run");
            let provider = capability(&connection, "provider", "openai");
            let first = harness
                .attach_extensions(adapter, vec![ExtensionRegistration::hook(hook.clone())])
                .await
                .expect("first hook");
            let mut alternate = fixture.model.clone();
            alternate.id = pi_ai::ModelId::new("m5-alternate").expect("alternate model");
            let result = harness
                .attach_extensions(
                    CodingExtensionAdapter::from_connection(connection),
                    vec![
                        ExtensionRegistration::provider(provider, vec![alternate]),
                        ExtensionRegistration::hook(hook),
                    ],
                )
                .await;
            assert!(matches!(
                result,
                Err(pi_coding_runtime::CodingExtensionError::Hook(_))
            ));
            let restored = fixture.registry.get_models(Some(&fixture.model.provider));
            assert_eq!(restored.len(), 1);
            assert_eq!(restored[0].id, fixture.model.id);
            first.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_provider_auth_and_runtime_endpoint_inputs_are_refused_without_side_effects() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap = capability(adapter.connection(), "provider", "openai");
            let mut invalid = fixture.model.clone();
            invalid.headers = Some(Default::default());
            let result = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::provider(cap, vec![invalid])],
                )
                .await;
            assert!(matches!(
                result,
                Err(pi_coding_runtime::CodingExtensionError::InvalidRegistration(_))
            ));
            assert_eq!(fixture.registry.get_models(None).len(), 1);
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_tool_refuses_without_bounded_echo_route_before_registration() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            assert_eq!(
                CodingExtensionAdapter::echo_conflict_policy(),
                EchoConflictPolicy::RejectExisting
            );
            let peer = Peer::new(&[]);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(vec![]),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let result = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::echo_tool(
                        pi_coding_runtime::create_tools()
                            .into_iter()
                            .next()
                            .expect("coding tool"),
                        EchoRouteTiming {
                            method_limit_ms: NonZeroU64::new(1000).expect("method limit"),
                            connection_deadline_ms: 10_000,
                        },
                    )],
                )
                .await;
            assert!(matches!(
                result,
                Err(
                    pi_coding_runtime::CodingExtensionError::InvalidRegistration(
                        "echo registration requires a tool connection"
                    )
                )
            ));
            assert_eq!(
                harness
                    .harness()
                    .registered_hook_count(pi_agent::schema::HookName::BeforeRun),
                0
            );
            harness.harness().close().await.expect("close harness");
        });
}

struct ToolEventLog(Mutex<Vec<pi_agent::schema::HarnessEvent>>);
impl pi_agent::schema::EventListener for ToolEventLog {
    fn on_event(
        &self,
        event: pi_agent::schema::HarnessEvent,
    ) -> BoxFuture<'_, Result<(), pi_agent::schema::HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
}

#[test]
fn assembled_tool_route_executes_through_coding_harness_with_bounded_progress_and_terminal() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run")];
            let peer = Peer::new_tool(&selected);
            let owner = SingleToolOwner::new(
                Arc::new(Spawner),
                OwnerLimits {
                    outbound_bytes: NonZeroUsize::new(8192).expect("outbound"),
                    receipt_bytes: NonZeroUsize::new(8192).expect("receipt"),
                    write_timeout_ms: NonZeroU64::new(1000).expect("timeout"),
                },
            );
            let adapter = harness
                .connect_extension_tool_retained(pi_coding_runtime::ExtensionRetainedToolInput {
                    host: host_with_tool_generation(selected, 7, Some(echo_approval())),
                    owner,
                    channel: peer.clone(),
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                })
                .await
                .expect("ready tool");
            let attachment = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::echo_tool(
                        Arc::new(EchoTool),
                        EchoRouteTiming {
                            method_limit_ms: NonZeroU64::new(1000).expect("method limit"),
                            connection_deadline_ms: 10_000,
                        },
                    )],
                )
                .await
                .expect("attach echo");
            harness
                .harness()
                .set_active_tools(vec![ToolName::new("echo").expect("tool name")])
                .await
                .expect("activate echo");
            let events = Arc::new(ToolEventLog(Mutex::new(Vec::new())));
            let _subscriptions: Vec<_> = [
                pi_agent::schema::HarnessEventType::ToolStart,
                pi_agent::schema::HarnessEventType::ToolUpdate,
                pi_agent::schema::HarnessEventType::ToolEnd,
            ].into_iter().map(|kind| harness.harness().events().on(kind, events.clone())).collect();
            let run = harness
                .harness()
                .prompt(pi_agent::schema::PromptInput::Text {
                    text: "invoke assembled echo".into(),
                    images: vec![],
                })
                .await
                .expect("run admission")
                .expect("run completion");
            assert!(matches!(
                run.outcome,
                pi_agent::schema::RunOutcome::CompletedWithAssistant(_)
            ));
            {
                use pi_agent::schema::HarnessEvent;
                let events = events.0.lock().unwrap();
                let [HarnessEvent::ToolStart(start), HarnessEvent::ToolUpdate(update), HarnessEvent::ToolEnd(end)] = events.as_slice() else {
                    panic!("expected real ToolStart → ToolUpdate → ToolEnd, received {events:?}");
                };
                assert_eq!(start.tool_call_id.as_str(), "assembled-echo-call");
                assert_eq!(update.tool_call_id, start.tool_call_id);
                assert_eq!(end.tool_call_id, start.tool_call_id);
                assert_eq!(update.tool_name, start.tool_name);
                assert_eq!(end.tool_name, start.tool_name);
                assert_eq!(update.lane, start.lane);
                assert_eq!(end.lane, start.lane);
                assert_eq!(update.run_id, start.run_id);
                assert_eq!(end.run_id, start.run_id);
                assert_eq!(update.turn_id, start.turn_id);
                assert_eq!(end.turn_id, start.turn_id);
                assert_eq!(serde_json::to_value(&update.partial_result).unwrap(), json!({
                    "content": [{"type": "text", "text": format!("progress: {}", serde_json::to_value(&start.args).unwrap()["text"]) }],
                    "details": {}
                }));
            }
            assert_eq!(peer.tool_invocations(), 1);
            assert_eq!(peer.progress_items(), 1);
            assert_eq!(peer.terminal_items(), 1);
            assert!(attachment.connection().current());
            assert!(
                peer.max_frame_bytes() <= 8192,
                "assembled route exceeded the negotiated frame bound"
            );
            attachment.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_tool_refuses_peer_frame_above_negotiated_ready_bound() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run")];
            let peer = Peer::new_tool_with_frame_bound(&selected, 2048);
            let adapter = harness
                .connect_extension_tool_retained(pi_coding_runtime::ExtensionRetainedToolInput {
                    host: host_with_tool_generation_and_frame(
                        selected,
                        7,
                        Some(echo_approval()),
                        2048,
                    ),
                    owner: SingleToolOwner::new(
                        Arc::new(Spawner),
                        OwnerLimits {
                            outbound_bytes: NonZeroUsize::new(8192).expect("outbound"),
                            receipt_bytes: NonZeroUsize::new(8192).expect("receipt"),
                            write_timeout_ms: NonZeroU64::new(1000).expect("timeout"),
                        },
                    ),
                    channel: peer.clone(),
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                })
                .await
                .expect("ready bounded tool");
            let connection = adapter.connection().clone();
            assert_eq!(
                connection
                    .grant()
                    .wire()
                    .limits
                    .as_ref()
                    .expect("limits")
                    .frame_bytes,
                2048
            );
            peer.push_oversized_stream_end(1, 3000);
            for _ in 0..1000 {
                if peer.is_closed() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                peer.is_closed(),
                "oversized peer frame must close the retained channel"
            );
            assert_eq!(
                peer.tool_invocations(),
                0,
                "bounded refusal must precede any assembled tool invocation"
            );
            assert_eq!(peer.terminal_items(), 0);
            connection.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_tool_conflict_rejects_second_live_attachment() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("hook", "before_run")];
            let first_owner = SingleToolOwner::new(
                Arc::new(Spawner),
                OwnerLimits {
                    outbound_bytes: NonZeroUsize::new(8192).expect("outbound"),
                    receipt_bytes: NonZeroUsize::new(8192).expect("receipt"),
                    write_timeout_ms: NonZeroU64::new(1000).expect("timeout"),
                },
            );
            let second_owner = SingleToolOwner::new(
                Arc::new(Spawner),
                OwnerLimits {
                    outbound_bytes: NonZeroUsize::new(8192).expect("outbound"),
                    receipt_bytes: NonZeroUsize::new(8192).expect("receipt"),
                    write_timeout_ms: NonZeroU64::new(1000).expect("timeout"),
                },
            );
            let first_peer = Peer::new_tool(&selected);
            let first = harness
                .connect_extension_tool(pi_coding_runtime::ExtensionToolInput {
                    host: host_with_tool_generation(selected.clone(), 7, Some(echo_approval())),
                    owner: first_owner,
                    channel: first_peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                })
                .await
                .expect("first ready tool");
            let first_attachment = harness
                .attach_extensions(
                    first,
                    vec![ExtensionRegistration::echo_tool(
                        Arc::new(EchoTool),
                        EchoRouteTiming {
                            method_limit_ms: NonZeroU64::new(1000).expect("method limit"),
                            connection_deadline_ms: 10_000,
                        },
                    )],
                )
                .await
                .expect("first echo attachment");
            let first_connection = first_attachment.connection().clone();
            assert!(first_connection.current());

            let second_peer = Peer::new_tool(&selected);
            let second = harness
                .connect_extension_tool(pi_coding_runtime::ExtensionToolInput {
                    host: host_with_tool_generation(selected, 8, Some(echo_approval())),
                    owner: second_owner,
                    channel: second_peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                })
                .await
                .expect("second ready tool");
            let second_connection = second.connection().clone();
            let result = harness
                .attach_extensions(
                    second,
                    vec![ExtensionRegistration::echo_tool(
                        Arc::new(EchoTool),
                        EchoRouteTiming {
                            method_limit_ms: NonZeroU64::new(1000).expect("method limit"),
                            connection_deadline_ms: 10_000,
                        },
                    )],
                )
                .await;
            assert!(matches!(
                result,
                Err(
                    pi_coding_runtime::CodingExtensionError::InvalidRegistration(
                        "echo tool conflict: existing attachment wins"
                    )
                )
            ));
            assert!(
                first_connection.current(),
                "second independent owner must not replace generation 7"
            );
            assert!(second_connection.current());
            first_attachment.close();
            second_connection.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_close_drops_generation_and_preserves_no_live_binding() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap = capability(adapter.connection(), "provider", "openai");
            let attachment = harness
                .attach_extensions(
                    adapter,
                    vec![ExtensionRegistration::provider(
                        cap,
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("attach");
            assert!(attachment.connection().current());
            let connection = attachment.connection().clone();
            attachment.close();
            assert!(!connection.current());
            assert_eq!(connection.grant().wire().connection_generation, 7);
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn assembled_close_allows_replacement_generation_without_inheriting_binding() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let first_peer = Peer::new(&selected);
            let first = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected.clone()),
                    channel: first_peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("first service");
            let first_connection = first.connection().clone();
            let first_attachment = harness
                .attach_extensions(
                    first,
                    vec![ExtensionRegistration::provider(
                        capability(&first_connection, "provider", "openai"),
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("first attachment");
            first_attachment.close();
            assert!(!first_connection.current());

            let second_peer = Peer::new(&selected);
            let second = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: second_peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("replacement service");
            let second_connection = second.connection().clone();
            assert!(second_connection.current());
            assert!(
                !first_connection
                    .connection_identity()
                    .same_connection(&second_connection.connection_identity())
            );
            let second_attachment = harness
                .attach_extensions(
                    second,
                    vec![ExtensionRegistration::provider(
                        capability(&second_connection, "provider", "openai"),
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("replacement attachment");
            second_attachment.close();
            harness.harness().close().await.expect("close harness");
        });
}

#[test]
fn mutation_restoration_witness_rejects_ungranted_provider_then_restores_positive_path() {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let fixture = fixture().await;
            let harness = coding_harness(&fixture).await;
            let selected = vec![profile("provider", "openai")];
            let peer = Peer::new(&selected);
            let adapter = CodingExtensionAdapter::connect_service(
                &harness,
                pi_coding_runtime::ExtensionServiceInput {
                    host: host(selected),
                    channel: peer,
                    clock: Arc::new(Mono(AtomicU64::new(0))),
                    config: config(),
                    reservations: reservation(),
                    remaining_ms: 10_000,
                },
            )
            .await
            .expect("ready service");
            let cap = capability(adapter.connection(), "provider", "openai");
            let mut mutated = cap.clone();
            mutated.name = "missing".into();
            let rejected = harness
                .attach_extensions(
                    CodingExtensionAdapter::from_connection(adapter.connection().clone()),
                    vec![ExtensionRegistration::provider(
                        mutated,
                        vec![fixture.model.clone()],
                    )],
                )
                .await;
            assert!(matches!(
                rejected,
                Err(pi_coding_runtime::CodingExtensionError::MissingGrant { .. })
            ));
            let restored = harness
                .attach_extensions(
                    CodingExtensionAdapter::from_connection(adapter.connection().clone()),
                    vec![ExtensionRegistration::provider(
                        cap,
                        vec![fixture.model.clone()],
                    )],
                )
                .await
                .expect("restored grant");
            assert_eq!(restored.registration_order().len(), 1);
            restored.close();
            harness.harness().close().await.expect("close harness");
        });
}
