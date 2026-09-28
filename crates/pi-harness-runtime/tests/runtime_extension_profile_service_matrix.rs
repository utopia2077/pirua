#![cfg(feature = "extensions")]
mod runtime_extension_profiles_support;
mod runtime_r1_r4_support;

use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    session::RuntimeSession,
    storage::{AtomicStorage, RuntimeStorage},
};
use pi_extension_protocol::{profiles::wire_v1_1 as dto, wire::v1_1};
use pi_harness_runtime::{
    extensions::{
        profiles::{hooks::RemoteHook, resources::ResourceCatalog, telemetry::ProjectedTelemetry},
        transport::PumpConfig,
    },
    owner::{RuntimeFactory, RuntimeHarness},
};
use prost::Message;
use runtime_extension_profiles_support::*;
use runtime_r1_r4_support::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, atomic::Ordering},
};

fn keyed_payload_schema() -> pi_extension_protocol::jcs::PortableSchema {
    let tool = selected_tool_wire();
    let schema = serde_json::json!({
        "type":"object",
        "properties":{
            "model":{"type":"object"},
            "context":{"type":"object"},
            "tools":{"type":"array","items":{"type":"object","const":tool},"maxItems":1},
            "options":{"type":"object"}
        },
        "required":["model","context","tools","options"],
        "additionalProperties":false
    });
    pi_extension_protocol::jcs::admit_schema(
        &serde_json::to_vec(&schema).unwrap(),
        Default::default(),
    )
    .unwrap()
}

fn selected_tool_descriptor() -> AgentTool {
    AgentTool {
        name: pi_ai_core::ToolName::new("matrix-tool").unwrap(),
        label: "matrix".into(),
        description: "native matrix tool".into(),
        parameters: serde_json::from_value(
            serde_json::json!({"type":"object","properties":{},"additionalProperties":false}),
        )
        .unwrap(),
        constrained_sampling: None,
        execution_mode: None,
        replay: Some(ToolReplay::Safe),
    }
}

fn selected_tool_wire() -> serde_json::Value {
    let descriptor = selected_tool_descriptor();
    serde_json::to_value(pi_ai_core::Tool {
        name: descriptor.name,
        description: descriptor.description,
        parameters: descriptor.parameters,
        constrained_sampling: descriptor.constrained_sampling,
    })
    .unwrap()
}

fn selected_provider_profile(
    model: &pi_ai_core::Model,
) -> pi_extension_protocol::profiles::ApprovedProfile {
    use pi_extension_protocol::profiles::{ApprovedProfile, Exposure, ProfileApproval};
    let empty = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{},"additionalProperties":false}"#,
        Default::default(),
    )
    .unwrap();
    let null = pi_extension_protocol::jcs::admit_schema(br#"{"type":"null"}"#, Default::default())
        .unwrap();
    let tool = selected_tool_wire();
    let parameters = pi_extension_protocol::jcs::admit_schema(
        &serde_json::to_vec(&exact_schema(&tool["parameters"])).unwrap(),
        Default::default(),
    )
    .unwrap();
    let mut leaves = BTreeMap::from([
        ("provider.tools.v1[].parameters".into(), parameters),
        (
            "provider.assistant_message.v1.content[].arguments".into(),
            empty.clone(),
        ),
        (
            "provider.assistant_event.v1.toolCall.arguments".into(),
            empty.clone(),
        ),
        (
            "provider.context.v1.messages[].content[].arguments".into(),
            empty,
        ),
        ("provider.context.v1.messages[].details".into(), null),
    ]);
    let raw_model = serde_json::to_value(model).unwrap();
    for part in ["compat", "samplingParams"] {
        leaves.insert(
            format!("provider.model.v1.{part}"),
            pi_extension_protocol::jcs::admit_schema(
                &serde_json::to_vec(&exact_schema(&raw_model[part])).unwrap(),
                Default::default(),
            )
            .unwrap(),
        );
    }
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: "provider".into(),
        name: model.provider.as_str().into(),
        profile: "provider.v1".into(),
        handler: "remote-handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 2,
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(leaves, BTreeSet::new()).unwrap(),
    })
    .unwrap()
}

struct MatrixTool {
    descriptor: AgentTool,
    calls: std::sync::atomic::AtomicUsize,
}
impl AgentHarnessTool<()> for MatrixTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(
        &self,
        args: pi_ai_core::JsonValue,
    ) -> Result<pi_ai_core::JsonObject, ToolExecutionError> {
        let pi_ai_core::JsonValue::Object(args) = args else {
            panic!("native tool arguments object")
        };
        Ok(args)
    }
    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        args: pi_ai_core::JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a (),
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            assert!(args.as_map().is_empty());
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(AgentToolResult {
                content: vec![],
                details: pi_ai_core::JsonValue::Null,
                usage: None,
                added_tool_names: None,
                terminate: None,
            })
        })
    }
}

fn keyed_before_payload_profile(
    model: &pi_ai_core::Model,
) -> pi_extension_protocol::profiles::ApprovedProfile {
    use pi_extension_protocol::profiles::{ApprovedProfile, Exposure, ProfileApproval};
    let payload = keyed_payload_schema();
    let mut leaves = BTreeMap::from([
        (
            "hook.input:before_payload.event.payload".into(),
            payload.clone(),
        ),
        ("hook.output:before_payload.result.payload".into(), payload),
    ]);
    let model = serde_json::to_value(model).unwrap();
    for part in ["compat", "samplingParams"] {
        let schema = pi_extension_protocol::jcs::admit_schema(
            &serde_json::to_vec(&exact_schema(&model[part])).unwrap(),
            Default::default(),
        )
        .unwrap();
        leaves.insert(
            format!("hook.input:before_payload.event.model.{part}"),
            schema,
        );
    }
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: "hook".into(),
        name: "before_payload".into(),
        profile: "hook.v1".into(),
        handler: "remote-handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(leaves, BTreeSet::new()).unwrap(),
    })
    .unwrap()
}

fn selected_after_response_profile() -> pi_extension_protocol::profiles::ApprovedProfile {
    use pi_extension_protocol::profiles::{ApprovedProfile, Exposure, ProfileApproval};
    let arguments = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{},"additionalProperties":false}"#,
        Default::default(),
    )
    .unwrap();
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: "hook".into(),
        name: "after_response".into(),
        profile: "hook.v1".into(),
        handler: "remote-handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(
            BTreeMap::from([(
                "hook.input:after_response.event.message.content[].arguments".into(),
                arguments,
            )]),
            BTreeSet::new(),
        )
        .unwrap(),
    })
    .unwrap()
}

fn selected_transform_context_profile() -> pi_extension_protocol::profiles::ApprovedProfile {
    use pi_extension_protocol::profiles::{ApprovedProfile, Exposure, ProfileApproval};
    let arguments = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{},"additionalProperties":false}"#,
        Default::default(),
    )
    .unwrap();
    let details =
        pi_extension_protocol::jcs::admit_schema(br#"{"type":"null"}"#, Default::default())
            .unwrap();
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: "hook".into(),
        name: "transform_context".into(),
        profile: "hook.v1".into(),
        handler: "remote-handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: "main".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(
            BTreeMap::from([
                (
                    "hook.input:transform_context.event.messages[].content[].arguments".into(),
                    arguments,
                ),
                (
                    "hook.input:transform_context.event.messages[].details".into(),
                    details,
                ),
            ]),
            BTreeSet::new(),
        )
        .unwrap(),
    })
    .unwrap()
}

fn sqlite_case(
    setup: &Setup,
) -> (
    Arc<RuntimeSession<pi_agent_sqlite::SqliteStorage>>,
    Arc<pi_agent_sqlite::SqliteStorage>,
    std::path::PathBuf,
) {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "runtime-service-matrix-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir(&dir).unwrap();
    let codec = Arc::new(
        SessionCodec::new(
            Default::default(),
            ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
        )
        .unwrap(),
    );
    let store = pi_agent_sqlite::SqliteStorage::create_from_snapshot(
        dir.join("session.db"),
        setup.session.metadata(),
        setup.pump.drive(setup.store.snapshot()).unwrap(),
        codec.clone(),
        setup.time.clone(),
        Default::default(),
    )
    .unwrap();
    let session = Arc::new(
        setup
            .pump
            .drive(RuntimeSession::new(
                store.clone(),
                setup.session.metadata(),
                codec,
                setup.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    (session, store, dir)
}

fn response(peer: &Peer, id: u64, setup: &Setup) -> v1_1::Response {
    for _ in 0..100 {
        setup.pump.tick();
        if let Some(value) = peer
            .responses
            .lock()
            .unwrap()
            .iter()
            .find(|response| response.request_id == id)
            .cloned()
        {
            return value;
        }
    }
    panic!("service did not respond to request {id}")
}

fn run<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    let sink =
        ProjectedTelemetry::new(Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT), 1048576, 256)
            .unwrap();
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        setup.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = setup.options_for::<(), _>(session, DriveMode::Automatic);
    let tool = Arc::new(MatrixTool {
        descriptor: selected_tool_descriptor(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    options.tools = vec![tool.clone()];
    options.tool_context = Some(AgentHarnessToolContextSource::Static(()));
    options.telemetry_context = Arc::new(sink.clone());
    let harness: Arc<RuntimeHarness<(), S, SessionMetadata>> = setup
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let profiles = HOOKS
        .iter()
        .map(|name| {
            if *name == "before_payload" {
                keyed_before_payload_profile(&setup.faux.model)
            } else if *name == "after_response" {
                selected_after_response_profile()
            } else if *name == "transform_context" {
                selected_transform_context_profile()
            } else {
                profile("hook.v1", name)
            }
        })
        .chain([
            selected_provider_profile(&setup.faux.model),
            profile("resource.v1", "resources"),
            all_actions_profile(false),
            all_actions_profile(true),
            passive_event_profile(),
            profile("telemetry-sink.v1", "telemetry"),
        ])
        .collect::<Vec<_>>();
    let peer = Peer::new(core, minor, profiles.clone());
    let clock = Arc::new(Mono(std::sync::atomic::AtomicU64::new(0)));
    let connection = setup
        .pump
        .drive(harness.connect_profile_service(
            service_host(core, profiles),
            peer.clone(),
            clock.clone(),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(1024).unwrap(),
                write_timeout_ms: NonZeroU64::new(500).unwrap(),
            },
            reservations(),
            10000,
        ))
        .unwrap();
    let cap = |name: &str| {
        connection
            .grant()
            .profiles()
            .iter()
            .find(|profile| profile.reference().name == name)
            .unwrap()
            .reference()
            .clone()
    };
    for name in HOOKS {
        let grant = cap(name);
        assert_eq!(grant.handler_id.as_deref(), Some("remote-handler"));
        harness
            .register_profile_hook(
                RemoteHook::new(connection.clone(), grant, Default::default()).unwrap(),
            )
            .unwrap();
    }
    setup
        .pump
        .drive(harness.register_profile_provider(
            connection.clone(),
            cap(setup.faux.model.provider.as_str()),
            vec![setup.faux.model.clone()],
            Default::default(),
            Some(keyed_payload_schema()),
        ))
        .unwrap();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let captured = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap("resources"), &catalog))
        .unwrap();
    assert_eq!(captured.get("skill-id", None).unwrap().value, "hello");

    // A held provider call leaves a genuine active parent while the service handles
    // peer actions and passive delivery on this same connection.
    peer.hold_provider.store(true, Ordering::SeqCst);
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "one selected service".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let parent = (0..100)
        .find_map(|_| {
            setup.pump.tick();
            if let std::task::Poll::Ready(result) = poll(prompt.as_mut()) {
                panic!("prompt ended before provider: {result:?}; requests={:?}; responses={:?}; closed={}; occupancy={}", peer.requests.lock().unwrap().iter().map(|r| r.method.clone()).collect::<Vec<_>>(), peer.responses.lock().unwrap().iter().map(|r| (r.request_id, r.terminal.clone())).collect::<Vec<_>>(), peer.closed.load(Ordering::SeqCst), connection.occupancy());
            }
            peer.requests
                .lock()
                .unwrap()
                .iter()
                .find(|request| request.method == "provider.invoke")
                .cloned()
        })
        .unwrap_or_else(|| panic!("real selected provider parent; requests={:?}", peer.requests.lock().unwrap().iter().map(|r| r.method.clone()).collect::<Vec<_>>()));
    let key = parent.effect_key.as_ref().unwrap();
    assert_eq!(key.session_id, "runtime-session");
    assert_eq!(key.effect_kind, "assistant");
    let original = setup.pump.drive(store.snapshot()).unwrap();
    peer.push(request(
        100,
        Some(parent.request_id),
        cap("actions"),
        "snapshot.get",
        dto::SnapshotGetRequest {
            lane: "main".into(),
            visibility: "captured_policy".into(),
        }
        .encode_to_vec(),
        0,
    ));
    let Some(v1_1::response::Terminal::Result(bytes)) = response(&peer, 100, setup).terminal else {
        panic!("selected host action did not succeed")
    };
    let snapshot = dto::SnapshotGetResult::decode(bytes.as_slice()).unwrap();
    assert_eq!(
        wire_value(snapshot.filtered_snapshot.unwrap().value.unwrap())["operation"]["id"],
        key.operation_id
    );
    let denied_payload = dto::QueueNextRunRequest {
        lane: "main".into(),
        item: Some(
            pi_harness_runtime::extensions::profiles::bound(
                &pi_extension_protocol::profiles::Registry::bundled().unwrap(),
                "host.queue_item.v1",
                &serde_json::json!({"role":"user","content":"must not enqueue","timestamp":7}),
            )
            .unwrap(),
        ),
    }
    .encode_to_vec();
    let mut wrong_cap = cap("actions");
    wrong_cap.name = "not-granted".into();
    for (id, parent_id, grant, expected) in [
        (
            101,
            Some(parent.request_id),
            wrong_cap,
            v1_1::ErrorCode::Forbidden,
        ),
        (
            102,
            Some(9999),
            cap("actions"),
            v1_1::ErrorCode::InvalidParent,
        ),
    ] {
        peer.push(request(
            id,
            parent_id,
            grant,
            "queue.next_run",
            denied_payload.clone(),
            0,
        ));
        assert!(
            matches!(response(&peer, id, setup).terminal, Some(v1_1::response::Terminal::Error(error)) if error.code == expected as i32)
        );
        assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), original);
    }
    peer.push(request(
        103,
        None,
        cap("events"),
        "event.subscribe",
        dto::SubscribeRequest {
            scope: "allowed".into(),
        }
        .encode_to_vec(),
        0,
    ));
    for _ in 0..30 {
        setup.pump.tick();
    }
    assert!(peer.items.lock().unwrap().is_empty());
    peer.push(v1_1::envelope::Frame::Credit(v1_1::Credit {
        request_id: 103,
        additional_bytes: 1048576,
    }));
    for _ in 0..100 {
        setup.pump.tick();
        if !peer.items.lock().unwrap().is_empty() {
            break;
        }
    }
    assert_eq!(peer.items.lock().unwrap()[0].sequence, 0);
    peer.release_request(parent.request_id);
    let finished = setup.pump.drive(prompt).unwrap().unwrap();
    assert!(matches!(
        finished.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(finished.run_id.as_str(), key.operation_id);
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap().usage.len(), 1);
    clock.0.store(500, Ordering::SeqCst);
    for _ in 0..100 {
        setup.pump.tick();
    }
    assert!(
        peer.ends
            .lock()
            .unwrap()
            .iter()
            .any(|end| end.request_id == 103)
    );
    assert!(setup.pump.drive(harness.deliver_profile_telemetry(
        connection.clone(),
        cap("telemetry"),
        &sink
    )));
    assert!(
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.method == "telemetry.emit_batch")
    );

    peer.results
        .lock()
        .unwrap()
        .insert("provider_mode".into(), Some(serde_json::json!("deferred")));
    let suspended = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "deferred on same service".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(suspended.outcome, RunOutcome::Deferred(_)));
    assert!(matches!(
        setup.pump.drive(harness.resume()).unwrap().unwrap(),
        ResumeOutcome::RunCompletedWithAssistant(_)
    ));
    peer.results.lock().unwrap().remove("provider_mode");
    let before_compact = setup.pump.drive(store.snapshot()).unwrap();
    let target = before_compact.entries.iter().find_map(|entry| match entry {
        Entry::Message(entry) => Some(entry.id.clone()),
        _ => None,
    });
    assert!(matches!(
        setup
            .pump
            .drive(harness.compact(CompactOptions::default()))
            .unwrap()
            .unwrap()
            .outcome,
        CompactionOutcome::Completed(_)
    ));
    assert!(matches!(
        setup
            .pump
            .drive(harness.navigate_tree(
                target,
                NavigateOptions {
                    summarize: Some(true),
                    ..Default::default()
                }
            ))
            .unwrap()
            .unwrap()
            .outcome,
        NavigationOutcome::Completed(_)
    ));
    let methods = peer
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| request.method.clone())
        .collect::<Vec<_>>();
    let requests = peer.requests.lock().unwrap();
    let payload_at = requests
        .iter()
        .position(|request| request.method == "before_payload")
        .unwrap();
    let provider_at = requests
        .iter()
        .position(|request| request.method == "provider.invoke")
        .unwrap();
    assert!(payload_at < provider_at);
    let first_payload = &requests[payload_at];
    assert_eq!(
        first_payload.capability_ref.as_ref(),
        Some(&cap("before_payload"))
    );
    assert_eq!(first_payload.initial_stream_credit_bytes, Some(0));
    let invocation = dto::InvokeHookRequest::decode(first_payload.payload.as_slice()).unwrap();
    assert_eq!(invocation.handler_id, "remote-handler");
    let input = wire_value(invocation.input.unwrap().value.unwrap());
    assert_eq!(
        input["event"]["payload"]["context"]["messages"][0]["content"],
        "one selected service"
    );
    assert_eq!(input["event"]["payload"]["tools"], serde_json::json!([]));
    drop(requests);
    for method in [
        "before_run",
        "before_resume",
        "before_request",
        "before_payload",
        "transform_context",
        "provider.invoke",
        "after_response",
        "before_run_end",
        "before_compaction",
        "before_navigation",
        "resource.list",
        "resource.read",
        "telemetry.emit_batch",
    ] {
        assert!(
            methods.iter().any(|actual| actual == method),
            "missing {method}; got {methods:?}"
        );
    }
    assert!(setup.faux.requests.lock().unwrap().is_empty());
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap().usage.len(), 6);
    let before_tool = setup.pump.drive(store.snapshot()).unwrap();
    let first_new = peer.requests.lock().unwrap().len();
    setup
        .pump
        .drive(harness.set_active_tools(vec![pi_ai_core::ToolName::new("matrix-tool").unwrap()]))
        .unwrap();
    peer.hold_provider.store(true, Ordering::SeqCst);
    let mut tool_prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "same service native tool".into(),
        images: vec![],
    }));
    assert!(poll(tool_prompt.as_mut()).is_pending());
    let provider_request = (0..200)
        .find_map(|_| {
            setup.pump.tick();
            if let std::task::Poll::Ready(result) = poll(tool_prompt.as_mut()) {
                panic!(
                    "tool prompt ended before provider: {result:?}; methods={:?}",
                    peer.requests
                        .lock()
                        .unwrap()
                        .iter()
                        .skip(first_new)
                        .map(|r| r.method.clone())
                        .collect::<Vec<_>>()
                );
            }
            peer.requests
                .lock()
                .unwrap()
                .iter()
                .skip(first_new)
                .find(|r| r.method == "provider.invoke")
                .cloned()
        })
        .expect("actual selected provider request");
    assert_eq!(
        provider_request.capability_ref.as_ref(),
        Some(&cap(setup.faux.model.provider.as_str()))
    );
    let first_key = provider_request.effect_key.as_ref().unwrap().clone();
    assert_eq!(first_key.session_id, "runtime-session");
    assert_eq!(first_key.effect_kind, "assistant");
    assert_eq!(provider_request.initial_stream_credit_bytes, Some(0));
    let actual_provider =
        dto::InvokeProviderRequest::decode(provider_request.payload.as_slice()).unwrap();
    assert_eq!(
        wire_value(actual_provider.tools.unwrap().value.unwrap()),
        serde_json::json!([selected_tool_wire()])
    );
    let mut tool_use = assistant(&setup.faux.model, pi_ai_core::StopReason::ToolUse);
    tool_use.content = vec![pi_ai_core::AssistantContent::ToolCall(
        pi_ai_core::ToolCall {
            content_type: Default::default(),
            id: pi_ai_core::ToolCallId::new("matrix-call").unwrap(),
            name: pi_ai_core::ToolName::new("matrix-tool").unwrap(),
            arguments: Default::default(),
            thought_signature: None,
            namespace: None,
        },
    )];
    let message = serde_json::to_value(&tool_use).unwrap();
    let registry = pi_extension_protocol::profiles::Registry::bundled().unwrap();
    let result = dto::InvokeProviderResult {
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
    for _ in 0..20 {
        setup.pump.tick();
    }
    assert!(connection.current());
    peer.hold_provider.store(false, Ordering::SeqCst);
    peer.push(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: provider_request.request_id,
        terminal: Some(v1_1::stream_end::Terminal::Result(result)),
    }));
    let tool_outcome = setup
        .pump
        .drive(tool_prompt)
        .unwrap_or_else(|error| {
            panic!(
                "native tool prompt {error:?} display={error}; methods={:?}; calls={} occupancy={} current={} peer_closed={}",
                peer.requests
                    .lock()
                    .unwrap()
                    .iter()
                    .skip(first_new)
                    .map(|r| r.method.clone())
                    .collect::<Vec<_>>(),
                tool.calls.load(Ordering::SeqCst),
                connection.occupancy(),
                connection.current(),
                peer.closed.load(Ordering::SeqCst)
            )
        })
        .unwrap();
    assert!(matches!(
        tool_outcome.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(tool_outcome.run_id.as_str(), first_key.operation_id);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    let tool_requests = peer.requests.lock().unwrap();
    let new_requests = &tool_requests[first_new..];
    let providers = new_requests
        .iter()
        .filter(|r| r.method == "provider.invoke")
        .collect::<Vec<_>>();
    assert_eq!(providers.len(), 2);
    assert_eq!(providers[0].request_id, provider_request.request_id);
    let first_after_response = new_requests
        .iter()
        .find(|r| {
            r.method == "after_response" && r.parent_request_id == Some(provider_request.request_id)
        })
        .unwrap();
    assert_eq!(first_after_response.effect_key.as_ref(), Some(&first_key));
    for (method, kind) in [("before_tool", "hook"), ("after_tool", "tool")] {
        let hooks = new_requests
            .iter()
            .filter(|r| r.method == method)
            .collect::<Vec<_>>();
        assert_eq!(hooks.len(), 1, "{method}");
        let request = hooks[0];
        assert_eq!(request.capability_ref.as_ref(), Some(&cap(method)));
        assert_eq!(
            request.parent_request_id, None,
            "local native Tool has no wire Tool parent"
        );
        assert_eq!(request.initial_stream_credit_bytes, Some(0));
        let key = request.effect_key.as_ref().unwrap();
        assert_eq!(key.operation_id, first_key.operation_id);
        assert_eq!(key.step_or_task_id, first_key.step_or_task_id);
        assert_eq!(key.effect_kind, kind);
        assert_eq!(key.item_index, Some(0));
        let invocation = dto::InvokeHookRequest::decode(request.payload.as_slice()).unwrap();
        assert_eq!(invocation.handler_id, "remote-handler");
        let value = wire_value(invocation.input.unwrap().value.unwrap());
        assert_eq!(value["event"]["toolCallId"], "matrix-call");
        assert_eq!(value["event"]["toolName"], "matrix-tool");
        assert_eq!(value["event"]["args"], serde_json::json!({}));
        if method == "after_tool" {
            assert!(value["event"]["details"].is_null());
        }
    }
    drop(tool_requests);
    let after_tool = setup.pump.drive(store.snapshot()).unwrap();
    let new_entries = &after_tool.entries[before_tool.entries.len()..];
    let tool_results = new_entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(entry) => match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) => Some(result),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tool_results.len(), 1);
    assert_eq!(tool_results[0].tool_call_id.as_str(), "matrix-call");
    assert_eq!(tool_results[0].tool_name.as_str(), "matrix-tool");
    assert!(!tool_results[0].is_error);
    let assistant_ids = new_entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(entry)
                if matches!(
                    entry.message,
                    AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_))
                ) =>
            {
                Some(entry.id.clone())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(assistant_ids.len(), 2);
    let new_usage = &after_tool.usage[before_tool.usage.len()..];
    assert_eq!(new_usage.len(), 2);
    assert_eq!(
        new_usage
            .iter()
            .filter_map(|usage| usage.entry_id.clone())
            .collect::<BTreeSet<_>>(),
        assistant_ids
    );
    assert_eq!(connection.occupancy(), 0);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}

#[test]
fn memory_one_service_all_minor_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            run(
                &setup,
                setup.session.clone(),
                setup.store.clone(),
                core,
                minor,
            );
        }
    }
}

#[test]
fn sqlite_one_service_all_minor_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            run(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

struct AcceptNative;
impl pi_extension_protocol::profiles::NativeValidator for AcceptNative {
    fn validate(
        &mut self,
        _: &pi_extension_protocol::profiles::ValidatedPayload,
    ) -> Result<(), pi_extension_protocol::profiles::NativeError> {
        Ok(())
    }
}

#[test]
fn malformed_before_payload_replacement_fails_selected_exposure() {
    use pi_extension_protocol::profiles::{Error, Stage};
    let setup = Setup::new();
    let approved = keyed_before_payload_profile(&setup.faux.model);
    let malformed = serde_json::json!({"request":"malformed"});
    assert!(
        keyed_payload_schema()
            .validate_instance(&serde_json::to_vec(&malformed).unwrap(), Default::default())
            .is_err()
    );
    let result = serde_json::json!({"name":"before_payload","result":{"payload":malformed}});
    let bound = pi_harness_runtime::extensions::profiles::bound(
        approved.registry(),
        "hook.output:before_payload",
        &result,
    )
    .unwrap();
    let bytes = dto::InvokeHookResult {
        output: Some(bound),
    }
    .encode_to_vec();
    for minor in 0..=1 {
        assert_eq!(
            approved.validate_payload(
                minor,
                "before_payload",
                Stage::Terminal,
                &bytes,
                Default::default(),
                Default::default(),
                &mut AcceptNative,
            ),
            Err(Error::Exposure)
        );
    }
}
