#![cfg(feature = "extensions")]
mod runtime_extension_profiles_support;
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    session::RuntimeSession,
    storage::{AtomicStorage, RuntimeStorage},
};
use pi_harness_runtime::{
    extensions::profiles::hooks::RemoteHook,
    owner::{RuntimeFactory, RuntimeHarness},
};
use prost::Message;
use runtime_extension_profiles_support::*;
use runtime_r1_r4_support::*;
use std::sync::{Arc, atomic::Ordering};
fn create<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
) -> Arc<RuntimeHarness<(), S, SessionMetadata>> {
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        setup.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    setup
        .pump
        .drive(factory.create_configured(setup.options_for::<(), _>(session, DriveMode::Automatic)))
        .unwrap()
        .harness
}
fn install<S: RuntimeStorage + 'static>(
    setup: &Setup,
    harness: &RuntimeHarness<(), S, SessionMetadata>,
    core: u32,
    minor: u32,
) -> (
    Arc<pi_harness_runtime::extensions::profiles::connection::ProfileConnection>,
    Arc<Peer>,
) {
    install_named(
        setup,
        harness,
        core,
        minor,
        &[
            "before_run",
            "before_request",
            "transform_context",
            "after_response",
            "before_run_end",
        ],
    )
}
fn install_named<S: RuntimeStorage + 'static>(
    setup: &Setup,
    harness: &RuntimeHarness<(), S, SessionMetadata>,
    core: u32,
    minor: u32,
    names: &[&str],
) -> (
    Arc<pi_harness_runtime::extensions::profiles::connection::ProfileConnection>,
    Arc<Peer>,
) {
    let profiles = names
        .iter()
        .map(|name| profile("hook.v1", name))
        .chain([profile("provider.v1", setup.faux.model.provider.as_str())])
        .collect();
    let (connection, peer, _) = connection(setup, core, minor, profiles);
    for granted in connection
        .grant()
        .profiles()
        .iter()
        .filter(|p| p.reference().kind == "hook")
    {
        harness
            .register_profile_hook(
                RemoteHook::new(
                    connection.clone(),
                    granted.reference().clone(),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let cap = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.reference().kind == "provider")
        .unwrap()
        .reference()
        .clone();
    setup
        .pump
        .drive(harness.register_profile_provider(
            connection.clone(),
            cap,
            vec![setup.faux.model.clone()],
            Default::default(),
            None,
        ))
        .unwrap();
    (connection, peer)
}
fn run<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    let harness = create(setup, session);
    let (connection, peer) = install(setup, &harness, core, minor);
    let call = setup.pump.drive(harness.prompt(PromptInput::Text {
        text: "native prompt".into(),
        images: vec![],
    }));
    assert!(
        call.is_ok(),
        "native call failed; dispatched {:?}",
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.method.clone())
            .collect::<Vec<_>>()
    );
    let result = call.unwrap().unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    let requests = peer.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        [
            "before_run",
            "before_request",
            "transform_context",
            "provider.invoke",
            "after_response",
            "before_run_end"
        ]
    );
    assert!(requests[0].effect_key.is_none());
    let provider = &requests[3];
    let key = provider.effect_key.as_ref().unwrap();
    assert_eq!(key.session_id, "runtime-session");
    assert_eq!(key.operation_id, result.run_id.as_str());
    assert_eq!(key.effect_kind, "assistant");
    assert_eq!(requests[4].effect_key.as_ref(), Some(key));
    assert_eq!(requests[4].parent_request_id, Some(provider.request_id));
    assert_eq!(
        setup.faux.requests.lock().unwrap().len(),
        0,
        "generic adapter must not dispatch a remote provider"
    );
    drop(requests);
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    let assistants: Vec<_> = snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(entry) => match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)) => Some(message),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(assistants.len(), 1);
    assert_eq!(assistants[0].usage.total_tokens.get(), 5);
    assert!(
        matches!(&assistants[0].content[0],pi_ai_core::AssistantContent::Text(text) if text.text=="remote completion")
    );
    assert_eq!(snapshot.usage.len(), 1);
    assert_eq!(snapshot.usage[0].usage.total_tokens.get(), 5);
    assert!(snapshot.registers.iter().all(|r| !matches!(
        r,
        Register::OperationMeta { .. } | Register::OperationState { .. }
    )));
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_keyed_provider_and_ordered_native_hooks_all_minor_pairs() {
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
fn real_sqlite_keyed_provider_and_ordered_native_hooks_all_minor_pairs() {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let directory = std::env::temp_dir().join(format!(
                "runtime-profile-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir(&directory).unwrap();
            let codec = Arc::new(
                SessionCodec::new(
                    Default::default(),
                    ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                )
                .unwrap(),
            );
            let sqlite = pi_agent_sqlite::SqliteStorage::create_from_snapshot(
                directory.join("session.db"),
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
                        sqlite.clone(),
                        setup.session.metadata(),
                        codec,
                        setup.ids.clone(),
                        Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
                    ))
                    .unwrap(),
            );
            run(&setup, session, sqlite.clone(), core, minor);
            drop(sqlite);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
#[test]
fn expected_abort_target_keeps_a_successor_operation_running() {
    let setup = Setup::new();
    let harness = create(&setup, setup.session.clone());
    let (connection, peer) = install(&setup, &harness, 1, 1);
    let first = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "first".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    peer.hold_provider.store(true, Ordering::SeqCst);
    let mut second = Box::pin(harness.prompt(PromptInput::Text {
        text: "second".into(),
        images: vec![],
    }));
    assert!(poll(second.as_mut()).is_pending());
    for _ in 0..100 {
        setup.pump.tick();
        if peer.requests.lock().unwrap().last().is_some_and(|r| {
            r.method == "provider.invoke"
                && r.effect_key
                    .as_ref()
                    .is_some_and(|key| key.operation_id != first.run_id.as_str())
        }) {
            break;
        }
    }
    assert!(poll(second.as_mut()).is_pending());
    let before = setup.pump.drive(setup.store.snapshot()).unwrap();
    let aborted = setup
        .pump
        .drive(harness.abort_expected_operation(&first.run_id))
        .unwrap();
    let after = setup.pump.drive(setup.store.snapshot()).unwrap();
    assert_eq!(
        (aborted, after),
        (false, before),
        "wrong expected target must leave the successor backend exact"
    );
    peer.release();
    let second = setup.pump.drive(second).unwrap().unwrap();
    assert!(matches!(
        second.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_ne!(first.run_id, second.run_id);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
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
        "runtime-profile-more-{}-{}",
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
fn resource_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_harness_runtime::extensions::profiles::resources::ResourceCatalog;
    let harness = create(setup, session);
    let (connection, peer, _) = connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let first = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog))
        .unwrap();
    assert_eq!(first.resources().skills.unwrap()[0].content, "hello");
    assert!(!catalog.refresh_pending());
    assert!(catalog.restored_capture(None).is_err());
    assert!(Arc::ptr_eq(
        &catalog.restored_capture(Some(&first)).unwrap(),
        &first
    ));
    setup
        .pump
        .drive(harness.skill("remote-skill".into(), Some("additional".into())))
        .unwrap()
        .unwrap();
    let message = setup.faux.requests.lock().unwrap()[0].0.messages[0].clone();
    assert!(
        matches!(message,pi_ai_core::Message::User(message) if matches!(&message.content,pi_ai_core::UserContent::Text(text) if text.contains("hello")&&text.contains("/remote/skill.md")&&text.ends_with("additional")))
    );
    catalog.changed("allowed").unwrap();
    peer.results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    let second = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap, &catalog))
        .unwrap();
    assert_eq!(
        first.get("skill-id", Some("1")).unwrap().value,
        serde_json::json!("hello")
    );
    assert_eq!(
        second.get("skill-id", Some("2")).unwrap().value,
        serde_json::json!("world")
    );
    assert!(catalog.restored_capture(Some(&first)).is_err());
    setup
        .pump
        .drive(harness.prompt_from_template("remote-template".into(), vec![]))
        .unwrap()
        .unwrap();
    let message = setup
        .faux
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .0
        .messages
        .last()
        .unwrap()
        .clone();
    assert!(
        matches!(message,pi_ai_core::Message::User(message) if matches!(&message.content,pi_ai_core::UserContent::Text(text) if text=="world"))
    );
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap().usage.len(), 2);
    assert_eq!(
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        [
            "resource.list",
            "resource.read",
            "resource.read",
            "resource.list",
            "resource.read",
            "resource.read"
        ]
    );
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_resource_revision_drives_native_skill_and_template_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            resource_consumers(
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
fn real_sqlite_resource_revision_drives_native_skill_and_template_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            resource_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
fn telemetry_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_harness_runtime::extensions::profiles::telemetry::ProjectedTelemetry;
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
    options.telemetry_context = Arc::new(sink.clone());
    let harness = setup
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let (connection, peer, _) = connection(
        setup,
        core,
        minor,
        vec![profile("telemetry-sink.v1", "telemetry")],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "SENTINEL-user-content".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let mut delivered = 0;
    loop {
        let before = peer.requests.lock().unwrap().len();
        assert!(setup.pump.drive(harness.deliver_profile_telemetry(
            connection.clone(),
            cap.clone(),
            &sink
        )));
        if peer.requests.lock().unwrap().len() == before {
            break;
        }
        delivered += 1;
        assert!(delivered <= 256);
    }
    assert!(delivered > 0);
    let requests = peer.requests.lock().unwrap();
    let last = requests.last().unwrap();
    let batch = pi_extension_protocol::profiles::wire_v1_1::TelemetryEmitBatchRequest::decode(
        last.payload.as_slice(),
    )
    .unwrap();
    let through = batch.sequence;
    for request in requests.iter() {
        let batch = pi_extension_protocol::profiles::wire_v1_1::TelemetryEmitBatchRequest::decode(
            request.payload.as_slice(),
        )
        .unwrap();
        for record in batch.records {
            let value = wire_value(record.value.unwrap());
            let serialized = serde_json::to_string(&value).unwrap();
            assert!(!serialized.contains("SENTINEL") && !serialized.contains("custom-provider"));
        }
    }
    drop(requests);
    assert!(setup.pump.drive(harness.flush_profile_telemetry(
        connection.clone(),
        cap.clone(),
        &sink,
        through
    )));
    use pi_telemetry::TelemetryContext;
    let span = sink.start_span(pi_telemetry::SpanOptions {
        name: "pi.harness.run".into(),
        attributes: Default::default(),
    });
    span.finish();
    peer.malformed.store(true, Ordering::SeqCst);
    let before = setup.pump.drive(store.snapshot()).unwrap();
    assert!(
        !setup
            .pump
            .drive(harness.deliver_profile_telemetry(connection.clone(), cap, &sink))
    );
    assert!(sink.disabled());
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
    setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "native run after disabled sink".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap().usage.len(), 2);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}

#[test]
fn real_memory_telemetry_native_spans_ack_flush_failure_isolation_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            telemetry_consumers(
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
fn real_sqlite_telemetry_native_spans_ack_flush_failure_isolation_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            telemetry_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn structural_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    let harness = create(setup, session);
    let (connection, peer) = install_named(
        setup,
        &harness,
        core,
        minor,
        &[
            "before_run",
            "before_request",
            "transform_context",
            "after_response",
            "before_run_end",
            "before_compaction",
            "before_navigation",
        ],
    );
    for text in ["first sufficient context", "second native turn"] {
        setup
            .pump
            .drive(harness.prompt(PromptInput::Text {
                text: text.into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
    }
    let before = setup.pump.drive(store.snapshot()).unwrap();
    let target = before.entries.iter().find_map(|entry| match entry {
        Entry::Message(entry) => Some(entry.id.clone()),
        _ => None,
    });
    let compact = setup
        .pump
        .drive(harness.compact(CompactOptions::default()))
        .unwrap()
        .unwrap();
    assert!(matches!(compact.outcome, CompactionOutcome::Completed(_)));
    let after = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(after.entries.len(), before.entries.len() + 1);
    assert_eq!(after.usage.len(), before.usage.len() + 2);
    let navigation = setup
        .pump
        .drive(harness.navigate_tree(
            target,
            NavigateOptions {
                summarize: Some(true),
                ..Default::default()
            },
        ))
        .unwrap()
        .unwrap();
    assert!(matches!(
        navigation.outcome,
        NavigationOutcome::Completed(_)
    ));
    let requests = peer.requests.lock().unwrap();
    assert!(requests.iter().any(|r| r.method == "before_compaction"));
    assert!(requests.iter().any(|r| r.method == "before_navigation"));
    let summaries: Vec<_> = requests
        .iter()
        .filter(|r| {
            r.method == "provider.invoke"
                && r.effect_key
                    .as_ref()
                    .is_some_and(|k| k.effect_kind == "summary")
        })
        .collect();
    assert!(summaries.len() >= 2);
    for provider in summaries {
        let request = pi_extension_protocol::profiles::wire_v1_1::InvokeProviderRequest::decode(
            provider.payload.as_slice(),
        )
        .unwrap();
        assert_eq!(
            wire_value(request.tools.unwrap().value.unwrap()),
            serde_json::json!([])
        );
        assert!(
            wire_value(request.context.unwrap().value.unwrap())
                .get("tools")
                .is_none()
        );
        let after_response = requests
            .iter()
            .find(|r| {
                r.method == "after_response" && r.parent_request_id == Some(provider.request_id)
            })
            .unwrap();
        assert_eq!(after_response.effect_key, provider.effect_key);
    }
    assert!(setup.faux.requests.lock().unwrap().is_empty());
    assert!(after.registers.iter().all(|r| !matches!(
        r,
        Register::OperationMeta { .. }
            | Register::OperationState { .. }
            | Register::OperationPreparation { .. }
    )));
    drop(requests);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_compaction_summary_and_navigation_hooks_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            structural_consumers(
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
fn real_sqlite_compaction_summary_and_navigation_hooks_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            structural_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn deferred_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    let harness = create(setup, session);
    let (connection, peer) = install_named(
        setup,
        &harness,
        core,
        minor,
        &[
            "before_run",
            "before_resume",
            "before_request",
            "after_response",
            "before_run_end",
        ],
    );
    peer.results
        .lock()
        .unwrap()
        .insert("provider_mode".into(), Some(serde_json::json!("deferred")));
    let suspended = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "actual deferred source".into(),
            images: vec![],
        }))
        .map_err(|error| match error {
            HarnessRuntimeError::Fault(fault) => format!(
                "{fault}; requests={:?}",
                peer.requests
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|r| r.method.clone())
                    .collect::<Vec<_>>()
            ),
            _ => "other".into(),
        })
        .unwrap()
        .unwrap();
    assert!(matches!(suspended.outcome, RunOutcome::Deferred(_)));
    let first = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(first.usage.len(), 1);
    let fetched = setup.pump.drive(harness.resume()).unwrap().unwrap();
    assert!(matches!(
        fetched,
        ResumeOutcome::RunCompletedWithAssistant(_)
    ));
    let after = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(after.usage.len(), 2);
    let requests = peer.requests.lock().unwrap();
    let fetch = requests
        .iter()
        .find(|r| {
            r.method == "provider.invoke"
                && r.effect_key
                    .as_ref()
                    .is_some_and(|k| k.effect_kind == "deferred")
        })
        .unwrap();
    let invoke = pi_extension_protocol::profiles::wire_v1_1::InvokeProviderRequest::decode(
        fetch.payload.as_slice(),
    )
    .unwrap();
    let context = wire_value(invoke.context.unwrap().value.unwrap());
    let source = first
        .entries
        .iter()
        .find_map(|entry| match entry {
            Entry::Message(entry) => match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message))
                    if message.deferred.is_some() =>
                {
                    Some(message)
                }
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert_eq!(context["messages"], serde_json::json!([source]));
    assert_eq!(
        wire_value(invoke.tools.unwrap().value.unwrap()),
        serde_json::json!([])
    );
    assert_eq!(
        wire_value(invoke.options.unwrap().value.unwrap())["deferred"],
        false
    );
    let resume = requests
        .iter()
        .find(|r| r.method == "before_resume")
        .unwrap();
    assert!(resume.effect_key.is_none());
    assert!(resume.parent_request_id.is_none());
    drop(requests);
    let second = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "cancel suspended source".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(second.outcome, RunOutcome::Deferred(_)));
    let before = setup.pump.drive(store.snapshot()).unwrap();
    setup.pump.drive(harness.abort()).unwrap().unwrap();
    for _ in 0..500 {
        setup.pump.tick();
        if !setup
            .pump
            .drive(store.snapshot())
            .unwrap()
            .registers
            .iter()
            .any(|r| matches!(r, Register::OperationState { .. }))
        {
            break;
        }
    }
    let after = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(
        after.usage.len(),
        before.usage.len(),
        "best-effort cancel terminal cannot create Usage"
    );
    assert_eq!(
        after.entries.len(),
        before.entries.len(),
        "best-effort cancel terminal cannot create assistant entry"
    );
    assert!(peer.requests.lock().unwrap().iter().any(|r| {
        r.method == "provider.invoke"
            && r.effect_key
                .as_ref()
                .is_some_and(|k| k.effect_kind == "cancel_deferred")
            && r.parent_request_id.is_none()
    }));
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_deferred_fetch_resume_and_native_cancel_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            deferred_consumers(
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
fn real_sqlite_deferred_fetch_resume_and_native_cancel_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            deferred_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
fn passive_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_extension_protocol::profiles::wire_v1_1 as dto;
    use pi_harness_runtime::extensions::profiles::events::EventOptions;
    use std::num::NonZeroUsize;
    let harness = create(setup, session);
    let (connection, _, clock) =
        connection(setup, core, minor, vec![profile("event-sink.v1", "events")]);
    let cap = connection.grant().profiles()[0].reference().clone();
    let subscription = setup
        .pump
        .drive(harness.subscribe_profile_events(
            &connection,
            &cap,
            clock.clone(),
            EventOptions {
                queue_bytes: 128,
                gap_bytes: 1024,
                deadline_ms: 900,
                max_subscriptions: NonZeroUsize::new(1).unwrap(),
            },
        ))
        .unwrap();
    let dto::event_sink_item::Item::Snapshot(initial) =
        dto::EventSinkItem::decode(subscription.take().unwrap().unwrap().as_slice())
            .unwrap()
            .item
            .unwrap()
    else {
        panic!("native initial snapshot");
    };
    assert_eq!(initial.next_event_sequence, 0);
    let message = |text: &str| {
        AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {
            role: Default::default(),
            content: pi_ai_core::UserContent::Text(text.into()),
            timestamp: n(7),
        }))
    };
    let first = setup
        .pump
        .drive(harness.next_run(message("actual bounded first")))
        .unwrap()
        .unwrap();
    assert!(subscription.paused());
    let gap_bytes = subscription.take().unwrap().unwrap();
    let dto::event_sink_item::Item::Gap(gap) = dto::EventSinkItem::decode(gap_bytes.as_slice())
        .unwrap()
        .item
        .unwrap()
    else {
        panic!("reserved immutable native Gap");
    };
    setup
        .pump
        .drive(harness.next_run(message("second while paused")))
        .unwrap()
        .unwrap();
    assert!(subscription.take().unwrap().is_none());
    assert!(subscription.dropped() >= 2);
    let baseline = setup
        .pump
        .drive(harness.resync_profile_events(&subscription))
        .unwrap();
    assert!(!subscription.paused());
    assert!(baseline.next_event_sequence > gap.last_dropped);
    let snapshot = wire_value(baseline.filtered_snapshot.unwrap().value.unwrap());
    assert_eq!(
        snapshot["queues"]["nextRun"][0]["entryId"],
        first.entry_id.as_str()
    );
    assert_eq!(snapshot["queues"]["nextRun"].as_array().unwrap().len(), 2);
    let after = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(after.entries.len(), 0);
    assert_eq!(after.usage.len(), 0);
    clock.0.store(900, Ordering::SeqCst);
    setup.pump.tick();
    assert!(!subscription.active());
    assert!(subscription.take().unwrap().is_none());
    assert!(
        setup
            .pump
            .drive(harness.resync_profile_events(&subscription))
            .is_err()
    );
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), after);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_passive_publication_gap_atomic_resync_and_idle_deadline_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            passive_consumers(
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
fn real_sqlite_passive_publication_gap_atomic_resync_and_idle_deadline_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            passive_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
struct ActualTool {
    descriptor: AgentTool,
    calls: std::sync::atomic::AtomicUsize,
}
impl AgentHarnessTool<()> for ActualTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(
        &self,
        args: pi_ai_core::JsonValue,
    ) -> Result<pi_ai_core::JsonObject, ToolExecutionError> {
        let pi_ai_core::JsonValue::Object(args) = args else {
            panic!("native arguments object");
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
fn tool_payload_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    let descriptor = AgentTool {
        name: pi_ai_core::ToolName::new("same-tool").unwrap(),
        label: "same".into(),
        description: "same".into(),
        parameters: serde_json::from_value(
            serde_json::json!({"type":"object","properties":{},"additionalProperties":false}),
        )
        .unwrap(),
        constrained_sampling: None,
        execution_mode: None,
        replay: Some(ToolReplay::Safe),
    };
    let tool = Arc::new(ActualTool {
        descriptor,
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        setup.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = setup.options_for::<(), _>(session, DriveMode::Automatic);
    options.tools = vec![tool.clone()];
    options.tool_context = Some(AgentHarnessToolContextSource::Static(()));
    let harness = setup
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    setup
        .pump
        .drive(harness.set_active_tools(vec![pi_ai_core::ToolName::new("same-tool").unwrap()]))
        .unwrap();
    let mut response = assistant(&setup.faux.model, pi_ai_core::StopReason::ToolUse);
    response.content = (0..2)
        .map(|_| {
            pi_ai_core::AssistantContent::ToolCall(pi_ai_core::ToolCall {
                content_type: Default::default(),
                id: pi_ai_core::ToolCallId::new("identical-call-id").unwrap(),
                name: pi_ai_core::ToolName::new("same-tool").unwrap(),
                arguments: Default::default(),
                thought_signature: None,
                namespace: None,
            })
        })
        .collect();
    setup.faux.responses.lock().unwrap().push_front(response);
    let (connection, peer, _) = connection(
        setup,
        core,
        minor,
        ["before_tool", "after_tool", "before_payload"]
            .into_iter()
            .map(|name| profile("hook.v1", name))
            .collect(),
    );
    peer.results.lock().unwrap().insert(
        "before_payload".into(),
        Some(serde_json::json!({"payload":{"request":"native replacement"}})),
    );
    for grant in connection.grant().profiles() {
        harness
            .register_profile_hook(
                RemoteHook::new(
                    connection.clone(),
                    grant.reference().clone(),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "distinct actual source items".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 2);
    assert!(
        setup
            .faux
            .payloads
            .lock()
            .unwrap()
            .iter()
            .all(|payload| payload == &serde_json::json!({"request":"native replacement"}))
    );
    let requests = peer.requests.lock().unwrap();
    for method in ["before_tool", "after_tool"] {
        let calls: Vec<_> = requests.iter().filter(|r| r.method == method).collect();
        assert_eq!(calls.len(), 2);
        let keys: std::collections::BTreeSet<_> = calls
            .iter()
            .map(|r| r.effect_key.as_ref().unwrap().item_index.unwrap())
            .collect();
        assert_eq!(keys, std::collections::BTreeSet::from([0, 1]));
        assert_ne!(calls[0].effect_key, calls[1].effect_key);
        for call in calls {
            let request = pi_extension_protocol::profiles::wire_v1_1::InvokeHookRequest::decode(
                call.payload.as_slice(),
            )
            .unwrap();
            let value = wire_value(request.input.unwrap().value.unwrap());
            assert_eq!(value["event"]["toolCallId"], "identical-call-id");
            assert_eq!(value["event"]["args"], serde_json::json!({}));
        }
    }
    drop(requests);
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(snapshot.entries.iter().filter(|entry|matches!(entry,Entry::Message(entry) if matches!(entry.message,AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))).count(),2);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_identical_tool_calls_distinct_keys_and_native_payload_replacement_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            tool_payload_consumers(
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
fn real_sqlite_identical_tool_calls_distinct_keys_and_native_payload_replacement_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            tool_payload_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn terminal_receipt_consumer<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
) {
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        setup.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = setup
        .pump
        .drive(factory.create_configured(setup.options_for::<(), _>(session, DriveMode::Manual)))
        .unwrap()
        .harness;
    let (connection, peer) = install(setup, &harness, 1, 1);
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "actual retained final receipt".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut arrived = false;
    for _ in 0..500 {
        setup.pump.tick();
        if let Some(action) = setup.pump.drive(harness.peek_action()).unwrap() {
            if action.kind == "commit_terminal" {
                arrived = true;
                break;
            }
            setup.pump.drive(harness.execute_action()).unwrap();
        }
    }
    assert!(arrived);
    assert_eq!(
        peer.requests.lock().unwrap().last().unwrap().method,
        "before_run_end"
    );
    assert_eq!(
        connection.occupancy(),
        1,
        "native Terminal output must retain its first hook receipt until actual final consumer"
    );
    let before = setup.pump.drive(store.snapshot()).unwrap();
    assert!(
        before
            .registers
            .iter()
            .any(|r| matches!(r, Register::OperationState { .. }))
    );
    let queued = AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {
        role: Default::default(),
        content: pi_ai_core::UserContent::Text("native concurrent queue CAS".into()),
        timestamp: n(7),
    }));
    setup.pump.drive(harness.next_run(queued)).unwrap().unwrap();
    let changed = setup.pump.drive(store.snapshot()).unwrap();
    assert_ne!(before, changed);
    setup.pump.drive(harness.execute_action()).unwrap();
    assert_eq!(
        setup.pump.drive(store.snapshot()).unwrap(),
        changed,
        "stale native terminal candidate must make no backend write"
    );
    assert_eq!(
        connection.occupancy(),
        1,
        "refused terminal keeps exact first receipt custody"
    );
    setup.pump.drive(harness.run_to_completion()).unwrap();
    let result = setup.pump.drive(prompt).unwrap().unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(
        connection.occupancy(),
        0,
        "only applied final native terminal retires retained receipt"
    );
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_first_receipt_survives_stale_terminal_consumer_then_retires() {
    let setup = Setup::new();
    terminal_receipt_consumer(&setup, setup.session.clone(), setup.store.clone());
}
#[test]
fn real_sqlite_first_receipt_survives_stale_terminal_consumer_then_retires() {
    let setup = Setup::new();
    let (session, store, dir) = sqlite_case(&setup);
    terminal_receipt_consumer(&setup, session, store.clone());
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

fn physical_tool_consumer<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
    before_hook: bool,
) {
    let captured_session = session.clone();
    use pi_extension_protocol::{
        activation::HostAuthority, profile_live::Reservations, wire::v1_1,
    };
    use pi_harness_runtime::extensions::{
        tool_driver::{EchoRouteTiming, OwnerLimits, SingleToolOwner},
        transport::PumpConfig,
    };
    use std::num::{NonZeroU64, NonZeroUsize};
    let catalog = echo_catalog();
    let metadata = &catalog["entries"][0]["agentTool"];
    let tool = Arc::new(ActualTool {
        descriptor: AgentTool {
            name: pi_ai_core::ToolName::new("echo").unwrap(),
            label: metadata["label"].as_str().unwrap().into(),
            description: metadata["description"].as_str().unwrap().into(),
            parameters: serde_json::from_value(metadata["parameters"].clone()).unwrap(),
            constrained_sampling: None,
            execution_mode: Some(ToolExecutionMode::Sequential),
            replay: Some(ToolReplay::Safe),
        },
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        setup.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let mut options = setup.options_for::<(), _>(session, DriveMode::Automatic);
    options.tools = vec![tool.clone()];
    options.tool_context = Some(AgentHarnessToolContextSource::Static(()));
    let harness = setup
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let mut profiles = vec![echo_hook("after_tool"), actions_profile()];
    if before_hook {
        profiles.insert(0, echo_hook("before_tool"));
    }
    let peer = Peer::new(core, minor, profiles.clone());
    peer.enable_tool(minor);
    let clock = Arc::new(Mono(std::sync::atomic::AtomicU64::new(0)));
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
    let permissions = vec![v1_1::PermissionGrant {
        name: "queue.next_run".into(),
        scope: "main".into(),
    }];
    let host = HostAuthority::new(
        "peer-a".into(),
        7,
        core,
        1,
        limits,
        vec![echo_approval()],
        permissions,
    )
    .unwrap()
    .with_profiles(profiles)
    .unwrap();
    let owner = SingleToolOwner::new(
        setup.pump.clone(),
        OwnerLimits {
            outbound_bytes: NonZeroUsize::new(1048576).unwrap(),
            receipt_bytes: NonZeroUsize::new(262144).unwrap(),
            write_timeout_ms: NonZeroU64::new(500).unwrap(),
        },
    );
    let (connection, route) = setup
        .pump
        .drive(harness.connect_profile_tool(
            &owner,
            host,
            peer.clone(),
            clock,
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(1024).unwrap(),
                write_timeout_ms: NonZeroU64::new(500).unwrap(),
            },
            Reservations {
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
    setup
        .pump
        .drive(harness.register_ready_echo_tool(
            tool.clone(),
            Arc::new(route),
            EchoRouteTiming {
                method_limit_ms: NonZeroU64::new(1000).unwrap(),
                connection_deadline_ms: 10000,
            },
        ))
        .unwrap()
        .unwrap();
    setup
        .pump
        .drive(harness.set_active_tools(vec![pi_ai_core::ToolName::new("echo").unwrap()]))
        .unwrap();
    for granted in connection
        .grant()
        .profiles()
        .iter()
        .filter(|profile| profile.approved().profile() == "hook.v1")
    {
        harness
            .register_profile_hook(
                RemoteHook::new(
                    connection.clone(),
                    granted.reference().clone(),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let mut response = assistant(&setup.faux.model, pi_ai_core::StopReason::ToolUse);
    response.content = (0..2)
        .map(|_| {
            pi_ai_core::AssistantContent::ToolCall(pi_ai_core::ToolCall {
                content_type: Default::default(),
                id: pi_ai_core::ToolCallId::new("same-real-call").unwrap(),
                name: pi_ai_core::ToolName::new("echo").unwrap(),
                arguments: serde_json::from_value(
                    serde_json::json!({"text":"actual physical Tool"}),
                )
                .unwrap(),
                thought_signature: None,
                namespace: None,
            })
        })
        .collect();
    setup.faux.responses.lock().unwrap().push_front(response);
    peer.hold_tools.store(true, Ordering::SeqCst);
    peer.results
        .lock()
        .unwrap()
        .insert("peer_child_offset".into(), Some(serde_json::json!(1000)));
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "physical shared connection".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..500 {
        setup.pump.tick();
        if owner.retained_binding().is_some_and(|binding| {
            peer.requests.lock().unwrap().iter().any(|request| {
                request.method == "tool.invoke" && request.request_id == binding.request_id()
            })
        }) {
            break;
        }
    }
    let tool_binding = owner.retained_binding().unwrap();
    assert_eq!(
        owner.binding_phase(&tool_binding).unwrap(),
        pi_extension_protocol::tool_live::LivePhase::Open
    );
    let parent = owner.profile_parent(&tool_binding).unwrap();
    let observed = parent.observation(0).unwrap();
    let before_foreign = setup.pump.drive(store.snapshot()).unwrap();
    let (foreign, _, _) = runtime_extension_profiles_support::connection(
        setup,
        core,
        minor,
        vec![echo_hook("after_tool")],
    );
    assert_eq!(
        foreign.grant().wire().authenticated_extension_id,
        observed.principal
    );
    assert_eq!(
        foreign.grant().wire().connection_generation,
        observed.generation
    );
    assert!(
        !foreign
            .connection_identity()
            .same_connection(&observed.connection)
    );
    let mut probe = pi_extension_protocol::profile_live::Owner::new_with_connection(
        foreign.grant().clone(),
        Reservations {
            blocking_bytes: 1048576,
            control_bytes: 8388608,
            terminal_bytes: 8388608,
            progress_bytes: 1048576,
            passive_bytes: 1048576,
            telemetry_bytes: 1048576,
        },
        0,
        10000,
        foreign.connection_identity(),
    )
    .unwrap();
    let cap = foreign.grant().profiles()[0].reference().clone();
    let input = serde_json::json!({"name":"after_tool","event":{"lane":tool_binding.effect_key().lane,"runId":tool_binding.effect_key().operation_id,"toolCallId":"same-real-call","toolName":"echo","args":{"text":"actual physical Tool"},"content":[],"details":{},"isError":false}});
    let payload = pi_extension_protocol::profiles::wire_v1_1::InvokeHookRequest {
        handler_id: cap.handler_id.clone().unwrap(),
        input: Some(
            pi_harness_runtime::extensions::profiles::bound(
                foreign.grant().profiles()[0].approved().registry(),
                "hook.input:after_tool",
                &input,
            )
            .unwrap(),
        ),
    }
    .encode_to_vec();
    struct CountNative<'a> {
        domain: pi_harness_runtime::extensions::profiles::native::NativeDomain<'a>,
        calls: usize,
    }
    impl pi_extension_protocol::profiles::NativeValidator for CountNative<'_> {
        fn validate(
            &mut self,
            value: &pi_extension_protocol::profiles::ValidatedPayload,
        ) -> Result<(), pi_extension_protocol::profiles::NativeError> {
            self.calls += 1;
            self.domain.validate(value)
        }
    }
    let model_policy = Default::default();
    let mut native = CountNative {
        domain: pi_harness_runtime::extensions::profiles::native::NativeDomain::new(
            captured_session.codec(),
            &model_policy,
        ),
        calls: 0,
    };
    let result = probe.start_with_external_parent(
        pi_extension_protocol::profile_live::Start {
            capability: cap,
            method: "after_tool".into(),
            direction: pi_extension_protocol::profiles::Direction::CoreToExtension,
            role: pi_extension_protocol::profile_live::Role::Inline,
            key: Some(tool_binding.effect_key().clone()),
            parent: None,
            scope: "main".into(),
            payload,
            now_ms: 0,
            requested_deadline_ms: Some(300),
            method_default_ms: 500,
            method_limit_ms: 500,
            initial_credit: Some(0),
            generation: 7,
        },
        parent,
        &mut native,
    );
    assert!(
        matches!(
            result,
            Err(pi_extension_protocol::profile_live::Error::Parent)
        ),
        "same principal/gen/key/ID on a different physical connection cannot admit child"
    );
    assert_eq!(native.calls, 0);
    assert_eq!(probe.occupancy(), 0);
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_foreign);
    // All seven source-bound no-work controls share this real outstanding Tool,
    // exact native K/codec and actual backend snapshot; no raw tuple is a proof.
    let mut controls = pi_extension_protocol::profile_live::Owner::new_with_connection(
        connection.grant().clone(),
        Reservations {
            blocking_bytes: 1048576,
            control_bytes: 8388608,
            terminal_bytes: 8388608,
            progress_bytes: 1048576,
            passive_bytes: 1048576,
            telemetry_bytes: 1048576,
        },
        0,
        10000,
        connection.connection_identity(),
    )
    .unwrap();
    let hook_cap = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.reference().name == "after_tool")
        .unwrap()
        .reference()
        .clone();
    let action_cap = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.approved().profile() == "host-action.v1")
        .unwrap()
        .reference()
        .clone();
    let input = serde_json::json!({"name":"after_tool","event":{"lane":tool_binding.effect_key().lane,"runId":tool_binding.effect_key().operation_id,"toolCallId":"same-real-call","toolName":"echo","args":{"text":"actual physical Tool"},"content":[],"details":{},"isError":false}});
    let hook_payload = pi_extension_protocol::profiles::wire_v1_1::InvokeHookRequest {
        handler_id: hook_cap.handler_id.clone().unwrap(),
        input: Some(
            pi_harness_runtime::extensions::profiles::bound(
                connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| p.reference() == &hook_cap)
                    .unwrap()
                    .approved()
                    .registry(),
                "hook.input:after_tool",
                &input,
            )
            .unwrap(),
        ),
    }
    .encode_to_vec();
    let start = |cap, method: &str, key, generation, payload, role| {
        pi_extension_protocol::profile_live::Start {
            capability: cap,
            method: method.into(),
            direction: if role == pi_extension_protocol::profile_live::Role::HostAction {
                pi_extension_protocol::profiles::Direction::ExtensionToCore
            } else {
                pi_extension_protocol::profiles::Direction::CoreToExtension
            },
            role,
            key,
            parent: None,
            scope: "main".into(),
            payload,
            now_ms: 0,
            requested_deadline_ms: Some(300),
            method_default_ms: 500,
            method_limit_ms: 500,
            initial_credit: Some(0),
            generation,
        }
    };
    let mut wrong_key = tool_binding.effect_key().clone();
    wrong_key.operation_id = "different-operation".into();
    for (key, generation) in [(wrong_key, 7), (tool_binding.effect_key().clone(), 8)] {
        native.calls = 0;
        assert!(
            controls
                .start_with_external_parent(
                    start(
                        hook_cap.clone(),
                        "after_tool",
                        Some(key),
                        generation,
                        hook_payload.clone(),
                        pi_extension_protocol::profile_live::Role::Inline
                    ),
                    owner.profile_parent(&tool_binding).unwrap(),
                    &mut native
                )
                .is_err()
        );
        assert_eq!(native.calls, 0);
        assert_eq!(controls.occupancy(), 0);
    }
    let item=pi_harness_runtime::extensions::profiles::bound(&pi_extension_protocol::profiles::Registry::bundled().unwrap(),"host.queue_item.v1",&serde_json::json!({"role":"user","content":"admission only; never native allocation","timestamp":7})).unwrap();
    let payload = pi_extension_protocol::profiles::wire_v1_1::QueueNextRunRequest {
        lane: "main".into(),
        item: Some(item),
    }
    .encode_to_vec();
    let peer_request = |id, parent, cap, method: &str, payload| v1_1::Request {
        request_id: id,
        parent_request_id: Some(parent),
        capability_ref: Some(cap),
        effect_key: None,
        method: method.into(),
        deadline_after_ms: Some(300),
        initial_stream_credit_bytes: Some(0),
        payload,
    };
    let duplicate_id = tool_binding.request_id() + 2;
    let request = peer_request(
        duplicate_id,
        tool_binding.request_id(),
        action_cap.clone(),
        "queue.next_run",
        payload.clone(),
    );
    let binding = controls
        .accept_request_with_external_parent(
            request.clone(),
            start(
                action_cap.clone(),
                "queue.next_run",
                None,
                7,
                vec![],
                pi_extension_protocol::profile_live::Role::HostAction,
            ),
            owner.profile_parent(&tool_binding).unwrap(),
            &mut native,
        )
        .unwrap();
    controls
        .retire(
            &binding,
            pi_extension_protocol::profile_live::RetireReason::NoLongerExpected,
        )
        .unwrap();
    native.calls = 0;
    assert!(
        controls
            .accept_request_with_external_parent(
                request,
                start(
                    action_cap.clone(),
                    "queue.next_run",
                    None,
                    7,
                    vec![],
                    pi_extension_protocol::profile_live::Role::HostAction
                ),
                owner.profile_parent(&tool_binding).unwrap(),
                &mut native
            )
            .is_err()
    );
    assert_eq!(native.calls, 0);
    for (id, parent) in [
        (duplicate_id + 1, 9999),
        (tool_binding.request_id(), tool_binding.request_id()),
    ] {
        native.calls = 0;
        assert!(
            controls
                .accept_request_with_external_parent(
                    peer_request(
                        id,
                        parent,
                        action_cap.clone(),
                        "queue.next_run",
                        payload.clone()
                    ),
                    start(
                        action_cap.clone(),
                        "queue.next_run",
                        None,
                        7,
                        vec![],
                        pi_extension_protocol::profile_live::Role::HostAction
                    ),
                    owner.profile_parent(&tool_binding).unwrap(),
                    &mut native
                )
                .is_err()
        );
        assert_eq!(native.calls, 0);
    }
    let mut omitted = action_cap.clone();
    omitted.name = "ungranted-coherent".into();
    for (cap, method) in [
        (omitted, "queue.next_run"),
        (action_cap.clone(), "secret.resolve"),
    ] {
        native.calls = 0;
        assert!(
            controls
                .accept_request_with_external_parent(
                    peer_request(
                        duplicate_id + 2,
                        tool_binding.request_id(),
                        cap.clone(),
                        method,
                        vec![]
                    ),
                    start(
                        cap,
                        method,
                        None,
                        7,
                        vec![],
                        pi_extension_protocol::profile_live::Role::HostAction
                    ),
                    owner.profile_parent(&tool_binding).unwrap(),
                    &mut native
                )
                .is_err()
        );
        assert_eq!(native.calls, 0);
    }
    assert_eq!(controls.occupancy(), 0);
    assert_eq!(
        setup.pump.drive(store.snapshot()).unwrap(),
        before_foreign,
        "all seven admission denials leave actual backend exact"
    );
    foreign.close();
    peer.release_request(tool_binding.request_id());
    let result = setup.pump.drive(prompt).unwrap().unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(
        tool.calls.load(Ordering::SeqCst),
        0,
        "routed native Tool must use actual peer bytes"
    );
    let requests = peer.requests.lock().unwrap();
    let tools: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "tool.invoke")
        .collect();
    assert_eq!(tools.len(), 2);
    for tool in &tools {
        let hook = requests
            .iter()
            .find(|request| {
                request.method == "after_tool" && request.parent_request_id == Some(tool.request_id)
            })
            .unwrap();
        assert_eq!(hook.effect_key, tool.effect_key);
        assert!(hook.request_id > tool.request_id);
    }
    let responses = peer.responses.lock().unwrap();
    assert_eq!(responses.len(), 2);
    for response in responses.iter() {
        let Some(v1_1::response::Terminal::Result(bytes)) = &response.terminal else {
            panic!("actual native action denied");
        };
        let result =
            pi_extension_protocol::profiles::wire_v1_1::QueueResult::decode(bytes.as_slice())
                .unwrap();
        assert!(result.accepted && !result.item_id.is_empty());
    }
    let ids: Vec<_> = requests
        .iter()
        .map(|request| request.request_id)
        .chain(responses.iter().map(|response| response.request_id))
        .collect();
    assert_eq!(
        ids.len(),
        ids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        "same Route allocator must keep future Tool and profile/peer IDs unique"
    );
    assert!(
        tools[1].request_id
            > requests
                .iter()
                .find(|request| request.method == "after_tool"
                    && request.parent_request_id == Some(tools[0].request_id))
                .unwrap()
                .request_id
    );
    drop(responses);
    drop(requests);
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(
        snapshot
            .registers
            .iter()
            .filter(|register| matches!(register, Register::PendingEntry { .. }))
            .count(),
        2
    );
    assert_eq!(snapshot.entries.iter().filter(|entry|matches!(entry,Entry::Message(entry) if matches!(entry.message,AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))).count(),2);
    assert_eq!(connection.occupancy(), 0);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_physical_tool_parent_actions_inline_and_future_ids_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            physical_tool_consumer(
                &setup,
                setup.session.clone(),
                setup.store.clone(),
                core,
                minor,
                true,
            );
        }
    }
}
#[test]
fn real_sqlite_physical_tool_parent_actions_inline_and_future_ids_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            physical_tool_consumer(&setup, session, store.clone(), core, minor, true);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[test]
fn real_memory_inline_only_profile_ids_advance_the_next_native_tool() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            physical_tool_consumer(
                &setup,
                setup.session.clone(),
                setup.store.clone(),
                core,
                minor,
                false,
            );
        }
    }
}
#[test]
fn real_sqlite_inline_only_profile_ids_advance_the_next_native_tool() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            physical_tool_consumer(&setup, session, store.clone(), core, minor, false);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn passive_wire_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_extension_protocol::{profiles::wire_v1_1 as dto, wire::v1_1};
    use pi_harness_runtime::extensions::transport::PumpConfig;
    use std::num::{NonZeroU64, NonZeroUsize};
    let harness = create(setup, session);
    let profiles = vec![passive_event_profile(), profile("resource.v1", "resources")];
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
    let resource = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.approved().profile() == "resource.v1")
        .unwrap()
        .reference()
        .clone();
    let event = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.approved().profile() == "event-sink.v1")
        .unwrap()
        .reference()
        .clone();
    let catalog =
        pi_harness_runtime::extensions::profiles::resources::ResourceCatalog::new("allowed".into())
            .unwrap();
    let captured = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), resource.clone(), &catalog))
        .unwrap();
    assert!(!catalog.refresh_pending());
    let before = setup.pump.drive(store.snapshot()).unwrap();
    peer.push(request(
        10,
        None,
        event.clone(),
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
    assert!(
        peer.items.lock().unwrap().is_empty(),
        "zero outgoing credit keeps initial actual snapshot queued"
    );
    peer.push(v1_1::envelope::Frame::Credit(v1_1::Credit {
        request_id: 10,
        additional_bytes: 1048576,
    }));
    for _ in 0..50 {
        setup.pump.tick();
    }
    let items = peer.items.lock().unwrap();
    assert_eq!(items.len(), 1);
    let dto::event_sink_item::Item::Snapshot(baseline) =
        dto::EventSinkItem::decode(items[0].payload.as_slice())
            .unwrap()
            .item
            .unwrap()
    else {
        panic!("actual native snapshot")
    };
    assert_eq!(items[0].sequence, 0);
    drop(items);
    let message = AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {
        role: Default::default(),
        content: pi_ai_core::UserContent::Text("real passive queue publication".into()),
        timestamp: n(7),
    }));
    let queued = setup
        .pump
        .drive(harness.next_run(message))
        .unwrap()
        .unwrap();
    for _ in 0..50 {
        setup.pump.tick();
    }
    assert!(peer.items.lock().unwrap().len() > 1);
    peer.push(request(
        11,
        None,
        event,
        "event.resync",
        dto::ResyncRequest {
            subscription_id: baseline.subscription_id,
        }
        .encode_to_vec(),
        0,
    ));
    peer.push(request(
        12,
        None,
        resource,
        "resource.changed",
        dto::ResourceChanged {
            scope: "allowed".into(),
        }
        .encode_to_vec(),
        0,
    ));
    for _ in 0..100 {
        setup.pump.tick();
    }
    let responses = peer.responses.lock().unwrap();
    let result = responses.iter().find(|r| r.request_id == 11).unwrap();
    let Some(v1_1::response::Terminal::Result(bytes)) = &result.terminal else {
        panic!("actual resync result")
    };
    let resync = dto::ResyncResult::decode(bytes.as_slice())
        .unwrap()
        .snapshot
        .unwrap();
    let snap = wire_value(resync.filtered_snapshot.unwrap().value.unwrap());
    assert_eq!(
        snap["queues"]["nextRun"][0]["entryId"],
        queued.entry_id.as_str()
    );
    assert!(
        responses.iter().any(|r| r.request_id == 12
            && matches!(r.terminal, Some(v1_1::response::Terminal::Result(_))))
    );
    drop(responses);
    assert!(catalog.refresh_pending());
    assert_eq!(captured.get("skill-id", None).unwrap().value, "hello");
    clock.0.store(500, Ordering::SeqCst);
    for _ in 0..100 {
        setup.pump.tick();
    }
    assert!(
        peer.ends
            .lock()
            .unwrap()
            .iter()
            .any(|end| end.request_id == 10)
    );
    assert_eq!(connection.occupancy(), 0);
    let after = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.usage, before.usage);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_passive_three_methods_idle_wire_service_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            passive_wire_consumers(
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
fn real_sqlite_passive_three_methods_idle_wire_service_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            passive_wire_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn actions_wire_consumers<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_extension_protocol::{profiles::wire_v1_1 as dto, wire::v1_1};
    use pi_harness_runtime::extensions::transport::PumpConfig;
    use std::num::{NonZeroU64, NonZeroUsize};
    let harness = create(setup, session);
    let profiles = vec![
        profile("provider.v1", setup.faux.model.provider.as_str()),
        profile("resource.v1", "resources"),
        all_actions_profile(false),
        all_actions_profile(true),
    ];
    let peer = Peer::new(core, minor, profiles.clone());
    let clock = Arc::new(Mono(std::sync::atomic::AtomicU64::new(0)));
    let connection = setup
        .pump
        .drive(harness.connect_profile_service(
            service_host(core, profiles),
            peer.clone(),
            clock,
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
            .find(|p| p.reference().name == name)
            .unwrap()
            .reference()
            .clone()
    };
    let catalog =
        pi_harness_runtime::extensions::profiles::resources::ResourceCatalog::new("allowed".into())
            .unwrap();
    let old = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap("resources"), &catalog))
        .unwrap();
    setup
        .pump
        .drive(harness.register_profile_provider(
            connection.clone(),
            cap(setup.faux.model.provider.as_str()),
            vec![setup.faux.model.clone()],
            Default::default(),
            None,
        ))
        .unwrap();
    peer.hold_provider.store(true, Ordering::SeqCst);
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "native six actions".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        setup.pump.tick();
        if peer
            .requests
            .lock()
            .unwrap()
            .last()
            .is_some_and(|r| r.method == "provider.invoke")
        {
            break;
        }
    }
    let parent = peer.requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(parent.method, "provider.invoke");
    let (fresh, fresh_peer, _) = runtime_extension_profiles_support::connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    fresh_peer
        .results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    let fresh_cap = fresh.grant().profiles()[0].reference().clone();
    let new = setup
        .pump
        .drive(harness.refresh_profile_resources(fresh.clone(), fresh_cap, &catalog))
        .unwrap();
    assert_eq!(new.get("skill-id", None).unwrap().value, "world");
    assert_eq!(old.get("skill-id", None).unwrap().value, "hello");
    fresh.close();
    let item = pi_harness_runtime::extensions::profiles::bound(
        &pi_extension_protocol::profiles::Registry::bundled().unwrap(),
        "host.queue_item.v1",
        &serde_json::json!({"role":"user","content":"actual native action item","timestamp":7}),
    )
    .unwrap();
    let actions = [
        (
            20,
            "snapshot.get",
            cap("actions"),
            dto::SnapshotGetRequest {
                lane: "main".into(),
                visibility: "captured_policy".into(),
            }
            .encode_to_vec(),
        ),
        (
            21,
            "resource.get",
            cap("actions-resource"),
            dto::ResourceGetRequest {
                scope: "allowed".into(),
                id: "skill-id".into(),
                version: Some("1".into()),
            }
            .encode_to_vec(),
        ),
        (
            22,
            "queue.steer",
            cap("actions"),
            dto::QueueSteerRequest {
                lane: "main".into(),
                item: Some(item.clone()),
            }
            .encode_to_vec(),
        ),
        (
            23,
            "queue.follow_up",
            cap("actions"),
            dto::QueueFollowUpRequest {
                lane: "main".into(),
                item: Some(item.clone()),
            }
            .encode_to_vec(),
        ),
        (
            24,
            "queue.next_run",
            cap("actions"),
            dto::QueueNextRunRequest {
                lane: "main".into(),
                item: Some(item),
            }
            .encode_to_vec(),
        ),
        (
            25,
            "operation.abort",
            cap("actions"),
            dto::OperationAbortRequest {
                lane: "main".into(),
                operation_id: "different-native-operation".into(),
            }
            .encode_to_vec(),
        ),
    ];
    for (id, method, cap, payload) in actions {
        peer.push(request(
            id,
            Some(parent.request_id),
            cap,
            method,
            payload,
            0,
        ));
        for _ in 0..100 {
            setup.pump.tick();
            if peer
                .responses
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.request_id == id)
            {
                break;
            }
        }
        let responses = peer.responses.lock().unwrap();
        let result = responses.iter().find(|r| r.request_id == id).unwrap();
        let Some(v1_1::response::Terminal::Result(bytes)) = &result.terminal else {
            panic!("actual action failed {method}: {:?}", result.terminal)
        };
        match method {
            "resource.get" => {
                let result = dto::ResourceGetResult::decode(bytes.as_slice()).unwrap();
                assert_eq!(
                    wire_value(result.snapshot.unwrap().value.unwrap())["value"],
                    "hello",
                    "active native run keeps captured revision after global refresh"
                );
            }
            "operation.abort" => assert!(
                !dto::OperationAbortResult::decode(bytes.as_slice())
                    .unwrap()
                    .aborted
            ),
            "snapshot.get" => {
                let snapshot = dto::SnapshotGetResult::decode(bytes.as_slice()).unwrap();
                assert_eq!(
                    wire_value(snapshot.filtered_snapshot.unwrap().value.unwrap())["operation"]["id"],
                    parent.effect_key.as_ref().unwrap().operation_id
                );
            }
            _ => assert!(dto::QueueResult::decode(bytes.as_slice()).unwrap().accepted),
        }
    }
    let before = setup.pump.drive(store.snapshot()).unwrap();
    let mut omitted = cap("actions");
    omitted.name = "ungranted-coherent".into();
    for (id, method, cap, parent_id, expected) in [
        (
            26,
            "secret.resolve",
            cap("actions"),
            Some(parent.request_id),
            v1_1::ErrorCode::BrokerUnavailable,
        ),
        (
            27,
            "unknown.fixed",
            cap("actions"),
            Some(parent.request_id),
            v1_1::ErrorCode::UnsupportedMethod,
        ),
        (
            28,
            "snapshot.get",
            omitted,
            Some(parent.request_id),
            v1_1::ErrorCode::Forbidden,
        ),
        (
            29,
            "queue.next_run",
            cap("actions"),
            Some(9999),
            v1_1::ErrorCode::InvalidParent,
        ),
    ] {
        peer.push(request(id, parent_id, cap, method, vec![], 0));
        for _ in 0..100 {
            setup.pump.tick();
            if peer
                .responses
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.request_id == id)
            {
                break;
            }
        }
        let responses = peer.responses.lock().unwrap();
        let response = responses.iter().find(|r| r.request_id == id).unwrap();
        assert!(
            matches!(&response.terminal,Some(v1_1::response::Terminal::Error(error)) if error.code==expected as i32&&error.safe_message==pi_extension_protocol::profile_live::safe_error(expected))
        );
        assert_eq!(
            setup.pump.drive(store.snapshot()).unwrap(),
            before,
            "denial performs no native backend work"
        );
    }
    assert_eq!(
        before
            .registers
            .iter()
            .filter(|r| matches!(r, Register::PendingEntry { .. }))
            .count(),
        3
    );
    peer.release_request(parent.request_id);
    let result = setup.pump.drive(prompt).unwrap().unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(setup.faux.requests.lock().unwrap().len(), 0);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_six_actions_captured_resources_and_fixed_denials_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            actions_wire_consumers(
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
fn real_sqlite_six_actions_captured_resources_and_fixed_denials_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            actions_wire_consumers(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn action_cas_consumer<S: RuntimeStorage + 'static>(
    setup: &Setup,
    source_session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
    mode: u8,
) {
    use pi_extension_protocol::{profiles::wire_v1_1 as dto, wire::v1_1};
    use pi_harness_runtime::extensions::transport::PumpConfig;
    use runtime_extension_profiles_support::storage::*;
    use std::num::{NonZeroU64, NonZeroUsize};
    let gate = Arc::new(GateStorage::new(store.clone()));
    let session = Arc::new(
        setup
            .pump
            .drive(RuntimeSession::new(
                gate.clone(),
                source_session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        Default::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                setup.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let ids = Arc::new(CountingIds {
        inner: setup.ids.clone(),
        count: std::sync::atomic::AtomicUsize::new(0),
    });
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let harness = setup
        .pump
        .drive(factory.create_configured(setup.options_for::<(), _>(session, DriveMode::Manual)))
        .unwrap()
        .harness;
    let profiles = vec![
        profile("provider.v1", setup.faux.model.provider.as_str()),
        all_actions_profile(false),
    ];
    let peer = Peer::new(core, minor, profiles.clone());
    let clock = Arc::new(Mono(std::sync::atomic::AtomicU64::new(0)));
    let connection = setup
        .pump
        .drive(harness.connect_profile_service(
            service_host(core, profiles),
            peer.clone(),
            clock,
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(1024).unwrap(),
                write_timeout_ms: NonZeroU64::new(500).unwrap(),
            },
            reservations(),
            10000,
        ))
        .unwrap();
    let cap = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.approved().profile() == "provider.v1")
        .unwrap()
        .reference()
        .clone();
    setup
        .pump
        .drive(harness.register_profile_provider(
            connection.clone(),
            cap,
            vec![setup.faux.model.clone()],
            Default::default(),
            None,
        ))
        .unwrap();
    peer.hold_provider.store(true, Ordering::SeqCst);
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "actual native action CAS guard".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..200 {
        setup.pump.tick();
        if peer
            .requests
            .lock()
            .unwrap()
            .last()
            .is_some_and(|r| r.method == "provider.invoke")
        {
            break;
        }
        if setup.pump.drive(harness.peek_action()).unwrap().is_some() {
            setup.pump.drive(harness.execute_action()).unwrap();
        }
    }
    let parent = peer.requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(parent.method, "provider.invoke");
    let before = setup.pump.drive(store.snapshot()).unwrap();
    let before_ids = ids.count.load(Ordering::SeqCst);
    if mode == 0 {
        gate.arm_read.store(true, Ordering::SeqCst);
    } else {
        gate.arm_cas.store(true, Ordering::SeqCst);
    }
    let cap = connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.reference().name == "actions")
        .unwrap()
        .reference()
        .clone();
    let item = pi_harness_runtime::extensions::profiles::bound(
        &pi_extension_protocol::profiles::Registry::bundled().unwrap(),
        "host.queue_item.v1",
        &serde_json::json!({"role":"user","content":"actual CAS guarded item","timestamp":7}),
    )
    .unwrap();
    peer.push(request(
        20,
        Some(parent.request_id),
        cap,
        "queue.next_run",
        dto::QueueNextRunRequest {
            lane: "main".into(),
            item: Some(item),
        }
        .encode_to_vec(),
        0,
    ));
    for _ in 0..100 {
        setup.pump.tick();
        if gate.entered.is_cancelled() {
            break;
        }
    }
    assert!(gate.entered.is_cancelled());
    let expected = if mode == 0 {
        connection.close();
        assert_eq!(
            ids.count.load(Ordering::SeqCst),
            before_ids,
            "parent loss before native check allocates no ID"
        );
        before
    } else {
        let Register::OperationState { key, mut value, .. } = before
            .registers
            .iter()
            .find(|r| matches!(r, Register::OperationState { .. }))
            .unwrap()
            .clone()
        else {
            unreachable!()
        };
        if mode == 2 {
            let OperationState::Run(run) = value.as_mut() else {
                unreachable!()
            };
            run.control = Control::CancelRequested(CancelRequestedControl {
                status: Default::default(),
                requested_at: n(7),
                drained_steer: vec![],
                drained_follow_up: vec![],
            });
        }
        setup
            .pump
            .drive(source_session.commit(Transaction {
                writes: vec![Write::SetRegister(Box::new(RegisterSet::OperationState {
                    key,
                    value,
                }))],
            }))
            .unwrap();
        setup.pump.drive(store.snapshot()).unwrap()
    };
    gate.release.cancel();
    for _ in 0..200 {
        setup.pump.tick();
        if mode == 0
            || peer
                .responses
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.request_id == 20)
        {
            break;
        }
    }
    if mode == 1 {
        assert!(
            gate.refused.load(Ordering::SeqCst) >= 1,
            "actual backend stale prepared CAS refused"
        );
        let response = peer
            .responses
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.request_id == 20)
            .unwrap()
            .clone();
        assert!(matches!(
            response.terminal,
            Some(v1_1::response::Terminal::Result(_))
        ));
        assert_eq!(
            setup
                .pump
                .drive(store.snapshot())
                .unwrap()
                .registers
                .iter()
                .filter(|r| matches!(r, Register::PendingEntry { .. }))
                .count(),
            1
        );
    } else {
        for _ in 0..30 {
            setup.pump.tick();
        }
        assert_eq!(
            setup.pump.drive(store.snapshot()).unwrap(),
            expected,
            "lost parent or current key after actual CAS refusal performs no native write"
        );
        assert_eq!(
            ids.count.load(Ordering::SeqCst),
            before_ids + usize::from(mode == 2),
            "only initially current CAS attempt allocates one ID; refusal retry allocates none"
        );
        if mode == 2 {
            assert!(gate.refused.load(Ordering::SeqCst) >= 1);
            assert!(
                matches!(&peer.responses.lock().unwrap().iter().find(|r|r.request_id==20).unwrap().terminal,Some(v1_1::response::Terminal::Error(error)) if error.code==v1_1::ErrorCode::InvalidParent as i32)
            );
        }
    }
    connection.close();
    let close = setup.pump.drive(harness.close());
    if mode == 0 {
        assert!(
            matches!(close, Err(HarnessRuntimeError::Fault(_))),
            "already observed lost provider retains first Fault"
        );
    } else {
        close.unwrap();
    }
    drop(prompt);
}
#[test]
fn real_memory_actions_parent_loss_and_actual_cas_retry_fences_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for mode in 0..=2 {
                let setup = Setup::new();
                action_cas_consumer(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                    mode,
                );
            }
        }
    }
}
#[test]
fn real_sqlite_actions_parent_loss_and_actual_cas_retry_fences_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for mode in 0..=2 {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                action_cas_consumer(&setup, session, store.clone(), core, minor, mode);
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProcessMode {
    Resource,
    Native,
    InvalidLane,
    Changed,
    ChangedWrongScope,
}
fn accepted_process_consumer<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
    language: &str,
    mode: ProcessMode,
) {
    use pi_harness_runtime::extensions::{
        profiles::{connection::ProfileConnection, resources::ResourceCatalog},
        transport::PumpConfig,
    };
    use runtime_extension_profiles_support::process::{Channel, RealClock, drive};
    use std::num::{NonZeroU64, NonZeroUsize};
    let harness = create(setup, session);
    let profiles = accepted_profiles();
    let invalid_lane = mode == ProcessMode::InvalidLane;
    let peer = if invalid_lane {
        Channel::spawn_invalid_lane(language, core, minor)
    } else if mode == ProcessMode::Changed {
        Channel::spawn_scenario(language, core, minor, "runtime-native-changed")
    } else if mode == ProcessMode::ChangedWrongScope {
        Channel::spawn_scenario(language, core, minor, "runtime-native-changed-wrong-scope")
    } else if mode == ProcessMode::Native {
        Channel::spawn_scenario(language, core, minor, "runtime-native")
    } else {
        Channel::spawn(language, core, minor)
    };
    let clock = Arc::new(RealClock::default());
    let config = PumpConfig {
        max_read_chunk: NonZeroUsize::new(4096).unwrap(),
        write_timeout_ms: NonZeroU64::new(5000).unwrap(),
    };
    let connection = if matches!(mode, ProcessMode::Changed | ProcessMode::ChangedWrongScope) {
        drive(
            setup,
            harness.connect_profile_service(
                service_host(core, profiles),
                peer.clone(),
                clock,
                config,
                reservations(),
                20000,
            ),
        )
    } else {
        drive(
            setup,
            ProfileConnection::connect(
                service_host(core, profiles),
                peer.clone(),
                clock,
                config,
                reservations(),
                20000,
            ),
        )
    }
    .unwrap();
    assert_eq!(connection.grant().profiles().len(), 17);
    assert_eq!(
        connection
            .grant()
            .wire()
            .selected_protocol
            .as_ref()
            .unwrap()
            .minor,
        core
    );
    for profile in connection.grant().profiles() {
        let reference = profile.reference();
        assert_eq!(reference.profile.as_ref().unwrap().minor, minor);
        assert_eq!(reference.handler_id.as_deref(), Some("qualified_handler"));
        assert_eq!(reference.schema_digest_sha256.len(), 32);
    }
    let cap = connection
        .grant()
        .profiles()
        .iter()
        .find(|profile| profile.approved().profile() == "resource.v1")
        .unwrap()
        .reference()
        .clone();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let revision = drive(
        setup,
        harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog),
    )
    .unwrap();
    assert_eq!(revision.revision(), "r1");
    assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
    assert_eq!(peer.requests(), 2);
    let requests = peer.request_frames();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["resource.list", "resource.read"]
    );
    for request in &requests {
        assert_eq!(request.capability_ref.as_ref(), Some(&cap));
        assert_eq!(request.initial_stream_credit_bytes, Some(0));
        assert_ne!(request.request_id, 0);
    }
    drive(
        setup,
        harness.skill(
            "s".into(),
            Some("actual accepted peer native consumer".into()),
        ),
    )
    .unwrap()
    .unwrap();
    let message = setup.faux.requests.lock().unwrap()[0].0.messages[0].clone();
    assert!(
        matches!(message,pi_ai_core::Message::User(message) if matches!(&message.content,pi_ai_core::UserContent::Text(text) if text.contains("inert/path")&&text.ends_with("actual accepted peer native consumer")))
    );
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(snapshot.usage.len(), 1);
    let usage = &snapshot.usage[0];
    assert!(!usage.adjustment);
    let linked = usage.entry_id.as_ref().expect("native skill usage entry");
    assert!(snapshot.entries.iter().any(|entry| matches!(entry, Entry::Message(message) if &message.id == linked && matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_))))));
    assert!(snapshot.entries.iter().any(|entry|matches!(entry,Entry::Message(entry) if matches!(&entry.message,AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_))))));
    if mode != ProcessMode::Resource {
        let names = [
            "before_run",
            "before_request",
            "transform_context",
            "after_response",
            "before_run_end",
        ];
        for name in names {
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|profile| profile.reference().name == name)
                .unwrap()
                .reference()
                .clone();
            harness
                .register_profile_hook(
                    RemoteHook::new(connection.clone(), selected, Default::default()).unwrap(),
                )
                .unwrap();
        }
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "runtime_extension_profiles_support/profiles-runtime-v1.json"
        ))
        .unwrap();
        let remote: pi_ai_core::Model = serde_json::from_value(
            corpus["methods"]["provider.invoke"]["request"]["model"]["value"].clone(),
        )
        .unwrap();
        let provider = connection
            .grant()
            .profiles()
            .iter()
            .find(|profile| profile.reference().name == "remote")
            .unwrap()
            .reference()
            .clone();
        drive(
            setup,
            harness.register_profile_provider(
                connection.clone(),
                provider,
                vec![remote.clone()],
                Default::default(),
                None,
            ),
        )
        .unwrap();
        drive(setup, harness.set_model(remote)).unwrap();
        let call = drive(
            setup,
            harness.prompt(PromptInput::Text {
                text: "native profile peer dynamic run".into(),
                images: vec![],
            }),
        );
        if invalid_lane {
            assert!(
                !matches!(call, Ok(Ok(_))),
                "invalid dynamic lane was accepted"
            );
            let actual = peer.request_frames();
            assert_eq!(actual.last().unwrap().method, "before_run");
            let hook = pi_extension_protocol::profiles::wire_v1_1::InvokeHookRequest::decode(
                actual.last().unwrap().payload.as_slice(),
            )
            .unwrap();
            assert_eq!(
                wire_value(hook.input.unwrap().value.unwrap())["event"]["lane"],
                "wrong-lane"
            );
            peer.finish(false);
            assert!(peer.stderr_text().contains("runtime hook event"));
            connection.close();
            let _ = setup.pump.drive(harness.close());
            return;
        }
        if call.is_err() {
            peer.finish(false);
            panic!(
                "native profile process failed: {call:?}; wire={:?}; faux={}",
                peer.request_frames()
                    .iter()
                    .map(|request| request.method.as_str())
                    .collect::<Vec<_>>(),
                setup.faux.requests.lock().unwrap().len()
            );
        }
        let completion = call.unwrap().unwrap();
        assert!(matches!(
            completion.outcome,
            RunOutcome::CompletedWithAssistant(_)
        ));
        let wire = peer.request_frames();
        assert_eq!(
            wire.iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            [
                "resource.list",
                "resource.read",
                "before_run",
                "before_request",
                "transform_context",
                "provider.invoke",
                "after_response",
                "before_run_end"
            ]
        );
        let mut ids = std::collections::BTreeSet::new();
        for request in &wire {
            assert!(ids.insert(request.request_id));
            assert_ne!(request.request_id, 0);
            assert_eq!(request.initial_stream_credit_bytes, Some(0));
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|profile| {
                    profile.reference().name == request.capability_ref.as_ref().unwrap().name
                })
                .unwrap()
                .reference();
            assert_eq!(request.capability_ref.as_ref(), Some(selected));
            assert_eq!(
                request.parent_request_id,
                (request.method == "after_response").then_some(wire[5].request_id)
            );
        }
        assert_eq!(
            wire[5].effect_key.as_ref().unwrap().operation_id,
            completion.run_id.as_str()
        );
        assert_eq!(setup.faux.requests.lock().unwrap().len(), 1);
        let after = setup.pump.drive(store.snapshot()).unwrap();
        assert_eq!(after.usage.len(), 2);
        assert_ne!(after.usage[0].entry_id, after.usage[1].entry_id);
        for usage in &after.usage {
            assert!(!usage.adjustment);
            let linked = usage
                .entry_id
                .as_ref()
                .expect("assistant-linked native usage");
            assert!(after.entries.iter().any(|entry| matches!(entry, Entry::Message(message) if &message.id == linked && matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_))))));
        }
        if matches!(mode, ProcessMode::Changed | ProcessMode::ChangedWrongScope) {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            let baseline = setup.pump.drive(store.snapshot()).unwrap();
            assert!(!catalog.refresh_pending());
            let refreshed = drive(
                setup,
                harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog),
            );
            let peer_requests = peer.peer_request_frames();
            assert_eq!(peer_requests.len(), 1);
            let changed = &peer_requests[0];
            assert_eq!(changed.method, "resource.changed");
            assert_eq!(changed.request_id, 2000);
            assert_eq!(changed.capability_ref.as_ref(), Some(&cap));
            assert_eq!(changed.parent_request_id, None);
            assert_eq!(changed.effect_key, None);
            assert_eq!(changed.initial_stream_credit_bytes, Some(0));
            let payload = dto::ResourceChanged::decode(changed.payload.as_slice()).unwrap();
            assert_eq!(
                payload.scope,
                if mode == ProcessMode::Changed {
                    "allowed"
                } else {
                    "wrong-scope"
                }
            );
            if mode == ProcessMode::ChangedWrongScope {
                assert!(refreshed.is_err(), "wrong scope cannot complete refresh");
                assert!(!catalog.refresh_pending());
                assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), baseline);
                assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
                connection.close();
                peer.finish(false);
                let _ = setup.pump.drive(harness.close());
                return;
            }
            let second = refreshed.unwrap();
            assert_eq!(second.revision(), "r1");
            assert!(!Arc::ptr_eq(&revision, &second));
            assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
            assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &second));
            assert!(
                catalog.refresh_pending(),
                "changed during fetch survives install"
            );
            assert!(
                connection.current(),
                "selected service remains live through install"
            );
            assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), baseline);
            let responses = peer.response_frames();
            assert_eq!(responses.len(), 1);
            assert_eq!(responses[0].request_id, 2000);
            let Some(v1_1::response::Terminal::Result(body)) = &responses[0].terminal else {
                panic!("selected changed acknowledgement")
            };
            dto::ResourceChangedAck::decode(body.as_slice()).unwrap();
            let wire = peer.request_frames();
            assert_eq!(
                wire.iter().map(|r| r.method.as_str()).collect::<Vec<_>>(),
                [
                    "resource.list",
                    "resource.read",
                    "before_run",
                    "before_request",
                    "transform_context",
                    "provider.invoke",
                    "after_response",
                    "before_run_end",
                    "resource.list",
                    "resource.read"
                ]
            );
            let mut all_ids = std::collections::BTreeSet::new();
            for request in &wire {
                assert!(all_ids.insert(request.request_id));
            }
            for request in &wire[8..] {
                assert_eq!(request.capability_ref.as_ref(), Some(&cap));
                assert_eq!(request.initial_stream_credit_bytes, Some(0));
                assert_eq!(request.parent_request_id, None);
            }
        }
    }
    assert_eq!(connection.occupancy(), 0);
    peer.finish(true);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn accepted_node_python_real_memory_resource_native_consumers_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                let setup = Setup::new();
                accepted_process_consumer(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                    language,
                    ProcessMode::Resource,
                );
            }
        }
    }
}
#[test]
fn accepted_node_python_real_sqlite_resource_native_consumers_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                accepted_process_consumer(
                    &setup,
                    session,
                    store.clone(),
                    core,
                    minor,
                    language,
                    ProcessMode::Resource,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_memory_profile_peer_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                let setup = Setup::new();
                accepted_process_consumer(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                    language,
                    ProcessMode::Native,
                );
            }
        }
    }
}
#[test]
fn runtime_native_sqlite_profile_peer_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                accepted_process_consumer(
                    &setup,
                    session,
                    store.clone(),
                    core,
                    minor,
                    language,
                    ProcessMode::Native,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_rejects_corrupted_before_run_lane() {
    for language in ["typescript", "python"] {
        let setup = Setup::new();
        accepted_process_consumer(
            &setup,
            setup.session.clone(),
            setup.store.clone(),
            0,
            0,
            language,
            ProcessMode::InvalidLane,
        );
    }
}
#[test]
fn runtime_native_changed_memory_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                let setup = Setup::new();
                accepted_process_consumer(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                    language,
                    ProcessMode::Changed,
                );
            }
        }
    }
}
#[test]
fn runtime_native_changed_sqlite_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                accepted_process_consumer(
                    &setup,
                    session,
                    store.clone(),
                    core,
                    minor,
                    language,
                    ProcessMode::Changed,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_changed_wrong_scope_memory_no_write() {
    for language in ["typescript", "python"] {
        let setup = Setup::new();
        accepted_process_consumer(
            &setup,
            setup.session.clone(),
            setup.store.clone(),
            0,
            0,
            language,
            ProcessMode::ChangedWrongScope,
        );
    }
}
#[test]
fn runtime_native_changed_wrong_scope_sqlite_no_write() {
    for language in ["typescript", "python"] {
        let setup = Setup::new();
        let (session, store, dir) = sqlite_case(&setup);
        accepted_process_consumer(
            &setup,
            session,
            store.clone(),
            0,
            0,
            language,
            ProcessMode::ChangedWrongScope,
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

fn suspended_snapshot<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) -> pi_agent_session::storage::StorageSnapshot {
    let harness = create(setup, session);
    let (connection, peer) = install_named(
        setup,
        &harness,
        core,
        minor,
        &[
            "before_resume",
            "before_request",
            "after_response",
            "before_run_end",
        ],
    );
    let (resources, _, _) = runtime_extension_profiles_support::connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    let catalog =
        pi_harness_runtime::extensions::profiles::resources::ResourceCatalog::new("allowed".into())
            .unwrap();
    setup
        .pump
        .drive(harness.refresh_profile_resources(
            resources.clone(),
            resources.grant().profiles()[0].reference().clone(),
            &catalog,
        ))
        .unwrap();
    peer.results
        .lock()
        .unwrap()
        .insert("provider_mode".into(), Some(serde_json::json!("deferred")));
    let result = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "actual durable restored deferred source".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(result.outcome, RunOutcome::Deferred(_)));
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    connection.close();
    resources.close();
    setup.pump.drive(harness.close()).unwrap();
    snapshot
}
fn restored_consumer<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
    wrong_source: bool,
) {
    let before = setup.pump.drive(store.snapshot()).unwrap();
    let factory = RuntimeFactory::new(
        setup.time.clone(),
        setup.pump.clone(),
        setup.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let created = setup.pump.drive(
        factory.create_configured(setup.options_for::<(), _>(session, DriveMode::Automatic)),
    );
    let harness = match created {
        Ok(created) => created.harness,
        Err(HarnessRuntimeError::Fault(fault)) if wrong_source => {
            assert_eq!(
                setup.pump.drive(store.snapshot()).unwrap(),
                before,
                "wrong source restore refuses before native work"
            );
            eprintln!("wrong source restore refused: {fault}");
            return;
        }
        Err(error) => panic!("restore wrong={wrong_source} core={core} minor={minor}: {error}"),
    };
    let (connection, peer) = install_named(
        setup,
        &harness,
        core,
        minor,
        &[
            "before_resume",
            "before_request",
            "after_response",
            "before_run_end",
        ],
    );
    let result = setup.pump.drive(harness.resume());
    if wrong_source {
        assert!(matches!(result, Err(HarnessRuntimeError::Fault(_))));
        assert_eq!(
            setup.pump.drive(store.snapshot()).unwrap(),
            before,
            "wrong actual stored source fails before provider intent/backend mutation"
        );
        assert!(
            !peer
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.method == "provider.invoke")
        );
        connection.close();
        assert!(matches!(
            setup.pump.drive(harness.close()),
            Err(HarnessRuntimeError::Fault(_))
        ));
        return;
    }
    assert!(matches!(
        result,
        Ok(Ok(ResumeOutcome::RunCompletedWithAssistant(_)))
    ));
    let fetched = peer
        .requests
        .lock()
        .unwrap()
        .iter()
        .find(|r| r.method == "provider.invoke")
        .unwrap()
        .clone();
    assert_eq!(fetched.effect_key.as_ref().unwrap().effect_kind, "deferred");
    let body = pi_extension_protocol::profiles::wire_v1_1::InvokeProviderRequest::decode(
        fetched.payload.as_slice(),
    )
    .unwrap();
    let source = before
        .entries
        .iter()
        .find_map(|entry| match entry {
            Entry::Message(entry) => match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message))
                    if message.deferred.is_some() =>
                {
                    Some(message)
                }
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert_eq!(
        wire_value(body.context.unwrap().value.unwrap())["messages"],
        serde_json::json!([source])
    );
    let after = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(after.usage.len(), before.usage.len() + 1);
    assert!(
        setup
            .pump
            .drive(harness.skill("remote-skill".into(), None))
            .unwrap()
            .is_err(),
        "restored native runtime has no unproved remote resource revision"
    );
    assert_eq!(
        setup.pump.drive(store.snapshot()).unwrap(),
        after,
        "unproved restored resource does no native write"
    );
    let (resources, resource_peer, _) = runtime_extension_profiles_support::connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    resource_peer
        .results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    let catalog =
        pi_harness_runtime::extensions::profiles::resources::ResourceCatalog::new("allowed".into())
            .unwrap();
    let repinned = setup
        .pump
        .drive(harness.refresh_profile_resources(
            resources.clone(),
            resources.grant().profiles()[0].reference().clone(),
            &catalog,
        ))
        .unwrap();
    assert_eq!(repinned.get("skill-id", None).unwrap().value, "world");
    setup
        .pump
        .drive(harness.skill("remote-skill".into(), None))
        .unwrap()
        .unwrap();
    assert_eq!(
        setup.pump.drive(store.snapshot()).unwrap().usage.len(),
        after.usage.len() + 1
    );
    connection.close();
    resources.close();
    setup.pump.drive(harness.close()).unwrap();
}
fn wrong_source_snapshot(
    mut snapshot: pi_agent_session::storage::StorageSnapshot,
) -> pi_agent_session::storage::StorageSnapshot {
    let id = snapshot
        .entries
        .iter()
        .find_map(|entry| match entry {
            Entry::Message(entry)
                if matches!(
                    entry.message,
                    AgentMessage::BuiltIn(pi_ai_core::Message::User(_))
                ) =>
            {
                Some(entry.id.clone())
            }
            _ => None,
        })
        .unwrap();
    for register in &mut snapshot.registers {
        if let Register::OperationState { value, .. } = register
            && let OperationState::Run(run) = value.as_mut()
            && let RunPhase::Deferred(phase) = &mut run.phase
        {
            match &mut phase.deferred {
                Deferred::Suspended {
                    source_entry_id, ..
                }
                | Deferred::EffectPending {
                    source_entry_id, ..
                } => *source_entry_id = id.clone(),
            }
        }
    }
    snapshot
}
#[test]
fn real_memory_restore_deferred_actual_source_and_resource_repin_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for wrong in [false, true] {
                let setup = Setup::new();
                let snapshot = suspended_snapshot(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                );
                let snapshot = if wrong {
                    wrong_source_snapshot(snapshot)
                } else {
                    snapshot
                };
                let backing = Arc::new(
                    pi_agent_session::memory::MemoryBacking::from_snapshot(snapshot).unwrap(),
                );
                let store = backing.open(setup.time.clone()).unwrap();
                let codec = Arc::new(
                    SessionCodec::new(
                        Default::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                );
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
                restored_consumer(&setup, session, store, core, minor, wrong);
            }
        }
    }
}
#[test]
fn real_sqlite_restore_deferred_actual_source_and_resource_repin_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for wrong in [false, true] {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                let snapshot = suspended_snapshot(&setup, session, store.clone(), core, minor);
                drop(store);
                let snapshot = if wrong {
                    wrong_source_snapshot(snapshot)
                } else {
                    snapshot
                };
                let codec = Arc::new(
                    SessionCodec::new(
                        Default::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                );
                let store = pi_agent_sqlite::SqliteStorage::create_from_snapshot(
                    dir.join("restored.db"),
                    setup.session.metadata(),
                    snapshot,
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
                restored_consumer(&setup, session, store.clone(), core, minor, wrong);
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}

fn retained_loss_consumer<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    let harness = create(setup, session);
    let (connection, peer) = install_named(
        setup,
        &harness,
        core,
        minor,
        &["after_response", "before_run_end"],
    );
    peer.results
        .lock()
        .unwrap()
        .insert("loss_after_terminal".into(), Some(serde_json::json!(true)));
    let result = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "accepted first terminal before optional hook transport loss".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    assert_eq!(snapshot.usage.len(), 1);
    assert_eq!(snapshot.usage[0].usage.total_tokens.get(), 5);
    assert_eq!(snapshot.entries.iter().filter(|entry|matches!(entry,Entry::Message(entry) if matches!(entry.message,AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_))))).count(),1);
    assert_eq!(
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        ["provider.invoke"]
    );
    assert_eq!(connection.occupancy(), 0);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn real_memory_first_terminal_survives_optional_hook_transport_loss_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            retained_loss_consumer(
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
fn real_sqlite_first_terminal_survives_optional_hook_transport_loss_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            retained_loss_consumer(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
#[test]
fn actual_native_validator_final_clock_sample_precedes_first_receipt() {
    use pi_extension_protocol::{
        endpoint::{ActiveFrame, SelectedEnvelope},
        profile_live::{Owner, Phase, Role, Start},
        profiles::{Direction, NativeError, NativeValidator, ValidatedPayload, wire_v1_1 as dto},
        wire::{framing, v1_1},
    };
    use std::num::NonZeroUsize;
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (connection, _, clock) = runtime_extension_profiles_support::connection(
                &setup,
                core,
                minor,
                vec![profile("provider.v1", setup.faux.model.provider.as_str())],
            );
            let cap = connection.grant().profiles()[0].reference().clone();
            let mut owner =
                Owner::new(connection.grant().clone(), reservations(), 0, 10000).unwrap();
            let registry = connection.grant().profiles()[0].approved().registry();
            let payload = dto::InvokeProviderRequest {
                model_id: setup.faux.model.id.as_str().into(),
                auth_mode: "none".into(),
                model: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        registry,
                        "provider.model.v1",
                        &serde_json::to_value(&setup.faux.model).unwrap(),
                    )
                    .unwrap(),
                ),
                context: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        registry,
                        "provider.context.v1",
                        &serde_json::json!({"messages":[]}),
                    )
                    .unwrap(),
                ),
                tools: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        registry,
                        "provider.tools.v1",
                        &serde_json::json!([]),
                    )
                    .unwrap(),
                ),
                options: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        registry,
                        "provider.options.v1",
                        &serde_json::json!({}),
                    )
                    .unwrap(),
                ),
            }
            .encode_to_vec();
            let policy = Default::default();
            let mut native = pi_harness_runtime::extensions::profiles::native::NativeDomain::new(
                setup.session.codec(),
                &policy,
            );
            let key = v1_1::EffectKey {
                session_id: "runtime-session".into(),
                lane: "main".into(),
                operation_id: "native-clock-api-test".into(),
                effect_kind: "assistant".into(),
                step_or_task_id: "native-step".into(),
                logical_attempt: 1,
                item_index: None,
            };
            let (binding, _) = owner
                .start(
                    Start {
                        capability: cap,
                        method: "provider.invoke".into(),
                        direction: Direction::CoreToExtension,
                        role: Role::Planned,
                        key: Some(key),
                        parent: None,
                        scope: "main".into(),
                        payload,
                        now_ms: 0,
                        requested_deadline_ms: Some(500),
                        method_default_ms: 500,
                        method_limit_ms: 500,
                        initial_credit: Some(0),
                        generation: 7,
                    },
                    &mut native,
                )
                .unwrap();
            owner.dispatched_at(&binding, 0).unwrap();
            let message = assistant(&setup.faux.model, pi_ai_core::StopReason::Stop);
            let result = dto::InvokeProviderResult {
                assistant_message: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        registry,
                        "provider.assistant_message.v1",
                        &serde_json::to_value(&message).unwrap(),
                    )
                    .unwrap(),
                ),
                usage: Some(
                    pi_harness_runtime::extensions::profiles::bound(
                        registry,
                        "provider.usage.v1",
                        &serde_json::to_value(&message.usage).unwrap(),
                    )
                    .unwrap(),
                ),
            }
            .encode_to_vec();
            let envelope = v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                    request_id: binding.id(),
                    terminal: Some(v1_1::stream_end::Terminal::Result(result)),
                })),
            };
            let encoded = framing::encode(&envelope, NonZeroUsize::new(262144).unwrap()).unwrap();
            let decoded = if core == 0 {
                SelectedEnvelope::V1_0(
                    pi_extension_protocol::wire::v1_0::Envelope::decode(
                        &encoded.bytes[encoded.bytes.len() - envelope.encoded_len()..],
                    )
                    .unwrap(),
                )
            } else {
                SelectedEnvelope::V1_1(envelope)
            };
            let frame = ActiveFrame {
                envelope: decoded,
                body_len: encoded.body_len,
            };
            struct Advancing<'a> {
                native: pi_harness_runtime::extensions::profiles::native::NativeDomain<'a>,
                clock: Arc<Mono>,
            }
            impl NativeValidator for Advancing<'_> {
                fn validate(&mut self, payload: &ValidatedPayload) -> Result<(), NativeError> {
                    self.native.validate(payload)?;
                    self.clock.0.store(500, Ordering::SeqCst);
                    Ok(())
                }
            }
            let mut advancing = Advancing {
                native,
                clock: clock.clone(),
            };
            let mut frame = frame;
            frame.body_len = encoded.body_len;
            assert!(matches!(
                owner.receive_checked(7, frame, 0, &mut advancing, || clock
                    .0
                    .load(Ordering::SeqCst)),
                Err(pi_extension_protocol::profile_live::Error::Deadline)
            ));
            assert!(owner.receipt(&binding).is_none());
            assert_eq!(owner.phase(&binding), Some(Phase::Abandoned));
            connection.close();
        }
    }
}

fn resource_receipt_failures<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
    scenario: ReceiptScenario,
) {
    use pi_harness_runtime::extensions::profiles::resources::ResourceCatalog;
    let harness = create(setup, session);
    let (connection, peer) = receipt_connection(
        setup,
        core,
        minor,
        vec![
            profile("resource.v1", "resources"),
            profile("telemetry-sink.v1", "telemetry-recovery"),
        ],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let first = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog))
        .unwrap();
    assert_eq!(connection.occupancy(), 0);
    let native_first = setup.pump.drive(harness.get_resources()).unwrap();
    let before = setup.pump.drive(store.snapshot()).unwrap();
    catalog.changed("allowed").unwrap();
    peer.peer
        .results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    peer.set(scenario);
    for attempt in 1..=if matches!(
        scenario,
        ReceiptScenario::InvalidRead | ReceiptScenario::DuplicateManifest
    ) {
        1
    } else {
        17
    } {
        let at = peer.peer.requests.lock().unwrap().len();
        assert!(
            setup
                .pump
                .drive(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog))
                .is_err()
        );
        assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &first));
        assert!(catalog.refresh_pending());
        assert_eq!(
            setup.pump.drive(harness.get_resources()).unwrap(),
            native_first
        );
        assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
        assert!(
            connection.current(),
            "completed {scenario:?} consumer attempt {attempt} leaves its connection reusable"
        );
        assert_eq!(
            connection.occupancy(),
            0,
            "completed {scenario:?} consumer stranded receipts on attempt {attempt}"
        );
        let requests = peer.peer.requests.lock().unwrap();
        assert_eq!(requests[at].method, "resource.list");
        if matches!(
            scenario,
            ReceiptScenario::LaterNotFound | ReceiptScenario::InvalidRead
        ) {
            assert_eq!(
                requests[at..]
                    .iter()
                    .map(|r| r.method.as_str())
                    .collect::<Vec<_>>(),
                ["resource.list", "resource.read", "resource.read"]
            );
        }
    }
    peer.set(ReceiptScenario::Healthy);
    let (recovery, cap) = if matches!(
        scenario,
        ReceiptScenario::InvalidRead | ReceiptScenario::DuplicateManifest
    ) {
        use pi_telemetry::TelemetryContext;
        let sink = pi_harness_runtime::extensions::profiles::telemetry::ProjectedTelemetry::new(
            Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            1048576,
            256,
        )
        .unwrap();
        sink.start_span(pi_telemetry::SpanOptions {
            name: "pi.harness.run".into(),
            attributes: Default::default(),
        })
        .finish();
        assert!(setup.pump.drive(harness.deliver_profile_telemetry(
            connection.clone(),
            connection.grant().profiles()[1].reference().clone(),
            &sink
        )));
        assert_eq!(connection.occupancy(), 0);
        assert!(
            connection.current(),
            "another approved capability still uses the same physical connection"
        );
        let (fresh, fresh_peer) = receipt_connection(
            setup,
            core,
            minor,
            vec![profile("resource.v1", "resources")],
        );
        fresh_peer
            .peer
            .results
            .lock()
            .unwrap()
            .insert("resource_phase".into(), Some(serde_json::json!(2)));
        let cap = fresh.grant().profiles()[0].reference().clone();
        (fresh, cap)
    } else {
        (connection.clone(), cap)
    };
    let second = setup
        .pump
        .drive(harness.refresh_profile_resources(recovery.clone(), cap, &catalog))
        .unwrap();
    assert_eq!(connection.occupancy(), 0);
    assert!(connection.current());
    assert_eq!(first.resources(), native_first);
    assert_eq!(second.resources().skills.unwrap()[0].content, "world");
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        second.resources()
    );
    assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &second));
    assert!(!catalog.refresh_pending());
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
    recovery.close();
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn new_memory_resource_failed_consumers_retire_all_acquired_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for scenario in [
                ReceiptScenario::LaterNotFound,
                ReceiptScenario::InvalidRead,
                ReceiptScenario::DuplicateManifest,
                ReceiptScenario::ListNotFound,
            ] {
                let setup = Setup::new();
                resource_receipt_failures(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                    scenario,
                );
            }
        }
    }
}
#[test]
fn new_sqlite_resource_failed_consumers_retire_all_acquired_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for scenario in [
                ReceiptScenario::LaterNotFound,
                ReceiptScenario::InvalidRead,
                ReceiptScenario::DuplicateManifest,
                ReceiptScenario::ListNotFound,
            ] {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                resource_receipt_failures(&setup, session, store.clone(), core, minor, scenario);
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
fn resource_receipt_abandonment<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
    mode: u8,
) {
    use pi_harness_runtime::extensions::profiles::resources::ResourceCatalog;
    let harness = create(setup, session);
    let (connection, peer) = receipt_connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let first = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog))
        .unwrap();
    let native_first = setup.pump.drive(harness.get_resources()).unwrap();
    let before = setup.pump.drive(store.snapshot()).unwrap();
    peer.peer
        .results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    catalog.changed("allowed").unwrap();
    if mode == 1 {
        peer.set(ReceiptScenario::PanicLaterRead);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            setup
                .pump
                .drive(harness.refresh_profile_resources(connection.clone(), cap, &catalog))
        }));
        assert!(result.is_err());
    } else {
        peer.set(ReceiptScenario::HoldLaterRead);
        let mut refresh =
            Box::pin(harness.refresh_profile_resources(connection.clone(), cap, &catalog));
        assert!(poll(refresh.as_mut()).is_pending());
        assert_eq!(
            peer.peer.requests.lock().unwrap().last().unwrap().method,
            "resource.read"
        );
        assert_eq!(
            connection.occupancy(),
            3,
            "two completed receipts plus the pending later read"
        );
        if mode == 2 {
            let mut close = Box::pin(harness.close());
            assert!(
                poll(close.as_mut()).is_pending(),
                "seal actual native settings admission before the install"
            );
            peer.set(ReceiptScenario::Healthy);
            peer.peer.release();
            assert!(
                setup.pump.drive(refresh).is_err(),
                "native resource install must refuse the sealed owner"
            );
            assert!(
                Arc::ptr_eq(&catalog.capture().unwrap(), &first),
                "refused native install must keep the original catalog capture"
            );
            assert!(catalog.refresh_pending());
            assert_eq!(
                connection.occupancy(),
                0,
                "install refusal must retire all completed resource replies"
            );
            assert!(connection.current());
            assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
            connection.close();
            setup.pump.drive(close).unwrap();
            return;
        }
        drop(refresh);
    }
    assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &first));
    assert!(catalog.refresh_pending());
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        native_first
    );
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
    assert_eq!(
        connection.occupancy(),
        0,
        "dropped or unwound consumer must retire its earlier completed replies"
    );
    // Pending-call abandonment keeps the existing transport closure behavior.
    assert!(!connection.current());
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn new_memory_resource_drop_unwind_and_install_refusal_release_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for mode in 0..=2 {
                let setup = Setup::new();
                resource_receipt_abandonment(
                    &setup,
                    setup.session.clone(),
                    setup.store.clone(),
                    core,
                    minor,
                    mode,
                );
            }
        }
    }
}
#[test]
fn new_sqlite_resource_drop_unwind_and_install_refusal_release_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            for mode in 0..=2 {
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                resource_receipt_abandonment(&setup, session, store.clone(), core, minor, mode);
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
fn telemetry_receipt_failures<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_harness_runtime::extensions::profiles::telemetry::ProjectedTelemetry;
    use pi_telemetry::TelemetryContext;
    let harness = create(setup, session);
    let (connection, peer) = receipt_connection(
        setup,
        core,
        minor,
        vec![
            profile("telemetry-sink.v1", "telemetry"),
            profile("resource.v1", "resources-recovery"),
        ],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    let before = setup.pump.drive(store.snapshot()).unwrap();
    let sink = || {
        ProjectedTelemetry::new(Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT), 1048576, 256)
            .unwrap()
    };
    let empty = sink();
    assert!(!setup.pump.drive(harness.flush_profile_telemetry(
        connection.clone(),
        cap.clone(),
        &empty,
        0
    )));
    assert!(empty.disabled(), "unemitted ack remains a native refusal");
    assert_eq!(
        connection.occupancy(),
        0,
        "valid terminal rejected by native flush ack must retire"
    );
    let healthy = sink();
    healthy
        .start_span(pi_telemetry::SpanOptions {
            name: "pi.harness.run".into(),
            attributes: Default::default(),
        })
        .finish();
    assert!(setup.pump.drive(harness.deliver_profile_telemetry(
        connection.clone(),
        cap.clone(),
        &healthy
    )));
    let request = peer.peer.requests.lock().unwrap().last().unwrap().clone();
    let batch = pi_extension_protocol::profiles::wire_v1_1::TelemetryEmitBatchRequest::decode(
        request.payload.as_slice(),
    )
    .unwrap();
    assert!(setup.pump.drive(harness.flush_profile_telemetry(
        connection.clone(),
        cap.clone(),
        &healthy,
        batch.sequence
    )));
    assert!(!healthy.disabled());
    assert_eq!(connection.occupancy(), 0);
    for scenario in [
        ReceiptScenario::TelemetryError,
        ReceiptScenario::TelemetryWrongAck,
    ] {
        let failed = sink();
        failed
            .start_span(pi_telemetry::SpanOptions {
                name: "pi.harness.run".into(),
                attributes: Default::default(),
            })
            .finish();
        peer.set(scenario);
        assert!(!setup.pump.drive(harness.deliver_profile_telemetry(
            connection.clone(),
            cap.clone(),
            &failed
        )));
        assert!(failed.disabled());
        assert!(connection.current());
        assert_eq!(
            connection.occupancy(),
            0,
            "rejected {scenario:?} telemetry consumer must retire"
        );
        assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
    }
    peer.set(ReceiptScenario::Healthy);
    let catalog =
        pi_harness_runtime::extensions::profiles::resources::ResourceCatalog::new("allowed".into())
            .unwrap();
    setup
        .pump
        .drive(harness.refresh_profile_resources(
            connection.clone(),
            connection.grant().profiles()[1].reference().clone(),
            &catalog,
        ))
        .unwrap();
    assert!(connection.current());
    assert_eq!(connection.occupancy(), 0);
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn new_memory_telemetry_rejected_consumers_retire_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            telemetry_receipt_failures(
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
fn new_sqlite_telemetry_rejected_consumers_retire_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            telemetry_receipt_failures(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[test]
fn new_memory_resource_install_refusal_keeps_catalog_and_releases_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            resource_receipt_abandonment(
                &setup,
                setup.session.clone(),
                setup.store.clone(),
                core,
                minor,
                2,
            );
        }
    }
}
#[test]
fn new_sqlite_resource_install_refusal_keeps_catalog_and_releases_receipts_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            resource_receipt_abandonment(&setup, session, store.clone(), core, minor, 2);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn resource_completed_consumer_drop<S: RuntimeStorage + 'static>(
    setup: &Setup,
    source: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_harness_runtime::extensions::profiles::resources::ResourceCatalog;
    use runtime_extension_profiles_support::storage::GateStorage;
    let gate = Arc::new(GateStorage::new(store.clone()));
    let session = Arc::new(
        setup
            .pump
            .drive(RuntimeSession::new(
                gate.clone(),
                source.metadata(),
                Arc::new(
                    SessionCodec::new(
                        Default::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                setup.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let harness = create(setup, session);
    let (connection, peer) = receipt_connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let first = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog))
        .unwrap();
    let native_first = setup.pump.drive(harness.get_resources()).unwrap();
    // The admitted lane creation parks during hydration while it owns settings.
    gate.arm_read.store(true, Ordering::SeqCst);
    let mut lane = Box::pin(harness.create_lane(LaneId::new("install-blocker").unwrap(), None));
    assert!(poll(lane.as_mut()).is_pending());
    setup.pump.tick();
    assert!(
        gate.entered.is_cancelled(),
        "actual native settings holder reached its parked hydration"
    );
    let before = setup.pump.drive(store.snapshot()).unwrap();
    catalog.changed("allowed").unwrap();
    peer.peer
        .results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    let mut refresh =
        Box::pin(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog));
    assert!(poll(refresh.as_mut()).is_pending());
    assert_eq!(
        peer.peer.requests.lock().unwrap().len(),
        6,
        "all three new resource terminals were actually received"
    );
    assert_eq!(
        connection.occupancy(),
        3,
        "retain all replies while native install remains possible"
    );
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        native_first
    );
    drop(refresh);
    assert!(
        Arc::ptr_eq(&catalog.capture().unwrap(), &first),
        "consumer Drop before native install preserves the old catalog"
    );
    assert!(catalog.refresh_pending());
    assert_eq!(
        connection.occupancy(),
        0,
        "true completed-consumer Drop retires its three receipts"
    );
    assert!(
        connection.current(),
        "completed replies require no connection closure for cleanup"
    );
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        native_first
    );
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before);
    gate.release.cancel();
    setup.pump.drive(lane).unwrap().unwrap();
    let second = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap, &catalog))
        .unwrap();
    assert_eq!(second.resources().skills.unwrap()[0].content, "world");
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        second.resources()
    );
    assert_eq!(connection.occupancy(), 0);
    assert!(connection.current());
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}
#[test]
fn new_memory_resource_completed_consumer_drop_before_install_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            resource_completed_consumer_drop(
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
fn new_sqlite_resource_completed_consumer_drop_before_install_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            resource_completed_consumer_drop(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

fn resource_change_during_native_install<S: RuntimeStorage + 'static>(
    setup: &Setup,
    source: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    core: u32,
    minor: u32,
) {
    use pi_harness_runtime::extensions::profiles::resources::ResourceCatalog;
    use runtime_extension_profiles_support::storage::GateStorage;

    let gate = Arc::new(GateStorage::new(store.clone()));
    let session = Arc::new(
        setup
            .pump
            .drive(RuntimeSession::new(
                gate.clone(),
                source.metadata(),
                Arc::new(
                    SessionCodec::new(
                        Default::default(),
                        ProviderHeaderPolicy::from_allowlist(["x-public".into()]),
                    )
                    .unwrap(),
                ),
                setup.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let harness = create(setup, session);
    let (connection, peer) = receipt_connection(
        setup,
        core,
        minor,
        vec![profile("resource.v1", "resources")],
    );
    let cap = connection.grant().profiles()[0].reference().clone();
    let catalog = ResourceCatalog::new("allowed".into()).unwrap();
    let first = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog))
        .unwrap();
    let first_native = setup.pump.drive(harness.get_resources()).unwrap();
    assert_eq!(first_native, first.resources());
    assert!(!catalog.refresh_pending());

    // A native lane holds settings while it hydrates from the parked storage read.
    gate.arm_read.store(true, Ordering::SeqCst);
    let mut lane = Box::pin(harness.create_lane(LaneId::new("install-blocker").unwrap(), None));
    assert!(poll(lane.as_mut()).is_pending());
    setup.pump.tick();
    assert!(gate.entered.is_cancelled());

    catalog.changed("allowed").unwrap();
    peer.peer
        .results
        .lock()
        .unwrap()
        .insert("resource_phase".into(), Some(serde_json::json!(2)));
    let mut refresh =
        Box::pin(harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog));
    assert!(poll(refresh.as_mut()).is_pending());
    assert_eq!(peer.peer.requests.lock().unwrap().len(), 6);
    assert_eq!(connection.occupancy(), 3);
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        first_native
    );
    assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &first));

    // These replies describe revision 2. This later notification cannot be
    // represented by that completed fetch, even though native install is pending.
    catalog.changed("allowed").unwrap();
    gate.release.cancel();
    setup.pump.drive(lane).unwrap().unwrap();
    let second = setup.pump.drive(refresh).unwrap();
    assert_eq!(second.revision(), "revision-2");
    assert_eq!(
        second.resources().skills.as_ref().unwrap()[0].content,
        "world"
    );
    assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &second));
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        second.resources()
    );
    assert!(
        catalog.refresh_pending(),
        "a newer change must survive catalog publication"
    );
    assert_eq!(connection.occupancy(), 0);
    assert!(connection.current());
    assert_eq!(
        first.resources(),
        first_native,
        "old captures stay immutable"
    );

    let after_install = setup.pump.drive(store.snapshot()).unwrap();
    let third = setup
        .pump
        .drive(harness.refresh_profile_resources(connection.clone(), cap, &catalog))
        .unwrap();
    assert_eq!(third.revision(), "revision-2");
    assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &third));
    assert!(
        !catalog.refresh_pending(),
        "healthy follow-up consumes its own observation"
    );
    assert_eq!(
        setup.pump.drive(harness.get_resources()).unwrap(),
        third.resources()
    );
    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), after_install);
    assert_eq!(first.resources(), first_native);
    assert_eq!(
        second.resources().skills.as_ref().unwrap()[0].content,
        "world"
    );
    assert_eq!(connection.occupancy(), 0);
    assert!(connection.current());
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}

#[test]
fn new_memory_resource_changed_during_install_survives_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            resource_change_during_native_install(
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
fn new_sqlite_resource_changed_during_install_survives_all_pairs() {
    for core in 0..=1 {
        for minor in 0..=1 {
            let setup = Setup::new();
            let (session, store, dir) = sqlite_case(&setup);
            resource_change_during_native_install(&setup, session, store.clone(), core, minor);
            drop(store);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
