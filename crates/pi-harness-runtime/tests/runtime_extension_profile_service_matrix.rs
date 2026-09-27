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
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, atomic::Ordering},
};

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
    options.telemetry_context = Arc::new(sink.clone());
    let harness: Arc<RuntimeHarness<(), S, SessionMetadata>> = setup
        .pump
        .drive(factory.create_configured(options))
        .unwrap()
        .harness;
    let profiles = HOOKS
        .iter()
        .map(|name| profile("hook.v1", name))
        .chain([
            profile("provider.v1", setup.faux.model.provider.as_str()),
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
    for name in HOOKS.into_iter().filter(|name| *name != "before_payload") {
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
            None,
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
    for method in [
        "before_run",
        "before_resume",
        "before_request",
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
