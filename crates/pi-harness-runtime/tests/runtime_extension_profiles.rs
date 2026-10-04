#![cfg(feature = "extensions")]
mod runtime_extension_profiles_support;
pub(crate) mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    memory::MemoryStorage,
    session::RuntimeSession,
    storage::{AtomicStorage, RuntimeStorage},
};
use pi_harness_runtime::{
    extensions::profiles::hooks::RemoteHook,
    extensions::transport::{OrderedByteChannel, RetainedOrderedByteChannel},
    owner::{RuntimeFactory, RuntimeHarness},
};
use prost::Message;
use runtime_extension_profiles_support::*;
use runtime_r1_r4_support::*;
use std::sync::{Arc, atomic::Ordering};

fn retained_ingress_api_is_public<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>() {
    let _ = RuntimeHarness::<C, S, M>::connect_profile_tool_retained;

    fn accepts_ordinary_channel<T: OrderedByteChannel + ?Sized>() {}
    fn accepts_retained_channel<T: RetainedOrderedByteChannel + ?Sized>() {}

    accepts_ordinary_channel::<dyn OrderedByteChannel>();
    accepts_retained_channel::<dyn RetainedOrderedByteChannel>();
}

#[test]
#[ignore = "explicit allocator evidence witness; run with --ignored --exact"]
fn source_root_allocator_peak_is_measured_and_cleanup_is_observed() {
    let setup = Setup::new();
    let harness = create(&setup, setup.session.clone());
    let (connection, peer) = install(&setup, &harness, 1, 1);

    allocator_measurement_start();
    let result = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "allocator measurement".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();

    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    let before_close = allocator_measurement_snapshot();
    assert!(
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.method == "provider.invoke")
    );

    connection.close();
    setup.pump.drive(harness.close()).unwrap();
    let snapshot = allocator_measurement_finish();

    assert!(
        before_close.peak_bytes > 0,
        "allocator peak must come from the real source-root scenario: {snapshot:?}"
    );
    assert!(
        before_close.allocations > 0 && snapshot.deallocations > before_close.deallocations,
        "allocator must observe both allocation and cleanup paths: {snapshot:?}"
    );
    assert!(
        snapshot.live_bytes <= before_close.live_bytes,
        "close cleanup must not increase measured live bytes: before={before_close:?} after={snapshot:?}"
    );
    assert_eq!(
        snapshot.allocator,
        "runtime-extension-profiles-alloc::TestAlloc"
    );
    assert!(
        snapshot.live_bytes >= 0,
        "measured live bytes must be finite and nonnegative: {snapshot:?}"
    );
}

#[test]
fn retained_ingress_api_is_additive_and_reachable_from_external_host_code() {
    retained_ingress_api_is_public::<(), MemoryStorage, SessionMetadata>();
}

#[test]
#[ignore = "explicit stack/CPU evidence witness; run with --ignored --exact"]
fn source_root_stack_high_water_and_finite_cpu_are_measured() {
    use runtime_extension_profiles_support::process::{CpuProbe, LIMIT};

    let cpu = CpuProbe::start();
    let setup = Setup::new();
    accepted_process_consumer(
        &setup,
        setup.session.clone(),
        setup.store.clone(),
        1,
        1,
        "python",
        ProcessMode::Native,
    );
    let measurement = cpu.finish();
    let stack = runtime_extension_profiles_support::process::last_stack_snapshot()
        .expect("completed child-process support must publish stack samples");

    assert!(
        stack.worker_names.iter().any(|name| name == "stdout")
            && stack.worker_names.iter().any(|name| name == "stdin")
            && stack.worker_names.iter().any(|name| name == "stderr"),
        "all source-owned child workers must contribute stack addresses: {stack:?}"
    );
    assert!(
        stack.samples > 0
            && stack.minimum_address > 0
            && stack.maximum_address > stack.minimum_address
            && stack.span_bytes > 0
            && stack
                .worker_snapshots
                .iter()
                .all(|worker| worker.samples > 0 && worker.span_bytes > 0),
        "stack high-water/span must come from sampled worker addresses: {stack:?}"
    );
    assert!(measurement.clock_hz > 0);
    assert!(
        measurement.cpu_ns > 0,
        "process CPU clock must advance: {measurement:?}"
    );
    assert_eq!(measurement.budget_ns, LIMIT.as_nanos());
    assert!(
        measurement.headroom_ns > 0 && measurement.cpu_ns < measurement.wall_ns,
        "CPU must be measured against the finite envelope, separately from wall time: {measurement:?}"
    );
    println!("E1703_RESOURCE stack={stack:?} cpu={measurement:?}");
}

#[test]
#[ignore = "explicit source-event/resource evidence witness; run with --ignored --exact"]
fn source_root_structured_events_bind_real_resources() {
    use pi_harness_runtime::extensions::transport::source_observer;
    use runtime_extension_profiles_support::process::{CpuProbe, LIMIT, last_stack_snapshot};

    source_observer::reset();

    let live_setup = Setup::new();
    let live_harness = create(&live_setup, live_setup.session.clone());
    let (live_connection, live_peer, live_writer) =
        install_live_bridge_for_test(&live_setup, &live_harness);
    let live_result = live_setup
        .pump
        .drive(live_harness.prompt(PromptInput::Text {
            text: "one same-route provider request".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        live_result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    let live_requests = live_peer.requests.lock().unwrap().clone();
    assert_eq!(live_requests.len(), 1);
    assert_eq!(live_requests[0].method, "provider.invoke");
    let live_id = live_requests[0].request_id;
    let live_generation = live_connection.grant().wire().connection_generation;
    let live_events = source_observer::snapshot();
    let wire = live_events
        .iter()
        .find(|event| {
            event.field == Some("D_req")
                && event.operation == Some("wire_body")
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        })
        .expect("actual provider terminal body from the sole reader");
    // The indexed seam consumes the retained body through take_active_body;
    // the original physical byte counts remain immutable in this event.
    let body_len = wire.body_len.unwrap();
    assert!(body_len > 0);
    assert_eq!(wire.bytes, Some(body_len));
    assert_eq!(wire.outcome, Some("retained"));
    let outgoing = pi_extension_protocol::wire::framing::encode(
        &pi_extension_protocol::wire::v1_0::Envelope {
            frame: Some(pi_extension_protocol::wire::v1_0::envelope::Frame::Request(
                pi_extension_protocol::wire::v1_0::Request::decode(
                    live_requests[0].encode_to_vec().as_slice(),
                )
                .unwrap(),
            )),
        },
        std::num::NonZeroUsize::new(live_connection.max_payload_bytes()).unwrap(),
    )
    .unwrap();
    for owner in ["ToolInbox", "Writer", "Route"] {
        for operation in ["enqueue", "pop", "write", "ack", "release"] {
            let expected_bytes = if owner == "ToolInbox" && matches!(operation, "enqueue" | "pop") {
                body_len
            } else {
                outgoing.bytes.len()
            };
            assert!(
                live_events.iter().any(|event| {
                    event.field == Some("Qreceive")
                        && event.owner == Some(owner)
                        && event.operation == Some(operation)
                        && event.request_id == Some(live_id)
                        && event.generation == Some(live_generation)
                        && event.bytes == Some(expected_bytes)
                }),
                "live request must have exact {owner}/{operation} edge"
            );
        }
    }
    assert_eq!(
        live_events
            .iter()
            .filter(|event| {
                event.kind == "interval"
                    && event.operation == Some("physical_write")
                    && event.outcome == Some("start")
                    && event.request_id == Some(live_id)
            })
            .map(|event| event.owner)
            .collect::<Vec<_>>(),
        [Some("Route")]
    );
    let live_observation = source_observer::resource_observation();
    println!("LIVE_ROUTE_EVENTS {live_events:?}");
    println!(
        "LIVE_ROUTE_RESOURCE {}",
        serde_json::to_string(&live_observation).unwrap()
    );
    for field in ["Qreceive", "D_req"] {
        assert_eq!(
            live_observation["fields"][field]["status"], "observed",
            "same physical route baseline must observe {field}"
        );
    }
    assert_eq!(live_observation["fields"]["P_req"]["status"], "observed");
    assert!(
        live_observation["fields"]["P_req"]["value"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    assert_eq!(live_observation["fields"]["P_req"]["checks"]["owner"], true);
    assert_eq!(
        live_observation["fields"]["P_req"]["checks"]["dispatch_release"],
        true
    );
    assert_eq!(
        live_observation["fields"]["P_req"]["checks"]["terminal_retention"],
        true
    );
    assert_eq!(
        live_observation["fields"]["P_req"]["checks"]["final_retire"],
        true
    );
    assert_eq!(
        live_observation["fields"]["P_req"]["checks"]["tool_inbox"],
        true
    );
    assert_eq!(
        live_observation["fields"]["P_req"]["checks"]["writer"],
        true
    );
    assert_eq!(live_observation["fields"]["P_req"]["checks"]["route"], true);
    let bound_payload = live_events
        .iter()
        .find(|event| {
            event.function == "ProviderRequestObservation::bind"
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        })
        .expect("fresh bound provider-owned encoding event");
    assert_eq!(bound_payload.body_len, Some(live_requests[0].payload.len()));
    assert_eq!(
        bound_payload.provider.as_deref(),
        Some(live_setup.faux.model.provider.as_str())
    );
    assert_eq!(
        bound_payload.model.as_deref(),
        Some(live_setup.faux.model.id.as_str())
    );
    let pre_id_payload = live_events
        .iter()
        .find(|event| {
            event.function == "provider::invoke" && event.operation == Some("payload_encoded")
        })
        .expect("original pre-ID encoding event");
    assert_eq!(
        (pre_id_payload.request_id, pre_id_payload.generation),
        (None, None)
    );
    let admission = live_events
        .iter()
        .filter(|event| {
            event.field == Some("D_req")
                && event.kind == "native_validation"
                && event.seq < bound_payload.seq
        })
        .collect::<Vec<_>>();
    assert_eq!(admission.len(), 2);
    assert!(admission.iter().all(|event| event.request_id.is_none()));
    let role_started = live_events
        .iter()
        .position(|event| {
            event.kind == "role"
                && event.outcome == Some("started")
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        })
        .unwrap();
    let selection = live_events
        .iter()
        .position(|event| {
            event.function == "ProviderDispatch::prepare"
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        })
        .unwrap();
    assert!(role_started < selection);
    assert!(live_events[role_started].seq < bound_payload.seq);
    assert!(bound_payload.seq < live_events[selection].seq);
    let validation = live_events
        .iter()
        .filter(|event| {
            event.field == Some("D_req")
                && event.kind == "native_validation"
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        })
        .collect::<Vec<_>>();
    assert_eq!(validation.len(), 2);
    assert_eq!(validation[0].outcome, Some("start"));
    assert_eq!(validation[1].outcome, Some("complete"));
    assert!(wire.seq < validation[0].seq);
    println!(
        "LIVE_ROUTE_BYTES request_id={live_id} generation={live_generation} payload={} frame={} body={} physical_writes=1",
        live_requests[0].payload.len(),
        outgoing.bytes.len(),
        body_len
    );
    live_connection.close();
    live_writer.close();
    live_setup.pump.drive(live_harness.close()).unwrap();

    let tool_setup = Setup::new();
    physical_tool_consumer(
        &tool_setup,
        tool_setup.session.clone(),
        tool_setup.store.clone(),
        0,
        0,
        true,
    );

    allocator_measurement_start();
    let setup = Setup::new();
    let harness = create(&setup, setup.session.clone());
    let (connection, peer) = install(&setup, &harness, 0, 0);
    let result = setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "structured source events".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    let before_close = allocator_measurement_snapshot();
    assert!(
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.method == "provider.invoke")
    );
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
    let allocation = allocator_measurement_finish();

    let cpu = CpuProbe::start();
    let process_setup = Setup::new();
    source_observer::with_case_id("memory/python/0/0", || {
        accepted_process_consumer(
            &process_setup,
            process_setup.session.clone(),
            process_setup.store.clone(),
            0,
            0,
            "python",
            ProcessMode::Resource,
        );
    });
    let cpu_measurement = cpu.finish();
    source_observer::with_case_id("memory/python/0/0", || {
        record_process_matrix_case("memory", "python", 0, 0);
    });
    for backend in ["memory", "sqlite"] {
        for language in ["typescript", "python"] {
            for core in 0..=1 {
                for minor in 0..=1 {
                    if backend == "memory" && language == "python" && core == 0 && minor == 0 {
                        continue;
                    }
                    let setup = Setup::new();
                    source_observer::with_case_id(
                        format!("{backend}/{language}/{core}/{minor}"),
                        || {
                            if backend == "memory" {
                                accepted_process_consumer(
                                    &setup,
                                    setup.session.clone(),
                                    setup.store.clone(),
                                    core,
                                    minor,
                                    language,
                                    ProcessMode::Resource,
                                );
                            } else {
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
                            record_process_matrix_case(backend, language, core, minor);
                        },
                    );
                }
            }
        }
    }
    let queue_setup = Setup::new();
    source_observer::with_case_id("memory/python/0/0/host-queue", || {
        accepted_process_consumer(
            &queue_setup,
            queue_setup.session.clone(),
            queue_setup.store.clone(),
            0,
            0,
            "python",
            ProcessMode::HostQueue,
        );
    });
    pi_harness_runtime::extensions::tool_driver::retained_resource_snapshot_for_test(9001);
    pi_harness_runtime::extensions::tool_driver::local_cancel_queue_tests::
        retained_indexed_walking_branch_for_test();
    let stack = last_stack_snapshot().expect("process source workers publish stack samples");
    for worker in &stack.worker_snapshots {
        let mut event = source_observer::SourceEvent::new(
            "stack",
            file!(),
            line!(),
            "source_root_structured_events_bind_real_resources",
        );
        event.field = Some("whole_stack");
        event.owner = Some("child-worker");
        event.thread = Some(worker.worker.clone());
        event.span_start = Some(worker.minimum_address);
        event.span_end = Some(worker.maximum_address);
        event.address = Some(worker.maximum_address);
        event.operation = Some("stack_sample");
        source_observer::record(event);
    }
    for (snapshot, phase) in [(before_close, "snapshot"), (allocation, "finish")] {
        let mut event = source_observer::SourceEvent::new(
            "allocator",
            file!(),
            line!(),
            "source_root_structured_events_bind_real_resources",
        );
        event.field = Some("allocator_peak");
        event.owner = Some("TestAlloc");
        event.allocator = Some(snapshot.allocator);
        event.operation = Some(phase);
        event.bytes = Some(snapshot.peak_bytes.min(usize::MAX as u64) as usize);
        event.bound_bytes = Some(snapshot.live_bytes.max(0).min(usize::MAX as i64) as usize);
        event.capacity = Some(snapshot.allocations.min(usize::MAX as u64) as usize);
        event.cleanup = Some(snapshot.deallocations > 0);
        source_observer::record(event);
        let mut composite = source_observer::SourceEvent::new(
            "allocator",
            file!(),
            line!(),
            "source_root_structured_events_bind_real_resources",
        );
        composite.field = Some("EH_inline_target_provider_allocator");
        composite.owner = Some("TestAlloc");
        composite.allocator = Some(snapshot.allocator);
        composite.operation = Some(phase);
        composite.bytes = Some(snapshot.peak_bytes.min(usize::MAX as u64) as usize);
        composite.cleanup = Some(snapshot.deallocations > 0);
        source_observer::record(composite);
    }
    let mut cpu_event = source_observer::SourceEvent::new(
        "cpu",
        file!(),
        line!(),
        "source_root_structured_events_bind_real_resources",
    );
    cpu_event.field = Some("work_cpu_headroom");
    cpu_event.owner = Some("CpuProbe");
    cpu_event.operation = Some("budget_headroom");
    cpu_event.bytes = Some(cpu_measurement.cpu_ns.min(usize::MAX as u128) as usize);
    cpu_event.bound_bytes = Some(cpu_measurement.headroom_ns.min(usize::MAX as u128) as usize);
    cpu_event.capacity = Some(cpu_measurement.budget_ns.min(usize::MAX as u128) as usize);
    source_observer::record(cpu_event);
    let events = source_observer::snapshot();
    assert!(
        events.len() > 20,
        "real owners must emit a nontrivial trace"
    );
    assert!(
        events
            .iter()
            .all(|event| event.schema_version == "s12-d-resource-source-events-v2"),
        "all source records must use the accepted schema version"
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].trace_id == pair[1].trace_id && pair[0].seq < pair[1].seq),
        "source event sequence must be monotonic"
    );
    assert!(
        events
            .iter()
            .all(|event| event.event_id == format!("source/{}/{}", event.trace_id, event.seq)),
        "event ids must bind the collector trace and sequence"
    );

    let phase_reads = events
        .iter()
        .filter(|event| event.kind == "phase" && event.outcome == Some("read"))
        .collect::<Vec<_>>();
    for phase in ["AwaitHello", "AwaitManifest", "AwaitReady", "Active"] {
        assert!(
            phase_reads.iter().any(|event| event.phase == Some(phase)),
            "missing source-owned phase read for {phase}: {phase_reads:?}"
        );
    }
    assert!(
        events.iter().any(|event| {
            event.kind == "resource"
                && event.capacity.is_some_and(|value| value > 0)
                && event.scratch_bytes.is_some()
                && event
                    .bound_bytes
                    .is_some_and(|value| value >= event.capacity.unwrap())
                && event.source_file.ends_with("extensions/transport.rs")
                && event.function == "ConnectionPump::new"
        }),
        "transport resource event must carry source-owned capacity/bound provenance"
    );

    let register_ok = live_events
        .iter()
        .position(|event| event.kind == "provider_register" && event.outcome == Some("ok"))
        .expect("provider registration owner event");
    let source_current = live_events
        .iter()
        .enumerate()
        .find(|(index, event)| {
            *index > register_ok
                && event.kind == "source_current"
                && event.outcome == Some("check")
                && event.function == "ProviderDispatch::prepare"
        })
        .map(|(index, _)| index)
        .expect("provider source-current owner event");
    assert!(
        events.iter().any(|event| {
            event.kind == "source_current"
                && event.outcome == Some("check")
                && event.function == "provider::invoke"
        }),
        "provider invoke source-current owner event"
    );
    let role_start = live_events
        .iter()
        .enumerate()
        .find(|(index, event)| {
            *index > register_ok
                && event.kind == "role"
                && event.outcome == Some("started")
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        })
        .map(|(index, _)| index)
        .expect("role start owner event");
    assert!(role_start < source_current);
    let terminal = live_events
        .iter()
        .enumerate()
        .find(|(index, event)| {
            *index > source_current
                && event.kind == "terminal"
                && event.outcome == Some("accepted")
                && event.request_id == Some(live_id)
        })
        .map(|(index, _)| index)
        .expect("terminal owner event");
    assert!(
        live_events.iter().enumerate().any(|(index, event)| {
            index > terminal
                && event.kind == "cleanup"
                && event.outcome == Some("complete")
                && event.request_id == Some(live_id)
                && event.generation == Some(live_generation)
        }),
        "cleanup must follow terminal settlement"
    );
    assert!(events.iter().any(|event| {
        event.kind == "queue"
            && event.operation == Some("reserve")
            && event.bytes.is_some_and(|value| value > 0)
            && event.reserved_control.is_some()
    }));
    assert!(events.iter().any(|event| {
        event.kind == "queue"
            && event.operation == Some("enqueue")
            && event.jobs.is_some_and(|value| value > 0)
    }));
    assert!(events.iter().any(|event| {
        event.kind == "queue"
            && event.operation == Some("release")
            && event.bytes.is_some_and(|value| value > 0)
    }));
    assert!(
        events.iter().any(|event| {
            event.kind == "role"
                && event.role == Some("Inline")
                && event.native_parent_present == Some(true)
                && event.source_file.ends_with("profiles/connection.rs")
        }),
        "physical tool path must expose its real inline/native-parent custody"
    );
    assert!(
        allocation.peak_bytes > 0
            && allocation.allocations > 0
            && allocation.deallocations > before_close.deallocations
            && allocation.live_bytes <= before_close.live_bytes
            && allocation.live_bytes >= 0
            && allocation.allocator == "runtime-extension-profiles-alloc::TestAlloc",
        "allocation evidence must include source activity and cleanup: before={before_close:?} after={allocation:?}"
    );
    assert!(
        stack.worker_names.iter().any(|name| name == "stdout")
            && stack.worker_names.iter().any(|name| name == "stdin")
            && stack.worker_names.iter().any(|name| name == "stderr")
            && stack
                .worker_snapshots
                .iter()
                .all(|worker| worker.span_bytes > 0),
        "stack evidence must come from all existing process workers: {stack:?}"
    );
    assert!(
        cpu_measurement.clock_hz > 0
            && cpu_measurement.cpu_ns > 0
            && cpu_measurement.budget_ns == LIMIT.as_nanos()
            && cpu_measurement.headroom_ns > 0,
        "CPU evidence must be finite and tied to the accepted budget: {cpu_measurement:?}"
    );
    let observation = source_observer::resource_observation();
    let fields = observation["fields"]
        .as_object()
        .expect("all twelve resource fields are present");
    assert_eq!(fields.len(), 12);
    for field in [
        "Hboot",
        "Hpre",
        "Qread",
        "Qreceive",
        "overlap",
        "D_req",
        "P_req",
        "whole_stack",
        "allocator_peak",
        "work_cpu_headroom",
        "Memory_SQLite_peer_parity",
        "EH_inline_target_provider_allocator",
    ] {
        let record = &fields[field];
        assert!(
            record["status"] == "observed" || record["status"] == "unresolved",
            "resource field status is explicit: {field}"
        );
        assert!(
            record["evidence"]["event_ids"].is_array(),
            "resource field evidence is source-bound: {field}"
        );
    }
    for field in ["Hboot", "Hpre", "Qread", "Memory_SQLite_peer_parity"] {
        assert_eq!(
            fields[field]["status"], "observed",
            "scenario expansion must close the complete bounded field: {field}"
        );
        assert!(
            fields[field]["value"]
                .as_u64()
                .is_some_and(|value| value > 0),
            "scenario expansion must report a finite source value: {field}"
        );
        assert!(
            fields[field]["evidence"]["event_ids"]
                .as_array()
                .is_some_and(|ids| !ids.is_empty()),
            "scenario expansion must bind source event IDs: {field}"
        );
    }
    for field in ["Qreceive", "D_req"] {
        assert_eq!(fields[field]["status"], "observed");
        assert!(
            fields[field]["value"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
    }
    assert_eq!(fields["P_req"]["status"], "observed");
    assert!(
        fields["P_req"]["value"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    for check in [
        "owner",
        "dispatch_release",
        "terminal_retention",
        "final_retire",
        "tool_inbox",
        "writer",
        "route",
    ] {
        assert_eq!(
            fields["P_req"]["checks"][check], true,
            "P_req same-token lifecycle check must close: {check}"
        );
    }
    for field in [
        "overlap",
        "whole_stack",
        "allocator_peak",
        "work_cpu_headroom",
        "EH_inline_target_provider_allocator",
    ] {
        assert_eq!(
            fields[field]["status"], "observed",
            "resource field must have a source observation: {field}"
        );
        assert!(
            fields[field]["evidence"]["event_ids"]
                .as_array()
                .is_some_and(|ids| !ids.is_empty()),
            "resource field must bind at least one source event: {field}"
        );
    }
    println!(
        "SOURCE_RESOURCE {}",
        serde_json::to_string(&observation).unwrap()
    );
}

#[test]
#[ignore = "causal P_req owner/token bridge witness; run with --ignored --exact"]
fn source_root_p_req_owner_lifecycle_causal_red_restore() {
    use pi_harness_runtime::extensions::transport::source_observer;

    fn run() -> (serde_json::Value, Vec<source_observer::SourceEvent>) {
        let setup = Setup::new();
        let harness = create(&setup, setup.session.clone());
        let (connection, peer, writer) = install_live_bridge_for_test(&setup, &harness);
        let result = setup
            .pump
            .drive(harness.prompt(PromptInput::Text {
                text: "one causal owner lifecycle request".into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
        assert!(matches!(
            result.outcome,
            RunOutcome::CompletedWithAssistant(_)
        ));
        let requests = peer.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "provider.invoke");
        let observation = source_observer::resource_observation();
        let events = source_observer::snapshot();
        assert!(
            events.iter().any(|event| {
                event.field == Some("Qreceive")
                    && event.owner == Some("ToolInbox")
                    && event.request_id == Some(requests[0].request_id)
                    && event.token.is_some()
            }),
            "ToolInbox must carry the opaque route token"
        );
        assert!(
            events.iter().any(|event| {
                event.field == Some("Qreceive")
                    && event.owner == Some("Writer")
                    && event.request_id == Some(requests[0].request_id)
                    && event.token.is_some()
            }),
            "Writer must carry the opaque route token"
        );
        assert!(
            events.iter().any(|event| {
                event.field == Some("Qreceive")
                    && event.owner == Some("Route")
                    && event.request_id == Some(requests[0].request_id)
                    && event.token.is_some()
            }),
            "Route must carry the opaque route token"
        );
        connection.close();
        writer.close();
        setup.pump.drive(harness.close()).unwrap();
        (observation, events)
    }

    source_observer::reset();
    source_observer::suppress_owner_admission(true);
    let (red_admission, red_admission_events) = run();
    assert_eq!(red_admission["fields"]["Qreceive"]["status"], "observed");
    assert_eq!(red_admission["fields"]["D_req"]["status"], "observed");
    assert_eq!(red_admission["fields"]["P_req"]["status"], "unresolved");
    assert!(
        red_admission_events.iter().any(|event| {
            event.field == Some("P_req")
                && event.owner == Some("profile_live::Owner")
                && event.operation == Some("terminal_retain")
        }),
        "downstream owner retention remains visible when admission marker is suppressed"
    );

    source_observer::reset();
    source_observer::mutate_next_owner_token();
    let (red_token, red_token_events) = run();
    assert_eq!(red_token["fields"]["Qreceive"]["status"], "observed");
    assert_eq!(red_token["fields"]["D_req"]["status"], "observed");
    assert_eq!(red_token["fields"]["P_req"]["status"], "unresolved");
    let owner_admission = red_token_events
        .iter()
        .find(|event| {
            event.field == Some("P_req")
                && event.owner == Some("profile_live::Owner")
                && event.operation == Some("admit")
        })
        .expect("mutated owner admission event");
    let route_event = red_token_events
        .iter()
        .find(|event| {
            event.field == Some("Qreceive")
                && event.owner == Some("Route")
                && event.operation == Some("enqueue")
        })
        .expect("route event with equal numeric request identity");
    assert_eq!(owner_admission.request_id, route_event.request_id);
    assert_eq!(owner_admission.generation, route_event.generation);
    assert_ne!(owner_admission.token, route_event.token);

    source_observer::reset();
    source_observer::suppress_final_retire(true);
    let (red_retire, _) = run();
    assert_eq!(red_retire["fields"]["Qreceive"]["status"], "observed");
    assert_eq!(red_retire["fields"]["D_req"]["status"], "observed");
    assert_eq!(red_retire["fields"]["P_req"]["status"], "unresolved");

    source_observer::reset();
    let (restored, restored_events) = run();
    assert_eq!(restored["fields"]["Qreceive"]["status"], "observed");
    assert_eq!(restored["fields"]["D_req"]["status"], "observed");
    assert_eq!(restored["fields"]["P_req"]["status"], "observed");
    for check in [
        "owner",
        "dispatch_release",
        "terminal_retention",
        "final_retire",
        "tool_inbox",
        "writer",
        "route",
    ] {
        assert_eq!(
            restored["fields"]["P_req"]["checks"][check], true,
            "{check}"
        );
    }
    assert!(
        restored_events.iter().any(|event| {
            event.field == Some("P_req")
                && event.owner == Some("profile_live::Owner")
                && event.operation == Some("final_retire")
                && event.outcome == Some("consumed")
                && event.token.is_some()
        }),
        "exact final-retire event must return on restore"
    );
}

#[test]
fn provider_request_observation_requires_opaque_binding_and_native_success() {
    use pi_extension_protocol::{
        profile_live::{ConnectionIdentity, Owner, Role, Start},
        profiles::{Direction, NativeError, NativeValidator, Stage, ValidatedPayload},
    };
    use pi_harness_runtime::extensions::{
        profiles::{native::NativeDomain, provider::ProviderRequestObservation},
        transport::source_observer,
    };
    use std::panic::{AssertUnwindSafe, catch_unwind};

    source_observer::reset();
    let setup = Setup::new();
    let harness = create(&setup, setup.session.clone());
    let (connection, peer) = install_named(&setup, &harness, 0, 0, &[]);
    setup
        .pump
        .drive(harness.prompt(PromptInput::Text {
            text: "capture one real provider request for provenance negatives".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let request = peer.requests.lock().unwrap()[0].clone();
    let capability = request.capability_ref.clone().unwrap();
    let model_policy = pi_ai_core::ModelHeaderPolicy::default();
    let mut native = NativeDomain::new(setup.session.codec(), &model_policy);
    native.expected_model = Some(&setup.faux.model);
    let start_fixture = |native: &mut NativeDomain<'_>| {
        let mut owner = Owner::new_with_connection(
            connection.grant().clone(),
            reservations(),
            0,
            10000,
            connection.connection_identity(),
        )
        .unwrap();
        owner
            .start(
                Start {
                    capability: capability.clone(),
                    method: request.method.clone(),
                    direction: Direction::CoreToExtension,
                    role: Role::Planned,
                    key: request.effect_key.clone(),
                    parent: None,
                    scope: connection
                        .grant()
                        .profiles()
                        .iter()
                        .find(|profile| profile.reference() == &capability)
                        .unwrap()
                        .approved()
                        .scope()
                        .into(),
                    payload: request.payload.clone(),
                    now_ms: 0,
                    requested_deadline_ms: Some(500),
                    method_default_ms: 500,
                    method_limit_ms: 500,
                    initial_credit: Some(0),
                    generation: connection.grant().wire().connection_generation,
                },
                native,
            )
            .unwrap()
    };
    let (binding, started) = start_fixture(&mut native);
    let (other_binding, other_started) = start_fixture(&mut native);
    assert_eq!(binding.id(), other_binding.id());
    assert_eq!(binding.generation(), other_binding.generation());
    assert!(!binding.same_identity(&other_binding));
    let observation = Arc::new(ProviderRequestObservation::new(
        &connection,
        capability,
        started.effect_key.clone().unwrap(),
        &setup.faux.model,
        started.payload.len(),
    ));
    let mut unbound = source_observer::SourceEvent::new(
        "native_validation",
        file!(),
        line!(),
        "provider_request_observation_requires_opaque_binding_and_native_success",
    );
    observation.annotate_bound(&mut unbound);
    assert_eq!((unbound.request_id, unbound.generation), (None, None));
    let before_wrong = source_observer::snapshot().len();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            observation.bind(&ConnectionIdentity::new(), &binding, &started);
        }))
        .is_err(),
        "equal numeric ID/generation cannot replace opaque connection identity"
    );
    assert_eq!(source_observer::snapshot().len(), before_wrong);
    observation.bind(&connection.connection_identity(), &binding, &started);
    let after_bound = source_observer::snapshot().len();
    observation.bind(
        &connection.connection_identity(),
        &binding.clone(),
        &started,
    );
    assert_eq!(source_observer::snapshot().len(), after_bound);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            observation.bind(
                &connection.connection_identity(),
                &other_binding,
                &other_started,
            );
        }))
        .is_err(),
        "another real Binding with equal numeric ID/generation cannot rebind"
    );
    assert_eq!(source_observer::snapshot().len(), after_bound);

    native.provider_observation = Some(observation);
    let before_rejected = source_observer::snapshot().len();
    assert_eq!(
        native.validate(&ValidatedPayload {
            method: "provider.invoke".into(),
            stage: Stage::Terminal,
            projections: std::collections::BTreeMap::from([(
                "provider.assistant_message.v1".into(),
                serde_json::Value::Null,
            )]),
            fields: serde_json::json!({}),
        }),
        Err(NativeError::Rejected)
    );
    let rejected_events = source_observer::snapshot();
    assert!(rejected_events[before_rejected..].iter().any(|event| {
        event.field == Some("D_req")
            && event.outcome == Some("start")
            && event.request_id == Some(binding.id())
            && event.generation == Some(binding.generation())
    }));
    assert!(!rejected_events[before_rejected..].iter().any(|event| {
        event.field == Some("D_req")
            && event.outcome == Some("complete")
            && event.request_id == Some(binding.id())
    }));
    connection.close();
    setup.pump.drive(harness.close()).unwrap();
}

fn install_live_bridge_for_test(
    setup: &Setup,
    harness: &RuntimeHarness<(), MemoryStorage, SessionMetadata>,
) -> (
    Arc<pi_harness_runtime::extensions::profiles::connection::ProfileConnection>,
    Arc<Peer>,
    Arc<pi_harness_runtime::extensions::profiles::service::Writer>,
) {
    use pi_ai_core::TaskSpawner;
    use pi_extension_protocol::activation::HostAuthority;
    use pi_harness_runtime::extensions::{
        tool_driver::{OwnerLimits, SingleToolOwner},
        transport::PumpConfig,
    };
    use std::num::{NonZeroU64, NonZeroUsize};
    let profiles = vec![profile("provider.v1", setup.faux.model.provider.as_str())];
    let peer = Peer::new(0, 0, profiles.clone());
    peer.enable_tool(0);
    let service = service_host(0, profiles.clone());
    let host = HostAuthority::new(
        "peer-a".into(),
        7,
        0,
        1,
        *service.limits(),
        vec![echo_approval()],
        vec![],
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
            Arc::new(Mono(std::sync::atomic::AtomicU64::new(0))),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(1024).unwrap(),
                write_timeout_ms: NonZeroU64::new(500).unwrap(),
            },
            reservations(),
            10000,
        ))
        .unwrap();
    assert!(connection.same_tool_connection(&route));
    assert!(
        connection
            .connection_identity()
            .same_connection(&route.connection_identity())
    );
    assert_eq!(
        route.generation(),
        connection.grant().wire().connection_generation
    );
    let inbox = connection.tool_inbox_for_test();
    let writer = inbox.bridge_writer_for_test(&connection, &route);
    setup
        .pump
        .spawn(Box::pin(writer.clone().run_on_route_for_test(
            Arc::downgrade(&connection),
            inbox,
            route,
        )))
        .unwrap();
    let capability = connection.grant().profiles()[0].reference().clone();
    setup
        .pump
        .drive(harness.register_profile_provider(
            connection.clone(),
            capability,
            vec![setup.faux.model.clone()],
            Default::default(),
            None,
        ))
        .unwrap();
    (connection, peer, writer)
}

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

fn record_process_matrix_case(
    backend: &'static str,
    language: &'static str,
    core: u32,
    minor: u32,
) {
    use pi_harness_runtime::extensions::transport::source_observer;
    use runtime_extension_profiles_support::process::last_transport_snapshot;

    let snapshot = last_transport_snapshot().expect("completed process channel publishes peaks");
    assert_eq!(snapshot.language, language);
    assert_eq!((snapshot.core, snapshot.minor), (core, minor));
    assert!(!snapshot.command.is_empty());
    assert!(
        snapshot
            .topology
            .iter()
            .any(|edge| edge.starts_with("ChildTransport/read"))
            && snapshot
                .topology
                .iter()
                .any(|edge| edge.starts_with("ChildTransport/write"))
            && snapshot
                .topology
                .iter()
                .any(|edge| edge == "PeerProcess/terminal")
            && snapshot
                .topology
                .iter()
                .any(|edge| edge == "PeerProcess/cleanup")
    );
    let generation = source_observer::snapshot()
        .iter()
        .rev()
        .find(|event| {
            event.field == Some("Qread")
                && event.owner == Some("ConnectionPump")
                && event.case_id.is_some()
        })
        .and_then(|event| event.generation);
    let mut read =
        source_observer::SourceEvent::new("queue", file!(), line!(), "Channel::transport");
    read.field = Some("Qread");
    read.owner = Some("ChildTransport");
    read.operation = Some("child_read_queue_peak");
    read.outcome = Some("observed");
    read.capacity = Some(snapshot.read_capacity);
    read.jobs = Some(snapshot.read_peak);
    read.method = Some(snapshot.command.clone());
    read.target = Some("ChildTransport::read_queue".into());
    read.topology = Some(snapshot.topology.clone());
    read.backend = Some(backend);
    read.language = Some(language);
    read.core = Some(core);
    read.minor = Some(minor);
    read.generation = generation;
    source_observer::record(read);
    let mut write =
        source_observer::SourceEvent::new("queue", file!(), line!(), "Channel::transport");
    write.field = Some("Qread");
    write.owner = Some("ChildTransport");
    write.operation = Some("child_write_queue_peak");
    write.outcome = Some("observed");
    write.capacity = Some(snapshot.write_capacity);
    write.jobs = Some(snapshot.write_peak);
    write.method = Some(snapshot.command.clone());
    write.target = Some("ChildTransport::write_queue".into());
    write.topology = Some(snapshot.topology.clone());
    write.backend = Some(backend);
    write.language = Some(language);
    write.core = Some(core);
    write.minor = Some(minor);
    write.generation = generation;
    source_observer::record(write);
    let mut parity = source_observer::SourceEvent::new(
        "peer_parity",
        file!(),
        line!(),
        "accepted_process_consumer",
    );
    parity.field = Some("Memory_SQLite_peer_parity");
    parity.owner = Some("PeerProcess");
    parity.operation = Some("matrix_case");
    parity.outcome = Some("complete");
    parity.method = Some(snapshot.command);
    parity.target = Some(
        match backend {
            "memory" => "MemoryStorage::new",
            "sqlite" => "SqliteStorage::new",
            _ => "unknown",
        }
        .into(),
    );
    parity.topology = Some(snapshot.topology);
    parity.bytes = Some(snapshot.read_peak.saturating_add(snapshot.write_peak));
    parity.backend = Some(backend);
    parity.language = Some(language);
    parity.core = Some(core);
    parity.minor = Some(minor);
    parity.generation = generation;
    source_observer::record(parity);
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
    Telemetry,
    TelemetryBadAck,
    TelemetryBadFlush,
    BeforeResume,
    BeforePayload,
    BeforePayloadMalformed,
    HostSnapshot,
    HostQueue,
    HostInbox,
    HostAbortOld,
    HostAbortCurrent,
    HostAbortCurrentPhysicalHeld,
    HostAbortCurrentWriteFailure,
    Structural,
    StructuralCompactionDecline,
    StructuralNavigationDecline,
    HostResource,
    HostResourceMissing,
    HostResourceScope,
    HostResourceVersion,
    HostResourceParent,
    EventSink,
    EventSinkWrongId,
}
fn before_payload_schema() -> pi_extension_protocol::jcs::PortableSchema {
    let schema = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{"model":{"type":"object"},"context":{"type":"object"},"tools":{"type":"array","items":{"type":"object"},"maxItems":0},"options":{"type":"object"}},"required":["model","context","tools","options"],"additionalProperties":false}"#,
        Default::default(),
    ).unwrap();
    assert_eq!(
        schema.canonical().sha256_hex(),
        "beccbba37a9f577ac7013fe76c88177e09a61858a91ed098076972629707643f"
    );
    schema
}
struct AcceptPayloadNative;
impl pi_extension_protocol::profiles::NativeValidator for AcceptPayloadNative {
    fn validate(
        &mut self,
        _: &pi_extension_protocol::profiles::ValidatedPayload,
    ) -> Result<(), pi_extension_protocol::profiles::NativeError> {
        Ok(())
    }
}
struct EventWatch(std::sync::Mutex<Vec<HarnessEvent>>);
impl EventListener for EventWatch {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
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
    let event_mode = matches!(mode, ProcessMode::EventSink | ProcessMode::EventSinkWrongId);
    let resource_mode = matches!(
        mode,
        ProcessMode::HostResource
            | ProcessMode::HostResourceMissing
            | ProcessMode::HostResourceScope
            | ProcessMode::HostResourceVersion
            | ProcessMode::HostResourceParent
    );
    let structural_mode = matches!(
        mode,
        ProcessMode::Structural
            | ProcessMode::StructuralCompactionDecline
            | ProcessMode::StructuralNavigationDecline
    );
    let payload_mode = matches!(
        mode,
        ProcessMode::BeforePayload | ProcessMode::BeforePayloadMalformed
    );
    use pi_harness_runtime::extensions::{
        profiles::{
            connection::ProfileConnection, resources::ResourceCatalog,
            telemetry::ProjectedTelemetry,
        },
        transport::{OrderedByteChannel, PumpConfig},
    };
    use runtime_extension_profiles_support::process::{Channel, RealClock, drive};
    use std::num::{NonZeroU64, NonZeroUsize};
    let telemetry_mode = matches!(
        mode,
        ProcessMode::Telemetry
            | ProcessMode::TelemetryBadAck
            | ProcessMode::TelemetryBadFlush
            | ProcessMode::BeforeResume
            | ProcessMode::HostSnapshot
            | ProcessMode::HostQueue
            | ProcessMode::EventSink
            | ProcessMode::EventSinkWrongId
    );
    let telemetry = telemetry_mode.then(|| {
        ProjectedTelemetry::new(Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT), 1048576, 256)
            .unwrap()
    });
    let harness = if let Some(sink) = &telemetry {
        let factory = RuntimeFactory::new(
            setup.time.clone(),
            setup.pump.clone(),
            setup.ids.clone(),
            Arc::new(()),
            Arc::new(Summary),
        );
        let mut options = setup.options_for::<(), _>(session, DriveMode::Automatic);
        options.telemetry_context = Arc::new(sink.clone());
        setup
            .pump
            .drive(factory.create_configured(options))
            .unwrap()
            .harness
    } else {
        create(setup, session)
    };
    let profiles = if mode == ProcessMode::BeforeResume || payload_mode {
        use std::collections::{BTreeMap, BTreeSet};
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "runtime_extension_profiles_support/profiles-runtime-v1.json"
        ))
        .unwrap();
        let mut leaves = corpus["exposure"]["open_leaves"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(path, schema)| {
                (
                    path.clone(),
                    pi_extension_protocol::jcs::admit_schema(
                        &serde_json::to_vec(schema).unwrap(),
                        Default::default(),
                    )
                    .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if payload_mode {
            let schema = before_payload_schema();
            for path in [
                "hook.input:before_payload.event.payload",
                "hook.output:before_payload.result.payload",
            ] {
                leaves.insert(path.into(), schema.clone());
            }
            let model = corpus["methods"]["provider.invoke"]["request"]["model"]["value"].clone();
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
        } else {
            let saved = pi_extension_protocol::jcs::admit_schema(
                br#"{"type":"object","properties":{"marker":{"type":"string","enum":["peer-before-resume"]}},"required":["marker"],"additionalProperties":false}"#,
                Default::default(),
            ).unwrap();
            for path in [
                "hook.output:before_run.result.resumeData",
                "hook.input:before_resume.event.resumeData",
            ] {
                leaves.insert(path.into(), saved.clone());
            }
        }
        let exposure =
            pi_extension_protocol::profiles::Exposure::new(leaves, BTreeSet::new()).unwrap();
        accepted_profiles()
            .into_iter()
            .map(|profile| {
                pi_extension_protocol::profiles::ApprovedProfile::new(
                    pi_extension_protocol::profiles::ProfileApproval {
                        principal: profile.principal().into(),
                        kind: profile.kind().into(),
                        name: profile.name().into(),
                        profile: profile.profile().into(),
                        handler: profile.handler().into(),
                        minors: profile.minors().clone(),
                        replay: profile.replay(),
                        execution: profile.execution(),
                        scope: profile.scope().into(),
                        permissions: profile.permissions().clone(),
                        exposure: exposure.clone(),
                    },
                )
                .unwrap()
            })
            .collect()
    } else {
        accepted_profiles()
    };
    let invalid_lane = mode == ProcessMode::InvalidLane;
    let peer = if invalid_lane {
        Channel::spawn_invalid_lane(language, core, minor)
    } else if mode == ProcessMode::Changed {
        Channel::spawn_scenario(language, core, minor, "runtime-native-changed")
    } else if mode == ProcessMode::ChangedWrongScope {
        Channel::spawn_scenario(language, core, minor, "runtime-native-changed-wrong-scope")
    } else if mode == ProcessMode::Telemetry {
        Channel::spawn_scenario(language, core, minor, "runtime-native-telemetry")
    } else if mode == ProcessMode::TelemetryBadAck {
        Channel::spawn_scenario(language, core, minor, "runtime-native-telemetry-bad-ack")
    } else if mode == ProcessMode::TelemetryBadFlush {
        Channel::spawn_scenario(language, core, minor, "runtime-native-telemetry-bad-flush")
    } else if mode == ProcessMode::BeforeResume {
        Channel::spawn_scenario(language, core, minor, "runtime-native-before-resume")
    } else if mode == ProcessMode::HostSnapshot {
        Channel::spawn_scenario(language, core, minor, "runtime-native-host-snapshot")
    } else if mode == ProcessMode::HostQueue {
        Channel::spawn_scenario(language, core, minor, "runtime-native-host-queue")
    } else if mode == ProcessMode::HostInbox {
        Channel::spawn_scenario(language, core, minor, "runtime-native-host-inbox")
    } else if matches!(
        mode,
        ProcessMode::HostAbortOld
            | ProcessMode::HostAbortCurrent
            | ProcessMode::HostAbortCurrentPhysicalHeld
    ) {
        Channel::spawn_scenario(
            language,
            core,
            minor,
            if mode == ProcessMode::HostAbortOld {
                "runtime-native-host-abort-old"
            } else {
                "runtime-native-host-abort-current"
            },
        )
    } else if mode == ProcessMode::HostAbortCurrentWriteFailure {
        Channel::spawn_cancel_write_failure(language, core, minor)
    } else if resource_mode {
        let scenario = match mode {
            ProcessMode::HostResource => "runtime-native-host-resource-get",
            ProcessMode::HostResourceMissing => "runtime-native-host-resource-get-missing",
            ProcessMode::HostResourceScope => "runtime-native-host-resource-get-scope",
            ProcessMode::HostResourceVersion => "runtime-native-host-resource-get-version",
            ProcessMode::HostResourceParent => "runtime-native-host-resource-get-parent",
            _ => unreachable!(),
        };
        Channel::spawn_scenario(language, core, minor, scenario)
    } else if mode == ProcessMode::EventSink {
        Channel::spawn_scenario(language, core, minor, "runtime-native-event-sink")
    } else if mode == ProcessMode::EventSinkWrongId {
        Channel::spawn_scenario(language, core, minor, "runtime-native-event-sink-wrong-id")
    } else if mode == ProcessMode::BeforePayload {
        Channel::spawn_scenario(language, core, minor, "runtime-native-before-payload")
    } else if mode == ProcessMode::BeforePayloadMalformed {
        Channel::spawn_scenario(
            language,
            core,
            minor,
            "runtime-native-before-payload-malformed",
        )
    } else if structural_mode {
        Channel::spawn_scenario(
            language,
            core,
            minor,
            match mode {
                ProcessMode::Structural => "runtime-native-structural",
                ProcessMode::StructuralCompactionDecline => {
                    "runtime-native-structural-compaction-decline"
                }
                ProcessMode::StructuralNavigationDecline => {
                    "runtime-native-structural-navigation-decline"
                }
                _ => unreachable!(),
            },
        )
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
    let host = if event_mode {
        let mut limits = *service_host(core, profiles.clone()).limits();
        limits.passive_queue_bytes = 128;
        let permissions = profiles
            .iter()
            .flat_map(|profile| {
                profile.permissions().iter().map(|(name, scope)| {
                    pi_extension_protocol::wire::v1_1::PermissionGrant {
                        name: name.clone(),
                        scope: scope.clone(),
                    }
                })
            })
            .collect();
        pi_extension_protocol::activation::HostAuthority::new(
            "peer-a".into(),
            7,
            core,
            1,
            limits,
            vec![],
            permissions,
        )
        .unwrap()
        .with_profiles(profiles)
        .unwrap()
    } else {
        service_host(core, profiles)
    };
    let mut reservation = reservations();
    if event_mode {
        reservation.passive_bytes = 128;
    }
    let held_channel = (mode == ProcessMode::HostAbortCurrentPhysicalHeld)
        .then(|| runtime_extension_profiles_support::process::HeldCancelChannel::new(peer.clone()));
    let channel: Arc<dyn pi_harness_runtime::extensions::transport::OrderedByteChannel> =
        held_channel
            .as_ref()
            .map(|held| {
                held.clone()
                    as Arc<dyn pi_harness_runtime::extensions::transport::OrderedByteChannel>
            })
            .unwrap_or_else(|| {
                peer.clone()
                    as Arc<dyn pi_harness_runtime::extensions::transport::OrderedByteChannel>
            });
    let connection = if matches!(
        mode,
        ProcessMode::Changed
            | ProcessMode::ChangedWrongScope
            | ProcessMode::Telemetry
            | ProcessMode::TelemetryBadAck
            | ProcessMode::TelemetryBadFlush
            | ProcessMode::BeforeResume
            | ProcessMode::BeforePayload
            | ProcessMode::BeforePayloadMalformed
            | ProcessMode::HostSnapshot
            | ProcessMode::HostQueue
            | ProcessMode::HostInbox
            | ProcessMode::HostAbortOld
            | ProcessMode::HostAbortCurrent
            | ProcessMode::HostAbortCurrentPhysicalHeld
            | ProcessMode::HostAbortCurrentWriteFailure
            | ProcessMode::Structural
            | ProcessMode::StructuralCompactionDecline
            | ProcessMode::StructuralNavigationDecline
            | ProcessMode::HostResource
            | ProcessMode::HostResourceMissing
            | ProcessMode::HostResourceScope
            | ProcessMode::HostResourceVersion
            | ProcessMode::HostResourceParent
            | ProcessMode::EventSink
            | ProcessMode::EventSinkWrongId
    ) {
        drive(
            setup,
            harness.connect_profile_service(host, channel, clock, config, reservation, 20000),
        )
    } else {
        drive(
            setup,
            ProfileConnection::connect(host, peer.clone(), clock, config, reservation, 20000),
        )
    }
    .unwrap();
    if event_mode {
        assert_eq!(
            connection
                .grant()
                .wire()
                .limits
                .unwrap()
                .passive_queue_bytes,
            128
        );
    }
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
        if structural_mode {
            for name in ["before_compaction", "before_navigation"] {
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
        }
        if payload_mode {
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|profile| profile.reference().name == "before_payload")
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
                payload_mode.then(before_payload_schema),
            ),
        )
        .unwrap();
        drive(setup, harness.set_model(remote)).unwrap();
        if matches!(
            mode,
            ProcessMode::HostAbortOld
                | ProcessMode::HostAbortCurrent
                | ProcessMode::HostAbortCurrentPhysicalHeld
                | ProcessMode::HostAbortCurrentWriteFailure
        ) {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            use std::time::{Duration, Instant};
            let first = drive(
                setup,
                harness.prompt(PromptInput::Text {
                    text: "native profile peer dynamic run first".into(),
                    images: vec![],
                }),
            )
            .unwrap()
            .unwrap();
            assert!(matches!(
                first.outcome,
                RunOutcome::CompletedWithAssistant(_)
            ));
            let old_id = first.run_id.as_str().to_owned();
            let future = harness.prompt(PromptInput::Text {
                text: "native profile peer dynamic run successor".into(),
                images: vec![],
            });
            let mut future = std::pin::pin!(future);
            let deadline = Instant::now() + Duration::from_secs(4);
            let parent = loop {
                assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                setup.pump.tick();
                if let Some(request) = peer
                    .request_frames()
                    .into_iter()
                    .filter(|r| r.method == "provider.invoke")
                    .nth(1)
                {
                    break request;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite successor provider parent: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            let parent_key = parent
                .effect_key
                .as_ref()
                .expect("successor assistant EffectKey");
            assert_eq!(parent_key.effect_kind, "assistant");
            assert_eq!(parent_key.session_id, "runtime-session");
            assert_eq!(parent_key.lane, "main");
            assert_ne!(parent_key.operation_id, old_id);
            let before = setup.pump.drive(store.snapshot()).unwrap();
            peer.signal("actions");
            let deadline = Instant::now() + Duration::from_secs(4);
            let action = loop {
                assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                setup.pump.tick();
                if let Some(request) = peer
                    .peer_request_frames()
                    .into_iter()
                    .find(|r| r.method == "operation.abort")
                {
                    break request;
                }
                assert!(
                    Instant::now() < deadline,
                    "selected abort child missing: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| {
                    p.reference().name == "actions"
                        && p.reference().profile.as_ref().unwrap().name == "host-action.v1"
                })
                .unwrap()
                .reference();
            assert_eq!(action.request_id, 1000);
            assert!(action.request_id > parent.request_id);
            assert_eq!(action.capability_ref.as_ref(), Some(selected));
            assert_eq!(action.parent_request_id, Some(parent.request_id));
            assert_eq!(action.effect_key, None);
            assert_eq!(action.initial_stream_credit_bytes, Some(0));
            assert!(
                action
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 2000)
            );
            let payload = dto::OperationAbortRequest::decode(action.payload.as_slice()).unwrap();
            assert_eq!(payload.lane, "main");
            if let Some(held) = held_channel.as_ref() {
                held.set_expected_cancel_id(parent.request_id);
            }
            if mode == ProcessMode::HostAbortOld && payload.operation_id == parent_key.operation_id
            {
                let deadline = Instant::now() + Duration::from_secs(4);
                loop {
                    setup.pump.tick();
                    if peer.response_frames().len() == 1 {
                        break;
                    }
                    assert!(Instant::now() < deadline, "finite current-ID mutant reply");
                    std::thread::sleep(Duration::from_millis(1));
                }
                let response = &peer.response_frames()[0];
                let Some(v1_1::response::Terminal::Result(body)) = &response.terminal else {
                    panic!("current-ID mutant typed Result")
                };
                assert!(
                    dto::OperationAbortResult::decode(body.as_slice())
                        .unwrap()
                        .aborted
                );
                assert_ne!(
                    setup.pump.drive(store.snapshot()).unwrap(),
                    before,
                    "current-ID mutant must write cancellation state"
                );
                panic!("old-ID no-write witness rejects current-ID abort");
            }
            assert_eq!(
                payload.operation_id,
                if mode == ProcessMode::HostAbortOld {
                    old_id.as_str()
                } else {
                    parent_key.operation_id.as_str()
                }
            );
            if mode == ProcessMode::HostAbortOld {
                let deadline = Instant::now() + Duration::from_secs(4);
                loop {
                    assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                    setup.pump.tick();
                    if peer.response_frames().len() == 1 {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "old-ID abort reply missing: {}",
                        peer.stderr_text()
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                let response = &peer.response_frames()[0];
                assert_eq!(response.request_id, action.request_id);
                let Some(v1_1::response::Terminal::Result(body)) = &response.terminal else {
                    panic!("old-ID typed abort Result: {:?}", response.terminal)
                };
                let result = dto::OperationAbortResult::decode(body.as_slice()).unwrap();
                assert!(!result.aborted);
                assert_eq!(result.encode_to_vec(), *body);
                assert_eq!(
                    setup.pump.drive(store.snapshot()).unwrap(),
                    before,
                    "old-ID abort is full Session no-write"
                );
                assert!(connection.current());
                peer.signal("provider");
                let second = drive(setup, future).unwrap().unwrap();
                assert_eq!(second.run_id.as_str(), parent_key.operation_id);
                assert!(matches!(
                    second.outcome,
                    RunOutcome::CompletedWithAssistant(_)
                ));
                assert_eq!(connection.occupancy(), 0);
                peer.finish(true);
                connection.close();
                setup.pump.drive(harness.close()).unwrap();
                return;
            }
            let mut cancel_marker_seqs = Vec::new();
            let mut cancel_requested_at = None;
            let write_fault_mode = mode == ProcessMode::HostAbortCurrentWriteFailure;
            let mut held_cancel_released = false;
            let deadline = Instant::now() + Duration::from_secs(4);
            let call = loop {
                setup.pump.tick();
                if let Some(held) = held_channel.as_ref()
                    && !held_cancel_released
                    && held.cancel_id().is_some()
                {
                    let bytes = held.cancel_bytes().expect("captured Cancel bytes");
                    assert!(!bytes.is_empty(), "physical Cancel frame bytes");
                    assert_eq!(held.cancel_id(), Some(parent.request_id));
                    assert!(
                        held.reader_eof_published(),
                        "reader clean EOF is delivered while Cancel write is held"
                    );
                    for _ in 0..50 {
                        setup.pump.tick();
                        if held.reader_eof_read() {
                            break;
                        }
                    }
                    assert!(held.reader_eof_read(), "reader consumed clean EOF");
                    assert_eq!(
                        held.close_count(),
                        0,
                        "reader clean EOF cannot close a held physical Cancel"
                    );
                    assert!(
                        connection.current(),
                        "active owner binding survives reader EOF during held Cancel"
                    );
                    held.release();
                    held_cancel_released = true;
                }
                let state = setup.pump.drive(store.snapshot()).unwrap();
                for register in &state.registers {
                    if let Register::OperationState { key, value, seq } = register
                        && key == &parent_key.operation_id
                        && let OperationState::Run(run) = value.as_ref()
                        && let Control::CancelRequested(marker) = &run.control
                    {
                        assert!(marker.drained_steer.is_empty());
                        assert!(marker.drained_follow_up.is_empty());
                        assert!(run.inbox.steer.is_empty());
                        assert!(run.inbox.follow_up.is_empty());
                        match cancel_requested_at {
                            Some(time) => assert_eq!(
                                marker.requested_at.get(),
                                time,
                                "one stable abort request timestamp through reconciliation"
                            ),
                            None => cancel_requested_at = Some(marker.requested_at.get()),
                        }
                        if !cancel_marker_seqs.contains(&seq.get()) {
                            cancel_marker_seqs.push(seq.get());
                        }
                    }
                }
                if let std::task::Poll::Ready(result) = runtime_r1_r4_support::poll(future.as_mut())
                {
                    break result;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite exact-current abort: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            if mode == ProcessMode::HostAbortCurrentPhysicalHeld {
                assert!(
                    held_cancel_released,
                    "held physical Cancel write was exercised and released"
                );
                let close_deadline = Instant::now() + Duration::from_secs(4);
                while held_channel.as_ref().unwrap().close_count() == 0 {
                    setup.pump.tick();
                    if Instant::now() >= close_deadline {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                eprintln!(
                    "PHYSICAL_HELD cancel_id={:?} close_count={} call={call:?}",
                    held_channel.as_ref().unwrap().cancel_id(),
                    held_channel.as_ref().unwrap().close_count(),
                );
                assert_eq!(
                    held_channel.as_ref().unwrap().cancel_count(),
                    1,
                    "exact admitted parent Cancel is the sole held physical write"
                );
                assert!(
                    !held_channel.as_ref().unwrap().early_close(),
                    "final close is deferred until physical Cancel publication"
                );
                assert!(
                    held_channel.as_ref().unwrap().close_count() == 1,
                    "exactly one final close occurs after physical Cancel publication"
                );
                assert!(
                    held_channel.as_ref().unwrap().cancel_complete(),
                    "physical Cancel write completed before acknowledgement publication"
                );
            }
            if write_fault_mode {
                peer.close();
                peer.finish(false);
                connection.close();
                let close = setup.pump.drive(harness.close());
                assert!(
                    matches!(close, Err(HarnessRuntimeError::Fault(_))),
                    "write fault mode result: {call:?}, close: {close:?}"
                );
                return;
            }
            let after = setup.pump.drive(store.snapshot()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(4);
            let order = loop {
                setup.pump.tick();
                let order = peer.action_control_order(action.request_id, parent.request_id);
                if order.len() >= 2 {
                    break order;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite child Response/parent Cancel order: {order:?}; {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            eprintln!(
                "EXACT_CURRENT_ABORT language={language} core={core} minor={minor} old={old_id} current={} marker_seqs={cancel_marker_seqs:?} order={order:?} call={call:?} response_count={} before_seq={} after_seq={} peer={}",
                parent_key.operation_id,
                peer.response_frames().len(),
                before.next_seq.unwrap().get(),
                after.next_seq.unwrap().get(),
                peer.stderr_text()
            );
            assert!(
                cancel_requested_at.is_some(),
                "durable CancelRequested observed"
            );
            assert_eq!(
                order.len(),
                2,
                "one child Response precedes one parent Cancel"
            );
            assert!(order[0].starts_with("action-response:"));
            assert!(order[1].starts_with("parent-cancel:"));
            let response = &peer.response_frames()[0];
            assert_eq!(response.request_id, action.request_id);
            let Some(v1_1::response::Terminal::Result(body)) = &response.terminal else {
                panic!("exact-current typed abort Result: {:?}", response.terminal)
            };
            let result = dto::OperationAbortResult::decode(body.as_slice()).unwrap();
            assert!(result.aborted);
            assert_eq!(result.encode_to_vec(), *body);
            assert_ne!(
                after, before,
                "exact-current abort must write native cancellation state"
            );
            assert!(
                matches!(call, Ok(Ok(ref completion)) if matches!(completion.outcome,
                RunOutcome::AbortedWithAssistant(_) | RunOutcome::AbortedWithoutAssistant(_)))
            );
            assert_eq!(
                after
                    .registers
                    .iter()
                    .filter(|r| matches!(r,
                Register::LaneLastResult { key, value: LaneLastResult::RunAborted(last), .. }
                    if key == "main" && last.operation_id.as_str() == parent_key.operation_id))
                    .count(),
                1
            );
            if matches!(
                mode,
                ProcessMode::HostAbortCurrentWriteFailure
                    | ProcessMode::HostAbortCurrentPhysicalHeld
            ) {
                peer.close();
                peer.finish(false);
            } else {
                peer.finish(true);
            }
            connection.close();
            let close = setup.pump.drive(harness.close());
            if matches!(mode, ProcessMode::HostAbortCurrentWriteFailure) {
                assert!(matches!(close, Err(HarnessRuntimeError::Fault(_))));
            } else {
                close.unwrap();
            }
            return;
        }
        let before_payload = (mode == ProcessMode::BeforePayload)
            .then(|| setup.pump.drive(store.snapshot()).unwrap());
        let before_malformed = (mode == ProcessMode::BeforePayloadMalformed)
            .then(|| setup.pump.drive(store.snapshot()).unwrap());
        let mut queued_item_id = None;
        let call = if matches!(
            mode,
            ProcessMode::HostSnapshot | ProcessMode::HostQueue | ProcessMode::HostInbox
        ) || resource_mode
            || event_mode
        {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            use std::time::{Duration, Instant};
            let future = harness.prompt(PromptInput::Text {
                text: "native profile peer dynamic run".into(),
                images: vec![],
            });
            let mut future = std::pin::pin!(future);
            let deadline = Instant::now() + Duration::from_secs(4);
            let parent = loop {
                assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                setup.pump.tick();
                if let Some(request) = peer
                    .request_frames()
                    .into_iter()
                    .find(|r| r.method == "provider.invoke")
                {
                    break request;
                }
                assert!(Instant::now() < deadline, "finite live provider parent");
                std::thread::sleep(Duration::from_millis(1));
            };
            let parent_key = parent
                .effect_key
                .as_ref()
                .expect("selected provider EffectKey");
            assert_eq!(parent_key.effect_kind, "assistant");
            assert_eq!(parent_key.lane, "main");
            assert_eq!(parent_key.session_id, "runtime-session");
            assert_eq!(parent.initial_stream_credit_bytes, Some(0));
            assert!(
                parent
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            assert_eq!(peer.peer_request_frames().len(), 0, "actions await release");
            let before_actions = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(before_actions.usage.len(), 1);
            if mode == ProcessMode::HostInbox {
                let selected = connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| {
                        p.reference().name == "actions"
                            && p.reference().profile.as_ref().unwrap().name == "host-action.v1"
                    })
                    .unwrap()
                    .reference();
                let mut before = before_actions;
                peer.signal("actions");
                for phase in 0..4 {
                    let deadline = Instant::now() + Duration::from_secs(4);
                    loop {
                        assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                        setup.pump.tick();
                        if peer.response_frames().len() == phase + 1 {
                            break;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "selected inbox child reply missing at {phase}: {}",
                            peer.stderr_text()
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    let requests = peer.peer_request_frames();
                    assert_eq!(requests.len(), phase + 1);
                    let request = &requests[phase];
                    let response = &peer.response_frames()[phase];
                    let method = if phase % 2 == 0 {
                        "queue.steer"
                    } else {
                        "queue.follow_up"
                    };
                    let lane = if phase == 3 { "other" } else { "main" };
                    let content = if phase % 2 == 0 {
                        "native steer item"
                    } else {
                        "native follow-up item"
                    };
                    assert_eq!(request.request_id, 1002 + phase as u64);
                    assert!(request.request_id > parent.request_id && request.request_id < 2000);
                    assert_eq!(request.method, method);
                    assert_eq!(request.capability_ref.as_ref(), Some(selected));
                    if phase == 0 && request.parent_request_id.is_none() {
                        let Some(v1_1::response::Terminal::Error(error)) = &response.terminal
                        else {
                            panic!("missing steer parent must be refused")
                        };
                        assert_eq!(error.code, v1_1::ErrorCode::InvalidParent as i32);
                        assert_eq!(
                            setup.pump.drive(store.snapshot()).unwrap(),
                            before,
                            "missing steer parent is full Session no-write"
                        );
                        panic!("positive steer admission requires selected live parent");
                    }
                    assert_eq!(
                        request.parent_request_id,
                        (phase != 2).then_some(parent.request_id)
                    );
                    assert_eq!(request.effect_key, None);
                    assert_eq!(request.initial_stream_credit_bytes, Some(0));
                    assert!(
                        request
                            .deadline_after_ms
                            .is_some_and(|ms| ms > 0 && ms <= 2000)
                    );
                    let (actual_lane, item) = if phase % 2 == 0 {
                        let p = dto::QueueSteerRequest::decode(request.payload.as_slice()).unwrap();
                        (p.lane, p.item.unwrap())
                    } else {
                        let p =
                            dto::QueueFollowUpRequest::decode(request.payload.as_slice()).unwrap();
                        (p.lane, p.item.unwrap())
                    };
                    assert_eq!(actual_lane, lane);
                    assert_eq!(item.projection_id, "host.queue_item.v1");
                    assert_eq!(
                        item.schema_digest_sha256,
                        pi_extension_protocol::profiles::Registry::bundled()
                            .unwrap()
                            .digest("host.queue_item.v1")
                            .unwrap()
                    );
                    let message = serde_json::json!({
                        "role":"user", "content":content, "timestamp":7 + phase
                    });
                    assert_eq!(wire_value(item.value.unwrap()), message);
                    assert_eq!(response.request_id, request.request_id);
                    let after = setup.pump.drive(store.snapshot()).unwrap();
                    if phase < 2 {
                        let Some(v1_1::response::Terminal::Result(body)) = &response.terminal
                        else {
                            panic!("selected {method} Result: {:?}", response.terminal)
                        };
                        let result = dto::QueueResult::decode(body.as_slice()).unwrap();
                        assert!(result.accepted && !result.item_id.is_empty());
                        assert_eq!(result.encode_to_vec(), *body);
                        assert_eq!(after.entries, before.entries);
                        assert_eq!(after.usage, before.usage);
                        assert_eq!(after.stats, before.stats);
                        assert_eq!(after.storage_version, before.storage_version);
                        assert_eq!(after.store_generation, before.store_generation);
                        assert_eq!(
                            after.next_seq.unwrap().get(),
                            before.next_seq.unwrap().get() + 2
                        );
                        let prior = before
                            .registers
                            .iter()
                            .filter(|r| matches!(r, Register::PendingEntry { .. }))
                            .count();
                        let pending = after
                            .registers
                            .iter()
                            .filter_map(|r| match r {
                                Register::PendingEntry { key, value, .. } => Some((key, value)),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(pending.len(), prior + 1);
                        let (_, value) = pending
                            .iter()
                            .find(|(key, _)| *key == &result.item_id)
                            .expect("minted pending item");
                        assert_eq!(value.entry_type, PendingEntryType::Message);
                        let pi_ai_core::Presence::Value(payload) = &value.payload else {
                            panic!("pending message payload")
                        };
                        assert_eq!(serde_json::to_value(payload).unwrap(), message);
                        let run = |registers: &Vec<Register>| {
                            registers
                                .iter()
                                .find_map(|r| match r {
                                    Register::OperationState { key, value, .. }
                                        if key == &parent_key.operation_id =>
                                    {
                                        match value.as_ref() {
                                            OperationState::Run(run) => Some(run.as_ref().clone()),
                                            _ => None,
                                        }
                                    }
                                    _ => None,
                                })
                                .unwrap()
                        };
                        let old_run = run(&before.registers);
                        let new_run = run(&after.registers);
                        let mut expected = old_run.clone();
                        if phase == 0 {
                            expected
                                .inbox
                                .steer
                                .push(EntryId::new(result.item_id.clone()).unwrap());
                        } else {
                            expected
                                .inbox
                                .follow_up
                                .push(EntryId::new(result.item_id.clone()).unwrap());
                        }
                        assert_eq!(new_run, expected);
                        let other = |registers: &Vec<Register>| {
                            registers.iter().filter(|r|
                            !matches!(r, Register::PendingEntry { .. }) &&
                            !matches!(r, Register::OperationState { key, .. } if key == &parent_key.operation_id)
                        ).cloned().collect::<Vec<_>>()
                        };
                        assert_eq!(other(&after.registers), other(&before.registers));
                    } else {
                        let Some(v1_1::response::Terminal::Error(error)) = &response.terminal
                        else {
                            panic!("typed {method} refusal")
                        };
                        assert_eq!(
                            error.code,
                            if phase == 2 {
                                v1_1::ErrorCode::InvalidParent as i32
                            } else {
                                v1_1::ErrorCode::Forbidden as i32
                            }
                        );
                        assert_eq!(error.safe_message, "request failed");
                        assert_eq!(after, before, "refusal is full Session no-write");
                    }
                    before = after;
                    peer.signal(
                        [
                            "inbox-follow-up",
                            "inbox-denials",
                            "inbox-wrong-lane",
                            "provider",
                        ][phase],
                    );
                }
                let completion = drive(setup, future)
                    .unwrap_or_else(|error| {
                        panic!(
                            "inbox prompt fault: {error:?}; peer={}; wire={:?}",
                            peer.stderr_text(),
                            peer.request_frames()
                                .iter()
                                .map(|r| r.method.as_str())
                                .collect::<Vec<_>>()
                        )
                    })
                    .unwrap();
                assert!(matches!(
                    completion.outcome,
                    RunOutcome::CompletedWithAssistant(_)
                ));
                assert_eq!(connection.occupancy(), 0);
                peer.finish(true);
                connection.close();
                setup.pump.drive(harness.close()).unwrap();
                return;
            }
            if resource_mode {
                let selected = connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| {
                        p.reference().name == "actions-resource"
                            && p.reference().profile.as_ref().unwrap().name == "host-action.v1"
                    })
                    .unwrap()
                    .reference();
                let check_result = |response: &v1_1::Response| {
                    let Some(v1_1::response::Terminal::Result(body)) = &response.terminal else {
                        panic!("real resource action Result: {:?}", response.terminal);
                    };
                    let result = dto::ResourceGetResult::decode(body.as_slice()).unwrap();
                    let snapshot = result.snapshot.unwrap();
                    assert_eq!(snapshot.projection_id, "host.resource_snapshot.v1");
                    assert_eq!(
                        snapshot
                            .schema_digest_sha256
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>(),
                        "a2a4403093db407c6cd2efbe7d9c47689ebdd5c065fdb70bc8b43d90337e4a7f"
                    );
                    assert_eq!(
                        wire_value(snapshot.value.unwrap()),
                        serde_json::json!({
                            "id":"s", "version":"1", "kind":"skill", "value":"x",
                            "digest":"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
                        })
                    );
                };
                peer.signal("actions");
                let deadline = Instant::now() + Duration::from_secs(4);
                loop {
                    assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                    setup.pump.tick();
                    if peer.response_frames().len() == 1 {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "first resource action: {}",
                        peer.stderr_text()
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                let actions = peer.peer_request_frames();
                assert_eq!(actions.len(), 1);
                let first = &actions[0];
                assert_eq!(first.method, "resource.get", "real child Request");
                assert_eq!(first.request_id, 1000);
                assert!(first.request_id > parent.request_id && first.request_id < 2000);
                assert_eq!(first.capability_ref.as_ref(), Some(selected));
                assert_eq!(first.parent_request_id, Some(parent.request_id));
                assert_eq!(first.effect_key, None);
                assert_eq!(first.initial_stream_credit_bytes, Some(0));
                assert!(
                    first
                        .deadline_after_ms
                        .is_some_and(|ms| ms > 0 && ms <= 2000)
                );
                assert_eq!(
                    dto::ResourceGetRequest::decode(first.payload.as_slice()).unwrap(),
                    dto::ResourceGetRequest {
                        scope: "allowed".into(),
                        id: "s".into(),
                        version: Some("1".into())
                    }
                );
                let responses = peer.response_frames();
                assert_eq!(responses[0].request_id, 1000);
                check_result(&responses[0]);
                assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_actions);
                assert!(connection.current());
                assert_eq!(revision.revision(), "r1");
                peer.signal("resource-changed");
                let deadline = Instant::now() + Duration::from_secs(4);
                loop {
                    assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                    setup.pump.tick();
                    if peer.response_frames().len() == 2 {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "resource.changed Ack: {}",
                        peer.stderr_text()
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                let changed = &peer.peer_request_frames()[1];
                assert_eq!(changed.request_id, 1500);
                assert_eq!(changed.method, "resource.changed");
                assert_eq!(changed.capability_ref.as_ref(), Some(&cap));
                assert_eq!(changed.parent_request_id, None);
                assert_eq!(changed.effect_key, None);
                assert_eq!(changed.initial_stream_credit_bytes, Some(0));
                assert_eq!(
                    dto::ResourceChanged::decode(changed.payload.as_slice())
                        .unwrap()
                        .scope,
                    "allowed"
                );
                let responses = peer.response_frames();
                assert_eq!(responses[1].request_id, 1500);
                let Some(v1_1::response::Terminal::Result(body)) = &responses[1].terminal else {
                    panic!("typed resource.changed Ack");
                };
                dto::ResourceChangedAck::decode(body.as_slice()).unwrap();
                assert!(catalog.refresh_pending());
                // The first connection's provider owns its serial outgoing-call slot.
                // A distinct selected resource process supplies the complete refresh
                // while the original child and provider parent remain live.
                let refresh_peer =
                    Channel::spawn_scenario(language, core, minor, "runtime-native-resource-r2");
                let refresh_connection = drive(
                    setup,
                    harness.connect_profile_service(
                        service_host(core, accepted_profiles()),
                        refresh_peer.clone(),
                        Arc::new(RealClock::default()),
                        config,
                        reservations(),
                        20000,
                    ),
                )
                .unwrap();
                let refresh_cap = refresh_connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| p.reference().name == "resources")
                    .unwrap()
                    .reference()
                    .clone();
                assert_eq!(refresh_cap, cap);
                let refreshed = drive(setup, harness.refresh_profile_resources(refresh_connection.clone(), refresh_cap.clone(), &catalog))
                    .unwrap_or_else(|error| panic!(
                        "independent selected refresh: {error:?}; current={}; requests={:?}; stderr={}",
                        refresh_connection.current(),
                        refresh_peer.request_frames().iter().map(|r| (&r.method, r.request_id)).collect::<Vec<_>>(),
                        refresh_peer.stderr_text()
                    ));
                let refresh_requests = refresh_peer.request_frames();
                assert_eq!(
                    refresh_requests
                        .iter()
                        .map(|r| r.method.as_str())
                        .collect::<Vec<_>>(),
                    ["resource.list", "resource.read"]
                );
                assert!(
                    refresh_requests
                        .iter()
                        .all(|r| r.capability_ref.as_ref() == Some(&refresh_cap)
                            && r.initial_stream_credit_bytes == Some(0))
                );
                let refresh_replies = refresh_peer.peer_response_frames();
                assert_eq!(refresh_replies.len(), 2);
                let Some(v1_1::response::Terminal::Result(list_body)) =
                    &refresh_replies[0].terminal
                else {
                    panic!("selected r2 resource.list Result");
                };
                let list = dto::ResourceListResult::decode(list_body.as_slice()).unwrap();
                assert_eq!(list.revision, "r2");
                assert_eq!(list.resources.len(), 1);
                assert_eq!(
                    (
                        list.resources[0].id.as_str(),
                        list.resources[0].version.as_str(),
                        list.resources[0].kind.as_str()
                    ),
                    ("s", "2", "skill")
                );
                let Some(v1_1::response::Terminal::Result(read_body)) =
                    &refresh_replies[1].terminal
                else {
                    panic!("selected r2 resource.read Result");
                };
                let read = dto::ResourceReadResult::decode(read_body.as_slice()).unwrap();
                assert_eq!(read.digest_sha256, list.resources[0].digest_sha256);
                assert_eq!(
                    read.value,
                    Some(dto::resource_read_result::Value::Text("y".into()))
                );
                refresh_peer.finish(true);
                refresh_connection.close();
                assert_eq!(refreshed.revision(), "r2");
                assert_eq!(refreshed.get("s", Some("2")).unwrap().value, "y");
                assert_eq!(
                    refreshed.get("s", Some("2")).unwrap().digest,
                    "a1fce4363854ff888cff4b8e7875d600c2682390412a8cf79b37d0b11148b0fa"
                );
                assert!(Arc::ptr_eq(&catalog.capture().unwrap(), &refreshed));
                assert!(!catalog.refresh_pending());
                assert!(!Arc::ptr_eq(&revision, &refreshed));
                assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
                assert_eq!(
                    revision.get("s", Some("1")).unwrap().digest,
                    "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
                );
                assert!(connection.current());
                assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                let before_second = setup.pump.drive(store.snapshot()).unwrap();
                peer.signal("resource-second");
                let deadline = Instant::now() + Duration::from_secs(4);
                loop {
                    if let std::task::Poll::Ready(result) =
                        runtime_r1_r4_support::poll(future.as_mut())
                    {
                        panic!(
                            "provider settled before second action: {result:?}; current={}; responses={:?}; requests={:?}; stderr={}",
                            connection.current(),
                            peer.response_frames()
                                .iter()
                                .map(|r| r.request_id)
                                .collect::<Vec<_>>(),
                            peer.peer_request_frames()
                                .iter()
                                .map(|r| (&r.method, r.request_id))
                                .collect::<Vec<_>>(),
                            peer.stderr_text()
                        );
                    }
                    setup.pump.tick();
                    if peer.response_frames().len() == 3 {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "second resource action: {}",
                        peer.stderr_text()
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                let actions = peer.peer_request_frames();
                assert_eq!(actions.len(), 3);
                let second = &actions[2];
                assert_eq!(second.request_id, 1501);
                assert_eq!(second.method, "resource.get");
                if second.capability_ref.as_ref() != Some(selected) {
                    let responses = peer.response_frames();
                    let Some(v1_1::response::Terminal::Error(error)) = &responses[2].terminal
                    else {
                        panic!("wrong capability must be refused");
                    };
                    assert_eq!(error.code, v1_1::ErrorCode::Forbidden as i32);
                    assert_eq!(error.safe_message, "request failed");
                    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_second);
                    panic!("second resource.get lost its selected resource.read capability");
                }
                assert_eq!(second.capability_ref.as_ref(), Some(selected));
                assert_eq!(
                    second.parent_request_id,
                    (mode != ProcessMode::HostResourceParent).then_some(parent.request_id)
                );
                assert_eq!(second.effect_key, None);
                assert_eq!(second.initial_stream_credit_bytes, Some(0));
                assert!(
                    second
                        .deadline_after_ms
                        .is_some_and(|ms| ms > 0 && ms <= 2000)
                );
                let expected = match mode {
                    ProcessMode::HostResourceMissing => ("allowed", "missing-s", None),
                    ProcessMode::HostResourceScope => ("wrong-scope", "s", Some("1".into())),
                    ProcessMode::HostResourceVersion => ("allowed", "s", Some("2".into())),
                    _ => ("allowed", "s", Some("1".into())),
                };
                assert_eq!(
                    dto::ResourceGetRequest::decode(second.payload.as_slice()).unwrap(),
                    dto::ResourceGetRequest {
                        scope: expected.0.into(),
                        id: expected.1.into(),
                        version: expected.2
                    }
                );
                let responses = peer.response_frames();
                assert_eq!(responses[2].request_id, 1501);
                if mode == ProcessMode::HostResource {
                    check_result(&responses[2]);
                } else {
                    let code = match mode {
                        ProcessMode::HostResourceMissing => v1_1::ErrorCode::NotFound,
                        ProcessMode::HostResourceScope => v1_1::ErrorCode::Forbidden,
                        ProcessMode::HostResourceVersion => v1_1::ErrorCode::VersionMismatch,
                        ProcessMode::HostResourceParent => v1_1::ErrorCode::InvalidParent,
                        _ => unreachable!(),
                    };
                    let Some(v1_1::response::Terminal::Error(error)) = &responses[2].terminal
                    else {
                        panic!("typed resource refusal: {:?}", responses[2].terminal);
                    };
                    assert_eq!(error.code, code as i32);
                    assert_eq!(error.safe_message, "request failed");
                }
                assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_second);
                assert_eq!(
                    setup.pump.drive(store.snapshot()).unwrap().usage,
                    before_second.usage
                );
                assert!(connection.current());
                assert_eq!(catalog.capture().unwrap().revision(), "r2");
                assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                peer.signal("provider");
                drive(setup, future)
            } else {
                peer.signal("actions");
                let deadline = Instant::now() + Duration::from_secs(4);
                loop {
                    assert!(
                        runtime_r1_r4_support::poll(future.as_mut()).is_pending(),
                        "provider must remain Open: {}",
                        peer.stderr_text()
                    );
                    setup.pump.tick();
                    if peer.response_frames().len()
                        == if mode == ProcessMode::HostQueue { 3 } else { 2 }
                    {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "finite child action replies: {}",
                        peer.stderr_text()
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                let actions = peer.peer_request_frames();
                assert_eq!(
                    actions.len(),
                    if mode == ProcessMode::HostQueue { 3 } else { 2 }
                );
                let selected = connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| {
                        p.reference().name == "actions"
                            && p.reference().profile.as_ref().unwrap().name == "host-action.v1"
                    })
                    .unwrap()
                    .reference();
                for (index, request) in actions.iter().take(2).enumerate() {
                    assert_eq!(request.request_id, 1000 + index as u64);
                    assert!(request.request_id > parent.request_id && request.request_id < 2000);
                    assert_eq!(request.method, "snapshot.get");
                    assert_eq!(request.capability_ref.as_ref(), Some(selected));
                    assert_eq!(
                        request.parent_request_id,
                        (index == 0).then_some(parent.request_id)
                    );
                    assert_eq!(request.effect_key, None);
                    assert_eq!(request.initial_stream_credit_bytes, Some(0));
                    assert!(
                        request
                            .deadline_after_ms
                            .is_some_and(|ms| ms > 0 && ms <= 2000)
                    );
                    assert_eq!(
                        dto::SnapshotGetRequest::decode(request.payload.as_slice()).unwrap(),
                        dto::SnapshotGetRequest {
                            lane: "main".into(),
                            visibility: "captured_policy".into()
                        }
                    );
                }
                let responses = peer.response_frames();
                let Some(v1_1::response::Terminal::Result(body)) = &responses[0].terminal else {
                    panic!("real snapshot action Result: {:?}", responses[0].terminal);
                };
                assert_eq!(responses[0].request_id, 1000);
                let snapshot_result = dto::SnapshotGetResult::decode(body.as_slice()).unwrap();
                let filtered = snapshot_result.filtered_snapshot.unwrap();
                assert_eq!(filtered.projection_id, "host.filtered_snapshot.v1");
                let value = wire_value(filtered.value.unwrap());
                assert_eq!(value["lane"], "main");
                assert_eq!(
                    value["operation"]["id"],
                    parent.effect_key.as_ref().unwrap().operation_id
                );
                assert!(!value.to_string().contains("SENTINEL"));
                assert_eq!(responses[1].request_id, 1001);
                let Some(v1_1::response::Terminal::Error(error)) = &responses[1].terminal else {
                    panic!("typed missing-parent refusal");
                };
                assert_eq!(error.code, v1_1::ErrorCode::InvalidParent as i32);
                assert_eq!(error.safe_message, "request failed");
                if mode == ProcessMode::HostQueue {
                    let queue = &actions[2];
                    assert_eq!(queue.request_id, 1002);
                    assert!(queue.request_id > parent.request_id && queue.request_id < 2000);
                    assert_eq!(queue.method, "queue.next_run");
                    assert_eq!(queue.capability_ref.as_ref(), Some(selected));
                    if queue.parent_request_id.is_none() {
                        let Some(v1_1::response::Terminal::Error(error)) =
                            &peer.response_frames()[2].terminal
                        else {
                            panic!("missing queue parent must be denied")
                        };
                        assert_eq!(error.code, v1_1::ErrorCode::InvalidParent as i32);
                        assert_eq!(error.safe_message, "request failed");
                        assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_actions);
                    }
                    assert_eq!(queue.parent_request_id, Some(parent.request_id));
                    assert_eq!(queue.effect_key, None);
                    assert_eq!(queue.initial_stream_credit_bytes, Some(0));
                    assert!(
                        queue
                            .deadline_after_ms
                            .is_some_and(|ms| ms > 0 && ms <= 2000)
                    );
                    let queued =
                        dto::QueueNextRunRequest::decode(queue.payload.as_slice()).unwrap();
                    assert_eq!(queued.lane, "main");
                    let item = queued.item.expect("selected bound queue item");
                    assert_eq!(item.projection_id, "host.queue_item.v1");
                    assert_eq!(
                        item.schema_digest_sha256,
                        pi_extension_protocol::profiles::Registry::bundled()
                            .unwrap()
                            .digest("host.queue_item.v1")
                            .unwrap()
                    );
                    let queued_message = serde_json::json!({
                        "role": "user", "content": "native queued item", "timestamp": 7
                    });
                    assert_eq!(wire_value(item.value.unwrap()), queued_message);
                    let Some(v1_1::response::Terminal::Result(body)) = &responses[2].terminal
                    else {
                        if let Some(v1_1::response::Terminal::Error(error)) = &responses[2].terminal
                        {
                            assert_eq!(error.code, v1_1::ErrorCode::InvalidParent as i32);
                            assert_eq!(error.safe_message, "request failed");
                            assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_actions);
                        }
                        panic!("real queue.next_run Result: {:?}", responses[2].terminal);
                    };
                    assert_eq!(responses[2].request_id, 1002);
                    let result = dto::QueueResult::decode(body.as_slice()).unwrap();
                    assert!(result.accepted && !result.item_id.is_empty());
                    assert_eq!(
                        result.encode_to_vec(),
                        *body,
                        "typed queue Result round trip"
                    );
                    queued_item_id = Some(result.item_id.clone());
                    let after_queue = setup.pump.drive(store.snapshot()).unwrap();
                    assert_eq!(after_queue.entries, before_actions.entries);
                    assert_eq!(after_queue.usage, before_actions.usage);
                    assert_eq!(after_queue.stats, before_actions.stats);
                    assert_eq!(after_queue.storage_version, before_actions.storage_version);
                    assert_eq!(
                        after_queue.store_generation,
                        before_actions.store_generation
                    );
                    assert_eq!(
                        after_queue.next_seq.unwrap().get(),
                        before_actions.next_seq.unwrap().get() + 2,
                        "queue admission writes only PendingEntry and LaneState"
                    );
                    let pending_before = before_actions
                        .registers
                        .iter()
                        .filter(|r| matches!(r, Register::PendingEntry { .. }))
                        .count();
                    assert_eq!(pending_before, 0, "selected queue starts empty");
                    let pending = after_queue
                        .registers
                        .iter()
                        .filter_map(|r| match r {
                            Register::PendingEntry { key, value, .. } => Some((key, value)),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(pending.len(), pending_before + 1);
                    let (key, value) = pending
                        .iter()
                        .find(|(key, _)| *key == &result.item_id)
                        .expect("minted pending item");
                    assert_eq!(*key, &result.item_id);
                    assert_eq!(value.entry_type, PendingEntryType::Message);
                    let pi_ai_core::Presence::Value(payload) = &value.payload else {
                        panic!("pending message payload")
                    };
                    assert_eq!(serde_json::to_value(payload).unwrap(), queued_message);
                    let before_lane = before_actions
                        .registers
                        .iter()
                        .find_map(|r| match r {
                            Register::LaneState { key, value, .. } if key == "main" => Some(value),
                            _ => None,
                        })
                        .unwrap();
                    let after_lane = after_queue
                        .registers
                        .iter()
                        .find_map(|r| match r {
                            Register::LaneState { key, value, .. } if key == "main" => Some(value),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(
                        after_lane.current_operation_id,
                        before_lane.current_operation_id
                    );
                    assert_eq!(
                        after_lane.pending_next_run.len(),
                        before_lane.pending_next_run.len() + 1
                    );
                    assert_eq!(
                        &after_lane.pending_next_run[..before_lane.pending_next_run.len()],
                        before_lane.pending_next_run.as_slice()
                    );
                    assert_eq!(
                        after_lane.pending_next_run.last().unwrap().as_str(),
                        result.item_id
                    );
                    let other = |registers: &Vec<Register>| {
                        registers
                            .iter()
                            .filter(|r| match r {
                                Register::PendingEntry { .. } => false,
                                Register::LaneState { key, .. } if key == "main" => false,
                                _ => true,
                            })
                            .cloned()
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(
                        other(&after_queue.registers),
                        other(&before_actions.registers)
                    );
                    for (request_id, with_parent, lane, expected) in [
                        (1003, false, "main", v1_1::ErrorCode::InvalidParent),
                        (1004, true, "other", v1_1::ErrorCode::Forbidden),
                    ] {
                        peer.signal(if request_id == 1003 {
                            "queue-denials"
                        } else {
                            "queue-wrong-lane"
                        });
                        let deadline = Instant::now() + Duration::from_secs(4);
                        loop {
                            assert!(runtime_r1_r4_support::poll(future.as_mut()).is_pending());
                            setup.pump.tick();
                            if peer.response_frames().len() == (request_id - 999) as usize {
                                break;
                            }
                            assert!(
                                Instant::now() < deadline,
                                "finite queue refusal: {}",
                                peer.stderr_text()
                            );
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        let requests = peer.peer_request_frames();
                        assert_eq!(requests.len(), (request_id - 999) as usize);
                        let request = requests.last().unwrap();
                        assert_eq!(request.request_id, request_id);
                        assert_eq!(request.method, "queue.next_run");
                        assert_eq!(request.capability_ref.as_ref(), Some(selected));
                        assert_eq!(
                            request.parent_request_id,
                            with_parent.then_some(parent.request_id)
                        );
                        assert_eq!(request.effect_key, None);
                        assert_eq!(request.initial_stream_credit_bytes, Some(0));
                        assert!(
                            request
                                .deadline_after_ms
                                .is_some_and(|ms| ms > 0 && ms <= 2000)
                        );
                        let payload =
                            dto::QueueNextRunRequest::decode(request.payload.as_slice()).unwrap();
                        assert_eq!(payload.lane, lane);
                        assert_eq!(
                            payload.item.as_ref().unwrap().projection_id,
                            "host.queue_item.v1"
                        );
                        assert_eq!(
                            wire_value(payload.item.unwrap().value.unwrap()),
                            queued_message
                        );
                        let responses = peer.response_frames();
                        let response = responses.last().unwrap();
                        assert_eq!(response.request_id, request_id);
                        let Some(v1_1::response::Terminal::Error(error)) = &response.terminal
                        else {
                            panic!("typed queue refusal")
                        };
                        assert_eq!(error.code, expected as i32);
                        assert_eq!(error.safe_message, "request failed");
                        assert_eq!(
                            setup.pump.drive(store.snapshot()).unwrap(),
                            after_queue,
                            "queue refusal is a full Session no-write"
                        );
                        assert!(connection.current());
                    }
                } else {
                    assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_actions);
                }
                assert!(connection.current());
                peer.signal("provider");
                drive(setup, future)
            }
        } else {
            drive(
                setup,
                harness.prompt(PromptInput::Text {
                    text: "native profile peer dynamic run".into(),
                    images: vec![],
                }),
            )
        };
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
        if mode == ProcessMode::BeforePayloadMalformed {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            assert!(
                !matches!(call, Ok(Ok(_))),
                "malformed selected terminal cannot settle: {call:?}"
            );
            let wire = peer.request_frames();
            assert_eq!(
                wire.iter().map(|r| r.method.as_str()).collect::<Vec<_>>(),
                [
                    "resource.list",
                    "resource.read",
                    "before_run",
                    "before_request",
                    "transform_context",
                    "before_payload",
                ]
            );
            let request = wire.last().unwrap();
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "before_payload")
                .unwrap()
                .reference();
            assert_eq!(request.capability_ref.as_ref(), Some(selected));
            assert_eq!(request.initial_stream_credit_bytes, Some(0));
            let replies = peer.peer_response_frames();
            let reply = replies
                .iter()
                .find(|r| r.request_id == request.request_id)
                .expect("malformed child terminal");
            let Some(v1_1::response::Terminal::Result(body)) = &reply.terminal else {
                panic!("typed malformed Result")
            };
            let result = dto::InvokeHookResult::decode(body.as_slice()).unwrap();
            assert_eq!(
                wire_value(result.output.unwrap().value.unwrap())["result"]["payload"],
                serde_json::json!({"request":"malformed"})
            );
            assert_eq!(
                connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| p.reference().name == "before_payload")
                    .unwrap()
                    .approved()
                    .validate_payload(
                        minor,
                        "before_payload",
                        pi_extension_protocol::profiles::Stage::Terminal,
                        body,
                        Default::default(),
                        Default::default(),
                        &mut AcceptPayloadNative,
                    ),
                Err(pi_extension_protocol::profiles::Error::Exposure),
                "actual child terminal is rejected by selected host exposure"
            );
            let before = before_malformed.unwrap();
            let after = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(
                after.usage, before.usage,
                "no new Usage after host exposure refusal"
            );
            assert_eq!(
                &after.entries[..before.entries.len()],
                before.entries.as_slice()
            );
            assert!(
                after.entries.len() > before.entries.len(),
                "accepted prompt is durable"
            );
            assert!(after.entries[before.entries.len()..].iter().any(|entry| matches!(entry, Entry::Message(message)
                if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::User(_))))));
            assert!(!after.entries[before.entries.len()..].iter().any(|entry| matches!(entry, Entry::Message(message)
                if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(_))))));
            eprintln!(
                "malformed exposure boundary: call={call:?} occupancy={} current={} new_entries={} new_usage=0",
                connection.occupancy(),
                connection.current(),
                after.entries.len() - before.entries.len()
            );
            connection.close();
            peer.finish(false);
            let close = setup.pump.drive(harness.close());
            eprintln!(
                "malformed close: {close:?}; occupancy={}",
                connection.occupancy()
            );
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
        let provider_index = if payload_mode { 6 } else { 5 };
        assert_eq!(
            wire.iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            if payload_mode {
                vec![
                    "resource.list",
                    "resource.read",
                    "before_run",
                    "before_request",
                    "transform_context",
                    "before_payload",
                    "provider.invoke",
                    "after_response",
                    "before_run_end",
                ]
            } else {
                vec![
                    "resource.list",
                    "resource.read",
                    "before_run",
                    "before_request",
                    "transform_context",
                    "provider.invoke",
                    "after_response",
                    "before_run_end",
                ]
            }
        );
        if payload_mode {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            let hook = &wire[5];
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "before_payload")
                .unwrap()
                .reference();
            assert_eq!(hook.capability_ref.as_ref(), Some(selected));
            assert_eq!(hook.parent_request_id, None);
            assert_eq!(hook.initial_stream_credit_bytes, Some(0));
            assert!(
                hook.deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            let key = hook
                .effect_key
                .as_ref()
                .expect("selected payload EffectKey");
            assert_eq!(key.operation_id, completion.run_id.as_str());
            assert_eq!(key.effect_kind, "assistant");
            assert_eq!(key.lane, "main");
            assert_eq!(key.session_id, "runtime-session");
            let request = dto::InvokeHookRequest::decode(hook.payload.as_slice()).unwrap();
            assert_eq!(request.handler_id, "qualified_handler");
            let event = wire_value(request.input.unwrap().value.unwrap());
            assert_eq!(event["event"]["runId"], completion.run_id.as_str());
            assert_eq!(event["event"]["payload"]["tools"], serde_json::json!([]));
            assert!(
                event["event"]["payload"]["context"]["messages"]
                    .to_string()
                    .contains("native profile peer dynamic run")
            );
            let reply = peer
                .peer_response_frames()
                .into_iter()
                .find(|r| r.request_id == hook.request_id)
                .expect("same child selected payload Result");
            let Some(v1_1::response::Terminal::Result(body)) = reply.terminal else {
                panic!("typed before_payload Result")
            };
            let output = dto::InvokeHookResult::decode(body.as_slice()).unwrap();
            let replacement = wire_value(output.output.unwrap().value.unwrap());
            assert_eq!(
                replacement["result"]["payload"]["context"]["messages"][0]["content"],
                "native payload replacement"
            );
            let provider = dto::InvokeProviderRequest::decode(wire[6].payload.as_slice()).unwrap();
            assert_eq!(
                wire_value(provider.context.unwrap().value.unwrap())["messages"][0]["content"],
                "native payload replacement"
            );
            assert_eq!(
                wire[6].effect_key.as_ref().unwrap().effect_kind,
                "assistant"
            );
            assert_eq!(wire[6].effect_key, hook.effect_key);
            let end = peer
                .peer_stream_end_frames()
                .into_iter()
                .find(|end| end.request_id == wire[6].request_id)
                .expect("same child selected provider StreamEnd");
            let Some(v1_1::stream_end::Terminal::Result(body)) = end.terminal else {
                panic!("typed selected provider Result")
            };
            let result = dto::InvokeProviderResult::decode(body.as_slice()).unwrap();
            assert_eq!(
                wire_value(result.assistant_message.unwrap().value.unwrap())["role"],
                "assistant"
            );
            assert!(result.usage.is_some());
        }
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
                (request.method == "after_response").then_some(wire[provider_index].request_id)
            );
        }
        assert_eq!(
            wire[provider_index]
                .effect_key
                .as_ref()
                .unwrap()
                .operation_id,
            completion.run_id.as_str()
        );
        assert_eq!(setup.faux.requests.lock().unwrap().len(), 1);
        let after = setup.pump.drive(store.snapshot()).unwrap();
        if let Some(item_id) = &queued_item_id {
            assert!(
                after
                    .registers
                    .iter()
                    .any(|r| matches!(r, Register::PendingEntry { key, .. } if key == item_id))
            );
            let lane = after
                .registers
                .iter()
                .find_map(|r| match r {
                    Register::LaneState { key, value, .. } if key == "main" => Some(value),
                    _ => None,
                })
                .unwrap();
            assert!(
                lane.pending_next_run
                    .iter()
                    .any(|id| id.as_str() == item_id)
            );
            assert!(!after.entries.iter().any(|entry| matches!(entry, Entry::Message(message)
                if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::User(user))
                    if matches!(&user.content, pi_ai_core::UserContent::Text(text) if text == "native queued item")))));
            let provider_end = peer
                .peer_stream_end_frames()
                .into_iter()
                .find(|end| end.request_id == wire[provider_index].request_id)
                .expect("original provider terminal");
            let Some(pi_extension_protocol::wire::v1_1::stream_end::Terminal::Result(body)) =
                provider_end.terminal
            else {
                panic!("original provider Result")
            };
            pi_extension_protocol::profiles::wire_v1_1::InvokeProviderResult::decode(
                body.as_slice(),
            )
            .unwrap();
        }
        if let Some(before) = before_payload {
            assert_eq!(
                &after.entries[..before.entries.len()],
                before.entries.as_slice()
            );
            assert_eq!(&after.usage[..before.usage.len()], before.usage.as_slice());
            assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
        }
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
        if structural_mode {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            let target = after
                .entries
                .iter()
                .find_map(|entry| match entry {
                    Entry::Message(message) => Some(message.id.clone()),
                    _ => None,
                })
                .expect("real navigation target");
            let before_wire = peer.requests();
            let compact = drive(setup, harness.compact(CompactOptions::default()))
                .unwrap_or_else(|error| {
                    panic!(
                        "selected compact: {error:?}; wire={:?}; stderr={}",
                        peer.request_frames()
                            .iter()
                            .map(|r| (
                                &r.method,
                                r.request_id,
                                r.parent_request_id,
                                r.effect_key
                                    .as_ref()
                                    .map(|k| (&k.effect_kind, &k.operation_id))
                            ))
                            .collect::<Vec<_>>(),
                        peer.stderr_text()
                    )
                })
                .unwrap();
            let compact_wire = peer.request_frames();
            assert!(compact_wire.len() > before_wire);
            let compact_hook = compact_wire
                .iter()
                .find(|r| r.method == "before_compaction")
                .expect("real selected before_compaction Request");
            let compact_selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "before_compaction")
                .unwrap()
                .reference();
            assert_eq!(compact_hook.capability_ref.as_ref(), Some(compact_selected));
            assert_eq!(compact_hook.parent_request_id, None);
            assert_eq!(compact_hook.initial_stream_credit_bytes, Some(0));
            assert!(
                compact_hook
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            let compact_key = compact_hook
                .effect_key
                .as_ref()
                .expect("structural Hook EffectKey");
            assert_eq!(compact_key.operation_id, compact.run_id.as_str());
            assert_eq!(compact_key.effect_kind, "hook");
            assert_eq!(compact_key.lane, "main");
            assert_eq!(compact_key.session_id, "runtime-session");
            let compact_payload =
                dto::InvokeHookRequest::decode(compact_hook.payload.as_slice()).unwrap();
            assert_eq!(compact_payload.handler_id, "qualified_handler");
            let input = compact_payload.input.unwrap();
            assert_eq!(input.projection_id, "hook.input:before_compaction");
            assert_eq!(
                input
                    .schema_digest_sha256
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                "bcd551ff0bed9117d6269ca978babcfe7f4a0b1c27a7d7398d5e2b8be8b3bd6c"
            );
            let event = wire_value(input.value.unwrap());
            assert_eq!(event["name"], "before_compaction");
            assert_eq!(event["event"]["runId"], compact.run_id.as_str());
            assert_eq!(event["event"]["reason"], "manual");
            assert_eq!(event["event"]["lane"], "main");
            assert_eq!(event["event"]["preparation"]["settings"]["enabled"], false);
            let reply = peer
                .peer_response_frames()
                .into_iter()
                .find(|r| r.request_id == compact_hook.request_id)
                .expect("selected before_compaction Result");
            let Some(v1_1::response::Terminal::Result(body)) = reply.terminal else {
                panic!("typed before_compaction Result")
            };
            let output = dto::InvokeHookResult::decode(body.as_slice())
                .unwrap()
                .output
                .unwrap();
            assert_eq!(output.projection_id, "hook.output:before_compaction");
            assert_eq!(
                output
                    .schema_digest_sha256
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                "a78500c3a2b4801c3be727e378f1047606e2793c714c1f69a8bd605773e51ea9"
            );
            let compact_output_value = wire_value(output.value.unwrap());
            if mode == ProcessMode::StructuralCompactionDecline {
                assert_eq!(
                    compact_output_value,
                    serde_json::json!({"name":"before_compaction","result":{"decline":true}})
                );
                assert!(matches!(compact.outcome, CompactionOutcome::Declined(_)));
                let declined = setup.pump.drive(store.snapshot()).unwrap();
                assert_eq!(declined.entries, after.entries);
                assert_eq!(declined.usage, after.usage);
                assert_ne!(
                    declined.registers, after.registers,
                    "accepted structural lifecycle is durable"
                );
                let leaf = |registers: &[Register]| {
                    registers.iter().find_map(|r| match r {
                        Register::LaneLeaf { key, value, .. } if key == "main" => {
                            Some(value.clone())
                        }
                        _ => None,
                    })
                };
                assert_eq!(leaf(&declined.registers), leaf(&after.registers));
                assert!(declined.registers.iter().any(|r| matches!(
                    r,
                    Register::LaneLastResult {
                        value: LaneLastResult::CompactionDeclined(_),
                        ..
                    }
                )));
                assert!(declined.registers.iter().all(|r| !matches!(
                    r,
                    Register::OperationMeta { .. }
                        | Register::OperationState { .. }
                        | Register::OperationPreparation { .. }
                )));
                let unrelated = |registers: &[Register]| {
                    registers
                        .iter()
                        .filter(|r| {
                            !matches!(
                                r,
                                Register::LaneState { .. } | Register::LaneLastResult { .. }
                            )
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                };
                assert_eq!(unrelated(&declined.registers), unrelated(&after.registers));
                assert_eq!(
                    compact_wire
                        .iter()
                        .filter(|r| r.method == "before_compaction")
                        .count(),
                    1
                );
                assert!(!compact_wire.iter().any(|r| {
                    r.effect_key
                        .as_ref()
                        .is_some_and(|k| k.effect_kind == "summary")
                }));
                assert_eq!(connection.occupancy(), 0);
                peer.finish(true);
                connection.close();
                setup.pump.drive(harness.close()).unwrap();
                return;
            }
            let CompactionOutcome::Completed(completed) = &compact.outcome else {
                panic!(
                    "selected compaction did not complete: {:?}; stderr={}",
                    compact.outcome,
                    peer.stderr_text()
                );
            };
            assert_eq!(
                compact_output_value,
                serde_json::json!({"name":"before_compaction","result":{}})
            );
            let compact_snapshot = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(compact_snapshot.entries.len(), after.entries.len() + 1);
            assert_eq!(compact_snapshot.usage.len(), after.usage.len() + 2);
            assert_eq!(
                &compact_snapshot.entries[..after.entries.len()],
                after.entries.as_slice()
            );
            assert_eq!(
                &compact_snapshot.usage[..after.usage.len()],
                after.usage.as_slice()
            );
            assert_eq!(
                compact_snapshot.entries.last(),
                Some(&Entry::Compaction(completed.entry.clone()))
            );
            assert!(!completed.entry.from_hook);
            assert_eq!(
                completed.entry.parent_id,
                after.entries.last().and_then(|entry| match entry {
                    Entry::Message(m) => Some(m.id.clone()),
                    _ => None,
                })
            );
            assert!(
                compact_snapshot.usage[after.usage.len()..]
                    .iter()
                    .all(|u| u.entry_id.is_none() && !u.adjustment)
            );
            assert!(compact_snapshot.registers.iter().any(|r| matches!(r,
                Register::LaneLeaf { key, value, .. } if key == "main" && value.as_ref() == Some(&completed.entry.id))));
            let repeat_before = setup.pump.drive(store.snapshot()).unwrap();
            let repeat_wire = peer.requests();
            assert!(matches!(
                drive(setup, harness.compact(CompactOptions::default())),
                Ok(Err(CompactionExpectedError::NothingToCompact(_)))
            ));
            assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), repeat_before);
            assert_eq!(peer.requests(), repeat_wire);
            let navigation = drive(
                setup,
                harness.navigate_tree(
                    Some(target.clone()),
                    NavigateOptions {
                        summarize: Some(true),
                        ..Default::default()
                    },
                ),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "selected navigation: {error:?}; stderr={}",
                    peer.stderr_text()
                )
            })
            .unwrap();
            let nav_wire = peer.request_frames();
            let nav_hook = nav_wire
                .iter()
                .find(|r| r.method == "before_navigation")
                .expect("real selected before_navigation Request");
            let nav_selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "before_navigation")
                .unwrap()
                .reference();
            assert_eq!(nav_hook.capability_ref.as_ref(), Some(nav_selected));
            assert_eq!(nav_hook.parent_request_id, None);
            assert_eq!(nav_hook.initial_stream_credit_bytes, Some(0));
            assert!(
                nav_hook
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            let nav_key = nav_hook
                .effect_key
                .as_ref()
                .expect("navigation Hook EffectKey");
            assert_eq!(nav_key.effect_kind, "hook");
            assert_eq!(nav_key.operation_id, navigation.run_id.as_str());
            assert_eq!(nav_key.lane, "main");
            assert_eq!(nav_key.session_id, "runtime-session");
            let nav_payload = dto::InvokeHookRequest::decode(nav_hook.payload.as_slice()).unwrap();
            assert_eq!(nav_payload.handler_id, "qualified_handler");
            let input = nav_payload.input.unwrap();
            assert_eq!(input.projection_id, "hook.input:before_navigation");
            assert_eq!(
                input
                    .schema_digest_sha256
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                "9e27ffecbb33bde8f6c6be8ff44696929150ad9e46df81d0addc3acf3a83f563"
            );
            let event = wire_value(input.value.unwrap());
            assert_eq!(event["name"], "before_navigation");
            assert_eq!(event["event"]["targetId"], target.as_str());
            assert_eq!(event["event"]["runId"], navigation.run_id.as_str());
            assert_eq!(event["event"]["lane"], "main");
            assert!(
                event["event"]["preparation"]["messages"]
                    .as_array()
                    .is_some_and(|m| !m.is_empty())
            );
            let reply = peer
                .peer_response_frames()
                .into_iter()
                .find(|r| r.request_id == nav_hook.request_id)
                .expect("selected before_navigation Result");
            let Some(v1_1::response::Terminal::Result(body)) = reply.terminal else {
                panic!("typed before_navigation Result")
            };
            let output = dto::InvokeHookResult::decode(body.as_slice())
                .unwrap()
                .output
                .unwrap();
            assert_eq!(output.projection_id, "hook.output:before_navigation");
            assert_eq!(
                output
                    .schema_digest_sha256
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                "17508eba0470ed4018aa682417ba09e2998e62fdc3b4b534a95e88e9a73bd848"
            );
            assert_eq!(
                wire_value(output.value.unwrap()),
                if mode == ProcessMode::StructuralNavigationDecline {
                    serde_json::json!({"name":"before_navigation","result":{"decline":true}})
                } else {
                    serde_json::json!({"name":"before_navigation","result":{}})
                }
            );
            if mode == ProcessMode::StructuralNavigationDecline {
                assert!(matches!(navigation.outcome, NavigationOutcome::Declined(_)));
                let declined = setup.pump.drive(store.snapshot()).unwrap();
                assert_eq!(declined.entries, compact_snapshot.entries);
                assert_eq!(declined.usage, compact_snapshot.usage);
                assert_ne!(declined.registers, compact_snapshot.registers);
                let leaf = |registers: &[Register]| {
                    registers.iter().find_map(|r| match r {
                        Register::LaneLeaf { key, value, .. } if key == "main" => {
                            Some(value.clone())
                        }
                        _ => None,
                    })
                };
                assert_eq!(leaf(&declined.registers), leaf(&compact_snapshot.registers));
                assert!(declined.registers.iter().any(|r| matches!(
                    r,
                    Register::LaneLastResult {
                        value: LaneLastResult::NavigationDeclined(_),
                        ..
                    }
                )));
                assert!(declined.registers.iter().all(|r| !matches!(
                    r,
                    Register::OperationMeta { .. }
                        | Register::OperationState { .. }
                        | Register::OperationPreparation { .. }
                )));
                let unrelated = |registers: &[Register]| {
                    registers
                        .iter()
                        .filter(|r| {
                            !matches!(
                                r,
                                Register::LaneState { .. } | Register::LaneLastResult { .. }
                            )
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                };
                assert_eq!(
                    unrelated(&declined.registers),
                    unrelated(&compact_snapshot.registers)
                );
                assert_eq!(
                    nav_wire
                        .iter()
                        .filter(|r| r.method == "before_navigation")
                        .count(),
                    1
                );
                assert_eq!(
                    nav_wire
                        .iter()
                        .filter(|r| r
                            .effect_key
                            .as_ref()
                            .is_some_and(|k| k.effect_kind == "summary")
                            && r.method == "provider.invoke")
                        .count(),
                    2
                );
                assert_eq!(connection.occupancy(), 0);
                peer.finish(true);
                connection.close();
                setup.pump.drive(harness.close()).unwrap();
                return;
            }
            let NavigationOutcome::Completed(navigated) = &navigation.outcome else {
                panic!(
                    "selected navigation did not complete: {:?}; stderr={}",
                    navigation.outcome,
                    peer.stderr_text()
                );
            };
            let branch = navigated
                .summary_entry
                .as_ref()
                .expect("real branch summary");
            let navigation_snapshot = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(
                navigation_snapshot.entries.len(),
                compact_snapshot.entries.len() + 1
            );
            assert_eq!(
                navigation_snapshot.usage.len(),
                compact_snapshot.usage.len() + 1
            );
            assert_eq!(
                &navigation_snapshot.entries[..compact_snapshot.entries.len()],
                compact_snapshot.entries.as_slice()
            );
            assert_eq!(
                &navigation_snapshot.usage[..compact_snapshot.usage.len()],
                compact_snapshot.usage.as_slice()
            );
            assert_eq!(
                navigation_snapshot.entries.last(),
                Some(&Entry::BranchSummary(branch.clone()))
            );
            assert_eq!(branch.parent_id, Some(target.clone()));
            assert_eq!(branch.from_id, completed.entry.id);
            assert!(!branch.from_hook);
            assert!(navigation_snapshot.usage.last().unwrap().entry_id.is_none());
            assert!(navigation_snapshot.registers.iter().any(|r| matches!(r,
                Register::LaneLeaf { key, value, .. } if key == "main" && value.as_ref() == Some(&branch.id))));
            let wire = peer.request_frames();
            let summaries = wire
                .iter()
                .filter(|r| {
                    r.method == "provider.invoke"
                        && r.effect_key
                            .as_ref()
                            .is_some_and(|k| k.effect_kind == "summary")
                })
                .collect::<Vec<_>>();
            assert_eq!(summaries.len(), 3);
            assert_eq!(
                summaries
                    .iter()
                    .filter(
                        |r| r.effect_key.as_ref().unwrap().operation_id == compact.run_id.as_str()
                    )
                    .count(),
                2
            );
            assert_eq!(
                summaries
                    .iter()
                    .filter(|r| r.effect_key.as_ref().unwrap().operation_id
                        == navigation.run_id.as_str())
                    .count(),
                1
            );
            let provider_selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "remote")
                .unwrap()
                .reference();
            let after_selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "after_response")
                .unwrap()
                .reference();
            for request in summaries {
                let key = request.effect_key.as_ref().unwrap();
                assert!(
                    key.operation_id == compact.run_id.as_str()
                        || key.operation_id == navigation.run_id.as_str()
                );
                assert_eq!(key.lane, "main");
                assert_eq!(key.session_id, "runtime-session");
                assert_eq!(request.capability_ref.as_ref(), Some(provider_selected));
                assert_eq!(request.parent_request_id, None);
                assert_eq!(request.initial_stream_credit_bytes, Some(0));
                assert!(
                    request
                        .deadline_after_ms
                        .is_some_and(|ms| ms > 0 && ms <= 5000)
                );
                let invoked =
                    dto::InvokeProviderRequest::decode(request.payload.as_slice()).unwrap();
                assert_eq!(invoked.model_id, "m");
                assert_eq!(invoked.auth_mode, "none");
                let tools = invoked.tools.unwrap();
                assert_eq!(tools.projection_id, "provider.tools.v1");
                assert_eq!(wire_value(tools.value.unwrap()), serde_json::json!([]));
                let response = wire
                    .iter()
                    .find(|r| {
                        r.method == "after_response"
                            && r.parent_request_id == Some(request.request_id)
                    })
                    .expect("selected summary after_response");
                assert_eq!(response.effect_key, request.effect_key);
                assert_eq!(response.capability_ref.as_ref(), Some(after_selected));
                assert_eq!(response.initial_stream_credit_bytes, Some(0));
                assert!(
                    response
                        .deadline_after_ms
                        .is_some_and(|ms| ms > 0 && ms <= 5000)
                );
                let hook = dto::InvokeHookRequest::decode(response.payload.as_slice()).unwrap();
                assert_eq!(hook.handler_id, "qualified_handler");
                assert_eq!(
                    wire_value(hook.input.unwrap().value.unwrap())["event"]["message"]["role"],
                    "assistant"
                );
                let end = peer
                    .peer_stream_end_frames()
                    .into_iter()
                    .find(|end| end.request_id == request.request_id)
                    .expect("selected summary provider terminal");
                let Some(v1_1::stream_end::Terminal::Result(body)) = end.terminal else {
                    panic!("typed summary Result")
                };
                let result = dto::InvokeProviderResult::decode(body.as_slice()).unwrap();
                let usage = result.usage.unwrap();
                assert_eq!(usage.projection_id, "provider.usage.v1");
                let assistant = result.assistant_message.unwrap();
                assert_eq!(assistant.projection_id, "provider.assistant_message.v1");
                assert_eq!(wire_value(assistant.value.unwrap())["role"], "assistant");
            }
            let before_unknown = setup.pump.drive(store.snapshot()).unwrap();
            let unknown_requests = peer.requests();
            assert!(matches!(
                drive(
                    setup,
                    harness.navigate_tree(
                        Some(EntryId::new("missing-native-target").unwrap()),
                        NavigateOptions {
                            summarize: Some(true),
                            ..Default::default()
                        }
                    )
                ),
                Ok(Err(NavigationExpectedError::UnknownTarget(_)))
            ));
            assert_eq!(setup.pump.drive(store.snapshot()).unwrap(), before_unknown);
            assert_eq!(peer.requests(), unknown_requests);
            assert_eq!(setup.faux.requests.lock().unwrap().len(), 1);
            assert!(connection.current());
            assert_eq!(connection.occupancy(), 0);
        }
        if matches!(
            mode,
            ProcessMode::Changed
                | ProcessMode::ChangedWrongScope
                | ProcessMode::Telemetry
                | ProcessMode::TelemetryBadAck
                | ProcessMode::TelemetryBadFlush
                | ProcessMode::BeforeResume
                | ProcessMode::HostSnapshot
                | ProcessMode::HostQueue
                | ProcessMode::EventSink
                | ProcessMode::EventSinkWrongId
        ) {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            let baseline = setup.pump.drive(store.snapshot()).unwrap();
            assert!(!catalog.refresh_pending());
            let refreshed = drive(
                setup,
                harness.refresh_profile_resources(connection.clone(), cap.clone(), &catalog),
            );
            let peer_requests = peer.peer_request_frames();
            assert_eq!(
                peer_requests.len(),
                if mode == ProcessMode::HostQueue {
                    6
                } else if mode == ProcessMode::HostSnapshot || event_mode {
                    3
                } else {
                    1
                }
            );
            let changed = peer_requests.last().unwrap();
            assert_eq!(changed.method, "resource.changed");
            assert_eq!(changed.request_id, 2000);
            assert_eq!(changed.capability_ref.as_ref(), Some(&cap));
            assert_eq!(changed.parent_request_id, None);
            assert_eq!(changed.effect_key, None);
            assert_eq!(changed.initial_stream_credit_bytes, Some(0));
            let payload = dto::ResourceChanged::decode(changed.payload.as_slice()).unwrap();
            assert_eq!(
                payload.scope,
                if mode == ProcessMode::ChangedWrongScope {
                    "wrong-scope"
                } else {
                    "allowed"
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
            assert_eq!(
                responses.len(),
                if mode == ProcessMode::HostQueue {
                    6
                } else if mode == ProcessMode::HostSnapshot || event_mode {
                    3
                } else {
                    1
                }
            );
            let changed_response = responses.last().unwrap();
            assert_eq!(changed_response.request_id, 2000);
            let Some(v1_1::response::Terminal::Result(body)) = &changed_response.terminal else {
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
            if let Some(sink) = &telemetry {
                use pi_telemetry::TelemetryContext;
                let metrics = connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| p.reference().name == "metrics")
                    .unwrap()
                    .reference()
                    .clone();
                let before_telemetry = setup.pump.drive(store.snapshot()).unwrap();
                let mut attributes = pi_telemetry::SpanAttributes::new();
                attributes.insert(
                    "pi.operation.id".into(),
                    pi_telemetry::AttributeValue::String("SENTINEL-telemetry-local".into()),
                );
                sink.start_span(pi_telemetry::SpanOptions {
                    name: "pi.harness.run".into(),
                    attributes,
                })
                .finish();
                let mut delivered = 0;
                loop {
                    let before_requests = peer.requests();
                    let ok = drive(
                        setup,
                        harness.deliver_profile_telemetry(
                            connection.clone(),
                            metrics.clone(),
                            sink,
                        ),
                    );
                    if mode == ProcessMode::TelemetryBadAck {
                        assert!(!ok, "wrong telemetry count must disable sink");
                        assert_eq!(peer.requests(), before_requests + 1);
                        break;
                    }
                    assert!(ok, "selected telemetry batch acknowledgement");
                    if peer.requests() == before_requests {
                        break;
                    }
                    delivered += 1;
                    assert!(delivered <= 256, "finite telemetry batch history");
                }
                let wire = peer.request_frames();
                let batches = wire[10..]
                    .iter()
                    .filter(|r| r.method == "telemetry.emit_batch")
                    .collect::<Vec<_>>();
                assert!(!batches.is_empty(), "actual native telemetry batch");
                assert_eq!(
                    batches.len(),
                    if mode == ProcessMode::TelemetryBadAck {
                        1
                    } else {
                        delivered
                    }
                );
                let child_replies = peer.peer_response_frames();
                let mut request_ids = wire[..10]
                    .iter()
                    .map(|r| r.request_id)
                    .collect::<std::collections::BTreeSet<_>>();
                let mut through = None;
                for (index, request) in batches.iter().enumerate() {
                    assert!(request_ids.insert(request.request_id));
                    assert_eq!(request.capability_ref.as_ref(), Some(&metrics));
                    assert_eq!(request.parent_request_id, None);
                    assert_eq!(request.effect_key, None);
                    assert_eq!(request.initial_stream_credit_bytes, Some(0));
                    assert!(
                        request
                            .deadline_after_ms
                            .is_some_and(|ms| ms > 0 && ms <= 5000)
                    );
                    let batch =
                        dto::TelemetryEmitBatchRequest::decode(request.payload.as_slice()).unwrap();
                    assert_eq!(batch.schema_version, 1);
                    assert_eq!(batch.sequence, index as u64);
                    assert!(!batch.records.is_empty());
                    for record in &batch.records {
                        assert_eq!(record.projection_id, "telemetry.record.v1");
                        assert_eq!(
                            record
                                .schema_digest_sha256
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>(),
                            "f6bc4911e2cdcbaaa0e84b3d62f2b4aa8278afcb448c56a6971aec61a55bc91b"
                        );
                        let value = wire_value(record.value.clone().unwrap());
                        assert!(!value.to_string().contains("SENTINEL"));
                    }
                    let replies = child_replies
                        .iter()
                        .filter(|r| r.request_id == request.request_id)
                        .collect::<Vec<_>>();
                    assert_eq!(replies.len(), 1);
                    let Some(v1_1::response::Terminal::Result(body)) = &replies[0].terminal else {
                        panic!("typed telemetry batch Result")
                    };
                    let ack = dto::TelemetryEmitBatchResult::decode(body.as_slice()).unwrap();
                    assert_eq!(
                        ack.accepted,
                        batch.records.len() as u64
                            + u64::from(mode == ProcessMode::TelemetryBadAck)
                    );
                    assert_eq!(ack.dropped, 0);
                    through = Some(batch.sequence);
                }
                if mode == ProcessMode::TelemetryBadAck {
                    assert!(sink.disabled());
                    assert_eq!(wire.len(), 11, "bad ack cannot reach flush");
                } else {
                    let through = through.unwrap();
                    let flushed = drive(
                        setup,
                        harness.flush_profile_telemetry(
                            connection.clone(),
                            metrics.clone(),
                            sink,
                            through,
                        ),
                    );
                    assert_eq!(flushed, mode != ProcessMode::TelemetryBadFlush);
                    let wire = peer.request_frames();
                    assert_eq!(wire.len(), 10 + batches.len() + 1);
                    let flush = wire.last().unwrap();
                    assert_eq!(flush.method, "telemetry.flush");
                    assert!(request_ids.insert(flush.request_id));
                    assert_eq!(flush.capability_ref.as_ref(), Some(&metrics));
                    assert_eq!(flush.parent_request_id, None);
                    assert_eq!(flush.effect_key, None);
                    assert_eq!(flush.initial_stream_credit_bytes, Some(0));
                    assert!(
                        flush
                            .deadline_after_ms
                            .is_some_and(|ms| ms > 0 && ms <= 5000)
                    );
                    let payload =
                        dto::TelemetryFlushRequest::decode(flush.payload.as_slice()).unwrap();
                    assert_eq!(payload.through_sequence, through);
                    let replies = peer.peer_response_frames();
                    let replies = replies
                        .iter()
                        .filter(|r| r.request_id == flush.request_id)
                        .collect::<Vec<_>>();
                    assert_eq!(replies.len(), 1);
                    let Some(v1_1::response::Terminal::Result(body)) = &replies[0].terminal else {
                        panic!("typed telemetry flush Result")
                    };
                    let ack = dto::TelemetryFlushResult::decode(body.as_slice()).unwrap();
                    assert_eq!(
                        ack.flushed_through,
                        through + u64::from(mode == ProcessMode::TelemetryBadFlush)
                    );
                    assert_eq!(sink.disabled(), mode == ProcessMode::TelemetryBadFlush);
                    if matches!(
                        mode,
                        ProcessMode::Telemetry
                            | ProcessMode::BeforeResume
                            | ProcessMode::HostSnapshot
                            | ProcessMode::HostQueue
                            | ProcessMode::EventSink
                            | ProcessMode::EventSinkWrongId
                    ) {
                        assert_eq!(sink.dropped(), 0);
                    }
                }
                assert!(connection.current());
                assert_eq!(connection.occupancy(), 0);
                assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
                assert!(catalog.refresh_pending());
                assert_eq!(
                    setup.pump.drive(store.snapshot()).unwrap(),
                    before_telemetry
                );
                if event_mode {
                    assert!(
                        peer.peer_request_frames()
                            .iter()
                            .all(|request| request.method != "event.subscribe"),
                        "event subscribe waits until the shared prefix is settled"
                    );
                    peer.signal("event-subscribe");
                }
            }
        }
        if event_mode {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            use std::time::{Duration, Instant};
            let event = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "events")
                .unwrap()
                .reference()
                .clone();
            assert_eq!(event.profile.as_ref().unwrap().minor, minor);
            assert_eq!(event.schema_digest_sha256.len(), 32);
            let deadline = Instant::now() + Duration::from_secs(4);
            let subscribe = loop {
                setup.pump.tick();
                if let Some(request) = peer
                    .peer_request_frames()
                    .into_iter()
                    .find(|r| r.method == "event.subscribe")
                {
                    break request;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite peer subscribe: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            assert_eq!(subscribe.request_id, 3000);
            assert_eq!(subscribe.capability_ref.as_ref(), Some(&event));
            assert_eq!(subscribe.parent_request_id, None);
            assert_eq!(subscribe.effect_key, None);
            assert_eq!(subscribe.initial_stream_credit_bytes, Some(0));
            assert!(
                subscribe
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            assert_eq!(
                dto::SubscribeRequest::decode(subscribe.payload.as_slice())
                    .unwrap()
                    .scope,
                "captured_scope"
            );
            for _ in 0..30 {
                setup.pump.tick();
            }
            assert!(peer.peer_credit_frames().is_empty());
            assert!(
                peer.stream_item_frames().is_empty(),
                "zero credit gates actual Snapshot"
            );
            peer.signal("event-credit");
            let deadline = Instant::now() + Duration::from_secs(4);
            let initial = loop {
                setup.pump.tick();
                let captured = peer.stream_item_frames();
                if peer.stage("event-snapshot").is_some() && !captured.is_empty() {
                    break captured;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite native Snapshot capture: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            let credit = peer.peer_credit_frames();
            assert_eq!(credit.len(), 1);
            assert_eq!(credit[0].request_id, subscribe.request_id);
            assert_eq!(credit[0].additional_bytes, 1048576);
            assert_eq!(initial.len(), 1);
            assert_eq!(initial[0].request_id, subscribe.request_id);
            assert_eq!(initial[0].sequence, 0);
            let dto::event_sink_item::Item::Snapshot(snapshot) =
                dto::EventSinkItem::decode(initial[0].payload.as_slice())
                    .unwrap()
                    .item
                    .unwrap()
            else {
                panic!("typed native initial Snapshot")
            };
            assert!(!snapshot.subscription_id.is_empty());
            assert_eq!(snapshot.next_event_sequence, 0);
            assert_eq!(
                peer.stage("event-snapshot").unwrap()["subscription_id"],
                snapshot.subscription_id
            );
            let projection = snapshot.filtered_snapshot.as_ref().unwrap();
            assert_eq!(projection.projection_id, "event.filtered_snapshot.v1");
            assert_eq!(
                wire_value(projection.value.clone().unwrap())["lane"],
                "main"
            );
            assert_eq!(
                connection
                    .grant()
                    .wire()
                    .limits
                    .unwrap()
                    .max_stream_credit_bytes,
                1048576
            );
            let watch = drive(setup, harness.watch()).unwrap();
            let listener = Arc::new(EventWatch(std::sync::Mutex::new(vec![])));
            watch.start(listener.clone());
            let message = |text: &str| {
                AgentMessage::BuiltIn(pi_ai_core::Message::User(pi_ai_core::UserMessage {
                    role: Default::default(),
                    content: pi_ai_core::UserContent::Text(text.into()),
                    timestamp: n(7),
                }))
            };
            let first = drive(setup, harness.next_run(message("actual bounded first")))
                .unwrap()
                .unwrap();
            let second = drive(setup, harness.next_run(message("second while paused")))
                .unwrap()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(4);
            let items = loop {
                setup.pump.tick();
                let captured = peer.stream_item_frames();
                if peer.stage("event-gap").is_some()
                    && captured.len() >= 2
                    && listener
                        .0
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|e| matches!(e, HarnessEvent::QueueUpdate(_)))
                        .count()
                        >= 2
                {
                    break captured;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite native Gap/watch capture: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            assert_eq!(items.len(), 2, "paused remote stream has no ordinary event");
            assert_eq!(items[1].request_id, subscribe.request_id);
            assert_eq!(items[1].sequence, 1);
            let dto::event_sink_item::Item::Gap(gap) =
                dto::EventSinkItem::decode(items[1].payload.as_slice())
                    .unwrap()
                    .item
                    .unwrap()
            else {
                panic!("reserved native Gap")
            };
            assert_eq!(gap.subscription_id, snapshot.subscription_id);
            assert_eq!((gap.first_dropped, gap.last_dropped), (0, 0));
            assert_eq!(
                watch.snapshot().queues.next_run.len(),
                0,
                "trusted initial watch snapshot remains captured"
            );
            assert_eq!(
                listener
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| matches!(e, HarnessEvent::QueueUpdate(_)))
                    .count(),
                2,
                "local watch retains both native queue publications"
            );
            let after_publication = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(after_publication.usage.len(), 2);
            peer.signal("event-resync");
            let deadline = Instant::now() + Duration::from_secs(4);
            let response = loop {
                setup.pump.tick();
                let captured = peer
                    .response_frames()
                    .into_iter()
                    .find(|response| response.request_id == 3001);
                if peer.stage("event-resync-result").is_some()
                    && let Some(response) = captured
                {
                    break response;
                }
                assert!(
                    Instant::now() < deadline,
                    "finite native resync Response capture: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            };
            let resync = peer
                .peer_request_frames()
                .into_iter()
                .find(|r| r.method == "event.resync")
                .unwrap();
            assert_eq!(resync.request_id, 3001);
            assert_eq!(resync.capability_ref.as_ref(), Some(&event));
            assert_eq!(resync.parent_request_id, None);
            assert_eq!(resync.effect_key, None);
            assert_eq!(resync.initial_stream_credit_bytes, Some(0));
            assert!(
                resync
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            let requested = dto::ResyncRequest::decode(resync.payload.as_slice()).unwrap();
            assert_eq!(response.request_id, resync.request_id);
            if mode == ProcessMode::EventSinkWrongId {
                assert_eq!(
                    requested.subscription_id,
                    format!("{}-wrong", snapshot.subscription_id)
                );
                let Some(v1_1::response::Terminal::Error(error)) = response.terminal else {
                    panic!("typed wrong subscription refusal")
                };
                assert_eq!(error.code, v1_1::ErrorCode::InvalidParent as i32);
                assert_eq!(error.safe_message, "request failed");
                assert_eq!(
                    peer.stream_item_frames().len(),
                    2,
                    "wrong ID leaves actual subscription paused without baseline"
                );
            } else {
                assert_eq!(requested.subscription_id, snapshot.subscription_id);
                let Some(v1_1::response::Terminal::Result(body)) = response.terminal else {
                    panic!("typed native resync Result")
                };
                let baseline = dto::ResyncResult::decode(body.as_slice())
                    .unwrap()
                    .snapshot
                    .unwrap();
                assert_eq!(baseline.subscription_id, snapshot.subscription_id);
                assert!(baseline.snapshot_revision >= snapshot.snapshot_revision);
                assert_eq!(
                    baseline.next_event_sequence, 2,
                    "both real next_run publications advance the paused cursor"
                );
                let filtered = baseline.filtered_snapshot.unwrap();
                assert_eq!(filtered.projection_id, "event.filtered_snapshot.v1");
                let value = wire_value(filtered.value.unwrap());
                assert_eq!(
                    value["queues"]["nextRun"][0]["entryId"],
                    first.entry_id.as_str()
                );
                assert_eq!(
                    value["queues"]["nextRun"][1]["entryId"],
                    second.entry_id.as_str()
                );
            }
            let deadline = Instant::now() + Duration::from_secs(4);
            while connection.occupancy() != 1 {
                setup.pump.tick();
                assert!(
                    Instant::now() < deadline,
                    "finite resync Request retirement: {}",
                    peer.stderr_text()
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                connection.occupancy(),
                1,
                "live passive subscription remains owned"
            );
            assert_eq!(
                setup.pump.drive(store.snapshot()).unwrap(),
                after_publication,
                "resync and wrong-ID refusal make no Session write"
            );
            assert_eq!(peer.stream_item_frames().len(), 2);
            assert!(connection.current());
            watch.unsubscribe();
        }
        if mode == ProcessMode::BeforeResume {
            use pi_extension_protocol::profiles::wire_v1_1 as dto;
            use pi_extension_protocol::wire::v1_1;
            let selected = connection
                .grant()
                .profiles()
                .iter()
                .find(|p| p.reference().name == "before_resume")
                .unwrap()
                .reference()
                .clone();
            harness
                .register_profile_hook(
                    RemoteHook::new(connection.clone(), selected.clone(), Default::default())
                        .unwrap(),
                )
                .unwrap();
            let before_deferred = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(before_deferred.usage.len(), 2);
            let suspended = drive(
                setup,
                harness.prompt(PromptInput::Text {
                    text: "native profile peer dynamic run deferred source".into(),
                    images: vec![],
                }),
            )
            .unwrap_or_else(|error| panic!("deferred prompt: {error}; {}", peer.stderr_text()))
            .unwrap();
            assert!(matches!(suspended.outcome, RunOutcome::Deferred(_)));
            let suspended_snapshot = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(suspended_snapshot.usage.len(), 3);
            assert_eq!(
                &suspended_snapshot.usage[..2],
                before_deferred.usage.as_slice()
            );
            assert_eq!(
                &suspended_snapshot.entries[..before_deferred.entries.len()],
                before_deferred.entries.as_slice()
            );
            let source = suspended_snapshot
                .entries
                .iter()
                .find_map(|entry| match entry {
                    Entry::Message(entry) => match &entry.message {
                        AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message))
                            if message.deferred.is_some() =>
                        {
                            Some((entry.id.clone(), message))
                        }
                        _ => None,
                    },
                    _ => None,
                })
                .expect("actual deferred assistant source");
            assert_eq!(
                suspended_snapshot.usage[2].entry_id.as_ref(),
                Some(&source.0)
            );
            let resumed = drive(setup, harness.resume())
                .unwrap_or_else(|error| panic!("resume: {error}; {}", peer.stderr_text()))
                .unwrap();
            assert!(matches!(
                resumed,
                ResumeOutcome::RunCompletedWithAssistant(_)
            ));
            let after = setup.pump.drive(store.snapshot()).unwrap();
            assert_eq!(after.usage.len(), 4);
            assert_eq!(&after.usage[..3], suspended_snapshot.usage.as_slice());
            assert_eq!(
                &after.entries[..before_deferred.entries.len()],
                before_deferred.entries.as_slice()
            );
            let final_id = after.usage[3]
                .entry_id
                .as_ref()
                .expect("resumed Usage entry");
            assert!(after.entries.iter().any(|entry| matches!(entry, Entry::Message(message)
                if &message.id == final_id && matches!(&message.message,
                    AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(a)) if a.deferred.is_none()))));
            assert_eq!(revision.get("s", Some("1")).unwrap().value, "x");
            assert!(catalog.refresh_pending());
            let wire = peer.request_frames();
            let flushed = wire
                .iter()
                .position(|r| r.method == "telemetry.flush")
                .unwrap();
            let tail = &wire[flushed + 1..];
            assert_eq!(
                tail.iter().map(|r| r.method.as_str()).collect::<Vec<_>>(),
                [
                    "before_run",
                    "before_request",
                    "transform_context",
                    "provider.invoke",
                    "after_response",
                    "before_resume",
                    "before_request",
                    "provider.invoke",
                    "after_response",
                    "before_run_end",
                ]
            );
            let before_resume = wire
                .iter()
                .position(|r| r.method == "before_resume")
                .expect("real selected before_resume Request");
            let resume_request = &wire[before_resume];
            assert_eq!(resume_request.capability_ref.as_ref(), Some(&selected));
            assert_eq!(resume_request.parent_request_id, None);
            assert_eq!(resume_request.effect_key, None);
            assert_eq!(resume_request.initial_stream_credit_bytes, Some(0));
            assert!(
                resume_request
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            let hook = dto::InvokeHookRequest::decode(resume_request.payload.as_slice()).unwrap();
            assert_eq!(hook.handler_id, "qualified_handler");
            let event = wire_value(hook.input.unwrap().value.unwrap());
            assert_eq!(event["event"]["kind"], "run");
            assert_eq!(event["event"]["runId"], suspended.run_id.as_str());
            assert_eq!(
                event["event"]["resumeData"],
                serde_json::json!({"marker":"peer-before-resume"})
            );
            assert!(
                event["event"]["prompt"]
                    .to_string()
                    .contains("native profile peer dynamic run deferred source")
            );
            let fetch = wire
                .iter()
                .skip(before_resume + 1)
                .find(|r| {
                    r.method == "provider.invoke"
                        && r.effect_key
                            .as_ref()
                            .is_some_and(|key| key.effect_kind == "deferred")
                })
                .expect("real keyed deferred fetch");
            assert_eq!(
                fetch.effect_key.as_ref().unwrap().operation_id,
                suspended.run_id.as_str()
            );
            assert_eq!(
                tail[3].effect_key.as_ref().unwrap().effect_kind,
                "assistant"
            );
            assert_eq!(
                tail[3].effect_key.as_ref().unwrap().operation_id,
                suspended.run_id.as_str()
            );
            assert_eq!(tail[4].parent_request_id, Some(tail[3].request_id));
            assert_eq!(tail[8].parent_request_id, Some(fetch.request_id));
            let roles = [
                None,
                Some("hook"),
                Some("assistant"),
                Some("assistant"),
                Some("assistant"),
                None,
                Some("hook"),
                Some("deferred"),
                Some("deferred"),
                Some("hook"),
            ];
            for (index, (request, expected_role)) in tail.iter().zip(roles).enumerate() {
                assert_eq!(
                    request
                        .effect_key
                        .as_ref()
                        .map(|key| key.effect_kind.as_str()),
                    expected_role
                );
                if let Some(key) = &request.effect_key {
                    assert_eq!(key.operation_id, suspended.run_id.as_str());
                    assert_eq!(key.lane, "main");
                    assert_eq!(key.session_id, "runtime-session");
                    assert_eq!(key.logical_attempt, u64::from(index != 9));
                    assert_eq!(key.item_index, None);
                }
                if index != 4 && index != 8 {
                    assert_eq!(request.parent_request_id, None);
                }
            }
            assert_eq!(tail[2].effect_key, tail[3].effect_key);
            assert_eq!(tail[4].effect_key, tail[3].effect_key);
            assert_eq!(tail[8].effect_key, fetch.effect_key);
            assert_eq!(fetch.parent_request_id, None);
            assert_eq!(fetch.initial_stream_credit_bytes, Some(0));
            assert!(
                fetch
                    .deadline_after_ms
                    .is_some_and(|ms| ms > 0 && ms <= 5000)
            );
            let invoke = dto::InvokeProviderRequest::decode(fetch.payload.as_slice()).unwrap();
            assert_eq!(
                wire_value(invoke.context.unwrap().value.unwrap())["messages"],
                serde_json::json!([source.1])
            );
            assert_eq!(
                wire_value(invoke.options.unwrap().value.unwrap())["deferred"],
                false
            );
            let child_replies = peer.peer_response_frames();
            let reply = child_replies
                .iter()
                .find(|r| r.request_id == resume_request.request_id)
                .expect("same child hook Result");
            let Some(v1_1::response::Terminal::Result(body)) = &reply.terminal else {
                panic!("typed before_resume Result");
            };
            dto::BeforeResumeUnitResult::decode(body.as_slice()).unwrap();
            assert!(body.is_empty(), "before_resume is a typed unit Result");
            let provider_ends = peer.peer_stream_end_frames();
            for (request, stop_reason) in
                [(tail[3].request_id, "deferred"), (fetch.request_id, "stop")]
            {
                let end = provider_ends
                    .iter()
                    .find(|end| end.request_id == request)
                    .expect("same child provider terminal");
                let Some(v1_1::stream_end::Terminal::Result(body)) = &end.terminal else {
                    panic!("typed provider Result");
                };
                let result = dto::InvokeProviderResult::decode(body.as_slice()).unwrap();
                assert_eq!(
                    wire_value(result.assistant_message.unwrap().value.unwrap())["stopReason"],
                    stop_reason
                );
                assert!(result.usage.is_some());
            }
            let mut ids = std::collections::BTreeSet::new();
            for request in &wire {
                assert!(ids.insert(request.request_id));
                assert_eq!(request.initial_stream_credit_bytes, Some(0));
                assert!(
                    request
                        .deadline_after_ms
                        .is_some_and(|ms| ms > 0 && ms <= 5000)
                );
                let reference = connection
                    .grant()
                    .profiles()
                    .iter()
                    .find(|p| p.reference().name == request.capability_ref.as_ref().unwrap().name)
                    .unwrap()
                    .reference();
                assert_eq!(request.capability_ref.as_ref(), Some(reference));
            }
            let no_work_snapshot = setup.pump.drive(store.snapshot()).unwrap();
            let no_work_requests = peer.requests();
            let no_work_replies = peer.peer_response_frames().len();
            let no_work_ends = peer.peer_stream_end_frames().len();
            assert!(matches!(
                drive(setup, harness.resume()),
                Ok(Err(ResumeExpectedError::NothingToResume(_)))
            ));
            assert_eq!(
                setup.pump.drive(store.snapshot()).unwrap(),
                no_work_snapshot
            );
            assert_eq!(peer.requests(), no_work_requests);
            assert_eq!(peer.peer_response_frames().len(), no_work_replies);
            assert_eq!(peer.peer_stream_end_frames().len(), no_work_ends);
        }
    }
    if !event_mode {
        assert_eq!(connection.occupancy(), 0);
    }
    peer.finish(true);
    connection.close();
    if event_mode {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
        while connection.occupancy() != 0 {
            setup.pump.tick();
            assert!(
                std::time::Instant::now() < deadline,
                "finite passive retirement"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    assert_eq!(connection.occupancy(), 0);
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

fn native_tool_hook_pair<S: RuntimeStorage + 'static>(
    setup: &Setup,
    session: Arc<RuntimeSession<S>>,
    store: Arc<S>,
    language: &str,
    core: u32,
    minor: u32,
    blocked: bool,
) {
    use pi_extension_protocol::activation::HostAuthority;
    use pi_extension_protocol::profiles::{ApprovedProfile, Exposure, ProfileApproval};
    use pi_harness_runtime::extensions::{
        tool_driver::{EchoRouteTiming, OwnerLimits, SingleToolOwner},
        transport::{OrderedByteChannel, PumpConfig},
    };
    use runtime_extension_profiles_support::process::{Channel, RealClock, drive};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::{NonZeroU64, NonZeroUsize};
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "runtime_extension_profiles_support/profiles-runtime-v1.json"
    ))
    .unwrap();
    let catalog = echo_catalog();
    let metadata = &catalog["entries"][0]["agentTool"];
    let descriptor = AgentTool {
        name: pi_ai_core::ToolName::new("echo").unwrap(),
        label: metadata["label"].as_str().unwrap().into(),
        description: metadata["description"].as_str().unwrap().into(),
        parameters: serde_json::from_value(metadata["parameters"].clone()).unwrap(),
        constrained_sampling: None,
        execution_mode: Some(ToolExecutionMode::Sequential),
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
    let mut leaves = corpus["exposure"]["open_leaves"]
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
    let args = pi_extension_protocol::jcs::admit_schema(
        &serde_json::to_vec(&metadata["parameters"]).unwrap(),
        Default::default(),
    )
    .unwrap();
    let parameters = pi_extension_protocol::jcs::admit_schema(
        &serde_json::to_vec(&exact_schema(&metadata["parameters"])).unwrap(),
        Default::default(),
    )
    .unwrap();
    let empty = pi_extension_protocol::jcs::admit_schema(
        br#"{"type":"object","properties":{},"additionalProperties":false}"#,
        Default::default(),
    )
    .unwrap();
    for path in [
        "hook.input:before_tool.event.args",
        "hook.output:before_tool.result.args",
        "hook.input:after_tool.event.args",
        "provider.assistant_message.v1.content[].arguments",
        "provider.assistant_event.v1.content[].arguments",
        "provider.context.v1.messages[].content[].arguments",
    ] {
        leaves.insert(path.into(), args.clone());
    }
    leaves.insert("provider.tools.v1[].parameters".into(), parameters);
    leaves.insert("hook.input:after_tool.event.details".into(), empty);
    leaves.insert(
        "provider.context.v1.messages[].details".into(),
        pi_extension_protocol::jcs::admit_schema(
            br#"{"type":"object","properties":{},"additionalProperties":false}"#,
            Default::default(),
        )
        .unwrap(),
    );
    let exposure = Exposure::new(leaves, BTreeSet::new()).unwrap();
    let profiles = accepted_profiles()
        .into_iter()
        .map(|profile| {
            ApprovedProfile::new(ProfileApproval {
                principal: profile.principal().into(),
                kind: profile.kind().into(),
                name: profile.name().into(),
                profile: profile.profile().into(),
                handler: profile.handler().into(),
                minors: profile.minors().clone(),
                replay: profile.replay(),
                execution: profile.execution(),
                scope: profile.scope().into(),
                permissions: profile.permissions().clone(),
                exposure: exposure.clone(),
            })
            .unwrap()
        })
        .collect::<Vec<_>>();
    let baseline = service_host(core, profiles.clone());
    let permissions = profiles
        .iter()
        .flat_map(|profile| {
            profile.permissions().iter().map(|(name, scope)| {
                pi_extension_protocol::wire::v1_1::PermissionGrant {
                    name: name.clone(),
                    scope: scope.clone(),
                }
            })
        })
        .collect();
    let host = HostAuthority::new(
        "peer-a".into(),
        7,
        core,
        1,
        *baseline.limits(),
        vec![echo_approval()],
        permissions,
    )
    .unwrap()
    .with_profiles(profiles)
    .unwrap();
    let peer = Channel::spawn_scenario(
        language,
        core,
        minor,
        if blocked {
            "runtime-native-tool-hook-block"
        } else {
            "runtime-native-tool-hook"
        },
    );
    let owner = SingleToolOwner::new(
        setup.pump.clone(),
        OwnerLimits {
            outbound_bytes: NonZeroUsize::new(1048576).unwrap(),
            receipt_bytes: NonZeroUsize::new(262144).unwrap(),
            write_timeout_ms: NonZeroU64::new(5000).unwrap(),
        },
    );
    let (connection, route) = drive(
        setup,
        harness.connect_profile_tool(
            &owner,
            host,
            peer.clone(),
            Arc::new(RealClock::default()),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(4096).unwrap(),
                write_timeout_ms: NonZeroU64::new(5000).unwrap(),
            },
            reservations(),
            20000,
        ),
    )
    .unwrap();
    assert!(
        connection
            .grant()
            .tools()
            .iter()
            .any(|tool| tool.descriptor().name == "echo")
    );
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
    assert!(
        connection.grant().profiles().iter().all(|profile| profile
            .reference()
            .profile
            .as_ref()
            .unwrap()
            .minor
            == minor)
    );
    let selected_tool = connection
        .grant()
        .tools()
        .iter()
        .find(|tool| tool.descriptor().name == "echo")
        .unwrap()
        .reference()
        .clone();
    assert_eq!(selected_tool.profile.as_ref().unwrap().minor, minor);
    assert_eq!(selected_tool.authenticated_extension_id, "peer-a");
    assert_eq!(
        selected_tool.handler_id.as_deref(),
        Some("qualified_handler")
    );
    assert_eq!(selected_tool.schema_digest_sha256.len(), 32);
    drive(
        setup,
        harness.register_ready_echo_tool(
            tool.clone(),
            Arc::new(route),
            EchoRouteTiming {
                method_limit_ms: NonZeroU64::new(5000).unwrap(),
                connection_deadline_ms: 20000,
            },
        ),
    )
    .unwrap()
    .unwrap();
    drive(
        setup,
        harness.set_active_tools(vec![pi_ai_core::ToolName::new("echo").unwrap()]),
    )
    .unwrap();
    for name in ["before_tool", "after_tool"] {
        let selected = connection
            .grant()
            .profiles()
            .iter()
            .find(|profile| profile.reference().name == name)
            .unwrap();
        harness
            .register_profile_hook(
                RemoteHook::new(
                    connection.clone(),
                    selected.reference().clone(),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let model: pi_ai_core::Model = serde_json::from_value(
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
            vec![model.clone()],
            Default::default(),
            None,
        ),
    )
    .unwrap();
    drive(setup, harness.set_model(model)).unwrap();
    let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
        text: "native hook Tool run".into(),
        images: vec![],
    }));
    if !blocked {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        loop {
            if let std::task::Poll::Ready(result) = poll(prompt.as_mut()) {
                panic!(
                    "prompt completed before selected Tool: {result:?}; methods={:?}; ends={}; peer={}",
                    peer.request_frames()
                        .iter()
                        .map(|r| &r.method)
                        .collect::<Vec<_>>(),
                    peer.peer_stream_end_frames().len(),
                    peer.stderr_text()
                );
            }
            setup.pump.tick();
            if peer
                .request_frames()
                .iter()
                .any(|request| request.method == "tool.invoke")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "actual selected Tool Request before terminal; peer={}",
                peer.stderr_text()
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let before_terminal = setup.pump.drive(store.snapshot()).unwrap();
        assert_eq!(before_terminal.entries.iter().filter(|entry| matches!(entry, Entry::Message(entry) if matches!(entry.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))).count(), 0);
        assert!(
            before_terminal
                .usage
                .iter()
                .all(|usage| usage.usage.input.get() != 3)
        );
        let binding = owner.retained_binding().expect("live Tool owner B");
        let tool_request = peer
            .request_frames()
            .into_iter()
            .find(|request| request.method == "tool.invoke")
            .unwrap();
        assert_eq!(binding.request_id(), tool_request.request_id);
        assert_eq!(
            binding.effect_key(),
            tool_request.effect_key.as_ref().unwrap()
        );
        assert_eq!(binding.capability_ref(), &selected_tool);
        assert_eq!(
            binding.generation(),
            connection.grant().wire().connection_generation
        );
        assert_eq!(tool_request.initial_stream_credit_bytes, Some(0));
        assert!(
            tool_request
                .deadline_after_ms
                .is_some_and(|ms| ms > 0 && ms <= 5000)
        );
        assert_eq!(tool_request.parent_request_id, None);
        peer.signal("tool");
    }
    let result = drive(setup, prompt).unwrap_or_else(|error| panic!("native Tool pair run: {error:?}/{error}; methods={:?}; ends={}; responses={}; snapshot={:?}; peer={}", peer.request_frames().iter().map(|r| &r.method).collect::<Vec<_>>(), peer.peer_stream_end_frames().len(), peer.peer_response_frames().len(), setup.pump.drive(store.snapshot()).unwrap(), peer.stderr_text())).unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(
        tool.calls.load(Ordering::SeqCst),
        0,
        "selected process executes Tool"
    );
    let requests = peer.request_frames();
    let expected = if blocked {
        vec!["provider.invoke", "before_tool", "provider.invoke"]
    } else {
        vec![
            "provider.invoke",
            "before_tool",
            "tool.invoke",
            "after_tool",
            "provider.invoke",
        ]
    };
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        expected
    );
    let snapshot = setup.pump.drive(store.snapshot()).unwrap();
    let results = snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Message(entry) => match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) => Some(result),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tool_call_id.as_str(), "native-hook-call");
    assert_eq!(results[0].tool_name.as_str(), "echo");
    let assistant = snapshot
        .entries
        .iter()
        .find_map(|entry| match entry {
            Entry::Message(entry) => match &entry.message {
                AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message))
                    if message.stop_reason == pi_ai_core::StopReason::ToolUse =>
                {
                    Some((entry, message))
                }
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    let source_index = assistant.1.content.iter().position(|content| matches!(content, pi_ai_core::AssistantContent::ToolCall(call) if call.id.as_str() == "native-hook-call" && call.name.as_str() == "echo")).unwrap() as u64;
    let before = requests
        .iter()
        .find(|request| request.method == "before_tool")
        .unwrap();
    let before_key = before.effect_key.as_ref().unwrap();
    assert_eq!(before_key.effect_kind, "hook");
    assert_eq!(before_key.item_index, Some(source_index));
    assert_eq!(before_key.operation_id, result.run_id.as_str());
    assert_eq!(before.parent_request_id, None);
    assert_eq!(before.initial_stream_credit_bytes, Some(0));
    assert!(
        before
            .deadline_after_ms
            .is_some_and(|ms| ms > 0 && ms <= 5000)
    );
    if blocked {
        assert!(results[0].is_error);
        assert_eq!(
            peer.peer_stream_end_frames().len(),
            2,
            "provider terminals only"
        );
        assert!(
            snapshot
                .usage
                .iter()
                .all(|usage| usage.usage.input.get() != 3)
        );
        assert!(
            !requests
                .iter()
                .any(|request| request.method == "tool.invoke")
        );
    } else {
        assert!(!results[0].is_error);
        assert!(
            matches!(&results[0].content[0], pi_ai_core::InputContent::Text(text) if text.text == "native after patch")
        );
        let tool_request = requests
            .iter()
            .find(|request| request.method == "tool.invoke")
            .unwrap();
        let after = requests
            .iter()
            .find(|request| request.method == "after_tool")
            .unwrap();
        let tool_key = tool_request.effect_key.as_ref().unwrap();
        assert_eq!(tool_key.effect_kind, "tool");
        assert_eq!(tool_key.item_index, before_key.item_index);
        assert_eq!(tool_key.operation_id, before_key.operation_id);
        assert_eq!(tool_key.step_or_task_id, before_key.step_or_task_id);
        assert_eq!(tool_request.capability_ref.as_ref(), Some(&selected_tool));
        assert_eq!(after.parent_request_id, Some(tool_request.request_id));
        assert_eq!(after.effect_key, tool_request.effect_key);
        assert_eq!(after.initial_stream_credit_bytes, Some(0));
        assert!(
            after
                .deadline_after_ms
                .is_some_and(|ms| ms > 0 && ms <= 5000)
        );
        assert_eq!(tool_request.initial_stream_credit_bytes, Some(0));
        assert_eq!(
            peer.peer_stream_end_frames()
                .iter()
                .filter(|end| end.request_id == tool_request.request_id)
                .count(),
            1
        );
        let result_entry = snapshot
            .entries
            .iter()
            .find_map(|entry| match entry {
                Entry::Message(entry)
                    if matches!(
                        entry.message,
                        AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))
                    ) =>
                {
                    Some(entry)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            snapshot
                .usage
                .iter()
                .filter(|usage| usage.entry_id.as_ref() == Some(&result_entry.id)
                    && usage.usage.input.get() == 3)
                .count(),
            1
        );
    }
    assert_eq!(
        requests
            .iter()
            .map(|request| request.request_id)
            .collect::<BTreeSet<_>>()
            .len(),
        requests.len()
    );
    assert_eq!(connection.occupancy(), 0);
    connection.close();
    peer.close();
    drive(setup, harness.close()).unwrap();
}
#[test]
fn runtime_native_tool_hook_pair_memory_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                for blocked in [false, true] {
                    let setup = Setup::new();
                    native_tool_hook_pair(
                        &setup,
                        setup.session.clone(),
                        setup.store.clone(),
                        language,
                        core,
                        minor,
                        blocked,
                    );
                }
            }
        }
    }
}
#[test]
fn runtime_native_tool_hook_pair_sqlite_all_pairs() {
    for language in ["typescript", "python"] {
        for core in 0..=1 {
            for minor in 0..=1 {
                for blocked in [false, true] {
                    let setup = Setup::new();
                    let (session, store, dir) = sqlite_case(&setup);
                    native_tool_hook_pair(
                        &setup,
                        session,
                        store.clone(),
                        language,
                        core,
                        minor,
                        blocked,
                    );
                    drop(store);
                    std::fs::remove_dir_all(dir).unwrap();
                }
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
#[test]
fn runtime_native_host_resource_get_memory_all_pairs() {
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
                    ProcessMode::HostResource,
                );
            }
        }
    }
}
#[test]
fn runtime_native_host_resource_get_sqlite_all_pairs() {
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
                    ProcessMode::HostResource,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
fn host_resource_refusal_all_pairs(mode: ProcessMode) {
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
                    mode,
                );
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                accepted_process_consumer(
                    &setup,
                    session,
                    store.clone(),
                    core,
                    minor,
                    language,
                    mode,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_host_resource_get_missing_catalog_id_refusal() {
    host_resource_refusal_all_pairs(ProcessMode::HostResourceMissing);
}
#[test]
fn runtime_native_host_resource_get_wrong_scope_refusal() {
    host_resource_refusal_all_pairs(ProcessMode::HostResourceScope);
}
#[test]
fn runtime_native_host_resource_get_wrong_version_refusal() {
    host_resource_refusal_all_pairs(ProcessMode::HostResourceVersion);
}
#[test]
fn runtime_native_host_resource_get_missing_parent_refusal() {
    host_resource_refusal_all_pairs(ProcessMode::HostResourceParent);
}
#[test]
fn runtime_native_structural_positive_all_pairs() {
    structural_case_all_pairs(ProcessMode::Structural);
}
#[test]
fn runtime_native_structural_compaction_decline_all_pairs() {
    structural_case_all_pairs(ProcessMode::StructuralCompactionDecline);
}
#[test]
fn runtime_native_structural_navigation_decline_all_pairs() {
    structural_case_all_pairs(ProcessMode::StructuralNavigationDecline);
}
fn structural_case_all_pairs(mode: ProcessMode) {
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
                    mode,
                );
                let setup = Setup::new();
                let (session, store, dir) = sqlite_case(&setup);
                accepted_process_consumer(
                    &setup,
                    session,
                    store.clone(),
                    core,
                    minor,
                    language,
                    mode,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_host_snapshot_memory_all_pairs() {
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
                    ProcessMode::HostSnapshot,
                );
            }
        }
    }
}
#[test]
fn runtime_native_host_snapshot_sqlite_all_pairs() {
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
                    ProcessMode::HostSnapshot,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_host_queue_memory_all_pairs() {
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
                    ProcessMode::HostQueue,
                );
            }
        }
    }
}
#[test]
fn runtime_native_host_queue_sqlite_all_pairs() {
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
                    ProcessMode::HostQueue,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_host_inbox_memory_all_pairs() {
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
                    ProcessMode::HostInbox,
                );
            }
        }
    }
}
#[test]
fn runtime_native_host_inbox_sqlite_all_pairs() {
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
                    ProcessMode::HostInbox,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_host_abort_old_memory_all_pairs() {
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
                    ProcessMode::HostAbortOld,
                );
            }
        }
    }
}
#[test]
fn runtime_native_host_abort_old_sqlite_all_pairs() {
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
                    ProcessMode::HostAbortOld,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_host_abort_current_memory_all_pairs() {
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
                    ProcessMode::HostAbortCurrent,
                );
            }
        }
    }
}
#[test]
fn runtime_native_host_abort_current_sqlite_all_pairs() {
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
                    ProcessMode::HostAbortCurrent,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}

#[test]
fn runtime_native_host_abort_current_physical_held_write_close_fence() {
    let setup = Setup::new();
    accepted_process_consumer(
        &setup,
        setup.session.clone(),
        setup.store.clone(),
        0,
        0,
        "typescript",
        ProcessMode::HostAbortCurrentPhysicalHeld,
    );
}

#[test]
fn runtime_native_host_abort_current_write_failure_generic_fault() {
    let setup = Setup::new();
    accepted_process_consumer(
        &setup,
        setup.session.clone(),
        setup.store.clone(),
        0,
        0,
        "typescript",
        ProcessMode::HostAbortCurrentWriteFailure,
    );
}
#[test]
fn runtime_native_event_sink_memory_all_pairs() {
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
                    ProcessMode::EventSink,
                );
            }
        }
    }
}
#[test]
fn runtime_native_event_sink_sqlite_all_pairs() {
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
                    ProcessMode::EventSink,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_event_sink_wrong_id_memory_no_write() {
    for language in ["typescript", "python"] {
        let setup = Setup::new();
        accepted_process_consumer(
            &setup,
            setup.session.clone(),
            setup.store.clone(),
            0,
            0,
            language,
            ProcessMode::EventSinkWrongId,
        );
    }
}
#[test]
fn runtime_native_event_sink_wrong_id_sqlite_no_write() {
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
            ProcessMode::EventSinkWrongId,
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[test]
fn runtime_native_telemetry_memory_all_pairs() {
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
                    ProcessMode::Telemetry,
                );
            }
        }
    }
}
#[test]
fn runtime_native_telemetry_sqlite_all_pairs() {
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
                    ProcessMode::Telemetry,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_before_payload_memory_all_pairs() {
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
                    ProcessMode::BeforePayload,
                );
            }
        }
    }
}
#[test]
fn runtime_native_before_payload_sqlite_all_pairs() {
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
                    ProcessMode::BeforePayload,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_before_payload_malformed_memory_all_pairs() {
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
                    ProcessMode::BeforePayloadMalformed,
                );
            }
        }
    }
}
#[test]
fn runtime_native_before_payload_malformed_sqlite_all_pairs() {
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
                    ProcessMode::BeforePayloadMalformed,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_before_resume_memory_all_pairs() {
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
                    ProcessMode::BeforeResume,
                );
            }
        }
    }
}
#[test]
fn runtime_native_before_resume_sqlite_all_pairs() {
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
                    ProcessMode::BeforeResume,
                );
                drop(store);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}
#[test]
fn runtime_native_telemetry_bad_ack_memory_no_write() {
    for language in ["typescript", "python"] {
        let setup = Setup::new();
        accepted_process_consumer(
            &setup,
            setup.session.clone(),
            setup.store.clone(),
            0,
            0,
            language,
            ProcessMode::TelemetryBadAck,
        );
    }
}
#[test]
fn runtime_native_telemetry_bad_ack_sqlite_no_write() {
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
            ProcessMode::TelemetryBadAck,
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[test]
fn runtime_native_telemetry_bad_flush_memory_no_write() {
    for language in ["typescript", "python"] {
        let setup = Setup::new();
        accepted_process_consumer(
            &setup,
            setup.session.clone(),
            setup.store.clone(),
            0,
            0,
            language,
            ProcessMode::TelemetryBadFlush,
        );
    }
}
#[test]
fn runtime_native_telemetry_bad_flush_sqlite_no_write() {
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
            ProcessMode::TelemetryBadFlush,
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
