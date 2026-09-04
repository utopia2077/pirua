use std::error::Error;
use std::fmt;
use std::panic::panic_any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Weak};
use std::thread;

use pi_ai_core::{
    SessionResource, SessionResourceError, SessionResourceHandle, SessionResourceRegistry,
    SessionResourceRegistryErrorCode,
};

type EventLog = Arc<Mutex<Vec<String>>>;

fn record(log: &EventLog, event: impl Into<String>) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(event.into());
}

fn events(log: &EventLog) -> Vec<String> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

fn register_resource<R>(
    registry: &SessionResourceRegistry,
    resource: &Arc<R>,
) -> SessionResourceHandle
where
    R: SessionResource + 'static,
{
    let resource: Arc<dyn SessionResource> = resource.clone();
    registry.register(resource).expect("registration succeeds")
}

#[derive(Debug)]
struct TaggedError(&'static str);

impl fmt::Display for TaggedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TaggedError {}

fn failure_tags(failures: &[SessionResourceError]) -> Vec<Option<String>> {
    failures
        .iter()
        .map(|failure| Error::source(failure).map(ToString::to_string))
        .collect()
}

enum BasicOutcome {
    Success,
    Fail(&'static str),
    FailThenSuccess(&'static str),
}

struct BasicResource {
    id: &'static str,
    outcome: BasicOutcome,
    calls: AtomicUsize,
    log: EventLog,
    sessions: Arc<Mutex<Vec<Option<String>>>>,
}

impl BasicResource {
    fn new(id: &'static str, outcome: BasicOutcome, log: EventLog) -> Self {
        Self {
            id,
            outcome,
            calls: AtomicUsize::new(0),
            log,
            sessions: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SessionResource for BasicResource {
    fn cleanup(&self, session_id: Option<&str>) -> Result<(), SessionResourceError> {
        record(&self.log, format!("callback:{}", self.id));
        self.sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(session_id.map(str::to_owned));
        let prior_calls = self.calls.fetch_add(1, Ordering::AcqRel);
        match self.outcome {
            BasicOutcome::Success => Ok(()),
            BasicOutcome::Fail(tag) => Err(SessionResourceError::with_source(TaggedError(tag))),
            BasicOutcome::FailThenSuccess(tag) if prior_calls == 0 => {
                Err(SessionResourceError::with_source(TaggedError(tag)))
            }
            BasicOutcome::FailThenSuccess(_) => Ok(()),
        }
    }
}

#[test]
fn duplicate_object_registrations_receive_independent_tokens() {
    let registry = SessionResourceRegistry::new();
    let resource = Arc::new(BasicResource::new(
        "shared",
        BasicOutcome::Success,
        EventLog::default(),
    ));
    let mut first = register_resource(&registry, &resource);
    let mut second = register_resource(&registry, &resource);

    assert!(first.remove());
    assert!(!first.remove());
    assert!(registry.cleanup(Some("session")).is_ok());
    assert_eq!(resource.calls.load(Ordering::Acquire), 1);
    assert!(!second.remove());
}

#[test]
fn cleanup_uses_insertion_ordered_snapshot() {
    let registry = SessionResourceRegistry::new();
    let log = EventLog::default();
    let resources = [
        Arc::new(BasicResource::new(
            "first",
            BasicOutcome::Success,
            Arc::clone(&log),
        )),
        Arc::new(BasicResource::new(
            "second",
            BasicOutcome::Success,
            Arc::clone(&log),
        )),
        Arc::new(BasicResource::new(
            "third",
            BasicOutcome::Success,
            Arc::clone(&log),
        )),
    ];
    let _handles = resources
        .iter()
        .map(|resource| register_resource(&registry, resource))
        .collect::<Vec<_>>();

    assert!(registry.cleanup(Some("ordered")).is_ok());
    assert_eq!(
        events(&log),
        ["callback:first", "callback:second", "callback:third"]
    );
    assert!(registry.cleanup(Some("again")).is_ok());
    assert_eq!(events(&log).len(), 3);
}

#[test]
fn cleanup_forwards_absent_session_id() {
    let registry = SessionResourceRegistry::new();
    let resource = Arc::new(BasicResource::new(
        "none",
        BasicOutcome::Success,
        EventLog::default(),
    ));
    let _handle = register_resource(&registry, &resource);

    assert!(registry.cleanup(None).is_ok());
    assert_eq!(
        *resource
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()),
        [None]
    );
}

#[test]
fn cleanup_forwards_exact_session_id() {
    let registry = SessionResourceRegistry::new();
    let resource = Arc::new(BasicResource::new(
        "exact",
        BasicOutcome::Success,
        EventLog::default(),
    ));
    let _handle = register_resource(&registry, &resource);
    let session_id = "会话/alpha\nexact";

    assert!(registry.cleanup(Some(session_id)).is_ok());
    assert_eq!(
        *resource
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()),
        [Some(session_id.to_owned())]
    );
}

#[test]
fn explicit_removal_is_idempotent() {
    let registry = SessionResourceRegistry::new();
    let resource = Arc::new(BasicResource::new(
        "removed",
        BasicOutcome::Success,
        EventLog::default(),
    ));
    let mut handle = register_resource(&registry, &resource);

    assert!(handle.remove());
    assert!(!handle.remove());
    assert!(registry.cleanup(None).is_ok());
    assert_eq!(resource.calls.load(Ordering::Acquire), 0);
}

#[test]
fn dropping_unique_handle_unregisters_exactly_one_token() {
    let registry = SessionResourceRegistry::new();
    let resource = Arc::new(BasicResource::new(
        "shared",
        BasicOutcome::Success,
        EventLog::default(),
    ));
    let first = register_resource(&registry, &resource);
    let _second = register_resource(&registry, &resource);

    drop(first);
    assert!(registry.cleanup(None).is_ok());
    assert_eq!(resource.calls.load(Ordering::Acquire), 1);
}

struct RegisteringResource {
    registry: Weak<SessionResourceRegistry>,
    next: Arc<BasicResource>,
    next_handle: Mutex<Option<SessionResourceHandle>>,
    log: EventLog,
}

impl SessionResource for RegisteringResource {
    fn cleanup(&self, _session_id: Option<&str>) -> Result<(), SessionResourceError> {
        record(&self.log, "callback:registrar");
        let registry = self.registry.upgrade().expect("registry remains live");
        let handle = register_resource(&registry, &self.next);
        *self
            .next_handle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(handle);
        Ok(())
    }
}

#[test]
fn registration_during_cleanup_waits_for_next_pass() {
    let registry = Arc::new(SessionResourceRegistry::new());
    let log = EventLog::default();
    let next = Arc::new(BasicResource::new(
        "next",
        BasicOutcome::Success,
        Arc::clone(&log),
    ));
    let registrar = Arc::new(RegisteringResource {
        registry: Arc::downgrade(&registry),
        next,
        next_handle: Mutex::new(None),
        log: Arc::clone(&log),
    });
    let _registrar_handle = register_resource(&registry, &registrar);

    assert!(registry.cleanup(Some("first")).is_ok());
    assert_eq!(events(&log), ["callback:registrar"]);
    assert!(registry.cleanup(Some("second")).is_ok());
    assert_eq!(events(&log), ["callback:registrar", "callback:next"]);
}

struct RemovingResource {
    target: Mutex<Option<SessionResourceHandle>>,
    log: EventLog,
}

impl SessionResource for RemovingResource {
    fn cleanup(&self, _session_id: Option<&str>) -> Result<(), SessionResourceError> {
        record(&self.log, "callback:remover");
        let removed = self
            .target
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .is_some_and(SessionResourceHandle::remove);
        assert!(removed);
        Ok(())
    }
}

#[test]
fn removal_during_cleanup_keeps_current_snapshot_but_skips_next_pass() {
    let registry = SessionResourceRegistry::new();
    let log = EventLog::default();
    let remover = Arc::new(RemovingResource {
        target: Mutex::new(None),
        log: Arc::clone(&log),
    });
    let target = Arc::new(BasicResource::new(
        "target",
        BasicOutcome::Fail("target-failure-must-still-run"),
        Arc::clone(&log),
    ));
    let _remover_handle = register_resource(&registry, &remover);
    let target_handle = register_resource(&registry, &target);
    *remover
        .target
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(target_handle);

    let error = registry
        .cleanup(None)
        .expect_err("removed snapshot member still runs and fails");
    assert_eq!(
        failure_tags(error.failures()),
        [Some("target-failure-must-still-run".to_owned())]
    );
    assert_eq!(events(&log), ["callback:remover", "callback:target"]);
    assert!(registry.cleanup(None).is_ok());
    assert_eq!(events(&log).len(), 2);
}

struct BlockingReentrantResource {
    registry: Weak<SessionResourceRegistry>,
    entered: SyncSender<()>,
    release: Mutex<Receiver<()>>,
    nested_code: Arc<Mutex<Option<SessionResourceRegistryErrorCode>>>,
}

impl SessionResource for BlockingReentrantResource {
    fn cleanup(&self, _session_id: Option<&str>) -> Result<(), SessionResourceError> {
        let registry = self.registry.upgrade().expect("registry remains live");
        let nested = registry
            .cleanup(Some("nested"))
            .expect_err("nested cleanup is rejected");
        *self
            .nested_code
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(nested.code());
        self.entered.send(()).expect("test receiver remains live");
        self.release
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .recv()
            .expect("test sender remains live");
        Ok(())
    }
}

#[test]
fn nested_and_concurrent_cleanup_return_cleanup_in_progress() {
    let registry = Arc::new(SessionResourceRegistry::new());
    let (entered_tx, entered_rx) = sync_channel(0);
    let (release_tx, release_rx) = sync_channel(0);
    let nested_code = Arc::new(Mutex::new(None));
    let resource = Arc::new(BlockingReentrantResource {
        registry: Arc::downgrade(&registry),
        entered: entered_tx,
        release: Mutex::new(release_rx),
        nested_code: Arc::clone(&nested_code),
    });
    let _handle = register_resource(&registry, &resource);
    let outer_registry = Arc::clone(&registry);
    let outer = thread::spawn(move || outer_registry.cleanup(Some("outer")));

    entered_rx.recv().expect("outer callback entered");
    let concurrent = registry
        .cleanup(Some("concurrent"))
        .expect_err("concurrent cleanup is rejected");
    assert_eq!(
        concurrent.code(),
        SessionResourceRegistryErrorCode::CleanupInProgress
    );
    assert_eq!(
        concurrent.to_string(),
        "session resource cleanup in progress"
    );
    assert_eq!(
        format!("{concurrent:?}"),
        "SessionResourceRegistryError { code: CleanupInProgress, failure_count: 0 }"
    );
    release_tx.send(()).expect("outer callback remains live");
    assert!(outer.join().expect("outer thread does not panic").is_ok());
    assert_eq!(
        *nested_code
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()),
        Some(SessionResourceRegistryErrorCode::CleanupInProgress)
    );
}

struct SecondaryPanicPayload {
    drops: Arc<AtomicUsize>,
}

impl Drop for SecondaryPanicPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        panic!("SECONDARY_PAYLOAD_SECRET_SENTINEL");
    }
}

struct CallbackPanicPayload {
    id: &'static str,
    registry: Weak<SessionResourceRegistry>,
    log: EventLog,
    secondary_drops: Arc<AtomicUsize>,
}

impl Drop for CallbackPanicPayload {
    fn drop(&mut self) {
        record(&self.log, format!("payload_drop:{}", self.id));
        let registry = self.registry.upgrade().expect("registry remains live");
        let code = registry
            .cleanup(Some("payload-drop"))
            .expect_err("payload disposal remains inside active cleanup")
            .code();
        record(&self.log, format!("payload_reentrant:{}:{code:?}", self.id));
        panic_any(SecondaryPanicPayload {
            drops: Arc::clone(&self.secondary_drops),
        });
    }
}

enum ProbeOutcome {
    Success,
    PanicWithPayload,
}

struct DisposalProbeResource {
    id: &'static str,
    registry: Weak<SessionResourceRegistry>,
    own_handle: Mutex<Option<SessionResourceHandle>>,
    remove_own: bool,
    outcome: ProbeOutcome,
    panic_on_drop: bool,
    log: EventLog,
    secondary_drops: Arc<AtomicUsize>,
}

impl SessionResource for DisposalProbeResource {
    fn cleanup(&self, _session_id: Option<&str>) -> Result<(), SessionResourceError> {
        record(&self.log, format!("callback:{}", self.id));
        if self.remove_own {
            let removed = self
                .own_handle
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_mut()
                .is_some_and(SessionResourceHandle::remove);
            assert!(removed);
        }
        match self.outcome {
            ProbeOutcome::Success => Ok(()),
            ProbeOutcome::PanicWithPayload => panic_any(CallbackPanicPayload {
                id: self.id,
                registry: self.registry.clone(),
                log: Arc::clone(&self.log),
                secondary_drops: Arc::clone(&self.secondary_drops),
            }),
        }
    }
}

impl Drop for DisposalProbeResource {
    fn drop(&mut self) {
        record(&self.log, format!("resource_drop:{}", self.id));
        if let Some(registry) = self.registry.upgrade() {
            let code = registry
                .cleanup(Some("resource-drop"))
                .expect_err("resource disposal remains inside active cleanup")
                .code();
            record(
                &self.log,
                format!("resource_reentrant:{}:{code:?}", self.id),
            );
        }
        if self.panic_on_drop {
            panic_any(SecondaryPanicPayload {
                drops: Arc::clone(&self.secondary_drops),
            });
        }
    }
}

fn disposal_probe(
    registry: &Arc<SessionResourceRegistry>,
    id: &'static str,
    remove_own: bool,
    outcome: ProbeOutcome,
    panic_on_drop: bool,
    log: &EventLog,
    secondary_drops: &Arc<AtomicUsize>,
) -> Arc<DisposalProbeResource> {
    Arc::new(DisposalProbeResource {
        id,
        registry: Arc::downgrade(registry),
        own_handle: Mutex::new(None),
        remove_own,
        outcome,
        panic_on_drop,
        log: Arc::clone(log),
        secondary_drops: Arc::clone(secondary_drops),
    })
}

#[test]
fn cleanup_continues_after_error_and_panic() {
    let registry = Arc::new(SessionResourceRegistry::new());
    let log = EventLog::default();
    let secondary_drops = Arc::new(AtomicUsize::new(0));
    let ordinary_error = Arc::new(BasicResource::new(
        "error",
        BasicOutcome::Fail("callback-error-source"),
        Arc::clone(&log),
    ));
    let first = disposal_probe(
        &registry,
        "first",
        true,
        ProbeOutcome::PanicWithPayload,
        false,
        &log,
        &secondary_drops,
    );
    let second = disposal_probe(
        &registry,
        "second",
        true,
        ProbeOutcome::Success,
        true,
        &log,
        &secondary_drops,
    );
    let third = disposal_probe(
        &registry,
        "third",
        false,
        ProbeOutcome::Success,
        false,
        &log,
        &secondary_drops,
    );
    let mut ordinary_error_handle = register_resource(&registry, &ordinary_error);
    for resource in [&first, &second] {
        let handle = register_resource(&registry, resource);
        *resource
            .own_handle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(handle);
    }
    let _third_handle = register_resource(&registry, &third);
    drop(first);
    drop(second);
    drop(third);

    let error = registry
        .cleanup(Some("outer"))
        .expect_err("callback and guarded destructors fail");
    assert_eq!(
        error.code(),
        SessionResourceRegistryErrorCode::CleanupFailed
    );
    assert_eq!(error.failures().len(), 4);
    assert_eq!(
        failure_tags(error.failures()),
        [Some("callback-error-source".to_owned()), None, None, None,]
    );
    assert_eq!(
        events(&log),
        [
            "callback:error",
            "callback:first",
            "callback:second",
            "callback:third",
            "payload_drop:first",
            "payload_reentrant:first:CleanupInProgress",
            "resource_drop:first",
            "resource_reentrant:first:CleanupInProgress",
            "resource_drop:second",
            "resource_reentrant:second:CleanupInProgress",
            "resource_drop:third",
            "resource_reentrant:third:CleanupInProgress",
        ]
    );
    assert_eq!(secondary_drops.load(Ordering::Acquire), 0);
    assert!(ordinary_error_handle.remove());
    assert!(registry.cleanup(Some("after")).is_ok());
}

#[test]
fn cleanup_aggregates_failures_in_order_and_retries_active_failures() {
    let registry = SessionResourceRegistry::new();
    let log = EventLog::default();
    let first = Arc::new(BasicResource::new(
        "first",
        BasicOutcome::FailThenSuccess("first-source"),
        Arc::clone(&log),
    ));
    let second = Arc::new(BasicResource::new(
        "second",
        BasicOutcome::Fail("second-source"),
        Arc::clone(&log),
    ));
    let success = Arc::new(BasicResource::new(
        "success",
        BasicOutcome::Success,
        Arc::clone(&log),
    ));
    let _first_handle = register_resource(&registry, &first);
    let mut second_handle = register_resource(&registry, &second);
    let _success_handle = register_resource(&registry, &success);

    let first_error = registry.cleanup(None).expect_err("two resources fail");
    assert_eq!(
        failure_tags(first_error.failures()),
        [
            Some("first-source".to_owned()),
            Some("second-source".to_owned()),
        ]
    );
    assert_eq!(first_error.to_string(), "session resource cleanup failed");
    assert_eq!(
        format!("{first_error:?}"),
        "SessionResourceRegistryError { code: CleanupFailed, failure_count: 2 }"
    );
    assert!(Error::source(&first_error).is_none());

    let second_error = registry
        .cleanup(None)
        .expect_err("only persistent failure remains");
    assert_eq!(
        failure_tags(second_error.failures()),
        [Some("second-source".to_owned())]
    );
    assert_eq!(
        events(&log),
        [
            "callback:first",
            "callback:second",
            "callback:success",
            "callback:first",
            "callback:second",
        ]
    );
    assert!(second_handle.remove());
    assert!(registry.cleanup(None).is_ok());
    assert_eq!(events(&log).len(), 5);
}

struct SelfRemovingFailure {
    handle: Mutex<Option<SessionResourceHandle>>,
    calls: AtomicUsize,
}

impl SessionResource for SelfRemovingFailure {
    fn cleanup(&self, _session_id: Option<&str>) -> Result<(), SessionResourceError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let removed = self
            .handle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .is_some_and(SessionResourceHandle::remove);
        assert!(removed);
        Err(SessionResourceError::with_source(TaggedError(
            "self-removal-source",
        )))
    }
}

#[test]
fn removal_during_failing_callback_wins_without_reinsertion() {
    let registry = SessionResourceRegistry::new();
    let resource = Arc::new(SelfRemovingFailure {
        handle: Mutex::new(None),
        calls: AtomicUsize::new(0),
    });
    let handle = register_resource(&registry, &resource);
    *resource
        .handle
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(handle);

    let error = registry
        .cleanup(None)
        .expect_err("current-pass failure remains observable");
    assert_eq!(
        failure_tags(error.failures()),
        [Some("self-removal-source".to_owned())]
    );
    assert!(registry.cleanup(None).is_ok());
    assert_eq!(resource.calls.load(Ordering::Acquire), 1);
}
