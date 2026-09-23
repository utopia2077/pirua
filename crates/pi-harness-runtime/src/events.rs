//! Ordered passive subscriptions and snapshot-first buffers.
use pi_agent_schema::*;
use pi_ai_core::TaskSpawner;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone)]
pub(crate) struct EventHub {
    host: Arc<dyn TaskSpawner>,
    shared: Arc<SharedLog>,
    telemetry: crate::owner::Telemetry,
    root_telemetry: crate::owner::Telemetry,
    fault: Arc<Mutex<Option<FaultEvent>>>,
}
// Appending is the only operation on the publication line. Registration
// publishes its cursor and weak entry in the same transaction.
struct SharedLog {
    publication: Mutex<Publication>,
    registrars: Mutex<()>,
    materializers: Mutex<()>,
    #[cfg(test)]
    traversal_probe: Mutex<Option<TraversalProbe>>,
}
#[cfg(test)]
type TraversalProbe = Arc<dyn Fn(bool) + Send + Sync>;
struct Publication {
    tail: Arc<Node>,
    deliveries: Arc<Vec<Weak<Delivery>>>,
}
struct Node {
    event: Option<BufferedEvent>,
    next: Mutex<Option<Arc<Node>>>,
}
type BufferedEvent = (
    HarnessEvent,
    Weak<dyn pi_telemetry::TelemetryContext>,
    HarnessEventType,
    LaneId,
);
impl Node {
    fn next(&self) -> Option<Arc<Self>> {
        self.next.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        // A long, unshared suffix must not recursively drop one stack frame
        // per event. Taking each successor also avoids dropping payloads
        // while the link mutex is held.
        let mut next = self
            .next
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        while let Some(link) = next {
            match Arc::try_unwrap(link) {
                Ok(mut node) => {
                    next = node
                        .next
                        .get_mut()
                        .unwrap_or_else(|p| p.into_inner())
                        .take();
                    drop(node);
                }
                Err(shared) => {
                    drop(shared);
                    break;
                }
            }
        }
    }
}
impl SharedLog {
    fn materialize(&self, only: Option<&Arc<Delivery>>) -> Vec<Arc<Delivery>> {
        let materializer = self.materializers.lock().unwrap_or_else(|p| p.into_inner());
        let (tail, registry) = {
            let publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
            (publication.tail.clone(), publication.deliveries.clone())
        };
        let deliveries: Vec<_> = match only {
            Some(delivery) => vec![delivery.clone()],
            None => registry.iter().filter_map(Weak::upgrade).collect(),
        };
        let mut retired = Vec::new();
        let mut discarded = Vec::new();
        for delivery in &deliveries {
            let Some(start) = delivery
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .cursor
                .clone()
            else {
                continue;
            };
            let mut cursor = start.clone();
            // The delivery can close concurrently. Keep the original cursor
            // (and its forward chain) alive until after materializer release.
            retired.push(Some(start));
            let mut buffered = Vec::new();
            while !Arc::ptr_eq(&cursor, &tail) {
                let next = cursor.next().expect("tail must follow registered cursor");
                if let Some((event, telemetry, kind, lane)) = &next.event
                    && !delivery.kind.is_some_and(|filter| filter != *kind)
                    && (matches!(
                        kind,
                        HarnessEventType::Usage
                            | HarnessEventType::Fault
                            | HarnessEventType::FactUpdate
                    ) || matches!(event, HarnessEvent::GlobalConfigUpdate(_))
                        || !delivery.lane.as_ref().is_some_and(|filter| filter != lane))
                {
                    buffered.push((event.clone(), telemetry.clone(), *kind, lane.clone()));
                }
                #[cfg(test)]
                if let Some(probe) = self.traversal_probe.lock().unwrap().clone() {
                    probe(false);
                }
                cursor = next;
                #[cfg(test)]
                if let Some(probe) = self.traversal_probe.lock().unwrap().clone() {
                    probe(true);
                }
            }
            let mut state = delivery.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed {
                state.queue.extend(buffered);
                retired.push(state.cursor.replace(cursor));
            } else {
                discarded.push(buffered);
            }
        }
        drop(materializer);
        drop(retired);
        drop(discarded);
        drop(registry);
        drop(tail);
        deliveries
    }
}
struct Delivery {
    host: Arc<dyn TaskSpawner>,
    root_telemetry: crate::owner::Telemetry,
    kind: Option<HarnessEventType>,
    lane: Option<LaneId>,
    state: Mutex<DeliveryState>,
}
#[derive(Default)]
struct DeliveryState {
    closed: bool,
    running: bool,
    listener: Option<Arc<dyn EventListener>>,
    cursor: Option<Arc<Node>>,
    queue: VecDeque<BufferedEvent>,
}
impl Delivery {
    fn stop(&self) {
        let discarded = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.closed = true;
            (
                state.listener.take(),
                state.cursor.take(),
                std::mem::take(&mut state.queue),
            )
        };
        drop(discarded);
    }
    fn schedule(self: &Arc<Self>) {
        let start = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed || state.running || state.listener.is_none() || state.queue.is_empty() {
                false
            } else {
                state.running = true;
                true
            }
        };
        if !start {
            return;
        }
        let delivery = self.clone();
        let mut task = Box::pin(async move {
            loop {
                let next = {
                    let mut state = delivery.state.lock().unwrap_or_else(|p| p.into_inner());
                    if state.closed {
                        state.running = false;
                        None
                    } else if let Some(event) = state.queue.pop_front() {
                        Some((
                            state.listener.as_ref().expect("started listener").clone(),
                            event,
                        ))
                    } else {
                        state.running = false;
                        None
                    }
                };
                let Some((listener, (event, telemetry, kind, lane))) = next else {
                    break;
                };
                // Errors are isolated; they neither change commits nor stop peers.
                // Buffering is not execution and must not prolong a completed
                // operation. A listener starting later belongs to the explicit
                // owner context; an already-started listener retains its parent.
                let telemetry = telemetry
                    .upgrade()
                    .unwrap_or_else(|| delivery.root_telemetry.clone());
                let kind = crate::telemetry::event_name(kind);
                let mut attributes = pi_telemetry::SpanAttributes::from([(
                    "pi.event.type".into(),
                    crate::telemetry::string(kind),
                )]);
                if kind != "usage" {
                    attributes.insert(
                        "pi.lane.name".into(),
                        crate::telemetry::string(lane.as_str()),
                    );
                }
                let span = crate::telemetry::Span::start(
                    telemetry,
                    "pi.harness.event_handler",
                    attributes,
                );
                let mut invocation = Box::pin(async move { listener.on_event(event).await });
                let failed = std::future::poll_fn(|cx| {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        invocation.as_mut().poll(cx)
                    })) {
                        Ok(std::task::Poll::Ready(result)) => {
                            std::task::Poll::Ready(result.is_err())
                        }
                        Err(_) => std::task::Poll::Ready(true),
                        Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                    }
                })
                .await;
                span.end(Default::default(), failed);
            }
        });
        // Begin passive listener work outside every owner/publication lock. This
        // mirrors immediate promise invocation; a pending continuation belongs
        // to the real host, never to a metadata completion flag.
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        if task.as_mut().poll(&mut context).is_pending() && self.host.spawn(task).is_err() {
            self.stop();
        }
    }
}
impl EventHub {
    pub fn new(host: Arc<dyn TaskSpawner>, telemetry: crate::owner::Telemetry) -> Self {
        let sentinel = Arc::new(Node {
            event: None,
            next: Mutex::new(None),
        });
        Self {
            host,
            root_telemetry: telemetry.clone(),
            telemetry,
            shared: Arc::new(SharedLog {
                publication: Mutex::new(Publication {
                    tail: sentinel,
                    deliveries: Arc::new(Vec::new()),
                }),
                registrars: Mutex::new(()),
                materializers: Mutex::new(()),
                #[cfg(test)]
                traversal_probe: Mutex::new(None),
            }),
            fault: Arc::new(Mutex::new(None)),
        }
    }
    pub(crate) fn with_context(&self, telemetry: crate::owner::Telemetry) -> Self {
        Self {
            host: self.host.clone(),
            shared: self.shared.clone(),
            fault: self.fault.clone(),
            root_telemetry: self.root_telemetry.clone(),
            telemetry,
        }
    }
    fn register(
        &self,
        kind: Option<HarnessEventType>,
        lane: Option<LaneId>,
        listener: Option<Arc<dyn EventListener>>,
    ) -> Arc<Delivery> {
        let delivery = Arc::new(Delivery {
            host: self.host.clone(),
            root_telemetry: self.root_telemetry.clone(),
            kind,
            lane,
            state: Mutex::new(DeliveryState {
                listener,
                ..Default::default()
            }),
        });
        // Only registrars contend on this gate. Append never scans or waits
        // for dead-Weak compaction while holding the publication mutex.
        let _registrar = self
            .shared
            .registrars
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let existing = self
            .shared
            .publication
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .deliveries
            .clone();
        let mut live: Vec<_> = existing
            .iter()
            .filter(|entry| entry.strong_count() != 0)
            .cloned()
            .collect();
        live.push(Arc::downgrade(&delivery));
        let replacement = Arc::new(live);
        let retired = {
            let mut publication = self
                .shared
                .publication
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            delivery
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .cursor = Some(publication.tail.clone());
            std::mem::replace(&mut publication.deliveries, replacement)
        };
        drop(retired);
        drop(existing);
        delivery
    }

    pub(crate) fn record_fault(&self) {
        let mut retained = self.fault.lock().unwrap_or_else(|p| p.into_inner());
        if retained.is_some() {
            return;
        }
        let event = FaultEvent {
            event_type: Default::default(),
            code: "HarnessFault".into(),
            message: "Harness operation failed".into(),
        };
        *retained = Some(event.clone());
        self.enqueue(
            HarnessEventType::Fault,
            &LaneId::new("main").expect("static lane"),
            HarnessEvent::Fault(event),
        );
    }
    /// Append in constant time, then eagerly buffer eligible events before
    /// returning. Only `flush` starts an idle listener after the caller line.
    pub fn enqueue(&self, kind: HarnessEventType, lane: &LaneId, event: HarnessEvent) {
        let node = Arc::new(Node {
            event: Some((event, Arc::downgrade(&self.telemetry), kind, lane.clone())),
            next: Mutex::new(None),
        });
        let old_tail = {
            let mut publication = self
                .shared
                .publication
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            *publication
                .tail
                .next
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(node.clone());
            std::mem::replace(&mut publication.tail, node)
        };
        drop(old_tail);
        self.shared.materialize(None);
    }
    pub fn flush(&self) {
        let deliveries = self.shared.materialize(None);
        for delivery in deliveries {
            delivery.schedule();
        }
    }
    /// Caller captures the snapshot and registers this buffer while holding the
    /// same publication line; the hub does not execute a snapshot callback.
    pub fn watch<T: FaultSnapshot + Clone + Send + Sync + 'static>(
        &self,
        mut snapshot: T,
        lane: Option<LaneId>,
    ) -> Arc<dyn WatchHandle<T>> {
        let fault = self.fault.lock().unwrap_or_else(|p| p.into_inner());
        snapshot.set_faulted(fault.is_some());
        Arc::new(Watch {
            snapshot,
            delivery: self.register(None, lane, None),
            shared: self.shared.clone(),
        })
    }
}
impl Events for EventHub {
    fn on(
        &self,
        kind: HarnessEventType,
        listener: Arc<dyn EventListener>,
    ) -> Arc<dyn EventSubscription> {
        Arc::new(Subscription(self.register(
            Some(kind),
            None,
            Some(listener),
        )))
    }
}
struct Subscription(Arc<Delivery>);
impl Drop for Subscription {
    fn drop(&mut self) {
        self.0.stop();
    }
}
impl EventSubscription for Subscription {
    fn unsubscribe(&self) {
        self.0.stop();
    }
}
struct Watch<T> {
    snapshot: T,
    delivery: Arc<Delivery>,
    shared: Arc<SharedLog>,
}
impl<T> Drop for Watch<T> {
    fn drop(&mut self) {
        self.delivery.stop();
    }
}
impl<T: Clone + Send + Sync + 'static> WatchHandle<T> for Watch<T> {
    fn snapshot(&self) -> T {
        self.snapshot.clone()
    }
    fn start(&self, listener: Arc<dyn EventListener>) {
        let mut candidate = Some(listener);
        {
            let mut state = self
                .delivery
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if !state.closed && state.listener.is_none() {
                state.listener = candidate.take();
            }
        }
        drop(candidate);
        self.shared.materialize(Some(&self.delivery));
        self.delivery.schedule();
    }
    fn unsubscribe(&self) {
        self.delivery.stop();
    }
}

pub(crate) trait FaultSnapshot {
    fn set_faulted(&mut self, value: bool);
}
impl FaultSnapshot for LaneSnapshot {
    fn set_faulted(&mut self, value: bool) {
        self.faulted = value;
    }
}
impl FaultSnapshot for SessionSnapshot {
    fn set_faulted(&mut self, value: bool) {
        self.faulted = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_ai_core::{BoxFuture, SpawnError};
    use std::{
        future::poll_fn,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll, Waker},
    };

    #[derive(Default)]
    struct ManualSpawner(Mutex<Vec<BoxFuture<'static, ()>>>);
    impl TaskSpawner for ManualSpawner {
        fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            self.0.lock().unwrap().push(task);
            Ok(())
        }
    }
    impl ManualSpawner {
        fn tick(&self) {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            for mut task in tasks {
                let mut context = Context::from_waker(Waker::noop());
                if task.as_mut().poll(&mut context).is_pending() {
                    self.0.lock().unwrap().push(task);
                }
            }
        }
    }
    fn hub() -> (EventHub, Arc<ManualSpawner>) {
        let host = Arc::new(ManualSpawner::default());
        (
            EventHub::new(host.clone(), Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT)),
            host,
        )
    }
    fn lane(name: &str) -> LaneId {
        LaneId::new(name).unwrap()
    }
    fn fault() -> HarnessEvent {
        HarnessEvent::Fault(FaultEvent {
            event_type: Default::default(),
            code: "test".into(),
            message: "test".into(),
        })
    }
    fn run_start(lane: &LaneId) -> HarnessEvent {
        HarnessEvent::RunStart(RunStartEvent {
            event_type: Default::default(),
            lane: lane.clone(),
            recovery: None,
            run_id: OperationId::new("run").unwrap(),
        })
    }
    fn run_abort(lane: &LaneId) -> HarnessEvent {
        HarnessEvent::RunAbort(RunAbortEvent {
            event_type: Default::default(),
            lane: lane.clone(),
            recovery: None,
            run_id: OperationId::new("run").unwrap(),
            steer: Vec::new(),
            follow_up: Vec::new(),
        })
    }
    struct Collector(Mutex<Vec<HarnessEvent>>);
    impl EventListener for Collector {
        fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
            Box::pin(async move {
                self.0.lock().unwrap().push(event);
                Ok(())
            })
        }
    }
    struct HeldListener {
        release: Arc<AtomicBool>,
        received: Mutex<Vec<HarnessEvent>>,
    }
    impl EventListener for HeldListener {
        fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
            Box::pin(async move {
                if matches!(event, HarnessEvent::Fault(_)) {
                    poll_fn(|_| {
                        if self.release.load(Ordering::SeqCst) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                }
                self.received.lock().unwrap().push(event);
                Ok(())
            })
        }
    }

    #[test]
    fn enqueue_buffers_run_abort_and_start_before_flush_for_running_listener() {
        let (hub, host) = hub();
        let lane = lane("main");
        let listener = Arc::new(HeldListener {
            release: Arc::new(AtomicBool::new(false)),
            received: Mutex::new(Vec::new()),
        });
        let watch = hub.watch(false_snapshot(), Some(lane.clone()));
        watch.start(listener.clone());
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        hub.flush();
        assert_eq!(host.0.lock().unwrap().len(), 1);

        // Pause the producer after both publications and before its Flush,
        // matching the await windows in abort_run and publication::apply.
        std::thread::scope(|scope| {
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            let producer_hub = hub.clone();
            let producer_lane = lane.clone();
            let producer = scope.spawn(move || {
                producer_hub.enqueue(
                    HarnessEventType::RunAbort,
                    &producer_lane,
                    run_abort(&producer_lane),
                );
                producer_hub.enqueue(
                    HarnessEventType::RunStart,
                    &producer_lane,
                    run_start(&producer_lane),
                );
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                producer_hub.flush();
            });
            entered_rx.recv().unwrap();
            assert_eq!(watch.snapshot(), false_snapshot());
            listener.release.store(true, Ordering::SeqCst);
            host.tick();
            let received = listener.received.lock().unwrap();
            assert!(matches!(
                &received[..],
                [
                    HarnessEvent::Fault(_),
                    HarnessEvent::RunAbort(_),
                    HarnessEvent::RunStart(_)
                ]
            ));
            drop(received);
            resume_tx.send(()).unwrap();
            producer.join().unwrap();
        });
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Snapshot(bool);
    impl FaultSnapshot for Snapshot {
        fn set_faulted(&mut self, value: bool) {
            self.0 = value;
        }
    }
    fn false_snapshot() -> Snapshot {
        Snapshot(false)
    }

    #[test]
    fn register_and_append_obey_both_publication_orders() {
        let (hub, _) = hub();
        let lane = lane("main");
        let before = Arc::new(Collector(Mutex::new(Vec::new())));
        let before_handle = hub.on(HarnessEventType::RunStart, before.clone());
        hub.enqueue(HarnessEventType::RunStart, &lane, run_start(&lane));
        let after = Arc::new(Collector(Mutex::new(Vec::new())));
        let after_handle = hub.on(HarnessEventType::RunStart, after.clone());
        hub.flush();
        assert_eq!(before.0.lock().unwrap().len(), 1);
        assert!(after.0.lock().unwrap().is_empty());
        hub.enqueue(HarnessEventType::RunStart, &lane, run_start(&lane));
        hub.flush();
        assert_eq!(before.0.lock().unwrap().len(), 2);
        assert_eq!(after.0.lock().unwrap().len(), 1);
        drop((before_handle, after_handle));
    }

    #[test]
    fn watch_start_replays_buffer_after_its_captured_snapshot() {
        let (hub, _) = hub();
        let lane = lane("main");
        let watch = hub.watch(false_snapshot(), Some(lane.clone()));
        hub.enqueue(HarnessEventType::RunStart, &lane, run_start(&lane));
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        assert_eq!(watch.snapshot(), Snapshot(false));
        watch.start(listener.clone());
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_)]
        ));
        hub.record_fault();
        let after_fault = hub.watch(false_snapshot(), None);
        assert_eq!(after_fault.snapshot(), Snapshot(true));
        hub.flush();
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_), HarnessEvent::Fault(_)]
        ));
    }

    #[test]
    fn lane_filter_keeps_global_config_but_skips_other_lane_events() {
        let (hub, _) = hub();
        let watch = hub.watch(false_snapshot(), Some(lane("a")));
        let other = lane("b");
        hub.enqueue(HarnessEventType::RunStart, &other, run_start(&other));
        hub.enqueue(
            HarnessEventType::ConfigUpdate,
            &other,
            HarnessEvent::GlobalConfigUpdate(GlobalConfigUpdateEvent {
                event_type: Default::default(),
                property: GlobalConfigProperty::Tools,
            }),
        );
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        watch.start(listener.clone());
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::GlobalConfigUpdate(_)]
        ));
    }

    struct Reentrant {
        hub: EventHub,
        received: Mutex<Vec<&'static str>>,
    }
    impl EventListener for Reentrant {
        fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
            Box::pin(async move {
                if matches!(event, HarnessEvent::Fault(_)) {
                    let lane = lane("main");
                    self.hub
                        .enqueue(HarnessEventType::RunStart, &lane, run_start(&lane));
                    self.hub.flush();
                    self.received.lock().unwrap().push("fault");
                } else {
                    self.received.lock().unwrap().push("start");
                }
                Ok(())
            })
        }
    }
    #[test]
    fn nested_flush_and_concurrent_flush_do_not_lose_the_running_latch() {
        let (hub, _) = hub();
        let lane = lane("main");
        let listener = Arc::new(Reentrant {
            hub: hub.clone(),
            received: Mutex::new(Vec::new()),
        });
        let watch = hub.watch(false_snapshot(), Some(lane.clone()));
        watch.start(listener.clone());
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        std::thread::scope(|scope| {
            let first = scope.spawn(|| hub.flush());
            let second = scope.spawn(|| hub.flush());
            first.join().unwrap();
            second.join().unwrap();
        });
        assert_eq!(&listener.received.lock().unwrap()[..], &["fault", "start"]);
    }

    #[test]
    fn append_and_flush_while_listener_is_running_preserve_the_last_wake() {
        let (hub, host) = hub();
        let lane = lane("main");
        let listener = Arc::new(HeldListener {
            release: Arc::new(AtomicBool::new(false)),
            received: Mutex::new(Vec::new()),
        });
        let watch = hub.watch(false_snapshot(), None);
        watch.start(listener.clone());
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        hub.flush();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    hub.enqueue(HarnessEventType::RunStart, &lane, run_start(&lane));
                    hub.flush();
                })
                .join()
                .unwrap();
        });
        listener.release.store(true, Ordering::SeqCst);
        host.tick();
        assert!(matches!(
            &listener.received.lock().unwrap()[..],
            [HarnessEvent::Fault(_), HarnessEvent::RunStart(_)]
        ));
    }

    #[test]
    fn dropped_watch_closes_worker_held_delivery_and_compacts_dead_entries() {
        let (hub, host) = hub();
        let lane = lane("main");
        let listener = Arc::new(HeldListener {
            release: Arc::new(AtomicBool::new(false)),
            received: Mutex::new(Vec::new()),
        });
        let watch = hub.watch(false_snapshot(), None);
        watch.start(listener.clone());
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        hub.flush();
        let weak = hub.shared.publication.lock().unwrap().deliveries[0].clone();
        drop(watch);
        assert!(weak.upgrade().unwrap().state.lock().unwrap().closed);
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        hub.flush();
        listener.release.store(true, Ordering::SeqCst);
        host.tick();
        assert_eq!(listener.received.lock().unwrap().len(), 1);
        assert!(weak.upgrade().is_none());
        let fresh = hub.watch(false_snapshot(), None);
        assert_eq!(hub.shared.publication.lock().unwrap().deliveries.len(), 1);
        drop(fresh);
    }

    #[test]
    fn unsubscribe_clears_queued_events_while_worker_holds_delivery() {
        let (hub, host) = hub();
        let lane = lane("main");
        let listener = Arc::new(HeldListener {
            release: Arc::new(AtomicBool::new(false)),
            received: Mutex::new(Vec::new()),
        });
        let subscription = hub.on(HarnessEventType::Fault, listener.clone());
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        hub.flush();
        hub.enqueue(HarnessEventType::Fault, &lane, fault());
        let delivery = hub.shared.publication.lock().unwrap().deliveries[0]
            .upgrade()
            .unwrap();
        assert_eq!(delivery.state.lock().unwrap().queue.len(), 1);
        subscription.unsubscribe();
        let state = delivery.state.lock().unwrap();
        assert!(state.closed && state.queue.is_empty() && state.cursor.is_none());
        drop(state);
        drop(delivery);
        listener.release.store(true, Ordering::SeqCst);
        host.tick();
        assert_eq!(listener.received.lock().unwrap().len(), 1);
    }

    #[test]
    fn unsubscribe_during_traversal_retires_cursor_after_materializer_unlock() {
        let (hub, _) = hub();
        let watch = hub.watch(false_snapshot(), None);
        let first = hub.shared.publication.lock().unwrap().deliveries[0]
            .upgrade()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .cursor
            .as_ref()
            .unwrap()
            .clone();
        let weak_first = Arc::downgrade(&first);
        drop(first);

        // Leave one node unmaterialized so a concurrent stop can remove the
        // delivery's only persistent reference to the old cursor.
        let node = Arc::new(Node {
            event: Some((
                fault(),
                Arc::downgrade(&hub.telemetry),
                HarnessEventType::Fault,
                lane("main"),
            )),
            next: Mutex::new(None),
        });
        let old_tail = {
            let mut publication = hub.shared.publication.lock().unwrap();
            *publication.tail.next.lock().unwrap() = Some(node.clone());
            std::mem::replace(&mut publication.tail, node)
        };
        drop(old_tail);

        let reached = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        let before_seen = Arc::new(AtomicBool::new(false));
        let after_seen = Arc::new(AtomicBool::new(false));
        let alive_after_step = Arc::new(AtomicBool::new(false));
        *hub.shared.traversal_probe.lock().unwrap() = Some(Arc::new({
            let reached = reached.clone();
            let resume = resume.clone();
            let before_seen = before_seen.clone();
            let after_seen = after_seen.clone();
            let alive_after_step = alive_after_step.clone();
            let weak_first = weak_first.clone();
            move |after| {
                if !after && !before_seen.swap(true, Ordering::SeqCst) {
                    reached.wait();
                    resume.wait();
                } else if after && !after_seen.swap(true, Ordering::SeqCst) {
                    alive_after_step.store(weak_first.upgrade().is_some(), Ordering::SeqCst);
                }
            }
        }));
        std::thread::scope(|scope| {
            let shared = hub.shared.clone();
            let worker = scope.spawn(move || shared.materialize(None));
            reached.wait();
            watch.unsubscribe();
            resume.wait();
            worker.join().unwrap();
        });
        assert!(alive_after_step.load(Ordering::SeqCst));
        assert!(weak_first.upgrade().is_none());
        drop(hub.shared.traversal_probe.lock().unwrap().take());
    }

    #[test]
    fn dormant_filtered_watch_advances_and_long_link_drop_is_iterative() {
        let (hub, _) = hub();
        let watch = hub.watch(false_snapshot(), Some(lane("a")));
        let other = lane("b");
        hub.enqueue(HarnessEventType::RunStart, &other, run_start(&other));
        let first = Arc::downgrade(&hub.shared.publication.lock().unwrap().tail);
        for _ in 0..256 {
            hub.enqueue(HarnessEventType::RunStart, &other, run_start(&other));
        }
        assert!(first.upgrade().is_none());
        let delivery = hub.shared.publication.lock().unwrap().deliveries[0]
            .upgrade()
            .unwrap();
        assert!(delivery.state.lock().unwrap().queue.is_empty());
        drop(delivery);
        drop(watch);

        let head = Arc::new(Node {
            event: None,
            next: Mutex::new(None),
        });
        let mut tail = head.clone();
        for _ in 0..50_000 {
            let next = Arc::new(Node {
                event: None,
                next: Mutex::new(None),
            });
            *tail.next.lock().unwrap() = Some(next.clone());
            tail = next;
        }
        drop(tail);
        drop(head);
    }
}
