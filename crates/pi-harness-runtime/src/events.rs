//! Ordered passive subscriptions and snapshot-first buffers.
use pi_agent_schema::*;
use pi_ai_core::TaskSpawner;
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, Weak},
};

#[derive(Clone)]
pub(crate) struct EventHub {
    host: Arc<dyn TaskSpawner>,
    shared: Arc<SharedLog>,
    telemetry: crate::owner::Telemetry,
    root_telemetry: crate::owner::Telemetry,
    fault: Arc<Mutex<Option<FaultEvent>>>,
}
// Append and registration share one publication order point; activation can
// also clear a completed Pending chain there. Registration publishes its
// cursor and weak entry in the same transaction.
struct SharedLog {
    publication: Mutex<Publication>,
    registrars: Mutex<()>,
    materializers: Mutex<()>,
    #[cfg(test)]
    traversal_probe: Mutex<Option<TraversalProbe>>,
    #[cfg(test)]
    pending_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
#[cfg(test)]
type TraversalProbe = Arc<dyn Fn(bool) + Send + Sync>;
struct Publication {
    tail: Arc<Node>,
    deliveries: Arc<Vec<Weak<Delivery>>>,
    // Kept until every Pending entry in this chain is Ready. Later nodes keep
    // weak links to the entry current at their own publication point.
    latest_pending: Option<Arc<PendingBarrier>>,
}
struct Node {
    event: Option<BufferedEvent>,
    next: Mutex<Option<Arc<Node>>>,
    predecessor: Mutex<Option<Weak<PendingBarrier>>>,
    readiness: Mutex<bool>,
    ready_changed: Condvar,
}
struct PendingBarrier {
    node: Arc<Node>,
    previous: Mutex<Option<Arc<PendingBarrier>>>,
}
type BufferedEvent = (
    HarnessEvent,
    Weak<dyn pi_telemetry::TelemetryContext>,
    HarnessEventType,
    LaneId,
);
impl Node {
    fn new(event: Option<BufferedEvent>, ready: bool) -> Self {
        Self {
            event,
            next: Mutex::new(None),
            predecessor: Mutex::new(None),
            readiness: Mutex::new(ready),
            ready_changed: Condvar::new(),
        }
    }

    fn next(&self) -> Option<Arc<Self>> {
        self.next.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn unresolved_predecessor(&self, retained: &mut Vec<Arc<PendingBarrier>>) -> Option<Arc<Self>> {
        let root = self
            .predecessor
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)?;
        // Keep the entire strong predecessor chain alive until after the
        // materializer unlocks, even if activation clears the hub's root.
        retained.push(root.clone());
        let mut barrier = Some(root);
        while let Some(entry) = barrier {
            if !entry.node.is_ready() {
                return Some(entry.node.clone());
            }
            barrier = entry
                .previous
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
        }
        None
    }

    fn is_ready(&self) -> bool {
        *self.readiness.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn wait_ready(&self) {
        let mut ready = self.readiness.lock().unwrap_or_else(|p| p.into_inner());
        while !*ready {
            ready = self
                .ready_changed
                .wait(ready)
                .unwrap_or_else(|p| p.into_inner());
        }
    }

    #[allow(dead_code)] // Activation has no live caller until the G1 source unit.
    fn mark_ready(&self) {
        let mut ready = self.readiness.lock().unwrap_or_else(|p| p.into_inner());
        *ready = true;
        drop(ready);
        self.ready_changed.notify_all();
    }
}

// This is built before entering a critical publication line. Appending it
// only links the preallocated node and barrier and replaces the hub tail.
#[allow(dead_code)] // The routed G1 caller belongs to a later source unit.
#[must_use = "a prepared critical event must be appended or dropped"]
pub(crate) struct PreparedPendingEvent {
    shared: Arc<SharedLog>,
    barrier: Arc<PendingBarrier>,
}

#[allow(dead_code)] // The routed G1 caller belongs to a later source unit.
impl PreparedPendingEvent {
    pub(crate) fn append(self) -> PendingActivation {
        let retired_tail = self.shared.append_pending(self.barrier.clone());
        PendingActivation {
            barrier: Some(self.barrier),
            retired_tail: Some(retired_tail),
            _shared: self.shared,
        }
    }
}

// This unused seam resolves an abandoned published node on Drop, including
// unwind. A live G1 caller cannot rely on Drop alone: if it unwinds before
// the control handoff, marking Ready would expose the Event too early. That
// caller must transfer service/token ownership to an independent fallback or
// publish the exact HostFault decision, then release Harness/route guards
// before activation, as required by E1157.
#[allow(dead_code)] // The routed G1 caller belongs to a later source unit.
#[must_use = "a published critical event must be activated after its handoff"]
pub(crate) struct PendingActivation {
    barrier: Option<Arc<PendingBarrier>>,
    retired_tail: Option<Arc<Node>>,
    _shared: Arc<SharedLog>,
}

#[allow(dead_code)] // The routed G1 caller belongs to a later source unit.
impl PendingActivation {
    /// Complete after the bounded control handoff and lock release.
    pub(crate) fn activate(mut self) {
        self.resolve();
    }

    fn resolve(&mut self) {
        if let Some(barrier) = self.barrier.take() {
            barrier.node.mark_ready();
            self._shared.compact_pending();
        }
        drop(self.retired_tail.take());
    }
}

impl Drop for PendingActivation {
    fn drop(&mut self) {
        self.resolve();
    }
}
impl Drop for PendingBarrier {
    fn drop(&mut self) {
        // The strong predecessor chain can be long when several critical
        // publications resolve out of order. Retire its unshared suffix
        // iteratively, outside the publication mutex.
        let mut previous = self
            .previous
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        while let Some(link) = previous {
            match Arc::try_unwrap(link) {
                Ok(mut entry) => {
                    previous = entry
                        .previous
                        .get_mut()
                        .unwrap_or_else(|p| p.into_inner())
                        .take();
                    drop(entry);
                }
                Err(shared) => {
                    drop(shared);
                    break;
                }
            }
        }
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
    fn append(&self, node: Arc<Node>) -> Arc<Node> {
        let mut publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        *node.predecessor.lock().unwrap_or_else(|p| p.into_inner()) =
            publication.latest_pending.as_ref().map(Arc::downgrade);
        *publication
            .tail
            .next
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(node.clone());
        std::mem::replace(&mut publication.tail, node)
    }

    fn append_pending(&self, barrier: Arc<PendingBarrier>) -> Arc<Node> {
        let node = barrier.node.clone();
        let mut publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        let previous = publication.latest_pending.take();
        *node.predecessor.lock().unwrap_or_else(|p| p.into_inner()) =
            previous.as_ref().map(Arc::downgrade);
        *barrier.previous.lock().unwrap_or_else(|p| p.into_inner()) = previous;
        *publication
            .tail
            .next
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(node.clone());
        let retired_tail = std::mem::replace(&mut publication.tail, node);
        publication.latest_pending = Some(barrier);
        retired_tail
    }

    fn compact_pending(&self) {
        let latest = self
            .publication
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .latest_pending
            .clone();
        let Some(latest) = latest else {
            return;
        };
        let mut scan = Some(latest.clone());
        while let Some(entry) = scan {
            if !entry.node.is_ready() {
                return;
            }
            scan = entry
                .previous
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
        }
        let retired = {
            let mut publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
            if publication
                .latest_pending
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &latest))
            {
                publication.latest_pending.take()
            } else {
                None
            }
        };
        drop(retired);
    }

    fn materialize(&self, only: Option<&Arc<Delivery>>) -> Vec<Arc<Delivery>> {
        let mut materializer = Some(self.materializers.lock().unwrap_or_else(|p| p.into_inner()));
        let (tail, registry) = {
            let publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
            (publication.tail.clone(), publication.deliveries.clone())
        };
        let deliveries: Vec<_> = match only {
            Some(delivery) => vec![delivery.clone()],
            None => registry.iter().filter_map(Weak::upgrade).collect(),
        };
        // Keep each initial cursor so a later materializer that passes this
        // call's fixed tail can be recognized after a Pending wait. The check
        // traverses only the finite prefix captured at entry.
        let origins: Vec<_> = deliveries
            .iter()
            .map(|delivery| {
                delivery
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .cursor
                    .clone()
            })
            .collect();
        loop {
            let mut retired = Vec::new();
            let mut retained_barriers = Vec::new();
            let mut discarded = Vec::new();
            let mut pending = None;
            for (delivery, origin) in deliveries.iter().zip(&origins) {
                let Some(start) = delivery
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .cursor
                    .clone()
                else {
                    continue;
                };
                if origin
                    .as_ref()
                    .is_some_and(|origin| Self::cursor_beyond_target(origin, &start, &tail))
                {
                    continue;
                }
                let mut cursor = start.clone();
                // The delivery can close concurrently. Keep the original
                // cursor alive until after materializer release.
                retired.push(Some(start));
                // A registrar starts at the current tail and excludes that
                // Event, but an unresolved Pending cursor still bars all of
                // its later successors.
                if !Arc::ptr_eq(&cursor, &tail) && !cursor.is_ready() {
                    pending = Some(cursor);
                    break;
                }
                let mut buffered = Vec::new();
                while !Arc::ptr_eq(&cursor, &tail) {
                    let next = cursor.next().expect("tail must follow registered cursor");
                    if let Some(blocked) = next.unresolved_predecessor(&mut retained_barriers) {
                        pending = Some(blocked);
                        break;
                    }
                    if !next.is_ready() {
                        pending = Some(next);
                        break;
                    }
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
                if pending.is_some() {
                    break;
                }
            }
            drop(materializer.take());
            drop(retired);
            drop(retained_barriers);
            drop(discarded);
            if let Some(node) = pending {
                // Neither this lock nor a publication/delivery lock may be
                // held while the critical owner's handoff is outstanding.
                #[cfg(test)]
                {
                    let probe = self.pending_probe.lock().unwrap().clone();
                    if let Some(probe) = probe {
                        probe();
                    }
                }
                node.wait_ready();
                materializer = Some(self.materializers.lock().unwrap_or_else(|p| p.into_inner()));
                continue;
            }
            return deliveries;
        }
    }

    fn cursor_beyond_target(origin: &Arc<Node>, cursor: &Arc<Node>, target: &Arc<Node>) -> bool {
        let mut scan = origin.clone();
        loop {
            if Arc::ptr_eq(&scan, cursor) {
                return false;
            }
            if Arc::ptr_eq(&scan, target) {
                return true;
            }
            scan = scan.next().expect("target must follow original cursor");
        }
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
        let sentinel = Arc::new(Node::new(None, true));
        Self {
            host,
            root_telemetry: telemetry.clone(),
            telemetry,
            shared: Arc::new(SharedLog {
                publication: Mutex::new(Publication {
                    tail: sentinel,
                    deliveries: Arc::new(Vec::new()),
                    latest_pending: None,
                }),
                registrars: Mutex::new(()),
                materializers: Mutex::new(()),
                #[cfg(test)]
                traversal_probe: Mutex::new(None),
                #[cfg(test)]
                pending_probe: Mutex::new(None),
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
        // This guard spans ordinary enqueue. A future G1 Pending handoff must
        // not need the fault mutex before resolving the node that enqueue may
        // wait behind.
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
    /// Build a critical node before entering its publication line. The caller
    /// must retain the returned activation token through its control handoff.
    #[allow(dead_code)] // The routed G1 caller belongs to a later source unit.
    pub(crate) fn prepare_pending(
        &self,
        kind: HarnessEventType,
        lane: &LaneId,
        event: HarnessEvent,
    ) -> PreparedPendingEvent {
        let node = Arc::new(Node::new(
            Some((event, Arc::downgrade(&self.telemetry), kind, lane.clone())),
            false,
        ));
        PreparedPendingEvent {
            shared: self.shared.clone(),
            barrier: Arc::new(PendingBarrier {
                node,
                previous: Mutex::new(None),
            }),
        }
    }
    /// Append in constant time, then eagerly buffer eligible events before
    /// returning. Only `flush` starts an idle listener after the caller line.
    pub fn enqueue(&self, kind: HarnessEventType, lane: &LaneId, event: HarnessEvent) {
        let node = Arc::new(Node::new(
            Some((event, Arc::downgrade(&self.telemetry), kind, lane.clone())),
            true,
        ));
        let old_tail = self.shared.append(node);
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
    fn append_ready_without_materializing(
        hub: &EventHub,
        kind: HarnessEventType,
        lane: &LaneId,
        event: HarnessEvent,
    ) {
        let node = Arc::new(Node::new(
            Some((event, Arc::downgrade(&hub.telemetry), kind, lane.clone())),
            true,
        ));
        drop(hub.shared.append(node));
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
    fn pending_blocks_flush_watch_start_and_later_enqueue_without_holding_log_locks() {
        let (hub, _) = hub();
        let lane = lane("main");
        let before = Arc::new(Collector(Mutex::new(Vec::new())));
        let before_handle = hub.on(HarnessEventType::RunStart, before.clone());
        let before_watch = hub.watch(false_snapshot(), None);
        let before_listener = Arc::new(Collector(Mutex::new(Vec::new())));
        before_watch.start(before_listener.clone());
        let dormant_watch = hub.watch(false_snapshot(), None);
        let dormant_listener = Arc::new(Collector(Mutex::new(Vec::new())));

        let pending = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        let after_watch = hub.watch(false_snapshot(), None);
        let after_listener = Arc::new(Collector(Mutex::new(Vec::new())));

        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            waiting_tx.send(()).unwrap();
        }));
        std::thread::scope(|scope| {
            let flush_hub = hub.clone();
            let flush = scope.spawn(move || flush_hub.flush());
            waiting_rx.recv().unwrap();

            let start = scope.spawn(|| dormant_watch.start(dormant_listener.clone()));
            waiting_rx.recv().unwrap();

            let enqueue_hub = hub.clone();
            let enqueue_lane = lane.clone();
            let enqueue = scope.spawn(move || {
                enqueue_hub.enqueue(
                    HarnessEventType::RunAbort,
                    &enqueue_lane,
                    run_abort(&enqueue_lane),
                );
            });
            waiting_rx.recv().unwrap();

            assert!(hub.shared.materializers.try_lock().is_ok());
            assert!(hub.shared.publication.try_lock().is_ok());
            assert!(before.0.lock().unwrap().is_empty());
            assert!(before_listener.0.lock().unwrap().is_empty());
            assert!(dormant_listener.0.lock().unwrap().is_empty());
            assert!(after_listener.0.lock().unwrap().is_empty());

            pending.activate();
            flush.join().unwrap();
            start.join().unwrap();
            enqueue.join().unwrap();
        });
        after_watch.start(after_listener.clone());
        hub.flush();
        assert!(matches!(
            &before.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_)]
        ));
        assert!(matches!(
            &before_listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_), HarnessEvent::RunAbort(_)]
        ));
        assert!(matches!(
            &dormant_listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_), HarnessEvent::RunAbort(_)]
        ));
        assert!(matches!(
            &after_listener.0.lock().unwrap()[..],
            [HarnessEvent::RunAbort(_)]
        ));
        drop(before_handle);
    }

    #[test]
    fn delivery_registered_at_pending_tail_cannot_observe_a_later_event_early() {
        let (hub, _) = hub();
        let lane = lane("main");
        let pending = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        // This is the only delivery. Its initial cursor is the Pending node.
        let watch = hub.watch(false_snapshot(), None);
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            waiting_tx.send(()).unwrap();
        }));
        std::thread::scope(|scope| {
            let enqueue_hub = hub.clone();
            let enqueue_lane = lane.clone();
            let enqueue = scope.spawn(move || {
                enqueue_hub.enqueue(
                    HarnessEventType::RunAbort,
                    &enqueue_lane,
                    run_abort(&enqueue_lane),
                );
            });
            assert!(
                waiting_rx
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .is_ok()
            );
            let start = scope.spawn(|| watch.start(listener.clone()));
            assert!(
                waiting_rx
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .is_ok()
            );
            assert!(listener.0.lock().unwrap().is_empty());
            pending.activate();
            enqueue.join().unwrap();
            start.join().unwrap();
        });
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunAbort(_)]
        ));
    }

    #[test]
    fn e1189_late_registration_after_ready_successor_respects_earlier_pending() {
        let (hub, _) = hub();
        let lane = lane("main");
        let pending = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();

        // With no deliveries yet, ordinary enqueue can append a Ready successor
        // and return while the earlier Pending node remains unresolved.
        hub.enqueue(HarnessEventType::RunAbort, &lane, run_abort(&lane));
        let watch = hub.watch(false_snapshot(), None);
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            waiting_tx.send(()).unwrap();
        }));
        std::thread::scope(|scope| {
            let enqueue_hub = hub.clone();
            let enqueue_lane = lane.clone();
            let third = scope.spawn(move || {
                enqueue_hub.enqueue(
                    HarnessEventType::RunStart,
                    &enqueue_lane,
                    run_start(&enqueue_lane),
                );
            });
            let third_waited = waiting_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok();
            let start = scope.spawn(|| watch.start(listener.clone()));
            let start_waited = waiting_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok();
            let visible_before_activation = !listener.0.lock().unwrap().is_empty();
            pending.activate();
            third.join().unwrap();
            start.join().unwrap();
            assert!(third_waited && start_waited);
            assert!(
                !visible_before_activation,
                "a later Ready event overtook an unresolved Pending predecessor"
            );
        });
        hub.flush();
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_)]
        ));
    }

    #[test]
    fn ready_prefix_reaches_running_listener_before_later_pending_activates() {
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

        // Reproduce a Ready append just before its eager materialization.
        // The concurrent Flush can see E1 and P together in one fixed tail.
        append_ready_without_materializing(
            &hub,
            HarnessEventType::RunAbort,
            &lane,
            run_abort(&lane),
        );
        let pending = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            waiting_tx.send(()).unwrap();
        }));
        std::thread::scope(|scope| {
            let flush_hub = hub.clone();
            let flush = scope.spawn(move || flush_hub.flush());
            waiting_rx.recv().unwrap();

            listener.release.store(true, Ordering::SeqCst);
            host.tick();
            assert!(matches!(
                &listener.received.lock().unwrap()[..],
                [HarnessEvent::Fault(_), HarnessEvent::RunAbort(_)]
            ));
            pending.activate();
            flush.join().unwrap();
        });
        assert!(matches!(
            &listener.received.lock().unwrap()[..],
            [
                HarnessEvent::Fault(_),
                HarnessEvent::RunAbort(_),
                HarnessEvent::RunStart(_)
            ]
        ));
    }

    #[test]
    fn later_pending_ready_cannot_release_an_earlier_pending_barrier() {
        let (hub, _) = hub();
        let lane = lane("main");
        let first = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        hub.enqueue(HarnessEventType::RunAbort, &lane, run_abort(&lane));
        let second = hub
            .prepare_pending(HarnessEventType::RunAbort, &lane, run_abort(&lane))
            .append();
        hub.enqueue(HarnessEventType::RunAbort, &lane, run_abort(&lane));
        let watch = hub.watch(false_snapshot(), None);
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            waiting_tx.send(()).unwrap();
        }));
        std::thread::scope(|scope| {
            let enqueue_hub = hub.clone();
            let enqueue_lane = lane.clone();
            let later = scope.spawn(move || {
                enqueue_hub.enqueue(
                    HarnessEventType::RunStart,
                    &enqueue_lane,
                    run_start(&enqueue_lane),
                );
            });
            waiting_rx.recv().unwrap();
            second.activate();
            waiting_rx.recv().unwrap();
            let start = scope.spawn(|| watch.start(listener.clone()));
            waiting_rx.recv().unwrap();
            assert!(listener.0.lock().unwrap().is_empty());
            first.activate();
            later.join().unwrap();
            start.join().unwrap();
        });
        assert!(
            hub.shared
                .publication
                .lock()
                .unwrap()
                .latest_pending
                .is_none()
        );
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_)]
        ));
    }

    #[test]
    fn flush_pinned_at_first_pending_does_not_wait_for_later_pending() {
        let (hub, _) = hub();
        let lane = lane("main");
        let watch = hub.watch(false_snapshot(), None);
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        watch.start(listener.clone());
        let first = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let pause_first = AtomicBool::new(true);
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            if pause_first.swap(false, Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        }));
        std::thread::scope(|scope| {
            let flush_hub = hub.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let flush = scope.spawn(move || {
                flush_hub.flush();
                done_tx.send(()).unwrap();
            });
            entered_rx.recv().unwrap();
            let later = hub
                .prepare_pending(HarnessEventType::RunAbort, &lane, run_abort(&lane))
                .append();
            first.activate();
            release_tx.send(()).unwrap();
            let finished_before_later_activation = done_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok();
            later.activate();
            flush.join().unwrap();
            assert!(finished_before_later_activation);
            assert!(matches!(
                &listener.0.lock().unwrap()[..],
                [HarnessEvent::RunStart(_)]
            ));
        });
        hub.flush();
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_), HarnessEvent::RunAbort(_)]
        ));
    }

    #[test]
    fn resolved_pending_chain_reclaims_its_entries_iteratively() {
        let (hub, _) = hub();
        let lane = lane("main");
        let mut pending = Vec::new();
        for _ in 0..20_000 {
            pending.push(
                hub.prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
                    .append(),
            );
        }
        let first = Arc::downgrade(pending[0].barrier.as_ref().unwrap());
        for token in pending {
            token.activate();
        }
        assert!(
            hub.shared
                .publication
                .lock()
                .unwrap()
                .latest_pending
                .is_none()
        );
        assert!(first.upgrade().is_none());
    }

    fn pinned_tail_race(advance_with_other_flush: bool) -> Vec<HarnessEvent> {
        let (hub, _) = hub();
        let lane = lane("main");
        let watch = hub.watch(false_snapshot(), None);
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        watch.start(listener.clone());
        let pending = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }));
        std::thread::scope(|scope| {
            let flush_hub = hub.clone();
            let first = scope.spawn(move || flush_hub.flush());
            let entered = entered_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok();
            if entered {
                // This append occurs after the first Flush captured Pending
                // as its target. It must not extend that call's target.
                append_ready_without_materializing(
                    &hub,
                    HarnessEventType::RunAbort,
                    &lane,
                    run_abort(&lane),
                );
            }
            pending.activate();
            if entered && advance_with_other_flush {
                // The second materializer can advance the cursor beyond the
                // first call's pinned target while that call is parked.
                hub.flush();
            }
            release_tx.send(()).ok();
            first.join().unwrap();
            assert!(entered, "first Flush did not reach Pending");
        });
        let observed = listener.0.lock().unwrap().clone();
        if !advance_with_other_flush {
            assert!(matches!(&observed[..], [HarnessEvent::RunStart(_)]));
            hub.flush();
        }
        listener.0.lock().unwrap().clone()
    }

    #[test]
    fn flush_pinned_at_pending_does_not_chase_later_append() {
        assert!(matches!(
            &pinned_tail_race(false)[..],
            [HarnessEvent::RunStart(_), HarnessEvent::RunAbort(_)]
        ));
    }

    #[test]
    fn blocked_flush_accepts_cursor_advanced_past_its_pinned_tail() {
        assert!(matches!(
            &pinned_tail_race(true)[..],
            [HarnessEvent::RunStart(_), HarnessEvent::RunAbort(_)]
        ));
    }

    #[test]
    fn dropped_published_token_resolves_pending_after_unwind() {
        let (hub, _) = hub();
        let lane = lane("main");
        let listener = Arc::new(Collector(Mutex::new(Vec::new())));
        let handle = hub.on(HarnessEventType::RunStart, listener.clone());
        let token = hub
            .prepare_pending(HarnessEventType::RunStart, &lane, run_start(&lane))
            .append();
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *hub.shared.pending_probe.lock().unwrap() = Some(Arc::new(move || {
            waiting_tx.send(()).unwrap();
        }));
        std::thread::scope(|scope| {
            let flush_hub = hub.clone();
            let flush = scope.spawn(move || flush_hub.flush());
            waiting_rx.recv().unwrap();
            // This is after a simulated handoff. A live G1 caller must also
            // resolve the control service before an unwind can release Ready.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _token = token;
                panic!("simulate cancellation after control handoff");
            }));
            assert!(result.is_err());
            flush.join().unwrap();
        });
        assert!(matches!(
            &listener.0.lock().unwrap()[..],
            [HarnessEvent::RunStart(_)]
        ));
        drop(handle);
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
    fn activated_pending_event_keeps_reentrant_flush_order() {
        let (hub, _) = hub();
        let lane = lane("main");
        let listener = Arc::new(Reentrant {
            hub: hub.clone(),
            received: Mutex::new(Vec::new()),
        });
        let watch = hub.watch(false_snapshot(), Some(lane.clone()));
        watch.start(listener.clone());
        hub.prepare_pending(HarnessEventType::Fault, &lane, fault())
            .append()
            .activate();
        hub.flush();
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
        let node = Arc::new(Node::new(
            Some((
                fault(),
                Arc::downgrade(&hub.telemetry),
                HarnessEventType::Fault,
                lane("main"),
            )),
            true,
        ));
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

        let head = Arc::new(Node::new(None, true));
        let mut tail = head.clone();
        for _ in 0..50_000 {
            let next = Arc::new(Node::new(None, true));
            *tail.next.lock().unwrap() = Some(next.clone());
            tail = next;
        }
        drop(tail);
        drop(head);
    }
}
