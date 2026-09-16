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
    deliveries: Arc<Mutex<Vec<Weak<Delivery>>>>,
    telemetry: crate::owner::Telemetry,
    root_telemetry: crate::owner::Telemetry,
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
    queue: VecDeque<(
        HarnessEvent,
        Weak<dyn pi_telemetry::TelemetryContext>,
        HarnessEventType,
        LaneId,
    )>,
}
impl Delivery {
    fn stop(&self) {
        let discarded = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.closed = true;
            (state.listener.take(), std::mem::take(&mut state.queue))
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
        Self {
            host,
            root_telemetry: telemetry.clone(),
            telemetry,
            deliveries: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(crate) fn with_context(&self, telemetry: crate::owner::Telemetry) -> Self {
        Self {
            host: self.host.clone(),
            deliveries: self.deliveries.clone(),
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
        self.deliveries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Arc::downgrade(&delivery));
        delivery
    }
    /// Called within publication ordering. This only buffers; `flush` starts
    /// passive work after the mutation line has been released.
    pub fn enqueue(&self, kind: HarnessEventType, lane: &LaneId, event: HarnessEvent) {
        let deliveries: Vec<_> = self
            .deliveries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for delivery in deliveries {
            if delivery.kind.is_some_and(|k| k != kind)
                || (kind != HarnessEventType::Usage
                    && delivery.lane.as_ref().is_some_and(|l| l != lane))
            {
                continue;
            }
            let mut candidate = Some((
                event.clone(),
                Arc::downgrade(&self.telemetry),
                kind,
                lane.clone(),
            ));
            {
                let mut state = delivery.state.lock().unwrap_or_else(|p| p.into_inner());
                if !state.closed {
                    state
                        .queue
                        .push_back(candidate.take().expect("candidate event"));
                }
            }
            drop(candidate);
        }
    }
    pub fn flush(&self) {
        let deliveries: Vec<_> = self
            .deliveries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for delivery in deliveries {
            delivery.schedule();
        }
    }
    /// Caller captures the snapshot and registers this buffer while holding the
    /// same publication line; the hub does not execute a snapshot callback.
    pub fn watch<T: Clone + Send + Sync + 'static>(
        &self,
        snapshot: T,
        lane: Option<LaneId>,
    ) -> Arc<dyn WatchHandle<T>> {
        Arc::new(Watch {
            snapshot,
            delivery: self.register(None, lane, None),
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
impl EventSubscription for Subscription {
    fn unsubscribe(&self) {
        self.0.stop();
    }
}
struct Watch<T> {
    snapshot: T,
    delivery: Arc<Delivery>,
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
        self.delivery.schedule();
    }
    fn unsubscribe(&self) {
        self.delivery.stop();
    }
}
