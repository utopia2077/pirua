//! Consumer-only event streams with validated assistant producers.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use crate::error::{
    REQUEST_CANCELLED_MESSAGE, STREAM_PRODUCER_DROPPED_MESSAGE, StreamProtocolError,
    StreamSetupFailure,
};
use crate::event::{AssistantMessageEvent, ErrorReason};
use crate::id::{ApiId, ModelId, ProviderId};
use crate::message::{AssistantMessage, AssistantRole, StopReason};
use crate::runtime::{
    CancellationRegistration, CancellationTarget, CancellationToken, Clock, safe_timestamp,
};
use crate::usage::Usage;

struct RegisteredWaker {
    id: u64,
    waker: Waker,
}

enum Settlement<R> {
    Open,
    Settled(R),
}

struct CancellationLink {
    _registration: CancellationRegistration,
    _target: Arc<dyn CancellationTarget>,
}

struct StreamState<E, R> {
    events: VecDeque<E>,
    settlement: Settlement<R>,
    next_id: u64,
    next_waiter: Option<RegisteredWaker>,
    result_waiters: BTreeMap<u64, Waker>,
    cancellation_link: Option<CancellationLink>,
}

struct StreamShared<E, R> {
    state: Mutex<StreamState<E, R>>,
}

impl<E, R> StreamShared<E, R> {
    fn new() -> Self {
        Self {
            state: Mutex::new(StreamState {
                events: VecDeque::new(),
                settlement: Settlement::Open,
                next_id: 0,
                next_waiter: None,
                result_waiters: BTreeMap::new(),
                cancellation_link: None,
            }),
        }
    }

    fn next_id(state: &mut StreamState<E, R>) -> u64 {
        loop {
            let candidate = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            let next_occupied = state
                .next_waiter
                .as_ref()
                .is_some_and(|waiter| waiter.id == candidate);
            if !next_occupied && !state.result_waiters.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

/// A non-cloneable, consumer-only asynchronous event stream.
///
/// The stream exposes no generic producer, push, end, or constructor. Assistant
/// streams are created only by [`create_assistant_message_event_stream`].
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::EventStream<String>>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::EventStream<String>>();
/// ```
///
/// ```compile_fail
/// # let mut stream: pi_ai_core::EventStream<String> = todo!();
/// stream.push("bypass".to_owned());
/// ```
///
/// ```compile_fail
/// # let mut stream: pi_ai_core::EventStream<String> = todo!();
/// stream.end("bypass".to_owned());
/// ```
pub struct EventStream<E, R = E> {
    shared: Arc<StreamShared<E, R>>,
}

impl<E, R> EventStream<E, R> {
    /// Waits for the next queued event, or `None` after the terminal event is drained.
    #[allow(clippy::should_implement_trait)] // This is an asynchronous consumer, not an Iterator.
    pub fn next(&mut self) -> NextFuture<'_, E, R> {
        NextFuture {
            shared: &self.shared,
            waiter_id: None,
            _exclusive_consumer: PhantomData,
        }
    }

    /// Waits for an owned clone of the terminal result without draining events.
    pub fn result(&self) -> ResultFuture<'_, E, R>
    where
        R: Clone,
    {
        ResultFuture {
            shared: &self.shared,
            waiter_id: None,
        }
    }
}

impl<E, R> fmt::Debug for EventStream<E, R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EventStream([PROCESS-LOCAL])")
    }
}

/// Future returned by [`EventStream::next`].
pub struct NextFuture<'a, E, R> {
    shared: &'a Arc<StreamShared<E, R>>,
    waiter_id: Option<u64>,
    _exclusive_consumer: PhantomData<&'a mut EventStream<E, R>>,
}

impl<E, R> Future for NextFuture<'_, E, R> {
    type Output = Option<E>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let candidate_waker = context.waker().clone();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(event) = state.events.pop_front() {
            let displaced = if let Some(id) = self.waiter_id.take()
                && state
                    .next_waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.id == id)
            {
                state.next_waiter.take()
            } else {
                None
            };
            drop(state);
            drop(displaced);
            return Poll::Ready(Some(event));
        }
        if matches!(state.settlement, Settlement::Settled(_)) {
            let displaced = if let Some(id) = self.waiter_id.take()
                && state
                    .next_waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.id == id)
            {
                state.next_waiter.take()
            } else {
                None
            };
            drop(state);
            drop(displaced);
            return Poll::Ready(None);
        }

        let id = match self.waiter_id {
            Some(id) => id,
            None => {
                let id = StreamShared::next_id(&mut state);
                self.waiter_id = Some(id);
                id
            }
        };
        let replace = state
            .next_waiter
            .as_ref()
            .is_none_or(|waiter| waiter.id != id || !waiter.waker.will_wake(context.waker()));
        let displaced = if replace {
            state.next_waiter.replace(RegisteredWaker {
                id,
                waker: candidate_waker,
            })
        } else {
            None
        };
        drop(state);
        drop(displaced);
        Poll::Pending
    }
}

impl<E, R> Drop for NextFuture<'_, E, R> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id.take() {
            let displaced = {
                let mut state = self
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                if state
                    .next_waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.id == id)
                {
                    state.next_waiter.take()
                } else {
                    None
                }
            };
            drop(displaced);
        }
    }
}

/// Future returned by [`EventStream::result`].
pub struct ResultFuture<'a, E, R> {
    shared: &'a Arc<StreamShared<E, R>>,
    waiter_id: Option<u64>,
}

impl<E, R> Future for ResultFuture<'_, E, R>
where
    R: Clone,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let candidate_waker = context.waker().clone();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Settlement::Settled(result) = &state.settlement {
            let result = result.clone();
            let displaced = self
                .waiter_id
                .take()
                .and_then(|id| state.result_waiters.remove(&id));
            drop(state);
            drop(displaced);
            return Poll::Ready(result);
        }

        let id = match self.waiter_id {
            Some(id) => id,
            None => {
                let id = StreamShared::next_id(&mut state);
                self.waiter_id = Some(id);
                id
            }
        };
        let replace = state
            .result_waiters
            .get(&id)
            .is_none_or(|waker| !waker.will_wake(context.waker()));
        let displaced = if replace {
            state.result_waiters.insert(id, candidate_waker)
        } else {
            None
        };
        drop(state);
        drop(displaced);
        Poll::Pending
    }
}

impl<E, R> Drop for ResultFuture<'_, E, R> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id.take() {
            let displaced = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .result_waiters
                .remove(&id);
            drop(displaced);
        }
    }
}

/// Provider-neutral assistant event stream.
pub type AssistantStream = EventStream<AssistantMessageEvent, AssistantMessage>;
/// Canonical assistant-message event stream projection.
pub type AssistantMessageEventStream = AssistantStream;

struct AssistantFallback {
    api: ApiId,
    provider: ProviderId,
    model: ModelId,
    clock: Arc<dyn Clock>,
}

impl AssistantFallback {
    fn event(&self, reason: ErrorReason, message: &'static str) -> AssistantMessageEvent {
        let stop_reason = match reason {
            ErrorReason::Aborted => StopReason::Aborted,
            ErrorReason::Error => StopReason::Error,
        };
        AssistantMessageEvent::Error {
            reason,
            error: Box::new(AssistantMessage {
                role: AssistantRole::Assistant,
                content: Vec::new(),
                api: self.api.clone(),
                provider: self.provider.clone(),
                model: self.model.clone(),
                response_model: None,
                response_id: None,
                diagnostics: None,
                usage: Usage::zero(),
                stop_reason,
                deferred: None,
                error_message: Some(message.to_owned()),
                raw_stop_reason: None,
                end_turn: None,
                timestamp: safe_timestamp(self.clock.as_ref()),
            }),
        }
    }
}

impl StreamShared<AssistantMessageEvent, AssistantMessage> {
    fn push(&self, event: AssistantMessageEvent) -> Result<(), StreamProtocolError> {
        event.validate()?;
        let result = match &event {
            AssistantMessageEvent::Done { message, .. } => Some(message.as_ref().clone()),
            AssistantMessageEvent::Error { error, .. } => Some(error.as_ref().clone()),
            _ => None,
        };

        let (next_waker, result_waiters, cancellation_link) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if matches!(state.settlement, Settlement::Settled(_)) {
                return Err(StreamProtocolError::AlreadySettled);
            }
            state.events.push_back(event);
            let next_waker = state.next_waiter.take().map(|waiter| waiter.waker);
            match result {
                Some(result) => {
                    state.settlement = Settlement::Settled(result);
                    let result_waiters = std::mem::take(&mut state.result_waiters);
                    let cancellation_link = state.cancellation_link.take();
                    (next_waker, result_waiters, cancellation_link)
                }
                None => (next_waker, BTreeMap::new(), None),
            }
        };

        drop(cancellation_link);
        if let Some(waker) = next_waker {
            waker.wake();
        }
        for waker in result_waiters.into_values() {
            waker.wake();
        }
        Ok(())
    }
}

struct AssistantCancellation {
    shared: Weak<StreamShared<AssistantMessageEvent, AssistantMessage>>,
    fallback: Arc<AssistantFallback>,
}

impl CancellationTarget for AssistantCancellation {
    fn cancel(&self) {
        if let Some(shared) = self.shared.upgrade() {
            let _ = shared.push(
                self.fallback
                    .event(ErrorReason::Aborted, REQUEST_CANCELLED_MESSAGE),
            );
        }
    }
}

struct ProducerGroup {
    shared: Arc<StreamShared<AssistantMessageEvent, AssistantMessage>>,
    fallback: Arc<AssistantFallback>,
    producers: AtomicUsize,
}

/// Cloneable validated producer for an [`AssistantMessageEventStream`].
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::AssistantStreamProducer>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::AssistantStreamProducer>();
/// ```
pub struct AssistantStreamProducer {
    group: Arc<ProducerGroup>,
}

impl AssistantStreamProducer {
    /// Pushes one validated progress or terminal event.
    pub fn push(&self, event: AssistantMessageEvent) -> Result<(), StreamProtocolError> {
        self.group.shared.push(event)
    }

    /// Converts a setup failure category into a fixed, sanitized terminal event.
    pub fn fail_setup(&self, failure: StreamSetupFailure) -> Result<(), StreamProtocolError> {
        self.push(
            self.group
                .fallback
                .event(ErrorReason::Error, failure.terminal_message()),
        )
    }
}

impl Clone for AssistantStreamProducer {
    fn clone(&self) -> Self {
        self.group.producers.fetch_add(1, Ordering::Relaxed);
        Self {
            group: Arc::clone(&self.group),
        }
    }
}

impl fmt::Debug for AssistantStreamProducer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AssistantStreamProducer([PROCESS-LOCAL])")
    }
}

impl Drop for AssistantStreamProducer {
    fn drop(&mut self) {
        if self.group.producers.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _ = self.group.shared.push(
                self.group
                    .fallback
                    .event(ErrorReason::Error, STREAM_PRODUCER_DROPPED_MESSAGE),
            );
        }
    }
}

/// Exact process-local initialization capability for an assistant stream.
///
/// Debug output redacts the process-local clock and cancellation registration.
/// This type intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::AssistantStreamInit>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::AssistantStreamInit>();
/// ```
pub struct AssistantStreamInit {
    /// API protocol copied into runtime-generated terminal messages.
    pub api: ApiId,
    /// Provider copied into runtime-generated terminal messages.
    pub provider: ProviderId,
    /// Model copied into runtime-generated terminal messages.
    pub model: ModelId,
    /// Injected timestamp authority.
    pub clock: Arc<dyn Clock>,
    /// Request cancellation source.
    pub cancellation: CancellationToken,
}

impl fmt::Debug for AssistantStreamInit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AssistantStreamInit([REDACTED])")
    }
}

/// Creates a consumer-only assistant stream and its validated producer.
pub fn create_assistant_message_event_stream(
    init: AssistantStreamInit,
) -> (AssistantMessageEventStream, AssistantStreamProducer) {
    let shared = Arc::new(StreamShared::new());
    let fallback = Arc::new(AssistantFallback {
        api: init.api,
        provider: init.provider,
        model: init.model,
        clock: init.clock,
    });
    let target: Arc<dyn CancellationTarget> = Arc::new(AssistantCancellation {
        shared: Arc::downgrade(&shared),
        fallback: Arc::clone(&fallback),
    });
    let registration = init.cancellation.register_target(&target);
    if let Some(registration) = registration {
        let link = CancellationLink {
            _registration: registration,
            _target: target,
        };
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if matches!(state.settlement, Settlement::Open) {
            state.cancellation_link = Some(link);
        }
    }

    let group = Arc::new(ProducerGroup {
        shared: Arc::clone(&shared),
        fallback,
        producers: AtomicUsize::new(1),
    });
    (EventStream { shared }, AssistantStreamProducer { group })
}
