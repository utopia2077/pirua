use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use pi_ai_core::{
    ApiId, AssistantMessage, AssistantMessageEvent, AssistantMessageEventStream, AssistantRole,
    AssistantStreamInit, BoxFuture, CancellationToken, Cancelled, Clock, DoneReason, ErrorReason,
    EventStream, JsonObject, ModelId, ProviderId, REQUEST_CANCELLED_MESSAGE, SafeUnsigned,
    StopReason, StreamProtocolError, StreamSetupFailure, ToolCall, ToolCallId, ToolCallType,
    ToolName, Usage, create_assistant_message_event_stream,
};

const INVENTORY: &str = include_str!("../../../conformance/ai-domain/stream-inventory.json");

struct CountingWake {
    wakes: AtomicUsize,
}

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

struct CallbackWake {
    callback: Mutex<Box<dyn FnMut() + Send>>,
}

struct DropReenterWake {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    dropped: Arc<AtomicBool>,
    wakes: AtomicUsize,
}

impl Wake for DropReenterWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for DropReenterWake {
    fn drop(&mut self) {
        if let Some(callback) = self
            .callback
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            callback();
        }
        self.dropped.store(true, Ordering::Release);
    }
}

fn drop_reenter_waker(callback: impl FnOnce() + Send + 'static) -> (Waker, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(DropReenterWake {
        callback: Mutex::new(Some(Box::new(callback))),
        dropped: Arc::clone(&dropped),
        wakes: AtomicUsize::new(0),
    }));
    (waker, dropped)
}

impl Wake for CallbackWake {
    fn wake(self: Arc<Self>) {
        (self
            .callback
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()))();
    }
}

fn noop_waker() -> Waker {
    Waker::noop().clone()
}

fn poll_with<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn ready<F: Future>(future: F) -> F::Output {
    let waker = noop_waker();
    let mut future = pin!(future);
    match poll_with(future.as_mut(), &waker) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future must already be ready"),
    }
}

#[derive(Default)]
struct SleepState {
    next_id: u64,
    waiters: BTreeMap<u64, Waker>,
}

struct TestClock {
    now: u64,
    sleeps: Mutex<SleepState>,
}

impl TestClock {
    fn new(now: u64) -> Self {
        Self {
            now,
            sleeps: Mutex::new(SleepState::default()),
        }
    }

    fn sleeper_count(&self) -> usize {
        self.sleeps
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .waiters
            .len()
    }

    fn wake_sleepers(&self) {
        let wakers = std::mem::take(
            &mut self
                .sleeps
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .waiters,
        )
        .into_values()
        .collect::<Vec<_>>();
        for waker in wakers {
            waker.wake();
        }
    }
}

struct TestSleep<'a> {
    clock: &'a TestClock,
    cancellation: &'a CancellationToken,
    waiter_id: Option<u64>,
}

impl Future for TestSleep<'_> {
    type Output = Result<(), Cancelled>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Err(Cancelled));
        }
        let mut sleeps = self
            .clock
            .sleeps
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let id = self.waiter_id.unwrap_or_else(|| {
            let id = sleeps.next_id;
            sleeps.next_id = sleeps.next_id.wrapping_add(1);
            self.waiter_id = Some(id);
            id
        });
        sleeps.waiters.insert(id, context.waker().clone());
        Poll::Pending
    }
}

impl Drop for TestSleep<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id.take() {
            self.clock
                .sleeps
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .waiters
                .remove(&id);
        }
    }
}

impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.now
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(TestSleep {
            clock: self,
            cancellation,
            waiter_id: None,
        })
    }
}

fn init(cancellation: CancellationToken, now: u64) -> AssistantStreamInit {
    AssistantStreamInit {
        api: ApiId::new("test-api").expect("nonempty API"),
        provider: ProviderId::new("test-provider").expect("nonempty provider"),
        model: ModelId::new("test-model").expect("nonempty model"),
        clock: Arc::new(TestClock::new(now)),
        cancellation,
    }
}

fn message(stop_reason: StopReason) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: Vec::new(),
        api: ApiId::new("test-api").expect("nonempty API"),
        provider: ProviderId::new("test-provider").expect("nonempty provider"),
        model: ModelId::new("test-model").expect("nonempty model"),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason,
        deferred: None,
        error_message: matches!(stop_reason, StopReason::Error | StopReason::Aborted)
            .then(|| "fixed terminal detail".to_owned()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: SafeUnsigned::new(7).expect("safe timestamp"),
    }
}

fn terminal_events() -> Vec<AssistantMessageEvent> {
    [
        (DoneReason::Stop, StopReason::Stop),
        (DoneReason::Length, StopReason::Length),
        (DoneReason::ToolUse, StopReason::ToolUse),
        (DoneReason::Deferred, StopReason::Deferred),
    ]
    .into_iter()
    .map(|(reason, stop_reason)| AssistantMessageEvent::Done {
        reason,
        message: Box::new(message(stop_reason)),
    })
    .chain([
        AssistantMessageEvent::Error {
            reason: ErrorReason::Error,
            error: Box::new(message(StopReason::Error)),
        },
        AssistantMessageEvent::Error {
            reason: ErrorReason::Aborted,
            error: Box::new(message(StopReason::Aborted)),
        },
    ])
    .collect()
}

fn pending() -> Box<AssistantMessage> {
    Box::new(message(StopReason::Pending))
}

fn progress_events() -> Vec<AssistantMessageEvent> {
    let index = SafeUnsigned::new(0).expect("safe index");
    vec![
        AssistantMessageEvent::Start { partial: pending() },
        AssistantMessageEvent::TextStart {
            content_index: index,
            partial: pending(),
        },
        AssistantMessageEvent::TextDelta {
            content_index: index,
            delta: "a".to_owned(),
            partial: pending(),
        },
        AssistantMessageEvent::TextEnd {
            content_index: index,
            content: "a".to_owned(),
            partial: pending(),
        },
        AssistantMessageEvent::ThinkingStart {
            content_index: index,
            partial: pending(),
        },
        AssistantMessageEvent::ThinkingDelta {
            content_index: index,
            delta: "b".to_owned(),
            partial: pending(),
        },
        AssistantMessageEvent::ThinkingEnd {
            content_index: index,
            content: "b".to_owned(),
            partial: pending(),
        },
        AssistantMessageEvent::ToolCallStart {
            content_index: index,
            partial: pending(),
        },
        AssistantMessageEvent::ToolCallDelta {
            content_index: index,
            delta: "{}".to_owned(),
            partial: pending(),
        },
        AssistantMessageEvent::ToolCallEnd {
            content_index: index,
            tool_call: ToolCall {
                content_type: ToolCallType::ToolCall,
                id: ToolCallId::new("call-1").expect("nonempty call id"),
                name: ToolName::new("tool").expect("nonempty tool name"),
                arguments: JsonObject::default(),
                thought_signature: None,
                namespace: None,
            },
            partial: pending(),
        },
    ]
}

fn next_ready(stream: &mut AssistantMessageEventStream) -> Option<AssistantMessageEvent> {
    ready(stream.next())
}

#[test]
fn stream_inventory_has_exactly_the_22_stable_rows() {
    let inventory: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    let rows = inventory["rows"].as_array().expect("rows array");
    assert_eq!(inventory["expectedRows"], 22);
    assert_eq!(rows.len(), 22);
    let ids = rows
        .iter()
        .map(|row| row["id"].as_str().expect("string id"))
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [
            "stream.terminal.stop",
            "stream.terminal.length",
            "stream.terminal.tool-use",
            "stream.terminal.deferred",
            "stream.terminal.error",
            "stream.terminal.aborted",
            "stream.result-without-events",
            "stream.queued-fifo",
            "stream.waiting-fifo",
            "stream.setup.unknown-provider",
            "stream.setup.auth",
            "stream.setup.adapter",
            "stream.setup.spawn",
            "stream.cancel.pre-cancelled",
            "stream.cancel-wins",
            "stream.provider-wins",
            "stream.cancel-provider-barrier-race",
            "stream.post-terminal-matrix",
            "stream.last-producer-drop",
            "stream.cancel-live-noncooperative-producer",
            "stream.reentrant-waker-matrix",
            "stream.dropped-waiter-matrix",
        ]
    );
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 22);
    for row in rows {
        for field in ["source", "test", "polarity", "owner", "expected"] {
            assert!(
                row[field].as_str().is_some_and(|value| !value.is_empty()),
                "missing {field} in {}",
                row["id"]
            );
        }
    }
}

#[test]
fn assistant_stream_init_debug_is_fully_redacted() {
    let init = AssistantStreamInit {
        api: ApiId::new("debug-api-sentinel").expect("nonempty API"),
        provider: ProviderId::new("debug-provider-sentinel").expect("nonempty provider"),
        model: ModelId::new("debug-model-sentinel").expect("nonempty model"),
        clock: Arc::new(TestClock::new(1)),
        cancellation: CancellationToken::new(),
    };
    let rendered = format!("{init:?}");
    for sentinel in [
        "debug-api-sentinel",
        "debug-provider-sentinel",
        "debug-model-sentinel",
    ] {
        assert!(!rendered.contains(sentinel));
    }
}

#[test]
fn stream_terminal_rows() {
    for terminal in terminal_events() {
        let expected = match &terminal {
            AssistantMessageEvent::Done { message, .. } => message.as_ref().clone(),
            AssistantMessageEvent::Error { error, .. } => error.as_ref().clone(),
            _ => unreachable!("terminal fixture"),
        };
        let (mut stream, producer) =
            create_assistant_message_event_stream(init(CancellationToken::new(), 1));
        producer
            .push(terminal.clone())
            .expect("first terminal wins");
        assert_eq!(ready(stream.result()), expected);
        assert_eq!(next_ready(&mut stream), Some(terminal));
        assert_eq!(next_ready(&mut stream), None);
    }
}

#[test]
fn result_is_independent_owned_and_multi_waiter() {
    struct NonClone;
    fn next_has_no_clone_bound<E, R>(stream: &mut EventStream<E, R>) {
        drop(stream.next());
    }
    let _compile_witness = next_has_no_clone_bound::<NonClone, String>;

    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let terminal = terminal_events().remove(0);
    let expected = match &terminal {
        AssistantMessageEvent::Done { message, .. } => message.as_ref().clone(),
        _ => unreachable!("done fixture"),
    };
    let waker = noop_waker();
    let mut waiters = (0..8)
        .map(|_| Box::pin(stream.result()))
        .collect::<Vec<_>>();
    for waiter in &mut waiters {
        assert!(matches!(poll_with(waiter.as_mut(), &waker), Poll::Pending));
    }
    producer.push(terminal.clone()).expect("terminal wins");
    for waiter in &mut waiters {
        assert_eq!(
            poll_with(waiter.as_mut(), &waker),
            Poll::Ready(expected.clone())
        );
    }
    drop(waiters);
    assert_eq!(ready(stream.result()), expected);
    assert_eq!(next_ready(&mut stream), Some(terminal));
}

#[test]
fn queued_and_waiting_events_are_fifo() {
    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let events = progress_events();
    producer.push(events[0].clone()).expect("first progress");
    producer.push(events[1].clone()).expect("second progress");
    assert_eq!(next_ready(&mut stream), Some(events[0].clone()));
    assert_eq!(next_ready(&mut stream), Some(events[1].clone()));

    let waker = noop_waker();
    let mut waiting = Box::pin(stream.next());
    assert!(matches!(poll_with(waiting.as_mut(), &waker), Poll::Pending));
    producer.push(events[2].clone()).expect("waiting progress");
    assert_eq!(
        poll_with(waiting.as_mut(), &waker),
        Poll::Ready(Some(events[2].clone()))
    );
    drop(waiting);
    producer.push(events[3].clone()).expect("queued after wake");
    assert_eq!(next_ready(&mut stream), Some(events[3].clone()));
}

#[test]
fn setup_failures_are_in_band_and_sanitized() {
    for failure in [
        StreamSetupFailure::UnknownProvider,
        StreamSetupFailure::Authentication,
        StreamSetupFailure::Adapter,
        StreamSetupFailure::Spawn,
    ] {
        let (mut stream, producer) =
            create_assistant_message_event_stream(init(CancellationToken::new(), u64::MAX));
        producer.fail_setup(failure).expect("setup failure settles");
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(result.usage, Usage::zero());
        assert!(
            result
                .error_message
                .as_ref()
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(result.timestamp, SafeUnsigned::MAX);
        assert_eq!(
            next_ready(&mut stream),
            Some(AssistantMessageEvent::Error {
                reason: ErrorReason::Error,
                error: Box::new(result),
            })
        );
    }
}

#[test]
fn cancellation_precedence_rows() {
    let pre = CancellationToken::new();
    pre.cancel();
    let (mut stream, producer) = create_assistant_message_event_stream(init(pre, 1));
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(
        result.error_message.as_deref(),
        Some(REQUEST_CANCELLED_MESSAGE)
    );
    assert!(matches!(
        producer.push(terminal_events().remove(0)),
        Err(StreamProtocolError::AlreadySettled)
    ));
    assert!(matches!(
        next_ready(&mut stream),
        Some(AssistantMessageEvent::Error {
            reason: ErrorReason::Aborted,
            ..
        })
    ));

    let cancellation = CancellationToken::new();
    let (stream, producer) = create_assistant_message_event_stream(init(cancellation.clone(), 1));
    cancellation.cancel();
    assert!(matches!(
        producer.push(terminal_events().remove(0)),
        Err(StreamProtocolError::AlreadySettled)
    ));
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);

    let cancellation = CancellationToken::new();
    let (stream, producer) = create_assistant_message_event_stream(init(cancellation.clone(), 1));
    producer
        .push(terminal_events().remove(0))
        .expect("provider wins");
    cancellation.cancel();
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
}

#[test]
fn cancellation_and_provider_barrier_have_one_winner() {
    for _ in 0..64 {
        let cancellation = CancellationToken::new();
        let (mut stream, producer) =
            create_assistant_message_event_stream(init(cancellation.clone(), 1));
        let barrier = Arc::new(Barrier::new(3));
        let cancel_barrier = Arc::clone(&barrier);
        let cancel_thread = thread::spawn(move || {
            cancel_barrier.wait();
            cancellation.cancel();
        });
        let push_barrier = Arc::clone(&barrier);
        let push_thread = thread::spawn(move || {
            push_barrier.wait();
            producer.push(terminal_events().remove(0))
        });
        barrier.wait();
        cancel_thread.join().expect("cancel thread");
        let push_result = push_thread.join().expect("push thread");
        let result = ready(stream.result());
        match result.stop_reason {
            StopReason::Stop => assert!(push_result.is_ok()),
            StopReason::Aborted => {
                assert!(matches!(
                    push_result,
                    Err(StreamProtocolError::AlreadySettled)
                ))
            }
            reason => panic!("unexpected race result {reason:?}"),
        }
        assert!(next_ready(&mut stream).is_some());
        assert_eq!(next_ready(&mut stream), None);
    }
}

#[test]
fn all_96_legal_post_terminal_pushes_are_rejected() {
    let mut checked = 0;
    for terminal in terminal_events() {
        let expected = match &terminal {
            AssistantMessageEvent::Done { message, .. } => message.as_ref().clone(),
            AssistantMessageEvent::Error { error, .. } => error.as_ref().clone(),
            _ => unreachable!("terminal fixture"),
        };
        let (mut stream, producer) =
            create_assistant_message_event_stream(init(CancellationToken::new(), 1));
        producer
            .push(terminal.clone())
            .expect("first terminal wins");
        for event in progress_events().into_iter().chain(terminal_events()) {
            assert_eq!(
                producer.push(event),
                Err(StreamProtocolError::AlreadySettled)
            );
            assert_eq!(ready(stream.result()), expected);
            checked += 1;
        }
        assert_eq!(next_ready(&mut stream), Some(terminal));
        assert_eq!(next_ready(&mut stream), None);
    }
    assert_eq!(checked, 96);
}

#[test]
fn last_of_32_producer_clones_settles_once() {
    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let clones = (0..32).map(|_| producer.clone()).collect::<Vec<_>>();
    let threads = clones
        .into_iter()
        .map(|producer| thread::spawn(move || drop(producer)))
        .collect::<Vec<_>>();
    drop(producer);
    for thread in threads {
        thread.join().expect("producer-drop thread");
    }
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(
        result.error_message.as_deref(),
        Some(pi_ai_core::STREAM_PRODUCER_DROPPED_MESSAGE)
    );
    assert!(next_ready(&mut stream).is_some());
    assert_eq!(next_ready(&mut stream), None);
}

#[test]
fn cancellation_settles_with_live_noncooperative_producer() {
    let cancellation = CancellationToken::new();
    let (mut stream, producer) =
        create_assistant_message_event_stream(init(cancellation.clone(), 1));
    let noncooperative = producer.clone();
    cancellation.cancel();
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert!(matches!(
        producer.push(progress_events().remove(0)),
        Err(StreamProtocolError::AlreadySettled)
    ));
    drop(noncooperative);
    drop(producer);
    assert!(next_ready(&mut stream).is_some());
    assert_eq!(next_ready(&mut stream), None);
}

#[test]
fn reentrant_wakers_run_after_unlock() {
    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let reentrant = producer.clone();
    let event_from_waker = progress_events().remove(1);
    let expected_from_waker = event_from_waker.clone();
    let waker = Waker::from(Arc::new(CallbackWake {
        callback: Mutex::new(Box::new(move || {
            reentrant
                .push(event_from_waker.clone())
                .expect("next waker re-enters stream");
        })),
    }));
    let mut next = Box::pin(stream.next());
    assert!(matches!(poll_with(next.as_mut(), &waker), Poll::Pending));
    let first = progress_events().remove(0);
    producer.push(first.clone()).expect("wake next");
    assert_eq!(
        poll_with(next.as_mut(), &noop_waker()),
        Poll::Ready(Some(first))
    );
    drop(next);
    assert_eq!(next_ready(&mut stream), Some(expected_from_waker));

    let observed = Arc::new(Mutex::new(None));
    let observed_from_waker = Arc::clone(&observed);
    let reentrant = producer.clone();
    let result_waker = Waker::from(Arc::new(CallbackWake {
        callback: Mutex::new(Box::new(move || {
            *observed_from_waker
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) =
                Some(reentrant.push(progress_events().remove(0)));
        })),
    }));
    let mut result = Box::pin(stream.result());
    assert!(matches!(
        poll_with(result.as_mut(), &result_waker),
        Poll::Pending
    ));
    producer
        .push(terminal_events().remove(0))
        .expect("terminal wakes result");
    assert!(matches!(
        observed
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref(),
        Some(Err(StreamProtocolError::AlreadySettled))
    ));
    assert!(matches!(
        poll_with(result.as_mut(), &noop_waker()),
        Poll::Ready(_)
    ));

    let cancellation = CancellationToken::new();
    let cancellation_from_waker = cancellation.clone();
    let cancelled_observed = Arc::new(Mutex::new(false));
    let cancelled_from_waker = Arc::clone(&cancelled_observed);
    let cancel_waker = Waker::from(Arc::new(CallbackWake {
        callback: Mutex::new(Box::new(move || {
            *cancelled_from_waker
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) =
                cancellation_from_waker.is_cancelled();
        })),
    }));
    let mut cancelled = Box::pin(cancellation.cancelled());
    assert!(matches!(
        poll_with(cancelled.as_mut(), &cancel_waker),
        Poll::Pending
    ));
    cancellation.cancel();
    assert!(
        *cancelled_observed
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    );

    let clock = Arc::new(TestClock::new(1));
    let clock_from_waker = Arc::clone(&clock);
    let sleep_observed = Arc::new(Mutex::new(false));
    let sleep_from_waker = Arc::clone(&sleep_observed);
    let sleep_waker = Waker::from(Arc::new(CallbackWake {
        callback: Mutex::new(Box::new(move || {
            *sleep_from_waker
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) =
                clock_from_waker.sleeper_count() == 0;
        })),
    }));
    let sleep_cancellation = CancellationToken::new();
    let mut sleep = clock.sleep(Duration::from_millis(1), &sleep_cancellation);
    assert!(matches!(
        poll_with(sleep.as_mut(), &sleep_waker),
        Poll::Pending
    ));
    clock.wake_sleepers();
    assert!(
        *sleep_observed
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    );
}

#[test]
fn dropped_waiters_release_registered_wakers() {
    let wake = Arc::new(CountingWake {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let baseline = Arc::strong_count(&wake);

    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    {
        let mut next = Box::pin(stream.next());
        assert!(matches!(poll_with(next.as_mut(), &waker), Poll::Pending));
        assert_eq!(Arc::strong_count(&wake), baseline + 1);
    }
    assert_eq!(Arc::strong_count(&wake), baseline);

    {
        let mut result = Box::pin(stream.result());
        assert!(matches!(poll_with(result.as_mut(), &waker), Poll::Pending));
        assert_eq!(Arc::strong_count(&wake), baseline + 1);
    }
    assert_eq!(Arc::strong_count(&wake), baseline);

    let cancellation = CancellationToken::new();
    {
        let mut cancelled = Box::pin(cancellation.cancelled());
        assert!(matches!(
            poll_with(cancelled.as_mut(), &waker),
            Poll::Pending
        ));
        assert_eq!(Arc::strong_count(&wake), baseline + 1);
    }
    assert_eq!(Arc::strong_count(&wake), baseline);

    let clock = TestClock::new(1);
    {
        let mut sleep = clock.sleep(Duration::from_millis(1), &cancellation);
        assert!(matches!(poll_with(sleep.as_mut(), &waker), Poll::Pending));
        assert_eq!(clock.sleeper_count(), 1);
        assert_eq!(Arc::strong_count(&wake), baseline + 1);
    }
    assert_eq!(clock.sleeper_count(), 0);
    assert_eq!(Arc::strong_count(&wake), baseline);
    drop(producer);
}

#[test]
fn stored_waker_destructors_reenter_after_unlock() {
    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let reentrant = producer.clone();
    let expected = progress_events().remove(0);
    let pushed = expected.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        reentrant
            .push(pushed)
            .expect("next waker destructor re-enters stream");
    });
    let mut next = Box::pin(stream.next());
    assert!(matches!(poll_with(next.as_mut(), &waker), Poll::Pending));
    drop(waker);
    assert!(matches!(
        poll_with(next.as_mut(), &noop_waker()),
        Poll::Pending
    ));
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(
        poll_with(next.as_mut(), &noop_waker()),
        Poll::Ready(Some(expected))
    );
    drop(next);
    drop(producer);

    let (mut stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let reentrant = producer.clone();
    let expected = progress_events().remove(0);
    let pushed = expected.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        reentrant
            .push(pushed)
            .expect("dropped next future re-enters stream");
    });
    let mut next = Box::pin(stream.next());
    assert!(matches!(poll_with(next.as_mut(), &waker), Poll::Pending));
    drop(waker);
    drop(next);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(next_ready(&mut stream), Some(expected));
    drop(producer);

    let (stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let reentrant = producer.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        reentrant
            .push(progress_events().remove(0))
            .expect("result waker destructor re-enters stream");
    });
    let mut result = Box::pin(stream.result());
    assert!(matches!(poll_with(result.as_mut(), &waker), Poll::Pending));
    drop(waker);
    drop(result);
    assert!(dropped.load(Ordering::Acquire));
    drop(producer);

    let (stream, producer) =
        create_assistant_message_event_stream(init(CancellationToken::new(), 1));
    let reentrant = producer.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        reentrant
            .push(progress_events().remove(0))
            .expect("replaced result waker re-enters stream");
    });
    let mut result = Box::pin(stream.result());
    assert!(matches!(poll_with(result.as_mut(), &waker), Poll::Pending));
    drop(waker);
    assert!(matches!(
        poll_with(result.as_mut(), &noop_waker()),
        Poll::Pending
    ));
    assert!(dropped.load(Ordering::Acquire));
    drop(result);
    drop(producer);

    let cancellation = CancellationToken::new();
    let reentrant = cancellation.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        assert!(!reentrant.is_cancelled());
    });
    let mut cancelled = Box::pin(cancellation.cancelled());
    assert!(matches!(
        poll_with(cancelled.as_mut(), &waker),
        Poll::Pending
    ));
    drop(waker);
    drop(cancelled);
    assert!(dropped.load(Ordering::Acquire));

    let cancellation = CancellationToken::new();
    let reentrant = cancellation.clone();
    let (waker, dropped) = drop_reenter_waker(move || {
        assert!(!reentrant.is_cancelled());
    });
    let mut cancelled = Box::pin(cancellation.cancelled());
    assert!(matches!(
        poll_with(cancelled.as_mut(), &waker),
        Poll::Pending
    ));
    drop(waker);
    assert!(matches!(
        poll_with(cancelled.as_mut(), &noop_waker()),
        Poll::Pending
    ));
    assert!(dropped.load(Ordering::Acquire));
    drop(cancelled);
}
