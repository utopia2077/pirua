use std::error::Error;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use pi_ai_core::{
    AssistantMessage, AssistantMessageEvent, AssistantMessageEventStream, AssistantStreamInit,
    AssistantStreamProducer, BoxFuture, CancellationSnapshotError, CancellationToken, Cancelled,
    Clock, DoneReason, ErrorReason, REQUEST_CANCELLED_MESSAGE, STREAM_PRODUCER_DROPPED_MESSAGE,
    SafeUnsigned, StopReason, StreamProtocolError, StreamSetupFailure, Usage,
    create_assistant_message_event_stream,
};

struct FixedClock;

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        99
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        panic!("snapshot publication and cancellation must not sleep")
    }
}

fn fixture() -> AssistantMessage {
    // Pinned Anthropic message_start usage and signature-bearing partial output
    // survive cancellation (anthropic-messages.ts:591-615, 778-796 at
    // 853a80d26c90a14c1886f0ebb8ffaae133ca2185). Values are synthetic.
    serde_json::from_value(serde_json::json!({
        "role": "assistant",
        "api": "anthropic-messages",
        "provider": "anthropic",
        "model": "requested-model",
        "responseModel": "observed-model",
        "responseId": "response-1",
        "content": [
            { "type": "text", "text": "partial answer", "textSignature": "text-signature" },
            { "type": "thinking", "thinking": "", "thinkingSignature": "thinking-signature", "redacted": true },
            { "type": "toolCall", "id": "call-1", "name": "read", "arguments": { "path": "file" }, "thoughtSignature": "tool-signature", "namespace": "files" }
        ],
        "usage": {
            "input": 17, "output": 5, "cacheRead": 11, "cacheWrite": 7,
            "cacheWrite1h": 3, "reasoning": 2, "totalTokens": 40,
            "cost": { "input": 1.5, "output": 2.5, "cacheRead": 0.5, "cacheWrite": 0.25, "total": 4.75 }
        },
        "diagnostics": [{ "type": "provider_transport_failure", "timestamp": 6, "details": { "phase": "before_message_stream_start" } }],
        "stopReason": "pending",
        "timestamp": 7
    }))
    .expect("valid synthetic assistant")
}

fn make_stream(
    cancellation: CancellationToken,
    clock: Arc<dyn Clock>,
) -> (AssistantMessageEventStream, AssistantStreamProducer) {
    let message = fixture();
    create_assistant_message_event_stream(AssistantStreamInit {
        api: message.api,
        provider: message.provider,
        model: message.model,
        clock,
        cancellation,
    })
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future must be ready without driving the producer"),
    }
}

fn expected_abort(mut snapshot: AssistantMessage) -> AssistantMessage {
    snapshot.stop_reason = StopReason::Aborted;
    snapshot.error_message = Some(REQUEST_CANCELLED_MESSAGE.to_owned());
    snapshot.deferred = None;
    snapshot
}

fn terminal() -> AssistantMessageEvent {
    let mut message = fixture();
    message.stop_reason = StopReason::Stop;
    message.response_id = Some("terminal-response".parse().expect("response id"));
    AssistantMessageEvent::Done {
        reason: DoneReason::Stop,
        message: Box::new(message),
    }
}

fn assert_single_abort(stream: &mut AssistantMessageEventStream, expected: AssistantMessage) {
    assert_eq!(ready(stream.result()), expected);
    let event = ready(stream.next()).expect("exactly one terminal event");
    event.validate().expect("valid aborted event");
    assert_eq!(
        event,
        AssistantMessageEvent::Error {
            reason: ErrorReason::Aborted,
            error: Box::new(expected),
        }
    );
    let encoded = serde_json::to_value(&event).expect("valid terminal serialization");
    assert_eq!(encoded["reason"], "aborted");
    assert_eq!(encoded["error"]["errorMessage"], REQUEST_CANCELLED_MESSAGE);
    assert_eq!(ready(stream.next()), None);
}

#[test]
fn cancellation_retains_owned_content_usage_and_metadata_without_progress() {
    let token = CancellationToken::new();
    let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
    let snapshot = fixture();
    producer
        .publish_cancellation_snapshot(snapshot.clone())
        .expect("publish snapshot without any progress event");
    token.cancel();
    let result = ready(stream.result());
    assert_eq!(result.content, snapshot.content);
    assert_eq!(result.usage, snapshot.usage);
    assert_eq!(result.timestamp, snapshot.timestamp);
    assert_eq!(result.diagnostics, snapshot.diagnostics);
    assert_eq!(result.response_model, snapshot.response_model);
    assert_eq!(result.response_id, snapshot.response_id);
    assert_single_abort(&mut stream, expected_abort(snapshot));
}

struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn usage_only_publications_emit_nothing_and_retain_latest_usage() {
    let token = CancellationToken::new();
    let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
    let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut next = Box::pin(stream.next());
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut snapshot = fixture();
    snapshot.content.clear();
    producer
        .publish_cancellation_snapshot(snapshot.clone())
        .unwrap();
    snapshot.usage.input = SafeUnsigned::new(23).unwrap();
    snapshot.usage.total_tokens = SafeUnsigned::new(46).unwrap();
    producer
        .publish_cancellation_snapshot(snapshot.clone())
        .unwrap();
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(next);
    token.cancel();
    assert_single_abort(&mut stream, expected_abort(snapshot));
}

#[test]
fn default_cancellation_ignores_progress_until_explicit_snapshot_publication() {
    let token = CancellationToken::new();
    let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
    producer
        .push(AssistantMessageEvent::Start {
            partial: Box::new(fixture()),
        })
        .unwrap();
    token.cancel();
    let result = ready(stream.result());
    assert!(result.content.is_empty());
    assert_eq!(result.usage, Usage::zero());
    assert_eq!(result.timestamp, SafeUnsigned::new(99).unwrap());
    assert!(matches!(
        ready(stream.next()),
        Some(AssistantMessageEvent::Start { .. })
    ));
    assert_single_abort(&mut stream, result);
}

#[test]
fn snapshots_do_not_change_setup_or_last_producer_drop_fallbacks() {
    for setup in [false, true] {
        let (stream, producer) = make_stream(CancellationToken::new(), Arc::new(FixedClock));
        producer.publish_cancellation_snapshot(fixture()).unwrap();
        if setup {
            producer.fail_setup(StreamSetupFailure::Adapter).unwrap();
        }
        drop(producer);
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(result.content.is_empty());
        assert_eq!(result.usage, Usage::zero());
        assert_eq!(result.timestamp, SafeUnsigned::new(99).unwrap());
        if !setup {
            assert_eq!(
                result.error_message.as_deref(),
                Some(STREAM_PRODUCER_DROPPED_MESSAGE)
            );
        }
    }
}

#[test]
fn every_provisional_reason_becomes_aborted_and_clears_deferred_only() {
    for reason in [
        StopReason::Pending,
        StopReason::Stop,
        StopReason::Length,
        StopReason::ToolUse,
        StopReason::Deferred,
        StopReason::Error,
        StopReason::Aborted,
    ] {
        let token = CancellationToken::new();
        let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
        let mut snapshot = fixture();
        snapshot.stop_reason = reason;
        snapshot.error_message = Some("discarded prior failure detail".to_owned());
        snapshot.raw_stop_reason = Some("completed".parse().unwrap());
        snapshot.end_turn = Some(true);
        snapshot.deferred = Some(serde_json::from_value(serde_json::json!({
            "provider": "anthropic", "api": "anthropic-messages", "modelId": "requested-model", "id": "deferred-1"
        })).unwrap());
        producer
            .publish_cancellation_snapshot(snapshot.clone())
            .unwrap();
        token.cancel();
        let result = ready(stream.result());
        assert!(result.deferred.is_none());
        assert_eq!(result.raw_stop_reason, snapshot.raw_stop_reason);
        assert_eq!(result.end_turn, Some(true));
        assert_single_abort(&mut stream, expected_abort(snapshot));
    }
}

#[test]
fn identity_mismatch_preserves_previous_snapshot_without_exposing_input() {
    let token = CancellationToken::new();
    let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
    producer.publish_cancellation_snapshot(fixture()).unwrap();
    for change_api in [false, true] {
        let mut foreign = fixture();
        if change_api {
            foreign.api = "foreign-api-sentinel".parse().unwrap();
        } else {
            foreign.provider = "foreign-provider-sentinel".parse().unwrap();
        }
        let error = producer.publish_cancellation_snapshot(foreign).unwrap_err();
        assert_eq!(error, CancellationSnapshotError::IdentityMismatch);
        assert!(!format!("{error:?} {error}").contains("sentinel"));
        assert!(error.source().is_none());
    }
    token.cancel();
    assert_single_abort(&mut stream, expected_abort(fixture()));
    let mut foreign = fixture();
    foreign.api = "foreign".parse().unwrap();
    assert_eq!(
        producer.publish_cancellation_snapshot(foreign),
        Err(CancellationSnapshotError::AlreadySettled)
    );
}

#[test]
fn anthropic_fallback_model_is_retained() {
    // Pinned anthropic-messages.ts:594 assigns event.message.model directly;
    // factory-model equality would reject a legitimate fallback response.
    let token = CancellationToken::new();
    let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
    let mut snapshot = fixture();
    snapshot.model = "fallback-model".parse().unwrap();
    producer
        .publish_cancellation_snapshot(snapshot.clone())
        .unwrap();
    token.cancel();
    assert_single_abort(&mut stream, expected_abort(snapshot));
}

#[test]
fn precancelled_stream_rejects_snapshot_and_keeps_original_result() {
    let token = CancellationToken::new();
    token.cancel();
    let (mut stream, producer) = make_stream(token, Arc::new(FixedClock));
    let original = ready(stream.result());
    assert_eq!(
        producer.publish_cancellation_snapshot(fixture()),
        Err(CancellationSnapshotError::AlreadySettled)
    );
    assert!(original.content.is_empty());
    assert_eq!(original.usage, Usage::zero());
    assert_single_abort(&mut stream, original);
}

#[test]
fn snapshot_cancel_and_terminal_orders_keep_the_first_settlement() {
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let token = CancellationToken::new();
        let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
        let mut settled = false;
        let mut first = None;
        for operation in order {
            match operation {
                0 => assert_eq!(
                    producer.publish_cancellation_snapshot(fixture()),
                    if settled {
                        Err(CancellationSnapshotError::AlreadySettled)
                    } else {
                        Ok(())
                    }
                ),
                1 => {
                    token.cancel();
                    settled = true;
                }
                2 => {
                    assert_eq!(
                        producer.push(terminal()),
                        if settled {
                            Err(StreamProtocolError::AlreadySettled)
                        } else {
                            Ok(())
                        }
                    );
                    settled = true;
                }
                _ => unreachable!(),
            }
            if settled {
                let result = ready(stream.result());
                if let Some(original) = &first {
                    assert_eq!(&result, original);
                } else {
                    first = Some(result);
                }
            }
        }
        assert!(ready(stream.next()).is_some());
        assert_eq!(ready(stream.next()), None);
    }
}

#[test]
fn simultaneous_update_cancel_and_terminal_have_one_atomic_winner() {
    for _ in 0..32 {
        let token = CancellationToken::new();
        let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
        producer.publish_cancellation_snapshot(fixture()).unwrap();
        let mut latest = fixture();
        latest.usage.input = SafeUnsigned::new(123).unwrap();
        let barrier = Arc::new(Barrier::new(4));
        let update_producer = producer.clone();
        let update_barrier = barrier.clone();
        let candidate = latest.clone();
        let update = thread::spawn(move || {
            update_barrier.wait();
            update_producer.publish_cancellation_snapshot(candidate)
        });
        let cancel_barrier = barrier.clone();
        let cancel = thread::spawn(move || {
            cancel_barrier.wait();
            token.cancel();
        });
        let final_barrier = barrier.clone();
        let final_producer = producer.clone();
        let finish = thread::spawn(move || {
            final_barrier.wait();
            final_producer.push(terminal())
        });
        barrier.wait();
        let published = update.join().unwrap();
        cancel.join().unwrap();
        let finished = finish.join().unwrap();
        let result = ready(stream.result());
        if finished.is_ok() {
            assert_eq!(result.stop_reason, StopReason::Stop);
            assert_eq!(result.response_id.unwrap().as_str(), "terminal-response");
            assert!(ready(stream.next()).is_some());
            assert_eq!(ready(stream.next()), None);
        } else {
            assert_eq!(finished, Err(StreamProtocolError::AlreadySettled));
            let expected = if published.is_ok() { latest } else { fixture() };
            assert_single_abort(&mut stream, expected_abort(expected));
        }
        assert_eq!(
            producer.publish_cancellation_snapshot(fixture()),
            Err(CancellationSnapshotError::AlreadySettled)
        );
    }
}

#[test]
fn cancellation_with_32_live_producers_does_not_wait_for_producer_cooperation() {
    let parent = CancellationToken::new();
    let token = CancellationToken::linked(std::slice::from_ref(&parent));
    // The request owns its linked token for the invocation lifetime; parent
    // registrations intentionally hold only weak links to that token.
    let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
    let producers = (0..32).map(|_| producer.clone()).collect::<Vec<_>>();
    producers[7]
        .publish_cancellation_snapshot(fixture())
        .unwrap();
    drop(producer);
    parent.cancel();
    assert_single_abort(&mut stream, expected_abort(fixture()));
    for producer in producers {
        assert_eq!(
            producer.publish_cancellation_snapshot(fixture()),
            Err(CancellationSnapshotError::AlreadySettled)
        );
        assert_eq!(
            producer.push(terminal()),
            Err(StreamProtocolError::AlreadySettled)
        );
    }
    assert_eq!(ready(stream.next()), None);
}

struct ReenterWake {
    producer: AssistantStreamProducer,
    wakes: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Wake for ReenterWake {
    fn wake(self: Arc<Self>) {
        assert_eq!(
            self.producer.publish_cancellation_snapshot(fixture()),
            Err(CancellationSnapshotError::AlreadySettled)
        );
        assert_eq!(
            self.producer.push(terminal()),
            Err(StreamProtocolError::AlreadySettled)
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for ReenterWake {
    fn drop(&mut self) {
        assert_eq!(
            self.producer.publish_cancellation_snapshot(fixture()),
            Err(CancellationSnapshotError::AlreadySettled)
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cancellation_wakes_and_drops_reentrant_waiters_outside_the_stream_lock() {
    for result_waiter in [false, true] {
        let token = CancellationToken::new();
        let (mut stream, producer) = make_stream(token.clone(), Arc::new(FixedClock));
        producer.publish_cancellation_snapshot(fixture()).unwrap();
        let wakes = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(ReenterWake {
            producer: producer.clone(),
            wakes: wakes.clone(),
            drops: drops.clone(),
        }));
        if result_waiter {
            let mut waiter = Box::pin(stream.result());
            assert!(
                waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            token.cancel();
            assert_eq!(ready(waiter), expected_abort(fixture()));
        } else {
            let mut waiter = Box::pin(stream.next());
            assert!(
                waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            token.cancel();
            assert!(ready(waiter).is_some());
        }
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(ready(stream.result()), expected_abort(fixture()));
    }
}

struct ReenterClock {
    producer: Mutex<Option<AssistantStreamProducer>>,
    calls: AtomicUsize,
}

impl Clock for ReenterClock {
    fn now_unix_ms(&self) -> u64 {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let producer = self.producer.lock().unwrap().take();
        if let Some(producer) = producer {
            producer.publish_cancellation_snapshot(fixture()).unwrap();
        }
        100
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        panic!("unexpected sleep")
    }
}

#[test]
fn fallback_clock_runs_outside_lock_and_cancellation_uses_latest_snapshot() {
    let token = CancellationToken::new();
    let clock = Arc::new(ReenterClock {
        producer: Mutex::new(None),
        calls: AtomicUsize::new(0),
    });
    let (mut stream, producer) = make_stream(token.clone(), clock.clone());
    *clock.producer.lock().unwrap() = Some(producer.clone());
    token.cancel();
    assert_eq!(clock.calls.load(Ordering::SeqCst), 2);
    assert_single_abort(&mut stream, expected_abort(fixture()));
}

#[test]
fn snapshot_publication_does_not_poll_or_retain_a_host_callback() {
    let clock = Arc::new(ReenterClock {
        producer: Mutex::new(None),
        calls: AtomicUsize::new(0),
    });
    let (stream, producer) = make_stream(CancellationToken::new(), clock.clone());
    let clock_owners = Arc::strong_count(&clock);
    for _ in 0..8 {
        producer.publish_cancellation_snapshot(fixture()).unwrap();
    }
    assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&clock), clock_owners);
    drop(producer);
    drop(stream);
    assert_eq!(Arc::strong_count(&clock), 1);
}

#[test]
fn duration_belongs_to_new_terminal_messages_and_preserves_forwarded_timing() {
    for (timestamp, existing, expected) in [
        (99, None, true),
        (100, None, true),
        (7, None, false),
        (99, Some(31), true),
    ] {
        let (mut stream, producer) = make_stream(CancellationToken::new(), Arc::new(FixedClock));
        let mut message = fixture();
        message.timestamp = SafeUnsigned::new(timestamp).unwrap();
        message.stop_reason = StopReason::Stop;
        message.duration_ms = existing.map(|v| SafeUnsigned::new(v).unwrap());
        std::thread::sleep(Duration::from_millis(8));
        producer
            .push(AssistantMessageEvent::Done {
                reason: DoneReason::Stop,
                message: Box::new(message),
            })
            .unwrap();
        let result = ready(stream.result());
        assert_eq!(result.duration_ms.is_some(), expected);
        if let Some(existing) = existing {
            assert_eq!(result.duration_ms.unwrap().get(), existing);
        } else if expected {
            assert!(result.duration_ms.unwrap().get() >= 5);
        }
        match ready(stream.next()).unwrap() {
            AssistantMessageEvent::Done { message, .. } => assert_eq!(*message, result),
            _ => panic!("terminal event"),
        }
    }
}
