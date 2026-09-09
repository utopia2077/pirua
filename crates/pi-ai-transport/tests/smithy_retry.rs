use pi_ai_transport::*;
use serde_json::Value;
fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/smithy-retry/source.json"
    ))
    .unwrap()
}
fn class(value: &str) -> SmithyRetryClass {
    match value {
        "TRANSIENT" => SmithyRetryClass::Transient,
        "THROTTLING" => SmithyRetryClass::Throttling,
        "SERVER_ERROR" => SmithyRetryClass::ServerError,
        "CLIENT_ERROR" => SmithyRetryClass::ClientError,
        _ => panic!("unknown source class"),
    }
}
#[test]
fn both_captured_generations_match_source_quota_backoff_refund_and_hints() {
    let c = corpus();
    let rows = c.as_array().unwrap();
    assert_eq!(rows.len(), 5);
    for row in rows {
        let generation = SmithyRetryGeneration::captured(row["flag"].as_str());
        assert_eq!(
            generation == SmithyRetryGeneration::V2026,
            row["active"].as_bool().unwrap()
        );
        for case in row["cases"].as_array().unwrap() {
            let class = class(case["type"].as_str().unwrap());
            let mut state =
                SmithyRetryState::new(SmithyRetryMode::Standard, generation, 1767323045000);
            for event in case["events"].as_array().unwrap() {
                if event.get("initialDelay").is_some() {
                    assert_eq!(state.capacity(), event["capacity"].as_u64().unwrap() as u32);
                } else if event.get("retry").is_some() {
                    let before = state.capacity();
                    let delay = state.next_delay(class, 3.0, 0.5, None).unwrap().unwrap();
                    assert_eq!(delay.as_millis(), event["delay"].as_u64().unwrap() as u128);
                    assert_eq!(
                        before - state.capacity(),
                        event["cost"].as_u64().unwrap() as u32
                    );
                    assert_eq!(state.capacity(), event["capacity"].as_u64().unwrap() as u32);
                    assert_eq!(
                        state.attempts(),
                        event["retry"].as_u64().unwrap() as u32 + 1
                    );
                } else if event.get("rejected").is_some() {
                    assert!(!state.can_retry(class, 3.0));
                    assert!(
                        state
                            .next_delay(class, 3.0, f64::NAN, None)
                            .unwrap()
                            .is_none()
                    );
                    assert_eq!(state.capacity(), event["capacity"].as_u64().unwrap() as u32);
                } else {
                    state.success(1767323045000);
                    assert_eq!(
                        state.capacity(),
                        event["successCapacity"].as_u64().unwrap() as u32
                    );
                }
            }
        }
        for quota in row["quotas"].as_array().unwrap() {
            let mut state = SmithyRetryState::new(SmithyRetryMode::Standard, generation, 0);
            let mut count = 0;
            let mut last = 0;
            while let Some(delay) = state
                .next_delay(class(quota["type"].as_str().unwrap()), 1000.0, 0.5, None)
                .unwrap()
            {
                count += 1;
                last = delay.as_millis();
            }
            assert_eq!(count, quota["count"].as_u64().unwrap());
            assert_eq!(last, quota["lastDelay"].as_u64().unwrap() as u128);
            assert_eq!(state.capacity(), quota["capacity"].as_u64().unwrap() as u32);
        }
        for hint in row["hints"].as_array().unwrap() {
            let mut state = SmithyRetryState::new(SmithyRetryMode::Standard, generation, 0);
            let delay = state
                .next_delay(SmithyRetryClass::Transient, 3.0, 0.5, hint["hint"].as_f64())
                .unwrap()
                .unwrap();
            assert_eq!(delay.as_millis(), hint["delay"].as_u64().unwrap() as u128);
        }
        for parsed in row["parsed"].as_array().unwrap() {
            let mut headers = HttpHeaders::new();
            for entry in parsed["headerEntries"].as_array().unwrap() {
                headers
                    .append(entry[0].as_str().unwrap(), entry[1].as_str().unwrap())
                    .unwrap();
            }
            let actual = smithy_retry_after(&headers, 1767323045000);
            assert_eq!(
                actual.is_some(),
                parsed["present"].as_bool().unwrap(),
                "{:?}",
                parsed["headerEntries"]
            );
            if parsed["invalid"] == true {
                assert!(actual.unwrap().is_nan());
                let mut state = SmithyRetryState::new(SmithyRetryMode::Standard, generation, 0);
                assert_eq!(
                    state
                        .next_delay(SmithyRetryClass::Transient, 3.0, 0.5, actual)
                        .unwrap()
                        .unwrap()
                        .as_millis(),
                    1
                );
            } else {
                assert_eq!(actual, parsed["delay"].as_f64());
            }
        }
    }
}
#[cfg(feature = "native")]
mod adaptive {
    use super::*;
    use pi_ai_core::*;
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    struct ClockPort {
        now: AtomicU64,
        waits: Mutex<Vec<f64>>,
    }
    impl Clock for ClockPort {
        fn now_unix_ms(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
        fn sleep<'a>(
            &'a self,
            delay: Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(async move {
                let ms = delay.as_secs_f64() * 1000.0;
                self.waits.lock().unwrap().push(ms);
                self.now.fetch_add(ms.ceil() as u64, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    struct Spawn;
    impl TaskSpawner for Spawn {
        fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            panic!("retry policy does not spawn")
        }
    }
    #[test]
    fn adaptive_send_waits_precede_cooldown_in_both_source_generations() {
        let engine = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for row in corpus().as_array().unwrap() {
            let clock = Arc::new(ClockPort {
                now: AtomicU64::new(1767323045000),
                waits: Mutex::new(vec![]),
            });
            let runtime = RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                Arc::new(Spawn),
            );
            let mut state = SmithyRetryState::new(
                SmithyRetryMode::Adaptive,
                SmithyRetryGeneration::captured(row["flag"].as_str()),
                clock.now_unix_ms(),
            );
            for event in row["adaptive"].as_array().unwrap() {
                let kind = event["event"].as_str().unwrap();
                if kind == "initial" {
                    engine.block_on(state.acquire_send_token(&runtime)).unwrap();
                } else if kind == "success" {
                    state.success(clock.now_unix_ms());
                } else {
                    let class = class(kind);
                    state.observe_failure(class, clock.now_unix_ms());
                    let delay = state.next_delay(class, 10.0, 0.5, None).unwrap().unwrap();
                    assert_eq!(
                        delay.as_millis(),
                        event["retryDelay"].as_u64().unwrap() as u128
                    );
                    engine.block_on(state.acquire_send_token(&runtime)).unwrap();
                }
                assert_eq!(clock.now_unix_ms(), event["time"].as_u64().unwrap());
                let actual = std::mem::take(&mut *clock.waits.lock().unwrap());
                let expected = event["waits"].as_array().unwrap();
                assert_eq!(actual.len(), expected.len());
                for (a, e) in actual.iter().zip(expected) {
                    assert!((a - e.as_f64().unwrap()).abs() < 0.00001, "wait {a} vs {e}");
                }
                if let Some(delay) = event["retryDelay"].as_u64() {
                    clock.now.fetch_add(delay, Ordering::SeqCst);
                }
            }
        }
    }
}

#[cfg(feature = "native")]
#[test]
fn cancellation_drops_noncooperative_cooldown_and_adaptive_sleep() {
    use pi_ai_core::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::Duration,
    };
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Blocked {
        calls: AtomicUsize,
        drops: Arc<AtomicUsize>,
    }
    impl Clock for Blocked {
        fn now_unix_ms(&self) -> u64 {
            1767323045000
        }
        fn sleep<'a>(
            &'a self,
            _: Duration,
            _: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let guard = Guard(self.drops.clone());
            Box::pin(async move {
                let _guard = guard;
                std::future::pending().await
            })
        }
    }
    struct Spawn;
    impl TaskSpawner for Spawn {
        fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            panic!("retry cannot spawn")
        }
    }
    let engine = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for adaptive in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let clock = Arc::new(Blocked {
            calls: AtomicUsize::new(0),
            drops: drops.clone(),
        });
        let runtime = RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            clock.clone(),
            Arc::new(Spawn),
        );
        let mut state = SmithyRetryState::new(
            SmithyRetryMode::Adaptive,
            SmithyRetryGeneration::Legacy,
            clock.now_unix_ms(),
        );
        state.observe_failure(SmithyRetryClass::Throttling, clock.now_unix_ms());
        let mut future: BoxFuture<'_, Result<(), TransportError>> = if adaptive {
            Box::pin(state.acquire_send_token(&runtime))
        } else {
            Box::pin(smithy_retry_sleep(&runtime, Duration::from_secs(60)))
        };
        engine.block_on(std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }));
        assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        runtime.cancellation.cancel();
        assert_eq!(
            engine.block_on(future).unwrap_err().kind(),
            TransportErrorKind::Cancelled
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine
                .block_on(smithy_retry_sleep(&runtime, Duration::ZERO))
                .unwrap_err()
                .kind(),
            TransportErrorKind::Cancelled
        );
        assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    }
}
