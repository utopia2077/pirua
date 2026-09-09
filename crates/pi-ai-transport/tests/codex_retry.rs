use pi_ai_core::*;
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
fn ready<T>(f: impl Future<Output = T>) -> T {
    match Box::pin(f)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("pending"),
    }
}
struct NoSpawn;
impl TaskSpawner for NoSpawn {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
#[derive(Default)]
struct Clock {
    delays: Mutex<Vec<u64>>,
    hold: bool,
    drops: Arc<AtomicUsize>,
}
struct Pending(Arc<AtomicUsize>);
impl Future for Pending {
    type Output = Result<(), Cancelled>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl pi_ai_core::Clock for Clock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.delays.lock().unwrap().push(d.as_millis() as u64);
        if self.hold {
            Box::pin(Pending(self.drops.clone()))
        } else {
            Box::pin(async { Ok(()) })
        }
    }
}
fn runtime(clock: Arc<Clock>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), clock, Arc::new(NoSpawn))
}
#[test]
fn source_number_delay_and_strict_cap_observations_match() {
    let f: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/codex-retry/trace.json"
    ))
    .unwrap();
    for case in f["cases"].as_array().unwrap() {
        let mut headers = HttpHeaders::new();
        for (k, v) in case["headers"].as_object().unwrap() {
            headers.set(k, v.as_str().unwrap()).unwrap();
        }
        let delay = codex_retry_after_delay_ms(&headers, 7, &HttpRetryDateParser);
        let actual = match delay {
            None => json!({"kind":"missing"}),
            Some(v) if v.is_finite() => json!({"kind":"finite","value":v}),
            _ => json!({"kind":"infinite"}),
        };
        assert_eq!(actual["kind"], case["expected"]["kind"]);
        if actual["kind"] == "finite" {
            assert_eq!(
                actual["value"].as_f64(),
                case["expected"]["value"].as_f64(),
                "{}",
                case["headers"]
            );
        }
    }
    for case in f["caps"].as_array().unwrap() {
        let clock = Arc::new(Clock::default());
        let rt = runtime(clock);
        let delay = case["delay"].as_f64().unwrap_or(f64::INFINITY);
        let result = ready(retry_codex_request(
            &rt,
            CodexRetryOptions {
                max_retries: 1,
                max_retry_delay_ms: case["cap"].as_u64(),
            },
            |index| {
                Box::pin(async move {
                    if index == 0 {
                        Err(CodexAttemptFailure {
                            error: "opaque",
                            retry: CodexRetryObservation::ServerDelay(delay),
                        })
                    } else {
                        Ok(())
                    }
                })
            },
        ));
        assert_eq!(
            matches!(result, Err(CodexRetryError::DelayExceeded { .. })),
            case["error"].is_string(),
            "{case}"
        );
    }
}
#[test]
fn inclusive_attempts_exponential_backoff_no_jitter_and_timer_coercion() {
    let clock = Arc::new(Clock::default());
    let rt = runtime(clock.clone());
    let result = ready(retry_codex_request(
        &rt,
        CodexRetryOptions {
            max_retries: 3,
            ..Default::default()
        },
        |index| {
            Box::pin(async move {
                if index < 3 {
                    Err(CodexAttemptFailure {
                        error: index,
                        retry: CodexRetryObservation::Backoff,
                    })
                } else {
                    Ok(index)
                }
            })
        },
    ))
    .unwrap();
    assert_eq!(result, 3);
    assert_eq!(*clock.delays.lock().unwrap(), [1000, 2000, 4000]);
    for (delay, expected) in [
        (0.0, 1),
        (0.5, 1),
        (1.5, 1),
        (2147483648.0, 1),
        (f64::INFINITY, 1),
    ] {
        clock.delays.lock().unwrap().clear();
        ready(retry_codex_request(
            &rt,
            CodexRetryOptions {
                max_retries: 1,
                max_retry_delay_ms: Some(0),
            },
            |index| {
                Box::pin(async move {
                    if index == 0 {
                        Err(CodexAttemptFailure {
                            error: (),
                            retry: CodexRetryObservation::ServerDelay(delay),
                        })
                    } else {
                        Ok(())
                    }
                })
            },
        ))
        .unwrap();
        assert_eq!(*clock.delays.lock().unwrap(), [expected]);
    }
}
#[test]
fn cancellation_drops_noncooperative_attempt_and_sleep_without_successor() {
    for sleeping in [false, true] {
        let clock = Arc::new(Clock {
            hold: true,
            ..Default::default()
        });
        let rt = runtime(clock.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut operation = retry_codex_request(
            &rt,
            CodexRetryOptions {
                max_retries: 3,
                ..Default::default()
            },
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                let pending = Pending(drops.clone());
                Box::pin(async move {
                    if !sleeping {
                        pending.await.unwrap();
                    }
                    Err::<(), _>(CodexAttemptFailure {
                        error: (),
                        retry: CodexRetryObservation::Backoff,
                    })
                })
            },
        );
        assert!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        rt.cancellation.cancel();
        assert!(matches!(ready(operation), Err(CodexRetryError::Cancelled)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        if sleeping {
            assert_eq!(clock.drops.load(Ordering::SeqCst), 1);
        }
    }
}
