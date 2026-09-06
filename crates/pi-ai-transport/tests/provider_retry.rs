use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, RequestEndpoint, RequestRuntime, SpawnError,
    TaskSpawner,
};
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, HttpTransport, ProviderAttemptFailure,
    ProviderRetryError, RawHttpClient, ResponseBody, RetryJitter, RetryProfile, TimeoutProfile,
    TransportError, TransportErrorKind, read_body_limited,
};
use serde_json::Value;
use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match poll(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
struct NeverClient;
impl RawHttpClient for NeverClient {
    fn send<'a>(
        &'a self,
        _request: &'a HttpRequest,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        panic!("generic helper must not send by itself")
    }
}
struct Jitter(f64);
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        self.0
    }
}
struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, _task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("retry helper does not spawn")
    }
}
#[derive(Default)]
struct TestClock {
    sleeps: Mutex<Vec<Duration>>,
    blocked: bool,
    dropped: Arc<AtomicUsize>,
}
struct DropWitness(Arc<AtomicUsize>);
impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        0
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _token: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(duration);
        let witness = DropWitness(self.dropped.clone());
        Box::pin(async move {
            let _witness = witness;
            if self.blocked {
                pending::<()>().await;
            }
            Ok(())
        })
    }
}
fn runtime(clock: Arc<TestClock>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), clock, Arc::new(Spawner))
}
fn transport(jitter: f64) -> HttpTransport {
    HttpTransport::new(Arc::new(NeverClient), Arc::new(Jitter(jitter)))
}
fn profile(max_retries: u64, cap: Option<u64>) -> RetryProfile {
    RetryProfile::SharedProvider {
        max_retries,
        max_retry_delay_ms: cap,
    }
}

// Deliberately neither Clone, Debug, Display, Error nor Serde. Transport must
// retain this opaque owned value without requiring or invoking those traits.
struct FamilyFailure {
    overflow: bool,
    retryable: bool,
    identity: Box<u64>,
    drops: Arc<AtomicUsize>,
}
impl Drop for FamilyFailure {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn failure(drops: &Arc<AtomicUsize>) -> FamilyFailure {
    FamilyFailure {
        overflow: true,
        retryable: true,
        identity: Box::new(77),
        drops: drops.clone(),
    }
}

#[test]
fn source_status_headers_delays_dates_and_caps_match_executed_vectors() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/provider-retry/vectors.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let mut headers = HttpHeaders::new();
        for pair in case["headers"].as_array().unwrap() {
            headers
                .append(pair[0].as_str().unwrap(), pair[1].as_str().unwrap())
                .unwrap();
        }
        let clock = Arc::new(TestClock::default());
        let runtime = runtime(clock.clone());
        let transport = transport(0.4);
        let attempts = AtomicUsize::new(0);
        let drops = Arc::new(AtomicUsize::new(0));
        let failures = case["index"].as_u64().unwrap_or(0) + 1;
        let result = ready(transport.retry_provider_request(
            &runtime,
            profile(failures, case["cap"].as_u64()),
            || {
                let index = attempts.fetch_add(1, Ordering::SeqCst);
                let result = if (index as u64) < failures {
                    Err(ProviderAttemptFailure::Provider {
                        status: case["status"].as_u64().map(|v| v as u16),
                        headers: Some(headers.clone()),
                        error: failure(&drops),
                    })
                } else {
                    Ok(17)
                };
                Box::pin(async move { result })
            },
        ));
        if case["retryable"] == false {
            assert!(
                matches!(result, Err(ProviderRetryError::Attempt(_))),
                "{}",
                case["id"]
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert!(clock.sleeps.lock().unwrap().is_empty());
        } else if case.get("policyMessage").is_some() {
            let Err(ProviderRetryError::Policy {
                error,
                previous: Some(ProviderAttemptFailure::Provider { error: family, .. }),
            }) = result
            else {
                panic!("expected retained cap failure")
            };
            assert_eq!(error.kind(), TransportErrorKind::RetryDelayExceeded);
            assert!(family.overflow && family.retryable);
            assert_eq!(*family.identity, 77);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(family);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(result.unwrap(), 17);
            assert_eq!(attempts.load(Ordering::SeqCst), failures as usize + 1);
            let actual = clock.sleeps.lock().unwrap().last().unwrap().as_secs_f64() * 1000.0;
            assert!(
                (actual - case["delay"].as_f64().unwrap()).abs() < 0.000001,
                "{}: {actual}",
                case["id"]
            );
        }
    }
}

#[test]
fn zero_default_other_failures_max_safe_and_policy_errors_keep_ownership() {
    for (profile_value, other) in [
        (RetryProfile::None, false),
        (profile(4, None), true),
        (profile(9_007_199_254_740_991, None), true),
    ] {
        let runtime = runtime(Arc::new(TestClock::default()));
        let transport = transport(0.4);
        let attempts = AtomicUsize::new(0);
        let drops = Arc::new(AtomicUsize::new(0));
        let result: Result<(), _> =
            ready(
                transport.retry_provider_request(&runtime, profile_value, || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let error = failure(&drops);
                    Box::pin(async move {
                        Err(if other {
                            ProviderAttemptFailure::Other(error)
                        } else {
                            ProviderAttemptFailure::Provider {
                                status: None,
                                headers: None,
                                error,
                            }
                        })
                    })
                }),
            );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(matches!(&result, Err(ProviderRetryError::Attempt(_))));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(result);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    let runtime = runtime(Arc::new(TestClock::default()));
    let transport = transport(0.4);
    let result: Result<(), ProviderRetryError<FamilyFailure>> = ready(
        transport.retry_provider_request(&runtime, profile(9_007_199_254_740_992, None), || {
            panic!("invalid retry budget must not invoke attempt")
        }),
    );
    assert!(matches!(
        result,
        Err(ProviderRetryError::Policy { previous: None, .. })
    ));
    let transport = HttpTransport::new(Arc::new(NeverClient), Arc::new(Jitter(f64::NAN)));
    let drops = Arc::new(AtomicUsize::new(0));
    let result: Result<(), _> =
        ready(
            transport.retry_provider_request(&runtime, profile(1, None), || {
                let error = failure(&drops);
                Box::pin(async move {
                    Err(ProviderAttemptFailure::Provider {
                        status: None,
                        headers: None,
                        error,
                    })
                })
            }),
        );
    let Err(ProviderRetryError::Policy {
        error,
        previous: Some(previous),
    }) = result
    else {
        panic!("invalid jitter retains family failure")
    };
    assert_eq!(error.kind(), TransportErrorKind::InvalidRequest);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!format!("{previous:?}").contains("77"));
    drop(previous);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn original_cancellation_wins_before_during_and_after_attempt_and_drops_losers() {
    let runtime = runtime(Arc::new(TestClock::default()));
    let transport = transport(0.0);
    runtime.cancellation.cancel();
    let result: Result<(), ProviderRetryError<FamilyFailure>> = ready(
        transport.retry_provider_request(&runtime, profile(1, None), || {
            panic!("pre-cancelled operation must not run")
        }),
    );
    assert!(matches!(
        result,
        Err(ProviderRetryError::Cancelled { previous: None })
    ));
    for ready_after_cancel in [false, true] {
        let runtime = super_runtime();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut future = transport.retry_provider_request(&runtime, profile(2, None), || {
            let guard = DropWitness(drops.clone());
            let token = runtime.cancellation.clone();
            Box::pin(async move {
                let _guard = guard;
                if ready_after_cancel {
                    token.cancel();
                    Ok::<_, ProviderAttemptFailure<FamilyFailure>>(1)
                } else {
                    pending().await
                }
            })
        });
        if !ready_after_cancel {
            assert!(poll(future.as_mut()).is_pending());
            runtime.cancellation.cancel();
        }
        assert!(matches!(
            ready(future),
            Err(ProviderRetryError::Cancelled { previous: None })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn raw_maximum_budget_matches_pinned_finite_success_and_recovery() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/provider-retry/vectors.json"
    ))
    .unwrap();
    for case in fixture["maxBudgetCases"].as_array().unwrap() {
        let runtime = super_runtime();
        let transport = transport(0.0);
        let calls = AtomicUsize::new(0);
        let result = ready(transport.retry_provider_request(
            &runtime,
            profile(case["maxRetries"].as_u64().unwrap(), None),
            || {
                let index = calls.fetch_add(1, Ordering::SeqCst) as u64;
                let mut headers = HttpHeaders::new();
                headers.set("retry-after-ms", "0").unwrap();
                let result = if index < case["failures"].as_u64().unwrap() {
                    Err(ProviderAttemptFailure::Provider {
                        status: Some(500),
                        headers: Some(headers),
                        error: (),
                    })
                } else {
                    Ok(17)
                };
                Box::pin(async move { result })
            },
        ));
        assert_eq!(result.unwrap(), case["result"].as_u64().unwrap());
        assert_eq!(
            calls.load(Ordering::SeqCst) as u64,
            case["calls"].as_u64().unwrap()
        );
    }
}
fn super_runtime() -> RequestRuntime {
    runtime(Arc::new(TestClock::default()))
}

#[test]
fn blocked_backoff_cancellation_retains_prior_failure_and_drops_sleep() {
    let clock = Arc::new(TestClock {
        blocked: true,
        ..Default::default()
    });
    let runtime = runtime(clock.clone());
    let transport = transport(0.0);
    let drops = Arc::new(AtomicUsize::new(0));
    let attempts = AtomicUsize::new(0);
    let mut operation =
        transport.retry_provider_request::<(), _, _>(&runtime, profile(1, None), || {
            attempts.fetch_add(1, Ordering::SeqCst);
            let error = failure(&drops);
            Box::pin(async move {
                Err(ProviderAttemptFailure::Provider {
                    status: Some(500),
                    headers: None,
                    error,
                })
            })
        });
    assert!(poll(operation.as_mut()).is_pending());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    runtime.cancellation.cancel();
    let Err(ProviderRetryError::Cancelled {
        previous: Some(previous),
    }) = ready(operation)
    else {
        panic!("cancelled backoff retains previous failure")
    };
    assert_eq!(clock.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(previous);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_retry_future_disposes_pending_attempt_and_backoff_without_cancelling_caller() {
    for backoff in [false, true] {
        let clock = Arc::new(TestClock {
            blocked: true,
            ..Default::default()
        });
        let runtime = runtime(clock.clone());
        let transport = transport(0.0);
        let drops = Arc::new(AtomicUsize::new(0));
        let attempt_drops = Arc::new(AtomicUsize::new(0));
        let mut operation =
            transport.retry_provider_request::<(), _, _>(&runtime, profile(1, None), || {
                let guard = DropWitness(attempt_drops.clone());
                let error = failure(&drops);
                Box::pin(async move {
                    let _guard = guard;
                    if !backoff {
                        pending::<()>().await;
                    }
                    Err(ProviderAttemptFailure::Provider {
                        status: Some(500),
                        headers: None,
                        error,
                    })
                })
            });
        assert!(poll(operation.as_mut()).is_pending());
        drop(operation);
        assert_eq!(attempt_drops.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(clock.dropped.load(Ordering::SeqCst), usize::from(backoff));
        assert!(!runtime.cancellation.is_cancelled());
    }
}

struct GateBody {
    gate: Arc<AtomicBool>,
    emitted: bool,
    fail: bool,
    closes: Arc<AtomicUsize>,
    wait_drops: Arc<AtomicUsize>,
}
impl ResponseBody for GateBody {
    fn next<'a>(
        &'a mut self,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        let guard = DropWitness(self.wait_drops.clone());
        Box::pin(async move {
            let _guard = guard;
            std::future::poll_fn(|_| {
                if self.gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            if self.fail {
                return Err(TransportError::new(TransportErrorKind::Connection));
            }
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            Ok(Some(b"context length exceeded".to_vec()))
        })
    }
    fn close(&mut self) {
        self.closes.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    attempts: AtomicUsize,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        _request: &'a HttpRequest,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Ok(self.responses.lock().unwrap().pop_front().unwrap())
        })
    }
}
fn complete_attempt<'a>(
    transport: &'a HttpTransport,
    request: &'a HttpRequest,
    runtime: &'a RequestRuntime,
    drops: &'a Arc<AtomicUsize>,
) -> BoxFuture<'a, Result<HttpResponse, ProviderAttemptFailure<FamilyFailure>>> {
    Box::pin(async move {
        let mut response = transport
            .send(request, runtime, RetryProfile::None, TimeoutProfile::None)
            .await
            .map_err(|_| ProviderAttemptFailure::Other(failure(drops)))?;
        if (200..300).contains(&response.status) {
            return Ok(response);
        }
        let bytes = read_body_limited(response.body.as_mut(), runtime, 1000)
            .await
            .map_err(|_| ProviderAttemptFailure::Other(failure(drops)))?;
        // Family-owned classification is intentionally outside transport.
        let mut error = failure(drops);
        error.overflow = bytes == b"context length exceeded";
        Err(ProviderAttemptFailure::Provider {
            status: Some(response.status),
            headers: Some(response.headers.clone()),
            error,
        })
    })
}

#[test]
fn full_sdk_attempt_waits_for_error_body_and_success_body_cannot_reenter_retry() {
    for (cancel, cap) in [(false, None), (true, None), (false, Some(1))] {
        let gate = Arc::new(AtomicBool::new(false));
        let closes = Arc::new(AtomicUsize::new(0));
        let wait_drops = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let body = |gate, fail| {
            Box::new(GateBody {
                gate,
                fail,
                emitted: false,
                closes: closes.clone(),
                wait_drops: wait_drops.clone(),
            }) as Box<dyn ResponseBody>
        };
        let mut headers = HttpHeaders::new();
        headers
            .set("retry-after-ms", if cap.is_some() { "2" } else { "0" })
            .unwrap();
        let client = Arc::new(Client {
            responses: Mutex::new(VecDeque::from([
                HttpResponse {
                    status: 429,
                    status_text: String::new(),
                    headers,
                    body: body(gate.clone(), false),
                },
                HttpResponse {
                    status: 200,
                    status_text: String::new(),
                    headers: HttpHeaders::new(),
                    body: body(Arc::new(AtomicBool::new(true)), true),
                },
            ])),
            attempts: AtomicUsize::new(0),
        });
        let transport = HttpTransport::new(client.clone(), Arc::new(Jitter(0.0)));
        let runtime = super_runtime();
        let request = HttpRequest {
            endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
            method: "POST".to_owned(),
            headers: HttpHeaders::new(),
            body: vec![],
            proxy: None,
        };
        let mut future = transport.retry_provider_request(&runtime, profile(1, cap), || {
            complete_attempt(&transport, &request, &runtime, &drops)
        });
        assert!(poll(future.as_mut()).is_pending());
        assert_eq!(
            client.attempts.load(Ordering::SeqCst),
            1,
            "SDK oracle attemptsBeforeErrorBody=1"
        );
        assert_eq!(closes.load(Ordering::SeqCst), 0);
        if cancel {
            runtime.cancellation.cancel();
            assert!(matches!(
                ready(future),
                Err(ProviderRetryError::Cancelled { .. })
            ));
            assert_eq!(client.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(closes.load(Ordering::SeqCst), 1);
            assert_eq!(wait_drops.load(Ordering::SeqCst), 1);
        } else {
            gate.store(true, Ordering::SeqCst);
            if cap.is_some() {
                let Err(ProviderRetryError::Policy {
                    error,
                    previous: Some(ProviderAttemptFailure::Provider { error: family, .. }),
                }) = ready(future)
                else {
                    panic!("expected retained classified SDK error on cap")
                };
                assert_eq!(error.kind(), TransportErrorKind::RetryDelayExceeded);
                assert!(family.overflow && family.retryable);
                assert_eq!(client.attempts.load(Ordering::SeqCst), 1);
                assert_eq!(closes.load(Ordering::SeqCst), 1);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(family);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                continue;
            }
            let mut response = ready(future).unwrap();
            assert_eq!(
                client.attempts.load(Ordering::SeqCst),
                2,
                "SDK oracle attemptsAfterErrorBody=2"
            );
            assert_eq!(closes.load(Ordering::SeqCst), 1);
            assert_eq!(
                ready(response.body.next(&runtime)).unwrap_err().kind(),
                TransportErrorKind::Connection
            );
            assert_eq!(client.attempts.load(Ordering::SeqCst), 2);
        }
    }
}
