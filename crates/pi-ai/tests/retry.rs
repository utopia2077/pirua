use pi_ai::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn fixture(reason: &str) -> AssistantMessage {
    serde_json::from_value(json!({"role":"assistant","api":"fixture","provider":"fixture","model":"fixture","timestamp":0,"content":[{"type":"text","text":"retained"}],"usage":{"input":17,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":17,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":if reason=="billing" {"error"} else {reason},"errorMessage":match reason {"error"=>"503 overloaded","billing"=>"billing 429",_=>"provider aborted"}})).unwrap()
}
fn error(stage: &str) -> AssistantRetryError {
    AssistantRetryError::with_source(std::io::Error::other(stage.to_owned()))
}
// These ordinary source cases use safe integer milliseconds. Keep JSON number
// representation identical without changing or rounding the observed f64.
fn fixture_delay(delay: RetryDelay) -> u64 {
    let raw = delay.raw_milliseconds();
    assert!(
        raw.is_finite() && raw.fract() == 0.0 && (0.0..=9_007_199_254_740_991.0).contains(&raw)
    );
    raw as u64
}
struct Harness {
    case: Value,
    sequence: Mutex<VecDeque<String>>,
    calls: Mutex<u64>,
    trace: Mutex<Vec<Value>>,
    cancellation: CancellationToken,
}
impl Harness {
    fn record(&self, v: Value) {
        self.trace.lock().unwrap().push(v);
    }
    fn fails(&self, s: &str) -> Result<(), AssistantRetryError> {
        if self.case["fail"] == s {
            Err(error(s))
        } else {
            Ok(())
        }
    }
}
impl AssistantRetryProducer for Harness {
    fn produce<'a>(&'a self) -> BoxFuture<'a, Result<AssistantMessage, AssistantRetryError>> {
        Box::pin(async move {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            self.record(json!(["produce", *n]));
            drop(n);
            let value = self
                .sequence
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra produce");
            if self.case["cancelProducer"] == true {
                self.cancellation.cancel();
            }
            if value == "reject" {
                return Err(error("produce"));
            }
            let mut response = fixture(&value);
            if !matches!(value.as_str(), "error" | "billing" | "aborted") {
                response.error_message = None;
            }
            Ok(response)
        })
    }
}
impl RetrySleeper for Harness {
    fn sleep<'a>(
        &'a self,
        delay: RetryDelay,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), RetrySleepError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(RetrySleepError::Cancelled);
            }
            self.record(json!(["sleep", fixture_delay(delay)]));
            if self.case["sleep"] == "fail" {
                return Err(RetrySleepError::Failed(error("sleep")));
            }
            if self.case["sleep"] == "cancel" {
                tokio::task::yield_now().await;
                c.cancel();
                c.cancelled().await;
                return Err(RetrySleepError::Cancelled);
            }
            Ok(())
        })
    }
}
impl RetryCallbacks for Harness {
    fn on_retry_scheduled<'a>(
        &'a self,
        a: SafeUnsigned,
        b: SafeUnsigned,
        d: RetryDelay,
        e: &'a str,
    ) -> BoxFuture<'a, Result<(), AssistantRetryError>> {
        Box::pin(async move {
            self.record(json!(["scheduled", a.get(), b.get(), fixture_delay(d), e]));
            if self.case["cancelScheduled"] == true {
                self.cancellation.cancel();
            }
            self.fails("scheduled")
        })
    }
    fn on_retry_attempt_start<'a>(&'a self) -> BoxFuture<'a, Result<(), AssistantRetryError>> {
        Box::pin(async move {
            self.record(json!(["start"]));
            self.fails("start")
        })
    }
    fn on_retry_finished<'a>(
        &'a self,
        s: bool,
        a: SafeUnsigned,
        e: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AssistantRetryError>> {
        Box::pin(async move {
            self.record(json!(["finish", s, a.get(), e]));
            self.fails("finish")
        })
    }
}
fn run_case(id: &str) {
    runtime().block_on(async {
 let cases:Value=serde_json::from_str(include_str!("../../../conformance/ai-compat/retry/cases.json")).unwrap();
 let source:Value=serde_json::from_str(include_str!("../../../conformance/ai-compat/retry/source-observations.json")).unwrap();
 let case=cases.as_array().unwrap().iter().find(|v|v["id"]==id).unwrap().clone();
 let expected=source["observations"].as_array().unwrap().iter().find(|v|v["id"]==id).unwrap();
 let h=Harness {sequence:Mutex::new(case["sequence"].as_array().unwrap().iter().map(|v|v.as_str().unwrap().to_owned()).collect()),case:case.clone(),calls:Mutex::new(0),trace:Mutex::new(vec![]),cancellation:CancellationToken::new()};
 if case["preCancelled"]==true {h.cancellation.cancel();}
 let policy=match case.get("policy") {Some(Value::Null)=>None,Some(p)=>Some(serde_json::from_value(p.clone()).unwrap()),None=>Some(serde_json::from_value(json!({"enabled":true,"maxRetries":2,"baseDelayMs":2})).unwrap())};
 let result=retry_assistant_call(&h,policy.as_ref(),&h.cancellation,&h,Some(&h)).await;
 let result=match result {Ok(r)=>{
 let normalized=expected["result"]["stopReason"]=="aborted" && expected["result"]["errorMessage"].is_null();
 if normalized {assert_eq!(r.error_message.as_deref(),Some("Retry backoff aborted"));assert!(serde_json::to_value(&r).is_ok());}
 json!({"stopReason":r.stop_reason,"errorMessage":if normalized {None}else{r.error_message.as_deref()},"content":r.content,"input":r.usage.input.get()})
 },Err(e)=>json!({"rejected":e.source().unwrap().to_string()})};
 assert_eq!(json!({"id":id,"trace":*h.trace.lock().unwrap(),"result":result}),*expected,"{id}");
 });
}
macro_rules! cases {($($name:ident => $id:literal),* $(,)?)=>{$(#[test] fn $name(){run_case($id);})*};}
cases! {
 retry_loop_policy_absent=>"retry-loop.policy-absent",
 retry_loop_policy_disabled=>"retry-loop.policy-disabled",
 retry_loop_zero_retries=>"retry-loop.zero-retries",
 retry_loop_immediate_success=>"retry-loop.immediate-success",
 retry_loop_immediate_aborted=>"retry-loop.immediate-aborted",
 retry_loop_immediate_nonretryable=>"retry-loop.immediate-nonretryable",
 retry_loop_exhaustion_call_count=>"retry-loop.exhaustion-call-count",
 retry_loop_later_success=>"retry-loop.later-success",
 retry_loop_aborted_after_scheduled=>"retry-loop.aborted-after-scheduled",
 retry_loop_observer_order=>"retry-loop.observer-order",
 retry_loop_exponential_delays=>"retry-loop.exponential-delays",
 retry_loop_cancel_during_sleep=>"retry-loop.cancel-during-sleep",
 retry_loop_finish_only_after_retry=>"retry-loop.finish-only-after-retry",
 retry_loop_finish_outcomes=>"retry-loop.finish-outcomes",
}
#[test]
fn retry_rejected_operations_preserve_source_stage() {
    for id in [
        "control.produce-reject-initial",
        "control.produce-reject-retry",
        "control.scheduled-reject",
        "control.start-reject",
        "control.finish-reject",
        "control.sleep-failure",
        "control.sleep-abort-finish-reject",
    ] {
        run_case(id);
    }
}
#[test]
fn retry_cancellation_belongs_to_sleep() {
    for id in [
        "control.pre-cancelled-sleep",
        "control.cancel-in-producer-success",
        "control.cancel-in-scheduled",
    ] {
        run_case(id);
    }
}
#[test]
fn retry_complete_pinned_source_and_timer_fingerprint() {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/ai-compat/retry");
    let output = std::process::Command::new("/usr/bin/node")
        .arg(root.join("source.mjs"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-compat/retry/source-observations.json"
    ))
    .unwrap();
    assert_eq!(actual, expected);
    let frozen = [
        "policy-absent",
        "policy-disabled",
        "zero-retries",
        "immediate-success",
        "immediate-aborted",
        "immediate-nonretryable",
        "exhaustion-call-count",
        "later-success",
        "aborted-after-scheduled",
        "observer-order",
        "exponential-delays",
        "cancel-during-sleep",
        "finish-only-after-retry",
        "finish-outcomes",
    ];
    let ids: Vec<_> = actual["observations"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["id"].as_str())
        .filter(|id| id.starts_with("retry-loop."))
        .collect();
    assert_eq!(ids, frozen.map(|id| format!("retry-loop.{id}")));
}
struct Extreme {
    calls: Mutex<u64>,
    raw: Mutex<Vec<RetryDelay>>,
}
impl AssistantRetryProducer for Extreme {
    fn produce<'a>(&'a self) -> BoxFuture<'a, Result<AssistantMessage, AssistantRetryError>> {
        Box::pin(async move {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            let mut r = fixture(if *n > 1025 { "stop" } else { "error" });
            if *n > 1025 {
                r.error_message = None;
            }
            Ok(r)
        })
    }
}
impl RetrySleeper for Extreme {
    fn sleep<'a>(
        &'a self,
        d: RetryDelay,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), RetrySleepError>> {
        Box::pin(async move {
            self.raw.lock().unwrap().push(d);
            Ok(())
        })
    }
}
struct ClockFixture(Mutex<Vec<Duration>>);
impl Clock for ClockFixture {
    fn now_unix_ms(&self) -> u64 {
        0
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(d);
            c.cancel();
            Err(Cancelled)
        })
    }
}
#[test]
fn retry_extreme_numeric_observations_and_clock_projection() {
    runtime().block_on(async {
        for base in [0, 1, 9_007_199_254_740_991_u64] {
            let h = Extreme {
                calls: Mutex::new(0),
                raw: Mutex::new(vec![]),
            };
            let p: RetryPolicy = serde_json::from_value(
                json!({"enabled":true,"maxRetries":1025,"baseDelayMs":base}),
            )
            .unwrap();
            assert_eq!(
                retry_assistant_call(&h, Some(&p), &CancellationToken::new(), &h, None)
                    .await
                    .unwrap()
                    .stop_reason,
                StopReason::Stop
            );
            let raw = h.raw.lock().unwrap().clone();
            assert_eq!(raw.len(), 1025);
            let source: Value = serde_json::from_str(include_str!(
                "../../../conformance/ai-compat/retry/source-observations.json"
            ))
            .unwrap();
            let expected = source["extremes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["base"].as_u64() == Some(base))
                .unwrap();
            let bits: Vec<String> = raw
                .iter()
                .map(|delay| {
                    let value = delay.raw_milliseconds();
                    if value.is_nan() {
                        "NaN".into()
                    } else if value.is_infinite() {
                        "Infinity".into()
                    } else {
                        format!("{:016x}", value.to_bits())
                    }
                })
                .collect();
            assert_eq!(json!(bits), expected["delays"]);
            assert_eq!(
                *h.calls.lock().unwrap(),
                expected["calls"].as_u64().unwrap()
            );
            assert_eq!(raw[0].raw_milliseconds(), base as f64);
            if base == 0 {
                assert!(raw[1024].raw_milliseconds().is_nan());
            } else {
                assert_eq!(raw[1024].raw_milliseconds(), f64::INFINITY);
            }
            assert_eq!(raw[1024].node_duration(), Duration::from_millis(1));
            let clock = Arc::new(ClockFixture(Mutex::new(vec![])));
            let sleeper = ClockRetrySleeper::new(clock.clone());
            let c = CancellationToken::new();
            assert!(matches!(
                sleeper.sleep(raw[1024], &c).await,
                Err(RetrySleepError::Cancelled)
            ));
            assert_eq!(*clock.0.lock().unwrap(), [Duration::from_millis(1)]);
            assert!(matches!(
                sleeper.sleep(raw[0], &c).await,
                Err(RetrySleepError::Cancelled)
            ));
            assert_eq!(clock.0.lock().unwrap().len(), 1);
        }
    });
}
#[test]
fn retry_error_formatting_redacts_source() {
    let e = error("synthetic-private-detail");
    assert!(!format!("{e} {e:?}").contains("synthetic-private-detail"));
    assert!(e.source().is_some());
}

#[test]
fn retry_causal_controls_reject_wrong_count_order_classifier_and_sleep() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/ai-compat/retry/causal.mjs");
    let output = std::process::Command::new("/usr/bin/node")
        .arg(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().lines().count(), 4);
}
