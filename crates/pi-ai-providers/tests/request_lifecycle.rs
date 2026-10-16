#![cfg(feature = "all-providers")]
use pi_ai_providers::api;
#[path = "request_lifecycle_support/configured_wrapper.rs"]
mod configured_wrapper_tests;
mod request_lifecycle_support;
use pi_ai_core::*;
use pi_ai_providers::api::{anthropic_messages::*, runtime::*, wire_json::WirePayload};
use request_lifecycle_support::*;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Transform {
    seen: Mutex<Vec<(String, Value)>>,
    replacement: bool,
    fail: bool,
}
impl RequestPayloadTransform for Transform {
    fn transform<'a>(
        &'a self,
        model: &'a Model,
        payload: &'a mut Value,
    ) -> BoxFuture<'a, Result<Option<Value>, ModelsError>> {
        Box::pin(async move {
            self.seen
                .lock()
                .unwrap()
                .push((model.provider.as_str().into(), payload.clone()));
            if self.fail {
                return Err(ModelsError::new(ModelsErrorCode::Provider));
            }
            let mut replacement = payload.clone();
            let target = if self.replacement {
                &mut replacement
            } else {
                payload
            };
            if model.provider.as_str() == "google" {
                target["config"]["temperature"] = json!(0.25);
            } else {
                target["temperature"] = json!(0.25);
            }
            Ok(self.replacement.then_some(replacement))
        })
    }
}
struct Captured;
impl PayloadMiddleware for Captured {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        p: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            let mut value = p.as_value().clone();
            value["captured"] = json!(true);
            Ok(Some(WirePayload::new(value)))
        })
    }
}
#[test]
fn actual_configured_adapters_raw_simple_mutation_replacement_and_ordinary_isolation() {
    run(selected_paths(false));
}
#[test]
fn actual_selected_factories_raw_simple_mutation_replacement_and_ordinary_isolation() {
    run(selected_paths(true));
}
async fn selected_paths(factory: bool) {
    for family in ["openai", "deepseek", "google", "anthropic"] {
        for replacement in [false, true] {
            let peer = Arc::new(Peer::default());
            let mut services = services(peer.clone());
            services.payload = Some(Arc::new(Captured));
            let (registry, model) = registry(if factory {
                selected_factory(family, services)
            } else {
                provider(family, services)
            })
            .await;
            let transform = Arc::new(Transform {
                replacement,
                ..Default::default()
            });
            let ordinary = registry.stream(
                model.clone(),
                context(),
                overrides(),
                StreamOptions {
                    wire: wire(),
                    runtime: runtime(),
                },
            );
            let (baseline, baseline_events) = drain(ordinary).await;
            assert_eq!(
                baseline.stop_reason,
                StopReason::Stop,
                "{family}: {baseline:?}"
            );
            assert!(transform.seen.lock().unwrap().is_empty());
            for simple in [false, true] {
                let options = RequestLifecycleOptions {
                    payload: Some(transform.clone()),
                };
                let observed = if simple {
                    registry.stream_simple_with_lifecycle(
                        model.clone(),
                        context(),
                        overrides(),
                        SimpleStreamOptions {
                            wire: SimpleStreamOptionsWire {
                                stream: wire(),
                                ..Default::default()
                            },
                            runtime: runtime(),
                        },
                        options,
                    )
                } else {
                    registry.stream_with_lifecycle(
                        model.clone(),
                        context(),
                        overrides(),
                        StreamOptions {
                            wire: wire(),
                            runtime: runtime(),
                        },
                        options,
                    )
                };
                let (result, events) = drain(observed.stream).await;
                assert_eq!(result, baseline, "{family}");
                assert_eq!(events, baseline_events, "{family}");
                assert_eq!(observed.metadata.snapshot().is_some(), family != "google");
            }
            let seen = transform.seen.lock().unwrap();
            assert_eq!(seen.len(), 2, "{family}");
            assert!(seen.iter().all(|(_, v)| v["captured"] == true));
            let calls = peer.calls.lock().unwrap();
            assert_eq!(calls.len(), 3, "{family}");
            let actual = if family == "google" {
                &calls[1]["generationConfig"]["temperature"]
            } else {
                &calls[1]["temperature"]
            };
            assert_eq!(actual, &json!(0.25), "{family}: {:?}", calls[1]);
            assert_ne!(calls[0].get("temperature"), Some(&json!(0.25)));
        }
    }
}

struct ResponseHook {
    calls: AtomicUsize,
    fail: bool,
}
impl ResponseMiddleware for ResponseHook {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(ModelsError::new(ModelsErrorCode::Provider))
            } else {
                Ok(())
            }
        })
    }
}
#[test]
fn admitted_unicode_anthropic_client_projection_is_passive_only_without_captured_middleware() {
    run(async {
        for captured in [false, true] {
            let peer = Arc::new(Peer::default());
            for _ in 0..2 {
                peer.script.lock().unwrap().push_back(Ok(response(
                    200,
                    "\u{100}",
                    body("anthropic"),
                )));
            }
            let hook = Arc::new(ResponseHook {
                calls: AtomicUsize::new(0),
                fail: false,
            });
            let mut api = AnthropicMessagesApi::new(services(peer.clone()));
            api.client = Some(peer.clone());
            if captured {
                api.services.response = Some(hook.clone());
            }
            let (registry, model) = registry(
                pi_ai_providers::providers::anthropic_provider(
                    Arc::new(api),
                    &pi_ai_providers::providers::FactoryAuthServices {
                        spawner: Arc::new(Host),
                        oauth: None,
                    },
                )
                .unwrap(),
            )
            .await;
            let baseline = drain(registry.stream(
                model.clone(),
                context(),
                overrides(),
                StreamOptions {
                    wire: wire(),
                    runtime: runtime(),
                },
            ))
            .await;
            let observed = registry.stream_with_lifecycle(
                model,
                context(),
                overrides(),
                StreamOptions {
                    wire: wire(),
                    runtime: runtime(),
                },
                Default::default(),
            );
            let result = drain(observed.stream).await;
            assert_eq!(result, baseline);
            assert_eq!(
                result.0.stop_reason,
                if captured {
                    StopReason::Error
                } else {
                    StopReason::Stop
                }
            );
            assert!(observed.metadata.snapshot().is_none());
            assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
            assert_eq!(peer.calls.lock().unwrap().len(), 2);
        }
    });
}
#[derive(Default)]
struct ConcurrentTransform {
    entered: AtomicUsize,
}
impl RequestPayloadTransform for ConcurrentTransform {
    fn transform<'a>(
        &'a self,
        _: &'a Model,
        p: &'a mut Value,
    ) -> BoxFuture<'a, Result<Option<Value>, ModelsError>> {
        Box::pin(async move {
            let index = self.entered.fetch_add(1, Ordering::SeqCst);
            while self.entered.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            p["temperature"] = json!(if index == 0 { 0.1 } else { 0.2 });
            Ok(None)
        })
    }
}
#[test]
fn concurrent_handler_reuse_has_detached_metadata_and_reader_does_not_retain_services() {
    run(async {
        let peer = Arc::new(Peer::default());
        let peer_weak = Arc::downgrade(&peer);
        let transform = Arc::new(ConcurrentTransform::default());
        let weak = Arc::downgrade(&transform);
        let (registry, model) = registry(selected_factory("openai", services(peer.clone()))).await;
        let a = registry.stream_with_lifecycle(
            model.clone(),
            context(),
            overrides(),
            StreamOptions {
                wire: wire(),
                runtime: runtime(),
            },
            RequestLifecycleOptions {
                payload: Some(transform.clone()),
            },
        );
        let b = registry.stream_with_lifecycle(
            model,
            context(),
            overrides(),
            StreamOptions {
                wire: wire(),
                runtime: runtime(),
            },
            RequestLifecycleOptions {
                payload: Some(transform.clone()),
            },
        );
        assert_eq!(drain(a.stream).await.0.stop_reason, StopReason::Stop);
        assert_eq!(drain(b.stream).await.0.stop_reason, StopReason::Stop);
        assert_eq!(transform.entered.load(Ordering::SeqCst), 2);
        let calls = peer.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(|v| v["temperature"] == 0.1));
        assert!(calls.iter().any(|v| v["temperature"] == 0.2));
        assert_ne!(a.metadata.snapshot(), b.metadata.snapshot());
        let original = a.metadata.snapshot();
        let mut detached = a.metadata.snapshot().unwrap();
        detached.headers.clear();
        assert_eq!(a.metadata.snapshot(), original);
        assert!(!format!("{:?}", a.metadata).contains("head-"));
        assert!(!format!("{:?}", original).contains("head-"));
        drop(transform);
        drop(registry);
        drop(peer);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(weak.upgrade().is_none());
        assert!(peer_weak.upgrade().is_none());
        assert!(a.metadata.snapshot().is_some());
    });
}
#[test]
fn transform_error_stops_before_transport_and_body_failure_retains_recorded_head() {
    run(async {
        let peer = Arc::new(Peer::default());
        let (registry, model) =
            registry(selected_factory("anthropic", services(peer.clone()))).await;
        let transform = Arc::new(Transform {
            fail: true,
            ..Default::default()
        });
        let observed = registry.stream_with_lifecycle(
            model.clone(),
            context(),
            overrides(),
            StreamOptions {
                wire: wire(),
                runtime: runtime(),
            },
            RequestLifecycleOptions {
                payload: Some(transform.clone()),
            },
        );
        assert_eq!(
            drain(observed.stream).await.0.stop_reason,
            StopReason::Error
        );
        assert_eq!(transform.seen.lock().unwrap().len(), 1);
        assert!(peer.calls.lock().unwrap().is_empty());
        assert!(observed.metadata.snapshot().is_none());
        peer.script.lock().unwrap().push_back(Ok(response(
            200,
            "complete-head",
            "data: not-json\n\n".into(),
        )));
        let observed = registry.stream_with_lifecycle(
            model,
            context(),
            overrides(),
            StreamOptions {
                wire: wire(),
                runtime: runtime(),
            },
            Default::default(),
        );
        assert_eq!(
            drain(observed.stream).await.0.stop_reason,
            StopReason::Error
        );
        assert_eq!(observed.metadata.snapshot().unwrap().status, 200);
    });
}

struct PendingTransform {
    entered: AtomicUsize,
    dropped: Arc<AtomicUsize>,
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl RequestPayloadTransform for PendingTransform {
    fn transform<'a>(
        &'a self,
        _: &'a Model,
        _: &'a mut Value,
    ) -> BoxFuture<'a, Result<Option<Value>, ModelsError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let _guard = DropCount(self.dropped.clone());
            std::future::pending().await
        })
    }
}
struct CancelCaptured;
impl PayloadMiddleware for CancelCaptured {
    fn on_payload<'a>(
        &'a self,
        r: &'a TextStreamRequest,
        _: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            r.options.runtime.cancellation.cancel();
            Ok(None)
        })
    }
}
#[test]
fn abort_before_during_and_between_captured_and_scoped_callbacks_preserves_cancellation() {
    run(async {
        for phase in ["before", "during", "between"] {
            let peer = Arc::new(Peer::default());
            let mut svc = services(peer.clone());
            if phase == "between" {
                svc.payload = Some(Arc::new(CancelCaptured));
            }
            let (registry, model) = registry(selected_factory("anthropic", svc)).await;
            let rt = runtime();
            let token = rt.cancellation.clone();
            let dropped = Arc::new(AtomicUsize::new(0));
            let transform = Arc::new(PendingTransform {
                entered: AtomicUsize::new(0),
                dropped: dropped.clone(),
            });
            if phase == "before" {
                token.cancel();
            }
            let observed = registry.stream_with_lifecycle(
                model,
                context(),
                overrides(),
                StreamOptions {
                    wire: wire(),
                    runtime: rt,
                },
                RequestLifecycleOptions {
                    payload: Some(transform.clone()),
                },
            );
            if phase == "during" {
                for _ in 0..20 {
                    if transform.entered.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                assert_eq!(transform.entered.load(Ordering::SeqCst), 1);
                token.cancel();
            }
            assert_eq!(
                drain(observed.stream).await.0.stop_reason,
                StopReason::Aborted
            );
            assert!(peer.calls.lock().unwrap().is_empty());
            assert!(observed.metadata.snapshot().is_none());
            assert_eq!(
                transform.entered.load(Ordering::SeqCst),
                usize::from(phase == "during")
            );
            assert_eq!(
                dropped.load(Ordering::SeqCst),
                usize::from(phase == "during")
            );
        }
    });
}

#[test]
fn captured_response_success_and_failure_keep_original_family_policy() {
    run(async {
        for family in ["openai", "deepseek", "google", "anthropic"] {
            for fail in [false, true] {
                let peer = Arc::new(Peer::default());
                let hook = Arc::new(ResponseHook {
                    calls: AtomicUsize::new(0),
                    fail,
                });
                let mut svc = services(peer.clone());
                svc.response = Some(hook.clone());
                let (registry, model) = registry(selected_factory(family, svc)).await;
                let baseline = drain(registry.stream(
                    model.clone(),
                    context(),
                    overrides(),
                    StreamOptions {
                        wire: wire(),
                        runtime: runtime(),
                    },
                ))
                .await;
                let observed = registry.stream_with_lifecycle(
                    model,
                    context(),
                    overrides(),
                    StreamOptions {
                        wire: wire(),
                        runtime: runtime(),
                    },
                    Default::default(),
                );
                let result = drain(observed.stream).await;
                assert_eq!(result, baseline, "{family} {fail}");
                assert_eq!(
                    result.0.stop_reason,
                    if fail && family != "google" {
                        StopReason::Error
                    } else {
                        StopReason::Stop
                    }
                );
                assert_eq!(
                    hook.calls.load(Ordering::SeqCst),
                    if family == "google" { 0 } else { 2 }
                );
                assert_eq!(
                    observed.metadata.snapshot().is_some(),
                    !fail && family != "google"
                );
            }
        }
    });
}

struct ConstructingPayload {
    constructions: AtomicUsize,
    polls: AtomicUsize,
    cancel_in_constructor: bool,
}
impl PayloadMiddleware for ConstructingPayload {
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        _: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        self.constructions.fetch_add(1, Ordering::SeqCst);
        if self.cancel_in_constructor {
            request.options.runtime.cancellation.cancel();
        }
        Box::pin(async move {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        })
    }
}
async fn construction_timing(pre_cancel: bool) {
    for observed in [false, true] {
        let peer = Arc::new(Peer::default());
        let captured = Arc::new(ConstructingPayload {
            constructions: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            cancel_in_constructor: !pre_cancel,
        });
        let mut svc = services(peer.clone());
        svc.payload = Some(captured.clone());
        let api = AnthropicMessagesApi::new(svc);
        let model = pi_ai_providers::catalog::anthropic_models().unwrap()[0].clone();
        let runtime = runtime();
        if pre_cancel {
            runtime.cancellation.cancel();
        }
        let request = TextStreamRequest {
            resolved: ResolvedRequest {
                model,
                endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
            },
            context: normalize_context(context()),
            auth: ResolvedAuth {
                api_key: Some(SecretString::new("fixture")),
                ..Default::default()
            },
            options: StreamOptions {
                wire: wire(),
                runtime,
            },
        };
        let scoped = Arc::new(Transform::default());
        let stream = if observed {
            let (invocation, _) = RequestLifecycleInvocation::new(RequestLifecycleOptions {
                payload: Some(scoped.clone()),
            });
            api.stream_with_lifecycle(request, invocation)
        } else {
            api.stream(request)
        };
        assert_eq!(drain(stream).await.0.stop_reason, StopReason::Aborted);
        assert_eq!(
            captured.constructions.load(Ordering::SeqCst),
            1,
            "observed={observed}"
        );
        assert_eq!(
            captured.polls.load(Ordering::SeqCst),
            0,
            "observed={observed}"
        );
        assert!(scoped.seen.lock().unwrap().is_empty());
        assert!(peer.calls.lock().unwrap().is_empty());
    }
}
#[test]
fn captured_payload_constructor_cancellation_preserves_zero_future_polls() {
    run(construction_timing(false));
}
#[test]
fn captured_payload_pre_cancelled_adapter_preserves_constructor_timing() {
    run(construction_timing(true));
}
