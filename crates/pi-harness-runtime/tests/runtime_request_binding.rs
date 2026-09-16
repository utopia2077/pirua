//! Independent review652 probes. Setup/Pump/Faux boilerplate is borrowed read-only
//! from runtime_r1_r4_support at candidate 3a9f1c1; scenarios below are independent.
mod runtime_request_binding_support;
use pi_agent_schema::*;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_request_binding_support::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct ReplaceOnAssistantStart {
    registry: Arc<pi_ai_core::ModelsRegistry>,
    replacement: Arc<Faux>,
    called: AtomicBool,
}
impl EventListener for ReplaceOnAssistantStart {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            if matches!(event, HarnessEvent::MessageStart(ref e) if matches!(e.message, LiveAgentMessage::Assistant(_)))
            {
                self.registry
                    .set_provider(Arc::new(Product {
                        faux: self.replacement.clone(),
                        auth: pi_ai_core::ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
                    }))
                    .await
                    .unwrap();
                self.called.store(true, Ordering::SeqCst);
            }
            Ok(())
        })
    }
}
#[test]
fn provider_capture_is_preserved_across_reentrant_message_start_listener() {
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let replacement = Arc::new(Faux::new(s.faux.model.clone()));
    let listener = Arc::new(ReplaceOnAssistantStart {
        registry: s.registry.clone(),
        replacement: replacement.clone(),
        called: AtomicBool::new(false),
    });
    let _subscription = c
        .harness
        .events()
        .on(HarnessEventType::MessageStart, listener.clone());
    let result = s.pump.drive(c.harness.prompt(PromptInput::Text {
        text: "replacement window".into(),
        images: vec![],
    }));
    assert!(listener.called.load(Ordering::SeqCst));
    println!(
        "result={result:?}, original_requests={}, replacement_requests={}",
        s.faux.requests.lock().unwrap().len(),
        replacement.requests.lock().unwrap().len()
    );
    assert!(
        replacement.requests.lock().unwrap().is_empty(),
        "replacement provider must not service the already captured intent"
    );
}

#[test]
fn capture_first_public_run_succeeds_and_executes_the_original_provider() {
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let replacement = Arc::new(Faux::new(s.faux.model.clone()));
    let listener = Arc::new(ReplaceOnAssistantStart {
        registry: s.registry.clone(),
        replacement: replacement.clone(),
        called: AtomicBool::new(false),
    });
    let _subscription = c
        .harness
        .events()
        .on(HarnessEventType::MessageStart, listener.clone());
    let result = s
        .pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "positive capture".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert!(listener.called.load(Ordering::SeqCst));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert_eq!(replacement.requests.lock().unwrap().len(), 0);
}

use std::task::Poll;
// Existing manual-intent fixture retained as an owning opposite-order control.
#[test]
fn replacement_before_capture_dispatches_neither_provider() {
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "replace".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut pending = false;
    for _ in 0..500 {
        s.pump.tick();
        let registers = s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap();
        pending=registers.iter().any(|r|matches!(r,Register::OperationState {value,..} if matches!(value.as_ref(),OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(AssistantPhase {generation:Generation::EffectPending{..},..})))));
        if pending {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert!(pending);
    let replacement = Arc::new(Faux::new(s.faux.model.clone()));
    s.pump
        .drive(s.registry.set_provider(Arc::new(Product {
            faux: replacement.clone(),
            auth: pi_ai_core::ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
    for _ in 0..1000 {
        s.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            let result = result.unwrap().unwrap();
            assert!(matches!(result.outcome, RunOutcome::FailedWithAssistant(_)));
            assert!(
                replacement.requests.lock().unwrap().is_empty(),
                "replacement must fail in-band after intent"
            );
            assert!(s.faux.requests.lock().unwrap().is_empty());
            return;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    panic!("replacement run did not finish");
}

struct ReentrantProduct {
    product: Product,
    probe: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    drop_probe: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl ReentrantProduct {
    fn check(&self) {
        let callback = self.probe.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback()
        }
    }
}
impl pi_ai_core::Provider for ReentrantProduct {
    fn id(&self) -> &pi_ai_core::ProviderId {
        pi_ai_core::Provider::id(&self.product)
    }
    fn name(&self) -> &str {
        "reentrant-capture-fixture"
    }
    fn models(&self) -> Result<Arc<[pi_ai_core::Model]>, pi_ai_core::ModelsError> {
        self.check();
        pi_ai_core::Provider::models(&self.product)
    }
    fn auth(&self) -> &pi_ai_core::ProviderAuth {
        self.check();
        pi_ai_core::Provider::auth(&self.product)
    }
    fn adapter(&self, api: &pi_ai_core::ApiId) -> Option<Arc<dyn pi_ai_core::ApiAdapter>> {
        self.check();
        pi_ai_core::Provider::adapter(&self.product, api)
    }
}
#[test]
fn request_provider_getters_can_reenter_the_lane_after_acceptance() {
    let s = Setup::new();
    let provider = Arc::new(ReentrantProduct {
        product: Product {
            faux: s.faux.clone(),
            auth: pi_ai_core::ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        },
        probe: Default::default(),
        drop_probe: Default::default(),
    });
    s.pump
        .drive(s.registry.set_provider(provider.clone()))
        .unwrap();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "reentrant getters".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut ready = false;
    for _ in 0..500 {
        s.pump.tick();
        let registers = s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap();
        ready=registers.iter().any(|r|matches!(r,Register::OperationState{value,..} if matches!(value.as_ref(),OperationState::Run(run) if matches!(&run.phase,RunPhase::Assistant(AssistantPhase{generation:Generation::Ready{..},..})))));
        if ready {
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert!(ready);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = calls.clone();
    let harness = c.harness.clone();
    *provider.probe.lock().unwrap() = Some(Arc::new(move || {
        let mut query = Box::pin(harness.get_thinking_level());
        assert!(
            matches!(poll(query.as_mut()), Poll::Ready(Ok(_))),
            "provider getter ran while lane line was held"
        );
        count.fetch_add(1, Ordering::SeqCst);
    }));
    for _ in 0..1000 {
        s.pump.tick();
        if let Poll::Ready(result) = poll(prompt.as_mut()) {
            assert!(matches!(
                result.unwrap().unwrap().outcome,
                RunOutcome::CompletedWithAssistant(_)
            ));
            assert!(calls.load(Ordering::SeqCst) >= 3);
            assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
            provider.probe.lock().unwrap().take();
            return;
        }
        let _ = s.pump.drive(c.harness.execute_action()).unwrap();
    }
    provider.probe.lock().unwrap().take();
    panic!("reentrant provider request did not finish")
}

impl Drop for ReentrantProduct {
    fn drop(&mut self) {
        if let Some(callback) = self.drop_probe.get_mut().unwrap().take() {
            callback();
        }
    }
}
#[test]
fn retired_registration_destructor_runs_after_lane_unlock() {
    let s = Setup::new();
    let provider = Arc::new(ReentrantProduct {
        product: Product {
            faux: s.faux.clone(),
            auth: pi_ai_core::ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        },
        probe: Default::default(),
        drop_probe: Default::default(),
    });
    s.pump
        .drive(s.registry.set_provider(provider.clone()))
        .unwrap();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = drops.clone();
    let harness = Arc::downgrade(&c.harness);
    *provider.drop_probe.lock().unwrap() = Some(Arc::new(move || {
        let harness = harness.upgrade().unwrap();
        let mut query = Box::pin(harness.get_thinking_level());
        assert!(
            matches!(poll(query.as_mut()), Poll::Ready(Ok(_))),
            "provider destructor ran under lane lock"
        );
        count.fetch_add(1, Ordering::SeqCst);
    }));
    drop(provider);
    let replacement = Arc::new(Faux::new(s.faux.model.clone()));
    let listener = Arc::new(ReplaceOnAssistantStart {
        registry: s.registry.clone(),
        replacement: replacement.clone(),
        called: AtomicBool::new(false),
    });
    let _subscription = c
        .harness
        .events()
        .on(HarnessEventType::MessageStart, listener);
    let result = s
        .pump
        .drive(c.harness.prompt(PromptInput::Text {
            text: "retire captured record".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(replacement.requests.lock().unwrap().is_empty());
}
