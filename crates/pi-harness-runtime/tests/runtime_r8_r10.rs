//! Public structural publication through actual Memory and Models.
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use std::sync::Arc;
fn factory(s: &Setup) -> RuntimeFactory<(), pi_agent_session::memory::MemoryStorage> {
    RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    )
}
#[test]
fn manual_compaction_publishes_real_summary_and_separate_usage() {
    let s = Setup::new();
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    for text in ["first turn with enough context", "second user turn"] {
        s.pump
            .drive(h.prompt(PromptInput::Text {
                text: text.into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
    }
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let result = s
        .pump
        .drive(h.compact(CompactOptions::default()))
        .unwrap()
        .unwrap();
    let CompactionOutcome::Completed(result) = result.outcome else {
        panic!("compaction did not complete")
    };
    let after = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(after.entries.len(), before.entries.len() + 1);
    assert_eq!(after.usage.len(), before.usage.len() + 2);
    assert!(
        after
            .entries
            .iter()
            .any(|e| e == &Entry::Compaction(result.entry.clone()))
    );
    assert!(!after.registers.iter().any(|r| matches!(
        r,
        Register::OperationMeta { .. }
            | Register::OperationState { .. }
            | Register::OperationPreparation { .. }
    )));
    let requests = s.faux.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for request in &requests[2..] {
        assert!(request.0.tools.is_none());
        assert_eq!(request.0.messages.len(), 1);
    }
}
#[test]
fn unsummarized_navigation_can_return_to_root_without_provider() {
    let s = Setup::new();
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    s.pump
        .drive(h.prompt(PromptInput::Text {
            text: "root navigation".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let result = s
        .pump
        .drive(h.navigate_tree(None, NavigateOptions::default()))
        .unwrap()
        .unwrap();
    assert!(
        matches!(result.outcome,NavigationOutcome::Completed(n) if n.new_leaf_id.is_none()&&n.summary_entry.is_none())
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}

struct Supplied;
impl HookHandler<BeforeCompaction> for Supplied {
    fn call(
        &self,
        event: BeforeCompactionEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
        Box::pin(async move {
            let mut usage = pi_ai_core::Usage::zero();
            usage.input = n(17);
            usage.total_tokens = n(17);
            Ok(Some(BeforeCompactionResult::Compaction(Box::new(
                BeforeCompactionSupply {
                    decline: None,
                    compaction: CompactResult {
                        summary: "hook supplied exact semantic".into(),
                        tokens_before: event.preparation.tokens_before,
                        retained_tail: event.preparation.retained_tail,
                        usage: Some(usage),
                        details: pi_ai_core::Presence::Missing,
                    },
                },
            ))))
        })
    }
}
#[test]
fn hook_result_keeps_actual_full_metadata_and_before_entry_usage() {
    let s = Setup::new();
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    for text in ["history", "tail"] {
        s.pump
            .drive(h.prompt(PromptInput::Text {
                text: text.into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
    }
    let parent = s.pump.drive(h.get_leaf_id()).unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let _hook = h
        .hooks()
        .before_compaction(None, Arc::new(Supplied))
        .unwrap();
    let result = s
        .pump
        .drive(h.compact(CompactOptions::default()))
        .unwrap()
        .unwrap();
    let CompactionOutcome::Completed(c) = result.outcome else {
        panic!("hook did not complete")
    };
    let after = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(s.faux.requests.lock().unwrap().len(), 2);
    assert_eq!(after.usage.len(), before.usage.len() + 1);
    let usage = after.usage.last().unwrap();
    assert!(usage.entry_id.is_none());
    assert_eq!(usage.usage.input, n(17));
    assert!(usage.seq < c.entry.seq);
    assert_eq!(c.entry.parent_id, parent);
    assert!(c.entry.from_hook);
    assert!(after.entries.contains(&Entry::Compaction(c.entry)));
}
struct BranchCapture(Arc<std::sync::Mutex<Vec<BranchPreparation>>>);
impl HookHandler<BeforeNavigation> for BranchCapture {
    fn call(
        &self,
        event: BeforeNavigationEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeNavigationResult>, HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event.preparation);
            Ok(None)
        })
    }
}
#[test]
fn branch_summary_selects_abandoned_source_side_and_empty_side_calls_no_provider() {
    let s = Setup::new();
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    s.pump
        .drive(h.prompt(PromptInput::Text {
            text: "shared common ancestor".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let common = s.pump.drive(h.get_leaf_id()).unwrap();
    s.pump
        .drive(h.prompt(PromptInput::Text {
            text: "abandoned SOURCE exclusive".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let source = s.pump.drive(h.get_leaf_id()).unwrap();
    s.pump
        .drive(h.navigate_tree(common.clone(), NavigateOptions::default()))
        .unwrap()
        .unwrap();
    s.pump
        .drive(h.prompt(PromptInput::Text {
            text: "TARGET exclusive must not summarize".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    let target = s.pump.drive(h.get_leaf_id()).unwrap();
    s.pump
        .drive(h.navigate_tree(source.clone(), NavigateOptions::default()))
        .unwrap()
        .unwrap();
    let capture = Arc::new(std::sync::Mutex::new(vec![]));
    let _hook = h
        .hooks()
        .before_navigation(None, Arc::new(BranchCapture(capture.clone())))
        .unwrap();
    let result = s
        .pump
        .drive(h.navigate_tree(
            target.clone(),
            NavigateOptions {
                summarize: Some(true),
                ..Default::default()
            },
        ))
        .unwrap()
        .unwrap();
    let NavigationOutcome::Completed(navigation) = result.outcome else {
        panic!("summary navigation did not complete")
    };
    let row = navigation.summary_entry.unwrap();
    assert_eq!(row.parent_id, target);
    assert_eq!(Some(row.from_id), source);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 4);
    let prep = serde_json::to_string(&capture.lock().unwrap()[0]).unwrap();
    assert!(prep.contains("abandoned SOURCE exclusive"));
    assert!(!prep.contains("TARGET exclusive"));
    assert!(!prep.contains("shared common ancestor"));
    s.pump
        .drive(h.navigate_tree(common, NavigateOptions::default()))
        .unwrap()
        .unwrap();
    let result = s
        .pump
        .drive(h.navigate_tree(
            target,
            NavigateOptions {
                summarize: Some(true),
                ..Default::default()
            },
        ))
        .unwrap()
        .unwrap();
    let NavigationOutcome::Completed(navigation) = result.outcome else {
        panic!("empty summary navigation did not complete")
    };
    assert!(
        navigation
            .summary_entry
            .unwrap()
            .summary
            .contains("No content to summarize")
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 4);
    assert!(capture.lock().unwrap()[1].messages.is_empty());
}

struct CaptureAndSupply(Arc<std::sync::Mutex<Vec<BeforeCompactionEvent>>>);
impl HookHandler<BeforeCompaction> for CaptureAndSupply {
    fn call(
        &self,
        event: BeforeCompactionEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event.clone());
            Ok(Some(BeforeCompactionResult::Compaction(Box::new(
                BeforeCompactionSupply {
                    decline: None,
                    compaction: CompactResult {
                        summary: "bounded compacted context".into(),
                        tokens_before: event.preparation.tokens_before,
                        retained_tail: event.preparation.retained_tail,
                        usage: None,
                        details: pi_ai_core::Presence::Missing,
                    },
                },
            ))))
        })
    }
}
#[test]
fn overflow_normalizes_error_excludes_prospective_response_and_resumes_once() {
    for length in [false, true] {
        let s = Setup::new();
        let h = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Automatic)))
            .unwrap()
            .harness;
        for text in ["older conversation", "more history"] {
            s.pump
                .drive(h.prompt(PromptInput::Text {
                    text: text.into(),
                    images: vec![],
                }))
                .unwrap()
                .unwrap();
        }
        let mut overflow = assistant(
            &s.faux.model,
            if length {
                pi_ai_core::StopReason::Length
            } else {
                pi_ai_core::StopReason::Error
            },
        );
        if !length {
            overflow.error_message = Some("request exceeds the context window".into())
        }
        overflow.usage.output = n(1);
        overflow.usage.total_tokens = n(1);
        s.faux.responses.lock().unwrap().push_back(overflow);
        s.faux
            .responses
            .lock()
            .unwrap()
            .push_back(assistant(&s.faux.model, pi_ai_core::StopReason::Stop));
        let capture = Arc::new(std::sync::Mutex::new(vec![]));
        let _hook = h
            .hooks()
            .before_compaction(None, Arc::new(CaptureAndSupply(capture.clone())))
            .unwrap();
        let result = s
            .pump
            .drive(h.prompt(PromptInput::Text {
                text: "overflow trigger".into(),
                images: vec![],
            }))
            .unwrap()
            .unwrap();
        assert!(
            matches!(result.outcome, RunOutcome::CompletedWithAssistant(_)),
            "length={length} result={:?} captures={:?}",
            result.outcome,
            capture.lock().unwrap()
        );
        let captures = capture.lock().unwrap();
        assert_eq!(captures.len(), 1);
        assert_eq!(captures[0].reason, HookCompactionReason::Overflow);
        assert!(!captures[0].preparation.retained_tail.iter().any(|m|matches!(m,AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(a)) if matches!(a.stop_reason,pi_ai_core::StopReason::Error|pi_ai_core::StopReason::Length))));
        let rows = s.pump.drive(s.store.snapshot()).unwrap().entries;
        let position = rows
            .iter()
            .position(|e| matches!(e, Entry::Compaction(_)))
            .unwrap();
        let Entry::Message(response) = &rows[position - 1] else {
            panic!("overflow response missing")
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(response)) = &response.message
        else {
            panic!("overflow assistant missing")
        };
        assert_eq!(response.stop_reason, pi_ai_core::StopReason::Error);
        let Entry::Compaction(compaction) = &rows[position] else {
            unreachable!()
        };
        assert_eq!(
            compaction.parent_id.as_ref(),
            Some(runtime_entry_id(&rows[position - 1]))
        );
        assert_eq!(s.faux.requests.lock().unwrap().len(), 4);
    }
}
fn runtime_entry_id(entry: &Entry) -> &EntryId {
    match entry {
        Entry::Message(e) => &e.id,
        Entry::Compaction(e) => &e.id,
        Entry::BranchSummary(e) => &e.id,
        Entry::Custom(e) => &e.id,
    }
}
#[test]
fn threshold_decline_marks_boundary_and_does_not_loop() {
    let s = Setup::new();
    let mut options = s.options(DriveMode::Automatic);
    options.compaction.enabled = true;
    options.compaction.reserve_tokens = s.faux.model.context_window;
    let h = s.pump.drive(factory(&s).create(options)).unwrap().harness;
    struct Decline(Arc<std::sync::atomic::AtomicUsize>);
    impl HookHandler<BeforeCompaction> for Decline {
        fn call(
            &self,
            _: BeforeCompactionEvent,
        ) -> BoxFuture<'_, Result<Option<BeforeCompactionResult>, HandlerError>> {
            Box::pin(async move {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Some(
                    BeforeCompactionResult::DeclineTrue(Default::default()),
                ))
            })
        }
    }
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _hook = h
        .hooks()
        .before_compaction(None, Arc::new(Decline(count.clone())))
        .unwrap();
    let result = s
        .pump
        .drive(h.prompt(PromptInput::Text {
            text: "threshold boundary".into(),
            images: vec![],
        }))
        .unwrap()
        .unwrap();
    assert!(matches!(
        result.outcome,
        RunOutcome::CompletedWithAssistant(_)
    ));
    assert!((1..=2).contains(&count.load(std::sync::atomic::Ordering::SeqCst)));
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
}
