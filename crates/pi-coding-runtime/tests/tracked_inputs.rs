use super::*;
use std::{collections::BTreeSet, sync::atomic::AtomicUsize};
use tokio::sync::{Notify, oneshot::error::TryRecvError};

fn blocked_first() -> (
    pi_agent::StreamFn,
    Arc<Notify>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let stream: pi_agent::StreamFn = {
        let (entered, release, count) = (entered.clone(), release.clone(), count.clone());
        Arc::new(move |request| {
            let (entered, release, count) = (entered.clone(), release.clone(), count.clone());
            Box::pin(async move {
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered.notify_one();
                    tokio::select! {
                        _ = release.notified() => {},
                        _ = request.cancellation.cancelled() => return Err(AgentError::Callback("cancelled fixture".into())),
                    }
                }
                Ok(response(request, json!({})))
            })
        })
    };
    (stream, entered, release, count)
}
async fn session_with(
    stream: pi_agent::StreamFn,
    hooks: Arc<dyn SessionHooks>,
    store: Arc<dyn SessionStore>,
) -> CodingSession {
    let mut configured = settings();
    configured.retry.enabled = false;
    configured.compaction.enabled = false;
    CodingSessionBuilder::new(
        SessionManager::create(store, "/tmp").unwrap(),
        model(),
        stream,
    )
    .with_hooks(hooks)
    .with_settings(configured)
    .with_tools(vec![])
    .with_image_policy(false, false)
    .with_system_prompt("tracking")
    .build()
    .await
    .unwrap()
}
fn queued(admission: QueueAdmission) -> InputTicket {
    match admission {
        QueueAdmission::Queued(ticket) => ticket,
        QueueAdmission::Handled => panic!("expected queued admission"),
    }
}
fn inserted(value: InputResolution) -> (String, AgentMessage) {
    match value {
        InputResolution::Inserted { entry_id, message } => (entry_id, message),
        other => panic!("expected inserted, got {other:?}"),
    }
}

#[test]
fn insertion_receipt_precedes_provider_completion_and_legacy_end_precedes_commit() {
    runtime().block_on(async {
        let (stream, entered, release, _) = blocked_first();
        let session = session_with(stream, Arc::new(NoopSessionHooks), Arc::new(MemoryStore::new())).await;
        let mut history = session.observe_history().unwrap();
        let before = Arc::new(Mutex::new(Vec::new()));
        let capture = before.clone();
        let weak = session.downgrade();
        session.subscribe(Arc::new(move |event| {
            let (capture, weak) = (capture.clone(), weak.clone());
            Box::pin(async move {
                if matches!(&event, SessionEvent::Agent(AgentEvent::MessageEnd { message }) if message.role() == "user") {
                    capture.lock().unwrap().push(weak.upgrade().unwrap().snapshot().entries.len());
                }
                Ok(())
            })
        }));
        let (ticket, driver) = session.tracked_prompt("hello", PromptOptions::default());
        let cancellation = ticket.cancellation_handle();
        let running = tokio::spawn(driver);
        entered.notified().await;
        let (id, message) = inserted(ticket.resolution.await.unwrap());
        assert!(!running.is_finished());
        assert_eq!(message.role(), "user");
        assert!(session.snapshot().entries.iter().any(|e| e.id == id));
        loop {
            let HistoryChange::Committed { appended, .. } = history.changes.recv().await.unwrap() else { panic!("expected commit") };
            if appended.iter().any(|e| e.id == id) { break; }
        }
        let user_position = session.snapshot().entries.iter().position(|e| e.id == id).unwrap();
        assert_eq!(before.lock().unwrap().as_slice(), &[user_position]);
        cancellation.cancel(); // insertion is irrevocable; this must not abort its driver.
        assert!(session.is_streaming());
        release.notify_one();
        running.await.unwrap().unwrap();
    });
}
struct RewriteInput;
impl SessionHooks for RewriteInput {
    fn input(
        &self,
        event: InputEvent,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async move {
            Ok(InputResult::Transform {
                text: format!("queued:{}", event.text),
                images: None,
            })
        })
    }
    fn transform_message(
        &self,
        phase: MessagePhase,
        message: AgentMessage,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<AgentMessage, AgentError>> {
        Box::pin(async move {
            if phase == MessagePhase::End && message.role() == "user" {
                let mut value = serde_json::to_value(message).unwrap();
                value["timestamp"] = json!(7); // duplicate timestamp is intentionally not identity.
                value["content"][0]["text"] = json!("all finalized text is identical");
                Ok(serde_json::from_value(value).unwrap())
            } else {
                Ok(message)
            }
        })
    }
}
#[test]
fn duplicate_transformed_inputs_keep_exact_images_and_identity_in_both_queue_modes() {
    runtime().block_on(async {
        for mode in [QueueMode::OneAtATime, QueueMode::All] {
            let (stream, entered, release, _) = blocked_first();
            let session =
                session_with(stream, Arc::new(RewriteInput), Arc::new(MemoryStore::new())).await;
            session.set_steering_mode(mode);
            session.set_follow_up_mode(mode);
            let (first, driver) = session.tracked_prompt("same", PromptOptions::default());
            let running = tokio::spawn(driver);
            entered.notified().await;
            let mut inputs = Vec::new();
            for (index, delivery) in [
                Delivery::Steer,
                Delivery::FollowUp,
                Delivery::Steer,
                Delivery::FollowUp,
            ]
            .into_iter()
            .enumerate()
            {
                let data = format!("image-{index}");
                let image = serde_json::from_value(
                    json!({"type":"image","data":data,"mimeType":"image/png"}),
                )
                .unwrap();
                let ticket = queued(
                    session
                        .queue_tracked("same", vec![image], delivery)
                        .await
                        .unwrap(),
                );
                inputs.push((ticket, data));
            }
            let (steer, follow) = session.queued_inputs();
            assert_eq!((steer.len(), follow.len()), (2, 2));
            assert_eq!(steer[1].id, inputs[2].0.id);
            assert_eq!(follow[0].id, inputs[1].0.id);
            release.notify_one();
            running.await.unwrap().unwrap();
            let mut entry_ids = BTreeSet::from([inserted(first.resolution.await.unwrap()).0]);
            for (ticket, data) in inputs {
                let (id, message) = inserted(ticket.resolution.await.unwrap());
                assert!(entry_ids.insert(id));
                let value = serde_json::to_value(message).unwrap();
                assert_eq!(value["timestamp"], json!(7));
                assert_eq!(
                    value["content"][0]["text"],
                    json!("all finalized text is identical")
                );
                assert_eq!(value["content"][1]["data"], json!(data));
            }
            assert_eq!(session.pending_message_count(), 0);
            assert!(session.queued_inputs().0.is_empty());
            assert!(session.queued_inputs().1.is_empty());
        }
    });
}
struct BlockDrained {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
impl SessionHooks for BlockDrained {
    fn transform_message(
        &self,
        phase: MessagePhase,
        message: AgentMessage,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<AgentMessage, AgentError>> {
        Box::pin(async move {
            if phase == MessagePhase::Start
                && serde_json::to_value(&message).unwrap()["content"][0]["text"] == "hold"
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(message)
        })
    }
}
#[test]
fn clear_withdraws_only_the_actual_fifo_and_preserves_an_already_drained_input() {
    runtime().block_on(async {
        let (stream, entered, release, _) = blocked_first();
        let drained = Arc::new(Notify::new());
        let finish_hook = Arc::new(Notify::new());
        let session = session_with(
            stream,
            Arc::new(BlockDrained {
                entered: drained.clone(),
                release: finish_hook.clone(),
            }),
            Arc::new(MemoryStore::new()),
        )
        .await;
        let (first, driver) = session.tracked_prompt("start", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        let mut held = queued(
            session
                .queue_tracked("hold", vec![], Delivery::Steer)
                .await
                .unwrap(),
        );
        let removed = queued(
            session
                .queue_tracked("remove", vec![], Delivery::Steer)
                .await
                .unwrap(),
        );
        release.notify_one();
        drained.notified().await;
        let cleared = session.clear_queue_tracked().await.unwrap();
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].id, removed.id);
        assert!(matches!(
            removed.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        assert!(matches!(
            held.resolution.try_recv(),
            Err(TryRecvError::Empty)
        ));
        finish_hook.notify_one();
        running.await.unwrap().unwrap();
        inserted(first.resolution.await.unwrap());
        inserted(held.resolution.await.unwrap());
    });
}
struct DispositionHooks;
impl SessionHooks for DispositionHooks {
    fn input(
        &self,
        event: InputEvent,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async move {
            match event.text.as_str() {
                "handled" => Ok(InputResult::Handled),
                "rejected" => Err(AgentError::Callback("reject input".into())),
                _ => Ok(InputResult::Continue),
            }
        })
    }
}
#[test]
fn tickets_resolve_handled_rejected_cancelled_unpolled_driver_and_disposal() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![]);
        let session = session_with(
            stream,
            Arc::new(DispositionHooks),
            Arc::new(MemoryStore::new()),
        )
        .await;
        let (handled, driver) = session.tracked_prompt("handled", PromptOptions::default());
        driver.await.unwrap();
        assert!(matches!(
            handled.resolution.await.unwrap(),
            InputResolution::Handled
        ));
        assert!(matches!(
            session
                .queue_tracked("handled", vec![], Delivery::Steer)
                .await
                .unwrap(),
            QueueAdmission::Handled
        ));
        let (rejected, driver) = session.tracked_prompt("rejected", PromptOptions::default());
        assert!(driver.await.is_err());
        assert!(matches!(
            rejected.resolution.await.unwrap(),
            InputResolution::Rejected { .. }
        ));
        let (unpolled, driver) = session.tracked_prompt("dropped", PromptOptions::default());
        drop(driver);
        assert!(matches!(
            unpolled.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        let pending = queued(
            session
                .queue_tracked("cancel me", vec![], Delivery::FollowUp)
                .await
                .unwrap(),
        );
        pending.cancel();
        assert!(matches!(
            pending.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        assert!(session.queued_inputs().1.is_empty());
        let pending = queued(
            session
                .queue_tracked("dispose me", vec![], Delivery::Steer)
                .await
                .unwrap(),
        );
        session.dispose().await.unwrap();
        assert!(matches!(
            pending.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
    });
}
#[test]
fn cancel_before_insertion_and_drop_running_driver_resolve_without_stranding_ownership() {
    runtime().block_on(async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (stream, requests) = scripted(vec![]);
        let session = session_with(
            stream,
            Arc::new(StartingHooks {
                entered: entered.clone(),
                release: release.clone(),
            }),
            Arc::new(MemoryStore::new()),
        )
        .await;
        let (ticket, driver) = session.tracked_prompt("cancel", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        ticket.cancel();
        assert!(matches!(
            ticket.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        release.notify_one();
        running.await.unwrap().unwrap();
        assert!(requests.lock().unwrap().is_empty());
        assert!(
            !session
                .agent()
                .state()
                .messages
                .iter()
                .any(|m| m.role() == "user")
        );
        let (ticket, driver) = session.tracked_prompt("drop", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        assert!(matches!(
            ticket.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        assert!(session.is_idle());
    });
}
struct RejectRole {
    inner: MemoryStore,
    role: &'static str,
    enabled: AtomicBool,
}
impl SessionStore for RejectRole {
    fn create(&self, s: &SessionSnapshot) -> Result<(), pi_coding_session::Error> {
        self.inner.create(s)
    }
    fn load(&self, id: &str) -> Result<SessionSnapshot, pi_coding_session::Error> {
        self.inner.load(id)
    }
    fn list(
        &self,
        cwd: Option<&str>,
    ) -> Result<Vec<pi_coding_session::SessionInfo>, pi_coding_session::Error> {
        self.inner.list(cwd)
    }
    fn commit(&self, id: &str, commit: &SessionCommit) -> Result<u64, pi_coding_session::Error> {
        if self.enabled.load(Ordering::SeqCst) && commit.append.iter().any(|e| matches!(&e.content, EntryContent::Message { message } if message["role"] == self.role)) { return Err(pi_coding_session::Error::Invalid(format!("reject {} commit", self.role))); }
        self.inner.commit(id, commit)
    }
}
#[test]
fn failed_user_commit_never_inserts_or_leaves_provisional_context_but_assistant_failure_keeps_receipt()
 {
    runtime().block_on(async {
        for role in ["user", "assistant"] {
            let store = Arc::new(RejectRole {
                inner: MemoryStore::new(),
                role,
                enabled: AtomicBool::new(true),
            });
            let (stream, requests) = scripted(vec![json!({}), json!({})]);
            let session = session_with(stream, Arc::new(NoopSessionHooks), store.clone()).await;
            let (ticket, driver) = session.tracked_prompt("first", PromptOptions::default());
            assert!(driver.await.is_err());
            let resolution = ticket.resolution.await.unwrap();
            if role == "user" {
                assert!(matches!(resolution, InputResolution::Rejected { .. }));
                assert!(requests.lock().unwrap().is_empty());
                assert!(
                    !session
                        .agent()
                        .state()
                        .messages
                        .iter()
                        .any(|m| m.role() == "user")
                );
            } else {
                inserted(resolution);
            }
            assert_eq!(
                serde_json::to_value(session.agent().state().messages).unwrap(),
                serde_json::to_value(session.messages().unwrap()).unwrap()
            );
            store.enabled.store(false, Ordering::SeqCst);
            let (ticket, driver) = session.tracked_prompt("second", PromptOptions::default());
            driver.await.unwrap();
            inserted(ticket.resolution.await.unwrap());
        }
    });
}

struct CompactHook;
impl SessionHooks for CompactHook {
    fn before_compact(
        &self,
        preparation: CompactionPreparation,
        _: CompactionReason,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async move {
            Ok(BeforeCompactionResult {
                cancel: false,
                compaction: Some(CompactionResult {
                    summary: "tracked summary".into(),
                    first_kept_entry_id: Some(preparation.first_kept_entry_id),
                    tokens_before: preparation.tokens_before,
                    details: None,
                    usage: None,
                }),
            })
        })
    }
}
#[test]
fn history_subscription_covers_runtime_mutations_shell_compaction_and_manager_replacement() {
    runtime().block_on(async {
        let store = Arc::new(MemoryStore::new());
        let (stream, _) = scripted(vec![json!({}), json!({})]);
        let session = session_with(stream, Arc::new(CompactHook), store.clone()).await;
        let mut observed = session.observe_history().unwrap();
        let mut history = observed.snapshot.clone();
        session
            .prompt("one", PromptOptions::default())
            .await
            .unwrap();
        session
            .prompt("two", PromptOptions::default())
            .await
            .unwrap();
        let target = session.snapshot().leaf_id.unwrap();
        session.set_name("named").await.unwrap();
        session
            .set_label(&target, Some("labeled".into()))
            .await
            .unwrap();
        let mut other = serde_json::to_value(model()).unwrap();
        other["id"] = json!("other");
        other["reasoning"] = json!(true);
        session
            .set_model(serde_json::from_value(other).unwrap())
            .await
            .unwrap();
        session
            .set_thinking_level(pi_ai::ModelThinkingLevel::High)
            .await
            .unwrap();
        session
            .record_bash_result(
                "echo history",
                BashResult {
                    output: "history".into(),
                    exit_code: Some(0),
                    cancelled: false,
                    truncated: false,
                    full_output_path: None,
                },
                false,
            )
            .await
            .unwrap();
        session
            .set_compaction_settings(CompactionSettings {
                enabled: false,
                keep_recent_tokens: pi_ai::SafeUnsigned::new(1).unwrap(),
                ..Default::default()
            })
            .unwrap();
        session.compact(None).await.unwrap();
        session
            .navigate_tree(&target, NavigateOptions::default())
            .await
            .unwrap();
        let expected = session.snapshot();
        let mut types = BTreeSet::new();
        let mut empty_append = false;
        while let Ok(change) = observed.changes.try_recv() {
            let HistoryChange::Committed {
                session_id,
                base_revision,
                revision,
                appended,
                leaf_id,
            } = change
            else {
                panic!("unexpected replace")
            };
            assert_eq!(session_id, history.header.id);
            assert_eq!(base_revision, history.revision);
            empty_append |= appended.is_empty();
            for entry in &appended {
                types.insert(
                    serde_json::to_value(entry).unwrap()["type"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            }
            history.entries.extend(appended);
            history.revision = revision;
            history.leaf_id = leaf_id;
        }
        assert_eq!(history, expected);
        assert!(empty_append);
        for kind in [
            "message",
            "session_info",
            "label",
            "model_change",
            "thinking_level_change",
            "compaction",
        ] {
            assert!(types.contains(kind), "missing {kind}: {types:?}");
        }
        session
            .new_session(store.clone(), "/tmp/new")
            .await
            .unwrap();
        let HistoryChange::Replaced { snapshot } = observed.changes.recv().await.unwrap() else {
            panic!("expected new-session replacement")
        };
        assert_eq!(snapshot.header.id, session.session_id());
        assert_ne!(snapshot.header.id, history.header.id);
        session
            .replace_session(SessionManager::open(store, &history.header.id).unwrap())
            .await
            .unwrap();
        let HistoryChange::Replaced { snapshot } = observed.changes.recv().await.unwrap() else {
            panic!("expected replacement")
        };
        assert_eq!(snapshot, history);
        session.fork(&target).await.unwrap();
        assert!(matches!(
            observed.changes.recv().await.unwrap(),
            HistoryChange::Replaced { .. }
        ));
    });
}
#[test]
fn history_snapshot_registration_is_atomic_with_concurrent_commit_and_drop_unregisters() {
    for register_first in [false, true] {
        let manager = Arc::new(Mutex::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
        ));
        let initial = if register_first {
            Some(manager.lock().unwrap().observe_history())
        } else {
            None
        };
        let writer = manager.clone();
        std::thread::spawn(move || writer.lock().unwrap().set_name("concurrent").unwrap())
            .join()
            .unwrap();
        let mut subscription = initial.unwrap_or_else(|| manager.lock().unwrap().observe_history());
        let expected = manager.lock().unwrap().snapshot().clone();
        let mut reconstructed = subscription.snapshot.clone();
        while let Ok(HistoryChange::Committed {
            base_revision,
            revision,
            appended,
            leaf_id,
            ..
        }) = subscription.changes.try_recv()
        {
            assert_eq!(base_revision, reconstructed.revision);
            reconstructed.entries.extend(appended);
            reconstructed.leaf_id = leaf_id;
            reconstructed.revision = revision;
        }
        assert_eq!(reconstructed, expected);
        drop(subscription);
        manager.lock().unwrap().set_name("after-drop").unwrap();
    }
}

#[test]
fn cancelling_primary_during_start_transform_does_not_call_provider() {
    runtime().block_on(async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (stream, requests) = scripted(vec![]);
        let session = session_with(
            stream,
            Arc::new(BlockDrained {
                entered: entered.clone(),
                release: release.clone(),
            }),
            Arc::new(MemoryStore::new()),
        )
        .await;
        let (ticket, driver) = session.tracked_prompt("hold", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        ticket.cancel();
        assert!(matches!(
            ticket.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        release.notify_one();
        running.await.unwrap().unwrap();
        assert!(requests.lock().unwrap().is_empty());
        assert!(
            !session
                .messages()
                .unwrap()
                .iter()
                .any(|m| m.role() == "user")
        );
    });
}
struct RejectQueuedTransform;
impl SessionHooks for RejectQueuedTransform {
    fn transform_message(
        &self,
        _: MessagePhase,
        message: AgentMessage,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<AgentMessage, AgentError>> {
        Box::pin(async move {
            if serde_json::to_value(&message).unwrap()["content"][0]["text"] == "bad queued" {
                Err(AgentError::Callback("queued start rejected".into()))
            } else {
                Ok(message)
            }
        })
    }
}
#[test]
fn rejected_queued_start_transform_resolves_its_exact_ticket() {
    runtime().block_on(async {
        let (stream, entered, release, _) = blocked_first();
        let session = session_with(stream, Arc::new(RejectQueuedTransform), Arc::new(MemoryStore::new())).await;
        let (first, driver) = session.tracked_prompt("start", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        let rejected = queued(session.queue_tracked("bad queued", vec![], Delivery::Steer).await.unwrap());
        release.notify_one();
        running.await.unwrap().unwrap();
        inserted(first.resolution.await.unwrap());
        assert!(matches!(rejected.resolution.await.unwrap(), InputResolution::Rejected { message } if message == "queued start rejected"));
        assert_eq!(session.pending_message_count(), 0);
    });
}
#[test]
fn rejected_queue_notification_rolls_back_its_real_admission() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![]);
        let session = session_with(stream, Arc::new(NoopSessionHooks), Arc::new(MemoryStore::new())).await;
        session.subscribe(Arc::new(|event| Box::pin(async move {
            if matches!(event, SessionEvent::QueueUpdate { steering, .. } if !steering.is_empty()) { Err(AgentError::Callback("queue subscriber rejected".into())) } else { Ok(()) }
        })));
        assert!(session.queue_tracked("rollback", vec![], Delivery::Steer).await.is_err());
        assert!(session.queued_inputs().0.is_empty());
        assert!(!session.agent().has_queued_messages());
        assert_eq!(session.pending_message_count(), 0);
    });
}

#[test]
fn rejected_queue_notification_preserves_a_committed_inputs_exact_ticket() {
    runtime().block_on(async {
        for delivery in [Delivery::Steer, Delivery::FollowUp] {
            let (stream, entered, release, calls) = blocked_first();
            let session = session_with(
                stream,
                Arc::new(NoopSessionHooks),
                Arc::new(MemoryStore::new()),
            )
            .await;
            let ack_entered = Arc::new(Notify::new());
            let reject_ack = Arc::new(Notify::new());
            session.subscribe(Arc::new({
                let (entered, reject) = (ack_entered.clone(), reject_ack.clone());
                move |event| {
                    let (entered, reject) = (entered.clone(), reject.clone());
                    Box::pin(async move {
                        if matches!(event, SessionEvent::QueueUpdate { steering, follow_up }
                            if !steering.is_empty() || !follow_up.is_empty())
                        {
                            entered.notify_one();
                            reject.notified().await;
                            return Err(AgentError::Callback(
                                "queue acknowledgement rejected".into(),
                            ));
                        }
                        Ok(())
                    })
                }
            }));
            // Equal payloads must not let acknowledgement infer the primary's outcome.
            let (primary, driver) = session.tracked_prompt("same", PromptOptions::default());
            let running = tokio::spawn(driver);
            entered.notified().await;
            let queuing = tokio::spawn({
                let session = session.clone();
                async move { session.queue_tracked("same", vec![], delivery).await }
            });
            ack_entered.notified().await;
            let (steering, follow_up) = session.queued_inputs();
            let queued_id = steering.iter().chain(&follow_up).next().unwrap().id;
            assert_ne!(queued_id, primary.id);
            release.notify_one();
            running.await.unwrap().unwrap();
            let (primary_entry, _) = inserted(primary.resolution.await.unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                session
                    .messages()
                    .unwrap()
                    .iter()
                    .filter(|m| m.role() == "user")
                    .count(),
                2,
                "both equal inputs have committed before queue acknowledgement fails"
            );
            reject_ack.notify_one();
            let admission = queuing.await.unwrap();
            assert!(
                admission.is_ok(),
                "queue returned an error after its exact input committed"
            );
            let ticket = queued(admission.unwrap());
            assert_eq!(ticket.id, queued_id);
            ticket.cancel(); // A late queue failure cannot make insertion revocable.
            let (entry, message) = inserted(ticket.resolution.await.unwrap());
            assert_ne!(entry, primary_entry);
            assert!(
                session
                    .snapshot()
                    .entries
                    .iter()
                    .any(|record| record.id == entry)
            );
            assert_eq!(
                serde_json::to_value(message).unwrap()["content"][0]["text"],
                "same"
            );
            assert!(session.clear_queue_tracked().await.unwrap().is_empty());
            assert!(session.queued_inputs().0.is_empty());
            assert!(session.queued_inputs().1.is_empty());
            assert!(!session.agent().has_queued_messages());
            assert_eq!(session.pending_message_count(), 0);
        }
    });
}

/// Holds the first non-empty queue acknowledgement open, then fails it on release.
fn failing_queue_ack() -> (SessionEventSink, Arc<Notify>, Arc<Notify>) {
    let (entered, reject) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let subscriber: SessionEventSink = Arc::new({
        let (entered, reject) = (entered.clone(), reject.clone());
        move |event| {
            let (entered, reject) = (entered.clone(), reject.clone());
            Box::pin(async move {
                if matches!(event, SessionEvent::QueueUpdate { steering, follow_up }
                    if !steering.is_empty() || !follow_up.is_empty())
                {
                    entered.notify_one();
                    reject.notified().await;
                    return Err(AgentError::Callback(
                        "queue acknowledgement rejected".into(),
                    ));
                }
                Ok(())
            })
        }
    });
    (subscriber, entered, reject)
}

#[test]
fn rejected_queue_notification_reports_cancellation_of_an_input_withdrawn_meanwhile() {
    runtime().block_on(async {
        for delivery in [Delivery::Steer, Delivery::FollowUp] {
            for dispose in [false, true] {
                let (stream, _) = scripted(vec![]);
                let session = session_with(stream, Arc::new(NoopSessionHooks), Arc::new(MemoryStore::new())).await;
                let (ack, ack_entered, reject_ack) = failing_queue_ack();
                session.subscribe(ack);
                let queuing = tokio::spawn({
                    let session = session.clone();
                    async move { session.queue_tracked("withdrawn", vec![], delivery).await }
                });
                ack_entered.notified().await;
                // The input is live in the FIFO while its notification awaits acknowledgement.
                let (steering, follow_up) = session.queued_inputs();
                assert_eq!(steering.len() + follow_up.len(), 1);
                if dispose {
                    session.dispose().await.unwrap();
                } else {
                    assert_eq!(session.clear_queue_tracked().await.unwrap().len(), 1);
                }
                reject_ack.notify_one();
                let admission = queuing.await.unwrap();
                assert!(
                    matches!(admission, Err(SessionError::Cancelled)),
                    "a cancelled input must not be reported as queued ({delivery:?}, dispose={dispose}): {}",
                    match &admission {
                        Ok(QueueAdmission::Queued(_)) => "queued".to_string(),
                        Ok(QueueAdmission::Handled) => "handled".to_string(),
                        Err(error) => error.to_string(),
                    }
                );
                assert!(session.queued_inputs().0.is_empty());
                assert!(session.queued_inputs().1.is_empty());
                assert!(!session.agent().has_queued_messages());
                if !dispose {
                    // Disposal leaves the closed session's display mirror untouched.
                    assert_eq!(session.pending_message_count(), 0);
                }
            }
        }
    });
}

#[test]
fn rejected_queue_notification_reports_the_rejection_of_an_input_the_driver_refused() {
    runtime().block_on(async {
        let (stream, entered, release, _) = blocked_first();
        let session = session_with(stream, Arc::new(RejectQueuedTransform), Arc::new(MemoryStore::new())).await;
        let (ack, ack_entered, reject_ack) = failing_queue_ack();
        session.subscribe(ack);
        let (first, driver) = session.tracked_prompt("start", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        let queuing = tokio::spawn({
            let session = session.clone();
            async move { session.queue_tracked("bad queued", vec![], Delivery::Steer).await }
        });
        ack_entered.notified().await;
        // The live driver drains the queued input and its start transform refuses it
        // while the queue notification still awaits acknowledgement.
        release.notify_one();
        running.await.unwrap().unwrap();
        inserted(first.resolution.await.unwrap());
        reject_ack.notify_one();
        let admission = queuing.await.unwrap();
        assert!(
            matches!(&admission, Err(SessionError::Invalid(message)) if message == "queued start rejected"),
            "a rejected input must report its rejection, not a queued receipt"
        );
        assert!(session.queued_inputs().0.is_empty());
        assert!(!session.agent().has_queued_messages());
        assert_eq!(session.pending_message_count(), 0);
    });
}

#[test]
fn dropping_last_session_owner_resolves_queued_tickets_as_cancelled() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![]);
        let session = session_with(
            stream,
            Arc::new(NoopSessionHooks),
            Arc::new(MemoryStore::new()),
        )
        .await;
        let pending = queued(
            session
                .queue_tracked("orphan", vec![], Delivery::FollowUp)
                .await
                .unwrap(),
        );
        drop(session);
        assert!(matches!(
            pending.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
    });
}

#[test]
fn dropping_during_legacy_message_end_discards_the_uncommitted_user_context() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![]);
        let session = session_with(stream, Arc::new(NoopSessionHooks), Arc::new(MemoryStore::new())).await;
        let entered = Arc::new(Notify::new());
        let blocked = Arc::new(Notify::new());
        let (notify, wait) = (entered.clone(), blocked.clone());
        session.subscribe(Arc::new(move |event| {
            let (notify, wait) = (notify.clone(), wait.clone());
            Box::pin(async move {
                if matches!(event, SessionEvent::Agent(AgentEvent::MessageEnd { message }) if message.role() == "user") {
                    notify.notify_one(); wait.notified().await;
                }
                Ok(())
            })
        }));
        let (ticket, driver) = session.tracked_prompt("provisional", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        assert!(session.agent().state().messages.iter().any(|m| m.role() == "user"));
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        assert!(matches!(ticket.resolution.await.unwrap(), InputResolution::Cancelled));
        assert!(session.is_idle());
        assert!(!session.agent().state().messages.iter().any(|m| m.role() == "user"));
        assert!(requests.lock().unwrap().is_empty());
    });
}

struct BlockPendingInput {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
impl SessionHooks for BlockPendingInput {
    fn input(
        &self,
        input: InputEvent,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async move {
            if input.text == "waiting" {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(InputResult::Continue)
        })
    }
}
#[test]
fn cancelling_a_preparing_request_does_not_abort_another_active_driver() {
    runtime().block_on(async {
        let hook_entered = Arc::new(Notify::new());
        let hook_release = Arc::new(Notify::new());
        let (stream, entered, release, _) = blocked_first();
        let session = session_with(
            stream,
            Arc::new(BlockPendingInput {
                entered: hook_entered.clone(),
                release: hook_release.clone(),
            }),
            Arc::new(MemoryStore::new()),
        )
        .await;
        let (first, driver) = session.tracked_prompt("first", PromptOptions::default());
        let running = tokio::spawn(driver);
        entered.notified().await;
        let (second, driver) = session.tracked_prompt("waiting", PromptOptions::default());
        let preparing = tokio::spawn(driver);
        hook_entered.notified().await;
        second.cancel();
        assert!(matches!(
            second.resolution.await.unwrap(),
            InputResolution::Cancelled
        ));
        assert!(!session.agent().signal().unwrap().is_cancelled());
        hook_release.notify_one();
        preparing.await.unwrap().unwrap();
        assert!(!running.is_finished());
        release.notify_one();
        running.await.unwrap().unwrap();
        inserted(first.resolution.await.unwrap());
    });
}

#[test]
fn a_queue_admission_withdrawn_during_its_input_hook_reports_cancellation() {
    runtime().block_on(async {
        for delivery in [Delivery::Steer, Delivery::FollowUp] {
            let store = Arc::new(MemoryStore::new());
            let (hook_entered, hook_release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
            let (stream, requests) = scripted(vec![]);
            let session = session_with(
                stream,
                Arc::new(BlockPendingInput {
                    entered: hook_entered.clone(),
                    release: hook_release.clone(),
                }),
                store.clone(),
            )
            .await;
            let queuing = tokio::spawn({
                let session = session.clone();
                async move { session.queue_tracked("waiting", vec![], delivery).await }
            });
            hook_entered.notified().await;
            // A session switch withdraws every pending receipt, the one still preprocessing too.
            assert!(session.new_session(store, "/tmp/elsewhere").await.unwrap());
            hook_release.notify_one();
            let admission = queuing.await.unwrap();
            assert!(
                matches!(admission, Err(SessionError::Cancelled)),
                "an input that never entered the FIFO must not be reported as queued ({delivery:?}): {}",
                match &admission {
                    Ok(QueueAdmission::Queued(_)) => "queued".to_string(),
                    Ok(QueueAdmission::Handled) => "handled".to_string(),
                    Err(error) => error.to_string(),
                }
            );
            assert!(session.queued_inputs().0.is_empty());
            assert!(session.queued_inputs().1.is_empty());
            assert!(!session.agent().has_queued_messages());
            assert_eq!(session.pending_message_count(), 0);
            assert!(requests.lock().unwrap().is_empty());
        }
    });
}

fn user_text(message: &AgentMessage) -> String {
    let value = serde_json::to_value(message).unwrap();
    value["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

#[test]
fn explicit_continuation_drives_fresh_queues_without_implicit_start() {
    runtime().block_on(async {
        for delivery in [Delivery::Steer, Delivery::FollowUp] {
            for mode in [QueueMode::OneAtATime, QueueMode::All] {
                let responses = if mode == QueueMode::All { 1 } else { 2 };
                let (stream, requests) = scripted(vec![json!({}); responses]);
                let session = session_with(
                    stream,
                    Arc::new(NoopSessionHooks),
                    Arc::new(MemoryStore::new()),
                )
                .await;
                session.set_steering_mode(mode);
                session.set_follow_up_mode(mode);
                assert!(matches!(
                    session.continue_run().await,
                    Err(SessionError::Agent(AgentError::InvalidContext(message)))
                        if message == "No messages to continue from"
                ));
                let mut first = queued(
                    session
                        .queue_tracked("first", vec![], delivery)
                        .await
                        .unwrap(),
                );
                let mut second = queued(
                    session
                        .queue_tracked("second", vec![], delivery)
                        .await
                        .unwrap(),
                );
                assert!(matches!(
                    first.resolution.try_recv(),
                    Err(TryRecvError::Empty)
                ));
                assert!(matches!(
                    second.resolution.try_recv(),
                    Err(TryRecvError::Empty)
                ));
                assert!(session.is_idle());
                assert!(requests.lock().unwrap().is_empty());
                assert_eq!(session.pending_message_count(), 2);

                session.continue_run().await.unwrap();
                let (first_id, first_message) = inserted(first.resolution.await.unwrap());
                let (second_id, second_message) = inserted(second.resolution.await.unwrap());
                assert_ne!(first_id, second_id);
                assert_eq!(user_text(&first_message), "first");
                assert_eq!(user_text(&second_message), "second");
                assert_eq!(requests.lock().unwrap().len(), responses);
                assert!(!session.agent().has_queued_messages());
                assert_eq!(session.pending_message_count(), 0);
                assert!(session.is_idle());
            }
        }
    });
}

#[test]
fn rejected_tracked_message_end_discards_provisional_context_only_for_tracked_inputs() {
    runtime().block_on(async {
        for tracked in [true, false] {
            let (stream, requests) = scripted(vec![]);
            let session = session_with(
                stream,
                Arc::new(NoopSessionHooks),
                Arc::new(MemoryStore::new()),
            )
            .await;
            session.subscribe(Arc::new(|event| Box::pin(async move {
                if matches!(event, SessionEvent::Agent(AgentEvent::MessageEnd { message }) if message.role() == "user") {
                    Err(AgentError::Callback("reject user end".into()))
                } else {
                    Ok(())
                }
            })));
            if tracked {
                let (ticket, driver) = session.tracked_prompt("provisional", PromptOptions::default());
                driver.await.unwrap(); // Native callback failure keeps its accepted assistant fallback.
                assert!(matches!(ticket.resolution.await.unwrap(), InputResolution::Rejected { message } if message == "reject user end"));
            } else {
                session.prompt("provisional", PromptOptions::default()).await.unwrap();
            }
            assert!(requests.lock().unwrap().is_empty());
            assert!(!session.messages().unwrap().iter().any(|message| message.role() == "user"));
            assert_eq!(
                session.agent().state().messages.iter().any(|message| message.role() == "user"),
                !tracked,
                "tracked provisional cleanup must preserve legacy untracked behavior"
            );
            assert!(session.is_idle());
        }
    });
}

#[test]
fn cancelling_queued_message_end_preserves_the_primary_driver_and_queued_sibling() {
    runtime().block_on(async {
        for delivery in [Delivery::Steer, Delivery::FollowUp] {
            for mode in [QueueMode::OneAtATime, QueueMode::All] {
                let (base_stream, entered, release, _) = blocked_first();
                let requests = Arc::new(Mutex::new(Vec::new()));
                let captured = requests.clone();
                let stream: pi_agent::StreamFn = Arc::new(move |request| {
                    captured.lock().unwrap().push(request.clone());
                    base_stream(request)
                });
                let session = session_with(stream, Arc::new(NoopSessionHooks), Arc::new(MemoryStore::new())).await;
                session.set_steering_mode(mode);
                session.set_follow_up_mode(mode);
                let at_end = Arc::new(Notify::new());
                let leave_end = Arc::new(Notify::new());
                let outcomes = Arc::new(Mutex::new(Vec::new()));
                let observed = outcomes.clone();
                let (notify, wait) = (at_end.clone(), leave_end.clone());
                session.subscribe(Arc::new(move |event| {
                    let (notify, wait, observed) = (notify.clone(), wait.clone(), observed.clone());
                    Box::pin(async move {
                        if matches!(&event, SessionEvent::AgentEnd { .. } | SessionEvent::AgentSettled { .. }) {
                            observed.lock().unwrap().push(event.clone());
                        }
                        if matches!(event, SessionEvent::Agent(AgentEvent::MessageEnd { message }) if message.role() == "user" && user_text(&message) == "withdraw") {
                            notify.notify_one();
                            wait.notified().await;
                        }
                        Ok(())
                    })
                }));
                let (primary, driver) = session.tracked_prompt("primary", PromptOptions::default());
                let running = tokio::spawn(driver);
                entered.notified().await;
                let withdrawn = queued(session.queue_tracked("withdraw", vec![], delivery).await.unwrap());
                let sibling = queued(session.queue_tracked("survivor", vec![], delivery).await.unwrap());
                release.notify_one();
                at_end.notified().await;
                withdrawn.cancel();
                assert!(matches!(withdrawn.resolution.await.unwrap(), InputResolution::Cancelled));
                assert!(!session.agent().signal().unwrap().is_cancelled());
                leave_end.notify_one();
                running.await.unwrap().unwrap();
                inserted(primary.resolution.await.unwrap());
                assert_eq!(user_text(&inserted(sibling.resolution.await.unwrap()).1), "survivor");
                assert!(!session.messages().unwrap().iter().any(|message| user_text(message) == "withdraw"));
                assert!(!session.agent().state().messages.iter().any(|message| user_text(message) == "withdraw"));
                assert!(requests.lock().unwrap().iter().all(|request| request.context.messages.iter().all(|message| {
                    let value = serde_json::to_value(message).unwrap();
                    value["content"][0]["text"] != "withdraw"
                })));
                let outcomes = outcomes.lock().unwrap();
                assert!(outcomes.iter().any(|event| matches!(event, SessionEvent::AgentEnd { messages, .. } if messages.iter().all(|message| user_text(message) != "withdraw"))));
                assert!(matches!(outcomes.last(), Some(SessionEvent::AgentSettled { outcome: ActivityOutcome::Completed })));
                assert_eq!(session.pending_message_count(), 0);
                assert!(!session.agent().has_queued_messages());
                assert!(session.is_idle());
            }
        }
    });
}
