mod session_support;
use pi_agent::{AgentError, AgentEvent, AgentMessage, BoxFuture, CancellationToken};
use pi_ai::Message;
use pi_coding_runtime::*;
use pi_coding_session::{
    EntryContent, MemoryStore, SessionCommit, SessionManager, SessionSnapshot, SessionStore,
    SqliteStore,
};
use serde_json::json;
use session_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[test]
fn prompt_tool_persists_reopens_and_forks_without_reexecution() {
    runtime().block_on(async{
 let path=std::env::temp_dir().join(format!("pirua-coding-session-{}-{}.sqlite",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
 let store=Arc::new(SqliteStore::open(&path).unwrap());let manager=SessionManager::create(store.clone(),"/tmp").unwrap();let id=manager.header().id.clone();
 let calls=Arc::new(Mutex::new(Vec::new()));let capture=calls.clone();let echo=tool("echo",Arc::new(move|input|{let capture=capture.clone();Box::pin(async move{capture.lock().unwrap().push(input.arguments);Ok(text_result("tool output"))})}));
 let (stream,requests)=scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"call-1","name":"echo","arguments":{"path":"a"}}],"usage":usage(10)}),json!({"content":[{"type":"text","text":"done"}],"usage":usage(20)})]);
 let session=CodingSessionBuilder::new(manager,model(),stream).with_tools(vec![echo]).with_system_prompt("Be precise.").build().await.unwrap();session.set_active_tools(vec!["echo".into()]).unwrap();
 session.prompt("hello",PromptOptions::default()).await.unwrap();session.wait_for_idle().await.unwrap();assert_eq!(calls.lock().unwrap().len(),1);assert_eq!(requests.lock().unwrap().len(),2);assert_eq!(session.stats().assistant_messages,2);assert_eq!(session.stats().tool_results,1);session.set_name("  Saved\nSession  ").await.unwrap();
 let snapshot=session.snapshot();let assistant_id=snapshot.entries.iter().rev().find(|e|matches!(&e.content,EntryContent::Message{message}if message["role"]=="assistant")).unwrap().id.clone();session.set_label(&assistant_id,Some("checkpoint".into())).await.unwrap();
 let (unused,_)=scripted(vec![]);let reopened=CodingSessionBuilder::new(SessionManager::open(store.clone(),&id).unwrap(),model(),unused).with_tools(Vec::new()).with_system_prompt("Be precise.").build().await.unwrap();assert_eq!(reopened.session_name().as_deref(),Some("Saved Session"));assert_eq!(reopened.stats().tool_results,1);assert_eq!(calls.lock().unwrap().len(),1);assert_eq!(reopened.label(&assistant_id).as_deref(),Some("checkpoint"));
 reopened.fork(&assistant_id).await.unwrap();assert_ne!(reopened.session_id(),id);assert_eq!(reopened.snapshot().header.parent_session.as_deref(),Some(id.as_str()));assert_eq!(reopened.stats().assistant_messages,2);assert_eq!(reopened.label(&assistant_id).as_deref(),Some("checkpoint"));
 drop(reopened);drop(session);drop(store);let _=std::fs::remove_file(path);
});
}

struct TransformHooks {
    events: Mutex<Vec<&'static str>>,
}
impl SessionHooks for TransformHooks {
    fn transform_message(
        &self,
        phase: MessagePhase,
        message: AgentMessage,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<AgentMessage, AgentError>> {
        Box::pin(async move {
            if phase == MessagePhase::End
                && matches!(message, AgentMessage::BuiltIn(Message::Assistant(_)))
            {
                let mut value = serde_json::to_value(message).unwrap();
                if value["content"][0]["type"] == "toolCall" {
                    value["content"][0]["arguments"]["path"] = json!("transformed");
                }
                Ok(serde_json::from_value(value).unwrap())
            } else {
                Ok(message)
            }
        })
    }
    fn input(
        &self,
        event: InputEvent,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async move {
            Ok(InputResult::Transform {
                text: format!("{}!", event.text),
                images: None,
            })
        })
    }
    fn event(&self, event: SessionEvent) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async move {
            if let SessionEvent::Agent(AgentEvent::MessageEnd { .. }) = event {
                self.events.lock().unwrap().push("message_end");
            }
            Ok(())
        })
    }
}
#[test]
fn finalized_transform_controls_dispatch_persistence_and_future_context() {
    runtime().block_on(async{
 let hooks=Arc::new(TransformHooks{events:Mutex::new(Vec::new())});let seen=Arc::new(Mutex::new(Vec::new()));let capture=seen.clone();let echo=tool("echo",Arc::new(move|i|{let capture=capture.clone();Box::pin(async move{capture.lock().unwrap().push(i.arguments);Ok(text_result("yes"))})}));let (stream,requests)=scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"x","name":"echo","arguments":{"path":"raw"}}]}),json!({})]);
 let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_hooks(hooks.clone()).with_tools(vec![echo]).with_system_prompt("test").build().await.unwrap();session.set_active_tools(vec!["echo".into()]).unwrap();session.prompt("input",PromptOptions::default()).await.unwrap();assert_eq!(serde_json::to_value(&seen.lock().unwrap()[0]).unwrap()["path"],json!("transformed"));let requests=requests.lock().unwrap();let second=serde_json::to_value(requests[1].context.messages()).unwrap();assert!(second.as_array().unwrap().iter().any(|m|m["role"]=="assistant"&&m["content"][0]["arguments"]["path"]=="transformed"));assert!(session.projection().messages.iter().any(|m|m["content"][0]["text"]=="input!"));assert!(session.projection().messages.iter().any(|m|m["role"]=="assistant"&&m["content"][0]["arguments"]["path"]=="transformed"));assert!(hooks.events.lock().unwrap().len()>=4);
});
}

#[test]
fn compaction_keeps_canonical_tail_and_context_usage_is_unknown_until_response() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![
            json!({"usage":usage(100)}),
            json!({"usage":usage(200)}),
            json!({"content":[{"type":"text","text":"A compact checkpoint."}],"usage":usage(30)}),
            json!({"content":[{"type":"text","text":"Turn checkpoint."}],"usage":usage(5)}),
            json!({"usage":usage(40)}),
        ]);
        let mut settings = settings();
        settings.compaction.keep_recent_tokens = pi_ai::SafeUnsigned::new(1).unwrap();
        settings.compaction.enabled = false;
        let store = Arc::new(MemoryStore::new());
        let session = CodingSessionBuilder::new(
            SessionManager::create(store.clone(), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_settings(settings)
        .with_tools(vec![])
        .with_system_prompt("system")
        .build()
        .await
        .unwrap();
        session
            .prompt("one", PromptOptions::default())
            .await
            .unwrap();
        session
            .prompt("two", PromptOptions::default())
            .await
            .unwrap();
        let compact = session.compact(None).await.unwrap();
        assert!(compact.summary.contains("A compact checkpoint."));
        assert_eq!(session.context_usage().unwrap().tokens, None);
        let projection = session.projection();
        assert_eq!(
            projection
                .messages
                .iter()
                .filter(|m| m["role"] == "compactionSummary")
                .count(),
            1
        );
        assert!(projection.messages.iter().any(|m| m["role"] == "system"));
        let reopened = SessionManager::open(store, &session.session_id()).unwrap();
        assert_eq!(reopened.projection(), projection);
        session
            .prompt("after", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(session.context_usage().unwrap().tokens, Some(41));
        assert_eq!(requests.lock().unwrap().len(), 5);
    });
}
fn settings() -> CodingSettingsSnapshot {
    CodingSettingsSnapshot {
        model: ModelRef::new(model().provider, model().id),
        thinking_level: ThinkingLevel::Off,
        active_tools: vec![],
        retry: pi_ai::RetryPolicy {
            enabled: true,
            max_retries: pi_ai::MaxRetries::new(2).unwrap(),
            base_delay_ms: pi_ai::SafeUnsigned::new(0).unwrap(),
            max_agent_delay_ms: None,
        },
        compaction: CompactionSettings {
            enabled: false,
            ..Default::default()
        },
        stream: Default::default(),
        steering_mode: QueueMode::OneAtATime,
        follow_up_mode: QueueMode::All,
    }
}

#[test]
fn retry_omits_failed_attempt_in_saved_projection_before_continuation() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![
            json!({"stopReason":"error","errorMessage":"503 overloaded","content":[]}),
            json!({"content":[{"type":"text","text":"recovered"}]}),
        ]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_settings(settings())
        .with_tools(vec![])
        .with_system_prompt("test")
        .build()
        .await
        .unwrap();
        session
            .prompt("try", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 2);
        let raw = session.snapshot();
        assert!(raw.entries.iter().any(
            |e| matches!(&e.content,EntryContent::Message{message}if message["stopReason"]=="error")
        ));
        assert!(raw.entries.iter().any(|e| matches!(
            e.content,
            EntryContent::ContextEdit {
                replacement: None,
                ..
            }
        )));
        assert!(
            !session
                .projection()
                .messages
                .iter()
                .any(|m| m["stopReason"] == "error")
        );
        assert!(
            !serde_json::to_value(requests.lock().unwrap()[1].context.messages())
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["stopReason"] == "error")
        );
    });
}

struct RejectAssistant {
    inner: MemoryStore,
    enabled: AtomicBool,
}
impl SessionStore for RejectAssistant {
    fn create(&self, snapshot: &SessionSnapshot) -> Result<(), pi_coding_session::Error> {
        self.inner.create(snapshot)
    }
    fn load(&self, id: &str) -> Result<SessionSnapshot, pi_coding_session::Error> {
        self.inner.load(id)
    }
    fn commit(&self, id: &str, commit: &SessionCommit) -> Result<u64, pi_coding_session::Error> {
        if self.enabled.load(Ordering::SeqCst)&&commit.append.iter().any(|e|matches!(&e.content,EntryContent::Message{message}if message["role"]=="assistant")){return Err(pi_coding_session::Error::Invalid("injected SQLite commit failure".into()));}
        self.inner.commit(id, commit)
    }
    fn list(
        &self,
        cwd: Option<&str>,
    ) -> Result<Vec<pi_coding_session::SessionInfo>, pi_coding_session::Error> {
        self.inner.list(cwd)
    }
}
#[test]
fn persistence_failure_is_observable_and_cannot_report_successful_idle() {
    runtime().block_on(async {
        let store = Arc::new(RejectAssistant {
            inner: MemoryStore::new(),
            enabled: AtomicBool::new(true),
        });
        let (stream, _) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(store, "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("test")
        .build()
        .await
        .unwrap();
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let capture = outcomes.clone();
        session.subscribe(Arc::new(move |event| {
            let capture = capture.clone();
            Box::pin(async move {
                if let SessionEvent::AgentSettled { outcome } = event {
                    capture.lock().unwrap().push(outcome);
                }
                Ok(())
            })
        }));
        assert!(
            session
                .prompt("hello", PromptOptions::default())
                .await
                .is_err()
        );
        assert!(session.wait_for_idle().await.is_err());
        assert!(session.is_idle());
        assert!(
            !outcomes
                .lock()
                .unwrap()
                .contains(&ActivityOutcome::Completed)
        );
        assert!(
            !session
                .projection()
                .messages
                .iter()
                .any(|m| m["role"] == "assistant")
        );
    });
}

#[test]
fn boundary_drafts_are_atomic_and_preview_does_not_publish() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("test")
        .build()
        .await
        .unwrap();
        session
            .prompt("hello", PromptOptions::default())
            .await
            .unwrap();
        let before = session.snapshot();
        let valid = SessionBoundaryDraft::Custom {
            custom_type: "store".into(),
            data: Some(json!({"k":1})),
        };
        let invalid = SessionBoundaryDraft::ContextEdit {
            target_id: "absent".into(),
            replacement: None,
        };
        assert!(
            session
                .commit_boundary_drafts(vec![valid.clone(), invalid])
                .await
                .is_err()
        );
        assert_eq!(session.snapshot(), before);
        let preview = session
            .preview_boundary_drafts(std::slice::from_ref(&valid))
            .await
            .unwrap();
        assert_eq!(preview.messages, session.projection().messages);
        assert_eq!(session.snapshot(), before);
        let entries = session
            .append_custom_entries(vec![
                ("a".into(), Some(json!(1))),
                ("b".into(), Some(json!(2))),
            ])
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(session.snapshot().revision, before.revision + 1);
        assert_eq!(
            entries[1].parent_id.as_deref(),
            Some(entries[0].id.as_str())
        );
    });
}

struct NestedHooks {
    parents: Mutex<Vec<Option<String>>>,
}
impl SessionHooks for NestedHooks {
    fn before_tool(
        &self,
        _: pi_agent::BeforeToolCallContext,
        parent: Option<String>,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<pi_agent::BeforeToolCallResult>, AgentError>> {
        Box::pin(async move {
            self.parents.lock().unwrap().push(parent);
            Ok(None)
        })
    }
}
#[test]
fn nested_callable_tools_share_hooks_usage_and_bounded_result_metadata() {
    runtime().block_on(async{
 let (stream,_)=scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"outer","name":"parent","arguments":{}}]}),json!({})]);let hooks=Arc::new(NestedHooks{parents:Mutex::new(Vec::new())});let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_hooks(hooks.clone()).with_tools(vec![]).with_system_prompt("nested").build().await.unwrap();
 let weak=session.downgrade();session.register_tool(RegisteredTool::direct(tool("parent",Arc::new(move|input|{let session=weak.upgrade().unwrap();Box::pin(async move{let arguments=serde_json::from_value(json!({"path":"nested.txt"})).unwrap();let outcome=session.execute_tool(input.call_id,"child",arguments,input.cancellation,None).await.map_err(|e|AgentError::Tool(e.to_string()))?;assert!(!outcome.is_error);Ok(text_result("parent done"))})})))).unwrap();
 let mut child=RegisteredTool::direct(tool("child",Arc::new(|_|Box::pin(async{let mut r=text_result("child done");r.usage=Some(serde_json::from_value(usage(3)).unwrap());Ok(r)}))));child.exposure=ToolExposure::Deferred;child.default_active=false;session.register_tool(child).unwrap();session.set_active_tools(vec!["parent".into()]).unwrap();assert_eq!(session.callable_tool_names(),vec!["parent","child"]);session.prompt("go",PromptOptions::default()).await.unwrap();let result=session.projection().messages.into_iter().find(|m|m["role"]=="toolResult").unwrap();assert_eq!(result["nestedCalls"]["calls"][0]["id"],"outer/1");assert_eq!(result["nestedCalls"]["calls"][0]["name"],"child");assert_eq!(result["nestedCalls"]["calls"][0]["status"],"ok");assert_eq!(result["nestedCalls"]["complete"],true);assert_eq!(result["usage"]["input"],3);assert!(hooks.parents.lock().unwrap().contains(&Some("outer".into())));
});
}

struct SettleHooks {
    once: AtomicBool,
}
impl SessionHooks for SettleHooks {
    fn boundary(
        &self,
        event: BoundaryContext,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BoundaryResult, AgentError>> {
        Box::pin(async move {
            if event.kind == BoundaryKind::AgentBeforeSettle
                && !self.once.swap(true, Ordering::SeqCst)
            {
                Ok(BoundaryResult {
                    entries: vec![SessionBoundaryDraft::CustomMessage {
                        custom_type: "finish".into(),
                        content: json!("one more"),
                        display: false,
                        details: None,
                    }],
                    continue_run: true,
                })
            } else {
                Ok(BoundaryResult::default())
            }
        })
    }
}
#[test]
fn queues_are_drained_before_settlement_and_boundary_can_add_a_final_run() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stream = {
            let (entered, release, count) = (entered.clone(), release.clone(), count.clone());
            Arc::new(move |request| {
                let (entered, release, count) = (entered.clone(), release.clone(), count.clone());
                Box::pin(async move {
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(response(request, json!({})))
                }) as BoxFuture<'static, Result<pi_ai::AssistantStream, AgentError>>
            })
        };
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_hooks(Arc::new(SettleHooks {
            once: AtomicBool::new(false),
        }))
        .with_tools(vec![])
        .with_system_prompt("queues")
        .build()
        .await
        .unwrap();
        let running = tokio::spawn({
            let s = session.clone();
            async move { s.prompt("first", PromptOptions::default()).await }
        });
        entered.notified().await;
        session.follow_up("queued", Vec::new()).await.unwrap();
        assert_eq!(session.pending_message_count(), 1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), session.wait_for_idle())
                .await
                .is_err()
        );
        release.notify_one();
        running.await.unwrap().unwrap();
        session.wait_for_idle().await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert_eq!(session.pending_message_count(), 0);
        let messages = session.projection().messages;
        assert!(
            messages
                .iter()
                .any(|m| m["role"] == "custom" && m["content"] == "one more")
        );
        assert_eq!(
            messages.iter().filter(|m| m["role"] == "assistant").count(),
            3
        );
    });
}

struct StartingHooks {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl SessionHooks for StartingHooks {
    fn before_agent_start(
        &self,
        _: BeforeAgentStart,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeAgentStartResult, AgentError>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(BeforeAgentStartResult::default())
        })
    }
}
#[test]
fn abort_during_before_start_prevents_provider_and_dispose_invalidates_session() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (stream, requests) = scripted(vec![]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_hooks(Arc::new(StartingHooks {
            entered: entered.clone(),
            release: release.clone(),
        }))
        .with_tools(vec![])
        .with_system_prompt("abort")
        .build()
        .await
        .unwrap();
        let running = tokio::spawn({
            let s = session.clone();
            async move { s.prompt("first", PromptOptions::default()).await }
        });
        entered.notified().await;
        assert!(session.is_streaming());
        session.abort();
        release.notify_one();
        running.await.unwrap().unwrap();
        session.wait_for_idle().await.unwrap();
        assert!(requests.lock().unwrap().is_empty());
        session.dispose().await.unwrap();
        assert!(session.is_disposed());
        assert!(matches!(
            session.prompt("after", PromptOptions::default()).await,
            Err(SessionError::Disposed)
        ));
    });
}

struct LoadoutHooks;
impl SessionHooks for LoadoutHooks {
    fn prepare_loadout(
        &self,
        _: ToolLoadout,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<ToolLoadoutChanges, AgentError>> {
        Box::pin(async {
            Ok(ToolLoadoutChanges {
                descriptions: std::collections::BTreeMap::from([(
                    "visible".into(),
                    "patched description".into(),
                )]),
                hidden_declarations: vec!["hidden-declaration".into()],
            })
        })
    }
}
#[test]
fn loadout_keeps_executable_tools_and_filters_outbound_declarations_after_context() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![json!({})]);
        let tools = ["visible", "hidden-declaration"]
            .into_iter()
            .map(|name| {
                tool(
                    name,
                    Arc::new(|_| Box::pin(async { Ok(text_result("ok")) })),
                )
            })
            .collect();
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_hooks(Arc::new(LoadoutHooks))
        .with_tools(tools)
        .with_system_prompt("FORCED EXACT")
        .build()
        .await
        .unwrap();
        session
            .set_active_tools(vec!["visible".into(), "hidden-declaration".into()])
            .unwrap();
        session
            .prompt("go", PromptOptions::default())
            .await
            .unwrap();
        let requests = requests.lock().unwrap();
        let tools = pi_ai::get_current_tools(requests[0].context.messages());
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name.as_str(), "visible");
        assert_eq!(tools[0].description, "patched description");
        let current = pi_ai::get_current_system_message(requests[0].context.messages()).unwrap();
        assert_eq!(pi_ai::get_system_message_text(&current), "FORCED EXACT");
        assert_eq!(session.agent().state().tools.len(), 2);
        assert!(
            !session
                .projection()
                .messages
                .iter()
                .any(|m| m["content"] == "FORCED EXACT")
        );
    });
}

struct Router {
    selected: pi_ai::Model,
    physical: pi_ai::Model,
    reasons: Mutex<Vec<ModelRouteReason>>,
}
impl ModelRuntime for Router {
    fn get_model(&self, p: &str, id: &str) -> Option<pi_ai::Model> {
        [self.selected.clone(), self.physical.clone()]
            .into_iter()
            .find(|m| m.provider.as_str() == p && m.id.as_str() == id)
    }
    fn models(&self) -> Vec<pi_ai::Model> {
        vec![self.selected.clone(), self.physical.clone()]
    }
    fn is_virtual(&self, m: &pi_ai::Model) -> bool {
        m.id == self.selected.id
    }
    fn resolve(&self, request: ModelRouteRequest) -> BoxFuture<'_, Result<ModelRoute, AgentError>> {
        Box::pin(async move {
            self.reasons.lock().unwrap().push(request.reason);
            Ok(ModelRoute {
                model: self.physical.clone(),
                thinking_level: ThinkingLevel::Off,
                state: Some(
                    json!({"routes":request.state.as_ref().and_then(|s|s["routes"].as_u64()).unwrap_or(0)+1}),
                ),
            })
        })
    }
}
#[test]
fn virtual_selection_and_branch_router_state_survive_physical_response_and_reopen() {
    runtime().block_on(async {
        let mut selected = model();
        selected.id = "router".to_owned().try_into().unwrap();
        let router = Arc::new(Router {
            selected: selected.clone(),
            physical: model(),
            reasons: Mutex::new(Vec::new()),
        });
        let store = Arc::new(MemoryStore::new());
        let (stream, requests) = scripted(vec![json!({}), json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(store.clone(), "/tmp").unwrap(),
            selected.clone(),
            stream,
        )
        .with_model_runtime(router.clone())
        .with_tools(vec![])
        .with_system_prompt("route")
        .build()
        .await
        .unwrap();
        session
            .prompt("first", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(session.model().id.as_str(), "router");
        assert_eq!(session.routed_model().unwrap().0.id.as_str(), "fixture");
        assert_eq!(requests.lock().unwrap()[0].model.id.as_str(), "fixture");
        let (unused, _) = scripted(vec![]);
        let reopened = CodingSessionBuilder::new(
            SessionManager::open(store, &session.session_id()).unwrap(),
            model(),
            unused,
        )
        .with_model_runtime(router.clone())
        .with_tools(vec![])
        .with_system_prompt("route")
        .build()
        .await
        .unwrap();
        assert_eq!(reopened.model().id.as_str(), "router");
        session
            .prompt("second", PromptOptions::default())
            .await
            .unwrap();
        let branch = session.snapshot();
        let states = branch
            .entries
            .iter()
            .filter_map(|e| match &e.content {
                EntryContent::Custom { custom_type, data }
                    if custom_type == VIRTUAL_MODEL_STATE_ENTRY =>
                {
                    data.clone()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(states.last().unwrap()["state"]["routes"], 2);
        assert_eq!(
            *router.reasons.lock().unwrap(),
            vec![ModelRouteReason::User, ModelRouteReason::User]
        );
    });
}

#[test]
fn disabling_auto_compaction_preserves_an_overflow_error_for_the_caller() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![
            json!({"stopReason":"error","errorMessage":"context length exceeded","content":[]}),
        ]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_settings(settings())
        .with_tools(vec![])
        .with_system_prompt("overflow")
        .build()
        .await
        .unwrap();
        session
            .prompt("go", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert!(
            session
                .projection()
                .messages
                .iter()
                .any(|m| m["stopReason"] == "error")
        );
        assert!(
            !session
                .snapshot()
                .entries
                .iter()
                .any(|e| matches!(e.content, EntryContent::ContextEdit { .. }))
        );
    });
}

#[test]
fn idle_cache_warming_uses_economics_one_token_cap_and_persisted_usage() {
    runtime().block_on(async {
        tokio::time::pause();
        let mut model_value = serde_json::to_value(model()).unwrap();
        model_value["promptCache"] = json!({"short":300});
        model_value["cost"] = json!({"input":20,"output":2,"cacheRead":1,"cacheWrite":25});
        let warm_model = serde_json::from_value(model_value).unwrap();
        let (stream, requests) = scripted(vec![
            json!({"usage":usage(100000)}),
            json!({"usage":usage(7)}),
        ]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            warm_model,
            stream,
        )
        .with_settings(settings())
        .with_tools(vec![])
        .with_system_prompt("cache")
        .with_cache_warming(CacheWarmingMode::Idle)
        .build()
        .await
        .unwrap();
        session
            .prompt("go", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(
            session.cache_warming_status().state,
            CacheWarmingState::Scheduled
        );
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_millis(270001)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if requests.lock().unwrap().len() == 2 {
                break;
            }
        }
        assert_eq!(requests.lock().unwrap().len(), 2);
        let requests = requests.lock().unwrap();
        assert_eq!(requests[1].options.stream.max_tokens.unwrap().get(), 1);
        assert_eq!(requests[1].options.stream.max_retries.unwrap().get(), 0);
        drop(requests);
        assert!(
            session
                .snapshot()
                .entries
                .iter()
                .any(|e| matches!(&e.content,EntryContent::Usage{kind,..}if kind=="cache_warm"))
        );
        session.set_cache_warming_mode(CacheWarmingMode::Off);
        assert_eq!(
            session.cache_warming_status().state,
            CacheWarmingState::Inactive
        );
    });
}

#[test]
fn navigation_to_user_returns_editor_text_and_summarizes_abandoned_branch() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![
            json!({}),
            json!({}),
            json!({"content":[{"type":"text","text":"Abandoned work."}]}),
        ]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("navigation")
        .build()
        .await
        .unwrap();
        session
            .prompt("first", PromptOptions::default())
            .await
            .unwrap();
        session
            .prompt("second", PromptOptions::default())
            .await
            .unwrap();
        let before = session.snapshot();
        let first = before
            .entries
            .iter()
            .find(|e| matches!(&e.content,EntryContent::Message{message}if message["role"]=="user"))
            .unwrap()
            .clone();
        let result = session
            .navigate_tree(
                &first.id,
                NavigateOptions {
                    summarize: true,
                    label: Some("return".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        {
            let requests = requests.lock().unwrap();
            let summary = &requests[2];
            assert_eq!(
                summary.options.stream.cache_retention,
                Some(pi_ai::CacheRetention::None)
            );
            assert_ne!(
                summary.options.stream.session_id,
                Some(session.session_id())
            );
        }
        assert_eq!(result.editor_text.as_deref(), Some("first"));
        let summary = result.summary_entry.unwrap();
        assert_eq!(summary.parent_id, first.parent_id);
        assert_eq!(session.label(&summary.id).as_deref(), Some("return"));
        assert!(
            session
                .projection()
                .messages
                .iter()
                .any(|m| m["role"] == "branchSummary"
                    && m["summary"].as_str().unwrap().contains("Abandoned work."))
        );
        assert!(session.snapshot().entries.len() > before.entries.len());
        assert!(
            !session
                .projection()
                .messages
                .iter()
                .any(|m| m["role"] == "user" && m["content"][0]["text"] == "second")
        );
    });
}

#[test]
fn branch_cursor_accepts_appends_and_rejects_navigation_away_and_back() {
    runtime().block_on(async{
 let (stream,_)=scripted(vec![json!({}),json!({})]);let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_system_prompt("cursor").build().await.unwrap();session.prompt("first",PromptOptions::default()).await.unwrap();let first=session.snapshot().leaf_id.unwrap();session.prompt("second",PromptOptions::default()).await.unwrap();let last=session.snapshot().leaf_id.unwrap();let cursor=session.branch_cursor();session.append_custom_entry("legitimate",Some(json!(1))).await.unwrap();session.append_custom_entries_if_cursor(&cursor,vec![("script".into(),Some(json!(2)))]).await.unwrap();session.navigate_tree(&first,NavigateOptions::default()).await.unwrap();session.navigate_tree(&last,NavigateOptions::default()).await.unwrap();assert!(session.append_custom_entries_if_cursor(&cursor,vec![("stale".into(),Some(json!(3)))]).await.is_err());assert!(!session.snapshot().entries.iter().any(|entry|matches!(&entry.content,EntryContent::Custom{custom_type,..}if custom_type=="stale")));
});
}

#[test]
fn image_blocking_is_outbound_only_and_deduplicates_adjacent_placeholders() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_image_policy(false, true)
        .with_system_prompt("images")
        .build()
        .await
        .unwrap();
        let image: pi_ai::ImageContent =
            serde_json::from_value(json!({"type":"image","data":"aGk=","mimeType":"image/png"}))
                .unwrap();
        session
            .prompt(
                "inspect",
                PromptOptions {
                    images: vec![image.clone(), image],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let raw = session
            .projection()
            .messages
            .into_iter()
            .find(|message| message["role"] == "user")
            .unwrap();
        assert_eq!(
            raw["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|block| block["type"] == "image")
                .count(),
            2
        );
        let request = serde_json::to_value(requests.lock().unwrap()[0].context.messages()).unwrap();
        let user = request
            .as_array()
            .unwrap()
            .iter()
            .find(|message| message["role"] == "user")
            .unwrap();
        assert_eq!(
            user["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|block| block["text"] == "Image reading is disabled.")
                .count(),
            1
        );
        assert!(
            !user["content"]
                .as_array()
                .unwrap()
                .iter()
                .any(|block| block["type"] == "image")
        );
    });
}

struct InjectedCompaction;
impl SessionHooks for InjectedCompaction {
    fn before_compact(
        &self,
        p: CompactionPreparation,
        _: CompactionReason,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async move {
            Ok(BeforeCompactionResult {
                cancel: false,
                compaction: Some(CompactionResult {
                    summary: "Recovered context".into(),
                    first_kept_entry_id: Some(p.first_kept_entry_id),
                    tokens_before: p.tokens_before,
                    details: None,
                    usage: None,
                }),
            })
        })
    }
}
#[test]
fn recoverable_length_compacts_and_retries_once_while_preserving_raw_attempt() {
    runtime().block_on(async{
 let (stream,requests)=scripted(vec![json!({}),json!({"stopReason":"length","content":[{"type":"text","text":"partial"}],"usage":usage(100)}),json!({"content":[{"type":"text","text":"complete"}],"usage":usage(20)})]);let mut configuration=settings();configuration.compaction=CompactionSettings{enabled:true,keep_recent_tokens:pi_ai::SafeUnsigned::new(1).unwrap(),..Default::default()};let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_settings(configuration).with_tools(vec![]).with_hooks(Arc::new(InjectedCompaction)).with_system_prompt("length").build().await.unwrap();session.prompt("first",PromptOptions::default()).await.unwrap();session.prompt("second",PromptOptions::default()).await.unwrap();assert_eq!(requests.lock().unwrap().len(),3);assert!(session.snapshot().entries.iter().any(|entry|matches!(&entry.content,EntryContent::Message{message}if message["stopReason"]=="length")));assert!(!session.projection().messages.iter().any(|message|message["stopReason"]=="length"));assert_eq!(session.last_assistant_text().as_deref(),Some("complete"));
});
}

#[test]
fn new_session_rebinds_default_tool_environment_and_preserves_selected_loadout() {
    runtime().block_on(async{
 let one=std::env::temp_dir().join(format!("pirua-cwd-one-{}",std::process::id()));let two=std::env::temp_dir().join(format!("pirua-cwd-two-{}",std::process::id()));std::fs::create_dir_all(&one).unwrap();std::fs::create_dir_all(&two).unwrap();std::fs::write(one.join("fixture.txt"),"one").unwrap();std::fs::write(two.join("fixture.txt"),"two").unwrap();let (stream,_)=scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"read-new","name":"read","arguments":{"path":"fixture.txt"}}]}),json!({})]);let store=Arc::new(MemoryStore::new());let session=CodingSessionBuilder::new(SessionManager::create(store.clone(),one.to_string_lossy()).unwrap(),model(),stream).with_system_prompt("cwd").build().await.unwrap();session.set_active_tools(vec!["read".into()]).unwrap();let old=session.session_id();session.new_session(store,two.to_string_lossy()).await.unwrap();assert_ne!(session.session_id(),old);assert_eq!(session.active_tool_names(),vec!["read"]);session.prompt("read",PromptOptions::default()).await.unwrap();let result=session.projection().messages.into_iter().find(|message|message["role"]=="toolResult").unwrap();assert_eq!(result["content"][0]["text"],"two");session.dispose().await.unwrap();std::fs::remove_dir_all(one).unwrap();std::fs::remove_dir_all(two).unwrap();
});
}

struct BlockingCompaction {
    entered: Arc<tokio::sync::Notify>,
}
impl SessionHooks for BlockingCompaction {
    fn before_compact(
        &self,
        _: CompactionPreparation,
        _: CompactionReason,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}
#[test]
fn dropping_manual_compaction_releases_idle_and_cancellation_ownership() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let (stream, _) = scripted(vec![json!({})]);
        let mut configuration = settings();
        configuration.compaction.keep_recent_tokens = pi_ai::SafeUnsigned::new(1).unwrap();
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_settings(configuration)
        .with_tools(vec![])
        .with_hooks(Arc::new(BlockingCompaction {
            entered: entered.clone(),
        }))
        .with_system_prompt("drop")
        .build()
        .await
        .unwrap();
        session
            .prompt("first", PromptOptions::default())
            .await
            .unwrap();
        let running = tokio::spawn({
            let session = session.clone();
            async move { session.compact(None).await }
        });
        entered.notified().await;
        assert!(session.is_compacting());
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), session.wait_for_idle())
            .await
            .unwrap()
            .unwrap();
        assert!(session.is_idle());
    });
}

#[test]
fn typed_context_and_boundary_rejections_leave_canonical_snapshot_unchanged() {
    runtime().block_on(async {
        let (stream, _) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(), stream,
        ).with_tools(vec![]).with_system_prompt("admission").build().await.unwrap();
        session.prompt("hello", PromptOptions::default()).await.unwrap();
        let before = session.snapshot();
        let assistant = before.entries.iter().find(|entry|
            matches!(&entry.content, EntryContent::Message { message } if message["role"] == "assistant")
        ).unwrap().id.clone();
        let edit = SessionBoundaryDraft::ContextEdit {
            target_id: assistant.clone(),
            replacement: Some(pi_coding_session::ContentReplacement { content: json!([{ "type": "not-a-content-block" }]) }),
        };
        let custom = SessionBoundaryDraft::CustomMessage {
            custom_type: "invalid".into(), content: json!([{ "type": "not-a-content-block" }]),
            display: false, details: None,
        };
        for invalid in [edit, custom] {
            assert!(session.preview_boundary_drafts(std::slice::from_ref(&invalid)).await.is_err());
            assert_eq!(session.snapshot(), before);
            assert!(session.commit_boundary_drafts(vec![invalid]).await.is_err());
            assert_eq!(session.snapshot(), before);
        }
        assert!(session.edit_context(&assistant, Some(pi_coding_session::ContentReplacement { content: json!([{ "type": "not-a-content-block" }]) })).await.is_err());
        assert_eq!(session.snapshot(), before);
        session.wait_for_idle().await.unwrap();
    });
}

struct CountNestedAfter(Arc<std::sync::atomic::AtomicU64>);
impl SessionHooks for CountNestedAfter {
    fn after_tool(
        &self,
        _: pi_agent::AfterToolCallContext,
        _: Option<String>,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<pi_agent::AfterToolCallResult>, AgentError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
}
#[test]
fn nested_rejected_progress_propagates_without_after_hook_or_success_event() {
    runtime().block_on(async {
        let after = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (stream, _) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(), model(), stream)
            .with_tools(vec![]).with_hooks(Arc::new(CountNestedAfter(after.clone()))).with_system_prompt("nested failure").build().await.unwrap();
        session.prompt("seed", PromptOptions::default()).await.unwrap();
        let mut registered = RegisteredTool::direct(tool("progress", Arc::new(|invocation| Box::pin(async move {
            (invocation.on_update)(text_result("partial"));
            Ok(text_result("complete"))
        }))));
        registered.exposure = ToolExposure::Deferred;
        session.register_tool(registered).unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let capture = events.clone();
        session.subscribe(Arc::new(move |event| { capture.lock().unwrap().push(event); Box::pin(async { Ok(()) }) }));
        let result = session.execute_tool("outer", "progress", Default::default(), CancellationToken::new(), Some(Arc::new(|_| Box::pin(async { Err(AgentError::Event("progress rejected".into())) })))).await;
        assert!(matches!(result, Err(SessionError::Agent(AgentError::Event(message))) if message == "progress rejected"));
        assert_eq!(after.load(Ordering::SeqCst), 0);
        assert!(!events.lock().unwrap().iter().any(|event| matches!(event, SessionEvent::NestedTool { event: AgentEvent::ToolExecutionEnd { .. }, .. })));
        let result = session.execute_tool("outer", "progress", Default::default(), CancellationToken::new(), None).await.unwrap();
        assert!(!result.is_error);
        assert_eq!(result.tool_call.id.as_str(), "outer/2");
        assert_eq!(after.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn default_thinking_is_medium_clamped_and_latest_empty_text_is_not_skipped() {
    runtime().block_on(async {
        let mut reasoning = model();
        reasoning.reasoning = true;
        let (stream, _) = scripted(vec![
            json!({"content":[{"type":"text","text":"older"}]}),
            json!({"content":[]}),
        ]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            reasoning.clone(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("thinking")
        .build()
        .await
        .unwrap();
        assert_eq!(session.thinking_level(), pi_ai::ModelThinkingLevel::Medium);
        session
            .prompt("first", PromptOptions::default())
            .await
            .unwrap();
        session
            .prompt("second", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(session.last_assistant_text(), None);
        let (unused, _) = scripted(vec![]);
        let explicit = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            reasoning,
            unused,
        )
        .with_thinking_level(pi_ai::ModelThinkingLevel::High)
        .with_tools(vec![])
        .with_system_prompt("thinking")
        .build()
        .await
        .unwrap();
        assert_eq!(explicit.thinking_level(), pi_ai::ModelThinkingLevel::High);
        let (unused, _) = scripted(vec![]);
        let clamped = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            unused,
        )
        .with_tools(vec![])
        .with_system_prompt("thinking")
        .build()
        .await
        .unwrap();
        assert_eq!(clamped.thinking_level(), pi_ai::ModelThinkingLevel::Off);
    });
}

#[test]
fn custom_images_are_blocked_after_conversion_and_retained_in_canonical_records() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("custom images")
        .with_image_policy(false, true)
        .build()
        .await
        .unwrap();
        session
            .send_custom_message(
                "image",
                json!([
                    {"type":"image","data":"aGk=","mimeType":"image/png"},
                    {"type":"image","data":"aGk=","mimeType":"image/png"}
                ]),
                false,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        let raw = session
            .projection()
            .messages
            .into_iter()
            .find(|message| message["role"] == "custom")
            .unwrap();
        assert_eq!(
            raw["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|block| block["type"] == "image")
                .count(),
            2
        );
        let outbound =
            serde_json::to_value(requests.lock().unwrap()[0].context.messages()).unwrap();
        assert!(!outbound.to_string().contains("\"type\":\"image\""));
        let user = outbound
            .as_array()
            .unwrap()
            .iter()
            .find(|message| message["role"] == "user")
            .unwrap();
        assert_eq!(
            user["content"],
            json!([{"type":"text","text":"Image reading is disabled."}])
        );
    });
}

#[test]
fn direct_summary_requests_disable_cache_and_allocate_fresh_routing_identity() {
    runtime().block_on(async {
        let (stream, requests) = scripted(vec![json!({}), json!({}), json!({}), json!({})]);
        let mut configuration = settings();
        configuration.compaction.keep_recent_tokens = pi_ai::SafeUnsigned::new(1).unwrap();
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_settings(configuration)
        .with_tools(vec![])
        .with_system_prompt("summary identity")
        .build()
        .await
        .unwrap();
        session.prompt("one", Default::default()).await.unwrap();
        session.prompt("two", Default::default()).await.unwrap();
        session.compact(None).await.unwrap();
        let requests = requests.lock().unwrap();
        let mut summary_ids = std::collections::BTreeSet::new();
        for request in &requests[2..] {
            assert_eq!(
                request.options.stream.cache_retention,
                Some(pi_ai::CacheRetention::None)
            );
            let id = request.options.stream.session_id.as_ref().unwrap();
            assert_ne!(id, &session.session_id());
            assert_eq!(id.len(), 36);
            assert_eq!(&id[14..15], "7");
            assert!(summary_ids.insert(id.clone()));
        }
        assert!(!summary_ids.is_empty());
    });
}

#[test]
fn direct_summary_retry_obeys_zero_custom_and_default_delay_caps() {
    runtime().block_on(async {
        tokio::time::pause();
        for (cap, base, expected) in [(Some(0), 1000, 0), (Some(7), 1000, 7), (None, 70000, 60000)]
        {
            let (script, requests) = scripted(vec![
                json!({}),
                json!({}),
                json!({"stopReason":"error","errorMessage":"HTTP 503 server unavailable"}),
                json!({}),
                json!({}),
            ]);
            let times = Arc::new(Mutex::new(Vec::new()));
            let capture = times.clone();
            let stream: pi_agent::StreamFn = Arc::new(move |request| {
                capture.lock().unwrap().push(tokio::time::Instant::now());
                script(request)
            });
            let mut configuration = settings();
            configuration.compaction.keep_recent_tokens = pi_ai::SafeUnsigned::new(1).unwrap();
            configuration.retry = pi_ai::RetryPolicy {
                enabled: true,
                max_retries: pi_ai::MaxRetries::new(1).unwrap(),
                base_delay_ms: pi_ai::SafeUnsigned::new(base).unwrap(),
                max_agent_delay_ms: cap.map(|cap| pi_ai::SafeUnsigned::new(cap).unwrap()),
            };
            let session = CodingSessionBuilder::new(
                SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
                model(),
                stream,
            )
            .with_settings(configuration)
            .with_tools(vec![])
            .with_system_prompt("summary retry")
            .build()
            .await
            .unwrap();
            session.prompt("one", Default::default()).await.unwrap();
            session.prompt("two", Default::default()).await.unwrap();
            session.compact(None).await.unwrap();
            let times = times.lock().unwrap();
            let elapsed = times[3].duration_since(times[2]);
            // AI preserves Node's 1ms timer admission for a zero policy delay;
            // Tokio's timer wheel may add one further millisecond while paused.
            let timer_delay = expected.max(1);
            assert!(
                elapsed >= std::time::Duration::from_millis(timer_delay)
                    && elapsed <= std::time::Duration::from_millis(timer_delay + 1),
                "cap {cap:?}, expected {expected}ms, observed {elapsed:?}"
            );
            let requests = requests.lock().unwrap();
            assert_eq!(
                requests[2].options.stream.session_id,
                requests[3].options.stream.session_id
            );
        }
    });
}

#[test]
fn disposal_cancels_admitted_provider_and_callbacks_observe_closed_admission() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let captured = Arc::new(Mutex::new(None));
        let stream: pi_agent::StreamFn = Arc::new({
            let (entered, cancelled, release, captured) = (
                entered.clone(),
                cancelled.clone(),
                release.clone(),
                captured.clone(),
            );
            move |request| {
                let (entered, cancelled, release, captured) = (
                    entered.clone(),
                    cancelled.clone(),
                    release.clone(),
                    captured.clone(),
                );
                Box::pin(async move {
                    let result = response(request.clone(), json!({}));
                    *captured.lock().unwrap() = Some(request.cancellation.clone());
                    entered.notify_one();
                    request.cancellation.cancelled().await;
                    cancelled.notify_one();
                    release.notified().await;
                    Ok(result)
                })
            }
        });
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_system_prompt("provider disposal")
        .build()
        .await
        .unwrap();
        let reentered = Arc::new(AtomicBool::new(false));
        let weak = session.downgrade();
        let capture = reentered.clone();
        session.subscribe(Arc::new(move |event| {
            let weak = weak.clone();
            let capture = capture.clone();
            Box::pin(async move {
                if matches!(event, SessionEvent::AgentSettled { .. }) {
                    assert!(matches!(
                        weak.upgrade()
                            .unwrap()
                            .prompt("from settlement callback", Default::default())
                            .await,
                        Err(SessionError::Disposed)
                    ));
                    capture.store(true, Ordering::SeqCst);
                }
                Ok(())
            })
        }));
        let prompt = tokio::spawn({
            let session = session.clone();
            async move { session.prompt("admitted", Default::default()).await }
        });
        entered.notified().await;
        let disposing = tokio::spawn({
            let session = session.clone();
            async move { session.dispose().await }
        });
        cancelled.notified().await;
        assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
        assert!(!disposing.is_finished());
        assert!(matches!(
            session
                .prompt("after shutdown boundary", Default::default())
                .await,
            Err(SessionError::Disposed)
        ));
        release.notify_one();
        let _ = prompt.await.unwrap();
        disposing.await.unwrap().unwrap();
        assert!(session.is_disposed() && !session.is_streaming());
        assert!(reentered.load(Ordering::SeqCst));
    });
}

#[path = "tracked_inputs.rs"]
mod tracked_inputs;

#[path = "tool_context.rs"]
mod tool_context;
