mod adapter_support;
use adapter_support::*;
use pi_agent::{AgentError, BoxFuture, CancellationToken};
use pi_coding_runtime::{codemode::*, mcp::*, *};
use pi_coding_session::{SessionManager, SessionStore, SqliteStore};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn exact_tag_codemode_observations_use_actual_worker() {
    session::runtime().block_on(async {
        let trace=source_trace();let mut checked=0;
        for row in source_cases() {
            let expected=&trace["observations"][row["id"].as_str().unwrap()];
            let actual=match row["kind"].as_str().unwrap() {
                "parse"=>match parse_codemode_source(row["source"].as_str().unwrap()) {
                    Ok(parsed)=>{let mut options=json!({});if let Some(timeout)=parsed.timeout{options["timeoutMs"]=json!(timeout.as_millis() as u64);}
if let Some(max)=parsed.max_output_tokens{options["maxOutputTokens"]=json!(max);}json!({"code":parsed.code,"options":options})},
                    Err(_)=>json!({"error":true}),
                },
                "format"=>{
                    let coding=coding(vec![],Arc::new(NoopSessionHooks)).await;let adapter=adapter(&coding);adapter.attach().unwrap();let result=invoke(&coding,"codemode",json!({"code":"text('a'); console.log('log'); text('b');"})).await;assert!(!result.is_error);serde_json::to_value(&result.result.content[1..]).unwrap()
                },
                "script-value"=>{
                    let mut result:pi_agent::AgentToolResult=serde_json::from_value(row["outcome"]["result"].clone()).unwrap();result.is_error=row["outcome"]["isError"].as_bool();
                    let mut sample=session::tool("sample",Arc::new(move|_|{let result=result.clone();Box::pin(async move{Ok(result)})}));if let Some(schema)=row["tool"].get("outputSchema") {sample.output_schema=Some(serde_json::from_value(schema.clone()).unwrap());}
                    let coding=coding(vec![sample],Arc::new(NoopSessionHooks)).await;let adapter=adapter(&coding);adapter.attach().unwrap();let result=invoke(&coding,"codemode",json!({"code":"try {return {value:await tools.sample({})};}catch(e){return {error:e.message};}"})).await;assert!(!result.is_error);serde_json::from_str(text_of(&result.result).lines().last().unwrap()).unwrap()
                },
                "search"=>{
                    let tools=row["tools"].as_array().unwrap().iter().map(|row|pi_agent::AgentTool::new(serde_json::from_value(row.clone()).unwrap(),Arc::new(|_|Box::pin(async{Ok(Default::default())})))).collect();let coding=coding(tools,Arc::new(NoopSessionHooks)).await;let adapter=adapter(&coding);adapter.attach().unwrap();let result=invoke(&coding,"codemode",json!({"code":format!("return (await searchTools({})).map(t=>t.name);",row["query"])})).await;assert!(!result.is_error);serde_json::from_str(text_of(&result.result).lines().last().unwrap()).unwrap()
                },
                _=>continue,
            };
            assert_eq!(&actual,expected,"{}",row["id"]);checked+=1;
        }
        assert_eq!(checked,11);
    });
}

#[derive(Default)]
struct Hooks {
    parents: Mutex<Vec<(String, Option<String>)>>,
    events: Mutex<Vec<String>>,
}
impl SessionHooks for Hooks {
    fn before_tool(
        &self,
        event: pi_agent::BeforeToolCallContext,
        parent: Option<String>,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<pi_agent::BeforeToolCallResult>, AgentError>> {
        Box::pin(async move {
            let name = event.tool_call.name.to_string();
            self.parents.lock().unwrap().push((name.clone(), parent));
            if name == "echo" {
                Ok(Some(pi_agent::BeforeToolCallResult {
                    args: Some(serde_json::from_value(json!({"value":9})).unwrap()),
                    ..Default::default()
                }))
            } else {
                Ok(None)
            }
        })
    }
    fn event(&self, event: SessionEvent) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async move {
            if let SessionEvent::NestedTool {
                parent_tool_call_id,
                event,
            } = event
            {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("{parent_tool_call_id}:{event:?}"));
            }
            Ok(())
        })
    }
}
fn adapter(session: &CodingSession) -> CodeModeAdapter {
    CodeModeAdapter::new(
        session.downgrade(),
        sandbox(),
        CodeModeOptions {
            save_output: Some(Arc::new(|_, _| {
                Box::pin(async { Ok("/fixture/output".into()) })
            })),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn options_match_target_and_preserve_source_lines() {
    let parsed = parse_codemode_source(
        "  // @options: {\"timeout_ms\":25,\"max_output_tokens\":0}\r\nreturn 1",
    )
    .unwrap();
    assert_eq!(parsed.code, "\nreturn 1");
    assert_eq!(parsed.timeout, Some(std::time::Duration::from_millis(25)));
    assert_eq!(parsed.max_output_tokens, Some(0));
    for source in [
        "",
        " // @options: {}",
        "// @options: null\nreturn 1",
        "// @options: {\"timeout_ms\":0}\nreturn 1",
        "// @options: {\"timeout_ms\":2147483648}\nreturn 1",
        "// @options: {\"max_output_tokens\":-1}\nreturn 1",
        "// @options: {\"unknown\":1}\nreturn 1",
        "text(1);\n// @options: {}\nreturn 1",
    ] {
        assert!(parse_codemode_source(source).is_err(), "{source}");
    }
}

#[test]
fn actual_worker_nested_hooks_mcp_images_usage_and_sqlite_transcript() {
    session::runtime().block_on(async {
        let path=sqlite_path("nested");let store=Arc::new(SqliteStore::open(&path).unwrap());let manager=SessionManager::create(store.clone(),"/tmp").unwrap();let id=manager.header().id.clone();
        let hooks=Arc::new(Hooks::default());let count=Arc::new(AtomicUsize::new(0));let capture=count.clone();
        let echo=session::tool("echo",Arc::new(move|invocation|{let capture=capture.clone();Box::pin(async move{capture.fetch_add(1,Ordering::SeqCst);let args=serde_json::to_value(invocation.arguments).unwrap();let mut result=session::text_result(&format!("echo {}",args["value"]));result.usage=Some(serde_json::from_value(session::usage(4)).unwrap());Ok(result)})}));
        let code="text(await tools.echo({value:1})); const r=await tools.mcp__peer__echo({value:2}); text(r.structuredContent.n); image(r.content[1]); store('answer',r.structuredContent.n); return load('answer');";
        let (stream,requests)=session::scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"script","name":"codemode","arguments":{"code":code}}]}),json!({})]);
        let coding=CodingSessionBuilder::new(manager,session::model(),stream).with_tools(vec![echo]).with_hooks(hooks.clone()).with_system_prompt("test").build().await.unwrap();
        coding.set_active_tools(vec!["echo".into()]).unwrap();let (client,peer)=client(vec![mcp_tool("echo")]).await;let registry=McpToolRegistry::new(coding.downgrade());registry.attach("peer",client.clone(),Default::default(),CancellationToken::new()).await.unwrap();let adapter=adapter(&coding);adapter.attach().unwrap();
        coding.prompt("batch",Default::default()).await.unwrap();assert_eq!(count.load(Ordering::SeqCst),1,"{:?}",coding.projection());assert_eq!(peer.calls.lock().unwrap().len(),1);assert_eq!(requests.lock().unwrap().len(),2);
        let snapshot=coding.snapshot();let result=snapshot.entries.iter().filter_map(|e|if let pi_coding_session::EntryContent::Message{message}=&e.content {Some(message)}else{None}).find(|m|m["role"]=="toolResult").unwrap();
        assert_eq!(result["isError"],false,"{result}");assert_eq!(result["nestedCalls"]["calls"].as_array().unwrap().len(),2);assert_eq!(result["nestedCalls"]["complete"],true);assert_eq!(result["nestedCalls"]["calls"][0]["id"],"script/1");assert_eq!(result["nestedCalls"]["calls"][1]["id"],"script/2");assert_eq!(result["usage"]["input"],4);assert!(result["content"].as_array().unwrap().iter().any(|c|c["type"]=="image"));assert!(result.to_string().contains("echo 9"));assert_eq!(read_codemode_store(&snapshot)["answer"],7);
        assert!(hooks.parents.lock().unwrap().contains(&("echo".into(),Some("script".into()))));assert!(hooks.parents.lock().unwrap().contains(&("mcp__peer__echo".into(),Some("script".into()))));assert!(hooks.events.lock().unwrap().iter().any(|e|e.contains("ToolExecutionEnd")));
        let (unused,_)=session::scripted(vec![]);let reopened=CodingSessionBuilder::new(SessionManager::open(store.clone(),&id).unwrap(),session::model(),unused).with_tools(vec![]).with_system_prompt("test").build().await.unwrap();assert_eq!(read_codemode_store(&reopened.snapshot())["answer"],7);assert_eq!(count.load(Ordering::SeqCst),1);
        client.close().await.unwrap();drop(reopened);drop(coding);drop(store);std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn success_store_reopens_forks_and_failed_scripts_never_commit() {
    session::runtime().block_on(async {
        let path = sqlite_path("store");
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        let manager = manager_with_parent(store.clone());
        let id = manager.header().id.clone();
        let (stream, _) = session::scripted(vec![]);
        let coding = CodingSessionBuilder::new(manager, session::model(), stream)
            .with_tools(vec![])
            .with_system_prompt("test")
            .build()
            .await
            .unwrap();
        let adapter = adapter(&coding);
        adapter.attach().unwrap();
        assert!(
            !invoke(
                &coding,
                "codemode",
                json!({"code":"store('x',1); text(load('x'));"})
            )
            .await
            .is_error
        );
        let first = coding.snapshot().leaf_id.unwrap();
        assert!(
            !invoke(
                &coding,
                "codemode",
                json!({"code":"store('x',2); store('remove',true);"})
            )
            .await
            .is_error
        );
        let before = coding.snapshot();
        let failed = invoke(
            &coding,
            "codemode",
            json!({"code":"store('x',999); throw new Error('deliberate');"}),
        )
        .await;
        assert!(failed.is_error);
        assert!(text_of(&failed.result).contains("deliberate"));
        assert_eq!(coding.snapshot(), before);
        coding
            .navigate_tree(&first, Default::default())
            .await
            .unwrap();
        assert_eq!(read_codemode_store(&coding.snapshot())["x"], 1);
        assert!(!read_codemode_store(&coding.snapshot()).contains_key("remove"));
        invoke(&coding, "codemode", json!({"code":"store('x',3);"})).await;
        assert_eq!(read_codemode_store(&coding.snapshot())["x"], 3);
        let persisted = store.load(&id).unwrap();
        assert_eq!(read_codemode_store(&persisted)["x"], 3);
        coding.fork(&first).await.unwrap();
        assert_ne!(coding.session_id(), id);
        assert_eq!(read_codemode_store(&coding.snapshot())["x"], 1);
        assert_eq!(read_codemode_store(&store.load(&id).unwrap())["x"], 3);
        drop(coding);
        drop(store);
        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn structured_errors_resolve_text_errors_reject_and_output_is_formatted() {
    session::runtime().block_on(async {
        let mut structured=session::tool("structured",Arc::new(|_|Box::pin(async{Ok(serde_json::from_value(json!({"content":[{"type":"text","text":"failure"}],"structuredContent":{"isError":true,"answer":42},"isError":true})).unwrap())})));structured.output_schema=Some(serde_json::from_value(json!({"type":"object"})).unwrap());
        let plain=session::tool("plain",Arc::new(|_|Box::pin(async{Err(AgentError::Tool("plain failure".into()))})));
        let coding=coding(vec![structured,plain],Arc::new(NoopSessionHooks)).await;let adapter=adapter(&coding);adapter.attach().unwrap();
        let result=invoke(&coding,"codemode",json!({"code":"const result=await tools.structured({}); text(result.answer); try {await tools.plain({});}catch(e){text(e.message);} console.log('diagnostic'); return 'returned';"})).await;assert!(!result.is_error);let text=text_of(&result.result);assert!(text.contains("==> text 1/3 <==\n42"));assert!(text.contains("plain failure"));assert!(text.contains("==> text 3/3 <==\nreturned"));assert!(text.contains("<console_output>\ndiagnostic\n</console_output>"));
        let result=invoke(&coding,"codemode",json!({"code":"text((await searchTools('structured'))[0].name); text(await describeTool('structured')); text(ALL_TOOLS.some(t=>t.name==='codemode'));"})).await;assert!(!result.is_error);let text=text_of(&result.result);assert!(text.contains("structured"));assert!(text.contains("-> {\"type\":\"object\"}"));assert!(text.contains("false"));
    });
}

#[test]
fn cancellation_timeout_and_end_of_script_cancel_pending_nested_calls() {
    session::runtime().block_on(async {
        let started = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(AtomicUsize::new(0));
        let capture = started.clone();
        let cancel_capture = cancelled.clone();
        let waiting = session::tool(
            "waiting",
            Arc::new(move |invocation| {
                let capture = capture.clone();
                let cancelled = cancel_capture.clone();
                Box::pin(async move {
                    capture.notify_one();
                    invocation.cancellation.cancelled().await;
                    cancelled.fetch_add(1, Ordering::SeqCst);
                    Err(AgentError::Tool("cancelled".into()))
                })
            }),
        );
        let fast = session::tool(
            "fast",
            Arc::new(|_| Box::pin(async { Ok(session::text_result("fast")) })),
        );
        let coding = coding(vec![waiting, fast], Arc::new(NoopSessionHooks)).await;
        let adapter = adapter(&coding);
        adapter.attach().unwrap();
        let signal = CancellationToken::new();
        let call = coding.execute_tool(
            "parent",
            "codemode",
            serde_json::from_value(json!({"code":"store('bad',1); await tools.waiting({});"}))
                .unwrap(),
            signal.clone(),
            None,
        );
        tokio::pin!(call);
        let cancel = async {
            started.notified().await;
            signal.cancel();
        };
        let (result, _) = tokio::join!(call, cancel);
        assert!(result.unwrap().is_error);
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        assert!(!read_codemode_store(&coding.snapshot()).contains_key("bad"));
        let outcome = invoke(
            &coding,
            "codemode",
            json!({"code":"// @options: {\"timeout_ms\":50}\nstore('bad',2); while(true){}"}),
        )
        .await;
        assert!(outcome.is_error);
        assert!(
            text_of(&outcome.result).contains("timeout")
                || text_of(&outcome.result).contains("deadline")
        );
        assert!(!read_codemode_store(&coding.snapshot()).contains_key("bad"));
        let outcome = invoke(
            &coding,
            "codemode",
            json!({"code":"tools.waiting({}); await tools.fast({}); return 'done';"}),
        )
        .await;
        assert!(!outcome.is_error);
        assert_eq!(cancelled.load(Ordering::SeqCst), 2);
        assert!(text_of(&outcome.result).contains("done"));
    });
}

#[test]
fn branch_cursor_allows_nested_append_but_rejects_navigation_away_and_back() {
    session::runtime().block_on(async {
        let coding = coding(vec![], Arc::new(NoopSessionHooks)).await;
        let first = coding.append_custom_entry("anchor", None).await.unwrap().id;
        let second = coding.append_custom_entry("anchor", None).await.unwrap().id;
        let weak = coding.downgrade();
        let append = session::tool(
            "append",
            Arc::new(move |_| {
                let weak = weak.clone();
                Box::pin(async move {
                    weak.upgrade()
                        .unwrap()
                        .append_custom_entry("nested", None)
                        .await
                        .unwrap();
                    Ok(session::text_result("ok"))
                })
            }),
        );
        coding
            .register_tool(RegisteredTool::direct(append))
            .unwrap();
        let adapter = adapter(&coding);
        adapter.attach().unwrap();
        let outcome = invoke(
            &coding,
            "codemode",
            json!({"code":"await tools.append({}); store('same',1);"}),
        )
        .await;
        assert!(!outcome.is_error);
        assert_eq!(read_codemode_store(&coding.snapshot())["same"], 1);
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let capture = started.clone();
        let rel = release.clone();
        let wait = session::tool(
            "wait",
            Arc::new(move |_| {
                let capture = capture.clone();
                let release = rel.clone();
                Box::pin(async move {
                    capture.notify_one();
                    release.notified().await;
                    Ok(session::text_result("ok"))
                })
            }),
        );
        coding.register_tool(RegisteredTool::direct(wait)).unwrap();
        let call = invoke(
            &coding,
            "codemode",
            json!({"code":"await tools.wait({}); store('wrongBranch',true);"}),
        );
        tokio::pin!(call);
        let navigate = async {
            started.notified().await;
            coding
                .navigate_tree(&first, Default::default())
                .await
                .unwrap();
            coding
                .navigate_tree(&second, Default::default())
                .await
                .unwrap();
            release.notify_one();
        };
        let (outcome, _) = tokio::join!(call, navigate);
        assert!(outcome.is_error);
        assert!(!read_codemode_store(&coding.snapshot()).contains_key("wrongBranch"));
    });
}

#[test]
fn tool_weak_session_does_not_keep_session_alive_and_duplicate_attach_rejects() {
    session::runtime().block_on(async {
        let coding = coding(vec![], Arc::new(NoopSessionHooks)).await;
        let weak = coding.downgrade();
        let installed = adapter(&coding);
        installed.attach().unwrap();
        let duplicate = adapter(&coding);
        assert!(duplicate.attach().is_err());
        let tool = installed.tool().unwrap().tool;
        drop(coding);
        assert!(weak.upgrade().is_none());
        let result = (tool.execute)(pi_agent::ToolInvocation {
            call_id: "gone".into(),
            arguments: serde_json::from_value(json!({"code":"return 1"})).unwrap(),
            cancellation: CancellationToken::new(),
            on_update: Arc::new(|_| {}),
        })
        .await;
        assert!(result.is_err());
    });
}

#[test]
fn actual_stdio_mcp_peer_runs_through_codemode_and_reopens_sqlite() {
    session::runtime().block_on(async {
        let path=sqlite_path("stdio");let store=Arc::new(SqliteStore::open(&path).unwrap());let manager=manager_with_parent(store.clone());let id=manager.header().id.clone();let (stream,_)=session::scripted(vec![]);
        let coding=CodingSessionBuilder::new(manager,session::model(),stream).with_tools(vec![]).with_system_prompt("test").build().await.unwrap();
        let mut options=pi_mcp::transport::StdioOptions::new("python3");options.inherit_env=false;options.args=vec![format!("{}/../pi-mcp/tests/fixtures/stdio_peer.py",env!("CARGO_MANIFEST_DIR"))];
        let transport=Arc::new(pi_mcp::transport::StdioTransport::new(options));let client=pi_mcp::McpClient::connect(pi_mcp::ClientOptions::new("coding-test","1"),transport.clone()).await.unwrap();
        let registry=McpToolRegistry::new(coding.downgrade());registry.attach("stdio",client.clone(),Default::default(),CancellationToken::new()).await.unwrap();let installed=adapter(&coding);installed.attach().unwrap();
        let result=invoke(&coding,"codemode",json!({"code":"const result=await tools.mcp__stdio__echo({word:'world'}); text(result.content[0].text); store('reply',result.content[0].text);"})).await;assert!(!result.is_error);assert!(text_of(&result.result).contains("你好"));assert!(read_codemode_store(&store.load(&id).unwrap())["reply"].as_str().unwrap().contains("world"));
        let (unused,_)=session::scripted(vec![]);let reopened=CodingSessionBuilder::new(SessionManager::open(store.clone(),&id).unwrap(),session::model(),unused).with_tools(vec![]).with_system_prompt("test").build().await.unwrap();assert_eq!(read_codemode_store(&reopened.snapshot()),read_codemode_store(&coding.snapshot()));assert!(reopened.messages().unwrap().iter().all(|m| !matches!(m,pi_agent::AgentMessage::BuiltIn(pi_ai::Message::ToolResult(_)))));client.close().await.unwrap();assert!(transport.pid().is_none());drop(reopened);drop(coding);drop(store);std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn successful_same_branch_scripts_commit_atomic_deltas_in_completion_order() {
    session::runtime().block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3),async {
            let (ready,mut ready_calls)=tokio::sync::mpsc::unbounded_channel();let first=Arc::new(tokio::sync::Notify::new());let second=Arc::new(tokio::sync::Notify::new());let a=first.clone();let b=second.clone();
            let wait=session::tool("wait",Arc::new(move|i|{let ready=ready.clone();let a=a.clone();let b=b.clone();Box::pin(async move{let args=serde_json::to_value(i.arguments).unwrap();let id=args["id"].as_u64().unwrap();ready.send(id).unwrap();if id==1{a.notified().await}else{b.notified().await}Ok(session::text_result("ok"))})}));
            let coding=coding(vec![wait],Arc::new(NoopSessionHooks)).await;let adapter=adapter(&coding);adapter.attach().unwrap();
            let script_a=coding.execute_tool("a","codemode",serde_json::from_value(json!({"code":"await tools.wait({id:1}); store('x',1); store('left',true);"})).unwrap(),CancellationToken::new(),None);
            let script_b=coding.execute_tool("b","codemode",serde_json::from_value(json!({"code":"await tools.wait({id:2}); store('x',2); store('right',true);"})).unwrap(),CancellationToken::new(),None);
            let a=coding.clone();let release=async move {let _=ready_calls.recv().await.unwrap();let _=ready_calls.recv().await.unwrap();second.notify_one();while read_codemode_store(&a.snapshot()).get("x")!=Some(&json!(2)){tokio::task::yield_now().await;}first.notify_one();};
            let (a,b,_)=tokio::join!(script_a,script_b,release);assert!(!a.unwrap().is_error);assert!(!b.unwrap().is_error);let snapshot=coding.snapshot();let store=read_codemode_store(&snapshot);assert_eq!(store["x"],1);assert_eq!(store["left"],true);assert_eq!(store["right"],true);let entries=snapshot.entries.iter().filter(|e|matches!(&e.content,pi_coding_session::EntryContent::Custom{custom_type,..}if custom_type==CODEMODE_STORE_ENTRY_TYPE)).count();assert_eq!(entries,2);
        }).await.unwrap();
    });
}

#[test]
fn cancellation_during_output_save_never_commits_successful_vm_writes() {
    session::runtime().block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let capture = entered.clone();
            let rel = release.clone();
            let coding = coding(vec![], Arc::new(NoopSessionHooks)).await;
            let adapter = CodeModeAdapter::new(
                coding.downgrade(),
                sandbox(),
                CodeModeOptions {
                    max_output_tokens: 1,
                    save_output: Some(Arc::new(move |_, _| {
                        let entered = capture.clone();
                        let release = rel.clone();
                        Box::pin(async move {
                            entered.notify_one();
                            release.notified().await;
                            Ok("/fixture/output".into())
                        })
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
            adapter.attach().unwrap();
            let signal = CancellationToken::new();
            let call = coding.execute_tool(
                "parent",
                "codemode",
                serde_json::from_value(
                    json!({"code":"text('long output'); store('aborted',true);"}),
                )
                .unwrap(),
                signal.clone(),
                None,
            );
            let cancel = async {
                entered.notified().await;
                signal.cancel();
                release.notify_one();
            };
            let (result, _) = tokio::join!(call, cancel);
            assert!(result.unwrap().is_error);
            assert!(!read_codemode_store(&coding.snapshot()).contains_key("aborted"));
        })
        .await
        .unwrap();
    });
}

#[test]
fn sqlite_commit_conflict_is_observable_and_keeps_script_delta_absent() {
    session::runtime().block_on(async {
        let path=sqlite_path("conflict");let store=Arc::new(SqliteStore::open(&path).unwrap());let manager=manager_with_parent(store.clone());let id=manager.header().id.clone();let external=store.clone();let session_id=id.clone();let write=session::tool("external",Arc::new(move|_|{let external=external.clone();let id=session_id.clone();Box::pin(async move{SessionManager::open(external,&id).unwrap().append(pi_coding_session::EntryContent::Custom{custom_type:"external".into(),data:None}).unwrap();Ok(session::text_result("external commit"))})}));
        let (stream,_)=session::scripted(vec![]);let coding=CodingSessionBuilder::new(manager,session::model(),stream).with_tools(vec![write]).with_system_prompt("test").build().await.unwrap();coding.set_active_tools(vec!["external".into()]).unwrap();let adapter=adapter(&coding);adapter.attach().unwrap();let before=coding.snapshot();let result=invoke(&coding,"codemode",json!({"code":"await tools.external({}); store('uncommitted',true);"})).await;assert!(result.is_error);assert!(text_of(&result.result).contains("revision conflict"));assert_eq!(coding.snapshot(),before);let persisted=store.load(&id).unwrap();assert!(!read_codemode_store(&persisted).contains_key("uncommitted"));assert!(persisted.entries.iter().any(|e|matches!(&e.content,pi_coding_session::EntryContent::Custom{custom_type,..}if custom_type=="external")));drop(coding);drop(store);std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn callable_exposure_and_deferred_description_follow_session_registry() {
    session::runtime().block_on(async {
        let coding = coding(vec![], Arc::new(NoopSessionHooks)).await;
        for (name, exposure, active) in [
            ("direct", ToolExposure::Direct, true),
            ("inactive", ToolExposure::Direct, false),
            ("hidden", ToolExposure::Hidden, true),
            ("modelOnly", ToolExposure::ModelOnly, true),
            ("nested", ToolExposure::CodeMode, true),
            ("deferred", ToolExposure::Deferred, true),
        ] {
            let mut registration = RegisteredTool::direct(session::tool(
                name,
                Arc::new(|_| Box::pin(async { Ok(session::text_result("ok")) })),
            ));
            registration.exposure = exposure;
            registration.default_active = active;
            coding.register_tool(registration).unwrap();
        }
        let adapter = adapter(&coding);
        let declaration = adapter.tool().unwrap().tool.declaration.description;
        assert!(declaration.contains("tools.direct("));
        assert!(declaration.contains("tools.nested("));
        assert!(!declaration.contains("tools.deferred("));
        adapter.attach().unwrap();
        let result = invoke(
            &coding,
            "codemode",
            json!({"code":"return ALL_TOOLS.map(t=>t.name).sort();"}),
        )
        .await;
        assert!(!result.is_error);
        assert_eq!(
            text_of(&result.result).lines().last().unwrap(),
            "[\"deferred\",\"direct\",\"nested\"]"
        );
    });
}

#[test]
fn dropping_live_invocation_cancels_nested_tools_and_releases_worker_admission() {
    // Cleanup only protects a failing regression from leaving a worker behind;
    // all assertions below require release before this guard is dropped.
    struct CloseSandbox(pi_codemode::Sandbox);
    impl Drop for CloseSandbox {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    session::runtime().block_on(async {
        let (started,mut admitted)=tokio::sync::mpsc::unbounded_channel();
        let waiting=session::tool("waiting",Arc::new(move|invocation| {
            let started=started.clone();
            Box::pin(async move {
                started.send(invocation.cancellation.clone()).unwrap();
                invocation.cancellation.cancelled().await;
                Ok(session::text_result("cancelled"))
            })
        }));
        let coding=coding(vec![waiting],Arc::new(NoopSessionHooks)).await;
        let worker=std::env::var_os("PI_CODEMODE_TEST_WORKER").map(std::path::PathBuf::from).unwrap_or_else(||std::path::PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").unwrap()).join("debug/pi-codemode-worker"));
        let mut options=pi_codemode::SandboxOptions::new(worker);
        options.max_workers=1;
        let sandbox=pi_codemode::Sandbox::new(options).unwrap();
        let _cleanup=CloseSandbox(sandbox.clone());
        let adapter=CodeModeAdapter::new(coding.downgrade(),sandbox.clone(),Default::default()).unwrap();
        adapter.attach().unwrap();
        let parent_signal=CancellationToken::new();
        let mut call=Box::pin(coding.execute_tool("drop-parent","codemode",serde_json::from_value(json!({"code":"store('uncommitted',true); await tools.waiting({});"})).unwrap(),parent_signal.clone(),None));
        let nested_signal=tokio::select! {
            signal=admitted.recv()=>signal.unwrap(),
            result=&mut call=>panic!("unexpected completion: {result:?}"),
            _=tokio::time::sleep(std::time::Duration::from_secs(2))=>panic!("nested tool was not admitted"),
        };
        assert_eq!(sandbox.active_workers(),1);
        assert!(!nested_signal.is_cancelled());
        drop(call);
        assert!(nested_signal.is_cancelled(),"dropping the invocation must signal admitted nested tools");
        assert!(!parent_signal.is_cancelled(),"the adapter owns its child signal, not the caller's token");
        tokio::time::timeout(std::time::Duration::from_secs(2),async {
            while sandbox.active_workers()!=0 {tokio::time::sleep(std::time::Duration::from_millis(1)).await;}
        }).await.expect("dropped invocation must reap its worker without Sandbox::close");
        assert!(!read_codemode_store(&coding.snapshot()).contains_key("uncommitted"));
        let replacement=sandbox.start("return 1;",pi_codemode::RunOptions::new(0.,1)).expect("worker admission must be reusable");
        let result=tokio::task::spawn_blocking(move||replacement.wait()).await.unwrap();
        assert!(matches!(result,pi_codemode::ExecutionResult::Success{value:Some(value),..}if value==json!(1)));
        assert_eq!(sandbox.active_workers(),0);
    });
}
