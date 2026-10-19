#[path = "../../pi-extension-protocol/tests/durable_support/mod.rs"]
mod protocol_support;
mod runtime_support;
mod support;
use pi_ai::Presence;
use pi_durable::{
    bindings::{
        coding::bind_coding_tool,
        extensions::{DurableExtensionAttachment, ExtensionRegistration, grant_scope},
        mcp::{self, McpBindingOptions, McpToolAttachment},
    },
    *,
};
use pi_extension_protocol::{host::HostConnection, wire::v1_1 as w};
use runtime_support::*;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn declaration(name: &str) -> pi_ai::Tool {
    serde_json::from_value(json!({"name":name,"description":"offline","parameters":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}})).unwrap()
}
async fn connection(names: &[&str], scope: &str) -> (HostConnection, protocol_support::Peer) {
    protocol_support::ready(names, scope).await
}
fn registration(connection: &HostConnection, name: &str) -> w::CapabilityRef {
    connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.reference().name == name)
        .unwrap()
        .reference()
        .clone()
}
async fn settled(root: &Conversation) -> session::SubmissionRecord {
    tokio::time::timeout(Duration::from_secs(5), async {
        root.submit(SubmissionDraft::input("go"))
            .await
            .unwrap()
            .wait()
            .await
            .unwrap()
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_remote_tool_preserves_metadata_channels_and_committed_progress() {
    for (_, storage, _temp) in support::stores() {
        let(d,root,_)=open(storage,&json!({"responses":[{"calls":[{"id":"c","name":"remote","arguments":{"n":1},"thoughtSignature":"sig","namespace":"ns"}]},{"text":"done"}]})).await;
        let (connection, mut peer) = connection(&["tool:remote"], &grant_scope(&d, root.id)).await;
        let mut tool = ToolRegistration::new(declaration("remote"), |_| {
            Box::pin(async { panic!("host placeholder must be replaced") })
        });
        tool.replay = ReplayPolicy::Safe;
        tool.structured_output_schema =
            Some(serde_json::from_value(json!({"type":["object","null"]})).unwrap());
        let attachment = DurableExtensionAttachment::attach(
            &d,
            root.id,
            connection.clone(),
            vec![ExtensionRegistration::Tool {
                capability: registration(&connection, "tool:remote"),
                tool: Box::new(tool),
            }],
        )
        .await
        .unwrap();
        let worker = tokio::spawn(async move {
            let w::envelope::Frame::Request(request) = peer.recv().await else {
                panic!()
            };
            let input = protocol_support::value(&request.payload);
            assert_eq!(input["call"]["thoughtSignature"], "sig");
            assert_eq!(input["call"]["namespace"], "ns");
            peer.progress(
                request.request_id,
                1,
                json!({"type":"output","chunk":{"type":"bytes","base64":"8J8="}}),
            )
            .await;
            assert!(matches!(peer.recv().await, w::envelope::Frame::Credit(_)));
            peer.progress(
                request.request_id,
                2,
                json!({"type":"output","chunk":{"type":"bytes","base64":"mIA="}}),
            )
            .await;
            assert!(matches!(peer.recv().await, w::envelope::Frame::Credit(_)));
            peer.progress(
                request.request_id,
                3,
                json!({"type":"details","value":{"phase":"running"}}),
            )
            .await;
            assert!(matches!(peer.recv().await, w::envelope::Frame::Credit(_)));
            peer.answer(request.request_id,"tool.call",json!({"result":{"details":null,"structuredOutput":null,"usage":pi_ai::Usage::zero(),"diagnostics":[{"severity":"warn","message":"reported"}],"control":{"addTools":[],"terminate":false,"handoff":"next"}}}),true).await;
            peer
        });
        settled(&root).await;
        let _peer = worker.await.unwrap();
        let history = d.session().history(root.id, None).await.unwrap();
        let entry = history.iter().find(|e| e.kind == "pi.tool-result").unwrap();
        let result: pi_ai::ToolResultMessage =
            serde_json::from_value(entry.model.as_ref().unwrap()[0].clone()).unwrap();
        assert!(
            matches!(result.details, Presence::Null),
            "result: {:?}",
            result
        );
        assert_eq!(
            serde_json::to_value(&result.content[0]).unwrap(),
            json!({"type":"text","text":"😀"})
        );
        assert!(
            serde_json::to_string(&result.content)
                .unwrap()
                .contains("reported")
        );
        assert!(result.usage.is_some());
        attachment.close();
        assert!(!connection.current());
        d.close().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seven_hooks_use_actual_runtime_registry_and_native_decisions() {
    let(d,root,_)=open(Arc::new(session::MemoryStorage::new()),&json!({"responses":[{"calls":[{"id":"c","name":"work","arguments":{"n":1}}]},{"text":"done"},{"text":"continued"}]})).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let e = effects.clone();
    let tool = ToolRegistration::new(declaration("work"), move |inv| {
        let e = e.clone();
        Box::pin(async move {
            assert_eq!(
                serde_json::to_value(inv.call.call.arguments).unwrap(),
                json!({"n":2})
            );
            e.fetch_add(1, Ordering::SeqCst);
            Ok(ToolExecutionResult::default())
        })
    });
    d.registry()
        .install(Extension {
            name: "local".into(),
            tools: vec![tool],
            ..Default::default()
        })
        .unwrap();
    let names = pi_durable::bindings::extensions::HOOK_METHODS;
    let (c, mut peer) = connection(&names, &grant_scope(&d, root.id)).await;
    let registrations = names
        .iter()
        .map(|n| ExtensionRegistration::Hook(registration(&c, n)))
        .collect();
    let attachment = DurableExtensionAttachment::attach(&d, root.id, c.clone(), registrations)
        .await
        .unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = seen.clone();
    let worker = tokio::spawn(async move {
        let mut yields = 0;
        loop {
            let frame = peer.recv().await;
            let w::envelope::Frame::Request(r) = frame else {
                break;
            };
            let input = protocol_support::value(&r.payload);
            calls.lock().unwrap().push(r.method.clone());
            let output = match r.method.as_str() {
                "before_request" => json!({"messages":input["messages"]}),
                "before_tool" => json!({"arguments":{"n":2}}),
                "after_tool" => {
                    json!({"result":{"output":[{"type":"text","text":"hook replacement"}]}})
                }
                "on_yield" => {
                    yields += 1;
                    if yields == 1 {
                        json!({"continue":"continue"})
                    } else {
                        json!({})
                    }
                }
                "before_compact" => {
                    json!({"decision":{"type":"summary","summary":"native summary"}})
                }
                _ => json!({}),
            };
            peer.answer(r.request_id, &r.method, output, false).await;
        }
    });
    settled(&root).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let messages = root.context(None).await.unwrap().messages;
    assert!(
        serde_json::to_string(&messages)
            .unwrap()
            .contains("hook replacement")
    );
    let cid = root.id;
    d.commit(move |tx| {
        let mut draft = session::EntryDraft::new("test.large");
        draft.model = Some(vec![
            json!({"role":"user","content":"x".repeat(100000),"timestamp":1000}),
        ]);
        tx.append_entry(cid, draft)?;
        Ok(())
    })
    .await
    .unwrap();
    let compact = root.compact(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), d.wait_for_task(compact))
        .await
        .unwrap()
        .unwrap();
    let observed = seen.lock().unwrap().clone();
    for name in names {
        assert!(
            observed.contains(&name.to_string()),
            "missing {name}: {observed:?}"
        );
    }
    attachment.close();
    worker.abort();
    d.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_coding_tool_uses_injected_environment_and_ordinary_finalizer() {
    for (_, storage, _temp) in support::stores() {
        let(d,root,_)=open(storage,&json!({"responses":[{"calls":[{"id":"read","name":"read","arguments":{"path":"Cargo.toml"}}]},{"text":"done"}]})).await;
        let context = pi_coding_runtime::tools::ExecutionToolContext {
            env: Arc::new(pi_coding_runtime::tools::LocalExecutionEnv::new(
                std::env::current_dir().unwrap(),
            )),
        };
        let tool = bind_coding_tool(pi_coding_runtime::tools::coding_tool("read"), context);
        d.registry()
            .install(Extension {
                name: "coding-operations".into(),
                tools: vec![tool],
                ..Default::default()
            })
            .unwrap();
        settled(&root).await;
        let context = root.context(None).await.unwrap();
        let text = serde_json::to_string(&context.messages).unwrap();
        assert!(text.contains("workspace"));
        assert!(text.contains("toolResult"));
        d.close().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_preserves_full_result_and_host_client_lifecycle() {
    use pi_mcp::transport::{TransportEvent, memory_transport_pair};
    let (transport, mut peer) = memory_transport_pair();
    let task = tokio::spawn(pi_mcp::McpClient::connect(
        pi_mcp::ClientOptions::new("test", "1"),
        Arc::new(transport),
    ));
    let init = peer.incoming.recv().await.unwrap();
    peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","id":init["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"test","version":"1"}}}))).unwrap();
    assert_eq!(
        peer.incoming.recv().await.unwrap()["method"],
        "notifications/initialized"
    );
    let client = task.await.unwrap().unwrap();
    let(d,root,_)=open(Arc::new(session::MemoryStorage::new()),&json!({"responses":[{"calls":[{"id":"mcp","name":"mcp__server__tool","arguments":{}}]},{"text":"done"}]})).await;
    let host = d.clone();
    let owned = client.clone();
    let cid = root.id;
    let attach = tokio::spawn(async move {
        McpToolAttachment::attach(
            &host,
            cid,
            "server",
            owned,
            McpBindingOptions {
                deferred: false,
                ..Default::default()
            },
            pi_ai::CancellationToken::new(),
        )
        .await
    });
    let discovery = peer.incoming.recv().await.unwrap();
    assert_eq!(discovery["method"], "tools/list");
    peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","id":discovery["id"],"result":{"tools":[{"name":"tool","inputSchema":{"type":"object"}}]}}))).unwrap();
    let attachment = attach.await.unwrap().unwrap();
    let worker = tokio::spawn(async move {
        let call = peer.incoming.recv().await.unwrap();
        assert_eq!(call["method"], "tools/call");
        peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","id":call["id"],"result":{"content":[{"type":"image","data":"aGVsbG8=","mimeType":"image/png"}],"structuredContent":null,"_meta":{"secret":"omit"},"custom":{"keep":true}}}))).unwrap();
        peer
    });
    settled(&root).await;
    let _peer = worker.await.unwrap();
    let result = root.context(None).await.unwrap();
    assert!(
        serde_json::to_string(&result.messages)
            .unwrap()
            .contains("image/png")
    );
    attachment.close();
    assert!(client.is_connected());
    client.close().await.unwrap();
    d.close().await.unwrap();
    let mapped=mcp::convert_result("s","t",serde_json::from_value(json!({"content":[],"structuredContent":null,"custom":{"keep":true},"_meta":{"secret":1}})).unwrap(),&McpBindingOptions::default()).await.unwrap();
    let wire = serde_json::to_value(mapped).unwrap();
    assert!(
        wire["structuredOutput"]
            .get("structuredContent")
            .unwrap()
            .is_null()
    );
    assert_eq!(wire["structuredOutput"]["custom"]["keep"], true);
    assert!(wire["structuredOutput"].get("_meta").is_none());
}

mod bindings_storage;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_credit_follows_actual_memory_and_sqlite_progress_settlement() {
    for fail in [false, true] {
        for (_, storage, _temp) in support::stores() {
            let store = Arc::new(bindings_storage::GateStorage::new(storage.clone()));
            let(d,root,_)=open(store.clone(),&json!({"responses":[{"calls":[{"id":"c","name":"remote","arguments":{}}]},{"text":"done"}]})).await;
            let (c, mut peer) = connection(&["tool:remote"], &grant_scope(&d, root.id)).await;
            let mut tool = ToolRegistration::new(declaration("remote"), |_| {
                Box::pin(async { unreachable!() })
            });
            tool.replay = ReplayPolicy::Safe;
            let _attachment = DurableExtensionAttachment::attach(
                &d,
                root.id,
                c.clone(),
                vec![ExtensionRegistration::Tool {
                    capability: registration(&c, "tool:remote"),
                    tool: Box::new(tool),
                }],
            )
            .await
            .unwrap();
            let submitted = root.submit(SubmissionDraft::input("go")).await.unwrap();
            let w::envelope::Frame::Request(request) = peer.recv().await else {
                panic!()
            };
            store.arm(fail);
            peer.progress(
                request.request_id,
                1,
                json!({"type":"details","value":{"commit":"gated"}}),
            )
            .await;
            tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(30), peer.recv())
                    .await
                    .is_err(),
                "credit before backend settlement"
            );
            store.release();
            if fail {
                let result = tokio::time::timeout(Duration::from_secs(3), submitted.wait())
                    .await
                    .unwrap();
                assert!(result.is_err());
                assert!(d.session().failure().is_some());
                assert!(!store.writes.lock().unwrap().iter().any(|w|matches!(w,session::StorageWrite::Entry{value} if value.kind=="pi.tool-result")),"ambiguous backend failure became successful tool data");
            } else {
                assert!(matches!(peer.recv().await, w::envelope::Frame::Credit(_)));
                let partial = d
                    .session()
                    .snapshot(
                        live_doc(),
                        live_doc()
                            .address(
                                session::Scope::Conversation {
                                    conversation_id: root.id,
                                },
                                None,
                            )
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                assert!(partial.to_string().contains("gated"));
                peer.answer(
                    request.request_id,
                    "tool.call",
                    json!({"result":{"output":[]}}),
                    true,
                )
                .await;
                tokio::time::timeout(Duration::from_secs(3), submitted.wait())
                    .await
                    .unwrap()
                    .unwrap();
                d.close().await.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attachment_rejects_wrong_grants_runtime_conversation_and_retained_callbacks() {
    let(d,root,_)=open(Arc::new(session::MemoryStorage::new()),&json!({"responses":[{"calls":[{"id":"c","name":"remote","arguments":{}}]},{"text":"done"},{"calls":[{"id":"c2","name":"remote","arguments":{}}]},{"text":"done"}]})).await;
    let (c, mut peer) = connection(&["tool:remote"], "foreign-scope").await;
    let mut tool = ToolRegistration::new(declaration("remote"), |_| {
        Box::pin(async { unreachable!() })
    });
    tool.replay = ReplayPolicy::Safe;
    assert!(
        DurableExtensionAttachment::attach(
            &d,
            root.id,
            c.clone(),
            vec![ExtensionRegistration::Tool {
                capability: registration(&c, "tool:remote"),
                tool: Box::new(tool.clone())
            }]
        )
        .await
        .is_err()
    );
    c.close();
    drop(peer);
    let (c, p) = connection(&["tool:remote"], &grant_scope(&d, root.id)).await;
    peer = p;
    let attachment = DurableExtensionAttachment::attach(
        &d,
        root.id,
        c.clone(),
        vec![ExtensionRegistration::Tool {
            capability: registration(&c, "tool:remote"),
            tool: Box::new(tool),
        }],
    )
    .await
    .unwrap();
    let retained = d
        .registry()
        .snapshot()
        .extension(attachment.name())
        .unwrap()
        .tools[0]
        .clone();
    let(other,other_root,_)=open(Arc::new(session::MemoryStorage::new()),&json!({"responses":[{"calls":[{"id":"c","name":"remote","arguments":{}}]},{"text":"done"}]})).await;
    assert_eq!(root.id, other_root.id);
    other
        .registry()
        .install(Extension {
            name: "foreign-copy".into(),
            tools: vec![retained.clone()],
            ..Default::default()
        })
        .unwrap();
    settled(&other_root).await;
    assert!(
        serde_json::to_string(&other_root.context(None).await.unwrap().messages)
            .unwrap()
            .contains("foreign Durable tool invocation")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv())
            .await
            .is_err()
    );
    let fork = d
        .create_conversation(ConversationCreateOptions {
            ownership: session::ConversationOwnership::Ownerless,
            options: ConversationOptions {
                agent: Some(AgentChange {
                    model: Presence::Value(ModelRef {
                        provider: "oracle".into(),
                        model_id: "scripted-1".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    settled(&fork).await;
    assert!(
        serde_json::to_string(&fork.context(None).await.unwrap().messages)
            .unwrap()
            .contains("foreign Durable tool invocation")
    );
    attachment.close();
    d.registry()
        .install(Extension {
            name: "retained-copy".into(),
            tools: vec![retained],
            ..Default::default()
        })
        .unwrap();
    settled(&root).await;
    assert!(
        serde_json::to_string(&root.context(None).await.unwrap().messages)
            .unwrap()
            .contains("stale")
    );
    assert!(!c.current());
    other.close().await.unwrap();
    d.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_nested_progress_has_no_frames_and_complete_terminal_channels() {
    let(d,root,_)=open(Arc::new(session::MemoryStorage::new()),&json!({"responses":[{"calls":[{"id":"p","name":"parent","arguments":{}}]},{"text":"done"}]})).await;
    let (c, mut peer) = connection(&["tool:remote"], &grant_scope(&d, root.id)).await;
    let mut remote = ToolRegistration::new(declaration("remote"), |_| {
        Box::pin(async { unreachable!() })
    });
    remote.replay = ReplayPolicy::Safe;
    remote.callers = Some(vec![ToolCaller::Tools]);
    remote.structured_output_schema = Some(serde_json::from_value(json!({"type":"null"})).unwrap());
    let _attachment = DurableExtensionAttachment::attach(
        &d,
        root.id,
        c.clone(),
        vec![ExtensionRegistration::Tool {
            capability: registration(&c, "tool:remote"),
            tool: Box::new(remote),
        }],
    )
    .await
    .unwrap();
    let parent = ToolRegistration::new(declaration("parent"), |invocation| {
        Box::pin(async move {
            let result = invocation
                .api
                .execute_tool(
                    "remote".into(),
                    serde_json::from_value(json!({})).unwrap(),
                    NestedToolOptions {
                        key: Some("nested".into()),
                        progress: Some(false),
                    },
                )
                .await?;
            assert!(matches!(result.details, Presence::Null));
            assert!(matches!(result.structured_output, Presence::Null));
            assert!(result.usage.is_some());
            Ok(ToolExecutionResult {
                output: Some(vec![pi_ai::InputContent::Text(pi_ai::TextContent {
                    content_type: Default::default(),
                    text: "nested complete".into(),
                    text_signature: None,
                })]),
                ..Default::default()
            })
        })
    });
    d.registry()
        .install(Extension {
            name: "parent".into(),
            tools: vec![parent],
            ..Default::default()
        })
        .unwrap();
    let worker = tokio::spawn(async move {
        let w::envelope::Frame::Request(request) = peer.recv().await else {
            panic!()
        };
        let input = protocol_support::value(&request.payload);
        assert_eq!(input["progress"]["enabled"], false);
        assert_eq!(input["call"]["parent"]["callId"], "p");
        peer.answer(request.request_id,"tool.call",json!({"result":{"output":[{"type":"text","text":"full terminal"}],"details":null,"structuredOutput":null,"usage":pi_ai::Usage::zero(),"diagnostics":[{"severity":"info","message":"terminal diagnostic"}],"control":{"terminate":true}}}),true).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), peer.recv())
                .await
                .is_err()
        );
        peer
    });
    settled(&root).await;
    let _peer = worker.await.unwrap();
    assert!(
        serde_json::to_string(&root.context(None).await.unwrap().messages)
            .unwrap()
            .contains("nested complete")
    );
    d.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retirement_joins_admitted_backend_progress_and_skips_buffered_updates() {
    for (_, storage, _temp) in support::stores() {
        let store = Arc::new(bindings_storage::GateStorage::new(storage));
        let(d,root,_)=open(store.clone(),&json!({"responses":[{"calls":[{"id":"c","name":"remote","arguments":{}}]},{"text":"done"}]})).await;
        let (c, mut peer) = connection(&["tool:remote"], &grant_scope(&d, root.id)).await;
        let mut tool = ToolRegistration::new(declaration("remote"), |_| {
            Box::pin(async { unreachable!() })
        });
        tool.replay = ReplayPolicy::Safe;
        let attachment = DurableExtensionAttachment::attach(
            &d,
            root.id,
            c.clone(),
            vec![ExtensionRegistration::Tool {
                capability: registration(&c, "tool:remote"),
                tool: Box::new(tool),
            }],
        )
        .await
        .unwrap();
        let submitted = root.submit(SubmissionDraft::input("go")).await.unwrap();
        let w::envelope::Frame::Request(request) = peer.recv().await else {
            panic!()
        };
        store.arm(false);
        peer.progress(
            request.request_id,
            1,
            json!({"type":"details","value":{"ordinal":1}}),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
            .await
            .unwrap();
        peer.progress(
            request.request_id,
            2,
            json!({"type":"details","value":{"ordinal":2}}),
        )
        .await;
        attachment.close();
        assert!(!c.current());
        // This call is already admitted to Storage. Retirement cannot roll it
        // back or admit the second callback while the first one is settling.
        store.release();
        tokio::time::timeout(Duration::from_secs(3), submitted.wait())
            .await
            .unwrap()
            .unwrap();
        let writes = serde_json::to_string(&*store.writes.lock().unwrap()).unwrap();
        assert!(writes.contains("ordinal"));
        assert!(!writes.contains("\"ordinal\":2"));
        d.close().await.unwrap();
    }
}
