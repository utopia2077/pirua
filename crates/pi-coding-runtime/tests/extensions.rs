mod session_support;
#[path = "../../pi-extension-protocol/tests/current_support/mod.rs"]
mod wire_support;
use pi_coding_runtime::{extensions::*, *};
use pi_coding_session::{EntryContent, MemoryStore, SessionManager};
use pi_extension_protocol::{host, jcs, wire::v1_1 as w};
use serde_json::{Value, json};
use session_support::*;
use std::sync::{Arc, Mutex};

fn schema() -> jcs::PortableSchema {
    jcs::admit_schema(&serde_json::to_vec(&json!({"type":"object","properties":{"content":{"type":"array","items":{"type":"object","properties":{"type":{"const":"text"},"text":{"type":"string"}},"required":["type","text"],"additionalProperties":false}}},"required":["content"],"additionalProperties":false})).unwrap(),jcs::Limits::default()).unwrap()
}
fn registrations(connection: &host::HostConnection) -> Vec<ExtensionRegistration> {
    connection.grant().profiles().iter().map(|g|{let c=g.reference().clone();match c.name.as_str(){"remote"=>ExtensionRegistration::Tool{capability:c,declaration:serde_json::from_value(json!({"name":"remote","description":"host-approved remote","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}})).unwrap(),progress:Arc::new(schema()),result:Arc::new(schema())},"event"=>ExtensionRegistration::Event(c),"session.snapshot"=>ExtensionRegistration::Snapshot(c),_=>ExtensionRegistration::Hook(c)}}).collect()
}
#[test]
fn framed_hooks_tool_progress_and_boundary_share_canonical_session_pipeline() {
    runtime().block_on(async{
 let hooks=Arc::new(CodingExtensions::new());let (stream,requests)=scripted(vec![json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"t1","name":"remote","arguments":{"path":"raw"}}]}),json!({"content":[{"type":"text","text":"done"}]})]);
 let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_hooks(hooks.clone()).with_system_prompt("fixture").build().await.unwrap();
 let names=["input","message_end","before_tool","after_tool","turn_end","event","remote","session.snapshot"];
 let(c,mut peer)=wire_support::ready(&names,&session.session_id()).await;let registered=registrations(&c);let attachment=hooks.attach(c.clone(),&session,registered).unwrap();
 let observed=Arc::new(Mutex::new(Vec::<(String,Value)>::new()));let captured=observed.clone();
 let peer_task=tokio::spawn(async move{loop{match peer.recv().await{
  w::envelope::Frame::Request(r)=>{let input=wire_support::value(&r.payload);captured.lock().unwrap().push((r.method.clone(),input.clone()));match r.method.as_str(){
   "input"=>peer.answer(r.request_id,"input",json!({"action":"transform","text":"changed input"}),false).await,
   "message_end"=>{let mut message=input["message"].clone();if message["role"]=="assistant" && message["content"][0]["type"]=="toolCall"{message["content"][0]["arguments"]["path"]=json!("message-patched");}peer.answer(r.request_id,"message_end",json!({"message":message}),false).await;},
   "before_tool"=>{assert_eq!(input["args"]["path"],"message-patched");peer.answer(r.request_id,"before_tool",json!({"block":false,"terminate":false,"args":{"path":"hook-patched"}}),false).await;},
   "tool.call"=>{assert_eq!(input["arguments"]["path"],"hook-patched");peer.progress(r.request_id,1,json!({"result":{"content":[{"type":"text","text":"partial"}]}})).await;peer.answer(r.request_id,"tool.call",json!({"result":{"content":[{"type":"text","text":"remote result"}]}}),true).await;},
   "after_tool"=>{assert_eq!(input["result"]["content"][0]["text"],"remote result");peer.answer(r.request_id,"after_tool",json!({"content":[{"type":"text","text":"after-hook result"}]}),false).await;},
   "turn_end"=>{peer.answer(r.request_id,"turn_end",json!({"value":{"entries":[{"type":"custom","customType":"extension-checkpoint","data":{"source":"wire"}}],"continueRun":false}}),false).await;},
   "event"=>peer.answer(r.request_id,"event",json!({}),false).await,
   other=>panic!("unexpected {other}")
  }},
  w::envelope::Frame::Cancel(_)|w::envelope::Frame::Credit(_)=>{},
  _=>break,
 }}});
 session.set_active_tools(vec!["remote".into()]).unwrap();session.prompt("original",PromptOptions::default()).await.unwrap();session.wait_for_idle().await.unwrap();
 assert!(attachment.current());let projection=session.projection();assert!(projection.messages.iter().any(|v|v["role"]=="user"&&v["content"][0]["text"]=="changed input"));assert!(projection.messages.iter().any(|v|v["role"]=="assistant"&&v["content"][0]["arguments"]["path"]=="message-patched"));assert!(projection.messages.iter().any(|v|v["role"]=="toolResult"&&v["content"][0]["text"]=="after-hook result"));
 assert!(session.snapshot().entries.iter().any(|e|matches!(&e.content,EntryContent::Custom{custom_type,..} if custom_type=="extension-checkpoint")));
 let second=serde_json::to_value(requests.lock().unwrap()[1].context.messages()).unwrap();assert!(second.as_array().unwrap().iter().any(|v|v["role"]=="toolResult"&&v["content"][0]["text"]=="after-hook result"));
 assert!(observed.lock().unwrap().iter().any(|(m,v)|m=="event"&&v["value"]["type"]=="agent"&&v["value"]["data"]["type"]=="tool_execution_update"));
 attachment.close();assert!(!attachment.current());assert!(!session.callable_tool_names().iter().any(|n|n=="remote"));peer_task.abort();
})
}
#[test]
fn native_role_rejection_cannot_enter_session() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream, _) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_hooks(hooks.clone())
        .with_system_prompt("fixture")
        .build()
        .await
        .unwrap();
        let (c, mut peer) = wire_support::ready(&["message_end"], &session.session_id()).await;
        let attachment = hooks
            .attach(c.clone(), &session, registrations(&c))
            .unwrap();
        let peer_task = tokio::spawn(async move {
            loop {
                let w::envelope::Frame::Request(r) = peer.recv().await else {
                    panic!()
                };
                let input = wire_support::value(&r.payload);
                if input["message"]["role"] == "assistant" {
                    peer.answer(
                        r.request_id,
                        "message_end",
                        json!({"message":{"role":"user","content":"foreign role","timestamp":7}}),
                        false,
                    )
                    .await;
                    break;
                }
                peer.answer(r.request_id, "message_end", input, false).await;
            }
        });
        session
            .prompt("original", PromptOptions::default())
            .await
            .unwrap();
        assert!(session.projection().messages.iter().any(|v| {
            v["role"] == "assistant"
                && v["stopReason"] == "error"
                && v["errorMessage"]
                    .as_str()
                    .is_some_and(|s| s.contains("NativeRejected"))
        }));
        peer_task.await.unwrap();
        assert!(!attachment.current());
        assert!(
            !session
                .projection()
                .messages
                .iter()
                .any(|v| v["content"] == "foreign role")
        );
    })
}
#[test]
fn attachment_close_and_session_replacement_retire_captured_tools() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_hooks(hooks.clone())
        .build()
        .await
        .unwrap();
        let (c, _peer) = wire_support::ready(&["remote"], &session.session_id()).await;
        let attachment = hooks
            .attach(c.clone(), &session, registrations(&c))
            .unwrap();
        let captured = session
            .callable_tools()
            .into_iter()
            .find(|t| t.declaration.name.as_str() == "remote")
            .unwrap();
        session
            .new_session(Arc::new(MemoryStore::new()), "/tmp")
            .await
            .unwrap();
        assert!(!attachment.current());
        let result = (captured.execute)(pi_agent::ToolInvocation {
            call_id: "late".into(),
            arguments: Default::default(),
            cancellation: pi_agent::CancellationToken::new(),
            on_update: Arc::new(|_| {}),
        })
        .await;
        assert!(result.is_err());
        assert!(!session.callable_tool_names().iter().any(|n| n == "remote"));
    })
}
#[test]
fn snapshot_action_requires_host_permission_and_exact_session_scope() {
    runtime().block_on(async{
 let hooks=Arc::new(CodingExtensions::new());let(stream,_)=scripted(vec![]);let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_hooks(hooks.clone()).build().await.unwrap();let(c,mut peer)=wire_support::ready(&["session.snapshot"],&session.session_id()).await;let attachment=hooks.attach(c.clone(),&session,registrations(&c)).unwrap();let cap=c.grant().profiles()[0].reference().clone();let payload=host::bound(&pi_extension_protocol::profiles::Registry::coding().unwrap(),"coding.input:session.snapshot",&json!({})).unwrap();
 use prost::Message;
 peer.send(w::envelope::Frame::Request(w::Request{request_id:2,parent_request_id:None,capability_ref:Some(cap.clone()),effect_key:None,method:"session.snapshot".into(),deadline_after_ms:Some(100),initial_stream_credit_bytes:None,payload:payload.encode_to_vec()})).await;
 let w::envelope::Frame::Response(r)=peer.recv().await else{panic!()};let Some(w::response::Terminal::Result(bytes))=r.terminal else{panic!("snapshot denied")};assert_eq!(wire_support::value(&bytes)["value"]["header"]["id"],session.session_id());
 let mut forged=cap;forged.authenticated_extension_id="claims-admin".into();peer.send(w::envelope::Frame::Request(w::Request{request_id:4,parent_request_id:None,capability_ref:Some(forged),effect_key:None,method:"session.snapshot".into(),deadline_after_ms:Some(100),initial_stream_credit_bytes:None,payload:payload.encode_to_vec()})).await;
 let w::envelope::Frame::Response(r)=peer.recv().await else{panic!()};assert!(matches!(r.terminal,Some(w::response::Terminal::Error(e))if e.code==w::ErrorCode::Forbidden as i32));attachment.close();
 let(c,_)=wire_support::ready(&["session.snapshot"],"different-session").await;assert!(hooks.attach(c.clone(),&session,registrations(&c)).is_err());c.close();
})
}

#[test]
fn ordered_attachments_transform_input_in_registration_order() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream, _) = scripted(vec![json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_hooks(hooks.clone())
        .with_system_prompt("fixture")
        .build()
        .await
        .unwrap();
        let (c1, mut p1) = wire_support::ready(&["input"], &session.session_id()).await;
        let a1 = hooks
            .attach(c1.clone(), &session, registrations(&c1))
            .unwrap();
        let (c2, mut p2) = wire_support::ready(&["input"], &session.session_id()).await;
        let a2 = hooks
            .attach(c2.clone(), &session, registrations(&c2))
            .unwrap();
        let t1 = tokio::spawn(async move {
            let w::envelope::Frame::Request(r) = p1.recv().await else {
                panic!()
            };
            assert_eq!(wire_support::value(&r.payload)["text"], "original");
            p1.answer(
                r.request_id,
                "input",
                json!({"action":"transform","text":"first"}),
                false,
            )
            .await;
            std::future::pending::<()>().await;
        });
        let t2 = tokio::spawn(async move {
            let w::envelope::Frame::Request(r) = p2.recv().await else {
                panic!()
            };
            assert_eq!(wire_support::value(&r.payload)["text"], "first");
            p2.answer(
                r.request_id,
                "input",
                json!({"action":"transform","text":"second"}),
                false,
            )
            .await;
            std::future::pending::<()>().await;
        });
        session
            .prompt("original", PromptOptions::default())
            .await
            .unwrap();
        assert!(
            session
                .projection()
                .messages
                .iter()
                .any(|v| v["role"] == "user" && v["content"][0]["text"] == "second")
        );
        a1.close();
        a2.close();
        t1.abort();
        t2.abort();
    })
}
#[test]
fn missing_grant_and_conflicting_tool_batch_leave_existing_attachment_intact() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_hooks(hooks.clone())
        .build()
        .await
        .unwrap();
        let (c1, _p1) = wire_support::ready(&["remote"], &session.session_id()).await;
        let a1 = hooks
            .attach(c1.clone(), &session, registrations(&c1))
            .unwrap();
        let (c2, _p2) = wire_support::ready(&["input", "remote"], &session.session_id()).await;
        let mut forged = c2
            .grant()
            .profiles()
            .iter()
            .find(|p| p.reference().name == "input")
            .unwrap()
            .reference()
            .clone();
        forged.handler_id = Some("forged".into());
        assert!(
            hooks
                .attach(
                    c2.clone(),
                    &session,
                    vec![ExtensionRegistration::Hook(forged)]
                )
                .is_err()
        );
        assert!(c2.current());
        assert!(
            hooks
                .attach(c2.clone(), &session, registrations(&c2))
                .is_err()
        );
        assert!(!c2.current());
        assert!(a1.current());
        assert_eq!(session.callable_tool_names(), vec!["remote"]);
        a1.close();
    })
}
#[test]
fn invalid_remote_boundary_batch_rejects_before_any_entry_is_committed() {
    runtime().block_on(async{
 let hooks=Arc::new(CodingExtensions::new());let(stream,_)=scripted(vec![json!({})]);let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_hooks(hooks.clone()).with_system_prompt("fixture").build().await.unwrap();let(c,mut peer)=wire_support::ready(&["turn_end"],&session.session_id()).await;let attachment=hooks.attach(c.clone(),&session,registrations(&c)).unwrap();
 let task=tokio::spawn(async move{let w::envelope::Frame::Request(r)=peer.recv().await else{panic!()};peer.answer(r.request_id,"turn_end",json!({"value":{"entries":[{"type":"custom","customType":"must-not-commit"},{"type":"context_edit","targetId":"foreign-session-entry","replacement":null}],"continueRun":false}}),false).await;});
 session.prompt("original",PromptOptions::default()).await.unwrap();assert!(session.projection().messages.iter().any(|v|v["role"]=="assistant"&&v["stopReason"]=="error"&&v["errorMessage"].as_str().is_some_and(|s|s.contains("NativeRejected"))));task.await.unwrap();assert!(!attachment.current());assert!(!session.snapshot().entries.iter().any(|e|matches!(&e.content,EntryContent::Custom{custom_type,..}if custom_type=="must-not-commit")));
})
}

#[test]
fn framed_remote_result_preserves_present_null_application_values() {
    runtime().block_on(async{
 use pi_agent::{AgentToolResult,ToolInvocation};
 let hooks=Arc::new(CodingExtensions::new());let(stream,_)=scripted(vec![]);let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_hooks(hooks.clone()).build().await.unwrap();
 let(c,mut peer)=wire_support::ready(&["remote"],&session.session_id()).await;let mut registrations=registrations(&c);let nullable=jcs::admit_schema(&serde_json::to_vec(&json!({"type":"object","properties":{"content":{"type":"array","items":{"type":"object","properties":{"type":{"const":"text"},"text":{"type":"string"}},"required":["type","text"],"additionalProperties":false}},"details":{"type":"null"},"structuredContent":{"type":"null"}},"required":["content","details","structuredContent"],"additionalProperties":false})).unwrap(),jcs::Limits::default()).unwrap();
 if let ExtensionRegistration::Tool{progress,result,..}=&mut registrations[0]{*progress=Arc::new(nullable.clone());*result=Arc::new(nullable);}
 let a=hooks.attach(c.clone(),&session,registrations).unwrap();let captured=session.callable_tools().into_iter().find(|t|t.declaration.name.as_str()=="remote").unwrap();let updates=Arc::new(Mutex::new(Vec::<AgentToolResult>::new()));let output_updates=updates.clone();
 let peer_task=tokio::spawn(async move{let w::envelope::Frame::Request(r)=peer.recv().await else{panic!()};let value=json!({"result":{"content":[],"details":null,"structuredContent":null}});peer.progress(r.request_id,1,value.clone()).await;peer.answer(r.request_id,"tool.call",value,true).await;std::future::pending::<()>().await;});
 let output=(captured.execute)(ToolInvocation{call_id:"null-result".into(),arguments:serde_json::from_value(json!({"path":"fixture"})).unwrap(),cancellation:pi_agent::CancellationToken::new(),on_update:Arc::new(move|r|{output_updates.lock().unwrap().push(r)})}).await.unwrap();
 assert_eq!(output.structured_content,Some(pi_ai::JsonValue::Null));assert_eq!(output.details,Some(pi_ai::JsonValue::Null));assert_eq!(updates.lock().unwrap()[0].structured_content,Some(pi_ai::JsonValue::Null));a.close();peer_task.abort();
})
}

#[test]
fn ordered_after_hook_patches_use_nullish_fallback_and_invalidate_stale_structure() {
    runtime().block_on(async{
 use pi_agent::{AfterToolCallContext,BeforeToolCallContext,AgentContext,AgentToolResult,AgentError};
 let hooks=Arc::new(CodingExtensions::new());let(stream,_)=scripted(vec![]);let session=CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()),"/tmp").unwrap(),model(),stream).with_tools(vec![]).with_hooks(hooks.clone()).build().await.unwrap();
 let(c1,mut p1)=wire_support::ready(&["after_tool"],&session.session_id()).await;let a1=hooks.attach(c1.clone(),&session,registrations(&c1)).unwrap();let(c2,mut p2)=wire_support::ready(&["after_tool"],&session.session_id()).await;let a2=hooks.attach(c2.clone(),&session,registrations(&c2)).unwrap();
 let t1=tokio::spawn(async move{let w::envelope::Frame::Request(r)=p1.recv().await else{panic!()};assert_eq!(wire_support::value(&r.payload)["result"]["structuredContent"],json!({"stale":true}));p1.answer(r.request_id,"after_tool",json!({"content":[{"type":"text","text":"changed"}],"structuredContent":null,"details":null}),false).await;std::future::pending::<()>().await;});
 let t2=tokio::spawn(async move{let w::envelope::Frame::Request(r)=p2.recv().await else{panic!()};let input=wire_support::value(&r.payload);assert!(input["result"].get("structuredContent").is_none());assert_eq!(input["result"]["details"],json!({"keep":1}));assert_eq!(input["result"]["content"][0]["text"],"changed");p2.answer(r.request_id,"after_tool",json!({"structuredContent":null,"details":null}),false).await;std::future::pending::<()>().await;});
 let context=AfterToolCallContext{before:BeforeToolCallContext{assistant_message:pi_agent::failure_message(&model(),&AgentError::Callback("fixture".into()),false),tool_call:serde_json::from_value(json!({"type":"toolCall","id":"tool","name":"echo","arguments":{}})).unwrap(),args:Default::default(),context:AgentContext::default()},result:AgentToolResult{content:vec![],structured_content:Some(serde_json::from_value(json!({"stale":true})).unwrap()),details:Some(serde_json::from_value(json!({"keep":1})).unwrap()),..Default::default()},is_error:false};
 let patch=hooks.after_tool(context,None,pi_agent::CancellationToken::new()).await.unwrap().unwrap();assert_eq!(serde_json::to_value(patch.content).unwrap()[0]["text"],"changed");assert_eq!(patch.structured_content,None);assert_eq!(patch.details,None);a1.close();a2.close();t1.abort();t2.abort();
})
}

#[test]
fn extension_hooks_reject_second_session_instance_before_callback_dispatch() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream_a, _) = scripted(vec![json!({})]);
        let session_a = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp/a").unwrap(),
            model(), stream_a,
        ).with_tools(vec![]).with_hooks(hooks.clone()).build().await.unwrap();
        let (connection, mut peer) = wire_support::ready(&["input"], &session_a.session_id()).await;
        let attachment = hooks.attach(connection.clone(), &session_a, registrations(&connection)).unwrap();
        let (stream_b, _) = scripted(vec![json!({})]);
        let second = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp/b").unwrap(),
            model(), stream_b,
        ).with_tools(vec![]).with_hooks(hooks.clone()).build().await;
        assert!(second.is_err(), "one scoped extension hook object must reject a different invoking session");
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), peer.recv()).await.is_err(), "second construction must not publish a callback to A's peer");
        let peer_task = tokio::spawn(async move {
            let w::envelope::Frame::Request(request) = peer.recv().await else { panic!() };
            assert_eq!(wire_support::value(&request.payload)["text"], "session A input");
            peer.answer(request.request_id, "input", json!({"action":"transform","text":"authorized A transform"}), false).await;
            std::future::pending::<()>().await;
        });
        session_a.prompt("session A input", PromptOptions::default()).await.unwrap();
        assert!(session_a.projection().messages.iter().any(|m|m["role"]=="user" && m["content"][0]["text"]=="authorized A transform"));
        assert!(attachment.current()); attachment.close(); peer_task.abort();
    })
}

#[test]
fn persisted_id_does_not_substitute_for_extension_session_instance_custody() {
    runtime().block_on(async {
        let store = Arc::new(MemoryStore::new());
        let manager = SessionManager::create(store.clone(), "/tmp/a").unwrap();
        let id = manager.header().id.clone();
        let hooks = Arc::new(CodingExtensions::new());
        let (stream_a, _) = scripted(vec![]);
        let session_a = CodingSessionBuilder::new(manager, model(), stream_a)
            .with_tools(vec![])
            .with_hooks(hooks.clone())
            .build()
            .await
            .unwrap();
        let (stream_b, _) = scripted(vec![]);
        let shared_id_manager = SessionManager::open(store.clone(), &id).unwrap();
        assert_eq!(shared_id_manager.header().id, session_a.session_id());
        let second = CodingSessionBuilder::new(shared_id_manager, model(), stream_b)
            .with_tools(vec![])
            .with_hooks(hooks.clone())
            .build()
            .await;
        assert!(
            second.is_err(),
            "a second live manager of the same persisted session is a different runtime instance"
        );
        let (stream_other, _) = scripted(vec![]);
        let other = CodingSessionBuilder::new(
            SessionManager::open(store, &id).unwrap(),
            model(),
            stream_other,
        )
        .with_tools(vec![])
        .build()
        .await
        .unwrap();
        let (connection, _peer) = wire_support::ready(&["input"], &id).await;
        assert!(
            hooks
                .attach(connection.clone(), &other, registrations(&connection))
                .is_err(),
            "attach must require the exact bound instance too"
        );
        assert!(connection.current());
        connection.close();
    })
}

#[test]
fn last_public_attachment_drop_retires_in_flight_tool_and_late_updates() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(), model(), stream).with_tools(vec![]).with_hooks(hooks.clone()).build().await.unwrap();
        let (connection, mut peer) = wire_support::ready(&["remote"], &session.session_id()).await;
        let attachment = hooks.attach(connection.clone(), &session, registrations(&connection)).unwrap();
        let last_public = attachment.clone();
        let tool = session.callable_tools().into_iter().find(|t|t.declaration.name.as_str()=="remote").unwrap();
        let updates = Arc::new(Mutex::new(Vec::new())); let capture = updates.clone();
        let task = tokio::spawn(async move { (tool.execute)(pi_agent::ToolInvocation {
            call_id:"in-flight-drop".into(), arguments:serde_json::from_value(json!({"path":"fixture"})).unwrap(), cancellation:pi_agent::CancellationToken::new(), on_update:Arc::new(move|value|capture.lock().unwrap().push(value)),
        }).await });
        let w::envelope::Frame::Request(request) = peer.recv().await else { panic!() };
        drop(attachment); assert!(connection.current()); assert!(session.callable_tool_names().iter().any(|n|n=="remote"));
        drop(last_public); assert!(!connection.current(), "last public drop must immediately close the connection despite internal invocation state");
        assert!(!session.callable_tool_names().iter().any(|n|n=="remote"), "last public drop must immediately remove its own tools");
        peer.progress(request.request_id, 1, json!({"result":{"content":[{"type":"text","text":"late progress"}]}})).await;
        peer.answer(request.request_id, "tool.call", json!({"result":{"content":[]}}), true).await;
        assert!(task.await.unwrap().is_err()); assert!(updates.lock().unwrap().is_empty());
    })
}

#[test]
fn last_public_attachment_drop_retires_in_flight_input_transform() {
    runtime().block_on(async {
        let hooks = Arc::new(CodingExtensions::new());
        let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_tools(vec![])
        .with_hooks(hooks.clone())
        .build()
        .await
        .unwrap();
        let (connection, mut peer) = wire_support::ready(&["input"], &session.session_id()).await;
        let attachment = hooks
            .attach(connection.clone(), &session, registrations(&connection))
            .unwrap();
        let task_session = session.clone();
        let task = tokio::spawn(async move {
            task_session
                .prompt("original input", PromptOptions::default())
                .await
        });
        let w::envelope::Frame::Request(request) = peer.recv().await else {
            panic!()
        };
        drop(attachment);
        assert!(!connection.current());
        peer.answer(
            request.request_id,
            "input",
            json!({"action":"transform","text":"late replacement"}),
            false,
        )
        .await;
        assert!(task.await.unwrap().is_err());
        assert!(
            !session
                .projection()
                .messages
                .iter()
                .any(|m| m["role"] == "user")
        );
    })
}

#[test]
fn accepted_terminal_survives_public_attachment_retirement() {
    runtime().block_on(async {
        use prost::Message;
        let hooks = Arc::new(CodingExtensions::new()); let (stream, _) = scripted(vec![]);
        let session = CodingSessionBuilder::new(SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(), model(), stream).with_tools(vec![]).with_hooks(hooks.clone()).build().await.unwrap();
        let (connection, mut peer) = wire_support::ready(&["remote"], &session.session_id()).await;
        let attachment = hooks.attach(connection.clone(), &session, registrations(&connection)).unwrap();
        let tool = session.callable_tools().into_iter().find(|t|t.declaration.name.as_str()=="remote").unwrap();
        let mut invocation = (tool.execute)(pi_agent::ToolInvocation {call_id:"accepted-before-drop".into(),arguments:serde_json::from_value(json!({"path":"fixture"})).unwrap(),cancellation:pi_agent::CancellationToken::new(),on_update:Arc::new(|_|{})});
        std::future::poll_fn(|cx| {assert!(invocation.as_mut().poll(cx).is_pending()); std::task::Poll::Ready(())}).await;
        let w::envelope::Frame::Request(request) = peer.recv().await else {panic!()};
        peer.answer(request.request_id, "tool.call", json!({"result":{"content":[{"type":"text","text":"accepted before close"}]}}), true).await;
        // A subsequent peer request/response is an actual ordered-frame barrier:
        // the actor has validated and retained the previous terminal, while the
        // invocation future remains deliberately unpolled by this task.
        let payload = host::bound(&pi_extension_protocol::profiles::Registry::coding().unwrap(), "coding.input:session.snapshot", &json!({})).unwrap();
        peer.send(w::envelope::Frame::Request(w::Request{request_id:2,parent_request_id:None,capability_ref:request.capability_ref,effect_key:None,method:"session.snapshot".into(),deadline_after_ms:Some(100),initial_stream_credit_bytes:None,payload:payload.encode_to_vec()})).await;
        let w::envelope::Frame::Response(response) = peer.recv().await else {panic!()};
        assert!(matches!(response.terminal,Some(w::response::Terminal::Error(e))if e.code==w::ErrorCode::Forbidden as i32));
        drop(attachment); assert!(!connection.current()); assert!(session.callable_tool_names().is_empty());
        let result = invocation.await.unwrap(); assert_eq!(serde_json::to_value(result).unwrap()["content"][0]["text"],"accepted before close");
    })
}

#[test]
fn default_native_session_hooks_remain_shareable() {
    struct SharedNativeHooks(std::sync::atomic::AtomicUsize);
    impl SessionHooks for SharedNativeHooks {
        fn input(
            &self,
            _: InputEvent,
            _: pi_agent::CancellationToken,
        ) -> pi_agent::BoxFuture<'_, Result<InputResult, pi_agent::AgentError>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(InputResult::Continue) })
        }
    }
    runtime().block_on(async {
        let hooks = Arc::new(SharedNativeHooks(std::sync::atomic::AtomicUsize::new(0)));
        for name in ["first native session", "second native session"] {
            let (stream, _) = scripted(vec![json!({})]);
            let session = CodingSessionBuilder::new(
                SessionManager::create(Arc::new(MemoryStore::new()), "/tmp").unwrap(),
                model(),
                stream,
            )
            .with_tools(vec![])
            .with_hooks(hooks.clone())
            .build()
            .await
            .unwrap();
            session
                .prompt(name, PromptOptions::default())
                .await
                .unwrap();
        }
        assert_eq!(hooks.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    })
}
