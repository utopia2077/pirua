mod adapter_support;
use adapter_support::*;
use pi_agent::{AgentError, AgentToolResult, BoxFuture, CancellationToken};
use pi_coding_runtime::{mcp::*, *};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, atomic::Ordering};

#[test]
fn exact_tag_mcp_observations() {
    session::runtime().block_on(async {
        let trace = source_trace();
        let mut checked = 0;
        for row in source_cases() {
            let expected = &trace["observations"][row["id"].as_str().unwrap()];
            let actual = match row["kind"].as_str().unwrap() {
                "name" => json!(create_mcp_tool_name(
                    row["server"].as_str().unwrap(),
                    row["tool"].as_str().unwrap(),
                    |_| row["taken"].as_bool().unwrap_or(false)
                )),
                "schema" => {
                    serde_json::to_value(create_mcp_result_schema(Some(&row["schema"])).unwrap())
                        .unwrap()
                }
                "mcp-result" => {
                    let converted = convert_mcp_result(
                        "server",
                        "tool",
                        serde_json::from_value(row["result"].clone()).unwrap(),
                        &McpResultOptions {
                            save_output: Some(Arc::new(|_, _| {
                                Box::pin(async { Ok("/fixture/output".into()) })
                            })),
                            resource_reader: Some("read_mcp_resource".into()),
                        },
                    )
                    .await
                    .unwrap();
                    serde_json::to_value(converted).unwrap()
                }
                _ => continue,
            };
            assert_eq!(&actual, expected, "{}", row["id"]);
            checked += 1;
        }
        assert_eq!(checked, 8);
    });
}

#[test]
fn target_names_and_relocated_output_schema() {
    assert_eq!(
        create_mcp_tool_name("my-server", "a.b", |_| false),
        "mcp__my_server__a_b"
    );
    assert_eq!(
        create_mcp_tool_name("s", "a-b", |_| true),
        "mcp__s__a_b_ed3867d6"
    );
    assert_eq!(
        create_mcp_tool_name(&"long".repeat(20), "tool", |_| false),
        format!(
            "{}_fb907b80",
            &format!("mcp__{}__tool", "long".repeat(20))[..55]
        )
    );
    let schema = create_mcp_result_schema(Some(
        &json!({"$ref":"#/$defs/Item","$defs":{"Item":{"type":"object","properties":{"$ref":{"const":{"$ref":"#"}}}}},"default":{"$ref":"#"},"properties":{"$ref":{"$ref":"#"}}}),
    ));
    let v = serde_json::to_value(schema.unwrap()).unwrap();
    assert_eq!(
        v["properties"]["structuredContent"]["$ref"],
        "#/properties/structuredContent/$defs/Item"
    );
    assert_eq!(v["properties"]["structuredContent"]["default"]["$ref"], "#");
    assert_eq!(
        v["properties"]["structuredContent"]["properties"]["$ref"]["$ref"],
        "#/properties/structuredContent"
    );
}

#[test]
fn model_projection_preserves_full_script_content_and_spills_resources() {
    session::runtime().block_on(async {
        let saved=Arc::new(Mutex::new(Vec::new()));let capture=saved.clone();
        let options=McpResultOptions { save_output:Some(Arc::new(move|data,ext| {capture.lock().unwrap().push((data,ext));Box::pin(async{Ok("/fixture/output".into())})})),resource_reader:Some("read_mcp_resource".into()) };
        let result:pi_mcp::CallToolResult=serde_json::from_value(json!({"content":[{"type":"text","text":"hello","_meta":{"block":"keep"}},{"type":"image","data":"aGk=","mimeType":"image/png"},{"type":"resource_link","uri":"resource://x","name":"x","title":"Title","mimeType":"text/plain","size":2048},{"type":"resource","resource":{"uri":"resource://text","mimeType":"application/json","blob":"eyJhIjoxfQ=="}},{"type":"resource","resource":{"uri":"resource://x.bin","mimeType":"application/octet-stream","blob":"AAEC"}},{"type":"audio","data":"AA==","mimeType":"audio/wav"}],"structuredContent":null,"isError":true,"_meta":{"private":"drop"},"future":42})).unwrap();
        let converted=convert_mcp_result("server","tool",result,&options).await.unwrap();
        let output=text_of(&converted);
        assert!(output.contains("hello"));assert!(output.contains("Resource resource://x \"Title\" (text/plain, 2.0KB)"));assert!(output.contains("read_mcp_resource"));assert!(output.contains("{\"a\":1}"));assert!(output.contains("saved to /fixture/output"));assert!(output.contains("audio audio/wav omitted"));
        assert_eq!(converted.is_error,Some(true));let script=serde_json::to_value(converted.structured_content.unwrap()).unwrap();assert_eq!(script["structuredContent"],Value::Null);assert_eq!(script["future"],42);assert!(script.get("_meta").is_none());assert_eq!(script["content"][0]["_meta"]["block"],"keep");assert_eq!(saved.lock().unwrap()[0],(vec![0,1,2],".bin".into()));
        let long="🐈".repeat(8000);let result=serde_json::from_value(json!({"content":[{"type":"text","text":long},{"type":"image","data":"aGk=","mimeType":"image/png"}]})).unwrap();let converted=convert_mcp_result("s","t",result,&options).await.unwrap();assert!(text_of(&converted).contains("truncated output"));assert_eq!(serde_json::to_value(converted.structured_content.as_ref().unwrap()).unwrap()["content"][0]["text"],long);assert_eq!(converted.content.len(),2);assert_eq!(saved.lock().unwrap().last().unwrap().0,long.as_bytes());
    });
}

#[derive(Default)]
struct Hooks {
    before: Mutex<Vec<String>>,
    after: Mutex<Vec<String>>,
    progress: Mutex<Vec<String>>,
}
impl SessionHooks for Hooks {
    fn before_tool(
        &self,
        event: pi_agent::BeforeToolCallContext,
        _: Option<String>,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<pi_agent::BeforeToolCallResult>, AgentError>> {
        Box::pin(async move {
            self.before
                .lock()
                .unwrap()
                .push(event.tool_call.name.to_string());
            Ok(None)
        })
    }
    fn after_tool(
        &self,
        event: pi_agent::AfterToolCallContext,
        _: Option<String>,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<pi_agent::AfterToolCallResult>, AgentError>> {
        Box::pin(async move {
            self.after
                .lock()
                .unwrap()
                .push(event.before.tool_call.name.to_string());
            Ok(None)
        })
    }
    fn event(&self, event: SessionEvent) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async move {
            if let SessionEvent::NestedTool {
                event: pi_agent::AgentEvent::ToolExecutionUpdate { partial_result, .. },
                ..
            } = event
            {
                self.progress.lock().unwrap().push(text_of(&partial_result));
            }
            Ok(())
        })
    }
}
#[test]
fn discovery_refresh_collision_validation_progress_and_retirement() {
    session::runtime().block_on(async {
        let hooks=Arc::new(Hooks::default());let coding=coding(vec![],hooks.clone()).await;
        let (client,peer)=client(vec![mcp_tool("a-b"),mcp_tool("a_b")]).await;
        let registry=McpToolRegistry::new(coding.downgrade());let names=registry.attach("s",client.clone(),McpServerOptions{exposure:ToolExposure::CodeMode,..Default::default()},CancellationToken::new()).await.unwrap();
        assert_eq!(names.len(),2);assert!(names.contains(&"mcp__s__a_b".into()));assert!(names.contains(&"mcp__s__a_b_8d47d403".into()));
        let outcome=invoke(&coding,"mcp__s__a_b",json!({"value":3})).await;assert!(!outcome.is_error);assert_eq!(peer.calls.lock().unwrap().len(),1);assert_eq!(serde_json::to_value(outcome.result.structured_content.unwrap()).unwrap()["structuredContent"]["n"],7);
        let rejected=invoke(&coding,"mcp__s__a_b",json!({"value":"wrong"})).await;assert!(rejected.is_error);assert_eq!(peer.calls.lock().unwrap().len(),1);assert_eq!(hooks.before.lock().unwrap().len(),1);assert_eq!(hooks.after.lock().unwrap().len(),1);assert_eq!(*hooks.progress.lock().unwrap(),vec!["halfway"]);
        *peer.tools.lock().unwrap()=vec![mcp_tool("a_b"),mcp_tool("a-b"),mcp_tool("new")];let refreshed=registry.refresh("s",CancellationToken::new()).await.unwrap();assert_eq!(refreshed.len(),3);assert!(refreshed.contains(&"mcp__s__a_b_8d47d403".into()));
        let foreign=session::tool("mcp__s__conflict",Arc::new(|_|Box::pin(async {Ok(AgentToolResult::default())})));coding.register_tool(RegisteredTool::direct(foreign)).unwrap();*peer.tools.lock().unwrap()=vec![mcp_tool("conflict")];assert!(registry.refresh("s",CancellationToken::new()).await.is_err());assert!(coding.callable_tool_names().contains(&"mcp__s__new".into()));
        registry.retire("s").await.unwrap();assert_eq!(coding.callable_tool_names(),vec!["mcp__s__conflict"]);assert!(client.is_connected());
        client.close().await.unwrap();
    });
}

#[test]
fn cancellation_reaches_peer_and_drop_retires_only_owned_tools() {
    session::runtime().block_on(async {
        let coding = coding(vec![], Arc::new(NoopSessionHooks)).await;
        let (client, peer) = client(vec![mcp_tool("wait")]).await;
        let registry = McpToolRegistry::new(coding.downgrade());
        registry
            .attach(
                "s",
                client.clone(),
                Default::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let signal = CancellationToken::new();
        let call = coding.execute_tool(
            "parent",
            "mcp__s__wait",
            serde_json::from_value(json!({"value":1})).unwrap(),
            signal.clone(),
            None,
        );
        tokio::pin!(call);
        let cancel = async {
            while peer.calls.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            signal.cancel();
        };
        let (result, _) = tokio::join!(call, cancel);
        assert!(result.unwrap().is_error);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while peer.cancelled.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(registry);
        assert!(coding.callable_tool_names().is_empty());
        client.close().await.unwrap();
    });
}

#[test]
fn retirement_during_discovery_cancels_pending_attachment_without_late_install() {
    session::runtime().block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3),async {
            use pi_mcp::{transport::{memory_transport_pair,TransportEvent},ClientOptions,McpClient};
            let (transport,mut peer)=memory_transport_pair();let ready=Arc::new(tokio::sync::Notify::new());let release=Arc::new(tokio::sync::Notify::new());let signal=ready.clone();let rel=release.clone();
            tokio::spawn(async move{while let Some(message)=peer.incoming.recv().await {let result=match message["method"].as_str().unwrap_or(""){"initialize"=>json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"slow","version":"1"}}),"tools/list"=>{signal.notify_one();rel.notified().await;json!({"tools":[mcp_tool("late")]})},_=>continue};let _=peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","id":message["id"],"result":result})));}});
            let client=McpClient::connect(ClientOptions::new("coding-test","1"),Arc::new(transport)).await.unwrap();let coding=coding(vec![],Arc::new(NoopSessionHooks)).await;let registry=McpToolRegistry::new(coding.downgrade());let attachment=registry.attach("s",client.clone(),Default::default(),CancellationToken::new());let retire=async{ready.notified().await;registry.retire("s").await.unwrap();release.notify_one();};let (result,_)=tokio::join!(attachment,retire);assert!(result.is_err());assert!(coding.callable_tool_names().is_empty());let (fast,_)=adapter_support::client(vec![mcp_tool("fresh")]).await;registry.attach("s",fast.clone(),Default::default(),CancellationToken::new()).await.unwrap();assert_eq!(coding.callable_tool_names(),vec!["mcp__s__fresh"]);client.close().await.unwrap();fast.close().await.unwrap();
        }).await.unwrap();
    });
}
