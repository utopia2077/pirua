use pi_mcp::{transport::*, *};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

async fn peer_message(peer: &mut MemoryPeer) -> Value {
    tokio::time::timeout(Duration::from_secs(2), peer.incoming.recv())
        .await
        .unwrap()
        .unwrap()
}
fn reply(peer: &MemoryPeer, id: Value, result: Value) {
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","id":id,"result":result}),
        ))
        .unwrap();
}
async fn connect(options: ClientOptions) -> (McpClient, MemoryPeer) {
    let (transport, mut peer) = memory_transport_pair();
    let task = tokio::spawn(McpClient::connect(options, Arc::new(transport)));
    let initialize = peer_message(&mut peer).await;
    assert_eq!(initialize["method"], "initialize");
    reply(
        &peer,
        initialize["id"].clone(),
        json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"test","version":"1"},"instructions":"hello"}),
    );
    assert_eq!(
        peer_message(&mut peer).await["method"],
        "notifications/initialized"
    );
    (task.await.unwrap().unwrap(), peer)
}
#[tokio::test]
async fn init_pagination_resources_and_detached_tool_results() {
    let (client, mut peer) = connect(ClientOptions::new("rust", "1")).await;
    assert_eq!(client.initialization().protocol_version, "2025-06-18");
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.list_tools(RequestOptions::default()).await });
    let first = peer_message(&mut peer).await;
    reply(
        &peer,
        first["id"].clone(),
        json!({"tools":[{"name":"one","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}}],"nextCursor":"page2"}),
    );
    let second = peer_message(&mut peer).await;
    assert_eq!(second["params"]["cursor"], "page2");
    reply(
        &peer,
        second["id"].clone(),
        json!({"tools":[{"name":"two","inputSchema":{}}],"nextCursor":null}),
    );
    let tools = task.await.unwrap().unwrap();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].extra["annotations"]["readOnlyHint"], true);
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.list_resources(RequestOptions::default()).await });
    let req = peer_message(&mut peer).await;
    reply(
        &peer,
        req["id"].clone(),
        json!({"resources":[{"uri":"test:///one"}],"nextCursor":""}),
    );
    assert_eq!(task.await.unwrap().unwrap()[0].name, "test:///one");
    for structured in [json!([1, 2]), Value::Null, json!(false)] {
        let calls = client.clone();
        let task = tokio::spawn(async move {
            calls
                .call_tool("t", json!({}), RequestOptions::default())
                .await
        });
        let req = peer_message(&mut peer).await;
        reply(
            &peer,
            req["id"].clone(),
            json!({"structuredContent":structured,"isError":true}),
        );
        let result = task.await.unwrap().unwrap();
        assert!(result.content.is_empty());
        assert_eq!(result.structured_content, Some(structured));
        assert_eq!(result.is_error, Some(true));
    }
    client.close().await.unwrap();
}
#[tokio::test]
async fn correlates_reordered_replies_and_ignores_cancelled_late_response() {
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    let cancelled = CancellationToken::new();
    let token = cancelled.clone();
    let calls = client.clone();
    let first = tokio::spawn(async move {
        calls
            .request(
                "first",
                None,
                RequestOptions {
                    cancellation: token,
                    ..Default::default()
                },
            )
            .await
    });
    let first_request = peer_message(&mut peer).await;
    cancelled.cancel();
    assert_eq!(first.await.unwrap(), Err(Error::Cancelled));
    let cancel = peer_message(&mut peer).await;
    assert_eq!(cancel["method"], "notifications/cancelled");
    assert_eq!(cancel["params"]["requestId"], first_request["id"]);
    let calls = client.clone();
    let second = tokio::spawn(async move {
        calls
            .request("second", None, RequestOptions::default())
            .await
    });
    let second_request = peer_message(&mut peer).await;
    let calls = client.clone();
    let third = tokio::spawn(async move {
        calls
            .request("third", None, RequestOptions::default())
            .await
    });
    let third_request = peer_message(&mut peer).await;
    assert_ne!(first_request["id"], second_request["id"]);
    reply(&peer, first_request["id"].clone(), json!("late"));
    reply(&peer, third_request["id"].clone(), json!(3));
    reply(&peer, second_request["id"].clone(), json!(2));
    assert_eq!(second.await.unwrap().unwrap(), 2);
    assert_eq!(third.await.unwrap().unwrap(), 3);
    client.close().await.unwrap();
}
#[tokio::test]
async fn progress_renews_timeout_but_timeout_cancels_without_progress() {
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    let (progress, mut updates) = mpsc::unbounded_channel();
    let calls = client.clone();
    let task = tokio::spawn(async move {
        calls
            .request(
                "long",
                Some(json!({"_meta":{"trace":"keep"}})),
                RequestOptions {
                    timeout: Some(Duration::from_millis(100)),
                    progress: Some(progress),
                    ..Default::default()
                },
            )
            .await
    });
    let req = peer_message(&mut peer).await;
    assert_eq!(req["params"]["_meta"]["trace"], "keep");
    for n in 0..3 {
        tokio::time::sleep(Duration::from_millis(55)).await;
        peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":req["id"],"progress":n}}))).unwrap();
        assert_eq!(updates.recv().await.unwrap().progress, f64::from(n));
    }
    reply(&peer, req["id"].clone(), json!({}));
    task.await.unwrap().unwrap();
    let calls = client.clone();
    let timeout = tokio::spawn(async move {
        calls
            .ping(RequestOptions {
                timeout: Some(Duration::from_millis(10)),
                ..Default::default()
            })
            .await
    });
    let req = peer_message(&mut peer).await;
    assert!(matches!(timeout.await.unwrap(), Err(Error::Timeout(_))));
    assert_eq!(
        peer_message(&mut peer).await["params"]["requestId"],
        req["id"]
    );
    client.close().await.unwrap();
}
#[tokio::test]
async fn close_settles_requests_once_and_server_requests_are_cancellable() {
    let mut options = ClientOptions::new("c", "1");
    options.roots = Some(vec![Root {
        uri: "file:///work".into(),
        name: None,
    }]);
    let (client, mut peer) = connect(options).await;
    let mut events = client.subscribe();
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","id":"roots","method":"roots/list"}),
        ))
        .unwrap();
    assert_eq!(
        peer_message(&mut peer).await["result"]["roots"][0]["uri"],
        "file:///work"
    );
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","id":"ping","method":"ping"}),
        ))
        .unwrap();
    assert_eq!(peer_message(&mut peer).await["result"], json!({}));
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","id":"unknown","method":"sampling/createMessage"}),
        ))
        .unwrap();
    assert_eq!(peer_message(&mut peer).await["error"]["code"], -32601);
    let (started, mut starts) = mpsc::unbounded_channel();
    let (aborted, mut aborts) = mpsc::unbounded_channel();
    client.set_request_handler(
        "slow",
        Arc::new(move |_, token| {
            let started = started.clone();
            let aborted = aborted.clone();
            Box::pin(async move {
                started.send(()).unwrap();
                token.cancelled().await;
                aborted.send(()).unwrap();
                Ok(json!({}))
            })
        }),
    );
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","id":77,"method":"slow"}),
        ))
        .unwrap();
    starts.recv().await.unwrap();
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":77}}),
        ))
        .unwrap();
    // The cancellation token is available to host handlers; cancellation may
    // also drop their future before it publishes its own acknowledgement.
    let _ = tokio::time::timeout(Duration::from_millis(20), aborts.recv()).await;
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.ping(RequestOptions::default()).await });
    peer_message(&mut peer).await;
    peer.outgoing
        .send(TransportEvent::Error(Error::Io("diagnostic".into())))
        .unwrap();
    peer.outgoing.send(TransportEvent::Closed).unwrap();
    assert_eq!(task.await.unwrap(), Err(Error::Closed));
    client.close().await.unwrap();
    client.close().await.unwrap();
    let mut closes = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, ClientEvent::Closed) {
            closes += 1;
        }
    }
    assert_eq!(closes, 1);
}
#[tokio::test]
async fn initialization_timeout_never_sends_cancel_and_dropped_future_cancels() {
    let (transport, mut peer) = memory_transport_pair();
    let mut options = ClientOptions::new("c", "1");
    options.request_timeout = Duration::from_millis(10);
    let task = tokio::spawn(McpClient::connect(options, Arc::new(transport)));
    peer_message(&mut peer).await;
    assert!(matches!(task.await.unwrap(), Err(Error::Timeout(_))));
    assert!(peer.incoming.recv().await.is_none());
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.ping(RequestOptions::default()).await });
    let req = peer_message(&mut peer).await;
    task.abort();
    let _ = task.await;
    assert_eq!(
        peer_message(&mut peer).await["params"]["requestId"],
        req["id"]
    );
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.incoming.recv())
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn rejects_malformed_lists_and_protocol_errors_without_losing_connection() {
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    for invalid in [
        json!({"tools":[{"name":"bad","inputSchema":[]}]}),
        json!({"tools":[],"nextCursor":42}),
    ] {
        let calls = client.clone();
        let task = tokio::spawn(async move { calls.list_tools(RequestOptions::default()).await });
        let req = peer_message(&mut peer).await;
        reply(&peer, req["id"].clone(), invalid);
        assert!(task.await.unwrap().is_err());
    }
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.ping(RequestOptions::default()).await });
    let req = peer_message(&mut peer).await;
    peer.outgoing.send(TransportEvent::Message(json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":-32000,"message":"no","data":{"retry":false}}}))).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::Protocol { code: -32000, .. })
    ));
    assert!(client.is_connected());
    client.close().await.unwrap();
}

#[tokio::test]
async fn resource_templates_reading_and_repeated_cursor_guard() {
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    let calls = client.clone();
    let task = tokio::spawn(async move {
        calls
            .list_resource_templates_page(Some("start"), RequestOptions::default())
            .await
    });
    let request = peer_message(&mut peer).await;
    assert_eq!(request["method"], "resources/templates/list");
    assert_eq!(request["params"]["cursor"], "start");
    reply(
        &peer,
        request["id"].clone(),
        json!({"resourceTemplates":[{"uriTemplate":"test:///{id}","mimeType":"text/plain"}]}),
    );
    assert_eq!(task.await.unwrap().unwrap().items[0].name, "test:///{id}");
    let calls = client.clone();
    let task = tokio::spawn(async move {
        calls
            .read_resource("test:///one", RequestOptions::default())
            .await
    });
    let request = peer_message(&mut peer).await;
    assert_eq!(request["params"]["uri"], "test:///one");
    reply(
        &peer,
        request["id"].clone(),
        json!({"contents":[{"uri":"test:///one","text":"text","mimeType":"text/plain"},{"uri":"test:///image","blob":"AAAA","mimeType":"image/png"}]}),
    );
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.contents[0].text.as_deref(), Some("text"));
    assert_eq!(result.contents[1].blob.as_deref(), Some("AAAA"));
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.list_resources(RequestOptions::default()).await });
    for _ in 0..2 {
        let request = peer_message(&mut peer).await;
        reply(
            &peer,
            request["id"].clone(),
            json!({"resources":[],"nextCursor":"same"}),
        );
    }
    assert!(matches!(task.await.unwrap(), Err(Error::Invalid(_))));
    client.close().await.unwrap();
}

#[tokio::test]
async fn a_transport_diagnostic_does_not_terminate_an_outstanding_request() {
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    let mut events = client.subscribe();
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.ping(RequestOptions::default()).await });
    let request = peer_message(&mut peer).await;
    peer.outgoing
        .send(TransportEvent::Error(Error::Io(
            "temporary diagnostic".into(),
        )))
        .unwrap();
    assert!(matches!(
        events.recv().await.unwrap(),
        ClientEvent::Error(_)
    ));
    assert!(!task.is_finished());
    reply(&peer, request["id"].clone(), json!({}));
    task.await.unwrap().unwrap();
    drop(peer);
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.is_connected() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client.ping(RequestOptions::default()).await,
        Err(Error::Closed)
    );
}

#[tokio::test]
async fn unsupported_or_malformed_initialization_closes_the_transport() {
    for result in [
        json!({"protocolVersion":"2099-01-01","capabilities":{},"serverInfo":{"name":"x","version":"1"}}),
        json!({"protocolVersion":"2025-11-25","capabilities":[],"serverInfo":{"name":"x","version":"1"}}),
    ] {
        let (transport, mut peer) = memory_transport_pair();
        let task = tokio::spawn(McpClient::connect(
            ClientOptions::new("c", "1"),
            Arc::new(transport),
        ));
        let request = peer_message(&mut peer).await;
        reply(&peer, request["id"].clone(), result);
        assert!(matches!(task.await.unwrap(), Err(Error::Invalid(_))));
        assert!(peer.incoming.recv().await.is_none());
    }
}

#[tokio::test]
async fn numeric_ids_correlate_when_peer_uses_decimal_json_notation() {
    let (client, mut peer) = connect(ClientOptions::new("c", "1")).await;
    let calls = client.clone();
    let task = tokio::spawn(async move {
        calls
            .request(
                "decimal-id",
                None,
                RequestOptions {
                    timeout: Some(Duration::from_millis(20)),
                    ..Default::default()
                },
            )
            .await
    });
    let request = peer_message(&mut peer).await;
    let id = request["id"].as_u64().unwrap();
    let decimal: Value = serde_json::from_str(&format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{id}.0,\"result\":\"matched\"}}"
    ))
    .unwrap();
    peer.outgoing
        .send(TransportEvent::Message(decimal))
        .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), "matched");
    client.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn queued_server_request_must_not_start_after_client_close() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (client, peer) = connect(ClientOptions::new("c", "1")).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let observed = effects.clone();
    client.set_request_handler(
        "effect",
        Arc::new(move |_, token| {
            let observed = observed.clone();
            Box::pin(async move {
                assert!(!token.is_cancelled());
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(json!({}))
            })
        }),
    );
    peer.outgoing
        .send(TransportEvent::Message(
            json!({"jsonrpc":"2.0","id":99,"method":"effect"}),
        ))
        .unwrap();
    // Memory close does not yield: the incoming request remains queued ahead
    // of the transport's Closed event when shutdown takes terminal ownership.
    client.close().await.unwrap();
    assert!(!client.is_connected());
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        effects.load(Ordering::SeqCst),
        0,
        "queued handler ran after closure"
    );
}
