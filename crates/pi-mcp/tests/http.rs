mod support;
use pi_mcp::{transport::*, *};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;

fn http_options(url: url::Url) -> HttpOptions {
    let mut options = HttpOptions::new(url);
    options.reconnect = ReconnectOptions {
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        max_retries: 2,
    };
    options
}
#[tokio::test]
async fn json_sse_session_protocol_headers_and_delete() {
    let mut server=server(Arc::new(|request|Box::pin(async move {
        if request.method=="DELETE" { return Response::empty(204); }
        if request.method=="GET" { return Response::empty(405); }
        let message=request.json();match message["method"].as_str().unwrap_or("") {
            "initialize"=>Response::json(initialized(message["id"].clone())).header("mcp-session-id","session-one"),
            "tools/call"=>Response::new(200,"text/event-stream",format!("id: result\r\ndata: {{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"hello\"}}],\"structuredContent\":{{\"n\":4}}}}}}\r\n\r\n",message["id"])),
            _=>Response::empty(202),
        }
    }))).await;
    let transport =
        Arc::new(StreamableHttpTransport::new(http_options(server.url.clone())).unwrap());
    let client = McpClient::connect(ClientOptions::new("rust", "1"), transport.clone())
        .await
        .unwrap();
    assert_eq!(transport.session_id().as_deref(), Some("session-one"));
    let result = client
        .call_tool("greet", json!({"name":"test"}), RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(result.content[0].fields["text"], "hello");
    assert_eq!(result.structured_content, Some(json!({"n":4})));
    client.close().await.unwrap();
    let mut methods = Vec::new();
    loop {
        let req = next_request(&mut server).await;
        methods.push(req.method.clone());
        if req.method == "POST" && req.json()["method"] == "initialize" {
            assert!(!req.headers.contains_key("mcp-session-id"));
            assert_eq!(req.headers["accept"], "application/json, text/event-stream");
        } else {
            assert_eq!(req.headers["mcp-session-id"], "session-one");
            assert_eq!(req.headers["mcp-protocol-version"], "2025-11-25");
        }
        if req.method == "DELETE" {
            break;
        }
    }
    assert!(methods.contains(&"GET".into()));
}
#[tokio::test]
async fn response_stream_resumes_from_id_only_priming_event() {
    let id = Arc::new(Mutex::new(json!(null)));
    let response_id = id.clone();
    let mut server = server(Arc::new(move |request| {
        let id = response_id.clone();
        Box::pin(async move {
            if request.method == "GET" {
                assert_eq!(request.headers["last-event-id"], "resume-42");
                return Response::new(
                    200,
                    "text/event-stream",
                    format!(
                        "data: {{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"content\":[]}}}}\n\n",
                        *id.lock().unwrap()
                    ),
                );
            }
            if request.method == "DELETE" {
                return Response::empty(204);
            }
            let message = request.json();
            match message["method"].as_str().unwrap_or("") {
                "initialize" => Response::json(initialized(message["id"].clone())),
                "tools/call" => {
                    *id.lock().unwrap() = message["id"].clone();
                    Response::new(200, "text/event-stream", "id: resume-42\nretry: 1\n\n")
                }
                _ => Response::empty(202),
            }
        })
    }))
    .await;
    let mut options = http_options(server.url.clone());
    options.open_get_stream = false;
    let client = McpClient::connect(
        ClientOptions::new("rust", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    assert!(
        client
            .call_tool("resume", json!({}), RequestOptions::default())
            .await
            .unwrap()
            .content
            .is_empty()
    );
    let mut resumed = false;
    for _ in 0..4 {
        let request = next_request(&mut server).await;
        if request.method == "GET" {
            resumed = true;
            break;
        }
    }
    assert!(resumed);
    client.close().await.unwrap();
}
#[tokio::test]
async fn get_stream_reconnects_and_forwards_server_requests_and_notifications() {
    let gets = Arc::new(AtomicUsize::new(0));
    let get_count = gets.clone();
    let (notification, mut notifications) = tokio::sync::mpsc::unbounded_channel();
    let mut server=server(Arc::new(move |request|{let count=get_count.clone();let notification=notification.clone();Box::pin(async move {
        if request.method=="GET" {
            let n=count.fetch_add(1,Ordering::SeqCst);
            if n==0 {assert!(!request.headers.contains_key("last-event-id"));return Response::new(200,"text/event-stream","retry: 1\nid: get-one\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"server-ping\",\"method\":\"ping\"}\n\n");}
            assert_eq!(request.headers["last-event-id"],"get-one");
            if n==1 {return Response::new(200,"text/event-stream","data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n");}
            return Response::empty(405);
        }
        let message=request.json();if message["method"]=="initialize" {return Response::json(initialized(message["id"].clone()));}
        if message["id"]=="server-ping" {assert_eq!(message["result"],json!({}));notification.send(()).unwrap();}
        Response::empty(202)
    })})).await;
    let client = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(http_options(server.url.clone())).unwrap()),
    )
    .await
    .unwrap();
    let mut events = client.subscribe();
    tokio::time::timeout(Duration::from_secs(2), notifications.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let ClientEvent::Notification { method, .. } = events.recv().await.unwrap()
                && method == "notifications/tools/list_changed"
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(gets.load(Ordering::SeqCst) >= 2);
    client.close().await.unwrap();
    while server.requests.try_recv().is_ok() {}
}
#[tokio::test]
async fn a_broken_response_stream_fails_only_its_request_and_session_expiry_is_typed() {
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let server = server(Arc::new(move |request| {
        let count = calls.clone();
        Box::pin(async move {
            if request.method == "DELETE" {
                return Response::empty(204);
            }
            let message = request.json();
            match message["method"].as_str().unwrap_or("") {
                "initialize" => {
                    Response::json(initialized(message["id"].clone())).header("mcp-session-id", "s")
                }
                "tools/call" => {
                    Response::new(200, "text/event-stream", ": no answer or cursor\n\n")
                }
                "ping" => match count.fetch_add(1, Ordering::SeqCst) {
                    0 => Response::json(json!({"jsonrpc":"2.0","id":message["id"],"result":{}})),
                    _ => Response::empty(404),
                },
                _ => Response::empty(202),
            }
        })
    }))
    .await;
    let mut options = http_options(server.url.clone());
    options.open_get_stream = false;
    let client = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    assert!(matches!(
        client
            .call_tool("broken", json!({}), RequestOptions::default())
            .await,
        Err(Error::Protocol { code: -32603, .. })
    ));
    client.ping(RequestOptions::default()).await.unwrap();
    assert_eq!(
        client.ping(RequestOptions::default()).await,
        Err(Error::SessionExpired)
    );
    client.close().await.unwrap();
}
#[tokio::test]
async fn rejects_no_response_batch_and_oversized_body() {
    for mode in 0..3 {
        let server = server(Arc::new(move |request| {
            Box::pin(async move {
                let message = request.json();
                if message["method"] == "initialize" {
                    return Response::json(initialized(message["id"].clone()));
                }
                if message["method"] == "ping" {
                    return match mode {
                        0 => Response::empty(202),
                        1 => Response::json(
                            json!([{ "jsonrpc":"2.0","id":message["id"],"result":{} }]),
                        ),
                        _ => Response::new(200, "application/json", "x".repeat(1024)),
                    };
                }
                Response::empty(202)
            })
        }))
        .await;
        let mut options = http_options(server.url.clone());
        options.open_get_stream = false;
        options.max_message_bytes = 512;
        let client = McpClient::connect(
            ClientOptions::new("c", "1"),
            Arc::new(StreamableHttpTransport::new(options).unwrap()),
        )
        .await
        .unwrap();
        assert!(client.ping(RequestOptions::default()).await.is_err());
        client.close().await.unwrap();
    }
}
#[test]
fn sse_chunking_crlf_multiline_utf8_and_limits() {
    let bytes=": comment\r\nid: first\r\nretry: 7\r\nevent: message\r\ndata: {\"text\":\r\ndata: \"你好\"}\r\n\r\nid: prime\n\n".as_bytes();
    let mut parser = SseParser::new(128);
    let mut events = Vec::new();
    for byte in bytes {
        events.extend(parser.push(&[*byte]).unwrap());
    }
    events.extend(parser.finish().unwrap());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data, "{\"text\":\n\"你好\"}");
    assert_eq!(parser.last_event_id.as_deref(), Some("prime"));
    assert_eq!(parser.retry_ms, Some(7));
    let mut parser = SseParser::new(10);
    assert!(parser.push(b"data: abc\ndata: def\ndata: ghi\n").is_err());
    let mut parser = SseParser::new(10);
    assert!(parser.push(b"long partial line").is_err());
}

#[tokio::test]
async fn auth_failures_are_typed_and_shutdown_uses_last_token_without_refresh() {
    struct CountingAuth(AtomicUsize);
    impl AuthProvider for CountingAuth {
        fn token(&self) -> BoxFuture<'_, Result<Option<String>>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Some("last-token".into())) })
        }
        fn on_unauthorized(&self, _: UnauthorizedContext) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { panic!("shutdown must not invoke authentication recovery") })
        }
    }
    let server = server(Arc::new(|request| {
        Box::pin(async move {
            if request.method == "DELETE" {
                assert_eq!(request.headers["authorization"], "Bearer last-token");
                return Response::empty(401);
            }
            let message = request.json();
            if message["method"] == "initialize" {
                return Response::json(initialized(message["id"].clone()))
                    .header("mcp-session-id", "session");
            }
            Response::empty(202)
        })
    }))
    .await;
    let auth = Arc::new(CountingAuth(AtomicUsize::new(0)));
    let mut options = http_options(server.url.clone());
    options.open_get_stream = false;
    options.auth = Some(auth.clone());
    let client = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    let tokens_before = auth.0.load(Ordering::SeqCst);
    client.close().await.unwrap();
    assert_eq!(auth.0.load(Ordering::SeqCst), tokens_before);

    let server = support::server(Arc::new(|_| {
        Box::pin(async { Response::empty(401).header("www-authenticate", "Bearer realm=\"test\"") })
    }))
    .await;
    let result = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(http_options(server.url.clone())).unwrap()),
    )
    .await;
    assert!(
        matches!(result, Err(Error::AuthRequired {challenge: Some(value)}) if value == "Bearer realm=\"test\"")
    );
}

#[tokio::test]
async fn closing_with_an_inflight_http_response_settles_promptly() {
    let mut server = server(Arc::new(|request| {
        Box::pin(async move {
            let message = request.json();
            match message["method"].as_str() {
                Some("initialize") => Response::json(initialized(message["id"].clone())),
                Some("ping") => {
                    Response::json(json!({"jsonrpc":"2.0","id":message["id"],"result":{}}))
                        .delayed(Duration::from_secs(10))
                }
                _ => Response::empty(202),
            }
        })
    }))
    .await;
    let mut options = http_options(server.url.clone());
    options.open_get_stream = false;
    let client = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    let calls = client.clone();
    let task = tokio::spawn(async move { calls.ping(RequestOptions::default()).await });
    loop {
        if next_request(&mut server).await.json()["method"] == "ping" {
            break;
        }
    }
    client.close().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Closed)
    );
}

#[test]
fn sse_initial_bom_survives_split_bytes_without_stripping_later_boms() {
    let data =
        "\u{feff}data: first\n\ndata: \u{feff}inside\n\n\u{feff}data: not-a-field\n\n".as_bytes();
    for split in 0..=3 {
        let mut parser = SseParser::new(1024);
        let mut events = parser.push(&data[..split]).unwrap();
        events.extend(parser.push(&data[split..]).unwrap());
        events.extend(parser.finish().unwrap());
        assert_eq!(
            events
                .iter()
                .map(|event| event.data.as_str())
                .collect::<Vec<_>>(),
            ["first", "\u{feff}inside"]
        );
    }
    let mut parser = SseParser::new(32);
    for byte in "\u{feff}id: first-cursor\n\ndata: answer\n\n".as_bytes() {
        parser.push(&[*byte]).unwrap();
    }
    assert_eq!(parser.last_event_id.as_deref(), Some("first-cursor"));
    // The decoder removes exactly one BOM before applying message byte limits.
    let mut parser = SseParser::new(1);
    for byte in b"\xef\xbb\xbf\n" {
        parser.push(&[*byte]).unwrap();
    }
    assert!(parser.finish().unwrap().is_empty());
}

#[tokio::test]
async fn bom_prefixed_http_initialization_and_response_deliver_their_results() {
    let server = server(Arc::new(|request| {
        Box::pin(async move {
            let message = request.json();
            let response = match message["method"].as_str() {
                Some("initialize") => initialized(message["id"].clone()),
                Some("ping") => json!({"jsonrpc":"2.0","id":message["id"],"result":{}}),
                _ => return Response::empty(202),
            };
            Response::new(
                200,
                "text/event-stream",
                format!("\u{feff}data: {response}\r\n\r\n"),
            )
        })
    }))
    .await;
    let mut options = http_options(server.url.clone());
    options.open_get_stream = false;
    let client = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    client.ping(RequestOptions::default()).await.unwrap();
    client.close().await.unwrap();
}
