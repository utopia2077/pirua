use pi_mcp::{transport::*, *};
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn options() -> StdioOptions {
    let mut options = StdioOptions::new("python3");
    options.args = vec![format!(
        "{}/tests/fixtures/stdio_peer.py",
        env!("CARGO_MANIFEST_DIR")
    )];
    options.close_timeout = Duration::from_millis(100);
    options
}
#[tokio::test]
async fn real_stdio_peer_tools_progress_stderr_and_graceful_shutdown() {
    let transport = Arc::new(StdioTransport::new(options()));
    let client = McpClient::connect(ClientOptions::new("rust", "1"), transport.clone())
        .await
        .unwrap();
    assert!(transport.pid().is_some());
    assert_eq!(
        client.list_tools(RequestOptions::default()).await.unwrap()[0].name,
        "echo"
    );
    let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
    let result = client
        .call_tool(
            "echo",
            json!({"word":"world"}),
            RequestOptions {
                progress: Some(progress),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updates.recv().await.unwrap().progress, 1.0);
    assert!(
        result.content[0].fields["text"]
            .as_str()
            .unwrap()
            .contains("你好")
    );
    assert!(transport.stderr().contains("fixture ready"));
    client.close().await.unwrap();
    assert!(transport.pid().is_none());
    assert_eq!(
        client.ping(RequestOptions::default()).await,
        Err(Error::Closed)
    );
    client.close().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn stubborn_process_group_is_killed_and_reaped() {
    let mut options = options();
    options.args.push("stubborn".into());
    let transport = Arc::new(StdioTransport::new(options));
    let client = McpClient::connect(ClientOptions::new("c", "1"), transport.clone())
        .await
        .unwrap();
    let pid = transport.pid().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let descendant = loop {
        if let Some(pid) = transport.stderr().lines().find_map(|line| {
            line.strip_prefix("descendant=")
                .and_then(|s| s.parse::<u32>().ok())
        }) {
            break pid;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    };
    tokio::time::timeout(Duration::from_secs(2), client.close())
        .await
        .unwrap()
        .unwrap();
    assert!(transport.pid().is_none());
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    // A killed orphan can briefly remain a zombie until its OS parent reaps it.
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{descendant}/stat")) {
        assert!(
            stat.split_whitespace().nth(2) == Some("Z"),
            "descendant is still running: {stat}"
        );
    }
}
#[tokio::test]
async fn spawn_failure_closes_without_hanging() {
    let transport = Arc::new(StdioTransport::new(StdioOptions::new(
        "/nonexistent/pi-mcp-server",
    )));
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            McpClient::connect(ClientOptions::new("c", "1"), transport.clone())
        )
        .await
        .unwrap()
        .is_err()
    );
    transport.close().await.unwrap();
}

#[tokio::test]
async fn final_response_is_delivered_before_process_exit_closes_the_transport() {
    let mut options = StdioOptions::new("python3");
    options.args = vec!["-c".into(), "import json,sys; r=json.loads(input()); print(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'final':True}}),flush=True)".into()];
    let transport = StdioTransport::new(options);
    let mut events = transport.start().await.unwrap();
    transport
        .send(
            json!({"jsonrpc":"2.0","id":42,"method":"ping"}),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    match events.recv().await.unwrap() {
        TransportEvent::Message(value) => assert_eq!(value["result"]["final"], true),
        other => panic!("final frame lost: {other:?}"),
    }
    assert!(matches!(
        events.recv().await.unwrap(),
        TransportEvent::Closed
    ));
    transport.close().await.unwrap();
}
