#![allow(dead_code)]
use pi_mcp::{BoxFuture, CancellationToken};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
impl Request {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub delay: Duration,
}
impl Response {
    pub fn new(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), content_type.into())],
            body: body.into(),
            delay: Duration::ZERO,
        }
    }
    pub fn json(value: Value) -> Self {
        Self::new(200, "application/json", serde_json::to_vec(&value).unwrap())
    }
    pub fn empty(status: u16) -> Self {
        Self::new(status, "text/plain", Vec::new())
    }
    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
    pub fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}
pub type Handler = Arc<dyn Fn(Request) -> BoxFuture<'static, Response> + Send + Sync>;
pub struct Server {
    pub url: url::Url,
    pub requests: mpsc::UnboundedReceiver<Request>,
    stop: CancellationToken,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
pub async fn server(handler: Handler) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let stop = CancellationToken::new();
    let (sender, requests) = mpsc::unbounded_channel();
    let task_stop = stop.clone();
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! { _ = task_stop.cancelled() => break, accepted = listener.accept() => accepted };
            let Ok((mut socket, _)) = accepted else {
                break;
            };
            let handler = handler.clone();
            let sender = sender.clone();
            let stop = task_stop.clone();
            tokio::spawn(async move {
                let mut data = Vec::new();
                let mut chunk = [0_u8; 4096];
                let (header_end, body_size) = loop {
                    let n = tokio::select! { _ = stop.cancelled() => return, read = socket.read(&mut chunk) => read.unwrap_or(0) };
                    if n == 0 {
                        return;
                    }
                    data.extend_from_slice(&chunk[..n]);
                    assert!(data.len() < 2 * 1024 * 1024);
                    if let Some(index) = data.windows(4).position(|window| window == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&data[..index]);
                        let size = header
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        break (index + 4, size);
                    }
                };
                while data.len() < header_end + body_size {
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    data.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&data[..header_end]);
                let mut lines = head.lines();
                let mut first = lines.next().unwrap().split_whitespace();
                let method = first.next().unwrap().to_string();
                let path = first.next().unwrap().to_string();
                let headers = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
                    .collect();
                let request = Request {
                    method,
                    path,
                    headers,
                    body: data[header_end..header_end + body_size].to_vec(),
                };
                sender.send(request.clone()).unwrap();
                let response = handler(request).await;
                tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(response.delay) => {} }
                let mut head = format!(
                    "HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\n",
                    response.status,
                    response.body.len()
                );
                for (key, value) in response.headers {
                    head.push_str(&format!("{key}: {value}\r\n"));
                }
                head.push_str("\r\n");
                if socket.write_all(head.as_bytes()).await.is_ok() {
                    let _ = socket.write_all(&response.body).await;
                }
                let _ = socket.shutdown().await;
            });
        }
    });
    Server {
        url,
        requests,
        stop,
    }
}
pub fn initialized(id: Value) -> Value {
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{},"resources":{}},"serverInfo":{"name":"fixture","version":"1"}}})
}
pub async fn next_request(server: &mut Server) -> Request {
    tokio::time::timeout(Duration::from_secs(3), server.requests.recv())
        .await
        .unwrap()
        .unwrap()
}
