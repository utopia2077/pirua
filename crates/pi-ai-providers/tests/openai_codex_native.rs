#![cfg(all(feature = "wire-openai-codex-responses", feature = "native"))]
use pi_ai_core::*;
use pi_ai_providers::api::{openai_codex_responses::*, runtime::AdapterServices};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};
struct Spawner(tokio::runtime::Handle);
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(task);
        Ok(())
    }
}
struct Entropy;
impl UuidV7Entropy for Entropy {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Ok([0; 16])
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("no SDK jitter")
    }
}
fn half_close(socket: &TcpStream) {
    // The client can finish physical close after the final write. Only that
    // precise already-closed outcome is allowed; each caller still requires EOF.
    if let Err(error) = socket.shutdown(Shutdown::Write) {
        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
    }
}
fn engine() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
fn wait<T>(e: &tokio::runtime::Runtime, f: impl Future<Output = T>) -> T {
    e.block_on(async {
        tokio::time::timeout(Duration::from_secs(4), f)
            .await
            .expect("bounded native operation")
    })
}
fn base64(bytes: &[u8]) -> String {
    const ABC: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in bytes.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        out.push(ABC[((n >> 18) & 63) as usize] as char);
        out.push(ABC[((n >> 12) & 63) as usize] as char);
        out.push(if c.len() > 1 {
            ABC[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            ABC[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
fn sha1(bytes: &[u8]) -> Vec<u8> {
    let mut data = bytes.to_vec();
    let bits = (data.len() as u64) * 8;
    data.push(128);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend(bits.to_be_bytes());
    let mut h = [
        0x67452301u32,
        0xefcdab89,
        0x98badcfe,
        0x10325476,
        0xc3d2e1f0,
    ];
    for chunk in data.chunks(64) {
        let mut w = [0u32; 80];
        for (i, v) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(v.try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, x) in w.into_iter().enumerate() {
            let (f, k) = match i {
                0..20 => ((b & c) | (!b & d), 0x5a827999),
                20..40 => (b ^ c ^ d, 0x6ed9eba1),
                40..60 => ((b & c) | (b & d) | (c & d), 0x8f1bbcdc),
                _ => (b ^ c ^ d, 0xca62c1d6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(x);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (v, n) in h.iter_mut().zip([a, b, c, d, e]) {
            *v = v.wrapping_add(n);
        }
    }
    h.into_iter().flat_map(u32::to_be_bytes).collect()
}
fn head(s: &mut TcpStream) -> BTreeMap<String, String> {
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut bytes = vec![];
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        s.read_exact(&mut b).unwrap();
        bytes.push(b[0]);
        assert!(bytes.len() < 65536);
    }
    String::from_utf8(bytes)
        .unwrap()
        .split("\r\n")
        .filter_map(|s| s.split_once(": "))
        .map(|(k, v)| (k.to_lowercase(), v.to_owned()))
        .collect()
}
fn upgrade(s: &mut TcpStream) {
    let h = head(s);
    let accept = base64(&sha1(
        format!(
            "{}258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
            h["sec-websocket-key"]
        )
        .as_bytes(),
    ));
    s.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover\r\n\r\n").as_bytes()).unwrap();
}
fn frame(op: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![128 | op];
    if body.len() < 126 {
        out.push(body.len() as u8);
    } else {
        out.push(126);
        out.extend((body.len() as u16).to_be_bytes());
    }
    out.extend(body);
    out
}
fn read_frame(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut h = [0; 2];
    s.read_exact(&mut h).unwrap();
    let n = match h[1] & 127 {
        126 => {
            let mut b = [0; 2];
            s.read_exact(&mut b).unwrap();
            u16::from_be_bytes(b) as usize
        }
        127 => {
            let mut b = [0; 8];
            s.read_exact(&mut b).unwrap();
            u64::from_be_bytes(b) as usize
        }
        n => n as usize,
    };
    assert!(h[1] & 128 != 0);
    assert_eq!(h[0] & 64, 0, "client frames stay uncompressed");
    let mut mask = [0; 4];
    s.read_exact(&mut mask).unwrap();
    let mut body = vec![0; n];
    s.read_exact(&mut body).unwrap();
    for (i, b) in body.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    (h[0] & 15, body)
}
fn events(id: &str) -> Vec<Value> {
    vec![
        json!({"type":"response.created","response":{"id":id}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":format!("m-{id}"),"role":"assistant","content":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":format!("m-{id}"),"role":"assistant","status":"completed","content":[{"type":"output_text","text":"Hello"}]}}),
        json!({"type":"response.completed","response":{"id":id,"status":"completed","end_turn":false,"usage":{"input_tokens":17,"output_tokens":1,"total_tokens":18}}}),
    ]
}
fn request(
    endpoint: &str,
    rt: RequestRuntime,
    context: Value,
    transport: &str,
) -> TextStreamRequest {
    TextStreamRequest{resolved:ResolvedRequest{model:serde_json::from_value(json!({"id":"gpt-5.5","name":"Codex","api":"openai-codex-responses","provider":"openai-codex","baseUrl":"https://example.invalid","reasoning":true,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":1000})).unwrap(),endpoint:RequestEndpoint::new_resolved(endpoint).unwrap()},context:serde_json::from_value(context).unwrap(),auth:ResolvedAuth{api_key:Some(SecretString::new(format!("aaa.{}.bbb",base64(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"account"}}"#)))),..Default::default()},options:StreamOptions{wire:serde_json::from_value(json!({"transport":transport,"sessionId":"session","timeoutMs":if transport=="sse"{50}else{0}})).unwrap(),runtime:rt}}
}
fn adapter(
    e: &tokio::runtime::Runtime,
    clock: Arc<dyn Clock>,
    presence: CodexSourceSignalPresence,
) -> (OpenAICodexResponsesApi, RequestRuntime) {
    let rt = RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        clock.clone(),
        Arc::new(Spawner(e.handle().clone())),
    );
    let services = AdapterServices::new(Arc::new(HttpTransport::new(
        Arc::new(NativeHttpClient::new(e.handle().clone())),
        Arc::new(Jitter),
    )));
    let api = OpenAICodexResponsesApi::new(CodexServices {
        adapter: services,
        websocket: Some(Arc::new(
            Node26WebSocketConnector::new(e.handle().clone()).unwrap(),
        )),
        request_ids: Arc::new(UuidV7Generator::new(clock, Arc::new(Entropy))),
        date_parser: Arc::new(HttpRetryDateParser),
        signal_presence: presence,
        compression: Some(ZstdRequestLimits {
            max_input_bytes: 1_048_576,
            max_output_bytes: 1_048_576,
        }),
        limits: Default::default(),
    })
    .unwrap();
    (api, rt)
}
#[test]
fn native_cached_delta_and_owned_debug_close_flush_exact_frame() {
    cached_delta_control(false);
    cached_delta_control(true);
}
fn cached_delta_control(cancel_previous: bool) {
    assert_eq!(
        base64(&sha1(
            b"dGhlIHNhbXBsZSBub25jZQ==258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        upgrade(&mut s);
        for turn in 1..=2 {
            let (op, bytes) = read_frame(&mut s);
            assert_eq!(op, 1);
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["store"], false);
            assert_eq!(body["type"], "response.create");
            if turn == 1 {
                assert!(body.get("previous_response_id").is_none());
            } else {
                assert_eq!(body["previous_response_id"], "r1");
                assert_eq!(
                    body["input"],
                    json!([{"role":"user","content":[{"type":"input_text","text":"next"}]}])
                );
            }
            let fixture: Value = serde_json::from_str(include_str!(
                "../../../conformance/provider-traces/openai-codex-responses/native-frames.json"
            ))
            .unwrap();
            for message in fixture["groups"][turn - 1].as_array().unwrap() {
                let bytes = message["deflate"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8)
                    .collect::<Vec<_>>();
                let mut compressed = frame(1, &bytes);
                compressed[0] |= 64;
                s.write_all(&compressed).unwrap();
            }
        }
        let (op, body) = read_frame(&mut s);
        assert_eq!(op, 8);
        assert_eq!(&body[..2], &1000u16.to_be_bytes());
        assert_eq!(&body[2..], b"debug_close");
        s.write_all(&frame(8, &1000u16.to_be_bytes())).unwrap();
        half_close(&s);
        let mut tail = vec![];
        s.read_to_end(&mut tail).unwrap();
    });
    let e = engine();
    let (api, rt) = adapter(
        &e,
        Arc::new(TokioClock::new(e.handle().clone())),
        CodexSourceSignalPresence::Present,
    );
    let context = json!({"messages":[{"role":"user","content":"hello","timestamp":7}]});
    let first = wait(
        &e,
        api.stream(request(&endpoint, rt.clone(), context.clone(), "auto"))
            .result(),
    );
    assert_eq!(first.stop_reason, StopReason::Stop);
    let mut second_context = context;
    second_context["messages"]
        .as_array_mut()
        .unwrap()
        .push(json!(first));
    second_context["messages"]
        .as_array_mut()
        .unwrap()
        .push(json!({"role":"user","content":"next","timestamp":8}));
    let mut next_runtime = rt.clone();
    if cancel_previous {
        rt.cancellation.cancel();
        next_runtime.cancellation = CancellationToken::new();
    }
    let second = wait(
        &e,
        api.stream(request(
            &endpoint,
            next_runtime.clone(),
            second_context,
            "auto",
        ))
        .result(),
    );
    assert_eq!(second.end_turn, Some(false));
    assert_eq!(second.usage.input.get(), 17);
    let stats = api
        .get_openai_codex_websocket_debug_stats("session")
        .unwrap();
    assert_eq!(stats.delta_requests.get(), 1);
    wait(
        &e,
        api.close_openai_codex_websocket_sessions(Some("session"), next_runtime),
    )
    .unwrap();
    assert_eq!(
        api.get_openai_codex_websocket_debug_stats("session")
            .unwrap()
            .requests
            .get(),
        2
    );
    server.join().unwrap();
}
struct TimerState {
    fired: AtomicBool,
    alive: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
struct GateSleep(Arc<TimerState>);
impl Future for GateSleep {
    type Output = Result<(), Cancelled>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.fired.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            let w = cx.waker().clone();
            let old = self.0.waker.lock().unwrap().replace(w);
            drop(old);
            Poll::Pending
        }
    }
}
impl Drop for GateSleep {
    fn drop(&mut self) {
        self.0.alive.store(false, Ordering::SeqCst);
        let old = self.0.waker.lock().unwrap().take();
        drop(old);
    }
}
#[derive(Default)]
struct GateClock {
    timers: Mutex<Vec<Arc<TimerState>>>,
}
impl Clock for GateClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        let state = Arc::new(TimerState {
            fired: AtomicBool::new(false),
            alive: AtomicBool::new(true),
            waker: Mutex::new(None),
        });
        self.timers.lock().unwrap().push(state.clone());
        Box::pin(GateSleep(state))
    }
}
impl GateClock {
    fn fire(&self) -> usize {
        let timers = self.timers.lock().unwrap().clone();
        let mut count = 0;
        for state in timers {
            if state.alive.load(Ordering::SeqCst) {
                count += 1;
                state.fired.store(true, Ordering::SeqCst);
                let waker = state.waker.lock().unwrap().take();
                if let Some(w) = waker {
                    w.wake();
                }
            }
        }
        count
    }
}
#[test]
fn native_headers_vs_whole_body_timeout_and_caller_cancel_have_distinct_custody() {
    for mode in ["present", "absent", "cancel"] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (release, receive) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let h = head(&mut s);
            let length = h["content-length"].parse::<usize>().unwrap();
            let mut request = vec![0; length];
            s.read_exact(&mut request).unwrap();
            assert_eq!(&request[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
            let initial=[json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m","role":"assistant","content":[]}}),json!({"type":"response.output_text.delta","output_index":0,"delta":"partial"})].into_iter().map(|v|format!("data: {v}\n\n")).collect::<String>();
            let tail=[json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m","role":"assistant","content":[{"type":"output_text","text":"partial"}]}}),json!({"type":"response.completed","response":{"id":"done","status":"completed","end_turn":true,"usage":{"input_tokens":17,"output_tokens":0,"total_tokens":17}}})].into_iter().map(|v|format!("data: {v}\n\n")).collect::<String>();
            s.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{initial}",initial.len()+tail.len()).as_bytes()).unwrap();
            if receive.recv_timeout(Duration::from_secs(3)).unwrap() {
                s.write_all(tail.as_bytes()).unwrap();
                half_close(&s);
            }
            let mut bytes = vec![];
            s.read_to_end(&mut bytes)
                .expect("physical body cancellation/release");
        });
        let e = engine();
        let clock = Arc::new(GateClock::default());
        let presence = if mode == "absent" {
            CodexSourceSignalPresence::Absent
        } else {
            CodexSourceSignalPresence::Present
        };
        let (api, rt) = adapter(&e, clock.clone(), presence);
        let mut stream = api.stream(request(
            &endpoint,
            rt.clone(),
            json!({"messages":[]}),
            "sse",
        ));
        loop {
            let event = wait(&e, stream.next()).unwrap();
            if matches!(event, AssistantMessageEvent::TextDelta { .. }) {
                break;
            }
        }
        if mode == "cancel" {
            rt.cancellation.cancel();
        } else {
            assert_eq!(clock.fire(), usize::from(mode == "absent"));
        }
        release.send(mode == "present").unwrap();
        let result = wait(&e, stream.result());
        assert_eq!(
            result.stop_reason,
            match mode {
                "present" => StopReason::Stop,
                "absent" => StopReason::Error,
                _ => StopReason::Aborted,
            }
        );
        assert!(matches!(&result.content[0],AssistantContent::Text(text)if text.text=="partial"));
        if mode == "present" {
            assert_eq!(result.end_turn, Some(true));
            assert_eq!(result.usage.input.get(), 17);
        }
        if mode != "cancel" {
            assert!(!rt.cancellation.is_cancelled());
        }
        server.join().unwrap();
    }
}
struct Heads(Arc<Mutex<Vec<u16>>>);
impl pi_ai_providers::api::runtime::ResponseMiddleware for Heads {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        head: &'a pi_ai_providers::api::runtime::AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(head.status);
            Ok(())
        })
    }
}
#[test]
fn native_upgrade_failure_falls_back_without_head_and_post_output_close_never_replays() {
    for after in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            if after {
                upgrade(&mut socket);
                assert_eq!(read_frame(&mut socket).0, 1);
                socket
                    .write_all(&frame(1, br#"{"type":"codex.rate_limits"}"#))
                    .unwrap();
                socket.write_all(&frame(8, &1009u16.to_be_bytes())).unwrap();
                half_close(&socket);
                assert_eq!(read_frame(&mut socket), (8, 1009u16.to_be_bytes().to_vec()));
                let mut bytes = vec![];
                socket.read_to_end(&mut bytes).unwrap();
            } else {
                let headers = head(&mut socket);
                assert!(headers.contains_key("upgrade"));
                socket.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\nRetry-After: 999\r\n\r\n").unwrap();
                drop(socket);
                let (mut socket, _) = listener.accept().unwrap();
                let headers = head(&mut socket);
                assert!(!headers.contains_key("upgrade"));
                let mut body = vec![0; headers["content-length"].parse().unwrap()];
                socket.read_exact(&mut body).unwrap();
                let sse = events("fallback")
                    .into_iter()
                    .map(|v| format!("data: {v}\n\n"))
                    .collect::<String>();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",sse.len()).as_bytes()).unwrap();
            }
        });
        let e = engine();
        let clock = Arc::new(TokioClock::new(e.handle().clone()));
        let rt = RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            clock.clone(),
            Arc::new(Spawner(e.handle().clone())),
        );
        let heads = Arc::new(Mutex::new(vec![]));
        let mut services = AdapterServices::new(Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(e.handle().clone())),
            Arc::new(Jitter),
        )));
        services.response = Some(Arc::new(Heads(heads.clone())));
        let api = OpenAICodexResponsesApi::new(CodexServices {
            adapter: services,
            websocket: Some(Arc::new(
                Node26WebSocketConnector::new(e.handle().clone()).unwrap(),
            )),
            request_ids: Arc::new(UuidV7Generator::new(clock, Arc::new(Entropy))),
            date_parser: Arc::new(HttpRetryDateParser),
            signal_presence: CodexSourceSignalPresence::Present,
            compression: None,
            limits: Default::default(),
        })
        .unwrap();
        let output = wait(
            &e,
            api.stream(request(&endpoint, rt, json!({"messages":[]}), "auto"))
                .result(),
        );
        assert_eq!(
            output.stop_reason,
            if after {
                StopReason::Error
            } else {
                StopReason::Stop
            }
        );
        assert_eq!(
            *heads.lock().unwrap(),
            if after { vec![] } else { vec![200] }
        );
        let stats = api
            .get_openai_codex_websocket_debug_stats("session")
            .unwrap();
        assert_eq!(stats.sse_fallbacks.get(), u64::from(!after));
        assert_eq!(stats.websocket_failures.get(), 1);
        if after {
            assert_eq!(
                serde_json::to_value(&output).unwrap()["diagnostics"][0]["error"]["code"],
                1009
            );
        }
        server.join().unwrap();
    }
}

#[test]
fn review_native_held_closes_bound_live_slots_and_recover_after_physical_eof() {
    use std::sync::atomic::AtomicUsize;
    struct ReapedSpawner {
        handle: tokio::runtime::Handle,
        next: AtomicUsize,
        done: mpsc::Sender<usize>,
    }
    impl TaskSpawner for ReapedSpawner {
        fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            let id = self.next.fetch_add(1, Ordering::SeqCst);
            let done = self.done.clone();
            self.handle.spawn(async move {
                task.await;
                let _ = done.send(id);
            });
            Ok(())
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (decision, decisions) = mpsc::channel();
    let (ack, acks) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut held = vec![];
        let mut retired = 0;
        for i in 0..8 {
            let (mut socket, _) = listener.accept().unwrap();
            upgrade(&mut socket);
            assert_eq!(read_frame(&mut socket).0, 1);
            for event in events(&format!("bounded-{i}")) {
                socket
                    .write_all(&frame(1, event.to_string().as_bytes()))
                    .unwrap();
            }
            if decisions.recv_timeout(Duration::from_secs(3)).unwrap() {
                held.push(socket);
            } else {
                let mut tail = vec![];
                socket
                    .read_to_end(&mut tail)
                    .expect("failed admission physically retires its socket");
                assert!(tail.is_empty());
                retired += 1;
            }
            ack.send(held.len()).unwrap();
        }
        for socket in &held {
            socket.set_nonblocking(true).unwrap();
            let mut byte = [0];
            match socket.peek(&mut byte) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                other => panic!("held unpolled close must still own live socket: {other:?}"),
            }
            socket.set_nonblocking(false).unwrap();
        }
        ack.send(held.len()).unwrap();
        released.recv_timeout(Duration::from_secs(3)).unwrap();
        for mut socket in held {
            let mut tail = vec![];
            socket
                .read_to_end(&mut tail)
                .expect("dropped close future must produce physical EOF");
            assert!(tail.is_empty());
            retired += 1;
        }
        assert_eq!(retired, 8);
        ack.send(retired).unwrap();
        let (mut socket, _) = listener.accept().unwrap();
        upgrade(&mut socket);
        assert_eq!(read_frame(&mut socket).0, 1);
        for event in events("recovery") {
            socket
                .write_all(&frame(1, event.to_string().as_bytes()))
                .unwrap();
        }
        let (op, body) = read_frame(&mut socket);
        assert_eq!(op, 8);
        assert_eq!(&body[..2], &1000u16.to_be_bytes());
        assert_eq!(&body[2..], b"debug_close");
        socket.write_all(&frame(8, &1000u16.to_be_bytes())).unwrap();
        half_close(&socket);
        let mut tail = vec![];
        socket
            .read_to_end(&mut tail)
            .expect("recovered graceful close reaches EOF");
        assert!(tail.is_empty());
    });
    let e = engine();
    let clock = Arc::new(TokioClock::new(e.handle().clone()));
    let (done, completed) = mpsc::channel();
    let spawner = Arc::new(ReapedSpawner {
        handle: e.handle().clone(),
        next: AtomicUsize::new(0),
        done,
    });
    let rt = RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        clock.clone(),
        spawner.clone(),
    );
    let api = OpenAICodexResponsesApi::new(CodexServices {
        adapter: AdapterServices::new(Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(e.handle().clone())),
            Arc::new(Jitter),
        ))),
        websocket: Some(Arc::new(
            Node26WebSocketConnector::new(e.handle().clone()).unwrap(),
        )),
        request_ids: Arc::new(UuidV7Generator::new(clock, Arc::new(Entropy))),
        date_parser: Arc::new(HttpRetryDateParser),
        signal_presence: CodexSourceSignalPresence::Present,
        compression: Some(ZstdRequestLimits {
            max_input_bytes: 1_048_576,
            max_output_bytes: 1_048_576,
        }),
        limits: CodexResourceLimits {
            max_sessions: 1,
            max_active_leases: 4,
            ..Default::default()
        },
    })
    .unwrap();
    let mut held = vec![];
    let mut rejected = 0;
    let mut peak_live = 0;
    for _ in 0..8 {
        let before = spawner.next.load(Ordering::SeqCst);
        let output = wait(
            &e,
            api.stream(request(
                &endpoint,
                rt.clone(),
                json!({"messages":[]}),
                "auto",
            ))
            .result(),
        );
        let success = output.stop_reason == StopReason::Stop;
        if !success {
            assert_eq!(output.stop_reason, StopReason::Error);
            rejected += 1;
        }
        held.push(api.close_openai_codex_websocket_sessions(None, rt.clone()));
        if success {
            assert_eq!(
                spawner.next.load(Ordering::SeqCst),
                before + 2,
                "one request worker plus one idle task"
            );
            let idle = before + 1;
            while completed.recv_timeout(Duration::from_secs(3)).unwrap() != idle {}
        }
        decision.send(success).unwrap();
        let live = acks.recv_timeout(Duration::from_secs(3)).unwrap();
        peak_live = peak_live.max(live);
    }
    let live = acks.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(live > 0);
    peak_live = peak_live.max(live);
    drop(held);
    release.send(()).unwrap();
    assert_eq!(acks.recv_timeout(Duration::from_secs(3)).unwrap(), 8);
    assert_eq!(
        wait(
            &e,
            api.stream(request(
                &endpoint,
                rt.clone(),
                json!({"messages":[]}),
                "auto"
            ))
            .result()
        )
        .stop_reason,
        StopReason::Stop
    );
    wait(
        &e,
        api.close_openai_codex_websocket_sessions(Some("session"), rt),
    )
    .unwrap();
    server.join().unwrap();
    eprintln!(
        "cap4 retained live peak={peak_live}, rejected={rejected}; eight EOFs and recovered graceful EOF"
    );
    assert!(
        peak_live <= 4,
        "per-retained-slot cap4 exceeded: {peak_live}"
    );
    assert!(rejected > 0);
}
