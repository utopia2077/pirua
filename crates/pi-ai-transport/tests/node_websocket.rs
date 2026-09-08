#![cfg(feature = "native")]
use base64::{Engine, engine::general_purpose::STANDARD};
use pi_ai_core::{
    BoxFuture, CancellationToken, RequestEndpoint, RequestRuntime, SpawnError, TaskSpawner,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

fn wire(op: u8, body: &[u8]) -> Vec<u8> {
    assert!(body.len() < 126);
    let mut v = vec![128 | op, body.len() as u8];
    v.extend_from_slice(body);
    v
}
fn read_application(socket: &mut impl Read) -> Vec<u8> {
    loop {
        let (op, compressed, body) = read_frame(socket);
        assert!(!compressed);
        if op == 1 {
            return body;
        }
        assert_eq!(op, 10);
    }
}
fn state(
    native: &tokio::runtime::Runtime,
    connection: &dyn PersistentWebSocket,
    wanted: WebSocketState,
) {
    wait(native, async {
        while connection.state() != wanted {
            tokio::task::yield_now().await;
        }
    })
}

#[test]
fn same_packet_tail_idle_dictionary_and_receiver_epochs_survive_wait_drop_and_old_cancel() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/node-websocket/trace.json"
    ))
    .unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == "context-no-takeover")
        .unwrap();
    let frames: Vec<Vec<u8>> = case["frames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| STANDARD.decode(v.as_str().unwrap()).unwrap())
        .collect();
    let expected = case["expected"][1]["text"].as_str().unwrap().to_owned();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/tail", listener.local_addr().unwrap());
    let (notice, noticed) = mpsc::channel();
    let (advance, advanced) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let head = read_head(&mut s);
        let mut tail = frames[0].clone();
        tail.extend(wire(9, b"idle-dictionary"));
        upgrade(
            &mut s,
            &head,
            Some("permessage-deflate; server_no_context_takeover"),
            &tail,
        );
        assert_eq!(read_frame(&mut s), (10, false, b"idle-dictionary".to_vec()));
        notice.send(()).unwrap();
        assert_eq!(read_application(&mut s), b"first");
        for frame in &frames[1..frames.len() - 1] {
            s.write_all(frame).unwrap();
        }
        advanced.recv_timeout(Duration::from_secs(3)).unwrap();
        s.write_all(&wire(1, b"after-wait-drop")).unwrap();
        advanced.recv_timeout(Duration::from_secs(3)).unwrap();
        s.write_all(&[wire(1, b"idle-poison"), wire(9, b"epoch-barrier")].concat())
            .unwrap();
        loop {
            let (op, _, body) = read_frame(&mut s);
            assert_eq!(op, 10);
            if body == b"epoch-barrier" {
                break;
            }
        }
        notice.send(()).unwrap();
        assert_eq!(read_application(&mut s), b"second");
        s.write_all(&[wire(1, b"fresh"), wire(8, &[3, 232])].concat())
            .unwrap();
        assert_eq!(read_frame(&mut s), (8, false, vec![3, 232]));
        s.shutdown(Shutdown::Write).unwrap();
    });
    let native = native();
    let rt = runtime(&native);
    let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
    let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
    noticed.recv_timeout(Duration::from_secs(3)).unwrap();
    let mut first = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("first".into()), &rt),
    )
    .unwrap();
    assert_eq!(
        wait(&native, first.next(&rt, None)).unwrap(),
        Some(WebSocketEvent::Message(WebSocketMessage::Text(expected)))
    );
    let mut pending = first.next(&rt, None);
    assert!(
        pending
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    drop(pending);
    assert_eq!(connection.state(), WebSocketState::Open);
    advance.send(()).unwrap();
    assert_eq!(
        event(wait(&native, first.next(&rt, None)).unwrap().unwrap()),
        json!({"type":"message","text":"after-wait-drop"})
    );
    drop(first);
    rt.cancellation.cancel();
    advance.send(()).unwrap();
    noticed.recv_timeout(Duration::from_secs(3)).unwrap();
    let new_rt = runtime(&native);
    let mut second = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("second".into()), &new_rt),
    )
    .unwrap();
    assert_eq!(
        event(wait(&native, second.next(&new_rt, None)).unwrap().unwrap()),
        json!({"type":"message","text":"fresh"})
    );
    assert_eq!(
        event(wait(&native, second.next(&new_rt, None)).unwrap().unwrap()),
        json!({"type":"close","code":1000,"reason":""})
    );
    assert!(wait(&native, second.next(&new_rt, None)).unwrap().is_none());
    assert_eq!(connection.state(), WebSocketState::Closed);
    server.join().unwrap();
}

#[test]
fn admitted_queue_prefix_precedes_limit_failure_and_closed_is_observed_without_next() {
    for by_bytes in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/limit", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let head = read_head(&mut s);
            upgrade(&mut s, &head, None, &[]);
            read_application(&mut s);
            s.write_all(&[wire(1, b"aa"), wire(1, b"bb"), wire(1, b"cc")].concat())
                .unwrap();
            let mut discarded = vec![];
            let _ = s.read_to_end(&mut discarded);
        });
        let native = native();
        let rt = runtime(&native);
        let limits = if by_bytes {
            NodeWebSocketLimits {
                max_queue_bytes: 4,
                ..Default::default()
            }
        } else {
            NodeWebSocketLimits {
                max_queue_messages: 2,
                ..Default::default()
            }
        };
        let connector = Node26WebSocketConnector::new(native.handle().clone())
            .unwrap()
            .with_limits(limits)
            .unwrap();
        let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
        let mut exchange = wait(
            &native,
            connection.start_exchange(WebSocketMessage::Text("start".into()), &rt),
        )
        .unwrap();
        state(&native, &*connection, WebSocketState::Closed);
        for expected in ["aa", "bb"] {
            assert_eq!(
                event(wait(&native, exchange.next(&rt, None)).unwrap().unwrap()),
                json!({"type":"message","text":expected})
            );
        }
        assert_eq!(
            wait(&native, exchange.next(&rt, None)).unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert!(wait(&native, exchange.next(&rt, None)).unwrap().is_none());
        server.join().unwrap();
    }
}

#[test]
fn graceful_shutdown_flushes_exact_frame_and_waits_for_peer_or_explicit_grace() {
    for mode in ["complete", "expiry", "zero", "cancel"] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/shutdown", listener.local_addr().unwrap());
        let (notice, noticed) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let head = read_head(&mut s);
            upgrade(&mut s, &head, None, &[]);
            read_application(&mut s);
            s.write_all(&wire(1, b"ready")).unwrap();
            if mode != "zero" {
                assert_eq!(
                    read_frame(&mut s),
                    (8, false, [vec![3, 232], b"debug_close".to_vec()].concat())
                );
                notice.send(()).unwrap();
            }
            if mode == "complete" {
                s.write_all(&wire(8, &[3, 232])).unwrap();
                s.shutdown(Shutdown::Write).unwrap();
            }
            let mut tail = vec![];
            s.read_to_end(&mut tail).unwrap();
            assert!(tail.is_empty());
        });
        let native = native();
        let rt = runtime(&native);
        let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
        let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
        let mut exchange = wait(
            &native,
            connection.start_exchange(WebSocketMessage::Text("start".into()), &rt),
        )
        .unwrap();
        wait(&native, exchange.next(&rt, None)).unwrap();
        drop(exchange);
        assert_eq!(
            wait(
                &native,
                connection.shutdown(1005, String::new(), &rt, Duration::from_secs(1))
            )
            .unwrap_err()
            .kind(),
            TransportErrorKind::InvalidRequest
        );
        assert_eq!(connection.state(), WebSocketState::Open);
        let grace = if mode == "zero" {
            Duration::ZERO
        } else {
            Duration::from_millis(100)
        };
        let mut shutdown = connection.shutdown(1000, "debug_close".into(), &rt, grace);
        if mode == "cancel" {
            assert!(
                shutdown
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            noticed.recv_timeout(Duration::from_secs(2)).unwrap();
            rt.cancellation.cancel();
        }
        let result = wait(&native, shutdown);
        if mode == "complete" {
            result.unwrap();
        } else {
            assert_eq!(
                result.unwrap_err().kind(),
                if mode == "cancel" {
                    TransportErrorKind::Cancelled
                } else {
                    TransportErrorKind::Timeout
                }
            );
        }
        assert_eq!(connection.state(), WebSocketState::Closed);
        server.join().unwrap();
    }
}

#[test]
fn peer_close_changes_idle_state_then_waits_under_finite_peer_grace() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/peer", listener.local_addr().unwrap());
    let (notice, noticed) = mpsc::channel();
    let (go, going) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let head = read_head(&mut s);
        upgrade(&mut s, &head, None, &[]);
        read_application(&mut s);
        s.write_all(&wire(1, b"done")).unwrap();
        going.recv_timeout(Duration::from_secs(2)).unwrap();
        s.write_all(&wire(8, &[3, 232])).unwrap();
        assert_eq!(read_frame(&mut s), (8, false, vec![3, 232]));
        notice.send(()).unwrap();
        let mut rest = vec![];
        s.read_to_end(&mut rest).unwrap();
    });
    let native = native();
    let clock = Arc::new(ManualClock::default());
    let mut rt = runtime(&native);
    rt.clock = clock.clone();
    let connector = Node26WebSocketConnector::new(native.handle().clone())
        .unwrap()
        .with_limits(NodeWebSocketLimits {
            peer_close_grace: Duration::from_millis(100),
            ..Default::default()
        })
        .unwrap();
    let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
    let mut exchange = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("start".into()), &rt),
    )
    .unwrap();
    wait(&native, exchange.next(&rt, None)).unwrap();
    drop(exchange);
    go.send(()).unwrap();
    noticed.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(connection.state(), WebSocketState::Closing);
    clock.expire();
    state(&native, &*connection, WebSocketState::Closed);
    server.join().unwrap();
}

#[derive(Default)]
struct ManualClock {
    expired: std::sync::atomic::AtomicBool,
    calls: std::sync::atomic::AtomicUsize,
    waiter: std::sync::Mutex<Option<std::task::Waker>>,
}
impl ManualClock {
    fn expire(&self) {
        self.expired
            .store(true, std::sync::atomic::Ordering::Release);
        let wake = self.waiter.lock().unwrap().take();
        if let Some(w) = wake {
            w.wake();
        }
    }
}
impl pi_ai_core::Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), pi_ai_core::Cancelled>> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(std::future::poll_fn(|cx| {
            if self.expired.load(std::sync::atomic::Ordering::Acquire) {
                return std::task::Poll::Ready(Ok(()));
            }
            let new = cx.waker().clone();
            let old = self.waiter.lock().unwrap().replace(new);
            drop(old);
            std::task::Poll::Pending
        }))
    }
}

#[test]
fn ping_only_does_not_wake_application_or_restart_injected_idle_timer() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ping", listener.local_addr().unwrap());
    let (go, going) = mpsc::channel();
    let (notice, noticed) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let head = read_head(&mut s);
        upgrade(&mut s, &head, None, &[]);
        read_application(&mut s);
        going.recv_timeout(Duration::from_secs(3)).unwrap();
        for n in 0..8 {
            s.write_all(&wire(9, &[n])).unwrap();
            assert_eq!(read_frame(&mut s), (10, false, vec![n]));
        }
        notice.send(()).unwrap();
        let mut b = vec![];
        s.read_to_end(&mut b).unwrap();
        assert!(b.is_empty());
    });
    let native = native();
    let rt = runtime(&native);
    let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
    let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
    let mut exchange = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("start".into()), &rt),
    )
    .unwrap();
    let clock = Arc::new(ManualClock::default());
    let mut waiting = runtime(&native);
    waiting.clock = clock.clone();
    let mut next = exchange.next(&waiting, Some(Duration::from_secs(99)));
    assert!(
        next.as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    go.send(()).unwrap();
    noticed.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(
        next.as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    assert_eq!(clock.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    clock.expire();
    assert_eq!(
        wait(&native, next).unwrap_err().kind(),
        TransportErrorKind::Timeout
    );
    assert_eq!(connection.state(), WebSocketState::Closed);
    server.join().unwrap();
}

#[test]
fn blocked_handshake_and_active_receive_cancel_release_real_sockets() {
    for handshake in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/cancel", listener.local_addr().unwrap());
        let (notice, noticed) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let head = read_head(&mut s);
            if !handshake {
                upgrade(&mut s, &head, None, &[]);
                read_application(&mut s);
            }
            notice.send(()).unwrap();
            let mut b = vec![];
            s.read_to_end(&mut b).unwrap();
            assert!(b.is_empty());
        });
        let native = native();
        let rt = runtime(&native);
        let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
        let req = request(endpoint);
        if handshake {
            let mut connecting = connector.connect(&req, &rt);
            assert!(
                connecting
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            noticed.recv_timeout(Duration::from_secs(3)).unwrap();
            rt.cancellation.cancel();
            assert!(
                matches!(wait(&native,connecting),Err(e)if e.kind()==TransportErrorKind::Cancelled)
            );
        } else {
            let mut connection = wait(&native, connector.connect(&req, &rt)).unwrap();
            let mut exchange = wait(
                &native,
                connection.start_exchange(WebSocketMessage::Text("start".into()), &rt),
            )
            .unwrap();
            noticed.recv_timeout(Duration::from_secs(3)).unwrap();
            let mut next = exchange.next(&rt, None);
            assert!(
                next.as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            rt.cancellation.cancel();
            assert_eq!(
                wait(&native, next).unwrap_err().kind(),
                TransportErrorKind::Cancelled
            );
            assert_eq!(connection.state(), WebSocketState::Closed);
        }
        server.join().unwrap();
    }
}

#[test]
fn abandoned_send_admission_and_runtime_loss_retire_without_an_orphan_task() {
    for runtime_loss in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/drop", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let head = read_head(&mut s);
            upgrade(&mut s, &head, None, &[]);
            let mut b = vec![];
            s.read_to_end(&mut b).unwrap();
            assert!(b.is_empty());
        });
        // The explicitly owned current-thread runtime does not poll its driver
        // after connect returns until the test chooses to drive it again.
        let native = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let rt = runtime(&native);
        let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
        let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
        if runtime_loss {
            drop(native);
            assert_eq!(connection.state(), WebSocketState::Closed);
        } else {
            let mut sending =
                connection.start_exchange(WebSocketMessage::Text("unsent".into()), &rt);
            assert!(
                sending
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            drop(sending);
            assert_eq!(connection.state(), WebSocketState::Closed);
            native.block_on(tokio::task::yield_now());
        }
        drop(connection);
        server.join().unwrap();
    }
}

struct Reenter {
    connection: Arc<std::sync::Mutex<Box<dyn PersistentWebSocket>>>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}
impl std::task::Wake for Reenter {
    fn wake(self: Arc<Self>) {
        let _ = self.connection.lock().unwrap().state();
    }
}
impl Drop for Reenter {
    fn drop(&mut self) {
        let _ = self.connection.lock().unwrap().state();
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }
}

#[test]
fn dropping_pending_next_releases_reentrant_waker_outside_shared_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/waker", listener.local_addr().unwrap());
    let (notice, noticed) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let head = read_head(&mut s);
        upgrade(&mut s, &head, None, &[]);
        read_application(&mut s);
        notice.send(()).unwrap();
        let mut b = vec![];
        s.read_to_end(&mut b).unwrap();
    });
    let native = native();
    let rt = runtime(&native);
    let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
    let connection = Arc::new(std::sync::Mutex::new(
        wait(&native, connector.connect(&request(endpoint), &rt)).unwrap(),
    ));
    let mut exchange = wait(
        &native,
        connection
            .lock()
            .unwrap()
            .start_exchange(WebSocketMessage::Text("start".into()), &rt),
    )
    .unwrap();
    noticed.recv_timeout(Duration::from_secs(2)).unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker = std::task::Waker::from(Arc::new(Reenter {
        connection: connection.clone(),
        dropped: dropped.clone(),
    }));
    let mut next = exchange.next(&rt, None);
    assert!(
        next.as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(next);
    assert_eq!(dropped.load(std::sync::atomic::Ordering::Acquire), 1);
    assert_eq!(connection.lock().unwrap().state(), WebSocketState::Open);
    connection.lock().unwrap().abort();
    server.join().unwrap();
}

#[test]
fn stalled_application_write_does_not_starve_receive_or_shutdown_deadline() {
    const SIZE: usize = 20 * 1024 * 1024;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/duplex", listener.local_addr().unwrap());
    let (go, going) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let head = read_head(&mut s);
        upgrade(&mut s, &head, None, &[]);
        let mut header = [0; 14];
        s.read_exact(&mut header).unwrap();
        assert_eq!(&header[..2], &[129, 255]);
        assert_eq!(
            u64::from_be_bytes(header[2..10].try_into().unwrap()),
            SIZE as u64
        );
        s.write_all(&wire(1, b"while-write-stalled")).unwrap();
        going.recv_timeout(Duration::from_secs(3)).unwrap();
        let mut bytes = vec![];
        s.read_to_end(&mut bytes).unwrap();
        assert!(
            bytes.len() < SIZE,
            "peer did not drain while write was stalled"
        );
    });
    let native = native();
    let rt = runtime(&native);
    let limits = NodeWebSocketLimits {
        max_pending_command_bytes: SIZE + 14,
        max_outbound_bytes: SIZE + 14,
        ..Default::default()
    };
    let connector = Node26WebSocketConnector::new(native.handle().clone())
        .unwrap()
        .with_limits(limits)
        .unwrap();
    let mut req = request(endpoint);
    req.max_message_bytes = SIZE;
    let mut connection = wait(&native, connector.connect(&req, &rt)).unwrap();
    let mut exchange = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("x".repeat(SIZE)), &rt),
    )
    .unwrap();
    assert!(
        matches!(wait(&native,connection.start_exchange(WebSocketMessage::Text("overlap".into()),&rt)),Err(e)if e.kind()==TransportErrorKind::InvalidRequest)
    );
    assert_eq!(
        event(
            wait(&native, exchange.next(&rt, Some(Duration::from_secs(2))))
                .unwrap()
                .unwrap()
        ),
        json!({"type":"message","text":"while-write-stalled"})
    );
    assert_eq!(
        wait(
            &native,
            connection.shutdown(1000, "stop".into(), &rt, Duration::from_millis(20))
        )
        .unwrap_err()
        .kind(),
        TransportErrorKind::Timeout
    );
    go.send(()).unwrap();
    server.join().unwrap();
}

struct NoSpawn;
impl TaskSpawner for NoSpawn {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
fn native() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
fn runtime(native: &tokio::runtime::Runtime) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        Arc::new(TokioClock::new(native.handle().clone())),
        Arc::new(NoSpawn),
    )
}
fn wait<T>(native: &tokio::runtime::Runtime, future: impl Future<Output = T>) -> T {
    native.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), future)
            .await
            .expect("bounded test")
    })
}
fn request(endpoint: String) -> WebSocketRequest {
    let mut headers = HttpHeaders::new();
    headers.set("x-synthetic", "value").unwrap();
    WebSocketRequest {
        endpoint: RequestEndpoint::new_resolved(endpoint).unwrap(),
        headers,
        proxy: None,
        connect_timeout: Some(Duration::from_secs(2)),
        max_message_bytes: 65536,
        max_frame_bytes: 65536,
    }
}
fn read_head(socket: &mut TcpStream) -> BTreeMap<String, String> {
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    generic_head(socket)
}
fn generic_head(socket: &mut impl Read) -> BTreeMap<String, String> {
    let mut bytes = vec![];
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        socket.read_exact(&mut b).unwrap();
        bytes.push(b[0]);
        assert!(bytes.len() < 65536);
    }
    let text = String::from_utf8(bytes).unwrap();
    text.split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_lowercase(), v.to_owned()))
        .collect()
}
fn upgrade(
    socket: &mut impl Write,
    head: &BTreeMap<String, String>,
    extension: Option<&str>,
    tail: &[u8],
) {
    upgrade_protocol(socket, head, extension, tail, None)
}
fn upgrade_protocol(
    socket: &mut impl Write,
    head: &BTreeMap<String, String>,
    extension: Option<&str>,
    tail: &[u8],
    protocol: Option<&str>,
) {
    let accept = derive_accept_key(head["sec-websocket-key"].as_bytes());
    let mut bytes=format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n{}{}\r\n",extension.map(|v|format!("Sec-WebSocket-Extensions: {v}\r\n")).unwrap_or_default(),protocol.map(|v|format!("Sec-WebSocket-Protocol: {v}\r\n")).unwrap_or_default()).into_bytes();
    bytes.extend_from_slice(tail);
    socket.write_all(&bytes).unwrap();
    socket.flush().unwrap();
}
fn read_frame(socket: &mut impl Read) -> (u8, bool, Vec<u8>) {
    let mut head = [0; 2];
    socket.read_exact(&mut head).unwrap();
    let mut len = usize::from(head[1] & 127);
    if len == 126 {
        let mut b = [0; 2];
        socket.read_exact(&mut b).unwrap();
        len = u16::from_be_bytes(b) as usize;
    } else if len == 127 {
        let mut b = [0; 8];
        socket.read_exact(&mut b).unwrap();
        len = u64::from_be_bytes(b) as usize;
    }
    assert!(head[1] & 128 != 0, "client messages must be masked");
    let mut mask = [0; 4];
    socket.read_exact(&mut mask).unwrap();
    let mut body = vec![0; len];
    socket.read_exact(&mut body).unwrap();
    for (i, b) in body.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    (head[0] & 15, head[0] & 64 != 0, body)
}
fn event(event: WebSocketEvent) -> Value {
    match event {
        WebSocketEvent::Message(WebSocketMessage::Text(s)) => json!({"type":"message","text":s}),
        WebSocketEvent::Message(WebSocketMessage::Binary(b)) => {
            json!({"type":"message","binary":b})
        }
        WebSocketEvent::Close { code, reason } => {
            json!({"type":"close","code":code,"reason":reason})
        }
    }
}

#[test]
fn explicit_tls_and_proxy_paths_use_the_same_node_profile() {
    use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
    let key = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let cert = key.cert.der().clone();
    let server_tls = Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.clone()],
            PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
        )
        .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let client_tls = Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("wss://{}/secure", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_tls).unwrap(), s);
        let head = generic_head(&mut tls);
        assert_eq!(head["accept-encoding"], "br, gzip, deflate, zstd");
        upgrade(&mut tls, &head, None, &[]);
        assert_eq!(read_application(&mut tls), b"secure");
        tls.write_all(&wire(1, b"tls-message")).unwrap();
        tls.flush().unwrap();
        assert_eq!(read_frame(&mut tls), (8, false, vec![3, 232]));
        tls.write_all(&wire(8, &[3, 232])).unwrap();
        tls.conn.send_close_notify();
        tls.flush().unwrap();
        tls.sock.shutdown(Shutdown::Write).unwrap();
    });
    let native = native();
    let rt = runtime(&native);
    let connector = Node26WebSocketConnector::with_tls_config(native.handle().clone(), client_tls);
    let mut connection = wait(&native, connector.connect(&request(endpoint), &rt)).unwrap();
    let mut exchange = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("secure".into()), &rt),
    )
    .unwrap();
    assert_eq!(
        event(wait(&native, exchange.next(&rt, None)).unwrap().unwrap()),
        json!({"type":"message","text":"tls-message"})
    );
    drop(exchange);
    wait(
        &native,
        connection.shutdown(1000, String::new(), &rt, Duration::from_secs(2)),
    )
    .unwrap();
    server.join().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let connect = read_head(&mut s);
        assert_eq!(connect["proxy-authorization"], "Basic c3ludGhldGljOnByb3h5");
        s.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .unwrap();
        let head = read_head(&mut s);
        assert_eq!(head["host"], "origin.invalid");
        assert!(!head.contains_key("proxy-authorization"));
        upgrade(&mut s, &head, None, &[]);
        assert_eq!(read_application(&mut s), b"proxy");
        s.write_all(&[wire(1, b"proxy-message"), wire(8, &[3, 232])].concat())
            .unwrap();
        assert_eq!(read_frame(&mut s), (8, false, vec![3, 232]));
        s.shutdown(Shutdown::Write).unwrap();
    });
    let mut req = request("ws://origin.invalid/proxy".into());
    req.proxy = Some(ProxyUrl::parse(&format!("http://synthetic:proxy@{address}")).unwrap());
    let mut connection = wait(&native, connector.connect(&req, &rt)).unwrap();
    let mut exchange = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("proxy".into()), &rt),
    )
    .unwrap();
    assert_eq!(
        event(wait(&native, exchange.next(&rt, None)).unwrap().unwrap()),
        json!({"type":"message","text":"proxy-message"})
    );
    assert!(matches!(
        wait(&native, exchange.next(&rt, None)).unwrap(),
        Some(WebSocketEvent::Close { code: 1000, .. })
    ));
    server.join().unwrap();
}

#[test]
fn handshake_resource_admission_and_source_fatal_protocol_fail_closed() {
    let native = native();
    let rt = runtime(&native);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut req = request(format!("ws://{}/limit", listener.local_addr().unwrap()));
    req.headers
        .set("x-private", "private-sentinel".repeat(100))
        .unwrap();
    let connector = Node26WebSocketConnector::new(native.handle().clone())
        .unwrap()
        .with_limits(NodeWebSocketLimits {
            max_handshake_bytes: 512,
            ..Default::default()
        })
        .unwrap();
    let error = match wait(&native, connector.connect(&req, &rt)) {
        Err(error) => error,
        Ok(_) => panic!("oversize input admitted"),
    };
    assert_eq!(error.kind(), TransportErrorKind::LimitExceeded);
    assert!(!format!("{error:?}").contains("private-sentinel"));
    assert!(matches!(listener.accept(),Err(e)if e.kind()==std::io::ErrorKind::WouldBlock));
    for excessive in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/head", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let head = read_head(&mut s);
            if excessive {
                let interim = format!(
                    "HTTP/1.1 103 Early Hints\r\nX-Padding: {}\r\n\r\n",
                    "a".repeat(250)
                );
                s.write_all(interim.repeat(2).as_bytes()).unwrap();
            } else {
                upgrade_protocol(&mut s, &head, None, &[], Some("unrequested"));
            }
            let mut b = vec![];
            s.read_to_end(&mut b).unwrap();
            assert!(b.is_empty());
        });
        let result = wait(&native, connector.connect(&request(endpoint), &rt));
        // Actual Node bundled connection15608 throws uncaught TypeError for
        // absent offered protocols. Controller-approved source-crash normalization
        // is typed Protocol + release, never process crash or a fabricated source
        // error/close golden. See oracle.mjs --unrequested-protocol (exit1).
        assert!(
            matches!(result,Err(e)if e.kind()==if excessive{TransportErrorKind::LimitExceeded}else{TransportErrorKind::Protocol})
        );
        server.join().unwrap();
    }
    assert!(
        !format!("{:?}", WebSocketMessage::Text("private-sentinel".into()))
            .contains("private-sentinel")
    );
    assert!(
        !format!(
            "{:?}",
            WebSocketEvent::Close {
                code: 1000,
                reason: "private-sentinel".into()
            }
        )
        .contains("private-sentinel")
    );
}

#[test]
fn exact_outgoing_budget_rejects_without_io_and_long_sessions_do_not_accumulate_length() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/long", listener.local_addr().unwrap());
    let (go, going) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_nodelay(true).unwrap();
        let head = read_head(&mut s);
        upgrade(&mut s, &head, None, &[]);
        assert_eq!(read_application(&mut s), b"1234");
        for _ in 0..100 {
            going.recv_timeout(Duration::from_secs(3)).unwrap();
            s.write_all(&wire(1, b"data")).unwrap();
        }
        s.write_all(&wire(8, &[3, 232])).unwrap();
        assert_eq!(read_frame(&mut s), (8, false, vec![3, 232]));
        s.shutdown(Shutdown::Write).unwrap();
    });
    let native = native();
    let rt = runtime(&native);
    let connector = Node26WebSocketConnector::new(native.handle().clone())
        .unwrap()
        .with_limits(NodeWebSocketLimits {
            max_pending_command_bytes: 10,
            max_outbound_bytes: 10,
            max_queue_bytes: 4,
            max_queue_messages: 1,
            ..Default::default()
        })
        .unwrap();
    let mut req = request(endpoint);
    req.max_message_bytes = 4;
    let mut connection = wait(&native, connector.connect(&req, &rt)).unwrap();
    assert!(
        matches!(wait(&native,connection.start_exchange(WebSocketMessage::Text("12345".into()),&rt)),Err(e)if e.kind()==TransportErrorKind::LimitExceeded)
    );
    assert_eq!(connection.state(), WebSocketState::Open);
    let mut exchange = wait(
        &native,
        connection.start_exchange(WebSocketMessage::Text("1234".into()), &rt),
    )
    .unwrap();
    for _ in 0..100 {
        go.send(()).unwrap();
        assert_eq!(
            event(
                wait(&native, exchange.next(&rt, Some(Duration::ZERO)))
                    .unwrap()
                    .unwrap()
            ),
            json!({"type":"message","text":"data"})
        );
    }
    assert!(matches!(
        wait(&native, exchange.next(&rt, None)).unwrap(),
        Some(WebSocketEvent::Close { code: 1000, .. })
    ));
    server.join().unwrap();
}

#[test]
fn actual_node_profile_frames_headers_and_errors_match_native_loopbacks() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/node-websocket/trace.json"
    ))
    .unwrap();
    let native = native();
    let rt = runtime(&native);
    for case in fixture["cases"].as_array().unwrap() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/profile", listener.local_addr().unwrap());
        let case_server = case.clone();
        let (sent, received) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut head = read_head(&mut socket);
            if case_server["informational"] == true {
                socket
                    .write_all(b"HTTP/1.1 103 Early Hints\r\nLink: </safe>\r\n\r\n")
                    .unwrap();
            }
            upgrade_protocol(
                &mut socket,
                &head,
                case_server["extension"].as_str(),
                &[],
                case_server["responseProtocol"].as_str(),
            );
            head.remove("host");
            head.remove("sec-websocket-key");
            sent.send(head).unwrap();
            if !case_server["opened"].as_bool().unwrap() {
                return;
            }
            assert_eq!(read_frame(&mut socket), (1, false, b"request".to_vec()));
            for bytes in case_server["frames"].as_array().unwrap() {
                socket
                    .write_all(&STANDARD.decode(bytes.as_str().unwrap()).unwrap())
                    .unwrap();
            }
            if case_server["expected"]
                .as_array()
                .unwrap()
                .last()
                .is_some_and(|e| e["type"] == "close")
            {
                loop {
                    let (opcode, compressed, body) = read_frame(&mut socket);
                    assert!(!compressed);
                    if opcode == 8 {
                        let code =
                            case_server["expected"].as_array().unwrap().last().unwrap()["code"]
                                .as_u64()
                                .unwrap() as u16;
                        assert_eq!(
                            body,
                            if code == 1005 {
                                vec![]
                            } else {
                                code.to_be_bytes().to_vec()
                            }
                        );
                        break;
                    }
                }
            }
            profile_peer_shutdown(socket.shutdown(Shutdown::Write)).unwrap();
            let mut replies = Vec::new();
            match socket.read_to_end(&mut replies) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                Err(error) => panic!("profile peer did not physically close: {error}"),
            }
        });
        let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
        let mut req = request(endpoint);
        if let Some(headers) = case["inputHeaders"].as_object() {
            for (name, value) in headers {
                req.headers.set(name, value.as_str().unwrap()).unwrap();
            }
        }
        let connected = wait(&native, connector.connect(&req, &rt));
        let mut observed = Vec::new();
        match connected {
            Err(_) => observed.push(json!({"type":"error"})),
            Ok(mut connection) => {
                let mut exchange = wait(
                    &native,
                    connection.start_exchange(WebSocketMessage::Text("request".into()), &rt),
                )
                .unwrap();
                loop {
                    match wait(&native, exchange.next(&rt, Some(Duration::from_secs(2)))) {
                        Ok(Some(value)) => observed.push(event(value)),
                        Ok(None) => break,
                        Err(_) => {
                            observed.push(json!({"type":"error"}));
                            break;
                        }
                    }
                }
            }
        }
        server.join().unwrap();
        assert_eq!(
            json!(received.recv().unwrap()),
            case["requestHeaders"],
            "{} headers",
            case["id"]
        );
        assert_eq!(json!(observed), case["expected"], "{} events", case["id"]);
    }
}

#[test]
fn info_stage_rejects_before_extended_length_and_preserves_source_controls() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/node-websocket/info-stage.json"
    ))
    .unwrap();
    let native = native();
    let rt = runtime(&native);
    for case in fixture["cases"].as_array().unwrap() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}/profile", listener.local_addr().unwrap());
        let case_server = case.clone();
        let (sent, received) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut head = read_head(&mut socket);
            if case_server["informational"] == true {
                socket
                    .write_all(b"HTTP/1.1 103 Early Hints\r\nLink: </safe>\r\n\r\n")
                    .unwrap();
            }
            upgrade_protocol(
                &mut socket,
                &head,
                case_server["extension"].as_str(),
                &[],
                case_server["responseProtocol"].as_str(),
            );
            head.remove("host");
            head.remove("sec-websocket-key");
            sent.send(head).unwrap();
            if !case_server["opened"].as_bool().unwrap() {
                return;
            }
            assert_eq!(read_frame(&mut socket), (1, false, b"request".to_vec()));
            let bytes: Vec<u8> = case_server["frames"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|v| STANDARD.decode(v.as_str().unwrap()).unwrap())
                .collect();
            socket.write_all(&bytes).unwrap();
            if case_server["expected"]
                .as_array()
                .unwrap()
                .last()
                .is_some_and(|e| e["type"] == "close")
            {
                loop {
                    let (opcode, compressed, body) = read_frame(&mut socket);
                    assert!(!compressed);
                    if opcode == 8 {
                        let code =
                            case_server["expected"].as_array().unwrap().last().unwrap()["code"]
                                .as_u64()
                                .unwrap() as u16;
                        assert_eq!(
                            body,
                            if code == 1005 {
                                vec![]
                            } else {
                                code.to_be_bytes().to_vec()
                            }
                        );
                        break;
                    }
                }
            }
            if case_server["expected"].as_array().unwrap().last().unwrap()["type"] == "close" {
                socket.shutdown(Shutdown::Write).unwrap();
            }
            // For INFO failures withhold every remaining byte and physical EOF.
            // Only the native Protocol retirement releases this peer.

            let mut replies = Vec::new();
            socket
                .read_to_end(&mut replies)
                .expect("native must release peer before test watchdog");
        });
        let connector = Node26WebSocketConnector::new(native.handle().clone()).unwrap();
        let mut req = request(endpoint);
        if let Some(headers) = case["inputHeaders"].as_object() {
            for (name, value) in headers {
                req.headers.set(name, value.as_str().unwrap()).unwrap();
            }
        }
        let connected = wait(&native, connector.connect(&req, &rt));
        let mut observed = Vec::new();
        match connected {
            Err(_) => observed.push(json!({"type":"error"})),
            Ok(mut connection) => {
                let mut exchange = wait(
                    &native,
                    connection.start_exchange(WebSocketMessage::Text("request".into()), &rt),
                )
                .unwrap();
                loop {
                    match wait(&native, exchange.next(&rt, None)) {
                        Ok(Some(value)) => observed.push(event(value)),
                        Ok(None) => break,
                        Err(error) => {
                            assert_eq!(
                                error.kind(),
                                TransportErrorKind::Protocol,
                                "{}",
                                case["id"]
                            );
                            assert_eq!(connection.state(), WebSocketState::Closed);
                            assert!(wait(&native, exchange.next(&rt, None)).unwrap().is_none());
                            observed.push(json!({"type":"error"}));
                            break;
                        }
                    }
                }
            }
        }
        server.join().unwrap();
        assert_eq!(
            json!(received.recv().unwrap()),
            case["requestHeaders"],
            "{} headers",
            case["id"]
        );
        assert_eq!(json!(observed), case["expected"], "{} events", case["id"]);
    }
}

// Source-fatal input can make the native peer close before this server's cleanup.
// Keep the shutdown classification separate so unexpected failures remain errors.
fn profile_peer_shutdown(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => Ok(()),
        other => other,
    }
}

#[cfg(target_os = "linux")]
#[test]
fn profile_cleanup_handles_peer_closure_before_local_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, received) = mpsc::channel();
    let (closed, closure) = mpsc::channel();
    let peer = thread::spawn(move || {
        let socket = TcpStream::connect(address).unwrap();
        received.recv_timeout(Duration::from_secs(3)).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        assert_eq!(socket.peek(&mut [0; 1]).unwrap(), 1);
        // Deliberately leave the server's data unread, as can happen when a
        // receiver retires at an earlier protocol error.
        drop(socket);
        closed.send(()).unwrap();
    });
    let (mut socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket.write_all(b"unread-after-protocol-failure").unwrap();
    sent.send(()).unwrap();
    closure.recv_timeout(Duration::from_secs(3)).unwrap();
    let mut bytes = Vec::new();
    let reset = socket.read_to_end(&mut bytes).unwrap_err();
    assert_eq!(reset.kind(), std::io::ErrorKind::ConnectionReset);
    let shutdown = socket.shutdown(Shutdown::Write);
    assert_eq!(
        shutdown.as_ref().unwrap_err().kind(),
        std::io::ErrorKind::NotConnected
    );
    eprintln!("selected peer-close-first: reset={reset:?}, shutdown={shutdown:?}");
    profile_peer_shutdown(shutdown).unwrap();
    // Consumed reset followed by EOF proves physical release, not a read timeout.
    assert_eq!(socket.read_to_end(&mut bytes).unwrap(), 0);
    peer.join().unwrap();
    profile_peer_shutdown(Ok(())).unwrap();
    for kind in [
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::Other,
    ] {
        assert_eq!(
            profile_peer_shutdown(Err(std::io::Error::from(kind)))
                .unwrap_err()
                .kind(),
            kind
        );
    }
}
