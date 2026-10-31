#![cfg(feature = "wire-openai-completions")]

mod retained_support;
use pi_ai_core::ProviderHeaders;
use pi_ai_core::{
    ApiAdapter, AssistantMessageEvent, BoxFuture, CancellationToken, Cancelled, Clock, ModelsError,
    RequestEndpoint, RequestRuntime, ResolvedAuth, ResolvedRequest, SafeUnsigned, SecretString,
    SimpleStreamOptions, SimpleStreamOptionsWire, SimpleStreamRequest, SpawnError, StopReason,
    StreamOptions, TaskSpawner, TextStreamRequest, is_context_overflow,
    is_retryable_assistant_error,
};
use pi_ai_providers::api::openai_completions::*;
use pi_ai_providers::api::runtime::{
    AdapterResponseHead, AdapterServices, PayloadMiddleware, ResponseMiddleware,
};
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, HttpTransport, RawHttpClient, ResponseBody,
    RetryJitter, TransportError,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[test]
fn complete_sdk_errors_and_cap_prefix_classify_final_messages_from_source() {
    let oracle: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-completions/retry-cap.json"
    ));
    assert_eq!(oracle["cases"].as_array().unwrap().len(), 36);
    for case in oracle["cases"].as_array().unwrap() {
        let (mut api, spawner, observed, _, mut request, options) =
            setup(&first(), 200, vec![], false);
        request.options.wire.max_retries =
            Some(SafeUnsigned::new(u64::from(case["cap"] == true)).unwrap());
        request.options.wire.max_retry_delay_ms = Some(SafeUnsigned::new(1).unwrap());
        let mut headers = HttpHeaders::new();
        for (name, value) in case["headers"].as_object().unwrap() {
            headers.set(name, value.as_str().unwrap()).unwrap();
        }
        let body: Box<dyn ResponseBody> = if case["readFailure"] == true {
            Box::new(ReadFailureBody)
        } else {
            Box::new(Body {
                chunks: vec![case["body"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            })
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([HttpResponse {
                    status: case["status"].as_u64().unwrap() as u16,
                    status_text: String::new(),
                    headers,
                    body,
                }])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let message = retained_support::message(ready(stream.result()));
        assert_eq!(
            (
                is_context_overflow(&message, None),
                is_retryable_assistant_error(&message)
            ),
            (
                case["contextOverflow"].as_bool().unwrap(),
                case["retryable"].as_bool().unwrap()
            ),
            "{}",
            case["id"]
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            case["calls"].as_u64().unwrap() as usize
        );
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(stream.next()).is_none());
        assert_eq!(
            serde_json::to_value(&message).unwrap()["diagnostics"][0]["details"]["status"],
            case["status"]
        );
        assert!(
            !message
                .error_message
                .unwrap()
                .contains(&case["status"].to_string())
        );
    }
}

struct GatedErrorBody {
    bytes: Option<Vec<u8>>,
    eof: Arc<AtomicBool>,
    observed: Arc<Observed>,
}
impl ResponseBody for GatedErrorBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.order.lock().unwrap().push("body");
        if let Some(bytes) = self.bytes.take() {
            return Box::pin(async move { Ok(Some(bytes)) });
        }
        Box::pin(std::future::poll_fn(|_| {
            if self.eof.load(Ordering::SeqCst) {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            }
        }))
    }
    fn close(&mut self) {
        self.observed.closed.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn error_body_eof_precedes_retry_and_cancellation_never_starts_successor() {
    for cancel in [false, true] {
        let case = first();
        let (mut api, spawner, observed, _, mut request, options) =
            setup(&case, 200, vec![], false);
        let eof = Arc::new(AtomicBool::new(false));
        let mut headers = HttpHeaders::new();
        headers.set("retry-after-ms", "0").unwrap();
        let first = HttpResponse {
            status: 429,
            status_text: String::new(),
            headers,
            body: Box::new(GatedErrorBody {
                bytes: Some(b"context length exceeded".to_vec()),
                eof: eof.clone(),
                observed: observed.clone(),
            }),
        };
        let second = HttpResponse {
            status: 200,
            status_text: String::new(),
            headers: HttpHeaders::new(),
            body: Box::new(Body {
                chunks: vec![case["sse"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            }),
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([first, second])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
        let token = request.options.runtime.cancellation.clone();
        let stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        assert_eq!(observed.closed.load(Ordering::SeqCst), 0);
        if cancel {
            token.cancel();
        } else {
            eof.store(true, Ordering::SeqCst);
        }
        ready(worker);
        assert_eq!(
            ready(stream.result()).stop_reason,
            if cancel {
                StopReason::Aborted
            } else {
                StopReason::Stop
            }
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            if cancel { 1 } else { 2 }
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .filter(|v| **v == "payload")
                .count(),
            1
        );
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            if cancel { 1 } else { 2 }
        );
    }
}

#[cfg(feature = "native")]
#[test]
fn native_redirect_chain_preserves_success_trace_and_final_error_body_veto() {
    use pi_ai_transport::NativeHttpClient;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    fn receive(socket: &mut TcpStream) -> (String, Vec<u8>) {
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            assert!(bytes.len() < 16384);
        }
        let head = String::from_utf8(bytes).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        (head, body)
    }
    fn released(socket: &mut TcpStream) {
        match socket.read(&mut [0]) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => panic!("socket release must be EOF/reset: {other:?}"),
        }
    }
    for mode in ["success", "error", "cancel-error"] {
        let source = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = source.local_addr().unwrap();
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let target_address = target.local_addr().unwrap();
        let source_worker = thread::spawn(move || {
            let (mut socket, _) = source.accept().unwrap();
            let initial = receive(&mut socket);
            socket
                .write_all(
                    b"HTTP/1.1 307 Redirect\r\nLocation: hop\r\nContent-Length: 999\r\n\r\nx",
                )
                .unwrap();
            socket.flush().unwrap();
            released(&mut socket);
            let (mut socket, _) = source.accept().unwrap();
            let relative = receive(&mut socket);
            write!(socket,"HTTP/1.1 302 Redirect\r\nLocation: http://{target_address}/final\r\nContent-Length: 999\r\n\r\nx").unwrap();
            socket.flush().unwrap();
            released(&mut socket);
            (initial, relative)
        });
        let case = first();
        let (mut api, spawner, observed, clock, mut request, options) =
            setup(&case, 200, vec![], false);
        let token = request.options.runtime.cancellation.clone();
        let bytes = if mode == "success" {
            case["sse"].as_str().unwrap().as_bytes().to_vec()
        } else {
            b"billing".to_vec()
        };
        let target_worker = thread::spawn(move || {
            let (mut socket, _) = target.accept().unwrap();
            let request = receive(&mut socket);
            write!(socket,"HTTP/1.1 {} Response\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",if mode=="success"{200}else{429}).unwrap();
            for chunk in bytes.chunks(3) {
                write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                socket.write_all(chunk).unwrap();
                socket.write_all(b"\r\n").unwrap();
                socket.flush().unwrap();
            }
            if mode == "cancel-error" {
                token.cancel();
            } else {
                socket.write_all(b"0\r\n\r\n").unwrap();
                socket.flush().unwrap();
            }
            released(&mut socket);
            request
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1")).unwrap();
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([
            ("cookie".into(), Some("synthetic-cookie".into())),
            ("x-custom".into(), Some("synthetic".into())),
        ]));
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        let worker = spawner.take();
        let (result, events) = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(worker);
                let mut events = Vec::new();
                while let Some(event) = stream.next().await {
                    events.push(retained_support::untimed(event));
                }
                worker.await.unwrap();
                (retained_support::message(stream.result().await), events)
            })
            .await
            .unwrap()
        });
        let ((initial, body), (relative, replayed)) = source_worker.join().unwrap();
        let (final_head, final_body) = target_worker.join().unwrap();
        assert!(initial.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        assert!(relative.starts_with("POST /v1/chat/hop HTTP/1.1\r\n"));
        assert_eq!(body, replayed);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            case["requests"][0]["body"]
        );
        for head in [&initial, &relative] {
            let head = head.to_ascii_lowercase();
            assert!(head.contains("authorization: bearer synthetic-key\r\n"));
            assert!(head.contains("cookie: synthetic-cookie\r\n"));
        }
        let head = final_head.to_ascii_lowercase();
        assert!(head.starts_with("get /final http/1.1\r\n"));
        assert!(final_body.is_empty());
        assert!(!head.contains("authorization:"));
        assert!(!head.contains("cookie:"));
        assert!(!head.contains("content-type:"));
        assert!(head.contains("x-custom: synthetic\r\n"));
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            vec![Duration::from_millis(600000)]
        );
        if mode == "success" {
            assert_eq!(
                events,
                serde_json::from_value::<Vec<AssistantMessageEvent>>(case["events"].clone())
                    .unwrap()
            );
            assert_eq!(result.stop_reason, StopReason::Stop);
            assert!(observed.order.lock().unwrap().contains(&"response"));
        } else {
            assert_eq!(events.len(), 1);
            assert!(!observed.order.lock().unwrap().contains(&"response"));
            assert_eq!(
                result.stop_reason,
                if mode == "error" {
                    StopReason::Error
                } else {
                    StopReason::Aborted
                }
            );
            assert!(!is_retryable_assistant_error(&result));
            if mode == "error" {
                assert_eq!(
                    serde_json::to_value(&result).unwrap()["diagnostics"][0]["details"]["status"],
                    429
                );
            }
        }
    }
}
struct ReadFailureBody;
impl ResponseBody for ReadFailureBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async {
            Err(TransportError::new(
                pi_ai_transport::TransportErrorKind::Connection,
            ))
        })
    }
    fn close(&mut self) {}
}
#[test]
fn cancellation_after_usage_only_update_preserves_snapshot_and_drops_pending_body() {
    let case = first();
    let (api, spawner, observed, _, request, options) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        true,
    );
    let token = request.options.runtime.cancellation.clone();
    let mut stream = api.stream_with_options(request, options);
    let mut worker = spawner.take();
    assert!(
        worker
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.usage.input.get(), 97);
    assert_eq!(
        serde_json::to_value(&result).unwrap()["content"][0]["text"],
        "hi"
    );
    ready(worker);
    assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    let mut terminals = 0;
    while let Some(event) = ready(stream.next()) {
        if matches!(
            event,
            AssistantMessageEvent::Error { .. } | AssistantMessageEvent::Done { .. }
        ) {
            terminals += 1;
        }
    }
    assert_eq!(terminals, 1);
}

#[test]
fn default_dispatch_and_spawn_api_mismatch_failures_keep_setup_in_band() {
    let case = first();
    let (mut api, spawner, observed, _, request, _) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        false,
    );
    api.defaults = serde_json::from_value(json!({"reasoningEffort":"high"})).unwrap();
    let stream = api.stream(request);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    assert_eq!(
        observed.requests.lock().unwrap()[0]["body"]["reasoning_effort"],
        json!("high")
    );
    for reject_spawn in [false, true] {
        let (api, spawner, observed, _, mut request, options) = setup(&case, 200, vec![], false);
        if reject_spawn {
            request.options.runtime.spawner = Arc::new(Spawner {
                reject: true,
                ..Default::default()
            });
        } else {
            request.resolved.model.api = "other-api".parse().unwrap();
        }
        let mut stream = api.stream_with_options(request, options);
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(stream.next()).is_none());
        assert!(spawner.tasks.lock().unwrap().is_empty());
        assert!(observed.requests.lock().unwrap().is_empty());
    }
}

#[test]
fn aggregate_input_bound_is_exact_and_body_failures_do_not_replay() {
    let case = first();
    let bytes = case["sse"].as_str().unwrap().as_bytes();
    for budget in [bytes.len(), bytes.len() - 1] {
        let (mut api, spawner, observed, _, mut request, options) = setup(
            &case,
            200,
            bytes.chunks(3).map(<[u8]>::to_vec).collect(),
            false,
        );
        api.max_stream_bytes = budget;
        request.options.wire.max_retries = Some(SafeUnsigned::new(3).unwrap());
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(
            ready(stream.result()).stop_reason,
            if budget == bytes.len() {
                StopReason::Stop
            } else {
                StopReason::Error
            }
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn cancelled_blocked_send_stays_aborted_and_runs_no_response_middleware() {
    let (mut api, spawner, observed, clock, request, options) = setup(&first(), 200, vec![], false);
    api.services.transport = Arc::new(HttpTransport::new(
        Arc::new(Client {
            responses: Mutex::new(VecDeque::new()),
            observed: observed.clone(),
            block: true,
        }),
        Arc::new(Jitter),
    ));
    let token = request.options.runtime.cancellation.clone();
    let stream = api.stream_with_options(request, options);
    let mut worker = spawner.take();
    assert!(
        worker
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    ready(worker);
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
    assert!(!observed.order.lock().unwrap().contains(&"response"));
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        vec![Duration::from_millis(600000)]
    );
}

#[test]
fn middleware_cancellation_and_auth_preflight_never_send_or_publish_start_early() {
    for response in [false, true] {
        let (mut api, spawner, observed, _, request, options) = setup(&first(), 200, vec![], true);
        let middleware = Arc::new(Middleware {
            observed: observed.clone(),
            replacement: None,
            block_payload: !response,
            block_response: response,
        });
        api.services.payload = Some(middleware.clone());
        api.services.response = Some(middleware);
        let token = request.options.runtime.cancellation.clone();
        let mut stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        token.cancel();
        ready(worker);
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            usize::from(response)
        );
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            usize::from(response)
        );
    }
    for value in [None, Some(""), Some(" \u{feff}\t")] {
        let (api, spawner, observed, _, mut request, options) = setup(&first(), 200, vec![], false);
        request.auth.api_key = None;
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
            "Authorization".to_owned(),
            value.map(str::to_owned),
        )]));
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
        assert!(observed.order.lock().unwrap().is_empty());
    }
}

#[test]
fn sdk_timeout_retry_defaults_and_explicit_overrides_have_one_owner() {
    let case = first();
    let (mut api, spawner, observed, clock, mut request, options) =
        setup(&case, 200, vec![], false);
    api.services.transport = Arc::new(HttpTransport::new(
        Arc::new(Client {
            responses: Mutex::new(VecDeque::new()),
            observed: observed.clone(),
            block: true,
        }),
        Arc::new(Jitter),
    ));
    request.options.runtime.clock = Arc::new(ClockProbe {
        timeout: true,
        ..Default::default()
    });
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(is_retryable_assistant_error(&result));
    assert_eq!(clock.sleeps.lock().unwrap().len(), 0);
    // Header timer wins before a blocked send, independently of caller cancellation.
    for retries in [0, 1] {
        let (mut api, spawner, observed, clock, mut request, options) =
            setup(&case, 200, vec![], false);
        let response = |status| HttpResponse {
            status,
            status_text: String::new(),
            headers: HttpHeaders::new(),
            body: Box::new(Body {
                chunks: vec![case["sse"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            }),
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([response(500), response(200)])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        request.options.wire.max_retries = Some(SafeUnsigned::new(retries).unwrap());
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(
            ready(stream.result()).stop_reason,
            if retries == 0 {
                StopReason::Error
            } else {
                StopReason::Stop
            }
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            retries as usize + 1
        );
        assert_eq!(observed.closed.load(Ordering::SeqCst), retries as usize + 1);
        assert_eq!(
            clock
                .sleeps
                .lock()
                .unwrap()
                .iter()
                .filter(|v| **v == Duration::from_millis(500))
                .count(),
            retries as usize
        );
        for request in observed.requests.lock().unwrap().iter() {
            assert_eq!(request["headers"]["x-stainless-retry-count"], "0");
        }
    }
}

#[cfg(feature = "native")]
#[test]
fn native_loopback_request_fragmented_sse_and_abort_release_the_socket() {
    use pi_ai_transport::NativeHttpClient;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    for cancel in [false, true] {
        let case = first();
        let sse = case["sse"].as_str().unwrap();
        let bytes = if cancel {
            format!("{}\n\n", sse.split("\n\n").next().unwrap()).into_bytes()
        } else {
            sse.as_bytes().to_vec()
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                head.push(byte[0]);
                assert!(head.len() < 16384);
            }
            let head = String::from_utf8(head).unwrap();
            let length = head
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            let mut body = vec![0; length];
            socket.read_exact(&mut body).unwrap();
            socket.write_all(b"HTTP/1.1 200 Synthetic\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
            for chunk in bytes.chunks(3) {
                write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                socket.write_all(chunk).unwrap();
                socket.write_all(b"\r\n").unwrap();
                socket.flush().unwrap();
            }
            // Standard SDK keeps consuming after finish_reason; success
            // needs EOF, while cancellation must release a still-blocked body.
            if !cancel {
                socket.write_all(b"0\r\n\r\n").unwrap();
                socket.flush().unwrap();
            }
            let closed = match socket.read(&mut [0]) {
                Ok(0) => true,
                Err(error) => matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ),
                _ => false,
            };
            (
                head,
                serde_json::from_slice::<Value>(&body).unwrap(),
                closed,
            )
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (mut api, spawner, _, _, mut request, options) = setup(&case, 200, vec![], false);
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1/")).unwrap();
        let token = request.options.runtime.cancellation.clone();
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        let task = spawner.take();
        let result = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(task);
                while let Some(event) = stream.next().await {
                    if cancel && matches!(event, AssistantMessageEvent::ThinkingDelta { .. }) {
                        token.cancel();
                    }
                }
                worker.await.unwrap();
                retained_support::message(stream.result().await)
            })
            .await
            .unwrap()
        });
        let (head, body, closed) = server.join().unwrap();
        assert!(
            closed,
            "socket release requires EOF/reset, not read timeout"
        );
        assert!(head.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        let head = head.to_ascii_lowercase();
        assert!(head.contains("authorization: bearer synthetic-key\r\n"));
        assert!(head.contains("accept: application/json\r\n"));
        assert!(head.contains("content-type: application/json\r\n"));
        assert_eq!(body, case["requests"][0]["body"]);
        assert_eq!(
            serde_json::to_value(&result).unwrap()["content"][0]["text"],
            "hi"
        );
        if cancel {
            assert_eq!(result.stop_reason, StopReason::Aborted);
        } else {
            assert_eq!(result.stop_reason, StopReason::Stop);
            assert_eq!(result.usage.input.get(), 97);
        }
    }
}
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

fn fixture() -> Value {
    retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-completions/trace.json"
    ))
}
fn first() -> Value {
    fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == "default")
        .unwrap()
        .clone()
}

#[test]
fn correction_unresolved_usage_and_model_only_chunks_refresh_cancellation_snapshot() {
    let corpus: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-completions/corrections.json"
    ));
    let cases = corpus["observations"].as_array().unwrap();
    for case in cases
        .iter()
        .filter(|c| c["id"].as_str().unwrap().contains("cancel"))
    {
        let (api, spawner, observed, _, request, options) = setup(
            case,
            200,
            vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
            true,
        );
        let token = request.options.runtime.cancellation.clone();
        let mut stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        token.cancel();
        let result = retained_support::message(ready(stream.result()));
        let expected_usage: pi_ai_core::Usage =
            serde_json::from_value(case["result"]["usage"].clone()).unwrap();
        assert_eq!(result.usage, expected_usage, "{}", case["id"]);
        assert_eq!(
            json!(result.response_model),
            case["result"]["responseModel"]
        );
        assert_eq!(json!(result.response_id), case["result"]["responseId"]);
        assert_eq!(result.stop_reason, StopReason::Aborted);
        if case["id"].as_str().unwrap().starts_with("unresolved") {
            assert_eq!(
                json!(result.content),
                json!([{"type":"text","text":"prefix"}])
            );
            while let Some(event) = ready(stream.next()) {
                assert!(!matches!(
                    event,
                    AssistantMessageEvent::ToolCallStart { .. }
                        | AssistantMessageEvent::ToolCallDelta { .. }
                        | AssistantMessageEvent::ToolCallEnd { .. }
                ));
            }
        }
        ready(worker);
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
        assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn correction_sdk_nonstream_parser_skips_only_the_source_defined_bodies() {
    let corpus: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/openai-completions/corrections.json"
    ));
    for case in corpus["observations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c.get("headers").is_some())
    {
        let (mut api, spawner, observed, _, mut request, options) = setup(case, 200, vec![], false);
        request.options.wire.max_retries = Some(SafeUnsigned::new(2).unwrap());
        let mut headers = HttpHeaders::new();
        for entry in case["headers"].as_array().unwrap() {
            headers
                .append(entry[0].as_str().unwrap(), entry[1].as_str().unwrap())
                .unwrap();
        }
        let blocked = case["block"] == true || case["status"] == 204;
        let chunks = if blocked {
            VecDeque::new()
        } else {
            VecDeque::from([case["body"].as_str().unwrap().as_bytes().to_vec()])
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([HttpResponse {
                    status: case["status"].as_u64().unwrap() as u16,
                    status_text: String::new(),
                    headers,
                    body: Box::new(Body {
                        chunks,
                        block: blocked,
                        observed: observed.clone(),
                    }),
                }])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(
            json!(result.stop_reason),
            case["result"]["stopReason"],
            "{}",
            case["id"]
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .filter(|s| **s == "body")
                .count(),
            case["reads"].as_u64().unwrap() as usize,
            "{}",
            case["id"]
        );
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(
            json!(
                observed
                    .order
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|s| **s != "body")
                    .copied()
                    .collect::<Vec<_>>()
            ),
            case["order"]
        );
        let mut events = Vec::new();
        while let Some(e) = ready(stream.next()) {
            events.push(retained_support::without_duration(json!(e)));
        }
        let normalize = |mut events: Value| {
            for e in events.as_array_mut().unwrap() {
                if e["type"] == "error" {
                    e["error"]["errorMessage"] = json!("safe error");
                    e["error"].as_object_mut().unwrap().remove("diagnostics");
                }
            }
            serde_json::from_value::<Vec<AssistantMessageEvent>>(events).unwrap()
        };
        assert_eq!(
            normalize(json!(events)),
            normalize(case["events"].clone()),
            "{}",
            case["id"]
        );
    }
}

#[test]
fn split_identity_eof_and_cancel_preserve_only_the_representable_prefix() {
    let fixture = fixture();
    let source = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == "missing-id")
        .unwrap();
    assert_eq!(
        source["events"].as_array().unwrap().last().unwrap()["message"]["content"][0]["id"],
        ""
    );
    for cancel in [false, true] {
        let prefix = "data: {\"choices\":[{\"delta\":{\"content\":\"prefix\"}}]}\n\n";
        let after =
            "data: {\"choices\":[{\"delta\":{\"reasoning\":\"after unresolved tool\"}}]}\n\n";
        let bytes = format!("{prefix}{}{after}", source["sse"].as_str().unwrap());
        let (api, spawner, observed, _, request, options) =
            setup(source, 200, vec![bytes.into_bytes()], cancel);
        let token = request.options.runtime.cancellation.clone();
        let mut stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        if cancel {
            assert!(
                worker
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            token.cancel();
        }
        ready(worker);
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(
            result.stop_reason,
            if cancel {
                StopReason::Aborted
            } else {
                StopReason::Error
            }
        );
        assert_eq!(
            json!(result.content),
            json!([{"type":"text","text":"prefix"}])
        );
        let mut events = Vec::new();
        while let Some(e) = ready(stream.next()) {
            events.push(retained_support::without_duration(json!(e)));
        }
        assert!(
            events
                .iter()
                .all(|e| !e["type"].as_str().unwrap().starts_with("toolcall"))
        );
        assert_eq!(events.last().unwrap()["type"], "error");
        assert!(observed.closed.load(Ordering::SeqCst) > 0);
    }
}

#[test]
fn raw_family_options_keep_exact_null_and_tool_selector_domains() {
    for value in [
        json!({}),
        json!({"reasoningEffort":"max","thinkingBudgets":{"high":0}}),
        json!({"toolChoice":"required"}),
        json!({"toolChoice":{"type":"function","function":{"name":""}}}),
        json!({"toolChoice":{"type":"custom","custom":{"name":"read"}}}),
        json!({"toolChoice":{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"arbitrary":null}]}}}),
    ] {
        let options: OpenAICompletionsOptions = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(json!(options), value);
    }
    for value in [
        json!({"toolChoice":null}),
        json!({"reasoningEffort":null}),
        json!({"thinkingBudgets":null}),
        json!({"toolChoice":false}),
        json!({"reasoningEffort":"off"}),
        json!({"toolChoice":{"type":"custom","custom":{"name":"read","extra":1}}}),
        json!({"toolChoice":{"type":"allowed_tools","allowed_tools":{"mode":"none","tools":[]}}}),
    ] {
        assert!(serde_json::from_value::<OpenAICompletionsOptions>(value).is_err());
    }
}

#[test]
fn malformed_wire_numbers_shapes_and_argument_overflow_settle_once() {
    for chunk in [
        json!({"id":1}),
        json!({"choices":[{"finish_reason":3}]}),
        json!({"choices":[{"delta":{"content":{}}}]}),
        json!({"usage":{"completion_tokens":1.5}}),
        json!({"usage":{"cached_tokens":-1}}),
        json!({"usage":{"completion_tokens":9007199254740992u64}}),
        json!({"choices":[{"delta":{"tool_calls":{}}}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"id":"c","function":{"name":"read","arguments":"[]"}}]}}]}),
    ] {
        let (api, spawner, _, _, request, options) = setup(
            &first(),
            200,
            vec![format!("data: {chunk}\n\n").into_bytes()],
            false,
        );
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
        let mut terminals = 0;
        while let Some(event) = ready(stream.next()) {
            if matches!(
                event,
                AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
            ) {
                terminals += 1;
            }
        }
        assert_eq!(terminals, 1);
    }
    let data = "x".repeat(262_145);
    let chunk = json!({"choices":[{"delta":{"tool_calls":[{"id":"c","function":{"name":"read","arguments":data}}]}}]});
    let (mut api, spawner, _, _, request, options) = setup(
        &first(),
        200,
        vec![format!("data: {chunk}\n\n").into_bytes()],
        false,
    );
    api.services.sse_limits.max_frame_bytes = 512 * 1024;
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
}

#[test]
fn numeric_wire_spelling_and_missing_finish_semantics_follow_javascript() {
    let sse = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":-5e0,\"cached_tokens\":2.0,\"completion_tokens\":3e0,\"prompt_tokens_details\":{\"cache_write_tokens\":-0.0}}}\n\n";
    let (api, spawner, _, _, request, options) =
        setup(&first(), 200, vec![sse.as_bytes().to_vec()], false);
    let stream = api.stream_with_options(request, options);
    ready(spawner.take());
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(
        (
            result.usage.input.get(),
            result.usage.output.get(),
            result.usage.cache_read.get(),
            result.usage.total_tokens.get()
        ),
        (0, 3, 2, 5)
    );
    assert_eq!(result.stop_reason, StopReason::Stop);
}

#[cfg(feature = "native")]
#[test]
fn native_truncated_error_body_keeps_status_and_retry_header_eligibility() {
    use pi_ai_transport::NativeHttpClient;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    for (status, veto, retry) in [
        (429, "", true),
        (429, "x-should-retry: false\r\n", false),
        (403, "x-should-retry: true\r\n", true),
        (403, "", false),
    ] {
        let case = first();
        let success = case["sse"].as_str().unwrap().as_bytes().to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for attempt in 0..if retry { 2 } else { 1 } {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    let mut b = [0];
                    socket.read_exact(&mut b).unwrap();
                    header.push(b[0]);
                    assert!(header.len() < 16384);
                }
                let header = String::from_utf8(header).unwrap();
                let length = header
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                socket.read_exact(&mut vec![0; length]).unwrap();
                if attempt == 0 {
                    write!(socket,"HTTP/1.1 {status} Synthetic\r\n{veto}retry-after-ms: 0\r\ncontent-length: 200\r\n\r\npartial").unwrap();
                    socket.flush().unwrap();
                } else {
                    write!(socket,"HTTP/1.1 200 Synthetic\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n",success.len()).unwrap();
                    socket.write_all(&success).unwrap();
                    socket.flush().unwrap();
                }
                // Closing before advertised length exercises the real native failed-body path.
            }
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (mut api, spawner, observed, _, mut request, options) =
            setup(&case, 200, vec![], false);
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1")).unwrap();
        request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        let stream = api.stream_with_options(request, options);
        let worker = spawner.take();
        let result = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                worker.await;
                retained_support::message(stream.result().await)
            })
            .await
            .unwrap()
        });
        server.join().unwrap();
        assert_eq!(
            result.stop_reason,
            if retry {
                StopReason::Stop
            } else {
                StopReason::Error
            }
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .filter(|v| **v == "response")
                .count(),
            usize::from(retry)
        );
        if !retry {
            assert_eq!(json!(result)["diagnostics"][0]["details"]["status"], status);
        }
    }
}

#[test]
fn unresolved_identity_queue_is_bounded_and_never_publishes_invalid_calls() {
    let first_chunk =
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}]});
    let next = json!({"choices":[{"delta":{"tool_calls":[{"index":0}]}}]});
    let mut chunks = vec![format!("data: {first_chunk}\n\n").into_bytes()];
    chunks.extend((0..3000).map(|_| format!("data: {next}\n\n").into_bytes()));
    let (api, spawner, observed, _, request, options) = setup(&first(), 200, chunks, false);
    let mut stream = api.stream_with_options(request, options);
    ready(spawner.take());
    let result = retained_support::message(ready(stream.result()));
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(result.content.is_empty());
    assert_eq!(
        json!(result)["diagnostics"][0]["details"]["kind"],
        "resource_limit"
    );
    assert!(matches!(
        ready(stream.next()),
        Some(AssistantMessageEvent::Start { .. })
    ));
    assert!(matches!(
        ready(stream.next()),
        Some(AssistantMessageEvent::Error { .. })
    ));
    assert!(ready(stream.next()).is_none());
    assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("expected ready"),
    }
}
#[derive(Default)]
struct Spawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    reject: bool,
}
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject {
            return Err(SpawnError::default());
        }
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Spawner {
    fn take(&self) -> BoxFuture<'static, ()> {
        self.tasks.lock().unwrap().pop().unwrap()
    }
}
#[derive(Default)]
struct ClockProbe {
    sleeps: Mutex<Vec<Duration>>,
    timeout: bool,
}
impl Clock for ClockProbe {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _token: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(duration);
        Box::pin(async move {
            if self.timeout || duration < Duration::from_millis(1000) {
                Ok(())
            } else {
                pending().await
            }
        })
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.0
    }
}
#[derive(Default)]
struct Observed {
    requests: Mutex<Vec<Value>>,
    order: Mutex<Vec<&'static str>>,
    closed: AtomicUsize,
    dropped: AtomicUsize,
    models: Mutex<Vec<Value>>,
}
struct PendingRead(Arc<Observed>);
impl Future for PendingRead {
    type Output = Result<Option<Vec<u8>>, TransportError>;
    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    block: bool,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.order.lock().unwrap().push("body");
        if self.chunks.is_empty() && self.block {
            return Box::pin(PendingRead(self.observed.clone()));
        }
        let next = self.chunks.pop_front();
        Box::pin(async move { Ok(next) })
    }
    fn close(&mut self) {
        self.observed.closed.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    observed: Arc<Observed>,
    block: bool,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            self.observed.requests.lock().unwrap().push(json!({"url":request.endpoint.expose_for_transport(),"headers":request.headers.iter_for_wire().collect::<BTreeMap<_,_>>(),"body":serde_json::from_slice::<Value>(&request.body).unwrap_or(Value::Null)}));
            if self.block {
                pending::<()>().await;
            }
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("one configured attempt"))
        })
    }
}
struct Middleware {
    observed: Arc<Observed>,
    replacement: Option<WirePayload>,
    block_payload: bool,
    block_response: bool,
}
impl PayloadMiddleware for Middleware {
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        _payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            self.observed
                .models
                .lock()
                .unwrap()
                .push(serde_json::to_value(&request.resolved.model).unwrap());
            if self.block_payload {
                pending::<()>().await;
            }
            Ok(self.replacement.clone())
        })
    }
}
impl ResponseMiddleware for Middleware {
    fn on_response<'a>(
        &'a self,
        _request: &'a TextStreamRequest,
        _head: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            if self.block_response {
                pending::<()>().await;
            }
            Ok(())
        })
    }
}
fn setup(
    case: &Value,
    status: u16,
    chunks: Vec<Vec<u8>>,
    block: bool,
) -> (
    OpenAICompletionsApi,
    Arc<Spawner>,
    Arc<Observed>,
    Arc<ClockProbe>,
    TextStreamRequest,
    OpenAICompletionsOptions,
) {
    let observed = Arc::new(Observed::default());
    let mut headers = HttpHeaders::new();
    headers
        .set(
            "content-type",
            case["contentType"].as_str().unwrap_or("text/event-stream"),
        )
        .unwrap();
    let client = Arc::new(Client {
        responses: Mutex::new(VecDeque::from([HttpResponse {
            status,
            status_text: String::new(),
            headers,
            body: Box::new(Body {
                chunks: chunks.into(),
                block,
                observed: observed.clone(),
            }),
        }])),
        observed: observed.clone(),
        block: false,
    });
    let middleware = Arc::new(Middleware {
        observed: observed.clone(),
        replacement: case.get("replacement").map(|v| WirePayload::new(v.clone())),
        block_payload: false,
        block_response: false,
    });
    // Match the oracle's SDK custom-fetch seam, before network defaults/decoding.
    let mut services = AdapterServices::new(Arc::new(
        HttpTransport::new(client, Arc::new(Jitter))
            .with_fetch_response_source(pi_ai_transport::FetchResponseSource::Preconstructed),
    ));
    services.payload = Some(middleware.clone());
    services.response = Some(middleware);
    let spawner = Arc::new(Spawner::default());
    let clock = Arc::new(ClockProbe::default());
    let mut common = case["options"].clone();
    let mut family = json!({});
    for key in ["reasoningEffort", "thinkingBudgets", "toolChoice"] {
        if let Some(value) = common.as_object_mut().unwrap().remove(key) {
            family[key] = value;
        }
    }
    let headers = common
        .as_object_mut()
        .unwrap()
        .remove("headers")
        .unwrap_or(json!({}));
    let env = common
        .as_object_mut()
        .unwrap()
        .remove("env")
        .unwrap_or(json!({}));
    common.as_object_mut().unwrap().remove("reasoning");
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(case["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved(
                case["endpoint"]
                    .as_str()
                    .unwrap_or(case["model"]["baseUrl"].as_str().unwrap()),
            )
            .unwrap(),
        },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: if case["noKey"] == true {
                None
            } else {
                Some(SecretString::new("synthetic-key"))
            },
            headers: serde_json::from_value(headers).unwrap(),
            env: serde_json::from_value(env).unwrap(),
        },
        options: StreamOptions {
            wire: serde_json::from_value(common).unwrap(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                spawner.clone(),
            ),
        },
    };
    (
        OpenAICompletionsApi::new(services),
        spawner,
        observed,
        clock,
        request,
        serde_json::from_value(family).unwrap(),
    )
}

#[test]
fn pinned_wrapper_sdk_requests_and_complete_snapshots_match_101_cases() {
    let fixture = fixture();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 100);
    for case in fixture["cases"].as_array().unwrap() {
        if case["invalidIdentity"] == true {
            continue;
        }
        let (api, spawner, observed, clock, request, options) = setup(
            case,
            case["status"].as_u64().unwrap() as u16,
            case["sse"]
                .as_str()
                .unwrap()
                .as_bytes()
                .chunks(3)
                .map(<[u8]>::to_vec)
                .collect(),
            false,
        );
        let mut stream = if case["simple"] == true {
            api.stream_simple(SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        stream: request.options.wire,
                        reasoning: Some(
                            serde_json::from_value(case["options"]["reasoning"].clone()).unwrap(),
                        ),
                        ..Default::default()
                    },
                    runtime: request.options.runtime,
                },
            })
        } else {
            api.stream_with_options(request, options)
        };
        ready(spawner.take());
        let result = retained_support::message(ready(stream.result()));
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "retry {}",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["contextOverflow"].as_bool().unwrap(),
            "overflow {}",
            case["id"]
        );
        let mut events = Vec::new();
        while let Some(event) = ready(stream.next()) {
            events.push(retained_support::without_duration(
                serde_json::to_value(event).unwrap(),
            ));
        }
        // Core costs are finite-f64 while JS has one Number kind. Decode both
        // complete snapshots through the DTO; signature strings remain byte-exact.
        let normalize = |mut events: Value| {
            if case["publicationNormalization"] == true {
                let final_content =
                    case["events"].as_array().unwrap().last().unwrap()["message"]["content"]
                        .clone();
                for event in events.as_array_mut().unwrap() {
                    if let Some(parts) = event
                        .get_mut("partial")
                        .and_then(|v| v.get_mut("content"))
                        .and_then(Value::as_array_mut)
                    {
                        for (i, part) in parts.iter_mut().enumerate() {
                            if part["type"] == "toolCall" {
                                for key in ["id", "name"] {
                                    if part[key] == "" {
                                        part[key] = final_content[i][key].clone();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for event in events.as_array_mut().unwrap() {
                if event["type"] == "error" {
                    event["error"]["errorMessage"] = json!("normalized provider error");
                    event["error"]
                        .as_object_mut()
                        .unwrap()
                        .remove("diagnostics");
                }
            }
            serde_json::from_value::<Vec<AssistantMessageEvent>>(events).unwrap()
        };
        assert_eq!(
            normalize(json!(events)),
            normalize(case["events"].clone()),
            "events {}",
            case["id"]
        );
        assert_eq!(
            json!(*observed.requests.lock().unwrap()),
            case["requests"],
            "request {}",
            case["id"]
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|v| *v != "body")
                .collect::<Vec<_>>(),
            case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
        );
        if !case["requests"].as_array().unwrap().is_empty() {
            assert!(observed.closed.load(Ordering::SeqCst) >= 1);
        } else {
            assert_eq!(observed.closed.load(Ordering::SeqCst), 0);
        }
        let order = observed.order.lock().unwrap();
        if case["requests"]
            .as_array()
            .is_some_and(|requests| !requests.is_empty())
            && case["requests"][0]["body"]["stream"] == true
            && case["status"].as_u64().unwrap() < 300
            && let (Some(response), Some(body)) = (
                order.iter().position(|v| *v == "response"),
                order.iter().position(|v| *v == "body"),
            )
        {
            assert!(
                response < body,
                "streaming response middleware precedes body reduction"
            );
        }
        if case["requests"].as_array().unwrap().is_empty() {
            assert!(clock.sleeps.lock().unwrap().is_empty());
        } else {
            assert_eq!(
                clock.sleeps.lock().unwrap()[0].as_millis(),
                case["options"]["timeoutMs"].as_u64().unwrap_or(600000) as u128
            );
        }
    }
}

// Recorded provider streams: official DeepSeek `reasoning_content` and vLLM 0.21
// `reasoning`, replayed byte-for-byte through the production SSE decoder. Expected
// values are transcribed from the recordings, never produced by this adapter; see
// conformance/provider-traces/openai-completions/recorded/README.md for provenance.
macro_rules! recorded {
    ($name:literal) => {
        include_bytes!(concat!(
            "../../../conformance/provider-traces/openai-completions/recorded/",
            $name
        ))
        .as_slice()
    };
}

const VLLM_REASONING: &str = "我们要求1+2+...+100的和。这是一个等差数列求和，首项a1=1，末项an=100，项数n=100。公式为S = n*(a1+an)/2 = 100*(1+100)/2 = 100*101/2 = 5050。因此答案是5050。";
const VLLM_TEXT: &str = "1+2+…+100 = 5050。这是利用等差数列求和公式得到的：  \n\\[ S = \\frac{n(a_1 + a_n)}{2} = \\frac{100 \\times (1 + 100)}{2} = 5050 \\]";
const DEEPSEEK_REASONING: &str = "The user wants me to read /etc/hosts and tell them the first line. Let me use the read_file tool.";

/// The catalog's DeepSeek profile, used when a gateway relays the official API.
fn deepseek_compat() -> Value {
    json!({"supportsStore":false,"supportsDeveloperRole":false,"maxTokensField":"max_tokens",
           "requiresReasoningContentOnAssistantMessages":true,"thinkingFormat":"deepseek",
           "supportsStrictMode":true})
}

/// vLLM 0.21 serving DeepSeek V4 reads thinking from chat-template arguments.
fn vllm_compat() -> Value {
    json!({"supportsDeveloperRole":false,"thinkingFormat":"chat-template",
           "chatTemplateKwargs":{"thinking":{"$var":"thinking.enabled"},
                                 "reasoning_effort":{"$var":"thinking.effort","omitWhenOff":true}}})
}

fn user(text: &str) -> Value {
    json!({"role":"user","content":text,"timestamp":0})
}

fn recorded_case(model: &str, compat: Value, effort: Option<&str>, messages: Vec<Value>) -> Value {
    json!({
        "model": {"id":model,"name":model,"api":"openai-completions","provider":"gateway",
                  "baseUrl":"https://gateway.invalid/v1","reasoning":true,"input":["text"],
                  "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},
                  "contextWindow":1000000,"maxTokens":32768,"compat":compat},
        "context": {"systemPrompt":"system","messages":messages,
                    "tools":[{"name":"read_file","description":"Read a file","parameters":
                              {"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}]},
        "options": effort.map_or(json!({}), |effort| json!({"reasoningEffort":effort})),
    })
}

/// Returns the terminal message, event types with consecutive repeats collapsed,
/// and the sent request body.
fn replay(case: &Value, sse: &[u8], chunk: usize) -> (Value, Vec<String>, Value) {
    let (api, spawner, observed, _, request, options) = setup(
        case,
        200,
        sse.chunks(chunk).map(<[u8]>::to_vec).collect(),
        false,
    );
    let mut stream = api.stream_with_options(request, options);
    ready(spawner.take());
    let message = serde_json::to_value(retained_support::message(ready(stream.result()))).unwrap();
    let mut types: Vec<String> = Vec::new();
    while let Some(event) = ready(stream.next()) {
        let kind = serde_json::to_value(event).unwrap()["type"]
            .as_str()
            .unwrap()
            .to_owned();
        if types.last() != Some(&kind) {
            types.push(kind);
        }
    }
    let body = observed.requests.lock().unwrap()[0]["body"].clone();
    (message, types, body)
}

/// One whole body and 7-byte reads that split multi-byte UTF-8 must settle identically.
fn replay_recording(case: &Value, sse: &[u8]) -> (Value, Vec<String>, Value) {
    let whole = replay(case, sse, sse.len());
    assert_eq!(
        replay(case, sse, 7),
        whole,
        "fragmented reads must not change the result"
    );
    whole
}

fn usage_counts(message: &Value) -> [u64; 4] {
    ["input", "output", "cacheRead", "totalTokens"]
        .map(|key| message["usage"][key].as_u64().unwrap())
}

#[test]
fn recorded_deepseek_thinking_stream_keeps_reasoning_content_beside_its_tool_call() {
    let case = recorded_case(
        "deepseek-v4-flash",
        deepseek_compat(),
        Some("high"),
        vec![user("read /etc/hosts")],
    );
    let (message, types, body) =
        replay_recording(&case, recorded!("sse_deepseek_v4_thinking_with_tools.txt"));
    assert_eq!(body["thinking"], json!({"type":"enabled"}));
    assert_eq!(body["reasoning_effort"], "high");
    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(
        types,
        [
            "start",
            "thinking_start",
            "thinking_delta",
            "text_start",
            "text_delta",
            "toolcall_start",
            "toolcall_delta",
            "thinking_end",
            "text_end",
            "toolcall_end",
            "done"
        ]
    );
    assert_eq!(
        message["content"],
        json!([
            {"type":"thinking","thinking":DEEPSEEK_REASONING,"thinkingSignature":"reasoning_content"},
            {"type":"text","text":"我来读取这个文件。"},
            {"type":"toolCall","id":"call_00_epqxJM7KTZNboYxcyNLO1033","name":"read_file","arguments":{"path":"/etc/hosts"}}
        ])
    );
    assert_eq!(message["stopReason"], "toolUse");
    assert_eq!(
        message["responseId"],
        "924a12ff-a6d7-4864-9120-a45074da6b7f"
    );
    // The gateway serves the requested alias under the provider's current name.
    assert_eq!(message["responseModel"], "deepseek-flash");
    assert_eq!(usage_counts(&message), [301, 70, 0, 371]);
    assert_eq!(message["usage"]["reasoning"], 25);
}

#[test]
fn recorded_deepseek_nonthinking_streams_carry_no_reasoning() {
    for (name, sse, text, call_id, counts) in [
        (
            "with text",
            recorded!("sse_deepseek_v4_nonthinking_with_tools.txt"),
            Some("我来读取这个文件。"),
            "call_00_lNcxb8fNzZXH7dM04oD19264",
            [149, 44, 128, 321],
        ),
        (
            "tool only",
            recorded!("sse_deepseek_v4_nonthinking_puretool.txt"),
            None,
            "call_00_xQXDClHtu9Tl7SdMRywn7699",
            [282, 40, 0, 322],
        ),
    ] {
        let case = recorded_case(
            "deepseek-v4-flash",
            deepseek_compat(),
            None,
            vec![user("read /etc/hosts")],
        );
        let (message, types, body) = replay_recording(&case, sse);
        assert_eq!(body["thinking"], json!({"type":"disabled"}), "{name}");
        assert!(body.get("reasoning_effort").is_none(), "{name}");
        let call = json!({"type":"toolCall","id":call_id,"name":"read_file","arguments":{"path":"/etc/hosts"}});
        let (content, expected_types) = match text {
            Some(text) => (
                json!([{"type":"text","text":text}, call]),
                vec![
                    "start",
                    "text_start",
                    "text_delta",
                    "toolcall_start",
                    "toolcall_delta",
                    "text_end",
                    "toolcall_end",
                    "done",
                ],
            ),
            None => (
                json!([call]),
                vec![
                    "start",
                    "toolcall_start",
                    "toolcall_delta",
                    "toolcall_end",
                    "done",
                ],
            ),
        };
        assert_eq!(message["content"], content, "{name}");
        assert_eq!(types, expected_types, "{name}");
        assert_eq!(message["stopReason"], "toolUse", "{name}");
        assert_eq!(usage_counts(&message), counts, "{name}");
    }
}

#[test]
fn recorded_vllm_stream_keeps_delta_reasoning_and_settles_without_usage() {
    let case = recorded_case(
        "deepseekv4-flash",
        vllm_compat(),
        Some("high"),
        vec![user("1+2+...+100")],
    );
    let (message, types, body) =
        replay_recording(&case, recorded!("sse_vllm021_delta_reasoning.txt"));
    assert_eq!(
        body["chat_template_kwargs"],
        json!({"thinking":true,"reasoning_effort":"high"})
    );
    assert!(body.get("thinking").is_none() && body.get("reasoning_effort").is_none());
    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(body["stream_options"], json!({"include_usage":true}));
    assert_eq!(
        types,
        [
            "start",
            "thinking_start",
            "thinking_delta",
            "text_start",
            "text_delta",
            "thinking_end",
            "text_end",
            "done"
        ]
    );
    assert_eq!(
        message["content"],
        json!([
            {"type":"thinking","thinking":VLLM_REASONING,"thinkingSignature":"reasoning"},
            {"type":"text","text":VLLM_TEXT}
        ])
    );
    assert_eq!(message["stopReason"], "stop");
    assert!(message.get("responseModel").is_none());
    // The recording has no usage frame; reported usage stays zero rather than estimated.
    assert_eq!(message["usage"], json!(pi_ai_core::Usage::zero()));
}

#[test]
fn synthetic_vllm_stream_delivers_each_tool_call_in_one_frame() {
    let case = recorded_case(
        "deepseekv4-flash",
        vllm_compat(),
        Some("high"),
        vec![user("read and list")],
    );
    let (message, types, _) =
        replay_recording(&case, recorded!("synthetic_vllm021_tools_thinking.sse"));
    assert_eq!(
        types,
        [
            "start",
            "thinking_start",
            "thinking_delta",
            "text_start",
            "text_delta",
            "toolcall_start",
            "toolcall_delta",
            "toolcall_start",
            "toolcall_delta",
            "thinking_end",
            "text_end",
            "toolcall_end",
            "done"
        ]
    );
    assert_eq!(
        message["content"],
        json!([
            {"type":"thinking","thinking":"用户要求同时读取 /etc/hosts 并列出 /tmp，我分别调用 read_file 和 run_command。","thinkingSignature":"reasoning"},
            {"type":"text","text":"好的，我来分别处理。\n\n"},
            {"type":"toolCall","id":"call_1f58e66539e74b6ca5fd8e85","name":"read_file","arguments":{"path":"/etc/hosts"}},
            {"type":"toolCall","id":"call_896317fabe144ed0a2c77cbf","name":"run_command","arguments":{"cmd":"ls -la /tmp","timeout":30}}
        ])
    );
    assert_eq!(message["stopReason"], "toolUse");
    assert_eq!(usage_counts(&message), [812, 122, 0, 934]);
}

#[test]
fn recorded_reasoning_replays_under_the_field_each_server_reads() {
    // vLLM 0.21 reads only `reasoning` from assistant history, while DeepSeek requires
    // `reasoning_content` on every assistant message. A DeepSeek profile in front of a
    // vLLM backend therefore sends both keys; the backend reads the populated one.
    let stop = b"data: {\"id\":\"next\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"next\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    for (model, compat, sse, field, text, keys) in [
        (
            "deepseekv4-flash",
            vllm_compat(),
            recorded!("sse_vllm021_delta_reasoning.txt"),
            "reasoning",
            VLLM_REASONING,
            vec!["reasoning"],
        ),
        (
            "deepseekv4-flash",
            deepseek_compat(),
            recorded!("sse_vllm021_delta_reasoning.txt"),
            "reasoning",
            VLLM_REASONING,
            vec!["reasoning", "reasoning_content"],
        ),
        (
            "deepseek-v4-flash",
            deepseek_compat(),
            recorded!("sse_deepseek_v4_thinking_with_tools.txt"),
            "reasoning_content",
            DEEPSEEK_REASONING,
            vec!["reasoning_content"],
        ),
    ] {
        let first = recorded_case(model, compat.clone(), Some("high"), vec![user("first")]);
        let (assistant, _, _) = replay(&first, sse, sse.len());
        let mut history = vec![user("first"), assistant.clone()];
        for call in assistant["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|b| b["type"] == "toolCall")
        {
            history.push(json!({"role":"toolResult","toolCallId":call["id"],"toolName":call["name"],
                                "content":[{"type":"text","text":"127.0.0.1 localhost"}],"isError":false,"timestamp":0}));
        }
        history.push(user("next"));
        let second = recorded_case(model, compat, Some("high"), history);
        let (_, _, body) = replay(&second, stop, stop.len());
        let replayed = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "assistant")
            .unwrap();
        assert_eq!(replayed[field], text, "{model} {keys:?}");
        let mut present = replayed
            .as_object()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with("reasoning"))
            .map(String::as_str)
            .collect::<Vec<_>>();
        present.sort_unstable();
        assert_eq!(present, keys, "{model}");
        if keys.len() == 2 {
            assert_eq!(
                replayed["reasoning_content"], "",
                "the DeepSeek placeholder stays empty"
            );
        }
    }
}
