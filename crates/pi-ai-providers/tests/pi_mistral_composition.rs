#![cfg(all(feature = "wire-pi-messages", feature = "wire-mistral-conversations"))]

use pi_ai_core::*;
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_providers::api::{
    mistral_conversations::MistralApi, pi_messages::PiMessagesApi, runtime::*,
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    future::{Future, pending},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/pi-messages/composition.json"
    ))
    .unwrap()
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
#[derive(Default)]
struct Host {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    sleeps: Mutex<Vec<Duration>>,
}
impl TaskSpawner for Host {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(duration);
        Box::pin(pending())
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("neither family retries")
    }
}
#[derive(Default)]
struct Observed {
    order: Mutex<Vec<&'static str>>,
    heads: Mutex<Vec<Value>>,
    payloads: Mutex<Vec<Value>>,
    requests: Mutex<Vec<Value>>,
    reads: AtomicUsize,
    chunks: AtomicUsize,
    closes: AtomicUsize,
}
struct Hook {
    observed: Arc<Observed>,
    mode: String,
}
impl PayloadMiddleware for Hook {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            self.observed
                .payloads
                .lock()
                .unwrap()
                .push(json!(payload.as_value()));
            Ok(None)
        })
    }
}
impl ResponseMiddleware for Hook {
    fn on_response<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        head: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            assert_eq!(
                self.observed.reads.load(Ordering::SeqCst),
                0,
                "callback precedes body polling"
            );
            self.observed.order.lock().unwrap().push("response");
            self.observed.heads.lock().unwrap().push(json!({"status":head.status,"entries":head.headers.iter_for_wire().collect::<Vec<_>>()}));
            if self.mode == "hook-cancel" {
                request.options.runtime.cancellation.cancel();
                return pending().await;
            }
            if self.mode == "hook-fail" {
                return Err(ModelsError::new(ModelsErrorCode::Stream));
            }
            Ok(())
        })
    }
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.reads.fetch_add(1, Ordering::SeqCst);
        let next = self.chunks.pop_front();
        if next.is_some() {
            self.observed.chunks.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move { Ok(next) })
    }
    fn close(&mut self) {
        self.observed.closes.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    response: Mutex<Option<HttpResponse>>,
    observed: Arc<Observed>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            assert!(
                request.headers.get_for_wire("accept-encoding").is_none(),
                "preconstructed port stays semantic"
            );
            self.observed.requests.lock().unwrap().push(json!({"path":request.endpoint.expose_for_transport().strip_prefix("https://example.invalid").unwrap(),"body":serde_json::from_slice::<Value>(&request.body).unwrap()}));
            Ok(self.response.lock().unwrap().take().expect("one attempt"))
        })
    }
}
fn setup(
    case: &Value,
    transport: HttpTransport,
    observed: &Arc<Observed>,
    host: &Arc<Host>,
) -> (Box<dyn ApiAdapter>, TextStreamRequest) {
    let hook = Arc::new(Hook {
        observed: observed.clone(),
        mode: case["mode"].as_str().unwrap().into(),
    });
    let mut services = AdapterServices::new(Arc::new(transport));
    services.payload = Some(hook.clone());
    services.response = Some(hook);
    let pi = case["family"] == "pi-messages";
    let adapter: Box<dyn ApiAdapter> = if pi {
        Box::new(PiMessagesApi::new(services))
    } else {
        Box::new(MistralApi::new(services))
    };
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(case["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved(case["model"]["baseUrl"].as_str().unwrap())
                .unwrap(),
        },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new("synthetic-key")),
            headers: ProviderHeaders::default(),
            env: ProviderEnv::default(),
        },
        options: StreamOptions {
            wire: serde_json::from_value(
                json!({"maxTokens":8,"maxRetries":3,"timeoutMs":if pi {1} else {60000}}),
            )
            .unwrap(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                host.clone(),
                host.clone(),
            ),
        },
    };
    (adapter, request)
}
fn assert_result(
    case: &Value,
    events: Vec<AssistantMessageEvent>,
    result: &AssistantMessage,
    observed: &Observed,
    host: &Host,
) {
    assert_eq!(
        json!([
            is_context_overflow(result, None),
            is_retryable_assistant_error(result)
        ]),
        case["classifiers"]
    );
    let normalize = |mut events: Value| {
        for event in events.as_array_mut().unwrap() {
            if event["type"] == "error" {
                event["error"]["errorMessage"] = json!("<safe>");
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
        "{} {}",
        case["family"],
        case["mode"]
    );
    assert_eq!(json!(*observed.payloads.lock().unwrap()), case["payloads"]);
    let sleeps = host.sleeps.lock().unwrap();
    if case["family"] == "pi-messages" {
        assert!(sleeps.is_empty());
    } else {
        assert_eq!(*sleeps, vec![Duration::from_secs(60)]);
    }
}

#[test]
fn pinned_callbacks_project_own_records_before_body_and_preserve_preconstructed_mode() {
    let corpus = corpus();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 14);
    for case in corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["network"] == false)
    {
        let observed = Arc::new(Observed::default());
        let host = Arc::new(Host::default());
        let mut headers = HttpHeaders::new();
        for pair in case["headers"].as_array().unwrap() {
            // This binding returns already-constructed Headers: source trims before joining.
            headers
                .append(
                    pair[0].as_str().unwrap(),
                    pair[1].as_str().unwrap().trim_matches([' ', '\t']),
                )
                .unwrap();
        }
        let raw = headers.clone();
        let response = HttpResponse {
            status: case["status"].as_u64().unwrap() as u16,
            status_text: "Synthetic".into(),
            headers,
            body: Box::new(Body {
                chunks: vec![case["body"].as_str().unwrap().as_bytes().to_vec()].into(),
                observed: observed.clone(),
            }),
        };
        let client = Arc::new(Client {
            response: Mutex::new(Some(response)),
            observed: observed.clone(),
        });
        let transport = HttpTransport::new(client, host.clone())
            .with_fetch_response_source(FetchResponseSource::Preconstructed);
        let (adapter, request) = setup(case, transport, &observed, &host);
        let mut stream = adapter.stream(request);
        ready(host.tasks.lock().unwrap().pop().unwrap());
        let result = ready(stream.result());
        let mut events = vec![];
        while let Some(event) = ready(stream.next()) {
            events.push(event);
        }
        assert_result(case, events, &result, &observed, &host);
        assert_eq!(json!(*observed.heads.lock().unwrap()), case["heads"]);
        assert_eq!(json!(*observed.order.lock().unwrap()), case["order"]);
        assert_eq!(json!(*observed.requests.lock().unwrap()), case["requests"]);
        assert_eq!(
            observed.chunks.load(Ordering::SeqCst),
            case["reads"].as_u64().unwrap() as usize
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
        assert_eq!(
            raw.iter_for_wire()
                .filter(|(k, _)| k.eq_ignore_ascii_case("set-cookie"))
                .count(),
            2
        );
        assert!(raw.get_for_wire("__proto__").is_some());
    }
}

#[cfg(feature = "native")]
#[test]
fn native_redirect_compressed_success_error_and_cancellation_match_full_source_wrappers() {
    let mut actual_heads = Vec::new();
    let mut expected_heads = Vec::new();
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    fn capture(socket: &mut TcpStream) -> (Value, String) {
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut head = vec![];
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
                let (k, v) = line.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        (
            json!({"path":head.lines().next().unwrap().split_whitespace().nth(1).unwrap(),"body":serde_json::from_slice::<Value>(&body).unwrap()}),
            head,
        )
    }
    fn closed(socket: &mut TcpStream) {
        match socket.read(&mut [0]) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => panic!("expected EOF/reset, not timeout: {other:?}"),
        }
    }
    for case in corpus()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["network"] == true)
    {
        let case = case.clone();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_case = case.clone();
        let server = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let initial = capture(&mut first);
            first.write_all(b"HTTP/1.1 307 Redirect\r\nLocation: /final\r\nContent-Encoding: not-a-codec\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            closed(&mut first);
            let (mut socket, _) = listener.accept().unwrap();
            let final_request = capture(&mut socket);
            let encoded = server_case["encoded"]
                .as_str()
                .unwrap()
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            let cancel = server_case["mode"] == "cancel";
            write!(
                socket,
                "HTTP/1.1 {} Synthetic\r\n",
                server_case["status"].as_u64().unwrap()
            )
            .unwrap();
            for pair in server_case["headers"].as_array().unwrap() {
                write!(
                    socket,
                    "{}: {}\r\n",
                    pair[0].as_str().unwrap(),
                    pair[1].as_str().unwrap()
                )
                .unwrap();
            }
            socket.write_all(b"Connection: close\r\n").unwrap();
            if cancel {
                socket
                    .write_all(b"Transfer-Encoding: chunked\r\n\r\n")
                    .unwrap();
            } else {
                write!(socket, "Content-Length: {}\r\n\r\n", encoded.len()).unwrap();
            }
            for chunk in encoded.chunks(3) {
                if cancel {
                    write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                }
                socket.write_all(chunk).unwrap();
                if cancel {
                    socket.write_all(b"\r\n").unwrap();
                }
                socket.flush().unwrap();
            }
            closed(&mut socket);
            (initial, final_request)
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let observed = Arc::new(Observed::default());
        let host = Arc::new(Host::default());
        let transport = HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            host.clone(),
        );
        let (adapter, mut request) = setup(&case, transport, &observed, &host);
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/base")).unwrap();
        let token = request.options.runtime.cancellation.clone();
        let mut stream = adapter.stream(request);
        let task = host.tasks.lock().unwrap().pop().unwrap();
        let (events, result) = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(task);
                let mut events = vec![];
                while let Some(event) = stream.next().await {
                    if case["mode"] == "cancel"
                        && matches!(event, AssistantMessageEvent::TextDelta { .. })
                    {
                        token.cancel();
                    }
                    events.push(event);
                }
                worker.await.unwrap();
                (events, stream.result().await)
            })
            .await
            .unwrap()
        });
        let (initial, final_request) = server.join().unwrap();
        assert_eq!(json!([initial.0, final_request.0]), case["requests"]);
        for head in [&initial.1, &final_request.1] {
            let lower = head.to_ascii_lowercase();
            assert!(lower.contains("accept-encoding: gzip, deflate\r\n"));
            assert!(lower.contains("authorization: bearer synthetic-key\r\n"));
        }
        assert_result(&case, events, &result, &observed, &host);
        assert_eq!(*observed.order.lock().unwrap(), vec!["payload", "response"]);
        actual_heads.push(json!(*observed.heads.lock().unwrap()));
        expected_heads.push(case["heads"].clone());
    }
    assert_eq!(actual_heads, expected_heads);
}
