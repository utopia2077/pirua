use pi_ai_core::*;
use pi_ai_transport::*;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
struct Host;
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("no spawn")
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no retry")
    }
}
fn runtime() -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), Arc::new(Host), Arc::new(Host))
}
fn ready<F: Future>(f: F) -> F::Output {
    match Box::pin(f)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("pending"),
    }
}
struct Body(Arc<AtomicUsize>);
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    absent: bool,
    status: u16,
    closed: Arc<AtomicUsize>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            Ok(HttpResponse {
                status: self.status,
                status_text: String::new(),
                headers: HttpHeaders::new(),
                body: if self.absent {
                    Box::new(AbsentResponseBody)
                } else {
                    Box::new(Body(self.closed.clone()))
                },
            })
        })
    }
}
impl CustomFetch for Client {
    fn fetch<'a>(
        &'a self,
        _: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            Ok(HttpResponse {
                status: self.status,
                status_text: String::new(),
                headers: HttpHeaders::new(),
                body: if self.absent {
                    Box::new(AbsentResponseBody)
                } else {
                    Box::new(Body(self.closed.clone()))
                },
            })
        })
    }
}
fn request() -> HttpRequest {
    HttpRequest {
        endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
        method: "POST".into(),
        headers: HttpHeaders::new(),
        body: vec![],
        proxy: None,
    }
}
#[test]
fn explicit_absence_and_present_empty_are_immutable_through_policy_and_close() {
    for absent in [false, true] {
        let client = Arc::new(Client {
            absent,
            status: 200,
            closed: Arc::new(AtomicUsize::new(0)),
        });
        let transport = HttpTransport::new_custom(client, Arc::new(Host));
        let request = request();
        let rt = runtime();
        let mut response = ready(transport.send_fetch(
            &request,
            &rt,
            RetryProfile::None,
            TimeoutProfile::WholeResponse(Duration::from_secs(1)),
        ))
        .unwrap();
        let expected = if absent {
            ResponseBodyPresence::Absent
        } else {
            ResponseBodyPresence::Present
        };
        assert_eq!(response.body.presence(), expected);
        assert!(ready(response.body.next(&rt)).unwrap().is_none());
        assert_eq!(response.body.presence(), expected);
        response.body.close();
        assert_eq!(response.body.presence(), expected);
    }
}
#[test]
fn raw_and_preconstructed_keep_selected_presence_while_network_suppression_marks_absent() {
    for raw in [false, true] {
        for preconstructed in [false, true] {
            let closed = Arc::new(AtomicUsize::new(0));
            let client = Arc::new(Client {
                absent: false,
                status: 204,
                closed: closed.clone(),
            });
            let transport = HttpTransport::new(client, Arc::new(Host)).with_fetch_response_source(
                if preconstructed {
                    FetchResponseSource::Preconstructed
                } else {
                    FetchResponseSource::RawNetwork
                },
            );
            let request = request();
            let rt = runtime();
            let mut response = ready(if raw {
                transport.send(&request, &rt, RetryProfile::None, TimeoutProfile::None)
            } else {
                transport.send_fetch(&request, &rt, RetryProfile::None, TimeoutProfile::None)
            })
            .unwrap();
            let suppressed = !raw && !preconstructed;
            assert_eq!(
                response.body.presence(),
                if suppressed {
                    ResponseBodyPresence::Absent
                } else {
                    ResponseBodyPresence::Present
                }
            );
            assert_eq!(closed.load(Ordering::SeqCst), usize::from(suppressed));
            response.body.close();
        }
    }
}
#[cfg(feature = "native")]
#[test]
fn actual_native_fetch_body_presence_matches_node_after_eof_and_close() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };
    let source: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/response-body-presence/trace.json"
    ))
    .unwrap();
    let engine = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for case in source["cases"].as_array().unwrap() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = case["status"].as_u64().unwrap();
        let (released, release_observed) = tokio::sync::oneshot::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(4)))
                .unwrap();
            let mut head = vec![];
            while !head.ends_with(b"\r\n\r\n") {
                let mut b = [0];
                socket.read_exact(&mut b).unwrap();
                head.push(b[0]);
                assert!(head.len() < 16384);
            }
            socket
                .write_all(
                    format!("HTTP/1.1 {status} Synthetic\r\ncontent-length: 0\r\n\r\n").as_bytes(),
                )
                .unwrap();
            assert_eq!(
                socket
                    .read(&mut [0])
                    .expect("physical EOF read must succeed"),
                0,
                "body retirement must close the physical peer"
            );
            released.send(()).unwrap();
        });
        let transport = HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Host),
        );
        let mut request = request();
        request.endpoint = RequestEndpoint::new_resolved(format!("http://{address}/body")).unwrap();
        request.method = case["method"].as_str().unwrap().into();
        let rt = runtime();
        engine.block_on(async {
            let mut response = transport
                .send_fetch(&request, &rt, RetryProfile::None, TimeoutProfile::None)
                .await
                .unwrap();
            let expected = if case["absent"] == true {
                ResponseBodyPresence::Absent
            } else {
                ResponseBodyPresence::Present
            };
            assert_eq!(response.body.presence(), expected, "{case}");
            assert!(response.body.next(&rt).await.unwrap().is_none());
            assert_eq!(response.body.presence(), expected);
            response.body.close();
            assert_eq!(response.body.presence(), expected);
            tokio::time::timeout(Duration::from_secs(5), release_observed)
                .await
                .expect("peer did not observe EOF while response owner remained alive")
                .expect("peer failed its physical EOF assertion");
            assert_eq!(response.body.presence(), expected);
        });
        server.join().unwrap();
    }
}
