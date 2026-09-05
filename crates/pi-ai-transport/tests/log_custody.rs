#![cfg(feature = "native")]
//! A privileged host logger witness in its own test process. Captured data is
//! synthetic and retained only in test memory, never emitted to a real log sink.
use pi_ai_core::{
    BoxFuture, CancellationToken, RequestEndpoint, RequestRuntime, SpawnError, TaskSpawner,
};
use pi_ai_transport::*;
use std::{
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio_tungstenite::tungstenite::{self, Message};

struct HostLogger {
    filter: AtomicBool,
    records: Mutex<Vec<String>>,
}
static LOGGER: HostLogger = HostLogger {
    filter: AtomicBool::new(false),
    records: Mutex::new(Vec::new()),
};
impl log::Log for HostLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        !self.filter.load(Ordering::SeqCst)
            || ![
                "tungstenite",
                "tokio_tungstenite",
                "reqwest",
                "hyper",
                "hyper_util",
                "h2",
                "rustls",
                "tokio_rustls",
                "hyper_rustls",
                "rustls_platform_verifier",
            ]
            .iter()
            .any(|prefix| {
                metadata.target() == *prefix
                    || metadata.target().starts_with(&format!("{prefix}::"))
            })
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            self.records
                .lock()
                .unwrap()
                .push(format!("{}: {}", record.target(), record.args()));
        }
    }
    fn flush(&self) {}
}
struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}

#[test]
fn default_quiet_and_host_filtered_logs_keep_wire_out_of_observability() {
    let native = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let runtime = RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        Arc::new(TokioClock::new(native.handle().clone())),
        Arc::new(Spawner),
    );
    assert_eq!(log::max_level(), log::LevelFilter::Off);
    let connector = NativeWebSocketConnector::new(native.handle().clone()).unwrap();
    assert_eq!(
        log::max_level(),
        log::LevelFilter::Off,
        "library must not configure host logging"
    );
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    for filtered in [false, true] {
        LOGGER.filter.store(filtered, Ordering::SeqCst);
        LOGGER.records.lock().unwrap().clear();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut socket = tungstenite::accept(stream).unwrap();
            let message = socket.read().unwrap();
            socket
                .send(Message::Text(message.into_text().unwrap()))
                .unwrap();
        });
        let request = WebSocketRequest {
            endpoint: RequestEndpoint::new_resolved(format!("ws://{address}/responses")).unwrap(),
            headers: HttpHeaders::new(),
            proxy: None,
            connect_timeout: Some(Duration::from_secs(2)),
            max_message_bytes: 1024,
            max_frame_bytes: 1024,
        };
        native.block_on(async {
            let mut connection=connector.connect(&request,&runtime).await.unwrap();
            connection.send(WebSocketFrame::Text("SYNTHETIC_WIRE_LOG_MARKER".into()),&runtime).await.unwrap();
            assert!(matches!(connection.next(&runtime,None).await.unwrap(),Some(WebSocketFrame::Text(s))if s=="SYNTHETIC_WIRE_LOG_MARKER"));connection.close();
        });
        server.join().unwrap();
        log::info!(target:"host::control","safe-control-marker");
        let records = LOGGER.records.lock().unwrap();
        assert!(records.iter().any(|s| s.contains("safe-control-marker")));
        assert_eq!(
            records
                .iter()
                .any(|s| s.contains("SYNTHETIC_WIRE_LOG_MARKER")),
            !filtered,
            "unfiltered dependency logging is privileged wire instrumentation"
        );
    }
    log::set_max_level(log::LevelFilter::Off);
}
