#![allow(dead_code)]
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_session::{jsonl::JsonlOptions, memory::StorageClock, repo::jsonl::*};
pub use std::sync::Arc;
pub fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("file repository suspended"),
    }
}
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
pub struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        "00000000-0001-7000-8000-000000000001".into()
    }
}
pub fn repository(path: &std::path::Path) -> JsonlSessionRepo {
    JsonlSessionRepo::new(
        path,
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
        Arc::new(Ids),
        Arc::new(Clock),
        Arc::new(pi_telemetry::NoopTelemetryContext),
        JsonlOptions::default(),
    )
    .unwrap()
}
