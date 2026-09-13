#![allow(dead_code)]
pub use pi_agent_schema::session_values::*;
pub use pi_agent_schema::*;
pub use pi_agent_session::{codec::SessionCodec, legacy_v3::*, memory::*};
pub use serde_json::{Value, json};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
pub fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
pub fn ready<F: Future>(future: F) -> F::Output {
    struct Thread(std::thread::Thread);
    impl Wake for Thread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Thread(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(result) => return result,
            Poll::Pending => std::thread::park(),
        }
    }
}
pub fn codec() -> SessionCodec {
    SessionCodec::new(coding_message_options(), ProviderHeaderPolicy::default()).unwrap()
}
pub fn source(rows: Vec<Value>) -> Vec<u8> {
    let mut bytes = vec![];
    for row in rows {
        bytes.extend(serde_json::to_vec(&row).unwrap());
        bytes.push(b'\n');
    }
    bytes
}
pub fn header() -> Value {
    json!({"type":"session","version":3,"id":"test-session","timestamp":"1970-01-01T00:00:00Z","cwd":"/project"})
}
pub fn node(id: &str, parent: Option<&str>, kind: &str) -> Value {
    json!({"type":kind,"id":id,"parentId":parent,"timestamp":"1970-01-01T00:00:01Z"})
}
pub fn user(id: &str, parent: Option<&str>) -> Value {
    let mut value = node(id, parent, "message");
    value["message"] = json!({"role":"user","content":"text","timestamp":77});
    value
}
pub fn usage() -> Value {
    json!({"input":1,"output":2,"cacheRead":3,"cacheWrite":4,"totalTokens":99,"cost":{"input":0.25,"output":0.5,"cacheRead":0.75,"cacheWrite":1,"total":-0.125}})
}
pub fn assistant(id: &str, parent: Option<&str>) -> Value {
    let mut value = node(id, parent, "message");
    value["message"] = json!({"role":"assistant","content":[{"type":"text","text":"answer","textSignature":"keep"}],"api":"a","provider":"p","model":"m","usage":usage(),"stopReason":"stop","timestamp":78});
    value
}
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(12345))
    }
}
pub struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        "00000000-3039-7000-8000-000000000001".into()
    }
}
