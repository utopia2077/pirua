//! Optional, process-local request payload and passive response observation.

use crate::{
    AssistantStream, BoxFuture, FiniteF64, Model, ModelsError, SimpleStreamRequest,
    TextStreamRequest,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

/// Trusted transform of the provider-logical payload, before its consumption.
pub trait RequestPayloadTransform: Send + Sync {
    /// `None` preserves mutations; `Some` replaces the complete payload.
    fn transform<'a>(
        &'a self,
        model: &'a Model,
        payload: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Result<Option<serde_json::Value>, ModelsError>>;
}

/// Per-call options; reusing the handler does not reuse response state.
#[derive(Clone, Default)]
pub struct RequestLifecycleOptions {
    /// Optional logical payload transform.
    pub payload: Option<Arc<dyn RequestPayloadTransform>>,
}

/// An observed header value, before consumer-specific sanitization.
#[derive(Clone, PartialEq)]
pub enum RequestResponseHeaderValue {
    /// Text retained exactly from the provider observation.
    Text(String),
    /// A finite SDK number, without string coercion.
    Number(FiniteF64),
}

/// One detached complete response observation.
#[derive(Clone, PartialEq)]
pub struct RequestResponseHead {
    /// Received response status.
    pub status: u16,
    /// Ordered provider observation header pairs.
    pub headers: Vec<(String, RequestResponseHeaderValue)>,
}

/// Passive reader, retaining only an independent metadata cell.
#[derive(Clone)]
pub struct ResponseMetadataReader(Arc<Mutex<Option<RequestResponseHead>>>);
impl ResponseMetadataReader {
    /// Returns a detached latest successful observation, if any.
    pub fn snapshot(&self) -> Option<RequestResponseHead> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Passive recorder; publishing executes no host callback or asynchronous work.
#[derive(Clone)]
pub struct ResponseMetadataRecorder(Arc<Mutex<Option<RequestResponseHead>>>);
impl ResponseMetadataRecorder {
    /// Atomically replaces the complete observation.
    pub fn record(&self, head: RequestResponseHead) {
        let previous = self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(head);
        drop(previous);
    }
}

/// Owned invocation bound to one adapter dispatch.
pub struct RequestLifecycleInvocation {
    /// Trusted optional request transform.
    pub payload: Option<Arc<dyn RequestPayloadTransform>>,
    /// Passive response observation destination.
    pub recorder: ResponseMetadataRecorder,
}
impl RequestLifecycleInvocation {
    /// Allocates independent response state for this invocation.
    pub fn new(options: RequestLifecycleOptions) -> (Self, ResponseMetadataReader) {
        let cell = Arc::new(Mutex::new(None));
        (
            Self {
                payload: options.payload,
                recorder: ResponseMetadataRecorder(cell.clone()),
            },
            ResponseMetadataReader(cell),
        )
    }
}

/// Validated ordinary stream plus its passive metadata reader.
pub struct ObservedAssistantStream {
    /// Existing validated assistant event/result stream.
    pub stream: AssistantStream,
    /// Latest successfully observed head; not a worker-completion ticket.
    pub metadata: ResponseMetadataReader,
}

/// Optional adapter capability; ordinary stream methods never activate it.
pub trait RequestLifecycleAdapter: Send + Sync {
    /// Starts a raw stream bound to one invocation.
    fn stream_with_lifecycle(
        &self,
        request: TextStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream;
    /// Starts a simple stream bound to one invocation.
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation,
    ) -> AssistantStream;
}

macro_rules! redacted_debug {
    ($($ty:ty),+ $(,)?) => {$(impl fmt::Debug for $ty {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(concat!(stringify!($ty), "([REDACTED])"))
        }
    })+};
}
redacted_debug!(
    RequestLifecycleOptions,
    RequestResponseHeaderValue,
    RequestResponseHead,
    ResponseMetadataReader,
    ResponseMetadataRecorder,
    RequestLifecycleInvocation,
    ObservedAssistantStream
);
