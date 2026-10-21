//! Process-local input identity. These values never enter provider/event serialization.
use crate::{AgentError, AgentEvent, AgentMessage, BoxFuture, CancellationToken};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Opaque identity of a distinct input admission, independent of its contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InputId(u64);
impl InputId {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
impl Default for InputId {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Display for InputId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "input-{}", self.0)
    }
}

#[derive(Clone)]
pub struct InputAdmission {
    pub id: InputId,
    pub cancellation: CancellationToken,
    /// Called outside Agent locks when this exact input fails before emission/commit.
    pub on_rejected: Option<Arc<dyn Fn(AgentError) + Send + Sync>>,
}
impl std::fmt::Debug for InputAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputAdmission")
            .field("id", &self.id)
            .field("cancellation", &self.cancellation)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug)]
pub struct AdmittedMessage {
    pub message: AgentMessage,
    pub input: Option<InputAdmission>,
}
impl From<AgentMessage> for AdmittedMessage {
    fn from(message: AgentMessage) -> Self {
        Self {
            message,
            input: None,
        }
    }
}
impl AdmittedMessage {
    pub fn is_cancelled(&self) -> bool {
        self.input
            .as_ref()
            .is_some_and(|i| i.cancellation.is_cancelled())
    }
}
pub type InputEventSink = Arc<
    dyn Fn(
            AgentEvent,
            Option<InputId>,
            CancellationToken,
        ) -> BoxFuture<'static, Result<(), AgentError>>
        + Send
        + Sync,
>;
