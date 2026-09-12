//! Per-cleanup-scope async close observation over accepted Codex retirement.
use pi_ai_core::*;
use pi_ai_providers::api::openai_codex_responses::{
    OpenAICodexResponsesApi, OpenAICodexWebSocketDebugStats,
};
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
pub struct CodexSessionResources {
    module: Arc<OpenAICodexResponsesApi>,
}
impl CodexSessionResources {
    pub fn new(module: Arc<OpenAICodexResponsesApi>) -> Self {
        Self { module }
    }
    pub fn module(&self) -> Arc<OpenAICodexResponsesApi> {
        self.module.clone()
    }
    pub fn begin_close(
        &self,
        session: Option<&str>,
        runtime: RequestRuntime,
    ) -> BoxFuture<'static, Result<(), SessionResourceError>> {
        let module = self.module.clone();
        let closing = module.close_openai_codex_websocket_sessions(session, runtime);
        Box::pin(async move {
            let result = closing.await.map_err(SessionResourceError::with_source);
            drop(module);
            result
        })
    }
    pub fn abort(&self, session: Option<&str>) {
        self.module.abort_websocket_sessions(session)
    }
    pub fn stats(&self, session: &str) -> Option<OpenAICodexWebSocketDebugStats> {
        self.module.get_openai_codex_websocket_debug_stats(session)
    }
    pub fn reset_stats(&self, session: Option<&str>) {
        self.module
            .reset_openai_codex_websocket_debug_stats(session)
    }
}
type CloseResult = Result<(), Arc<SessionResourceError>>;
#[derive(Default)]
struct CompletionState {
    result: Option<CloseResult>,
    next: u128,
    waiters: BTreeMap<u128, Waker>,
}
#[derive(Default)]
struct Completion(Mutex<CompletionState>);
impl Completion {
    fn finish(&self, result: CloseResult) {
        let waiters = {
            let mut s = self.0.lock().unwrap_or_else(|p| p.into_inner());
            if s.result.is_some() {
                drop(s);
                drop(result);
                return;
            }
            s.result = Some(result);
            std::mem::take(&mut s.waiters)
        };
        for (_, waker) in waiters {
            waker.wake();
        }
    }
}
/// An independently awaitable observation of one particular close attempt.
pub struct CodexCloseTicket {
    completion: Arc<Completion>,
    waiter: Option<u128>,
}
impl Clone for CodexCloseTicket {
    fn clone(&self) -> Self {
        Self {
            completion: self.completion.clone(),
            waiter: None,
        }
    }
}
impl Future for CodexCloseTicket {
    type Output = CloseResult;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut replacement = Some(cx.waker().clone());
        let (result, previous) = {
            let mut s = this.completion.0.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(result) = &s.result {
                (Poll::Ready(result.clone()), None)
            } else {
                let id = if let Some(id) = this.waiter {
                    Some(id)
                } else {
                    let next = s.next.checked_add(1);
                    if let Some(next) = next {
                        s.next = next;
                        this.waiter = Some(next);
                    }
                    next
                };
                if let Some(id) = id {
                    (
                        Poll::Pending,
                        s.waiters.insert(id, replacement.take().expect("new waker")),
                    )
                } else {
                    (
                        Poll::Ready(Err(Arc::new(SessionResourceError::new()))),
                        None,
                    )
                }
            }
        };
        drop(previous);
        drop(replacement);
        result
    }
}
impl Drop for CodexCloseTicket {
    fn drop(&mut self) {
        let old = self.waiter.and_then(|id| {
            self.completion
                .0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .waiters
                .remove(&id)
        });
        drop(old);
    }
}
struct CloseGuard {
    completion: Arc<Completion>,
    work: Option<BoxFuture<'static, Result<(), SessionResourceError>>>,
    finished: bool,
}
impl CloseGuard {
    fn finish(&mut self, result: Result<(), SessionResourceError>) {
        drop(self.work.take());
        self.finished = true;
        self.completion.finish(result.map_err(Arc::new));
    }
}
impl Drop for CloseGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Err(SessionResourceError::new()));
        }
    }
}
struct Cleanup {
    resources: CodexSessionResources,
    runtime: RequestRuntime,
    tickets: Arc<Mutex<Vec<CodexCloseTicket>>>,
}
impl SessionResource for Cleanup {
    fn cleanup(&self, session: Option<&str>) -> Result<(), SessionResourceError> {
        let work = self.resources.begin_close(session, self.runtime.clone());
        let completion = Arc::new(Completion::default());
        self.tickets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(CodexCloseTicket {
                completion: completion.clone(),
                waiter: None,
            });
        let mut guard = CloseGuard {
            completion: completion.clone(),
            work: Some(work),
            finished: false,
        };
        let task = Box::pin(async move {
            let result = guard.work.as_mut().expect("owned close future").await;
            guard.finish(result);
        });
        if let Err(error) = self.runtime.spawner.spawn(task) {
            completion.finish(Err(Arc::new(SessionResourceError::new())));
            return Err(SessionResourceError::with_source(error));
        }
        Ok(())
    }
}
/// One core registration. Successful core cleanup consumes it; retain tickets,
/// and explicitly register another scope for later cleanup sessions.
pub struct CodexCleanupScope {
    handle: SessionResourceHandle,
    tickets: Arc<Mutex<Vec<CodexCloseTicket>>>,
}
impl CodexCleanupScope {
    pub fn take_close_tickets(&self) -> Vec<CodexCloseTicket> {
        std::mem::take(&mut *self.tickets.lock().unwrap_or_else(|p| p.into_inner()))
    }
    pub fn remove(&mut self) -> bool {
        self.handle.remove()
    }
}
pub fn register_codex_cleanup_scope(
    registry: &SessionResourceRegistry,
    module: Arc<OpenAICodexResponsesApi>,
    runtime: RequestRuntime,
) -> Result<CodexCleanupScope, SessionResourceRegistryError> {
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let handle = registry.register(Arc::new(Cleanup {
        resources: CodexSessionResources::new(module),
        runtime,
        tickets: tickets.clone(),
    }))?;
    Ok(CodexCleanupScope { handle, tickets })
}
macro_rules! debug {($($ty:ty),*)=>{$(impl fmt::Debug for $ty{fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{f.write_str(concat!(stringify!($ty),"([REDACTED])"))}})*};}
debug!(CodexSessionResources, CodexCloseTicket, CodexCleanupScope);
