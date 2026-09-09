use super::*;
use std::{
    future::{Future, poll_fn},
    sync::atomic::{AtomicUsize, Ordering},
    task::{Poll, Waker},
};

#[derive(Clone)]
pub struct OpenAICodexWebSocketDebugStats {
    pub requests: SafeUnsigned,
    pub connections_created: SafeUnsigned,
    pub connections_reused: SafeUnsigned,
    pub cached_context_requests: SafeUnsigned,
    pub store_true_requests: SafeUnsigned,
    pub full_context_requests: SafeUnsigned,
    pub delta_requests: SafeUnsigned,
    pub last_input_items: SafeUnsigned,
    pub last_delta_input_items: Option<SafeUnsigned>,
    pub last_previous_response_id: Option<String>,
    pub websocket_failures: SafeUnsigned,
    pub sse_fallbacks: SafeUnsigned,
    pub websocket_fallback_active: Option<bool>,
    pub last_websocket_error: Option<String>,
}
impl Default for OpenAICodexWebSocketDebugStats {
    fn default() -> Self {
        let zero = SafeUnsigned::new(0).expect("zero");
        Self {
            requests: zero,
            connections_created: zero,
            connections_reused: zero,
            cached_context_requests: zero,
            store_true_requests: zero,
            full_context_requests: zero,
            delta_requests: zero,
            last_input_items: zero,
            websocket_failures: zero,
            sse_fallbacks: zero,
            last_delta_input_items: None,
            last_previous_response_id: None,
            websocket_fallback_active: None,
            last_websocket_error: None,
        }
    }
}
impl fmt::Debug for OpenAICodexWebSocketDebugStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenAICodexWebSocketDebugStats([REDACTED])")
    }
}
#[derive(Clone)]
pub(super) struct Continuation {
    pub body: Value,
    pub id: String,
    pub items: Vec<Value>,
}
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Open,
    Graceful,
    Aborted,
}
struct SlotState {
    socket: Option<Box<dyn PersistentWebSocket>>,
    mode: Mode,
    waiter: Option<Waker>,
    // Charge follows the retained slot, including detached close futures.
    permit: Arc<Permit>,
}
struct Slot(Mutex<SlotState>, CancellationToken);
struct Retirement(Vec<Arc<Slot>>);
impl Drop for Retirement {
    fn drop(&mut self) {
        for slot in &self.0 {
            slot.abort();
        }
    }
}
struct Permit(Weak<Resource>);
impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(resource) = self.0.upgrade() {
            resource.active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Slot {
    fn new(socket: Box<dyn PersistentWebSocket>, permit: Arc<Permit>) -> Arc<Self> {
        Arc::new(Self(
            Mutex::new(SlotState {
                socket: Some(socket),
                mode: Mode::Open,
                waiter: None,
                permit,
            }),
            CancellationToken::new(),
        ))
    }
    // Return the old charge so its destructor runs outside both cache and slot
    // locks. Cancelled idle tasks retain their own shared charge until reaped.
    fn replace_permit(&self, permit: Arc<Permit>) -> Arc<Permit> {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::replace(&mut state.permit, permit)
    }
    fn loan(self: &Arc<Self>) -> Result<Loan, Failure> {
        let socket = {
            let mut s = self.0.lock().unwrap_or_else(|p| p.into_inner());
            if s.mode != Mode::Open {
                return Err(Failure::Transport(TransportErrorKind::Connection));
            }
            s.socket
                .take()
                .ok_or(Failure::Transport(TransportErrorKind::Connection))?
        };
        Ok(Loan {
            slot: self.clone(),
            socket: Some(socket),
        })
    }
    fn abort(&self) {
        let (socket, waker) = {
            let mut s = self.0.lock().unwrap_or_else(|p| p.into_inner());
            s.mode = Mode::Aborted;
            (s.socket.take(), s.waiter.take())
        };
        self.1.cancel();
        if let Some(mut socket) = socket {
            socket.abort();
        }
        if let Some(w) = waker {
            w.wake();
        }
    }
    async fn close(
        self: Arc<Self>,
        runtime: RequestRuntime,
        grace: Duration,
        reason: &'static str,
    ) -> Result<(), TransportError> {
        struct Guard(Arc<Slot>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let guard = Guard(self.clone());
        if runtime.cancellation.is_cancelled() {
            return Err(TransportError::new(TransportErrorKind::Cancelled));
        }
        if grace.is_zero() {
            return Err(TransportError::new(TransportErrorKind::Timeout));
        }
        {
            let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
            if state.mode == Mode::Aborted {
                return Ok(());
            }
            state.mode = Mode::Graceful;
        }
        let mut runtime = runtime;
        runtime.cancellation =
            CancellationToken::linked(&[runtime.cancellation.clone(), self.1.clone()]);
        let mut cancel = Box::pin(runtime.cancellation.cancelled());
        let clock = runtime.clock.clone();
        let timer_token = CancellationToken::new();
        let started_at = clock.now_unix_ms();
        let mut timer = clock.sleep(grace, &timer_token);
        let mut socket = poll_fn(|cx| {
            if cancel.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(TransportError::new(TransportErrorKind::Cancelled)));
            }
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(TransportError::new(TransportErrorKind::Timeout)));
            }
            let registered = cx.waker().clone();
            let (result, old) = {
                let mut s = self.0.lock().unwrap_or_else(|p| p.into_inner());
                if s.mode == Mode::Aborted {
                    (
                        Poll::Ready(Err(TransportError::new(TransportErrorKind::Connection))),
                        s.waiter.take(),
                    )
                } else if let Some(socket) = s.socket.take() {
                    (Poll::Ready(Ok(socket)), s.waiter.take())
                } else {
                    (Poll::Pending, s.waiter.replace(registered))
                }
            };
            drop(old);
            result
        })
        .await?;
        let remaining = grace.saturating_sub(Duration::from_millis(
            clock.now_unix_ms().saturating_sub(started_at),
        ));
        let mut shutdown = socket.shutdown(1000, reason.to_owned(), &runtime, remaining);
        let result = poll_fn(|cx| {
            if cancel.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(TransportError::new(TransportErrorKind::Cancelled)));
            }
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(TransportError::new(TransportErrorKind::Timeout)));
            }
            let result = shutdown.as_mut().poll(cx);
            if runtime.cancellation.is_cancelled() {
                Poll::Ready(Err(TransportError::new(TransportErrorKind::Cancelled)))
            } else {
                result
            }
        })
        .await;
        drop(shutdown);
        drop(socket);
        drop(guard);
        result
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(mut socket) = self
            .0
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .socket
            .take()
        {
            socket.abort();
        }
    }
}
struct Loan {
    slot: Arc<Slot>,
    socket: Option<Box<dyn PersistentWebSocket>>,
}
impl Drop for Loan {
    fn drop(&mut self) {
        let Some(mut socket) = self.socket.take() else {
            return;
        };
        let (waker, _aborted) = {
            let mut s = self.slot.0.lock().unwrap_or_else(|p| p.into_inner());
            if s.mode == Mode::Aborted {
                (s.waiter.take(), true)
            } else {
                s.socket = Some(socket);
                let w = s.waiter.take();
                drop(s);
                if let Some(w) = w {
                    w.wake();
                }
                return;
            }
        };
        socket.abort();
        if let Some(w) = waker {
            w.wake();
        }
    }
}
struct Entry {
    slot: Arc<Slot>,
    busy: bool,
    born: u64,
    id: u64,
    idle: CancellationToken,
    continuation: Option<Continuation>,
}
#[derive(Default)]
struct State {
    slots: Vec<Weak<Slot>>,
    entries: BTreeMap<(String, String), Entry>,
    stats: BTreeMap<String, OpenAICodexWebSocketDebugStats>,
    sticky: BTreeSet<String>,
    next: u64,
}
pub(super) struct Resource {
    state: Mutex<State>,
    limits: CodexResourceLimits,
    active: AtomicUsize,
    lifetime: CancellationToken,
}
pub(super) struct ResourceOwner(pub Weak<Resource>);
impl Drop for ResourceOwner {
    fn drop(&mut self) {
        if let Some(resource) = self.0.upgrade() {
            resource.lifetime.cancel();
            resource.abort(None);
        }
    }
}
impl Resource {
    pub fn fresh_account(&self) -> Result<u64, Failure> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.next = s.next.checked_add(1).ok_or(Failure::Limit)?;
        Ok(s.next)
    }
    pub fn retired(&self) -> bool {
        self.lifetime.is_cancelled()
    }
    pub fn new(limits: CodexResourceLimits) -> Self {
        Self {
            state: Mutex::new(State::default()),
            limits,
            active: AtomicUsize::new(0),
            lifetime: CancellationToken::new(),
        }
    }
    fn validate_key(&self, session: Option<&str>, account: &str) -> Result<(), Failure> {
        if session.is_some_and(|s| s.len() > self.limits.max_key_bytes)
            || account.len() > self.limits.max_key_bytes
        {
            Err(Failure::Limit)
        } else {
            Ok(())
        }
    }
    pub fn stats(&self, id: &str) -> Option<OpenAICodexWebSocketDebugStats> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stats
            .get(id)
            .cloned()
    }
    pub fn reset(&self, id: Option<&str>) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(id) = id.filter(|s| !s.is_empty()) {
            s.stats.remove(id);
            s.sticky.remove(id);
        } else {
            s.stats.clear();
            s.sticky.clear();
        }
    }
    pub fn sticky(&self, id: Option<&str>) -> bool {
        id.is_some_and(|id| {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .sticky
                .contains(id)
        })
    }
    fn update(
        &self,
        id: Option<&str>,
        f: impl FnOnce(&mut OpenAICodexWebSocketDebugStats) -> Result<(), Failure>,
    ) -> Result<(), Failure> {
        let Some(id) = id.filter(|s| !s.is_empty()) else {
            return Ok(());
        };
        if id.len() > self.limits.max_key_bytes {
            return Err(Failure::Limit);
        }
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !s.stats.contains_key(id) && s.stats.len() >= self.limits.max_stats {
            return Err(Failure::Limit);
        }
        let mut next = s.stats.get(id).cloned().unwrap_or_default();
        f(&mut next)?;
        s.stats.insert(id.to_owned(), next);
        Ok(())
    }
    pub fn fallback(&self, id: Option<&str>) -> Result<(), Failure> {
        let active = self.sticky(id);
        self.update(id, |s| {
            inc(&mut s.sse_fallbacks)?;
            s.websocket_fallback_active = Some(active);
            Ok(())
        })
    }
    pub fn failure(&self, id: Option<&str>) -> Result<(), Failure> {
        self.update(id, |s| {
            inc(&mut s.websocket_failures)?;
            s.websocket_fallback_active = Some(true);
            s.last_websocket_error = Some("WebSocket transport failed".into());
            Ok(())
        })?;
        if let Some(id) = id.filter(|s| !s.is_empty()) {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .sticky
                .insert(id.to_owned());
        }
        Ok(())
    }
    pub fn request_stats(
        &self,
        id: Option<&str>,
        reused: bool,
        cached: bool,
        body: &Value,
    ) -> Result<(), Failure> {
        self.update(id, |s| {
            inc(&mut s.requests)?;
            inc(if reused {
                &mut s.connections_reused
            } else {
                &mut s.connections_created
            })?;
            if cached {
                inc(&mut s.cached_context_requests)?;
            }
            if body.is_null() {
                return Ok(());
            }
            if body["store"] == true {
                inc(&mut s.store_true_requests)?;
            }
            let count = SafeUnsigned::new(body["input"].as_array().map_or(0, |v| v.len() as u64))
                .map_err(|_| Failure::Limit)?;
            s.last_input_items = count;
            if let Some(previous) = body["previous_response_id"]
                .as_str()
                .filter(|s| !s.is_empty())
            {
                inc(&mut s.delta_requests)?;
                s.last_delta_input_items = Some(count);
                s.last_previous_response_id = Some(previous.to_owned());
            } else {
                inc(&mut s.full_context_requests)?;
                s.last_delta_input_items = None;
                s.last_previous_response_id = None;
            }
            Ok(())
        })
    }
    fn remove(&self, key: &(String, String), id: u64) -> Option<Entry> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.entries.get(key).is_some_and(|e| e.id == id) {
            s.entries.remove(key)
        } else {
            None
        }
    }
    fn detach(&self, session: Option<&str>) -> Vec<Entry> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let keys = s
            .entries
            .keys()
            .filter(|(id, _)| {
                session
                    .filter(|s| !s.is_empty())
                    .is_none_or(|wanted| id == wanted)
            })
            .cloned()
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| s.entries.remove(&key))
            .collect()
    }
    pub fn abort(&self, session: Option<&str>) {
        let entries = self.detach(session);
        let slots = if session.filter(|s| !s.is_empty()).is_none() {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .slots
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        for e in entries {
            e.idle.cancel();
            e.slot.abort();
        }
        for slot in slots {
            slot.abort();
        }
    }

    pub fn close(
        &self,
        session: Option<&str>,
        runtime: RequestRuntime,
    ) -> BoxFuture<'static, Result<(), TransportError>> {
        let retirement = Retirement(
            self.detach(session)
                .into_iter()
                .map(|entry| {
                    entry.idle.cancel();
                    entry.slot
                })
                .collect(),
        );
        if retirement.0.is_empty() {
            return Box::pin(async { Ok(()) });
        }
        let grace = self.limits.close_grace;
        Box::pin(async move {
            let retirement = retirement;
            let mut failure = None;
            for slot in &retirement.0 {
                if let Err(e) = slot
                    .clone()
                    .close(runtime.clone(), grace, "debug_close")
                    .await
                {
                    failure.get_or_insert(e);
                }
            }
            failure.map_or(Ok(()), Err)
        })
    }
    fn retire(
        self: &Arc<Self>,
        entry: Entry,
        runtime: &RequestRuntime,
        reason: &'static str,
    ) -> Result<(), Failure> {
        entry.idle.cancel();
        // The slot retains its existing charge throughout retirement. Acquiring
        // another charge here would count the same retained slot twice.
        let retirement = Retirement(vec![entry.slot.clone()]);
        let mut rt = runtime.clone();
        rt.cancellation = CancellationToken::new();
        let grace = self.limits.close_grace;
        let task = Box::pin(async move {
            let _retirement = retirement;
            let _ = entry.slot.close(rt, grace, reason).await;
        });
        runtime
            .spawner
            .spawn(task)
            .map_err(|_| Failure::setup("retire_spawn"))
    }
    pub async fn acquire(
        self: &Arc<Self>,
        session: Option<&str>,
        account: &str,
        connector: &dyn PersistentWebSocketConnector,
        request: &WebSocketRequest,
        runtime: &RequestRuntime,
    ) -> Result<Lease, Failure> {
        let mut linked = runtime.clone();
        linked.cancellation =
            CancellationToken::linked(&[runtime.cancellation.clone(), self.lifetime.clone()]);
        let runtime = &linked;
        if runtime.cancellation.is_cancelled() {
            return Err(Failure::Transport(TransportErrorKind::Cancelled));
        }
        self.validate_key(session, account)?;
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n < self.limits.max_active_leases {
                    n.checked_add(1)
                } else {
                    None
                }
            })
            .map_err(|_| Failure::Limit)?;
        let permit = Arc::new(Permit(Arc::downgrade(self)));
        let key = session
            .filter(|s| !s.is_empty())
            .map(|s| (s.to_owned(), account.to_owned()));
        let cached = key.as_ref().and_then(|key| {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            s.entries.get_mut(key).map(|e| {
                let busy = e.busy;
                if !busy {
                    e.busy = true;
                }
                (
                    e.slot.clone(),
                    e.id,
                    e.born,
                    e.idle.clone(),
                    if busy { None } else { e.continuation.take() },
                    busy,
                )
            })
        });
        let mut one_shot = false;
        if let Some((slot, id, born, idle, continuation, busy)) = cached {
            idle.cancel();
            if busy {
                one_shot = true;
            } else {
                let live = slot.loan().is_ok_and(|loan| {
                    loan.socket
                        .as_ref()
                        .is_some_and(|s| s.state() == WebSocketState::Open)
                });
                if live && runtime.clock.now_unix_ms().saturating_sub(born) < 55 * 60 * 1000 {
                    drop(slot.replace_permit(permit.clone()));
                    return Ok(Lease {
                        resource: Arc::downgrade(self),
                        slot,
                        key,
                        id,
                        reused: true,
                        continuation,
                        keep: false,
                        _permit: permit,
                    });
                }
                if let Some(e) = self.remove(key.as_ref().unwrap(), id) {
                    self.retire(
                        e,
                        runtime,
                        if live { "connection_age_limit" } else { "done" },
                    )?;
                }
            }
        }
        let socket = cancellable(connector.connect(request, runtime), &runtime.cancellation)
            .await
            .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))??;
        let born = runtime.clock.now_unix_ms();
        let slot = Slot::new(socket, permit.clone());
        {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if self.retired() {
                return Err(Failure::Transport(TransportErrorKind::Cancelled));
            }
            s.slots.retain(|slot| slot.strong_count() > 0);
            s.slots.push(Arc::downgrade(&slot));
        }
        let key = if one_shot { None } else { key };
        let mut id = 0;
        if let Some(key) = &key {
            let old = {
                let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if self.retired() {
                    return Err(Failure::Transport(TransportErrorKind::Cancelled));
                }
                if !s.entries.contains_key(key) && s.entries.len() >= self.limits.max_sessions {
                    return Err(Failure::Limit);
                }
                s.next = s.next.checked_add(1).ok_or(Failure::Limit)?;
                id = s.next;
                s.entries.insert(
                    key.clone(),
                    Entry {
                        slot: slot.clone(),
                        busy: true,
                        born,
                        id,
                        idle: CancellationToken::new(),
                        continuation: None,
                    },
                )
            };
            if let Some(e) = old {
                e.idle.cancel();
                e.slot.abort();
            }
        }
        Ok(Lease {
            resource: Arc::downgrade(self),
            slot,
            key,
            id,
            reused: false,
            continuation: None,
            keep: false,
            _permit: permit,
        })
    }
}
impl Drop for Resource {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        for (_, e) in std::mem::take(&mut state.entries) {
            e.idle.cancel();
            e.slot.abort();
        }
    }
}
fn inc(value: &mut SafeUnsigned) -> Result<(), Failure> {
    *value = SafeUnsigned::new(value.get().checked_add(1).ok_or(Failure::Limit)?)
        .map_err(|_| Failure::Limit)?;
    Ok(())
}
pub(super) struct Lease {
    resource: Weak<Resource>,
    slot: Arc<Slot>,
    key: Option<(String, String)>,
    id: u64,
    pub reused: bool,
    pub continuation: Option<Continuation>,
    keep: bool,
    _permit: Arc<Permit>,
}
impl Lease {
    pub fn cached(&self) -> bool {
        self.key.is_some()
    }
    pub fn runtime(&self, runtime: &RequestRuntime) -> RequestRuntime {
        let mut rt = runtime.clone();
        rt.cancellation =
            CancellationToken::linked(&[runtime.cancellation.clone(), self.slot.1.clone()]);
        rt
    }
    pub async fn close(&mut self, runtime: &RequestRuntime) {
        let grace = self
            .resource
            .upgrade()
            .map_or(Duration::from_secs(2), |r| r.limits.close_grace);
        let _ = self
            .slot
            .clone()
            .close(runtime.clone(), grace, "done")
            .await;
    }

    pub async fn start(
        &self,
        message: String,
        runtime: &RequestRuntime,
    ) -> Result<Box<dyn WebSocketExchange>, Failure> {
        let linked = self.runtime(runtime);
        let runtime = &linked;
        let mut loan = self.slot.loan()?;
        cancellable(
            loan.socket
                .as_mut()
                .unwrap()
                .start_exchange(WebSocketMessage::Text(message), runtime),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
        .map_err(Failure::from)
    }
    pub fn retain(
        &mut self,
        continuation: Option<Continuation>,
        runtime: &RequestRuntime,
    ) -> Result<(), Failure> {
        if !self.slot.loan().is_ok_and(|loan| {
            loan.socket
                .as_ref()
                .is_some_and(|s| s.state() == WebSocketState::Open)
        }) {
            return Ok(());
        }
        let (Some(resource), Some(key)) = (self.resource.upgrade(), self.key.clone()) else {
            return Ok(());
        };
        resource
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < resource.limits.max_active_leases)
                    .then(|| n.checked_add(1))
                    .flatten()
            })
            .map_err(|_| Failure::Limit)?;
        let timer_permit = Arc::new(Permit(Arc::downgrade(&resource)));
        let idle = CancellationToken::new();
        let deadline = runtime.clock.now_unix_ms().saturating_add(300_000);
        let (old_idle, old_permit) = {
            let mut s = resource.state.lock().unwrap_or_else(|p| p.into_inner());
            let Some(e) = s.entries.get_mut(&key).filter(|e| e.id == self.id) else {
                return Ok(());
            };
            e.busy = false;
            e.continuation = continuation;
            (
                std::mem::replace(&mut e.idle, idle.clone()),
                self.slot.replace_permit(timer_permit.clone()),
            )
        };
        drop(old_idle);
        drop(old_permit);
        let weak = Arc::downgrade(&resource);
        let id = self.id;
        let mut rt = runtime.clone();
        rt.cancellation = idle.clone();
        let task = Box::pin(async move {
            let _timer_permit = timer_permit;
            let remaining = Duration::from_millis(deadline.saturating_sub(rt.clock.now_unix_ms()));
            let expired = cancellable(rt.clock.sleep(remaining, &idle), &idle)
                .await
                .is_ok_and(|r| r.is_ok());
            if expired
                && let Some(resource) = weak.upgrade()
                && let Some(e) = resource.remove(&key, id)
            {
                let mut close_rt = rt.clone();
                close_rt.cancellation = CancellationToken::new();
                let _ = e
                    .slot
                    .close(close_rt, resource.limits.close_grace, "idle_timeout")
                    .await;
            }
        });
        if runtime.spawner.spawn(task).is_err() {
            return Err(Failure::setup("idle_spawn"));
        }
        self.keep = true;
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        if let (Some(resource), Some(key)) = (self.resource.upgrade(), self.key.as_ref())
            && let Some(e) = resource.remove(key, self.id)
        {
            e.idle.cancel();
        }
        self.slot.abort();
    }
}
