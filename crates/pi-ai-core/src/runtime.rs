//! Executor-neutral cancellation, clock, and task-spawning ports.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pi_telemetry::TelemetryContext;

use crate::error::{Cancelled, SpawnError};
use crate::json::{JS_SAFE_INTEGER_MAX, SafeUnsigned};
use crate::options::{
    DeferredCancelOptionsWire, DeferredFetchOptionsWire, ImagesOptionsWire,
    SimpleStreamOptionsWire, StreamOptionsWire,
};

/// A boxed, sendable future that may borrow inputs for one explicit lifetime.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) trait CancellationTarget: Send + Sync {
    fn cancel(&self);
}

struct CancellationInner {
    cancelled: bool,
    next_id: u64,
    waiters: BTreeMap<u64, Waker>,
    targets: BTreeMap<u64, Weak<dyn CancellationTarget>>,
    parent_registrations: Vec<CancellationRegistration>,
}

impl CancellationInner {
    fn next_target_id(&mut self) -> u64 {
        loop {
            let candidate = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if !self.targets.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    fn next_waiter_id(&mut self) -> u64 {
        loop {
            let candidate = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if !self.waiters.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

struct CancellationState {
    inner: Mutex<CancellationInner>,
}

impl CancellationState {
    fn cancel(&self) {
        let (waiters, targets, registrations) = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if inner.cancelled {
                return;
            }
            inner.cancelled = true;
            let waiters = std::mem::take(&mut inner.waiters);
            let targets = std::mem::take(&mut inner.targets);
            let registrations = std::mem::take(&mut inner.parent_registrations);
            (waiters, targets, registrations)
        };
        drop(registrations);
        for target in targets.into_values() {
            if let Some(target) = target.upgrade() {
                target.cancel();
            }
        }
        for waker in waiters.into_values() {
            waker.wake();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .cancelled
    }
}

impl CancellationTarget for CancellationState {
    fn cancel(&self) {
        self.cancel();
    }
}

/// Cloneable, idempotent executor-neutral cancellation token.
///
/// The handle is process-local and intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CancellationToken>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::CancellationToken>();
/// ```
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

#[allow(clippy::new_without_default)] // Revision 29 freezes the token's complete public API.
impl CancellationToken {
    /// Creates an initially active token.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancellationState {
                inner: Mutex::new(CancellationInner {
                    cancelled: false,
                    next_id: 0,
                    waiters: BTreeMap::new(),
                    targets: BTreeMap::new(),
                    parent_registrations: Vec::new(),
                }),
            }),
        }
    }

    /// Idempotently cancels the token and wakes current waiters.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Returns whether cancellation has already won.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Returns a cancellation future whose waiter unregisters when dropped.
    pub fn cancelled(&self) -> CancellationFuture<'_> {
        CancellationFuture {
            token: self,
            waiter_id: None,
        }
    }

    /// Creates a token cancelled by the first source to cancel.
    ///
    /// Parent links are weak and are removed when the linked token settles or
    /// is dropped. An empty source slice produces an active token.
    pub fn linked(sources: &[Self]) -> Self {
        let token = Self::new();
        let target: Arc<dyn CancellationTarget> = token.inner.clone();
        let mut registrations = Vec::with_capacity(sources.len());
        for source in sources {
            if let Some(registration) = source.register_target(&target) {
                registrations.push(registration);
            }
            if token.is_cancelled() {
                break;
            }
        }

        let mut inner = token
            .inner
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if inner.cancelled {
            drop(inner);
            drop(registrations);
        } else {
            inner.parent_registrations = registrations;
            drop(inner);
        }
        token
    }

    pub(crate) fn register_target(
        &self,
        target: &Arc<dyn CancellationTarget>,
    ) -> Option<CancellationRegistration> {
        let mut inner = self
            .inner
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if inner.cancelled {
            drop(inner);
            target.cancel();
            return None;
        }
        inner.targets.retain(|_, target| target.strong_count() != 0);
        let id = inner.next_target_id();
        inner.targets.insert(id, Arc::downgrade(target));
        Some(CancellationRegistration {
            owner: Arc::downgrade(&self.inner),
            id,
        })
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        self.inner
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .waiters
            .len()
    }

    #[cfg(test)]
    pub(crate) fn target_count(&self) -> usize {
        self.inner
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .targets
            .len()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

pub(crate) struct CancellationRegistration {
    owner: Weak<CancellationState>,
    id: u64,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner
                .inner
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .targets
                .remove(&self.id);
        }
    }
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct CancellationFuture<'a> {
    token: &'a CancellationToken,
    waiter_id: Option<u64>,
}

impl Future for CancellationFuture<'_> {
    type Output = Cancelled;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let candidate_waker = context.waker().clone();
        let mut inner = self
            .token
            .inner
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if inner.cancelled {
            let displaced = self
                .waiter_id
                .take()
                .and_then(|id| inner.waiters.remove(&id));
            drop(inner);
            drop(displaced);
            return Poll::Ready(Cancelled);
        }
        match self.waiter_id {
            Some(id) => {
                let replace = inner
                    .waiters
                    .get(&id)
                    .is_none_or(|waker| !waker.will_wake(context.waker()));
                let displaced = if replace {
                    inner.waiters.insert(id, candidate_waker)
                } else {
                    None
                };
                drop(inner);
                drop(displaced);
                Poll::Pending
            }
            None => {
                let id = inner.next_waiter_id();
                let displaced = inner.waiters.insert(id, candidate_waker);
                self.waiter_id = Some(id);
                drop(inner);
                drop(displaced);
                Poll::Pending
            }
        }
    }
}

impl Drop for CancellationFuture<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id.take() {
            let displaced = self
                .token
                .inner
                .inner
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .waiters
                .remove(&id);
            drop(displaced);
        }
    }
}

/// Executor-neutral clock and cancellation-aware sleep port.
pub trait Clock: Send + Sync {
    /// Returns the current Unix timestamp in milliseconds.
    fn now_unix_ms(&self) -> u64;

    /// Waits for a duration or caller cancellation.
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>>;
}

/// Executor-neutral task admission port.
pub trait TaskSpawner: Send + Sync {
    /// Admits a detached task or returns a sanitized spawn failure.
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError>;
}

/// Process-local services carried by one provider request.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::RequestRuntime>();
/// ```
///
/// ```compile_fail
/// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
/// requires_deserialize::<pi_ai_core::RequestRuntime>();
/// ```
#[derive(Clone)]
pub struct RequestRuntime {
    /// Caller cancellation.
    pub cancellation: CancellationToken,
    /// Explicit telemetry parent context.
    pub telemetry: Arc<dyn TelemetryContext>,
    /// Injected clock.
    pub clock: Arc<dyn Clock>,
    /// Injected task spawner.
    pub spawner: Arc<dyn TaskSpawner>,
}

impl fmt::Debug for RequestRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestRuntime")
            .field("cancellation", &self.cancellation)
            .field("telemetry", &"[PROCESS-LOCAL]")
            .field("clock", &"[PROCESS-LOCAL]")
            .field("spawner", &"[PROCESS-LOCAL]")
            .finish()
    }
}

macro_rules! runtime_options {
    ($name:ident, $wire:ty) => {
        #[doc = concat!("Process-local wrapper around [`", stringify!($wire), "`].")]
        ///
        /// ```compile_fail
        /// fn requires_serialize<T: serde::Serialize>() {}
        #[doc = concat!("requires_serialize::<pi_ai_core::", stringify!($name), ">();")]
        /// ```
        ///
        /// ```compile_fail
        /// fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
        #[doc = concat!("requires_deserialize::<pi_ai_core::", stringify!($name), ">();")]
        /// ```
        #[derive(Clone, Debug)]
        pub struct $name {
            /// Serializable provider-neutral values.
            pub wire: $wire,
            /// Process-local request services.
            pub runtime: RequestRuntime,
        }
    };
}

runtime_options!(StreamOptions, StreamOptionsWire);
runtime_options!(SimpleStreamOptions, SimpleStreamOptionsWire);
runtime_options!(DeferredFetchOptions, DeferredFetchOptionsWire);
runtime_options!(DeferredCancelOptions, DeferredCancelOptionsWire);
runtime_options!(ImagesOptions, ImagesOptionsWire);

/// Saturates an injected clock value at JavaScript's exact-integer boundary.
pub(crate) fn safe_timestamp(clock: &dyn Clock) -> SafeUnsigned {
    SafeUnsigned::new(clock.now_unix_ms().min(JS_SAFE_INTEGER_MAX))
        .expect("a saturated timestamp is JavaScript-safe")
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use super::CancellationToken;

    fn context() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }

    #[test]
    fn dropped_cancellation_waiter_and_linked_token_unregister() {
        let source = CancellationToken::new();
        let empty = CancellationToken::linked(&[]);
        assert!(!empty.is_cancelled());

        let already_cancelled = CancellationToken::new();
        already_cancelled.cancel();
        assert!(CancellationToken::linked(&[already_cancelled]).is_cancelled());

        {
            let linked = CancellationToken::linked(std::slice::from_ref(&source));
            let weak = Arc::downgrade(&linked.inner);
            assert_eq!(source.target_count(), 1);
            let mut wait = Box::pin(linked.cancelled());
            assert!(matches!(wait.as_mut().poll(&mut context()), Poll::Pending));
            assert_eq!(linked.waiter_count(), 1);
            drop(wait);
            drop(linked);
            assert!(weak.upgrade().is_none());
        }
        assert_eq!(source.target_count(), 0);
    }

    #[test]
    fn cancellation_is_clone_shared_idempotent_and_any_source_linked() {
        let first = CancellationToken::new();
        let second = CancellationToken::new();
        let linked = CancellationToken::linked(&[first.clone(), second.clone()]);
        let linked_clone = linked.clone();
        assert!(!linked.is_cancelled());
        second.cancel();
        second.cancel();
        assert!(linked.is_cancelled());
        assert!(linked_clone.is_cancelled());
        let mut wait = Box::pin(linked.cancelled());
        assert!(matches!(wait.as_mut().poll(&mut context()), Poll::Ready(_)));
    }
}
