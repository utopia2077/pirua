//! Instance-scoped registration and deterministic cleanup of session resources.

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::error::Error;
use std::fmt;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, Weak};

/// Trusted process-local resource cleaned at the end of a coding session.
pub trait SessionResource: Send + Sync {
    /// Cleans state associated with `session_id`, or all state when it is absent.
    fn cleanup(&self, session_id: Option<&str>) -> Result<(), SessionResourceError>;
}

/// Fixed, source-redacted failure returned by one resource callback.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::SessionResourceError>();
/// ```
pub struct SessionResourceError {
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

#[allow(clippy::new_without_default)] // The revision-36 public API is intentionally exact.
impl SessionResourceError {
    /// Creates a cleanup failure without a diagnostic source.
    pub const fn new() -> Self {
        Self { source: None }
    }

    /// Retains a diagnostic source without exposing it through Display or Debug.
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Debug for SessionResourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionResourceError")
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for SessionResourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("session resource cleanup failed")
    }
}

impl Error for SessionResourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Stable registry-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionResourceRegistryErrorCode {
    /// No further monotonic registration token can be allocated.
    TokenExhausted,
    /// Another cleanup pass is still invoking callbacks or disposing owners.
    CleanupInProgress,
    /// At least one callback or guarded destructor failed.
    CleanupFailed,
}

/// Fixed, source-redacted registry failure with ordered callback failures.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::SessionResourceRegistryError>();
/// ```
pub struct SessionResourceRegistryError {
    code: SessionResourceRegistryErrorCode,
    failures: Vec<SessionResourceError>,
}

impl SessionResourceRegistryError {
    /// Returns the stable public category.
    pub fn code(&self) -> SessionResourceRegistryErrorCode {
        self.code
    }

    /// Returns failures in callback order followed by guarded-disposal order.
    pub fn failures(&self) -> &[SessionResourceError] {
        &self.failures
    }

    fn without_failures(code: SessionResourceRegistryErrorCode) -> Self {
        Self {
            code,
            failures: Vec::new(),
        }
    }

    fn cleanup_failed(failures: Vec<SessionResourceError>) -> Self {
        Self {
            code: SessionResourceRegistryErrorCode::CleanupFailed,
            failures,
        }
    }
}

impl fmt::Debug for SessionResourceRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionResourceRegistryError")
            .field("code", &self.code)
            .field("failure_count", &self.failures.len())
            .finish()
    }
}

impl fmt::Display for SessionResourceRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.code {
            SessionResourceRegistryErrorCode::TokenExhausted => "session resource token exhausted",
            SessionResourceRegistryErrorCode::CleanupInProgress => {
                "session resource cleanup in progress"
            }
            SessionResourceRegistryErrorCode::CleanupFailed => "session resource cleanup failed",
        })
    }
}

impl Error for SessionResourceRegistryError {}

struct RegistryState {
    next_token: u128,
    cleanup_in_progress: bool,
    resources: BTreeMap<u128, Arc<dyn SessionResource>>,
}

struct RegistryInner {
    state: Mutex<RegistryState>,
}

/// Instance-owned session-resource registry.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::SessionResourceRegistry>();
/// ```
pub struct SessionResourceRegistry {
    inner: Arc<RegistryInner>,
}

#[allow(clippy::new_without_default)] // The revision-36 public API is intentionally exact.
impl SessionResourceRegistry {
    /// Creates an empty registry with no ambient or process-global authority.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState {
                    next_token: 0,
                    cleanup_in_progress: false,
                    resources: BTreeMap::new(),
                }),
            }),
        }
    }

    /// Registers one independent token for `resource`.
    pub fn register(
        &self,
        resource: Arc<dyn SessionResource>,
    ) -> Result<SessionResourceHandle, SessionResourceRegistryError> {
        let token = {
            let mut state = lock_state(&self.inner);
            let Some(token) = state.next_token.checked_add(1) else {
                return Err(SessionResourceRegistryError::without_failures(
                    SessionResourceRegistryErrorCode::TokenExhausted,
                ));
            };
            let Entry::Vacant(entry) = state.resources.entry(token) else {
                return Err(SessionResourceRegistryError::without_failures(
                    SessionResourceRegistryErrorCode::TokenExhausted,
                ));
            };
            entry.insert(resource);
            state.next_token = token;
            token
        };
        Ok(SessionResourceHandle {
            owner: Arc::downgrade(&self.inner),
            token: Some(token),
        })
    }

    /// Runs one insertion-ordered cleanup pass.
    pub fn cleanup(&self, session_id: Option<&str>) -> Result<(), SessionResourceRegistryError> {
        let mut snapshot = {
            let mut state = lock_state(&self.inner);
            if state.cleanup_in_progress {
                return Err(SessionResourceRegistryError::without_failures(
                    SessionResourceRegistryErrorCode::CleanupInProgress,
                ));
            }
            state.cleanup_in_progress = true;
            state
                .resources
                .iter()
                .map(|(&token, resource)| SnapshotEntry {
                    token,
                    resource: Some(Arc::clone(resource)),
                    removed_owner: None,
                    succeeded: false,
                })
                .collect::<Vec<_>>()
        };
        let mut active = ActiveCleanup::new(Arc::clone(&self.inner));
        let mut failures = Vec::new();
        let mut panic_payloads = Vec::<PanicPayload>::new();

        for entry in &mut snapshot {
            let resource = entry
                .resource
                .as_deref()
                .expect("snapshot resources remain present through callbacks");
            match catch_unwind(AssertUnwindSafe(|| resource.cleanup(session_id))) {
                Ok(Ok(())) => entry.succeeded = true,
                Ok(Err(error)) => failures.push(error),
                Err(payload) => {
                    failures.push(SessionResourceError::new());
                    panic_payloads.push(payload);
                }
            }
        }

        {
            let mut state = lock_state(&self.inner);
            for entry in &mut snapshot {
                if entry.succeeded {
                    entry.removed_owner = state.resources.remove(&entry.token);
                }
            }
        }

        for payload in panic_payloads {
            dispose_panic_payload(payload, &mut failures);
        }
        for entry in &mut snapshot {
            if let Some(resource) = entry.resource.take() {
                dispose_resource_owner(resource, &mut failures);
            }
            if let Some(resource) = entry.removed_owner.take() {
                dispose_resource_owner(resource, &mut failures);
            }
        }

        active.finish();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(SessionResourceRegistryError::cleanup_failed(failures))
        }
    }
}

/// Single-use registration ownership handle.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::SessionResourceHandle>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::SessionResourceHandle>();
/// ```
pub struct SessionResourceHandle {
    owner: Weak<RegistryInner>,
    token: Option<u128>,
}

impl SessionResourceHandle {
    /// Unregisters exactly this token. Repeated calls return false.
    pub fn remove(&mut self) -> bool {
        let Some(token) = self.token.take() else {
            return false;
        };
        let Some(owner) = self.owner.upgrade() else {
            return false;
        };
        let removed = {
            let mut state = lock_state(&owner);
            state.resources.remove(&token)
        };
        removed.is_some()
    }
}

impl Drop for SessionResourceHandle {
    fn drop(&mut self) {
        self.remove();
    }
}

struct SnapshotEntry {
    token: u128,
    resource: Option<Arc<dyn SessionResource>>,
    removed_owner: Option<Arc<dyn SessionResource>>,
    succeeded: bool,
}

type PanicPayload = Box<dyn Any + Send + 'static>;

struct ActiveCleanup {
    inner: Arc<RegistryInner>,
    armed: bool,
}

impl ActiveCleanup {
    fn new(inner: Arc<RegistryInner>) -> Self {
        Self { inner, armed: true }
    }

    fn finish(&mut self) {
        if self.armed {
            lock_state(&self.inner).cleanup_in_progress = false;
            self.armed = false;
        }
    }
}

impl Drop for ActiveCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

fn lock_state(inner: &RegistryInner) -> std::sync::MutexGuard<'_, RegistryState> {
    inner
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn dispose_panic_payload(payload: PanicPayload, failures: &mut Vec<SessionResourceError>) {
    if let Err(secondary_payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        mem::forget(secondary_payload);
        failures.push(SessionResourceError::new());
    }
}

fn dispose_resource_owner(
    resource: Arc<dyn SessionResource>,
    failures: &mut Vec<SessionResourceError>,
) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(resource))) {
        mem::forget(payload);
        failures.push(SessionResourceError::new());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopResource;

    impl SessionResource for NoopResource {
        fn cleanup(&self, _session_id: Option<&str>) -> Result<(), SessionResourceError> {
            Ok(())
        }
    }

    #[test]
    fn token_exhaustion_is_checked_without_mutation() {
        let registry = SessionResourceRegistry::new();
        lock_state(&registry.inner).next_token = u128::MAX - 1;
        let mut last = match registry.register(Arc::new(NoopResource)) {
            Ok(handle) => handle,
            Err(_) => panic!("the final monotonic token remains allocatable"),
        };
        {
            let state = lock_state(&registry.inner);
            assert_eq!(state.next_token, u128::MAX);
            assert_eq!(state.resources.len(), 1);
        }
        let error = match registry.register(Arc::new(NoopResource)) {
            Ok(_) => panic!("maximum token must be exhausted"),
            Err(error) => error,
        };
        assert_eq!(
            error.code(),
            SessionResourceRegistryErrorCode::TokenExhausted
        );
        assert_eq!(error.to_string(), "session resource token exhausted");
        assert_eq!(
            format!("{error:?}"),
            "SessionResourceRegistryError { code: TokenExhausted, failure_count: 0 }"
        );
        let state = lock_state(&registry.inner);
        assert_eq!(state.next_token, u128::MAX);
        assert_eq!(state.resources.len(), 1);
        drop(state);
        assert!(last.remove());
    }
}
