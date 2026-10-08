//! Injected credential capabilities for coding-runtime composition.
//!
//! This module deliberately does not implement credential storage, authentication,
//! refresh, or login.  The host supplies existing `pi-ai-core` ports and owns
//! their lifetime and side effects.

use pi_ai::{AuthContext, CredentialStore};
use std::fmt;
use std::sync::Arc;

/// Process-local credential capabilities injected by a host.
///
/// The values are handles to host-owned ports.  The coding runtime never reads,
/// writes, refreshes, or serializes them while resolving a model reference.
#[derive(Clone, Default)]
pub struct CredentialInputs {
    /// Optional host-owned credential store.
    pub store: Option<Arc<dyn CredentialStore>>,
    /// Optional host-owned ambient authentication context.
    pub auth_context: Option<Arc<dyn AuthContext>>,
}

impl CredentialInputs {
    /// Creates empty credential inputs for offline composition and catalog work.
    pub const fn empty() -> Self {
        Self {
            store: None,
            auth_context: None,
        }
    }

    /// Creates inputs from an injected credential store and authentication context.
    pub fn new(
        store: Option<Arc<dyn CredentialStore>>,
        auth_context: Option<Arc<dyn AuthContext>>,
    ) -> Self {
        Self {
            store,
            auth_context,
        }
    }

    /// Returns the host-owned credential store, when supplied.
    pub fn store(&self) -> Option<&Arc<dyn CredentialStore>> {
        self.store.as_ref()
    }

    /// Returns the host-owned authentication context, when supplied.
    pub fn auth_context(&self) -> Option<&Arc<dyn AuthContext>> {
        self.auth_context.as_ref()
    }
}

impl fmt::Debug for CredentialInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialInputs")
            .field("store_present", &self.store.is_some())
            .field("auth_context_present", &self.auth_context.is_some())
            .finish()
    }
}
