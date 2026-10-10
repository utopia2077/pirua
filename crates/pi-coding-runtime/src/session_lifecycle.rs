//! Application-boundary lifecycle ordering for coding Harness attachments.
//!
//! Durable session state and ownership remain in `RuntimeSession` and
//! `RuntimeHarness`. This adapter only sequences replacement: abort, wait for
//! idle/settlement, close the old owner, then attach the destination.

use crate::{CodingHarness, CodingHarnessBuilder, CodingHarnessError};
use pi_agent::schema::{HarnessRuntimeError, SessionMetadata, SessionMetadataRecord};
use pi_agent_session::storage::RuntimeStorage;
use std::sync::Arc;

/// One active coding Harness attachment at a time.
pub struct CodingSessionLifecycle<S, M = SessionMetadata>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    current: Option<CodingHarness<S, M>>,
}

impl<S, M> Default for CodingSessionLifecycle<S, M>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    fn default() -> Self {
        Self { current: None }
    }
}

impl<S, M> CodingSessionLifecycle<S, M>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    /// Creates an empty lifecycle adapter.
    pub const fn new() -> Self {
        Self { current: None }
    }

    /// Returns the currently attached Harness, if any.
    pub fn current(&self) -> Option<&CodingHarness<S, M>> {
        self.current.as_ref()
    }

    /// Returns the currently attached Harness handle, if any.
    pub fn current_harness(
        &self,
    ) -> Option<&Arc<pi_harness_runtime::owner::RuntimeHarness<crate::ExecutionToolContext, S, M>>>
    {
        self.current.as_ref().map(CodingHarness::harness)
    }

    /// Attaches a builder after replacing any current owner in contract order.
    pub async fn attach(
        &mut self,
        builder: CodingHarnessBuilder<S, M>,
    ) -> Result<&CodingHarness<S, M>, CodingHarnessError> {
        self.teardown_current()
            .await
            .map_err(CodingHarnessError::from)?;
        self.current = Some(builder.build().await?);
        Ok(self.current.as_ref().expect("attached above"))
    }

    /// Explicit replacement entry point.
    pub async fn replace(
        &mut self,
        builder: CodingHarnessBuilder<S, M>,
    ) -> Result<&CodingHarness<S, M>, CodingHarnessError> {
        self.attach(builder).await
    }

    /// Reopens/rebuilds a destination session through configured creation.
    pub async fn restart(
        &mut self,
        builder: CodingHarnessBuilder<S, M>,
    ) -> Result<&CodingHarness<S, M>, CodingHarnessError> {
        self.attach(builder).await
    }

    /// Applies a repository-produced fork destination through the same
    /// replacement ordering. Forking itself remains owned by `SessionRepo`.
    pub async fn attach_fork(
        &mut self,
        builder: CodingHarnessBuilder<S, M>,
    ) -> Result<&CodingHarness<S, M>, CodingHarnessError> {
        self.attach(builder).await
    }

    /// Applies an externally opened/imported destination through the same
    /// replacement ordering. JSONL path/open semantics live in `legacy_import`.
    pub async fn attach_import(
        &mut self,
        builder: CodingHarnessBuilder<S, M>,
    ) -> Result<&CodingHarness<S, M>, CodingHarnessError> {
        self.attach(builder).await
    }

    /// Aborts, settles and closes the current owner.
    pub async fn close(&mut self) -> Result<(), HarnessRuntimeError> {
        self.teardown_current().await
    }

    async fn teardown_current(&mut self) -> Result<(), HarnessRuntimeError> {
        let Some(current) = self.current.take() else {
            return Ok(());
        };
        current.teardown().await
    }
}
