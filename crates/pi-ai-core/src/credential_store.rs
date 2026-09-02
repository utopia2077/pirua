//! Revisioned credential storage and exclusive per-provider mutation leases.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::auth::{AuthType, Credential, CredentialInfo};
use crate::id::ProviderId;
use crate::runtime::{BoxFuture, CancellationRegistration, CancellationTarget, CancellationToken};

/// A nonzero, monotonically increasing credential revision.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CredentialRevision>();
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialRevision(u64);

impl CredentialRevision {
    /// The first revision assigned to a stored credential.
    pub const FIRST: Self = Self(1);

    /// Creates a revision, rejecting the internal never-written revision zero.
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the primitive revision value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next revision, or `None` at `u64::MAX`.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// A detached credential paired with its authoritative revision.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::VersionedCredential>();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct VersionedCredential {
    /// Authoritative nonzero revision.
    pub revision: CredentialRevision,
    /// Detached credential value.
    pub credential: Credential,
}

/// Value selected by an exclusive credential mutation.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CredentialMutationValue>();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum CredentialMutationValue {
    /// Preserve the current value and revision.
    Unchanged,
    /// Store a value and allocate the next revision, even when equal.
    Set(Credential),
    /// Delete a present value and retain its next revision as a tombstone.
    Delete,
}

/// Explicit repair permitted for a quarantined provider.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CredentialReconciliation>();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum CredentialReconciliation {
    /// Establish a newly authenticated known value.
    Replace(Credential),
    /// Establish a known missing value for logout.
    Delete,
}

/// Stable credential-store error category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CredentialStoreErrorCode {
    /// Cancellation won before effect admission.
    Cancelled,
    /// The backend proved that the prior known state remains authoritative.
    Failed,
    /// The backend cannot prove whether an effect occurred.
    OutcomeUnknown,
    /// The successor of `u64::MAX` cannot be represented.
    RevisionExhausted,
    /// Reconciliation was requested for a known provider state.
    ReconciliationNotRequired,
}

/// Redacted failure from a credential-store operation.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CredentialStoreError>();
/// ```
pub struct CredentialStoreError {
    code: CredentialStoreErrorCode,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl CredentialStoreError {
    /// Creates a categorized failure without a backend source.
    pub const fn new(code: CredentialStoreErrorCode) -> Self {
        Self { code, source: None }
    }

    /// Creates a normal backend failure while retaining its source.
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            code: CredentialStoreErrorCode::Failed,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable failure category.
    pub const fn code(&self) -> CredentialStoreErrorCode {
        self.code
    }
}

impl fmt::Debug for CredentialStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialStoreError")
            .field("code", &self.code)
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for CredentialStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.code {
            CredentialStoreErrorCode::Cancelled => "credential store operation cancelled",
            CredentialStoreErrorCode::Failed => "credential store operation failed",
            CredentialStoreErrorCode::OutcomeUnknown => "credential store outcome unknown",
            CredentialStoreErrorCode::RevisionExhausted => "credential revision exhausted",
            CredentialStoreErrorCode::ReconciliationNotRequired => {
                "credential reconciliation not required"
            }
        })
    }
}

impl Error for CredentialStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Object-safe credential storage with exclusive per-provider mutation.
///
/// Mutation and reconciliation share one FIFO gate per provider. A persistent
/// backend must reserve a successor revision before an ambiguous effect and
/// return `OutcomeUnknown` until explicit reconciliation establishes known
/// state at that same reserved revision.
pub trait CredentialStore: Send + Sync {
    /// Reads one detached credential without resolving it.
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>>;

    /// Lists non-secret metadata in live insertion order.
    fn list<'a>(
        &'a self,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>>;

    /// Acquires the exclusive FIFO mutation lease for one provider.
    fn begin_mutation<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>>;

    /// Repairs a quarantined provider under the same exclusive FIFO gate.
    fn reconcile<'a>(
        &'a self,
        provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>>;
}

/// Exclusive detached credential mutation capability.
///
/// The lease is process-local and intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<Box<dyn pi_ai_core::CredentialMutation>>();
/// ```
pub trait CredentialMutation: Send {
    /// Returns the detached value observed after acquiring the lease.
    fn current(&self) -> Option<&VersionedCredential>;

    /// Consumes the lease and admits a value-only commit on its first poll.
    ///
    /// Cancellation observed on that poll writes nothing. Once admitted, the
    /// backend must finish definitively or durably quarantine the provider.
    fn commit(
        self: Box<Self>,
        next: CredentialMutationValue,
    ) -> BoxFuture<'static, Result<Option<VersionedCredential>, CredentialStoreError>>;
}

struct StoredCredential {
    revision: u64,
    value: Option<Arc<Credential>>,
    order_ticket: Option<u128>,
    quarantine: Option<Quarantine>,
}

struct Quarantine {
    operation_id: u64,
    intended_action: QuarantinedAction,
    prior_revision: u64,
    reserved_revision: CredentialRevision,
    prior_order_ticket: Option<u128>,
}

#[allow(dead_code)] // Private source-unit fault injection constructs these marker actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QuarantinedAction {
    Set,
    Delete,
    ReconcileReplace,
    ReconcileDelete,
}

#[derive(Default)]
struct CredentialState {
    records: BTreeMap<ProviderId, StoredCredential>,
    next_order_ticket: u128,
}

impl CredentialState {
    fn rebase_order_tickets(&mut self) {
        let mut tickets = BTreeSet::new();
        for record in self.records.values() {
            tickets.extend(record.order_ticket);
            if let Some(quarantine) = &record.quarantine {
                tickets.extend(quarantine.prior_order_ticket);
            }
        }
        let mapping = tickets
            .into_iter()
            .enumerate()
            .map(|(index, ticket)| (ticket, index as u128))
            .collect::<BTreeMap<_, _>>();
        for record in self.records.values_mut() {
            if let Some(ticket) = &mut record.order_ticket {
                *ticket = mapping[ticket];
            }
            if let Some(ticket) = record
                .quarantine
                .as_mut()
                .and_then(|quarantine| quarantine.prior_order_ticket.as_mut())
            {
                *ticket = mapping[ticket];
            }
        }
        self.next_order_ticket = mapping.len() as u128;
    }

    fn allocate_order_ticket(&mut self) -> u128 {
        let candidate_in_use = self.records.values().any(|record| {
            record.order_ticket == Some(self.next_order_ticket)
                || record.quarantine.as_ref().is_some_and(|quarantine| {
                    quarantine.prior_order_ticket == Some(self.next_order_ticket)
                })
        });
        if self.next_order_ticket == u128::MAX || candidate_in_use {
            self.rebase_order_tickets();
        }
        let ticket = self.next_order_ticket;
        self.next_order_ticket = self.next_order_ticket.saturating_add(1);
        ticket
    }
}

struct GateWaiter {
    id: u64,
    waker: Waker,
}

#[derive(Default)]
struct ProviderGate {
    owner: Option<u64>,
    waiters: VecDeque<GateWaiter>,
}

#[derive(Default)]
struct GateState {
    next_id: u64,
    providers: BTreeMap<ProviderId, ProviderGate>,
}

struct CredentialStoreInner {
    state: Arc<Mutex<CredentialState>>,
    gates: Mutex<GateState>,
    #[cfg(test)]
    reconcile_fault: Mutex<Option<ReconcileFault>>,
}

#[cfg(test)]
enum ReconcileFault {
    Failed,
    OutcomeUnknown {
        operation_id: u64,
        physically_applied: bool,
    },
}

/// In-memory reference credential store with detached values and FIFO leases.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::InMemoryCredentialStore>();
/// ```
pub struct InMemoryCredentialStore {
    inner: Arc<CredentialStoreInner>,
}

impl InMemoryCredentialStore {
    /// Creates an empty in-memory credential store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CredentialStoreInner {
                state: Arc::new(Mutex::new(CredentialState::default())),
                gates: Mutex::new(GateState::default()),
                #[cfg(test)]
                reconcile_fault: Mutex::new(None),
            }),
        }
    }
}

impl Default for InMemoryCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

fn credential_type(credential: &Credential) -> AuthType {
    match credential {
        Credential::ApiKey(_) => AuthType::ApiKey,
        Credential::OAuth(_) => AuthType::OAuth,
    }
}

impl CredentialStoreInner {
    fn snapshot_handle(
        &self,
        provider: &ProviderId,
    ) -> Result<Option<(CredentialRevision, Arc<Credential>)>, CredentialStoreError> {
        let detached = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            match state.records.get(provider) {
                Some(record) if record.quarantine.is_some() => {
                    return Err(CredentialStoreError::new(
                        CredentialStoreErrorCode::OutcomeUnknown,
                    ));
                }
                Some(record) => record.value.as_ref().map(|value| {
                    (
                        CredentialRevision::new(record.revision)
                            .expect("stored live credentials have nonzero revisions"),
                        Arc::clone(value),
                    )
                }),
                None => None,
            }
        };
        Ok(detached)
    }

    fn snapshot(
        &self,
        provider: &ProviderId,
    ) -> Result<Option<VersionedCredential>, CredentialStoreError> {
        Ok(self
            .snapshot_handle(provider)?
            .map(|(revision, credential)| VersionedCredential {
                revision,
                credential: (*credential).clone(),
            }))
    }

    fn list(&self) -> Result<Vec<CredentialInfo>, CredentialStoreError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if state
            .records
            .values()
            .any(|record| record.quarantine.is_some())
        {
            return Err(CredentialStoreError::new(
                CredentialStoreErrorCode::OutcomeUnknown,
            ));
        }
        let mut ordered = state
            .records
            .iter()
            .filter_map(|(provider, record)| {
                record
                    .value
                    .as_ref()
                    .zip(record.order_ticket)
                    .map(|(credential, ticket)| {
                        (
                            ticket,
                            CredentialInfo {
                                provider_id: provider.clone(),
                                auth_type: credential_type(credential),
                            },
                        )
                    })
            })
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(ticket, _)| *ticket);
        Ok(ordered.into_iter().map(|(_, info)| info).collect())
    }

    fn apply_mutation(
        &self,
        provider: &ProviderId,
        next: CredentialMutationValue,
    ) -> Result<Option<VersionedCredential>, CredentialStoreError> {
        enum PreparedMutation {
            Unchanged,
            Set(Arc<Credential>),
            Delete,
        }
        let prepared = match next {
            CredentialMutationValue::Unchanged => PreparedMutation::Unchanged,
            CredentialMutationValue::Set(credential) => PreparedMutation::Set(Arc::new(credential)),
            CredentialMutationValue::Delete => PreparedMutation::Delete,
        };

        let (detached, old_record) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let current = state.records.get(provider);
            if current.is_some_and(|record| record.quarantine.is_some()) {
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::OutcomeUnknown,
                ));
            }
            let prior_revision = current.map_or(0, |record| record.revision);
            let was_live = current.is_some_and(|record| record.value.is_some());
            match prepared {
                PreparedMutation::Unchanged => {
                    let detached = current.and_then(|record| {
                        record.value.as_ref().map(|credential| {
                            (
                                CredentialRevision::new(record.revision)
                                    .expect("stored live credentials have nonzero revisions"),
                                Arc::clone(credential),
                            )
                        })
                    });
                    (detached, None)
                }
                PreparedMutation::Delete if !was_live => (None, None),
                PreparedMutation::Set(credential) => {
                    let revision = prior_revision.checked_add(1).ok_or_else(|| {
                        CredentialStoreError::new(CredentialStoreErrorCode::RevisionExhausted)
                    })?;
                    let order_ticket = current
                        .and_then(|record| record.order_ticket)
                        .unwrap_or_else(|| state.allocate_order_ticket());
                    let detached = Some((
                        CredentialRevision::new(revision).expect("successor revisions are nonzero"),
                        Arc::clone(&credential),
                    ));
                    let old = state.records.insert(
                        provider.clone(),
                        StoredCredential {
                            revision,
                            value: Some(credential),
                            order_ticket: Some(order_ticket),
                            quarantine: None,
                        },
                    );
                    (detached, old)
                }
                PreparedMutation::Delete => {
                    let revision = prior_revision.checked_add(1).ok_or_else(|| {
                        CredentialStoreError::new(CredentialStoreErrorCode::RevisionExhausted)
                    })?;
                    let old = state.records.insert(
                        provider.clone(),
                        StoredCredential {
                            revision,
                            value: None,
                            order_ticket: None,
                            quarantine: None,
                        },
                    );
                    (None, old)
                }
            }
        };
        drop(old_record);
        Ok(detached.map(|(revision, credential)| VersionedCredential {
            revision,
            credential: (*credential).clone(),
        }))
    }

    fn apply_reconciliation(
        &self,
        provider: &ProviderId,
        resolution: CredentialReconciliation,
    ) -> Result<Option<VersionedCredential>, CredentialStoreError> {
        let prepared = match resolution {
            CredentialReconciliation::Replace(credential) => Some(Arc::new(credential)),
            CredentialReconciliation::Delete => None,
        };
        let (detached, old_record) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(quarantine) = state
                .records
                .get(provider)
                .and_then(|record| record.quarantine.as_ref())
            else {
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::ReconciliationNotRequired,
                ));
            };
            debug_assert_eq!(
                quarantine.prior_revision.checked_add(1),
                Some(quarantine.reserved_revision.get())
            );
            debug_assert_ne!(quarantine.operation_id, 0);
            let _intended_action = quarantine.intended_action;
            let revision = quarantine.reserved_revision;
            let prior_order_ticket = quarantine.prior_order_ticket;
            let detached = prepared
                .as_ref()
                .map(|credential| (revision, Arc::clone(credential)));
            let order_ticket = if detached.is_some() {
                Some(prior_order_ticket.unwrap_or_else(|| state.allocate_order_ticket()))
            } else {
                None
            };
            let old = state.records.insert(
                provider.clone(),
                StoredCredential {
                    revision: revision.get(),
                    value: prepared,
                    order_ticket,
                    quarantine: None,
                },
            );
            (detached, old)
        };
        drop(old_record);
        Ok(detached.map(|(revision, credential)| VersionedCredential {
            revision,
            credential: (*credential).clone(),
        }))
    }

    #[cfg(test)]
    fn apply_reconciliation_fault(
        &self,
        provider: &ProviderId,
        resolution: CredentialReconciliation,
        fault: ReconcileFault,
    ) -> Result<Option<VersionedCredential>, CredentialStoreError> {
        match fault {
            ReconcileFault::Failed => {
                Err(CredentialStoreError::new(CredentialStoreErrorCode::Failed))
            }
            ReconcileFault::OutcomeUnknown {
                operation_id,
                physically_applied,
            } => {
                let (action, prepared) = match resolution {
                    CredentialReconciliation::Replace(credential) => (
                        QuarantinedAction::ReconcileReplace,
                        Some(Arc::new(credential)),
                    ),
                    CredentialReconciliation::Delete => (QuarantinedAction::ReconcileDelete, None),
                };
                let old_value = {
                    let mut state = self
                        .state
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    let (old_value, needs_ticket) = {
                        let Some(record) = state.records.get_mut(provider) else {
                            return Err(CredentialStoreError::new(
                                CredentialStoreErrorCode::ReconciliationNotRequired,
                            ));
                        };
                        let Some(quarantine) = record.quarantine.as_mut() else {
                            return Err(CredentialStoreError::new(
                                CredentialStoreErrorCode::ReconciliationNotRequired,
                            ));
                        };
                        quarantine.operation_id = operation_id;
                        quarantine.intended_action = action;
                        let old_value = if physically_applied {
                            let old_value = record.value.take();
                            record.value = prepared;
                            if record.value.is_none() {
                                record.order_ticket = None;
                            }
                            old_value
                        } else {
                            None
                        };
                        (
                            old_value,
                            physically_applied
                                && record.value.is_some()
                                && record.order_ticket.is_none(),
                        )
                    };
                    if needs_ticket {
                        let ticket = state.allocate_order_ticket();
                        state
                            .records
                            .get_mut(provider)
                            .expect("faulted record remains present")
                            .order_ticket = Some(ticket);
                    }
                    old_value
                };
                drop(old_value);
                Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::OutcomeUnknown,
                ))
            }
        }
    }

    fn release_gate(&self, provider: &ProviderId, owner: u64) {
        let wake = {
            let mut gates = self
                .gates
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(gate) = gates.providers.get_mut(provider) else {
                return;
            };
            if gate.owner != Some(owner) {
                return;
            }
            let next = gate.waiters.pop_front();
            let wake = if let Some(next) = next {
                gate.owner = Some(next.id);
                Some(next.waker)
            } else {
                gate.owner = None;
                None
            };
            let remove_gate = gate.owner.is_none() && gate.waiters.is_empty();
            if remove_gate {
                gates.providers.remove(provider);
            }
            wake
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn remove_waiter(&self, provider: &ProviderId, waiter_id: u64) {
        let (removed, wake) = {
            let mut gates = self
                .gates
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(gate) = gates.providers.get_mut(provider) else {
                return;
            };
            let (removed, wake) = if gate.owner == Some(waiter_id) {
                let next = gate.waiters.pop_front();
                if let Some(next) = next {
                    gate.owner = Some(next.id);
                    (None, Some(next.waker))
                } else {
                    gate.owner = None;
                    (None, None)
                }
            } else if let Some(position) = gate
                .waiters
                .iter()
                .position(|waiter| waiter.id == waiter_id)
            {
                (
                    gate.waiters.remove(position).map(|waiter| waiter.waker),
                    None,
                )
            } else {
                (None, None)
            };
            let remove_gate = gate.owner.is_none() && gate.waiters.is_empty();
            if remove_gate {
                gates.providers.remove(provider);
            }
            (removed, wake)
        };
        drop(removed);
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

enum WaiterAttribution {
    New,
    Assigned(u64),
    Returned,
    Completed,
    Cancelled,
}

struct AcquisitionShared {
    inner: Arc<CredentialStoreInner>,
    provider: ProviderId,
    cancelled: AtomicBool,
    attribution: Mutex<WaiterAttribution>,
    waker: Mutex<Option<Waker>>,
}

impl AcquisitionShared {
    fn new(inner: Arc<CredentialStoreInner>, provider: ProviderId) -> Self {
        Self {
            inner,
            provider,
            cancelled: AtomicBool::new(false),
            attribution: Mutex::new(WaiterAttribution::New),
            waker: Mutex::new(None),
        }
    }

    fn poll(&self, context: &Context<'_>) -> (bool, Option<Waker>) {
        if self.cancelled.load(Ordering::Acquire) {
            return (true, None);
        }
        let candidate = context.waker().clone();
        let displaced = {
            let mut stored = self
                .waker
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if stored
                .as_ref()
                .is_some_and(|waker| waker.will_wake(context.waker()))
            {
                None
            } else {
                stored.replace(candidate)
            }
        };
        (self.cancelled.load(Ordering::Acquire), displaced)
    }

    fn clear_waker(&self) -> Option<Waker> {
        self.waker
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
    }

    fn cancel_before_return(&self) {
        let waiter_id = {
            let mut attribution = self
                .attribution
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            self.cancelled.store(true, Ordering::Release);
            match *attribution {
                WaiterAttribution::Assigned(waiter_id) => {
                    *attribution = WaiterAttribution::Cancelled;
                    Some(waiter_id)
                }
                WaiterAttribution::New => {
                    *attribution = WaiterAttribution::Cancelled;
                    None
                }
                WaiterAttribution::Returned
                | WaiterAttribution::Completed
                | WaiterAttribution::Cancelled => None,
            }
        };
        if let Some(waiter_id) = waiter_id {
            self.inner.remove_waiter(&self.provider, waiter_id);
        }
    }

    fn abandon(&self) {
        let waiter_id = {
            let mut attribution = self
                .attribution
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            match *attribution {
                WaiterAttribution::Assigned(waiter_id) => {
                    *attribution = WaiterAttribution::Completed;
                    Some(waiter_id)
                }
                WaiterAttribution::New | WaiterAttribution::Cancelled => {
                    *attribution = WaiterAttribution::Completed;
                    None
                }
                WaiterAttribution::Returned | WaiterAttribution::Completed => None,
            }
        };
        if let Some(waiter_id) = waiter_id {
            self.inner.remove_waiter(&self.provider, waiter_id);
        }
    }
}

impl CancellationTarget for AcquisitionShared {
    fn cancel(&self) {
        self.cancel_before_return();
        let wake = self.clear_waker();
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

struct AcquireFuture {
    token: CancellationToken,
    shared: Arc<AcquisitionShared>,
    registration: Option<CancellationRegistration>,
    allow_quarantine: bool,
    completed: bool,
}

impl AcquireFuture {
    fn new(
        inner: Arc<CredentialStoreInner>,
        provider: ProviderId,
        token: CancellationToken,
        allow_quarantine: bool,
    ) -> Self {
        let shared = Arc::new(AcquisitionShared::new(inner, provider));
        let target: Arc<dyn CancellationTarget> = shared.clone();
        let registration = token.register_target(&target);
        Self {
            token,
            shared,
            registration,
            allow_quarantine,
            completed: false,
        }
    }

    fn finish_cancelled(&mut self) -> Poll<Result<CredentialLease, CredentialStoreError>> {
        self.shared.cancel_before_return();
        self.completed = true;
        let registration = self.registration.take();
        let displaced = self.shared.clear_waker();
        drop(registration);
        drop(displaced);
        Poll::Ready(Err(CredentialStoreError::new(
            CredentialStoreErrorCode::Cancelled,
        )))
    }
}

impl Future for AcquireFuture {
    type Output = Result<CredentialLease, CredentialStoreError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let (cancelled, displaced) = self.shared.poll(context);
        drop(displaced);
        if cancelled || self.token.is_cancelled() {
            return self.finish_cancelled();
        }

        let candidate_waker = context.waker().clone();
        let shared = Arc::clone(&self.shared);
        let mut attribution = shared
            .attribution
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if shared.cancelled.load(Ordering::Acquire) || self.token.is_cancelled() {
            drop(attribution);
            return self.finish_cancelled();
        }
        let existing_waiter_id = match *attribution {
            WaiterAttribution::New => None,
            WaiterAttribution::Assigned(waiter_id) => Some(waiter_id),
            WaiterAttribution::Returned
            | WaiterAttribution::Completed
            | WaiterAttribution::Cancelled => {
                panic!("completed acquisition polled again")
            }
        };
        let (granted, displaced, waiter_id) = {
            let mut gates = shared
                .inner
                .gates
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let waiter_id = if let Some(waiter_id) = existing_waiter_id {
                waiter_id
            } else {
                loop {
                    let candidate = gates.next_id;
                    gates.next_id = gates.next_id.wrapping_add(1);
                    let used = gates.providers.get(&shared.provider).is_some_and(|gate| {
                        gate.owner == Some(candidate)
                            || gate.waiters.iter().any(|waiter| waiter.id == candidate)
                    });
                    if !used {
                        break candidate;
                    }
                }
            };
            let gate = gates.providers.entry(shared.provider.clone()).or_default();
            if gate.owner == Some(waiter_id) {
                (true, None, waiter_id)
            } else if gate.owner.is_none() {
                gate.owner = Some(waiter_id);
                (true, None, waiter_id)
            } else if let Some(waiter) = gate
                .waiters
                .iter_mut()
                .find(|waiter| waiter.id == waiter_id)
            {
                if waiter.waker.will_wake(context.waker()) {
                    (false, None, waiter_id)
                } else {
                    (
                        false,
                        Some(std::mem::replace(&mut waiter.waker, candidate_waker)),
                        waiter_id,
                    )
                }
            } else {
                gate.waiters.push_back(GateWaiter {
                    id: waiter_id,
                    waker: candidate_waker,
                });
                (false, None, waiter_id)
            }
        };
        *attribution = WaiterAttribution::Assigned(waiter_id);
        if !granted {
            drop(attribution);
            drop(displaced);
            return Poll::Pending;
        }
        debug_assert!(displaced.is_none());
        drop(displaced);

        let current = if self.allow_quarantine {
            None
        } else {
            match shared.inner.snapshot_handle(&shared.provider) {
                Ok(current) => current,
                Err(error) => {
                    *attribution = WaiterAttribution::Completed;
                    drop(attribution);
                    shared.inner.release_gate(&shared.provider, waiter_id);
                    self.completed = true;
                    let registration = self.registration.take();
                    let displaced = shared.clear_waker();
                    drop(registration);
                    drop(displaced);
                    return Poll::Ready(Err(error));
                }
            }
        };
        *attribution = WaiterAttribution::Returned;
        drop(attribution);
        self.completed = true;
        let registration = self.registration.take();
        let displaced = shared.clear_waker();
        drop(registration);
        drop(displaced);
        let mut lease = CredentialLease {
            inner: Arc::clone(&shared.inner),
            provider: shared.provider.clone(),
            owner: waiter_id,
            token: self.token.clone(),
            current: None,
            released: false,
        };
        lease.current = current.map(|(revision, credential)| VersionedCredential {
            revision,
            credential: (*credential).clone(),
        });
        Poll::Ready(Ok(lease))
    }
}

impl Drop for AcquireFuture {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.shared.abandon();
        let registration = self.registration.take();
        let displaced = self.shared.clear_waker();
        drop(registration);
        drop(displaced);
    }
}

struct CredentialLease {
    inner: Arc<CredentialStoreInner>,
    provider: ProviderId,
    owner: u64,
    token: CancellationToken,
    current: Option<VersionedCredential>,
    released: bool,
}

impl CredentialLease {
    fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.inner.release_gate(&self.provider, self.owner);
        }
    }
}

impl Drop for CredentialLease {
    fn drop(&mut self) {
        self.release();
    }
}

impl CredentialMutation for CredentialLease {
    fn current(&self) -> Option<&VersionedCredential> {
        self.current.as_ref()
    }

    fn commit(
        self: Box<Self>,
        next: CredentialMutationValue,
    ) -> BoxFuture<'static, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(CommitFuture {
            lease: Some(*self),
            next: Some(next),
            completed: false,
        })
    }
}

struct CommitFuture {
    lease: Option<CredentialLease>,
    next: Option<CredentialMutationValue>,
    completed: bool,
}

impl Future for CommitFuture {
    type Output = Result<Option<VersionedCredential>, CredentialStoreError>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "credential commit polled after completion");
        self.completed = true;
        let lease = self.lease.take().expect("commit owns its lease");
        let next = self.next.take().expect("commit owns its value");
        if lease.token.is_cancelled() {
            drop(lease);
            drop(next);
            return Poll::Ready(Err(CredentialStoreError::new(
                CredentialStoreErrorCode::Cancelled,
            )));
        }
        let result = lease.inner.apply_mutation(&lease.provider, next);
        drop(lease);
        Poll::Ready(result)
    }
}

impl CredentialStore for InMemoryCredentialStore {
    fn read<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                ));
            }
            self.inner.snapshot(provider)
        })
    }

    fn list<'a>(
        &'a self,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                ));
            }
            self.inner.list()
        })
    }

    fn begin_mutation<'a>(
        &'a self,
        provider: &'a ProviderId,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        let acquire = AcquireFuture::new(
            Arc::clone(&self.inner),
            provider.clone(),
            cancellation.clone(),
            false,
        );
        Box::pin(async move {
            acquire
                .await
                .map(|lease| Box::new(lease) as Box<dyn CredentialMutation + 'static>)
        })
    }

    fn reconcile<'a>(
        &'a self,
        provider: &'a ProviderId,
        resolution: CredentialReconciliation,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        let inner = Arc::clone(&self.inner);
        let provider = provider.clone();
        let token = cancellation.clone();
        Box::pin(async move {
            let lease =
                AcquireFuture::new(Arc::clone(&inner), provider.clone(), token.clone(), true)
                    .await?;
            if token.is_cancelled() {
                drop(lease);
                return Err(CredentialStoreError::new(
                    CredentialStoreErrorCode::Cancelled,
                ));
            }
            #[cfg(test)]
            let fault = inner
                .reconcile_fault
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            #[cfg(test)]
            let result = match fault {
                Some(fault) => inner.apply_reconciliation_fault(&provider, resolution, fault),
                None => inner.apply_reconciliation(&provider, resolution),
            };
            #[cfg(not(test))]
            let result = inner.apply_reconciliation(&provider, resolution);
            drop(lease);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::auth::{ApiKeyCredential, ApiKeyCredentialType, SecretString};

    fn provider() -> ProviderId {
        ProviderId::new("provider").expect("valid provider")
    }

    fn credential(value: &str) -> Credential {
        Credential::ApiKey(ApiKeyCredential {
            credential_type: ApiKeyCredentialType::ApiKey,
            key: Some(SecretString::new(value)),
            env: None,
        })
    }

    fn poll_ready<F: Future + ?Sized>(future: Pin<&mut F>) -> F::Output {
        match future.poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future unexpectedly pending"),
        }
    }

    fn set(store: &InMemoryCredentialStore, provider: &ProviderId, value: &str) {
        let token = CancellationToken::new();
        let mut acquire = pin!(store.begin_mutation(provider, &token));
        let lease = poll_ready(acquire.as_mut()).expect("lease acquired");
        let mut commit = lease.commit(CredentialMutationValue::Set(credential(value)));
        poll_ready(commit.as_mut()).expect("commit succeeds");
    }

    #[test]
    fn private_quarantine_reuses_reserved_max_revision_and_then_exhausts() {
        let store = InMemoryCredentialStore::new();
        let provider = provider();
        {
            let mut state = store
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.records.insert(
                provider.clone(),
                StoredCredential {
                    revision: u64::MAX - 1,
                    value: None,
                    order_ticket: None,
                    quarantine: Some(Quarantine {
                        operation_id: 1,
                        intended_action: QuarantinedAction::Set,
                        prior_revision: u64::MAX - 1,
                        reserved_revision: CredentialRevision::new(u64::MAX)
                            .expect("maximum is nonzero"),
                        prior_order_ticket: None,
                    }),
                },
            );
        }
        let token = CancellationToken::new();
        let mut read = pin!(store.read(&provider, &token));
        assert_eq!(
            poll_ready(read.as_mut())
                .expect_err("quarantine blocks reads")
                .code(),
            CredentialStoreErrorCode::OutcomeUnknown
        );

        let mut repair = pin!(store.reconcile(
            &provider,
            CredentialReconciliation::Replace(credential("repaired")),
            &token,
        ));
        let repaired = poll_ready(repair.as_mut())
            .expect("repair succeeds")
            .expect("replacement is present");
        assert_eq!(repaired.revision.get(), u64::MAX);

        let mut acquire = pin!(store.begin_mutation(&provider, &token));
        let lease = poll_ready(acquire.as_mut()).expect("lease acquired");
        let mut commit = lease.commit(CredentialMutationValue::Set(credential("later")));
        assert_eq!(
            poll_ready(commit.as_mut())
                .expect_err("maximum revision cannot advance")
                .code(),
            CredentialStoreErrorCode::RevisionExhausted
        );
    }

    #[test]
    fn reconciliation_is_rejected_for_known_state() {
        let store = InMemoryCredentialStore::new();
        let provider = provider();
        let token = CancellationToken::new();
        let mut reconcile =
            pin!(store.reconcile(&provider, CredentialReconciliation::Delete, &token,));
        assert_eq!(
            poll_ready(reconcile.as_mut())
                .expect_err("known state needs no repair")
                .code(),
            CredentialStoreErrorCode::ReconciliationNotRequired
        );
    }

    #[test]
    fn private_quarantine_is_sticky_across_reopen_and_delete_clears_it() {
        let store = InMemoryCredentialStore::new();
        let provider = provider();
        {
            let mut state = store
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.records.insert(
                provider.clone(),
                StoredCredential {
                    revision: 5,
                    value: Some(Arc::new(credential("prior"))),
                    order_ticket: Some(0),
                    quarantine: Some(Quarantine {
                        operation_id: 1,
                        intended_action: QuarantinedAction::Delete,
                        prior_revision: 5,
                        reserved_revision: CredentialRevision::new(6).expect("nonzero"),
                        prior_order_ticket: Some(0),
                    }),
                },
            );
            state.next_order_ticket = 1;
        }
        let reopened = InMemoryCredentialStore {
            inner: Arc::new(CredentialStoreInner {
                state: Arc::clone(&store.inner.state),
                gates: Mutex::new(GateState::default()),
                reconcile_fault: Mutex::new(None),
            }),
        };
        let token = CancellationToken::new();
        let mut read = pin!(reopened.read(&provider, &token));
        assert_eq!(
            poll_ready(read.as_mut())
                .expect_err("reopen remains quarantined")
                .code(),
            CredentialStoreErrorCode::OutcomeUnknown
        );
        let mut list = pin!(reopened.list(&token));
        assert_eq!(
            poll_ready(list.as_mut())
                .expect_err("quarantine fails the whole list")
                .code(),
            CredentialStoreErrorCode::OutcomeUnknown
        );
        let mut acquire = pin!(reopened.begin_mutation(&provider, &token));
        match poll_ready(acquire.as_mut()) {
            Err(error) => assert_eq!(error.code(), CredentialStoreErrorCode::OutcomeUnknown),
            Ok(_) => panic!("ordinary mutation cannot clear quarantine"),
        }

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let mut cancelled_repair =
            pin!(reopened.reconcile(&provider, CredentialReconciliation::Delete, &cancelled,));
        assert_eq!(
            poll_ready(cancelled_repair.as_mut())
                .expect_err("cancelled repair preserves marker")
                .code(),
            CredentialStoreErrorCode::Cancelled
        );
        let mut still_quarantined = pin!(reopened.read(&provider, &token));
        assert_eq!(
            poll_ready(still_quarantined.as_mut())
                .expect_err("marker remains")
                .code(),
            CredentialStoreErrorCode::OutcomeUnknown
        );

        let mut repair =
            pin!(reopened.reconcile(&provider, CredentialReconciliation::Delete, &token,));
        assert!(
            poll_ready(repair.as_mut())
                .expect("delete reconciliation succeeds")
                .is_none()
        );
        let mut list = pin!(reopened.list(&token));
        assert!(poll_ready(list.as_mut()).expect("known list").is_empty());
        let mut acquire = pin!(reopened.begin_mutation(&provider, &token));
        let lease = poll_ready(acquire.as_mut()).expect("known state is mutable");
        let mut commit = lease.commit(CredentialMutationValue::Set(credential("new")));
        let value = poll_ready(commit.as_mut())
            .expect("set succeeds")
            .expect("value present");
        assert_eq!(value.revision.get(), 7);
    }

    #[test]
    fn private_reconciliation_faults_reuse_marker_and_restore_predelete_order() {
        let store = InMemoryCredentialStore::new();
        let a = ProviderId::new("a").expect("valid provider");
        let b = ProviderId::new("b").expect("valid provider");
        set(&store, &a, "a-old");
        set(&store, &b, "b");

        let old_value = {
            let mut state = store
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let record = state.records.get_mut(&a).expect("a exists");
            let old_value = record.value.take();
            record.order_ticket = None;
            record.quarantine = Some(Quarantine {
                operation_id: 11,
                intended_action: QuarantinedAction::Delete,
                prior_revision: 1,
                reserved_revision: CredentialRevision::new(2).expect("nonzero"),
                prior_order_ticket: Some(0),
            });
            old_value
        };
        drop(old_value);
        let reopened = InMemoryCredentialStore {
            inner: Arc::new(CredentialStoreInner {
                state: Arc::clone(&store.inner.state),
                gates: Mutex::new(GateState::default()),
                reconcile_fault: Mutex::new(None),
            }),
        };
        let token = CancellationToken::new();

        *reopened
            .inner
            .reconcile_fault
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(ReconcileFault::Failed);
        let mut failed = pin!(reopened.reconcile(
            &a,
            CredentialReconciliation::Replace(credential("repair-one")),
            &token,
        ));
        assert_eq!(
            poll_ready(failed.as_mut())
                .expect_err("normal failure preserves marker")
                .code(),
            CredentialStoreErrorCode::Failed
        );

        *reopened
            .inner
            .reconcile_fault
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(ReconcileFault::OutcomeUnknown {
            operation_id: 12,
            physically_applied: true,
        });
        let mut uncertain_replace = pin!(reopened.reconcile(
            &a,
            CredentialReconciliation::Replace(credential("repair-two")),
            &token,
        ));
        assert_eq!(
            poll_ready(uncertain_replace.as_mut())
                .expect_err("uncertain repair remains quarantined")
                .code(),
            CredentialStoreErrorCode::OutcomeUnknown
        );
        {
            let state = reopened
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let marker = state.records[&a]
                .quarantine
                .as_ref()
                .expect("marker remains");
            assert_eq!(marker.operation_id, 12);
            assert_eq!(marker.intended_action, QuarantinedAction::ReconcileReplace);
            assert_eq!(marker.prior_revision, 1);
            assert_eq!(marker.reserved_revision.get(), 2);
            assert_eq!(marker.prior_order_ticket, Some(0));
            assert_eq!(state.records[&b].order_ticket, Some(1));
            assert_eq!(state.records[&a].order_ticket, Some(2));
        }

        *reopened
            .inner
            .reconcile_fault
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(ReconcileFault::OutcomeUnknown {
            operation_id: 13,
            physically_applied: true,
        });
        let mut uncertain_delete =
            pin!(reopened.reconcile(&a, CredentialReconciliation::Delete, &token,));
        assert_eq!(
            poll_ready(uncertain_delete.as_mut())
                .expect_err("second uncertainty retains reservation")
                .code(),
            CredentialStoreErrorCode::OutcomeUnknown
        );
        {
            let state = reopened
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let marker = state.records[&a]
                .quarantine
                .as_ref()
                .expect("marker remains");
            assert_eq!(marker.operation_id, 13);
            assert_eq!(marker.intended_action, QuarantinedAction::ReconcileDelete);
            assert_eq!(marker.reserved_revision.get(), 2);
            assert_eq!(marker.prior_order_ticket, Some(0));
            assert_eq!(state.records[&b].order_ticket, Some(1));
            assert_eq!(state.records[&a].order_ticket, None);
        }

        let mut repair = pin!(reopened.reconcile(
            &a,
            CredentialReconciliation::Replace(credential("known")),
            &token,
        ));
        let repaired = poll_ready(repair.as_mut())
            .expect("final repair succeeds")
            .expect("replacement present");
        assert_eq!(repaired.revision.get(), 2);
        let mut list = pin!(reopened.list(&token));
        assert_eq!(
            poll_ready(list.as_mut())
                .expect("known list")
                .into_iter()
                .map(|info| info.provider_id.into_string())
                .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned()]
        );

        let c = ProviderId::new("c").expect("valid provider");
        {
            let mut state = reopened
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.records.insert(
                c.clone(),
                StoredCredential {
                    revision: 0,
                    value: Some(Arc::new(credential("uncertain-set"))),
                    order_ticket: Some(2),
                    quarantine: Some(Quarantine {
                        operation_id: 14,
                        intended_action: QuarantinedAction::Set,
                        prior_revision: 0,
                        reserved_revision: CredentialRevision::FIRST,
                        prior_order_ticket: None,
                    }),
                },
            );
            state.next_order_ticket = 3;
        }
        let mut delete = pin!(reopened.reconcile(&c, CredentialReconciliation::Delete, &token,));
        assert!(
            poll_ready(delete.as_mut())
                .expect("delete repair succeeds")
                .is_none()
        );
        let mut read = pin!(reopened.read(&c, &token));
        assert!(poll_ready(read.as_mut()).expect("known missing").is_none());
    }

    #[test]
    fn private_reconcile_waits_behind_same_provider_gate() {
        let store = InMemoryCredentialStore::new();
        let provider = provider();
        set(&store, &provider, "old");
        let token = CancellationToken::new();
        let mut acquire = pin!(store.begin_mutation(&provider, &token));
        let lease = poll_ready(acquire.as_mut()).expect("gate held");
        {
            let mut state = store
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let record = state.records.get_mut(&provider).expect("record exists");
            record.quarantine = Some(Quarantine {
                operation_id: 21,
                intended_action: QuarantinedAction::Set,
                prior_revision: 1,
                reserved_revision: CredentialRevision::new(2).expect("nonzero"),
                prior_order_ticket: Some(0),
            });
        }
        let mut reconcile =
            pin!(store.reconcile(&provider, CredentialReconciliation::Delete, &token,));
        assert!(matches!(
            reconcile
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(lease);
        assert!(
            poll_ready(reconcile.as_mut())
                .expect("reconcile runs after gate release")
                .is_none()
        );
    }

    #[test]
    fn stable_order_ticket_survives_unrelated_delete_before_reconcile() {
        let store = InMemoryCredentialStore::new();
        let a = ProviderId::new("a").expect("valid provider");
        let b = ProviderId::new("b").expect("valid provider");
        let c = ProviderId::new("c").expect("valid provider");
        set(&store, &a, "a");
        set(&store, &b, "b-old");
        set(&store, &c, "c");
        let old_value = {
            let mut state = store
                .inner
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let record = state.records.get_mut(&b).expect("b exists");
            let old_value = record.value.take();
            record.order_ticket = None;
            record.quarantine = Some(Quarantine {
                operation_id: 31,
                intended_action: QuarantinedAction::Delete,
                prior_revision: 1,
                reserved_revision: CredentialRevision::new(2).expect("nonzero"),
                prior_order_ticket: Some(1),
            });
            old_value
        };
        drop(old_value);

        let token = CancellationToken::new();
        let mut acquire = pin!(store.begin_mutation(&a, &token));
        let lease = poll_ready(acquire.as_mut()).expect("a lease acquired");
        let mut delete = lease.commit(CredentialMutationValue::Delete);
        assert!(
            poll_ready(delete.as_mut())
                .expect("a delete succeeds")
                .is_none()
        );

        let mut reconcile = pin!(store.reconcile(
            &b,
            CredentialReconciliation::Replace(credential("b-new")),
            &token,
        ));
        let repaired = poll_ready(reconcile.as_mut())
            .expect("b reconcile succeeds")
            .expect("b restored");
        assert_eq!(repaired.revision.get(), 2);
        let mut list = pin!(store.list(&token));
        assert_eq!(
            poll_ready(list.as_mut())
                .expect("list succeeds")
                .into_iter()
                .map(|info| info.provider_id.into_string())
                .collect::<Vec<_>>(),
            vec!["b".to_owned(), "c".to_owned()]
        );
        let state = store
            .inner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(state.records[&b].order_ticket, Some(1));
        assert_eq!(state.records[&c].order_ticket, Some(2));
    }

    #[test]
    fn order_ticket_exhaustion_rebases_without_changing_live_order() {
        let store = InMemoryCredentialStore::new();
        let a = ProviderId::new("a").expect("valid provider");
        let b = ProviderId::new("b").expect("valid provider");
        let c = ProviderId::new("c").expect("valid provider");
        set(&store, &a, "a");
        set(&store, &b, "b");
        store
            .inner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .next_order_ticket = u128::MAX;
        set(&store, &c, "c");
        let token = CancellationToken::new();
        let mut list = pin!(store.list(&token));
        assert_eq!(
            poll_ready(list.as_mut())
                .expect("list succeeds")
                .into_iter()
                .map(|info| info.provider_id.into_string())
                .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
    }
}
