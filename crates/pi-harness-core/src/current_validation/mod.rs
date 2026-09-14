//! Pure validation of a configured lane's current entry/register graph.
//!
//! Inputs must already satisfy their owning SessionCodec policy. These detached
//! checks neither establish read coherence nor validate ledger rows or restore a
//! runtime. Reserved absence never advances the durable program counter.

use pi_agent_schema::{
    session_values::{AssistantContent, Message, StopReason},
    *,
};
use std::collections::{BTreeMap, BTreeSet};

mod inventory;
mod semantic;

/// Configured lane roots read under the caller's lane ownership discipline.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentRoots {
    /// Requested lane, compared with all three lane register keys.
    pub lane: LaneId,
    /// Existing lane configuration register.
    pub configuration: Register,
    /// Existing lane state register.
    pub lane_state: Register,
    /// Existing lane leaf register.
    pub leaf: Register,
    /// Present exactly when the lane has an open operation.
    pub operation_meta: Option<Register>,
    /// Present exactly when the lane has an open operation.
    pub operation_state: Option<Register>,
}

/// Accumulated semantic constraint on a referenced entry.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum EntryConstraint {
    /// Any committed entry.
    Entry,
    /// Any Agent message entry.
    Message,
    /// A settled assistant, with current-running cancellation consistency.
    Assistant { running: bool },
    /// A deferred assistant with a handle.
    DeferredAssistant { running: bool },
    /// A tool-result message (source pairing is checked against the batch).
    ToolResult,
    /// A compaction entry.
    Compaction,
    /// A branch summary from this exact operation source.
    BranchSummary { from_id: EntryId },
}

/// Required/reserved transport read with every accumulated semantic role.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EntryReference {
    /// At least one role requires materialization.
    pub required: bool,
    /// All roles apply when the entry is materialized.
    pub constraints: BTreeSet<EntryConstraint>,
}

/// Constraint on an exact namespace/key register read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegisterConstraint {
    /// Message queue payload.
    PendingMessage,
    /// Deferred message or custom write payload.
    PendingWrite,
    /// Persisted effective object arguments.
    ToolArguments,
    /// Compaction preparation.
    CompactionPreparation,
    /// Branch-summary preparation.
    BranchSummaryPreparation,
}

/// Deterministic bounded hydration inventory. No history or ledger reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CurrentReferences {
    /// Required and reserved entry reads, deduplicated without losing roles.
    pub entries: BTreeMap<EntryId, EntryReference>,
    /// Required register reads keyed by exact namespace and unparsed key.
    pub registers: BTreeMap<(RegisterNamespace, String), RegisterConstraint>,
}

/// Fixed, redacted process-local validation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurrentValidationError {
    /// Root kind, key, presence or operation ownership disagrees.
    RootIdentity,
    /// Required entry is absent.
    MissingEntry,
    /// Required register is absent.
    MissingRegister,
    /// Loaded map identity differs from its contained value.
    LoadedIdentity,
    /// Loaded value does not satisfy its required role.
    EntryRole,
    /// Register payload kind differs from its current role.
    RegisterRole,
    /// A pending id has more than one live owner.
    PendingOwnership,
    /// Currently named reservations collide in the shared id namespace.
    ReservationCollision,
    /// Current cancellation and run data disagree.
    Control,
    /// Tool enumeration, pairing, prefix, termination or sequence disagrees.
    ToolBatch,
    /// Structural context/preparation owner or scalar relation disagrees.
    Structural,
    /// Navigation state, intent or source scalars disagree.
    Navigation,
    /// Attempt exceeds its captured normalized cap.
    Attempt,
}

impl std::fmt::Display for CurrentValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "current entry/register validation failed: {self:?}")
    }
}
impl std::error::Error for CurrentValidationError {}

type Result<T> = std::result::Result<T, CurrentValidationError>;

struct Joined<'a> {
    configuration: &'a LaneConfiguration,
    configuration_seq: session_values::SafeUnsigned,
    lane_state: &'a LaneState,
    lane_state_seq: session_values::SafeUnsigned,
    leaf: &'a Option<EntryId>,
    active: Option<(
        &'a Operation,
        &'a OperationState,
        session_values::SafeUnsigned,
    )>,
}

fn join(roots: &CurrentRoots) -> Result<Joined<'_>> {
    let lane = roots.lane.as_str();
    let Register::LaneConfig {
        key,
        value: configuration,
        seq: configuration_seq,
    } = &roots.configuration
    else {
        return Err(CurrentValidationError::RootIdentity);
    };
    if key != lane {
        return Err(CurrentValidationError::RootIdentity);
    }
    let Register::LaneState {
        key,
        value: lane_state,
        seq: lane_state_seq,
    } = &roots.lane_state
    else {
        return Err(CurrentValidationError::RootIdentity);
    };
    if key != lane {
        return Err(CurrentValidationError::RootIdentity);
    }
    let Register::LaneLeaf {
        key, value: leaf, ..
    } = &roots.leaf
    else {
        return Err(CurrentValidationError::RootIdentity);
    };
    if key != lane {
        return Err(CurrentValidationError::RootIdentity);
    }
    let active = match (
        &lane_state.current_operation_id,
        &roots.operation_meta,
        &roots.operation_state,
    ) {
        (None, None, None) => None,
        (
            Some(id),
            Some(Register::OperationMeta {
                key: mk,
                value: meta,
                ..
            }),
            Some(Register::OperationState {
                key: sk,
                value: state,
                seq,
            }),
        ) if mk == id.as_str()
            && sk == id.as_str()
            && &meta.operation_id == id
            && meta.lane == roots.lane =>
        {
            if !matches!(
                (&meta.intent, state.as_ref()),
                (OperationIntent::Run(_), OperationState::Run(_))
                    | (
                        OperationIntent::Compaction(_),
                        OperationState::Compaction(_)
                    )
                    | (
                        OperationIntent::Navigation(_),
                        OperationState::Navigation(_)
                    )
            ) {
                return Err(CurrentValidationError::RootIdentity);
            }
            Some((meta, state.as_ref(), *seq))
        }
        _ => return Err(CurrentValidationError::RootIdentity),
    };
    Ok(Joined {
        configuration,
        configuration_seq: *configuration_seq,
        lane_state,
        lane_state_seq: *lane_state_seq,
        leaf,
        active,
    })
}

/// Validate root joins and derive all required/reserved bounded point reads.
pub fn collect_current_references(roots: &CurrentRoots) -> Result<CurrentReferences> {
    inventory::collect(&join(roots)?)
}

/// Validate detached, already codec-checked values and preserve the existing CAS
/// projection. `None` denotes idle; it does not initialize or mutate the lane.
/// Unrelated loaded history is ignored, including its unrequested map identities.
pub fn validate_current_entries_and_registers(
    roots: &CurrentRoots,
    entries: &BTreeMap<EntryId, Entry>,
    registers: &BTreeMap<(RegisterNamespace, String), Register>,
) -> Result<Option<CurrentOperation>> {
    let joined = join(roots)?;
    let references = inventory::collect(&joined)?;
    semantic::validate(&references, entries, registers)?;
    if let Some((operation, state, operation_state_seq)) = joined.active {
        if let OperationState::Run(run) = state
            && let RunPhase::Tools(tools) = &run.phase
        {
            semantic::tools(&tools.batch, entries)?;
        }
        Ok(Some(CurrentOperation {
            operation: operation.clone(),
            state: state.clone(),
            operation_state_seq,
            lane_state: joined.lane_state.clone(),
            lane_state_seq: joined.lane_state_seq,
            leaf_id: joined.leaf.clone(),
            configuration: joined.configuration.clone(),
            configuration_seq: joined.configuration_seq,
        }))
    } else {
        Ok(None)
    }
}
