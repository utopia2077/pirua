//! Public operation outcomes, expected errors, and exact wire result envelopes.

#![allow(missing_docs)]

use pi_ai_core::{DeferredHandle, JsonValue, Presence, SafeUnsigned, Usage};
use serde::{Deserialize, Serialize};

use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};
use crate::{
    AgentMessage, BranchSummaryEntry, CompactionEntry, EntryId, LaneId, OperationError,
    OperationId, OperationKind, SettledAssistantMessage, UsageId,
};

/// Missing provider/tool identities reported without mutating durable state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingIdentities {
    /// Missing tool names.
    pub tools: Vec<String>,
    /// Missing provider/model identities.
    pub models: Vec<String>,
}

/// Literal operation-outcome kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicOutcomeKind {
    /// Completed normally.
    #[serde(rename = "completed")]
    Completed,
    /// Caller-owned cancellation completed.
    #[serde(rename = "aborted")]
    Aborted,
    /// Operation failed in-band.
    #[serde(rename = "failed")]
    Failed,
    /// Operation remains open and suspended.
    #[serde(rename = "suspended")]
    Suspended,
    /// Structural decision declined work.
    #[serde(rename = "declined")]
    Declined,
}

/// Literal suspended-operation reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SuspensionReason {
    /// A provider returned a deferred handle.
    #[serde(rename = "deferred")]
    Deferred,
    /// Captured provider/tool identities are unavailable.
    #[serde(rename = "missing_identities")]
    MissingIdentities,
}

/// Run completed with a final assistant pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunCompletedWithAssistant {
    /// Literal outcome kind.
    pub kind: CompletedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
    /// Final assistant entry identity.
    pub final_entry_id: EntryId,
    /// Final assistant message.
    pub final_message: SettledAssistantMessage,
}

/// Run completed without a final assistant because all tools terminated.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunCompletedWithoutAssistant {
    /// Literal outcome kind.
    pub kind: CompletedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
}

/// Aborted run with a final assistant pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunAbortedWithAssistant {
    /// Literal outcome kind.
    pub kind: AbortedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
    /// Final assistant entry identity.
    pub final_entry_id: EntryId,
    /// Final assistant message.
    pub final_message: SettledAssistantMessage,
}

/// Aborted run without a final assistant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunAbortedWithoutAssistant {
    /// Literal outcome kind.
    pub kind: AbortedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
}

/// Failed run with a final assistant pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunFailedWithAssistant {
    /// Literal outcome kind.
    pub kind: FailedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
    /// In-band operation failure.
    pub error: OperationError,
    /// Final assistant entry identity.
    pub final_entry_id: EntryId,
    /// Final assistant message.
    pub final_message: SettledAssistantMessage,
}

/// Failed run without a final assistant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunFailedWithoutAssistant {
    /// Literal outcome kind.
    pub kind: FailedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
    /// In-band operation failure.
    pub error: OperationError,
}

/// Deferred run suspension.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunDeferred {
    /// Literal suspended outcome.
    pub kind: SuspendedPublicOutcome,
    /// Literal deferred reason.
    pub reason: DeferredSuspension,
    /// Current non-null leaf.
    pub leaf_id: EntryId,
    /// Deferred response entry.
    pub final_entry_id: EntryId,
    /// Provider handle.
    pub deferred: DeferredHandle,
}

/// Run suspension caused by missing identities.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunMissingIdentities {
    /// Literal suspended outcome.
    pub kind: SuspendedPublicOutcome,
    /// Literal missing-identities reason.
    pub reason: MissingIdentitiesSuspension,
    /// Current non-null leaf.
    pub leaf_id: EntryId,
    /// Missing identities.
    pub missing: MissingIdentities,
}

macro_rules! literal_enum {
    ($name:ident, $variant:ident, $wire:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
        pub enum $name {
            /// The sole literal value.
            #[default]
            #[serde(rename = $wire)]
            $variant,
        }
    };
}

literal_enum!(
    CompletedPublicOutcome,
    Completed,
    "completed",
    "Literal completed public outcome."
);
literal_enum!(
    AbortedPublicOutcome,
    Aborted,
    "aborted",
    "Literal aborted public outcome."
);
literal_enum!(
    FailedPublicOutcome,
    Failed,
    "failed",
    "Literal failed public outcome."
);
literal_enum!(
    DeclinedPublicOutcome,
    Declined,
    "declined",
    "Literal declined public outcome."
);
literal_enum!(
    SuspendedPublicOutcome,
    Suspended,
    "suspended",
    "Literal suspended public outcome."
);
literal_enum!(
    DeferredSuspension,
    Deferred,
    "deferred",
    "Literal deferred suspension reason."
);
literal_enum!(
    MissingIdentitiesSuspension,
    MissingIdentities,
    "missing_identities",
    "Literal missing-identities suspension reason."
);
literal_enum!(RunOperation, Run, "run", "Literal resumed run operation.");
literal_enum!(
    CompactionOperation,
    Compaction,
    "compaction",
    "Literal resumed compaction operation."
);
literal_enum!(
    NavigationOperation,
    Navigation,
    "navigation",
    "Literal resumed navigation operation."
);

/// Complete structurally exclusive public run outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunOutcome {
    /// Completed with a final assistant.
    CompletedWithAssistant(RunCompletedWithAssistant),
    /// Completed through terminating tools.
    CompletedWithoutAssistant(RunCompletedWithoutAssistant),
    /// Aborted with a final assistant.
    AbortedWithAssistant(RunAbortedWithAssistant),
    /// Aborted without a final assistant.
    AbortedWithoutAssistant(RunAbortedWithoutAssistant),
    /// Failed with a final assistant.
    FailedWithAssistant(Box<RunFailedWithAssistant>),
    /// Failed without a final assistant.
    FailedWithoutAssistant(RunFailedWithoutAssistant),
    /// Deferred suspension.
    Deferred(RunDeferred),
    /// Missing-identity suspension.
    MissingIdentities(RunMissingIdentities),
}

/// Completed compaction outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionCompleted {
    /// Literal completed outcome.
    pub kind: CompletedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
    /// Published compaction entry.
    pub entry: CompactionEntry,
}

/// Declined compaction outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionDeclined {
    /// Literal declined outcome.
    pub kind: DeclinedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
}

/// Aborted compaction outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionAborted {
    /// Literal aborted outcome.
    pub kind: AbortedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
}

/// Failed compaction outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionFailed {
    /// Literal failed outcome.
    pub kind: FailedPublicOutcome,
    /// Terminal non-null leaf.
    pub leaf_id: EntryId,
    /// In-band failure.
    pub error: OperationError,
}

/// Missing-identity compaction suspension.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionMissingIdentities {
    /// Literal suspended outcome.
    pub kind: SuspendedPublicOutcome,
    /// Literal missing-identities reason.
    pub reason: MissingIdentitiesSuspension,
    /// Current non-null leaf.
    pub leaf_id: EntryId,
    /// Missing identities.
    pub missing: MissingIdentities,
}

/// Complete public compaction outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompactionOutcome {
    /// Completed compaction.
    Completed(CompactionCompleted),
    /// Declined compaction.
    Declined(CompactionDeclined),
    /// Aborted compaction.
    Aborted(CompactionAborted),
    /// Failed compaction.
    Failed(CompactionFailed),
    /// Missing-identity suspension.
    MissingIdentities(CompactionMissingIdentities),
}

/// Completed navigation outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationCompleted {
    /// Literal completed outcome.
    pub kind: CompletedPublicOutcome,
    /// Source leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub old_leaf_id: Option<EntryId>,
    /// Published leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub new_leaf_id: Option<EntryId>,
    /// Optional branch-summary entry.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub summary_entry: Option<BranchSummaryEntry>,
}

/// Declined navigation outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationDeclined {
    /// Literal declined outcome.
    pub kind: DeclinedPublicOutcome,
    /// Terminal leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
}

/// Aborted navigation outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationAborted {
    /// Literal aborted outcome.
    pub kind: AbortedPublicOutcome,
    /// Terminal leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
}

/// Failed navigation outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationFailed {
    /// Literal failed outcome.
    pub kind: FailedPublicOutcome,
    /// Terminal leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// In-band failure.
    pub error: OperationError,
}

/// Missing-identity navigation suspension.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigationMissingIdentities {
    /// Literal suspended outcome.
    pub kind: SuspendedPublicOutcome,
    /// Literal missing-identities reason.
    pub reason: MissingIdentitiesSuspension,
    /// Current leaf, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
    /// Missing identities.
    pub missing: MissingIdentities,
}

/// Complete public navigation outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NavigationOutcome {
    /// Completed navigation.
    Completed(Box<NavigationCompleted>),
    /// Declined navigation.
    Declined(NavigationDeclined),
    /// Aborted navigation.
    Aborted(NavigationAborted),
    /// Failed navigation.
    Failed(NavigationFailed),
    /// Missing-identity suspension.
    MissingIdentities(NavigationMissingIdentities),
}

/// Run acceptance success, including durable operation id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSuccess {
    /// Durable operation identity under its public compatibility name.
    pub run_id: OperationId,
    /// Accepted operation outcome.
    #[serde(flatten)]
    pub outcome: RunOutcome,
}

/// Compaction acceptance success.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionSuccess {
    /// Durable operation identity.
    pub run_id: OperationId,
    /// Accepted operation outcome.
    #[serde(flatten)]
    pub outcome: CompactionOutcome,
}

/// Navigation acceptance success.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NavigationSuccess {
    /// Durable operation identity.
    pub run_id: OperationId,
    /// Accepted operation outcome.
    #[serde(flatten)]
    pub outcome: NavigationOutcome,
}

macro_rules! resume_wrapper {
    ($name:ident, $operation:ty, $outcome:ty, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $name {
            /// Structurally fixed operation marker.
            pub operation: $operation,
            /// Durable operation identity.
            pub run_id: OperationId,
            /// Structurally compatible concrete outcome.
            #[serde(flatten)]
            pub outcome: $outcome,
        }
    };
}

resume_wrapper!(
    ResumeRunCompletedWithAssistant,
    RunOperation,
    RunCompletedWithAssistant,
    "Resumed run completed with a final assistant."
);
resume_wrapper!(
    ResumeRunCompletedWithoutAssistant,
    RunOperation,
    RunCompletedWithoutAssistant,
    "Resumed run completed without a final assistant."
);
resume_wrapper!(
    ResumeRunAbortedWithAssistant,
    RunOperation,
    RunAbortedWithAssistant,
    "Resumed run aborted with a final assistant."
);
resume_wrapper!(
    ResumeRunAbortedWithoutAssistant,
    RunOperation,
    RunAbortedWithoutAssistant,
    "Resumed run aborted without a final assistant."
);
resume_wrapper!(
    ResumeRunFailedWithAssistant,
    RunOperation,
    RunFailedWithAssistant,
    "Resumed run failed with a final assistant."
);
resume_wrapper!(
    ResumeRunFailedWithoutAssistant,
    RunOperation,
    RunFailedWithoutAssistant,
    "Resumed run failed without a final assistant."
);
resume_wrapper!(
    ResumeRunDeferred,
    RunOperation,
    RunDeferred,
    "Resumed run remains deferred."
);
resume_wrapper!(
    ResumeRunMissingIdentities,
    RunOperation,
    RunMissingIdentities,
    "Resumed run lacks identities."
);
resume_wrapper!(
    ResumeCompactionCompleted,
    CompactionOperation,
    CompactionCompleted,
    "Resumed compaction completed."
);
resume_wrapper!(
    ResumeCompactionDeclined,
    CompactionOperation,
    CompactionDeclined,
    "Resumed compaction declined."
);
resume_wrapper!(
    ResumeCompactionAborted,
    CompactionOperation,
    CompactionAborted,
    "Resumed compaction aborted."
);
resume_wrapper!(
    ResumeCompactionFailed,
    CompactionOperation,
    CompactionFailed,
    "Resumed compaction failed."
);
resume_wrapper!(
    ResumeCompactionMissingIdentities,
    CompactionOperation,
    CompactionMissingIdentities,
    "Resumed compaction lacks identities."
);
resume_wrapper!(
    ResumeNavigationCompleted,
    NavigationOperation,
    NavigationCompleted,
    "Resumed navigation completed."
);
resume_wrapper!(
    ResumeNavigationDeclined,
    NavigationOperation,
    NavigationDeclined,
    "Resumed navigation declined."
);
resume_wrapper!(
    ResumeNavigationAborted,
    NavigationOperation,
    NavigationAborted,
    "Resumed navigation aborted."
);
resume_wrapper!(
    ResumeNavigationFailed,
    NavigationOperation,
    NavigationFailed,
    "Resumed navigation failed."
);
resume_wrapper!(
    ResumeNavigationMissingIdentities,
    NavigationOperation,
    NavigationMissingIdentities,
    "Resumed navigation lacks identities."
);

/// Exactly eighteen concrete resume alternatives: eight run, five compaction, five navigation.
///
/// Generic crossed-operation helpers are deliberately absent.
///
/// ```compile_fail,E0432
/// use pi_agent_schema::{CompactionUnsuccessful, ResumeRun};
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResumeOutcome {
    RunCompletedWithAssistant(ResumeRunCompletedWithAssistant),
    RunCompletedWithoutAssistant(ResumeRunCompletedWithoutAssistant),
    RunAbortedWithAssistant(ResumeRunAbortedWithAssistant),
    RunAbortedWithoutAssistant(ResumeRunAbortedWithoutAssistant),
    RunFailedWithAssistant(Box<ResumeRunFailedWithAssistant>),
    RunFailedWithoutAssistant(ResumeRunFailedWithoutAssistant),
    RunDeferred(ResumeRunDeferred),
    RunMissingIdentities(ResumeRunMissingIdentities),
    CompactionCompleted(Box<ResumeCompactionCompleted>),
    CompactionDeclined(ResumeCompactionDeclined),
    CompactionAborted(ResumeCompactionAborted),
    CompactionFailed(ResumeCompactionFailed),
    CompactionMissingIdentities(ResumeCompactionMissingIdentities),
    NavigationCompleted(Box<ResumeNavigationCompleted>),
    NavigationDeclined(ResumeNavigationDeclined),
    NavigationAborted(ResumeNavigationAborted),
    NavigationFailed(ResumeNavigationFailed),
    NavigationMissingIdentities(ResumeNavigationMissingIdentities),
}

macro_rules! expected_tag {
    ($name:ident, $variant:ident, $wire:literal) => {
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
        pub enum $name {
            #[default]
            #[serde(rename = $wire)]
            $variant,
        }
    };
}

expected_tag!(LaneBusyTag, LaneBusy, "LaneBusy");
expected_tag!(MissingIdentitiesTag, MissingIdentities, "MissingIdentities");
expected_tag!(NoActiveRunTag, NoActiveRun, "NoActiveRun");
expected_tag!(NoActiveOperationTag, NoActiveOperation, "NoActiveOperation");
expected_tag!(NothingToResumeTag, NothingToResume, "NothingToResume");
expected_tag!(NothingToCompactTag, NothingToCompact, "NothingToCompact");
expected_tag!(InvalidMessageTag, InvalidMessage, "InvalidMessage");
expected_tag!(InvalidNavigationTag, InvalidNavigation, "InvalidNavigation");
expected_tag!(UnknownSkillTag, UnknownSkill, "UnknownSkill");
expected_tag!(UnknownTemplateTag, UnknownTemplate, "UnknownTemplate");
expected_tag!(UnknownTargetTag, UnknownTarget, "UnknownTarget");
expected_tag!(LaneExistsTag, LaneExists, "LaneExists");
expected_tag!(InvalidLaneTag, InvalidLane, "InvalidLane");
expected_tag!(ClosedTag, Closed, "Closed");

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneBusyError {
    #[serde(rename = "_tag")]
    pub tag: LaneBusyTag,
    pub message: String,
    pub lane: LaneId,
    pub operation_id: OperationId,
    pub operation_kind: OperationKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingIdentitiesError {
    #[serde(rename = "_tag")]
    pub tag: MissingIdentitiesTag,
    pub message: String,
    pub lane: LaneId,
    pub tools: Vec<String>,
    pub models: Vec<String>,
}

macro_rules! lane_expected_error {
    ($name:ident, $tag:ty) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "_tag")]
            pub tag: $tag,
            pub message: String,
            pub lane: LaneId,
        }
    };
}

lane_expected_error!(NoActiveRunError, NoActiveRunTag);
lane_expected_error!(NoActiveOperationError, NoActiveOperationTag);
lane_expected_error!(NothingToResumeError, NothingToResumeTag);
lane_expected_error!(NothingToCompactError, NothingToCompactTag);
lane_expected_error!(LaneExistsError, LaneExistsTag);

macro_rules! lane_reason_expected_error {
    ($name:ident, $tag:ty) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "_tag")]
            pub tag: $tag,
            pub message: String,
            pub lane: LaneId,
            pub reason: String,
        }
    };
}

lane_reason_expected_error!(InvalidMessageError, InvalidMessageTag);
lane_reason_expected_error!(InvalidNavigationError, InvalidNavigationTag);
lane_reason_expected_error!(InvalidLaneError, InvalidLaneTag);

macro_rules! name_expected_error {
    ($name:ident, $tag:ty) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(rename = "_tag")]
            pub tag: $tag,
            pub message: String,
            pub name: String,
        }
    };
}

name_expected_error!(UnknownSkillError, UnknownSkillTag);
name_expected_error!(UnknownTemplateError, UnknownTemplateTag);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnknownTargetError {
    #[serde(rename = "_tag")]
    pub tag: UnknownTargetTag,
    pub message: String,
    pub target_id: EntryId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedError {
    #[serde(rename = "_tag")]
    pub tag: ClosedTag,
    pub message: String,
}

macro_rules! expected_union {
    ($name:ident { $($variant:ident($ty:ty)),+ $(,)? }) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(untagged)]
        pub enum $name { $($variant($ty)),+ }
    };
}

expected_union!(RunExpectedError {
    LaneBusy(LaneBusyError), MissingIdentities(MissingIdentitiesError),
    InvalidMessage(InvalidMessageError), UnknownSkill(UnknownSkillError),
    UnknownTemplate(UnknownTemplateError), Closed(ClosedError),
});
expected_union!(CompactionExpectedError {
    LaneBusy(LaneBusyError), MissingIdentities(MissingIdentitiesError),
    NothingToCompact(NothingToCompactError), Closed(ClosedError),
});
expected_union!(NavigationExpectedError {
    LaneBusy(LaneBusyError), MissingIdentities(MissingIdentitiesError),
    InvalidNavigation(InvalidNavigationError), UnknownTarget(UnknownTargetError), Closed(ClosedError),
});
expected_union!(ResumeExpectedError {
    LaneBusy(LaneBusyError), NothingToResume(NothingToResumeError),
    MissingIdentities(MissingIdentitiesError), Closed(ClosedError),
});
expected_union!(QueueExpectedError {
    NoActiveRun(NoActiveRunError), InvalidMessage(InvalidMessageError), Closed(ClosedError),
});
expected_union!(NextRunExpectedError { InvalidMessage(InvalidMessageError), Closed(ClosedError) });
expected_union!(AbortExpectedError { NoActiveOperation(NoActiveOperationError), Closed(ClosedError) });
expected_union!(CreateLaneExpectedError {
    LaneExists(LaneExistsError), InvalidLane(InvalidLaneError),
    UnknownTarget(UnknownTargetError), Closed(ClosedError),
});

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClosedExpectedError(pub ClosedError);

/// Outer post-acceptance Harness runtime failure.
#[non_exhaustive]
pub enum HarnessRuntimeError {
    /// Close won after acceptance.
    Closed(crate::HarnessClosed),
    /// Admitted storage/invariant/trusted-application failure.
    Fault(crate::HarnessFault),
    /// An accepted extension invocation lost its peer before a valid terminal.
    PeerLost,
}

impl std::fmt::Debug for HarnessRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let category = match self {
            Self::Closed(_) => "closed",
            Self::Fault(_) => "fault",
            Self::PeerLost => "peer_lost",
        };
        formatter
            .debug_struct("HarnessRuntimeError")
            .field("category", &category)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for HarnessRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed(error) => error.fmt(formatter),
            Self::Fault(error) => error.fmt(formatter),
            Self::PeerLost => formatter.write_str("extension peer lost"),
        }
    }
}

impl std::error::Error for HarnessRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Closed(error) => Some(error),
            Self::Fault(error) => Some(error),
            Self::PeerLost => None,
        }
    }
}

/// Native pre-acceptance expected-result channel.
pub type ExpectedResult<T, E> = Result<T, E>;
/// Native nested public Harness call channel.
pub type HarnessCall<T, E> = Result<ExpectedResult<T, E>, HarnessRuntimeError>;

/// Exact wire projection of a Rust `Result`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireResult<T, E> {
    /// Successful value.
    Ok(WireOk<T>),
    /// Expected failure.
    Err(WireErr<E>),
}

/// Successful wire result arm.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireOk<T> {
    /// Literal true success marker.
    pub ok: pi_ai_core::LiteralTrue,
    /// Success value.
    pub value: T,
}

/// Failed wire result arm.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireErr<E> {
    /// Literal false failure marker.
    pub ok: pi_ai_core::LiteralFalse,
    /// Expected error.
    pub error: E,
}

/// Public run method result.
pub type RunResult = ExpectedResult<RunSuccess, RunExpectedError>;
/// Nested public run call.
pub type RunCall = HarnessCall<RunSuccess, RunExpectedError>;
/// Public compaction method result.
pub type CompactionResult = ExpectedResult<CompactionSuccess, CompactionExpectedError>;
/// Nested public compaction call.
pub type CompactionCall = HarnessCall<CompactionSuccess, CompactionExpectedError>;
/// Public navigation method result.
pub type NavigationResult = ExpectedResult<NavigationSuccess, NavigationExpectedError>;
/// Nested public navigation call.
pub type NavigationCall = HarnessCall<NavigationSuccess, NavigationExpectedError>;
/// Public resume method result.
pub type ResumeResult = ExpectedResult<ResumeOutcome, ResumeExpectedError>;
/// Nested public resume call.
pub type ResumeCall = HarnessCall<ResumeOutcome, ResumeExpectedError>;

/// Queued-entry admission value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueAdmission {
    /// Reserved entry identity.
    pub entry_id: EntryId,
}

/// Queue admission result for active runs.
pub type QueueResult = ExpectedResult<QueueAdmission, QueueExpectedError>;
/// Nested public active-run queue call.
pub type QueueCall = HarnessCall<QueueAdmission, QueueExpectedError>;
/// Lane-owned next-run queue admission result.
pub type NextRunResult = ExpectedResult<QueueAdmission, NextRunExpectedError>;
/// Nested public next-run queue call.
pub type NextRunCall = HarnessCall<QueueAdmission, NextRunExpectedError>;

/// Cancel-queued disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CancelQueuedOutcome {
    /// Pending content was deleted.
    Cancelled,
    /// The entry was already placed.
    AlreadyConsumed,
    /// The id was never present or was previously removed.
    NotFound,
}

/// Cancel-queued result.
pub type CancelQueuedResult = ExpectedResult<CancelQueuedOutcome, ClosedExpectedError>;
/// Nested public cancel-queued call.
pub type CancelQueuedCall = HarnessCall<CancelQueuedOutcome, ClosedExpectedError>;

/// Idempotent abort value with exactly drained payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AbortOutcome {
    /// Durable operation identity.
    pub run_id: OperationId,
    /// Drained steering payloads.
    pub steer: Vec<AgentMessage>,
    /// Drained follow-up payloads.
    pub follow_up: Vec<AgentMessage>,
}

/// Public abort result.
pub type AbortResult = ExpectedResult<AbortOutcome, AbortExpectedError>;
/// Nested public abort call.
pub type AbortCall = HarnessCall<AbortOutcome, AbortExpectedError>;

/// Usage adjustment acceptance value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordUsageOutcome {
    /// Minted usage-row identity.
    pub usage_id: UsageId,
}

/// Public usage-adjustment result.
pub type RecordUsageResult = ExpectedResult<RecordUsageOutcome, ClosedExpectedError>;
/// Nested public usage-adjustment call.
pub type RecordUsageCall = HarnessCall<RecordUsageOutcome, ClosedExpectedError>;

/// Usage adjustment options.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordUsageOptions {
    /// Optional associated entry.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_id: Option<EntryId>,
    /// Optional details, preserving explicit null.
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub details: Presence<JsonValue>,
}

/// Manual compaction options.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactOptions {
    /// Optional caller instructions.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
}

/// Navigation options.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NavigateOptions {
    /// Whether to summarize the abandoned branch.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub summarize: Option<bool>,
    /// Optional target label.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub label: Option<String>,
    /// Optional summary instructions.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub custom_instructions: Option<String>,
}

/// Usage adjustment request.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordUsageRequest {
    /// Caller-supplied usage.
    pub usage: Usage,
    /// Optional association and details.
    pub options: RecordUsageOptions,
}

/// Process-local public operation result used by the planner.
#[derive(Clone, Debug, PartialEq)]
pub enum OperationResult {
    /// Run outcome.
    Run(Box<RunOutcome>),
    /// Compaction outcome.
    Compaction(CompactionOutcome),
    /// Navigation outcome.
    Navigation(NavigationOutcome),
}

/// Retry event fields shared by public snapshots and actions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryInfo {
    /// Current one-based attempt.
    pub attempt: crate::OneBasedSafeUnsigned,
    /// Maximum one-based attempts.
    pub max_attempts: crate::OneBasedSafeUnsigned,
    /// Next-attempt Unix timestamp in milliseconds.
    pub next_attempt_at: SafeUnsigned,
}
