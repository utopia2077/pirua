//! Detached protocol records. IDs share an allocation namespace but not a Rust type.
use crate::{Error, Result, delta::Op};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
macro_rules! id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(u64);
        impl $name {
            pub fn new(value: u64) -> Result<Self> {
                if value == 0 || value > MAX_SAFE_INTEGER {
                    Err(Error::validation(concat!(
                        stringify!($name),
                        " must be a positive safe integer"
                    )))
                } else {
                    Ok(Self(value))
                }
            }
            pub const fn get(self) -> u64 {
                self.0
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
                Self::new(u64::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
id!(ConversationId);
id!(EntryId);
id!(TaskId);
id!(SubmissionId);
id!(DocumentId);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct Seq(u64);
impl Seq {
    pub fn new(value: u64) -> Result<Self> {
        if value <= MAX_SAFE_INTEGER {
            Ok(Self(value))
        } else {
            Err(Error::validation("Seq must be a nonnegative safe integer"))
        }
    }
    pub const fn get(self) -> u64 {
        self.0
    }
}
impl<'de> Deserialize<'de> for Seq {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::new(u64::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
pub const ROOT_CONVERSATION_ID: ConversationId = ConversationId(1);
fn present<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationParent {
    pub conversation_id: ConversationId,
    pub at: EntryId,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationOwner {
    pub conversation_id: ConversationId,
    pub task_id: TaskId,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationRecord {
    pub id: ConversationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ConversationParent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<ConversationOwner>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase", deny_unknown_fields)]
pub enum ContextEdit {
    Omit {
        target: EntryId,
    },
    Replace {
        target: EntryId,
        messages: Vec<Value>,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryRecord {
    pub id: EntryId,
    pub conversation_id: ConversationId,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Vec<Value>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<EntryId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edits: Option<Vec<ContextEdit>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by_task_id: Option<TaskId>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutcomeError {
    pub message: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub detail: Option<Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
pub enum TaskOutcome {
    Completed {
        result: Value,
    },
    Failed {
        error: TaskOutcomeError,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        result: Option<Value>,
    },
    Aborted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        result: Option<Value>,
    },
    Orphaned {
        reason: String,
    },
    Faulted {
        error: TaskOutcomeError,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinPolicy {
    FailFast,
    AllSettled,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
pub enum TaskState {
    Pending {
        checkpoint: Value,
    },
    Running {
        checkpoint: Value,
    },
    Waiting {
        checkpoint: Value,
        on: Vec<TaskId>,
        policy: JoinPolicy,
    },
    Sleeping {
        checkpoint: Value,
        until: f64,
    },
    Completing {
        outcome: TaskOutcome,
    },
    Terminal {
        outcome: TaskOutcome,
    },
}
impl TaskState {
    pub fn status(&self) -> &'static str {
        match self {
            Self::Pending { .. } => "pending",
            Self::Running { .. } => "running",
            Self::Waiting { .. } => "waiting",
            Self::Sleeping { .. } => "sleeping",
            Self::Completing { .. } => "completing",
            Self::Terminal { .. } => "terminal",
        }
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal { .. })
    }
    pub fn can_own(&self) -> bool {
        !matches!(self, Self::Completing { .. } | Self::Terminal { .. })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskRecord {
    pub id: TaskId,
    pub conversation_id: ConversationId,
    pub kind: String,
    pub version: u32,
    pub input: Value,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub trace: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<TaskId>,
    pub background: bool,
    pub abort_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_reason: Option<AbortReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abandon_on_restart: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restarts: Option<u64>,
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memos: Option<serde_json::Map<String, Value>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AbortReason {
    Restart,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
pub enum SubmissionState {
    Queued,
    Placed {
        entry: EntryId,
    },
    Done {
        entry: EntryId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        answer: Option<EntryId>,
    },
    Unanswered {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry: Option<EntryId>,
        reason: String,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        detail: Option<Value>,
    },
}
impl SubmissionState {
    pub fn status(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Placed { .. } => "placed",
            Self::Done { .. } => "done",
            Self::Unanswered { .. } => "unanswered",
        }
    }
    pub fn settled(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Unanswered { .. })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubmissionType {
    Input,
    Write,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmissionRecord {
    pub id: SubmissionId,
    pub conversation_id: ConversationId,
    #[serde(rename = "type")]
    pub submission_type: SubmissionType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub trace: Option<Value>,
    #[serde(flatten)]
    pub state: SubmissionState,
}
impl SubmissionRecord {
    pub fn validate(&self) -> Result<()> {
        let valid = !matches!(
            (&self.submission_type, &self.state),
            (SubmissionType::Write, SubmissionState::Placed { .. })
                | (
                    SubmissionType::Write,
                    SubmissionState::Done {
                        answer: Some(_),
                        ..
                    }
                )
                | (
                    SubmissionType::Input,
                    SubmissionState::Done { answer: None, .. }
                )
                | (
                    SubmissionType::Write,
                    SubmissionState::Unanswered { entry: Some(_), .. }
                )
        );
        if valid {
            Ok(())
        } else {
            Err(Error::validation(
                "invalid submission type/state combination",
            ))
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Scope {
    Session,
    Conversation {
        #[serde(rename = "conversationId")]
        conversation_id: ConversationId,
    },
    Task {
        #[serde(rename = "taskId")]
        task_id: TaskId,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum History {
    Latest,
    Rewindable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ForkPolicy {
    AsOf,
    Current,
    Initial,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocumentAddress {
    pub kind: String,
    pub scope: Scope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocumentCreate {
    pub id: DocumentId,
    pub kind: String,
    pub scope: Scope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<History>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<ForkPolicy>,
}
impl DocumentCreate {
    pub fn address(&self) -> DocumentAddress {
        DocumentAddress {
            kind: self.kind.clone(),
            scope: self.scope.clone(),
            key: self.key.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        let valid = if matches!(self.scope, Scope::Conversation { .. }) {
            self.history.is_some()
                && self.fork.is_some()
                && !(self.history == Some(History::Latest) && self.fork == Some(ForkPolicy::AsOf))
        } else {
            self.history.is_none() && self.fork.is_none()
        };
        if valid && !self.kind.is_empty() {
            Ok(())
        } else {
            Err(Error::validation(
                "invalid document scope/history/fork policy",
            ))
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentRecord {
    #[serde(flatten)]
    pub identity: DocumentCreate,
    pub created_at: Seq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<Seq>,
}
impl DocumentRecord {
    pub fn alive(&self, at: DocumentPoint) -> bool {
        match at {
            DocumentPoint::Current => self.retired_at.is_none(),
            DocumentPoint::At(at) => {
                self.created_at <= at && self.retired_at.is_none_or(|retired| at < retired)
            }
        }
    }
    pub fn rewindable(&self) -> bool {
        self.identity.history == Some(History::Rewindable)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentPoint {
    At(Seq),
    Current,
}
impl Serialize for DocumentPoint {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::At(at) => at.serialize(s),
            Self::Current => s.serialize_str("current"),
        }
    }
}
impl<'de> Deserialize<'de> for DocumentPoint {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        if v == Value::String("current".into()) {
            Ok(Self::Current)
        } else {
            serde_json::from_value(v)
                .map(Self::At)
                .map_err(serde::de::Error::custom)
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum DocumentContent {
    Base { version: u32, value: Value },
    Delta { version: u32, ops: Vec<Op> },
}
impl DocumentContent {
    pub fn version(&self) -> u32 {
        match self {
            Self::Base { version, .. } | Self::Delta { version, .. } => *version,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentCopySource {
    pub id: DocumentId,
    pub at: DocumentPoint,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredDocument {
    pub record: DocumentRecord,
    pub version: u32,
    pub value: Value,
    pub deltas_since_base: usize,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum StorageWrite {
    Conversation {
        value: ConversationRecord,
    },
    Entry {
        value: EntryRecord,
    },
    Task {
        value: TaskRecord,
    },
    Submission {
        value: SubmissionRecord,
    },
    #[serde(rename = "document.create")]
    DocumentCreate {
        record: DocumentCreate,
        content: DocumentContent,
    },
    #[serde(rename = "document.copy")]
    DocumentCopy {
        record: DocumentCreate,
        source: DocumentCopySource,
    },
    #[serde(rename = "document.change")]
    DocumentChange {
        id: DocumentId,
        content: DocumentContent,
    },
    #[serde(rename = "document.retire")]
    DocumentRetire {
        id: DocumentId,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScanOrder {
    Ascending,
    Descending,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationQuery {
    pub owner_conversation_id: Option<ConversationId>,
    pub owner_task_id: Option<TaskId>,
    pub order: Option<ScanOrder>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryQuery {
    pub conversation_id: ConversationId,
    pub min_entry_id: Option<EntryId>,
    pub max_entry_id: Option<EntryId>,
    pub order: Option<ScanOrder>,
}
impl EntryQuery {
    pub fn new(conversation_id: ConversationId) -> Self {
        Self {
            conversation_id,
            min_entry_id: None,
            max_entry_id: None,
            order: None,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskQuery {
    pub conversation_id: Option<ConversationId>,
    pub kind: Option<String>,
    pub status: Option<String>,
    pub abort_requested: Option<bool>,
    pub background: Option<bool>,
    pub order: Option<ScanOrder>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmissionQuery {
    pub conversation_id: Option<ConversationId>,
    pub status: Option<String>,
    pub order: Option<ScanOrder>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentQuery {
    pub scope: Scope,
    pub at: DocumentPoint,
    pub kind: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(pub Value);
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<Cursor>,
}
