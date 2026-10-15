use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current native session header. Timestamps are Unix milliseconds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionHeader {
    pub id: String,
    pub cwd: String,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session: Option<String>,
}

/// One immutable node in a session's transcript tree.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEntry {
    pub id: String,
    pub parent_id: Option<String>,
    pub timestamp: u64,
    #[serde(flatten)]
    pub content: EntryContent,
}

/// Current Pi Coding entry kinds. Message envelopes stay open to custom roles.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum EntryContent {
    Message {
        message: Value,
    },
    ThinkingLevelChange {
        thinking_level: String,
    },
    ModelChange {
        provider: String,
        model_id: String,
    },
    Usage {
        kind: String,
        provider: String,
        model: String,
        usage: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    Compaction {
        summary: String,
        first_kept_entry_id: String,
        tokens_before: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        from_hook: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        system_message: Option<Value>,
    },
    BranchSummary {
        from_id: String,
        summary: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        from_hook: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Value>,
    },
    Custom {
        custom_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<Value>,
    },
    Label {
        target_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    SessionInfo {
        name: String,
    },
    CustomMessage {
        custom_type: String,
        content: Value,
        display: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
    },
    ContextEdit {
        target_id: String,
        replacement: Option<ContentReplacement>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContentReplacement {
    pub content: Value,
}

/// Complete snapshot published after one successful store commit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    pub header: SessionHeader,
    pub revision: u64,
    pub leaf_id: Option<String>,
    pub entries: Vec<SessionEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelection {
    pub provider: String,
    pub model_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedEntry {
    pub source_entry: SessionEntry,
    pub messages: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionProjection {
    pub entries: Vec<ProjectedEntry>,
    pub messages: Vec<Value>,
    pub thinking_level: String,
    pub model: Option<ModelSelection>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionInfo {
    pub header: SessionHeader,
    pub name: Option<String>,
    pub entry_count: usize,
    pub last_timestamp: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TreeNode {
    pub entry: SessionEntry,
    pub children: Vec<TreeNode>,
    pub label: Option<String>,
    pub label_timestamp: Option<u64>,
}

/// Appends records and moves the active leaf in one version-checked commit.
#[derive(Clone, Debug)]
pub struct SessionCommit {
    pub expected_revision: u64,
    pub append: Vec<SessionEntry>,
    pub leaf_id: Option<String>,
}
