//! Session, tree-view, repository, and fork interfaces.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use pi_ai_core::{CustomRole, JsonSchema, JsonValue, SafeUnsigned};
use serde::{Deserialize, Serialize};

use crate::serde_support::{deserialize_optional_non_null, deserialize_required_nullable};
use crate::surface::BoxFuture;
use crate::{
    AgentMessage, BranchScan, CommitResult, CustomType, Entry, EntryId, EntryMap, EntryQuery,
    LaneId, Register, RegisterNamespace, SessionError, SessionId, SessionStats, Transaction,
};

/// Repository-visible session metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionMetadata {
    /// Session identity.
    pub id: SessionId,
    /// Creation timestamp in Unix milliseconds.
    pub created_at: SafeUnsigned,
    /// Current durable storage schema version.
    pub storage_version: SafeUnsigned,
    /// Precise-rewrite generation used by derived search cursors.
    pub store_generation: SafeUnsigned,
    /// Optional application working directory.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cwd: Option<String>,
    /// Optional parent session identity.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_session_id: Option<SessionId>,
    /// Unresolved legacy parent path retained during import.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub legacy_parent_session_path: Option<String>,
}

/// Open contract for repository-specific metadata that retains required base metadata.
///
/// ```compile_fail,E0277
/// use pi_agent_schema::SessionMetadataRecord;
/// fn require_metadata<T: SessionMetadataRecord>() {}
/// require_metadata::<()>();
/// ```
pub trait SessionMetadataRecord: Clone + Send + Sync + 'static {
    /// Returns the required common session metadata, including `storeGeneration`.
    fn base_metadata(&self) -> &SessionMetadata;
}

impl SessionMetadataRecord for SessionMetadata {
    fn base_metadata(&self) -> &SessionMetadata {
        self
    }
}

/// Error registering a custom role schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionCodecOptionsError {
    /// The supplied role was empty.
    EmptyRole,
    /// The supplied role shadows a built-in provider-message role.
    BuiltInRole,
}

impl fmt::Display for SessionCodecOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRole => formatter.write_str("custom message role must be nonempty"),
            Self::BuiltInRole => formatter.write_str("custom message role shadows a built-in role"),
        }
    }
}

impl std::error::Error for SessionCodecOptionsError {}

/// Deterministic custom-role schema registry used by repository codecs.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionCodecOptions {
    custom_message_schemas: BTreeMap<CustomRole, JsonSchema>,
}

impl SessionCodecOptions {
    /// Creates options from already validated custom roles.
    pub fn new(custom_message_schemas: BTreeMap<CustomRole, JsonSchema>) -> Self {
        Self {
            custom_message_schemas,
        }
    }

    /// Validates open role strings and rejects all built-in roles.
    pub fn from_role_schemas(
        schemas: BTreeMap<String, JsonSchema>,
    ) -> Result<Self, SessionCodecOptionsError> {
        let custom_message_schemas = schemas
            .into_iter()
            .map(|(role, schema)| {
                let error = if role.is_empty() {
                    SessionCodecOptionsError::EmptyRole
                } else {
                    SessionCodecOptionsError::BuiltInRole
                };
                CustomRole::new(role)
                    .map(|role| (role, schema))
                    .map_err(|_| error)
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(Self::new(custom_message_schemas))
    }

    /// Returns schemas in deterministic role order.
    pub const fn custom_message_schemas(&self) -> &BTreeMap<CustomRole, JsonSchema> {
        &self.custom_message_schemas
    }
}

/// Position of a branch fork relative to its selected entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ForkPosition {
    /// Fork before the selected entry.
    #[serde(rename = "before")]
    Before,
    /// Fork at and including the selected entry.
    #[serde(rename = "at")]
    At,
}

/// Branch-scope fork options.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BranchForkOptions {
    /// Optional literal branch scope; omission defaults to branch.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub scope: Option<BranchForkScope>,
    /// Optional selected entry. Omission selects the source view leaf.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_id: Option<EntryId>,
    /// Optional position relative to the selected entry.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub position: Option<ForkPosition>,
}

/// Literal branch fork scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum BranchForkScope {
    /// One branch path.
    #[default]
    #[serde(rename = "branch")]
    Branch,
}

/// Whole-tree fork options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeForkOptions {
    /// Literal tree scope.
    pub scope: TreeForkScope,
}

/// Literal whole-tree fork scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TreeForkScope {
    /// Complete tree.
    #[default]
    #[serde(rename = "tree")]
    Tree,
}

/// Fork scope and selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ForkOptions {
    /// Copy one path and create destination `main` only.
    Branch(BranchForkOptions),
    /// Copy the complete tree and configured lanes.
    Tree(TreeForkOptions),
}

/// Standard repository create options.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionCreateOptions {
    /// Caller-selected identity.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub id: Option<SessionId>,
    /// Parent session identity.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_session_id: Option<SessionId>,
    /// Optional application working directory.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cwd: Option<String>,
}

/// Typed fork request combining source selection and destination create options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionForkRequest<C> {
    /// Tree selection and fork scope.
    pub fork: ForkOptions,
    /// Repository-specific destination create options.
    pub create: C,
}

/// Process-local UUIDv7-compatible identifier generator.
pub trait IdGenerator: Send + Sync {
    /// Mints an id, optionally inheriting a supplied Unix-millisecond timestamp.
    fn next(&self, timestamp_ms: Option<SafeUnsigned>) -> String;
}

/// Behavior-free lane-bound conversation-tree interface.
pub trait SessionTree: Send + Sync {
    /// Reads the view leaf, or root when null.
    fn get_leaf_id(&self) -> BoxFuture<'_, Result<Option<EntryId>, SessionError>>;
    /// Reads one committed entry.
    fn get_entry(&self, id: EntryId) -> BoxFuture<'_, Result<Option<Entry>, SessionError>>;
    /// Reads maintained statistics.
    fn get_stats(&self) -> BoxFuture<'_, Result<SessionStats, SessionError>>;
    /// Reads the session name fact.
    fn get_name(&self) -> BoxFuture<'_, Result<Option<String>, SessionError>>;
    /// Sets or deletes the session name fact.
    fn set_name(&self, name: Option<String>) -> BoxFuture<'_, Result<(), SessionError>>;
    /// Reads an entry label.
    fn get_label(&self, target_id: EntryId) -> BoxFuture<'_, Result<Option<String>, SessionError>>;
    /// Sets or deletes an entry label.
    fn set_label(
        &self,
        target_id: EntryId,
        label: Option<String>,
    ) -> BoxFuture<'_, Result<(), SessionError>>;
    /// Reads a custom fact. `Some(JsonValue::Null)` remains data.
    fn get_custom_fact<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, SessionError>>;
    /// Sets a custom fact or deletes it when `None`.
    fn set_custom_fact(
        &self,
        key: String,
        value: Option<JsonValue>,
    ) -> BoxFuture<'_, Result<(), SessionError>>;
    /// Finds session-wide entries.
    fn find_entries(&self, query: EntryQuery) -> BoxFuture<'_, Result<Vec<Entry>, SessionError>>;
    /// Finds one session-wide entry.
    fn find_entry(&self, query: EntryQuery) -> BoxFuture<'_, Result<Option<Entry>, SessionError>>;
    /// Finds entries on this view's branch.
    fn find_entries_on_branch(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<Entry>, SessionError>>;
    /// Finds one entry on this view's branch.
    fn find_entry_on_branch(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Option<Entry>, SessionError>>;
    /// Appends a settled Agent message.
    fn append_message(&self, message: AgentMessage)
    -> BoxFuture<'_, Result<EntryId, SessionError>>;
    /// Appends an application custom entry.
    fn append_custom_entry(
        &self,
        custom_type: CustomType,
        data: pi_ai_core::Presence<JsonValue>,
    ) -> BoxFuture<'_, Result<EntryId, SessionError>>;
}

/// Exact SessionTree method inventory.
pub const SESSION_TREE_METHODS: &[&str] = &[
    "get_leaf_id",
    "get_entry",
    "get_stats",
    "get_name",
    "set_name",
    "get_label",
    "set_label",
    "get_custom_fact",
    "set_custom_fact",
    "find_entries",
    "find_entry",
    "find_entries_on_branch",
    "find_entry_on_branch",
    "append_message",
    "append_custom_entry",
];

/// Behavior-free typed session interface used internally by Harness.
pub trait Session: SessionTree {
    /// Metadata projection selected by the repository.
    type Metadata: SessionMetadataRecord;

    /// Returns detached repository metadata.
    fn metadata(&self) -> Self::Metadata;
    /// Returns the process-local id generator.
    fn id_generator(&self) -> &dyn IdGenerator;
    /// Creates a lane-bound tree view.
    fn view(&self, lane: LaneId) -> Arc<dyn SessionTree>;
    /// Validates and commits an internal transaction.
    fn commit(&self, transaction: Transaction)
    -> BoxFuture<'_, Result<CommitResult, SessionError>>;
    /// Reads exact detached entries for bounded hydration.
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, SessionError>>;
    /// Reads one typed register.
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, SessionError>>;
    /// Lists current registers in one namespace and prefix.
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, SessionError>>;
    /// Idempotently closes the session.
    fn close(&self) -> BoxFuture<'_, Result<(), SessionError>>;
}

/// Exact Session-owned method inventory, excluding inherited SessionTree methods.
pub const SESSION_METHODS: &[&str] = &[
    "metadata",
    "id_generator",
    "view",
    "commit",
    "get_entries",
    "get_register",
    "list_registers",
    "close",
];

/// Behavior-free repository discovery and session-lifecycle interface.
pub trait SessionRepo: Send + Sync {
    /// Repository metadata type.
    type Metadata: SessionMetadataRecord;
    /// Repository-specific create options.
    type CreateOptions: Clone + Send + Sync + 'static;
    /// Repository-specific list options.
    type ListOptions: Clone + Send + Sync + 'static;
    /// Concrete or erased session handle type.
    type Session: Session<Metadata = Self::Metadata> + ?Sized + 'static;

    /// Creates a session.
    fn create(
        &self,
        options: Self::CreateOptions,
    ) -> BoxFuture<'_, Result<Arc<Self::Session>, SessionError>>;
    /// Opens a discovered session.
    fn open(
        &self,
        metadata: Self::Metadata,
    ) -> BoxFuture<'_, Result<Arc<Self::Session>, SessionError>>;
    /// Lists repository metadata through typed options.
    fn list(
        &self,
        options: Self::ListOptions,
    ) -> BoxFuture<'_, Result<Vec<Self::Metadata>, SessionError>>;
    /// Deletes a session selected by metadata.
    fn delete(&self, metadata: Self::Metadata) -> BoxFuture<'_, Result<(), SessionError>>;
    /// Forks a source session into a typed destination.
    fn fork(
        &self,
        source: Self::Metadata,
        options: SessionForkRequest<Self::CreateOptions>,
    ) -> BoxFuture<'_, Result<Arc<Self::Session>, SessionError>>;
}

/// Exact SessionRepo method inventory.
pub const SESSION_REPO_METHODS: &[&str] = &["create", "open", "list", "delete", "fork"];

/// Required nullable session-tree leaf projection used by DTO adapters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionLeaf {
    /// Leaf entry, or root when null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub leaf_id: Option<EntryId>,
}
