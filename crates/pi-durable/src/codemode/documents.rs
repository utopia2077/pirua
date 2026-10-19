use crate::{Result, ToolExecutionResult, session::*};
use pi_codemode::{DeclaredGlobal, DeclaredTool, Outcome};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Adapter interpretation version, independent of each document's version.
pub const CODEMODE_MODULE_VERSION: u32 = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolDeclaration {
    pub name: String,
    pub js_name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub structured: bool,
}
impl From<&ToolDeclaration> for DeclaredTool {
    fn from(t: &ToolDeclaration) -> Self {
        Self {
            name: t.name.clone(),
            js_name: Some(t.js_name.clone()),
            description: t.description.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NamespaceDeclaration {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    pub tools: Vec<String>,
}
/// Immutable deterministic input, except the fingerprint recorded at Started.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeModeStart {
    pub version: u32,
    pub code: String,
    pub started_at: f64,
    pub seed: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<f64>,
    pub tools: Vec<ToolDeclaration>,
    pub namespaces: Vec<NamespaceDeclaration>,
    pub globals: Vec<DeclaredGlobal>,
    pub max_output_tokens: usize,
    pub store: BTreeMap<String, Value>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeModeRunState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<CodeModeStart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ToolExecutionResult>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GlobalResult {
    /// Absent means a normal global; Tool marks adapter-only raw argument validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<pi_codemode::CallTarget>,
    pub name: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub args: Option<Value>,
    pub outcome: Outcome,
}
fn present_value<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JournalEntry {
    pub id: u64,
    pub at: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued_after: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global: Option<GlobalResult>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeModeJournalState {
    pub entries: Vec<JournalEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_issued: Option<u64>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeModeStoreState {
    pub values: BTreeMap<String, Value>,
}

pub fn codemode_run_doc() -> DocDefinition {
    DocDefinition::new(
        "pi.codemode.run",
        1,
        DocumentSemantics::Task,
        false,
        |_| Ok(json!({})),
        |_, _, _| Ok(false),
    )
    .expect("CodeMode run definition")
}
pub fn codemode_journal_doc() -> DocDefinition {
    DocDefinition::new(
        "pi.codemode.journal",
        1,
        DocumentSemantics::Task,
        false,
        |_| Ok(json!({"entries":[]})),
        |_, _, _| Ok(false),
    )
    .expect("CodeMode journal definition")
}
pub fn codemode_store_doc() -> DocDefinition {
    DocDefinition::new(
        "pi.codemode.store",
        1,
        DocumentSemantics::Conversation {
            history: History::Rewindable,
            fork: ForkPolicy::AsOf,
        },
        false,
        |_| Ok(json!({"values":{}})),
        |_, _, info| Ok(info.deltas_since_base >= 64),
    )
    .expect("CodeMode store definition")
}
// Helpers preserve native emitted operations instead of substituting document values.
pub(super) fn read<T: serde::de::DeserializeOwned>(
    tx: &mut Transaction<'_>,
    def: &DocDefinition,
    scope: Scope,
) -> Result<(DocumentId, T)> {
    let id = tx.doc(def, def.address(scope, None)?, Value::Null)?;
    Ok((id, serde_json::from_value(tx.document_value(id)?.clone())?))
}
pub(super) fn field(
    tx: &mut Transaction<'_>,
    id: DocumentId,
    name: &str,
    value: impl Serialize,
) -> Result<()> {
    tx.apply(id, Op::Set(vec![name.into()], serde_json::to_value(value)?))
}
