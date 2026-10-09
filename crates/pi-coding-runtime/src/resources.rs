//! Detached prompt resource intake and the pinned coding skill formatter.
//!
//! Resource discovery, path canonicalization, trust, collisions and precedence
//! are host responsibilities. Input order, text and paths are retained exactly.

pub use pi_agent::schema::{PromptTemplate, Resources, Skill};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Project instructions already loaded by the host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextFile {
    /// Display path supplied by the host; no filesystem access is performed.
    pub path: String,
    /// Complete file content supplied by the host.
    pub content: String,
}

/// Detached values failed validation against the existing resource schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceNormalizationError;

impl fmt::Display for ResourceNormalizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("coding resources schema validation failed")
    }
}

impl std::error::Error for ResourceNormalizationError {}

/// Validates and detaches a resource snapshot through the existing schema.
///
/// Missing collections and explicitly empty collections remain distinct. No
/// sorting, text rewriting, discovery or precedence decisions are performed.
pub fn normalize_resources(resources: &Resources) -> Result<Resources, ResourceNormalizationError> {
    let bytes = serde_json::to_vec(resources).map_err(|_| ResourceNormalizationError)?;
    serde_json::from_slice(&bytes).map_err(|_| ResourceNormalizationError)
}

/// Formats model-invocable skills exactly as coding-agent/core/skills.ts at
/// pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
pub fn format_skills_for_prompt(skills: &[Skill]) -> String {
    let visible: Vec<_> = skills
        .iter()
        .filter(|skill| skill.disable_model_invocation != Some(true))
        .collect();
    if visible.is_empty() {
        return String::new();
    }
    let mut lines = vec![
        "\n\nThe following skills provide specialized instructions for specific tasks.".to_owned(),
        "Use the read tool to load a skill's file when the task matches its description.".to_owned(),
        "When a skill file references a relative path, resolve it against the skill directory (parent of SKILL.md / dirname of the path) and use that absolute path in tool commands.".to_owned(),
        String::new(),
        "<available_skills>".to_owned(),
    ];
    for skill in visible {
        lines.push("  <skill>".to_owned());
        lines.push(format!("    <name>{}</name>", escape_xml(&skill.name)));
        lines.push(format!(
            "    <description>{}</description>",
            escape_xml(&skill.description)
        ));
        lines.push(format!(
            "    <location>{}</location>",
            escape_xml(&skill.file_path)
        ));
        lines.push("  </skill>".to_owned());
    }
    lines.push("</available_skills>".to_owned());
    lines.join("\n")
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
