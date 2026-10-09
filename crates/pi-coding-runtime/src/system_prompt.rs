//! Pure coding system-prompt construction from detached host inputs.
//!
//! Translated from coding-agent/core/system-prompt.ts and
//! coding-agent/server/create-harness.ts at pi-mono
//! `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.

use crate::resources::{ContextFile, Resources, format_skills_for_prompt};
use std::collections::{BTreeMap, BTreeSet};

/// Documentation locations supplied by the host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentationPaths {
    /// Main documentation path.
    pub readme: String,
    /// Additional documentation directory.
    pub docs: String,
    /// Examples directory.
    pub examples: String,
}

/// Detached inputs to the coding system-prompt builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildSystemPromptOptions {
    /// Current working directory, formatted with forward slashes in the prompt.
    pub cwd: String,
    /// Host-supplied documentation locations; these are never discovered.
    pub documentation_paths: DocumentationPaths,
    /// Nonempty custom prompt replacing the default prompt.
    pub custom_prompt: Option<String>,
    /// Selected tools in display order; omission selects read/bash/edit/write.
    pub selected_tools: Option<Vec<String>>,
    /// One-line snippets keyed by tool name.
    pub tool_snippets: BTreeMap<String, String>,
    /// Additional guidelines, trimmed and deduplicated in source order.
    pub prompt_guidelines: Vec<String>,
    /// Nonempty text appended before project context and skills.
    pub append_system_prompt: Option<String>,
    /// Project instructions loaded by the host, in host-provided order.
    pub context_files: Vec<ContextFile>,
    /// Detached Harness resources; prompt templates are retained for callers.
    pub resources: Resources,
}

impl BuildSystemPromptOptions {
    /// Creates options with source defaults and explicit host locations.
    pub fn new(cwd: impl Into<String>, documentation_paths: DocumentationPaths) -> Self {
        Self {
            cwd: cwd.into(),
            documentation_paths,
            custom_prompt: None,
            selected_tools: None,
            tool_snippets: BTreeMap::new(),
            prompt_guidelines: Vec::new(),
            append_system_prompt: None,
            context_files: Vec::new(),
            resources: Resources::default(),
        }
    }
}

/// Prompt-only metadata supplied alongside an executable tool by composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolPromptMetadata {
    /// Tool name used for exact active-tool lookup.
    pub name: String,
    /// Optional snippet normalized to one line by the Harness prompt adapter.
    pub prompt_snippet: Option<String>,
    /// Tool guideline contributions in their supplied order.
    pub prompt_guidelines: Vec<String>,
}

/// Builds a system prompt using the first matching metadata for each active name.
///
/// Unknown active names are omitted. Active order and duplicates follow the
/// source; snippets use the last contribution for a repeated name, while
/// guidelines are later deduplicated by the core prompt builder.
pub fn build_coding_agent_harness_system_prompt(
    options: &BuildSystemPromptOptions,
    tools: &[ToolPromptMetadata],
    active_tool_names: &[String],
) -> String {
    let active_tools: Vec<_> = active_tool_names
        .iter()
        .filter_map(|name| tools.iter().find(|tool| tool.name == *name))
        .collect();
    let mut selected = options.clone();
    selected.selected_tools = Some(active_tools.iter().map(|tool| tool.name.clone()).collect());
    selected.tool_snippets = active_tools
        .iter()
        .filter_map(|tool| {
            let snippet = normalize_snippet(tool.prompt_snippet.as_deref()?);
            (!snippet.is_empty()).then(|| (tool.name.clone(), snippet))
        })
        .collect();
    selected.prompt_guidelines = active_tools
        .iter()
        .flat_map(|tool| tool.prompt_guidelines.iter().cloned())
        .collect();
    build_system_prompt(&selected)
}

/// Builds a deterministic prompt without callbacks or filesystem access.
pub fn build_system_prompt(options: &BuildSystemPromptOptions) -> String {
    let default_tools = vec!["read".into(), "bash".into(), "edit".into(), "write".into()];
    let tools = options.selected_tools.as_ref().unwrap_or(&default_tools);
    let has = |name: &str| tools.iter().any(|tool| tool == name);
    let custom = options
        .custom_prompt
        .as_deref()
        .filter(|text| !text.is_empty());
    let mut prompt = if let Some(custom) = custom {
        custom.to_owned()
    } else {
        let visible: Vec<_> = tools
            .iter()
            .filter_map(|name| {
                options
                    .tool_snippets
                    .get(name)
                    .filter(|snippet| !snippet.is_empty())
                    .map(|snippet| format!("- {name}: {snippet}"))
            })
            .collect();
        let tools_list = if visible.is_empty() {
            "(none)".to_owned()
        } else {
            visible.join("\n")
        };
        let mut guidelines = Vec::new();
        let mut seen = BTreeSet::new();
        let mut add = |guideline: &str| {
            if seen.insert(guideline.to_owned()) {
                guidelines.push(format!("- {guideline}"));
            }
        };
        if (has("bash") || has("powershell")) && !has("grep") && !has("find") && !has("ls") {
            add(if has("bash") && has("powershell") {
                "Use bash or PowerShell for file operations like listing, searching, and finding files"
            } else if has("powershell") {
                "Use PowerShell for file operations like listing, searching, and finding files"
            } else {
                "Use bash for file operations like ls, rg, find"
            });
        }
        for guideline in &options.prompt_guidelines {
            let normalized = guideline.trim_matches(is_ecmascript_whitespace);
            if !normalized.is_empty() {
                add(normalized);
            }
        }
        add("Be concise in your responses");
        add("Show file paths clearly when working with files");
        default_prompt(
            &tools_list,
            &guidelines.join("\n"),
            &options.documentation_paths,
        )
    };
    if let Some(append) = options
        .append_system_prompt
        .as_deref()
        .filter(|text| !text.is_empty())
    {
        prompt.push_str("\n\n");
        prompt.push_str(append);
    }
    if !options.context_files.is_empty() {
        prompt
            .push_str("\n\n<project_context>\n\nProject-specific instructions and guidelines:\n\n");
        for file in &options.context_files {
            prompt.push_str(&format!(
                "<project_instructions path=\"{}\">\n{}\n</project_instructions>\n\n",
                file.path, file.content
            ));
        }
        prompt.push_str("</project_context>\n");
    }
    if has("read") {
        prompt.push_str(&format_skills_for_prompt(
            options.resources.skills.as_deref().unwrap_or(&[]),
        ));
    }
    prompt.push_str(&format!(
        "\nCurrent working directory: {}",
        options.cwd.replace('\\', "/")
    ));
    if custom.is_some() {
        prompt.push('\n');
    }
    prompt
}

fn normalize_snippet(snippet: &str) -> String {
    snippet
        .split(is_ecmascript_whitespace)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

fn default_prompt(tools: &str, guidelines: &str, docs: &DocumentationPaths) -> String {
    format!("You are an expert coding assistant operating inside pi, a coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.

Available tools:
{tools}

In addition to the tools above, you may have access to other custom tools depending on the project.

Guidelines:
{guidelines}

Pi documentation (read only when the user asks about pi itself, its SDK, extensions, themes, skills, or TUI):
- Main documentation: {readme}
- Additional docs: {additional}
- Examples: {examples} (extensions, custom tools, SDK)
- When reading pi docs or examples, resolve docs/... under Additional docs and examples/... under Examples, not the current working directory
- When asked about: extensions (docs/extensions.md, examples/extensions/), themes (docs/themes.md), skills (docs/skills.md), prompt templates (docs/prompt-templates.md), TUI components (docs/tui.md), keybindings (docs/keybindings.md), SDK integrations (docs/sdk.md), custom providers (docs/custom-provider.md), adding models (docs/models.md), pi packages (docs/packages.md), environment variables (docs/environment-variables.md)
- When working on pi topics, read the docs and examples, and follow .md cross-references before implementing
- Always read pi .md files completely and follow links to related docs (e.g., tui.md for TUI API details)", readme=docs.readme, additional=docs.docs, examples=docs.examples)
}
