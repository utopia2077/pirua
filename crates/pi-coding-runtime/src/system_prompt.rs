//! Structured, independently replaceable system prompt sections from Pi v1.4.0.
use crate::resources::{
    ContextFile, Resources, SkillFileReader, format_skills_for_prompt_with_reader,
};
use pi_ai::{SystemContent, SystemSections};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentationPaths {
    pub readme: String,
    pub docs: String,
    pub examples: String,
}

/// Host-owned values. Section pairs retain host insertion order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildSystemPromptOptions {
    pub cwd: String,
    pub documentation_paths: DocumentationPaths,
    pub custom_prompt: Option<String>,
    pub force_system_prompt: Option<String>,
    pub selected_tools: Option<Vec<String>>,
    pub hidden_tools: Vec<String>,
    pub tool_snippets: BTreeMap<String, String>,
    pub tool_guidelines: BTreeMap<String, Vec<String>>,
    pub prompt_guidelines: Vec<String>,
    pub append_system_prompt: Option<String>,
    pub sections: Vec<(String, String)>,
    pub context_files: Vec<ContextFile>,
    pub resources: Resources,
}
impl BuildSystemPromptOptions {
    pub fn new(cwd: impl Into<String>, documentation_paths: DocumentationPaths) -> Self {
        Self {
            cwd: cwd.into(),
            documentation_paths,
            custom_prompt: None,
            force_system_prompt: None,
            selected_tools: None,
            hidden_tools: Vec::new(),
            tool_snippets: BTreeMap::new(),
            tool_guidelines: BTreeMap::new(),
            prompt_guidelines: Vec::new(),
            append_system_prompt: None,
            sections: Vec::new(),
            context_files: Vec::new(),
            resources: Resources::default(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolPromptMetadata {
    pub name: String,
    pub prompt_snippet: Option<String>,
    pub prompt_guidelines: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemPromptError(pub String);
impl std::fmt::Display for SystemPromptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SystemPromptError {}

/// Prompt state can be inserted directly into an AI SystemMessage.
#[derive(Clone, Debug, PartialEq)]
pub struct SystemPromptState {
    pub content: SystemContent,
    pub sections: Option<SystemSections>,
}

pub fn build_coding_agent_system_prompt(
    options: &BuildSystemPromptOptions,
    tools: &[ToolPromptMetadata],
    active_tool_names: &[String],
) -> Result<String, SystemPromptError> {
    let active: Vec<_> = active_tool_names
        .iter()
        .filter_map(|name| tools.iter().find(|tool| tool.name == *name))
        .collect();
    let mut options = options.clone();
    options.selected_tools = Some(active.iter().map(|tool| tool.name.clone()).collect());
    options.tool_snippets = active
        .iter()
        .filter_map(|tool| {
            let snippet = tool
                .prompt_snippet
                .as_deref()?
                .split(is_ecmascript_whitespace)
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            (!snippet.is_empty()).then(|| (tool.name.clone(), snippet))
        })
        .collect();
    options.tool_guidelines = active
        .iter()
        .map(|tool| (tool.name.clone(), tool.prompt_guidelines.clone()))
        .collect();
    build_system_prompt(&options)
}

pub fn build_system_prompt_state(
    options: &BuildSystemPromptOptions,
) -> Result<SystemPromptState, SystemPromptError> {
    if let Some(forced) = &options.force_system_prompt {
        return Ok(SystemPromptState {
            content: SystemContent::Text(forced.clone()),
            sections: None,
        });
    }
    Ok(SystemPromptState {
        content: SystemContent::Text(String::new()),
        sections: Some(build_system_prompt_sections(options)?),
    })
}

pub fn build_system_prompt_sections(
    options: &BuildSystemPromptOptions,
) -> Result<SystemSections, SystemPromptError> {
    for (name, _) in &options.sections {
        if name == "preamble" || !valid_section_name(name) {
            return Err(SystemPromptError(format!(
                "Invalid system prompt section name: {name}"
            )));
        }
    }
    let defaults = vec!["read".into(), "bash".into(), "edit".into(), "write".into()];
    let selected = options.selected_tools.as_ref().unwrap_or(&defaults);
    let declared: Vec<_> = selected
        .iter()
        .filter(|name| !options.hidden_tools.contains(name))
        .collect();
    let has = |name: &str| declared.iter().any(|tool| tool.as_str() == name);
    let mut sections: Vec<(String, Option<String>)> = Vec::new();
    let mut put = |name: &str, content: String| {
        if let Some((_, previous)) = sections.iter_mut().find(|(key, _)| key == name) {
            *previous = Some(content);
        } else {
            sections.push((name.into(), Some(content)));
        }
    };
    if let Some(custom) = options
        .custom_prompt
        .as_ref()
        .filter(|text| !text.is_empty())
    {
        put("preamble", custom.clone());
    } else {
        put("preamble", "You are an expert coding assistant operating inside pi, a coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.".into());
        let visible: Vec<_> = declared
            .iter()
            .filter_map(|name| {
                options
                    .tool_snippets
                    .get(*name)
                    .filter(|snippet| !snippet.is_empty())
                    .map(|snippet| format!("- {name}: {snippet}"))
            })
            .collect();
        put(
            "tools",
            format!(
                "{}\n\nIn addition to the tools above, you may have access to other custom tools depending on the project.",
                if visible.is_empty() {
                    "(none)".into()
                } else {
                    visible.join("\n")
                }
            ),
        );
        let mut rules = Vec::new();
        let mut seen = BTreeSet::new();
        let mut add = |rule: &str| {
            let rule = rule.trim_matches(is_ecmascript_whitespace);
            if !rule.is_empty() && seen.insert(rule.to_owned()) {
                rules.push(format!("- {rule}"));
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
        for name in &declared {
            for rule in options.tool_guidelines.get(*name).into_iter().flatten() {
                add(rule);
            }
        }
        for rule in &options.prompt_guidelines {
            add(rule);
        }
        add("Be concise in your responses");
        add("Show file paths clearly when working with files");
        put("rules", rules.join("\n"));
        put("docs", documentation(&options.documentation_paths));
    }
    if let Some(append) = options
        .append_system_prompt
        .as_ref()
        .filter(|text| !text.is_empty())
    {
        put("addendum", append.clone());
    }
    if !options.context_files.is_empty() {
        put(
            "project_context",
            std::iter::once("Project-specific instructions and guidelines:".into())
                .chain(options.context_files.iter().map(|file| {
                    format!(
                        "<project_instructions path=\"{}\">\n{}\n</project_instructions>",
                        file.path, file.content
                    )
                }))
                .collect::<Vec<_>>()
                .join("\n\n"),
        );
    }
    let reader = if has("read") {
        Some(SkillFileReader::Read)
    } else if has("bash") {
        Some(SkillFileReader::Bash)
    } else if selected.iter().any(|name| name == "read" || name == "bash") {
        Some(SkillFileReader::Indirect)
    } else {
        None
    };
    if let Some(reader) = reader {
        let skills = format_skills_for_prompt_with_reader(
            options.resources.skills.as_deref().unwrap_or(&[]),
            reader,
        );
        if !skills.trim_matches(is_ecmascript_whitespace).is_empty() {
            put(
                "skills",
                skills.trim_matches(is_ecmascript_whitespace).into(),
            );
        }
    }
    put("cwd", options.cwd.replace('\\', "/"));
    for (name, content) in &options.sections {
        if !content.is_empty() {
            put(name, content.clone());
        }
    }
    for (name, text) in &mut sections {
        if name != "preamble" {
            *text = text
                .take()
                .map(|content| format!("<{name}>\n{content}\n</{name}>"));
        }
    }
    Ok(SystemSections::new(sections))
}

pub fn build_system_prompt(
    options: &BuildSystemPromptOptions,
) -> Result<String, SystemPromptError> {
    let state = build_system_prompt_state(options)?;
    if let Some(sections) = state.sections {
        Ok(sections
            .iter()
            .filter_map(|(_, text)| text.as_deref())
            .collect::<Vec<_>>()
            .join("\n\n"))
    } else if let SystemContent::Text(text) = state.content {
        Ok(text)
    } else {
        unreachable!("prompt builders produce text content")
    }
}

pub fn diff_system_prompt_sections(
    previous: &SystemSections,
    current: &SystemSections,
) -> Option<SystemSections> {
    let mut changes = Vec::new();
    for (name, text) in current.iter() {
        if previous
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
            != Some(text)
        {
            changes.push((name.clone(), text.clone()));
        }
    }
    for (name, _) in previous.iter() {
        if !current.iter().any(|(key, _)| key == name) {
            changes.push((name.clone(), None));
        }
    }
    (!changes.is_empty()).then(|| SystemSections::new(changes))
}
fn valid_section_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|ch| ch.is_ascii_lowercase())
        && chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
}
fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
fn documentation(docs: &DocumentationPaths) -> String {
    format!(
        r#"Pi documentation (read only when the user asks about pi itself, its SDK, extensions, themes, skills, or TUI):
- Main documentation: {readme}
- Additional docs: {additional}
- Examples: {examples} (extensions, custom tools, SDK)
- When reading pi docs or examples, resolve docs/... under Additional docs and examples/... under Examples, not the current working directory
- When asked about: extensions (docs/extensions.md, examples/extensions/), themes (docs/themes.md), skills (docs/skills.md), prompt templates (docs/prompt-templates.md), TUI components (docs/tui.md), keybindings (docs/keybindings.md), SDK integrations (docs/sdk.md), custom providers (docs/custom-provider.md), adding models (docs/models.md), pi packages (docs/packages.md), environment variables (docs/environment-variables.md), MCP servers (docs/mcp.md), codemode scripts (docs/codemode.md)
- When working on pi topics, read the docs and examples, and follow .md cross-references before implementing
- Always read pi .md files completely and follow links to related docs (e.g., tui.md for TUI API details)"#,
        readme = docs.readme,
        additional = docs.docs,
        examples = docs.examples
    )
}
