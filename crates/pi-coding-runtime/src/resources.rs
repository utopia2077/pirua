//! Detached prompt resource intake and the pinned coding skill formatter.
//!
//! Hosts authorize local roots; this module discovers and detaches resources with
//! release precedence, collision diagnostics and canonical skill deduplication.

fn deserialize_optional_non_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    let value = Option::<T>::deserialize(deserializer)?;
    value
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("optional resource field must not be null"))
}

/// Detached skill content and its host-selected location.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub content: String,
    pub file_path: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub disable_model_invocation: Option<bool>,
}
/// Named prompt expansion supplied by the resource loader.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PromptTemplate {
    pub name: String,
    pub content: String,
    pub file_path: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub argument_hint: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
}
/// A detached resource snapshot owned by Coding rather than a runtime scheduler.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Resources {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub prompt_templates: Option<Vec<PromptTemplate>>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub skills: Option<Vec<Skill>>,
}
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
/// pi-mono `4a080252f06caf63a525d7873c5d94d3c23887e0`.
pub fn format_skills_for_prompt(skills: &[Skill]) -> String {
    format_skills_for_prompt_with_reader(skills, SkillFileReader::Read)
}

/// How a skill file can be reached through the active loadout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkillFileReader {
    Read,
    Bash,
    Indirect,
}

/// Formats skill hints for a direct or hidden read capability.
pub fn format_skills_for_prompt_with_reader(skills: &[Skill], reader: SkillFileReader) -> String {
    let visible: Vec<_> = skills
        .iter()
        .filter(|skill| skill.disable_model_invocation != Some(true))
        .collect();
    if visible.is_empty() {
        return String::new();
    }
    let mut lines = vec![
        "\n\nThe following skills provide specialized instructions for specific tasks.".to_owned(),
        match reader {
            SkillFileReader::Read => "Use the read tool to load a skill's file when the task matches its description.",
            SkillFileReader::Bash => "Use bash to load a skill's file when the task matches its description.",
            SkillFileReader::Indirect => "Load a skill's file when the task matches its description.",
        }.to_owned(),
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

/// A resource warning or name collision, including the source location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceDiagnostic {
    pub path: std::path::PathBuf,
    pub message: String,
    pub winner_path: Option<std::path::PathBuf>,
}
/// Explicit local resource roots; no package installation or executable loading occurs.
#[derive(Clone, Debug)]
pub struct ResourceLoaderOptions {
    pub cwd: std::path::PathBuf,
    pub agent_dir: std::path::PathBuf,
    pub skill_paths: Vec<std::path::PathBuf>,
    pub prompt_paths: Vec<std::path::PathBuf>,
    pub include_defaults: bool,
}
#[derive(Clone, Debug, Default)]
pub struct LoadedResources {
    pub resources: Resources,
    pub context_files: Vec<ContextFile>,
    pub diagnostics: Vec<ResourceDiagnostic>,
}

/// Load global resources first, then project resources, then explicit roots.
/// Skills are deduplicated by canonical path and name, with first declaration winning.
pub fn load_resources(options: &ResourceLoaderOptions) -> LoadedResources {
    use std::collections::{BTreeMap, BTreeSet};
    let process_cwd = std::env::current_dir().unwrap_or_default();
    let cwd = absolute_resource_path(&options.cwd, &process_cwd);
    let agent_dir = absolute_resource_path(&options.agent_dir, &process_cwd);
    let mut loaded = LoadedResources {
        context_files: load_project_context_files(&cwd, &agent_dir),
        ..Default::default()
    };
    let mut skill_roots = Vec::new();
    let mut prompt_roots = Vec::new();
    if options.include_defaults {
        skill_roots.extend([agent_dir.join("skills"), cwd.join(".pi/skills")]);
        prompt_roots.extend([agent_dir.join("prompts"), cwd.join(".pi/prompts")]);
    }
    skill_roots.extend(
        options
            .skill_paths
            .iter()
            .map(|path| absolute_resource_path(path, &cwd)),
    );
    prompt_roots.extend(
        options
            .prompt_paths
            .iter()
            .map(|path| absolute_resource_path(path, &cwd)),
    );
    let mut skills = Vec::new();
    let mut seen_paths = BTreeSet::new();
    let mut names = BTreeMap::<String, std::path::PathBuf>::new();
    for root in skill_roots {
        let mut candidates = Vec::new();
        if root.is_file() {
            candidates.push(root);
        } else {
            discover_skills(
                &root,
                &root,
                true,
                &mut Vec::new(),
                &mut BTreeSet::new(),
                &mut candidates,
            );
        }
        for path in candidates {
            let canonical = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
            if seen_paths.contains(&canonical) {
                continue;
            }
            let Some(skill) = load_skill(&path, &mut loaded.diagnostics) else {
                continue;
            };
            if let Some(winner) = names.get(&skill.name) {
                loaded.diagnostics.push(ResourceDiagnostic {
                    path,
                    message: format!("name \"{}\" collision", skill.name),
                    winner_path: Some(winner.clone()),
                });
            } else {
                names.insert(skill.name.clone(), path);
                seen_paths.insert(canonical);
                skills.push(skill);
            }
        }
    }
    let mut prompts = Vec::new();
    let mut prompt_names = BTreeMap::<String, std::path::PathBuf>::new();
    for root in prompt_roots {
        let candidates = if root.is_file() {
            vec![root]
        } else {
            sorted_children(&root)
                .into_iter()
                .filter(|path| path.is_file())
                .collect()
        };
        for path in candidates
            .into_iter()
            .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        {
            let result = (|| {
                let raw = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
                let (frontmatter, body) = parse_frontmatter(&raw)?;
                let name = path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let description = frontmatter
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        let line = body
                            .lines()
                            .find(|line| !line.trim().is_empty())
                            .unwrap_or("");
                        let mut text: String = line.chars().take(60).collect();
                        if line.chars().count() > 60 {
                            text.push_str("...");
                        }
                        text
                    });
                Ok::<_, String>(PromptTemplate {
                    name,
                    content: body,
                    file_path: path.to_string_lossy().into_owned(),
                    argument_hint: frontmatter
                        .get("argument-hint")
                        .and_then(serde_json::Value::as_str)
                        .filter(|hint| !hint.is_empty())
                        .map(str::to_owned),
                    description: Some(description),
                })
            })();
            match result {
                Ok(prompt) => {
                    if let Some(winner) = prompt_names.get(&prompt.name) {
                        loaded.diagnostics.push(ResourceDiagnostic {
                            path,
                            message: format!("name \"{}\" collision", prompt.name),
                            winner_path: Some(winner.clone()),
                        });
                    } else {
                        prompt_names.insert(prompt.name.clone(), path);
                        prompts.push(prompt);
                    }
                }
                Err(message) => loaded.diagnostics.push(ResourceDiagnostic {
                    path,
                    message,
                    winner_path: None,
                }),
            }
        }
    }
    loaded.resources.skills = Some(skills);
    loaded.resources.prompt_templates = Some(prompts);
    loaded
}
fn absolute_resource_path(path: &std::path::Path, cwd: &std::path::Path) -> std::path::PathBuf {
    crate::tools::absolute_local_path(path, cwd).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_owned()
        } else {
            cwd.join(path)
        }
    })
}
fn sorted_children(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut paths: Vec<_> = std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    paths
}
fn discover_skills(
    dir: &std::path::Path,
    root: &std::path::Path,
    root_files: bool,
    rules: &mut Vec<(String, String)>,
    visited: &mut std::collections::BTreeSet<std::path::PathBuf>,
    paths: &mut Vec<std::path::PathBuf>,
) {
    let canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_owned());
    if !visited.insert(canonical) {
        return;
    }
    let inherited = rules.len();
    for file in [".gitignore", ".ignore", ".fdignore"] {
        if let Ok(content) = std::fs::read_to_string(dir.join(file)) {
            rules.extend(
                content
                    .lines()
                    .map(|line| (dir.to_string_lossy().into_owned(), line.to_owned())),
            );
        }
    }
    let skill = dir.join("SKILL.md");
    if skill.is_file() && !crate::tools::resource_path_ignored(&skill, rules) {
        paths.push(skill);
        rules.truncate(inherited);
        return;
    }
    for path in sorted_children(dir) {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with('.')
            || name == "node_modules"
            || crate::tools::resource_path_ignored(&path, rules)
        {
            continue;
        }
        if path.is_dir() {
            discover_skills(&path, root, false, rules, visited, paths);
        } else if root_files && path.is_file() && path.extension().is_some_and(|ext| ext == "md") {
            paths.push(path);
        }
    }
    let _ = root;
    rules.truncate(inherited);
}
fn load_skill(path: &std::path::Path, diagnostics: &mut Vec<ResourceDiagnostic>) -> Option<Skill> {
    let declared = path.file_name().is_some_and(|name| name == "SKILL.md");
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) => {
            diagnostics.push(ResourceDiagnostic {
                path: path.into(),
                message: error.to_string(),
                winner_path: None,
            });
            return None;
        }
    };
    let (frontmatter, _) = match parse_frontmatter(&raw) {
        Ok(parsed) => parsed,
        Err(message) => {
            if declared {
                diagnostics.push(ResourceDiagnostic {
                    path: path.into(),
                    message,
                    winner_path: None,
                });
            }
            return None;
        }
    };
    let description = frontmatter
        .get("description")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !declared && description.trim().is_empty() {
        return None;
    }
    let fallback = path
        .parent()
        .and_then(std::path::Path::file_name)
        .unwrap_or_default()
        .to_string_lossy();
    let name = frontmatter
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or(&fallback);
    let mut warnings = Vec::new();
    if description.trim().is_empty() {
        warnings.push("description is required");
    }
    if description.chars().count() > 1024 {
        warnings.push("description exceeds 1024 characters");
    }
    if name.chars().count() > 64 {
        warnings.push("name exceeds 64 characters");
    }
    if name.is_empty()
        || !name
            .bytes()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'-')
    {
        warnings
            .push("name contains invalid characters (must be lowercase a-z, 0-9, hyphens only)");
    }
    if name.starts_with('-') || name.ends_with('-') {
        warnings.push("name must not start or end with a hyphen");
    }
    if name.contains("--") {
        warnings.push("name must not contain consecutive hyphens");
    }
    diagnostics.extend(warnings.into_iter().map(|message| ResourceDiagnostic {
        path: path.into(),
        message: message.into(),
        winner_path: None,
    }));
    if description.trim().is_empty() {
        return None;
    }
    Some(Skill {
        name: name.into(),
        description: description.into(),
        content: raw,
        file_path: path.to_string_lossy().into_owned(),
        disable_model_invocation: Some(
            frontmatter.get("disable-model-invocation") == Some(&serde_json::Value::Bool(true)),
        ),
    })
}

/// Parse the release's BOM/newline-normalized YAML frontmatter and trimmed body.
pub fn parse_frontmatter(content: &str) -> Result<(serde_json::Value, String), String> {
    let normalized = content
        .strip_prefix('\u{feff}')
        .unwrap_or(content)
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    if normalized.starts_with("---")
        && let Some(end) = normalized[3..].find("\n---").map(|index| index + 3)
    {
        let header = &normalized[4.min(end)..end];
        let value = if header.is_empty() {
            serde_json::json!({})
        } else {
            yaml_serde::from_str::<serde_json::Value>(header).map_err(|error| error.to_string())?
        };
        return Ok((
            if value.is_null() {
                serde_json::json!({})
            } else {
                value
            },
            normalized[end + 4..].trim().into(),
        ));
    }
    Ok((serde_json::json!({}), normalized))
}

/// Global instructions followed by root-to-cwd inheritance, one candidate per directory.
pub fn load_project_context_files(
    cwd: &std::path::Path,
    agent_dir: &std::path::Path,
) -> Vec<ContextFile> {
    let process_cwd = std::env::current_dir().unwrap_or_default();
    let resolved_cwd = absolute_resource_path(cwd, &process_cwd);
    let resolved_agent_dir = absolute_resource_path(agent_dir, &process_cwd);
    let cwd = resolved_cwd.as_path();
    let agent_dir = resolved_agent_dir.as_path();
    let mut files = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let context = |dir: &std::path::Path| {
        [
            "AGENTS.override.md",
            "AGENTS.md",
            "AGENTS.MD",
            "CLAUDE.md",
            "CLAUDE.MD",
        ]
        .into_iter()
        .find_map(|name| {
            let path = dir.join(name);
            if !path.is_file() {
                return None;
            }
            let content = std::fs::read_to_string(&path).ok()?;
            Some(ContextFile {
                path: path.to_string_lossy().into_owned(),
                content: content.strip_prefix('\u{feff}').unwrap_or(&content).into(),
            })
        })
    };
    if let Some(global) = context(agent_dir) {
        seen.insert(global.path.clone());
        files.push(global);
    }
    let mut ancestors: Vec<_> = cwd.ancestors().collect();
    ancestors.reverse();
    let shadowed = shadowed_context_path(cwd, &context);
    for dir in ancestors {
        if let Some(file) = context(dir) {
            let canonical_dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_owned());
            let candidate = canonical_dir.join(
                std::path::Path::new(&file.path)
                    .file_name()
                    .unwrap_or_default(),
            );
            if shadowed.as_ref() == Some(&candidate) {
                continue;
            }
            if seen.insert(file.path.clone()) {
                files.push(file);
            }
        }
    }
    files
}
fn shadowed_context_path(
    cwd: &std::path::Path,
    context: &impl Fn(&std::path::Path) -> Option<ContextFile>,
) -> Option<std::path::PathBuf> {
    for dir in cwd.ancestors() {
        let git = dir.join(".git");
        if !git.exists() {
            continue;
        }
        if !git.is_file() {
            return None;
        }
        let content = std::fs::read_to_string(git).ok()?;
        let gitdir = absolute_resource_path(
            std::path::Path::new(content.trim().strip_prefix("gitdir:")?.trim()),
            dir,
        );
        let common = std::fs::read_to_string(gitdir.join("commondir")).ok()?;
        let common = std::fs::canonicalize(absolute_resource_path(
            std::path::Path::new(common.trim()),
            &gitdir,
        ))
        .ok()?;
        let main = common.parent()?;
        let worktree = std::fs::canonicalize(dir).ok()?;
        if worktree == main
            || !worktree.starts_with(main)
            || std::fs::canonicalize(main.join(".git")).ok()? != common
        {
            return None;
        }
        let own = context(dir)?;
        return Some(main.join(std::path::Path::new(&own.path).file_name()?));
    }
    None
}

/// Shell-like argument splitting retains quoted empty arguments; values are never recursively expanded.
pub fn parse_command_args(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut token = false;
    for ch in input.chars() {
        if let Some(expected) = quote {
            if ch == expected {
                quote = None;
            } else {
                current.push(ch);
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
            token = true;
        } else if ch.is_whitespace() {
            if token {
                args.push(std::mem::take(&mut current));
                token = false;
            }
        } else {
            current.push(ch);
            token = true;
        }
    }
    if token {
        args.push(current);
    }
    args
}
pub fn substitute_args(content: &str, args: &[String]) -> String {
    let re = regex::Regex::new(
        r"\$\{(\d+|ARGUMENTS|@):-([^}]*)\}|\$\{@:(\d+)(?::(\d+))?\}|\$(ARGUMENTS|@|\d+)",
    )
    .expect("static argument pattern");
    let all = args.join(" ");
    re.replace_all(content, |captures: &regex::Captures<'_>| {
        let get = |target: &str| {
            if target == "@" || target == "ARGUMENTS" {
                all.clone()
            } else {
                target
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| index.checked_sub(1))
                    .and_then(|index| args.get(index))
                    .cloned()
                    .unwrap_or_default()
            }
        };
        if let Some(target) = captures.get(1) {
            let value = get(target.as_str());
            if value.is_empty() {
                captures[2].into()
            } else {
                value
            }
        } else if let Some(start) = captures.get(3) {
            let start = start
                .as_str()
                .parse::<usize>()
                .unwrap_or(1)
                .saturating_sub(1)
                .min(args.len());
            let count = captures
                .get(4)
                .and_then(|value| value.as_str().parse::<usize>().ok())
                .unwrap_or(args.len());
            args.iter()
                .skip(start)
                .take(count)
                .cloned()
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            get(&captures[5])
        }
    })
    .into_owned()
}
pub fn expand_prompt_template(text: &str, templates: &[PromptTemplate]) -> String {
    let Some(command) = text.strip_prefix('/') else {
        return text.into();
    };
    let (name, arguments) = command
        .split_once(char::is_whitespace)
        .unwrap_or((command, ""));
    templates
        .iter()
        .find(|template| template.name == name)
        .map_or_else(
            || text.into(),
            |template| substitute_args(&template.content, &parse_command_args(arguments)),
        )
}
