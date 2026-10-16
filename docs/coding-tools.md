# Common coding capabilities

The `pi-coding-runtime` common modules target Pi `v1.4.0`
(`4a080252f06caf63a525d7873c5d94d3c23887e0`). They provide local coding tools,
resource discovery, prompt construction, settings snapshots and model composition.
Session lifecycle and runtime scheduling are separate owners.

`tools::CodingTool` is the reusable environment-operation interface. It accepts
JSON arguments, an AI cancellation token, a detached output callback and an
explicit `ExecutionToolContext`. Its `ToolOutput` contains model-facing content,
optional details and structured content, usage, error status and termination
status. It has no session handle or invocation identity. A Durable adapter can
invoke these operations directly.

`tools::bind_agent_tool(operation, context)` creates an `AgentTool` with the new
Agent callback interface. The convenience factories `create_read_tool(context)`,
`create_write_tool(context)`, `create_edit_tool(context)`,
`create_bash_tool(context)`, `create_powershell_tool(context)`,
`create_ls_tool(context)`, `create_find_tool(context)` and
`create_grep_tool(context)` all use this binding. `create_tools(context)` returns
all eight declarations in stable order. JSON-schema validation and runtime hooks
belong to the calling runtime; edit argument preparation accepts the forms that
the selected release normalizes before validation.

```rust,ignore
use std::sync::Arc;
use pi_coding_runtime::{ExecutionToolContext, LocalExecutionEnv, create_read_tool};

let context = ExecutionToolContext {
    env: Arc::new(LocalExecutionEnv::new("/workspace")),
};
let read = create_read_tool(context);
// Supply `read` to Agent, or call `tools::coding_tool("read")` from another adapter.
```

`LocalExecutionEnv` resolves relative paths, `@` prefixes, home paths, file URLs,
Unicode spaces and the read tool's macOS screenshot variants. Write and edit share
a queue keyed by the normalized real file path. Queue registration is ordered;
operations on different files can proceed together. Cancellation is checked after
queued admission and after filesystem operations settle. Callers must await the
operation's settlement when cancelling it, so an in-flight filesystem operation
keeps its queue ownership until it finishes.

Read returns text or a vision input attachment. Local image processing supports
PNG, JPEG, GIF, WebP and BMP, converts BMP to PNG, applies orientation during
processing, and resizes large images toward the release's 2000-pixel and 4.5 MiB
base64 limits. Encoded image bytes can differ from the upstream Photon encoder.
The host can replace `ExecutionEnv::process_image` or disable resizing through
`ReadToolOptions`. `ReadToolOptions::for_model` captures detached model resize
limits and image support; it reports a non-vision note while retaining image
content for the caller's request projection. Unusable images become a text omission
note. Image generation is excluded.

Edit matches all replacements against the original file, rejects duplicate or
overlapping matches, preserves BOM and the original newline style, and returns
numbered display changes, a unified patch and `firstChangedLine`. When fuzzy
matching is needed, only touched lines are written from the normalized view;
untouched lines preserve their original text.

Shell execution streams stdout and stderr through one output channel, keeps a
bounded display tail, and spills full output to a local temporary file when the
line or byte limit is exceeded. Structured shell output is bounded to 1 MiB and
reports the exit code, duration and full-output path when needed. Nonzero exit
codes return `is_error = Some(true)` with structured output. Cancellation and
invalid or elapsed deadlines return an execution error. The local process runner
terminates the shell process group on Unix and uses `taskkill /T` on Windows when
cancelled or timed out. After a successful shell exit, each output chunk renews a
100 ms grace period; the runner closes idle pipes while background work continues.
Native Windows and PowerShell execution require verification on those hosts.
`ShellToolOptions` exposes an explicit executable path, command prefix and
environment overrides. Headless hooks may also wrap the environment port.

`with_shell_identity(env, ShellIdentity)` supplies current `PI_SESSION_ID`,
`PI_SESSION_FILE`, `PI_PROVIDER`, `PI_MODEL` and `PI_REASONING_LEVEL` values from a
detached host snapshot. The wrapper overwrites stale entries while preserving
other process options. The local runner removes inherited PI identity values
before applying those explicit overrides.

Local search uses the Rust `ignore` walker and does not read ripgrep command-line
configuration. Find respects ignore files outside Git repositories and recognizes
nested repositories; grep applies Git ignore files when a Git repository exists.
Listings and searches have byte and explicit result limits, with no extra 2000-line
read limit. The `ExecutionEnv` port also supports an injected walker for other
hosts. Resource discovery is local and does not execute extensions or install
packages.

`resources::load_resources` loads global, project and explicit skill/prompt roots,
parses YAML frontmatter, diagnoses name collisions, and deduplicates canonical
skill paths. Local roots support home paths, file URLs and dot segments. Prompt
templates retain their absolute file path and optional argument hint. Project context uses the first available candidate in each directory:
`AGENTS.override.md`, `AGENTS.md`, `AGENTS.MD`, `CLAUDE.md`, `CLAUDE.MD`. Global
context precedes root-to-working-directory inheritance. A nested linked worktree's
own instructions shadow the equivalent main-worktree copy. Prompt arguments
support quoted empty values, positional/default/all-argument substitutions and
slices without recursive substitution. Hosts retain control of which project
settings and resource paths are authorized.

`system_prompt` builds ordered AI `SystemSections`. Hidden tools do not appear in
the tool list or rules. A selected hidden read/bash capability still permits skill
hints through an indirect reader. Custom sections replace sections in place;
`diff_system_prompt_sections` emits changed values and null removals. A supplied
`force_system_prompt`, including an empty string, is opaque and has no sections.
Invalid custom section names are reported as `SystemPromptError`.

`settings_snapshot::load_settings` reads global and optional host-authorized
project JSON documents, merges nested objects, replaces arrays/scalars and applies
runtime overrides. It returns source-aware diagnostics and detached values. It
does not persist settings, perform authentication or decide project trust.
Compaction, branch-summary, retry, image-input and queue defaults follow the
selected release's headless settings; terminal and package-management settings
remain host responsibilities.

Builtin catalog entrances are exactly `openai`, `google`, `anthropic` and
`deepseek`, selected by Cargo features. OpenAI ChatGPT OAuth and Anthropic OAuth
features forward to AI. Configured provider/model IDs remain open and do not add
builtin provider factories. `ProviderComposition` retains detached overlays,
availability observations and host-owned credential handles without invoking
credential or provider callbacks during selection. Model references use
`provider/id` and optional `:thinking`; an exact complete ID wins before suffix
parsing. The pure resolver reports ambiguity and missing-reference targets, ranks
aliases before dated variants and uses provider usability to break provider ties.
Default selection checks the release's four selected preferred IDs in order, then
uses the first available model in host/catalog admission order. Thinking defaults
to `medium` independently. Old `provider:model` and `@thinking` syntax is not an
upgrade compatibility API.

Target-source oracle rows and real local boundary tests are documented in
[the coding-tools conformance notes](../conformance/coding-tools/README.md).
