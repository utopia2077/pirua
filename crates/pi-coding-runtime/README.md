# Headless CodingSession

`pi-coding-runtime` combines the transient `pi-agent` with the canonical
`pi-coding-session` transcript tree. `SessionManager` owns saved records and
projection; `CodingSession` owns live prompts, tools, hooks, routing, and recovery.
Memory and SQLite use the same logical session API. Reopening reconstructs context
and selections without executing recorded tool calls.

Create a `SessionManager` with a host-owned store, then build a session with a
selected `Model` and an Agent `StreamFn`. For real providers,
`CodingSessionBuilder::from_registry` connects an existing AI `ModelsRegistry` and
`RequestRuntime`; authentication, OAuth refresh, provider endpoints, and request
services remain owned by AI. `with_model_runtime` supplies dynamic model routing.
Initial thinking defaults to the release preference `medium`, clamped to the selected
model. `with_thinking_level` provides an explicit preference; saved selections are
restored when no explicit preference is supplied.
The builtin provider set remains OpenAI, Anthropic, Google, and DeepSeek; host
configured provider identities remain open.

```rust,no_run
use std::sync::Arc;
use pi_coding_runtime::{CodingSessionBuilder, PromptOptions};
use pi_coding_session::{SessionManager, SqliteStore};
# async fn example(model: pi_ai::Model, stream: pi_agent::StreamFn)
# -> Result<(), Box<dyn std::error::Error>> {
let store = Arc::new(SqliteStore::open("coding.sqlite")?);
let manager = SessionManager::create(store.clone(), "/workspace/project")?;
let session = CodingSessionBuilder::new(manager, model, stream)
    .with_system_prompt("Work carefully and verify changes.")
    .build().await?;
session.prompt("Read the project instructions", PromptOptions::default()).await?;
let saved_id = session.session_id();
session.wait_for_idle().await?;
session.dispose().await?;
let reopened = SessionManager::open(store, &saved_id)?;
# let _ = reopened;
# Ok(()) }
```

Run the complete offline example with:

```sh
cargo run -p pi-coding-runtime --example headless_session --locked
```

The example performs a real Agent tool call, commits to SQLite, reopens, forks,
and compacts through a headless hook. It uses no network or provider credentials.

Session mutations and transcript semantics:

- `prompt` expands host-supplied prompt templates and runs input/start hooks.
  While streaming, explicitly select steering or follow-up delivery. `steer`,
  `follow_up`, queue modes, queue inspection, and `clear_queue` use Agent's queues.
  Context-only custom messages wait for a tool-safe turn boundary; `NextTurn`
  custom messages accompany the next starting prompt.
- `execute_bash` runs host-requested shell commands with streamed sanitized deltas,
  cancellation, command prefixes, and full-output spill files. `record_bash_result`
  admits host-supplied results. Shell messages wait until the current Agent batch
  settles; results held during startup precede its starting user message.
  `exclude_from_context` keeps the saved record while omitting it from provider
  context. `abort_bash` and `wait_for_bash` govern shell activity independently;
  Agent `wait_for_idle` retains the release's Agent/compaction meaning. Disposal
  cancels and waits for both kinds of activity.
- `set_name`, `set_label`, `edit_context`, `navigate_tree`, and `fork` use native
  immutable session records. A user-message navigation returns editor text and
  moves to its parent; other entries become the selected leaf. Navigation may
  summarize abandoned work, and summary/label/leaf publication is one transaction.
  Fork creates an independent saved session with inherited labels.
- `snapshot`, `projection`, `messages`, `tree`, `stats`, and `context_usage` expose
  detached observations. Context estimates invalidate stale provider usage after
  edits or compaction. Immediately after compaction, context usage stays unknown
  until a later assistant response reports usage. Generated compaction and branch
  summaries explicitly disable cache retention and use fresh routing identities
  separate from the conversation; retries keep that summary identity and use AI's
  configured capped retry policy.
- `compact` uses canonical visible messages, preserves complete tool/result pairs,
  handles split turns, retains a system/tool checkpoint, tracks touched files,
  and records summary usage. Hooks may cancel or provide a compaction. Automatic
  threshold compaction preserves completed responses; overflow and recoverable
  length recovery omit a failed attempt and its results before one compact/retry.
  Transient provider retries use the configured bounded backoff and durable
  omission records. Raw history remains available.
- `set_model`, thinking-level selection/cycling, scoped model cycling, and
  `ModelRuntime::resolve` separate requested selection from the physical model
  used for one request. Router state is saved as canonical custom records on the
  selected branch, and virtual selections survive physical responses and reopen.
- Cache warming uses release cache lifetimes, replay eligibility, price tiers,
  continuation economics, fixed age limits, deadline margin, and one-token
  refreshes. Modes are off, streaming, and idle. Successful refresh usage is
  recorded separately from conversation messages. Routed requests that differ
  from the selected model are not warmed.

`RegisteredTool` separates executable capabilities from model-visible declarations
and nested-call reachability. Direct active tools are both declared and callable;
model-only tools are excluded from nested calls; deferred and CodeMode tools remain
callable while inactive; hidden tools cannot be activated. `prepare_loadout` hooks
may change descriptions and hide declarations without removing implementations.
Late registrations can satisfy pending names restored from saved declarations.
Tool execution always passes through Agent argument validation and the common
before/after hooks. `execute_tool` uses `run_tool_call`, carries parent IDs,
publishes nested progress, bounds nested argument/error records, and aggregates
nested usage onto the parent result. `downgrade` gives registered callbacks a weak
session capability so tools do not form ownership cycles.

`SessionHooks` is the language-neutral extension boundary. Input, pre-start,
context, finalized message transformation, loadout, tool, turn-end/pre-settlement,
compaction, navigation, and session-change hooks receive detached values and
cancellation. Outbound image blocking runs after Coding-to-provider conversion, so it covers
custom-message images as well as native user and tool-result images while saved
records retain their raw content. Message replacements retain their roles and feed dispatch, events,
future context, and the saved record. Forced prompt text is applied to outbound
context after context hooks; structured prompt sections remain in the transcript.
Hooks run before public event subscribers. A finalized message event is delivered
before its store commit, matching the release ordering. Event acknowledgements
are native asynchronous barriers; callback and commit errors remain observable.
`wait_for_idle` reports the last run failure. Disposal closes admission before
cancelling and waiting for outstanding Agent and shell activity, then releases
subscribers even after a failed run. Concurrent disposal callers wait for cleanup;
dropping a disposal wait leaves admission closed and another call can finish it.

Boundary drafts are previewed against a detached native session manager, validated
against native AI/Agent message types, and published in one canonical transaction.
Malformed custom blocks or context replacements leave the saved snapshot unchanged. `append_custom_entries` similarly commits
adapter state as one batch. `branch_cursor` captures session identity and a local
navigation generation. `append_custom_entries_if_cursor` accepts legitimate
same-branch appends and rejects navigation away and back, fork, or session switch.
Successful concurrent script deltas apply in completion order. A weak capability
keeps access to the same live session owner across replacements; adapters that bind
an operation to its originating branch must also capture and validate the cursor.
`replace_tools_for_source` replaces one adapter's tool batch atomically and rejects
collisions with other owners before changing the registry.

The library exposes the accepted headless release behavior. Terminal UI, slash
command dispatch, JS/Jiti loading, package management, HTML/export presentation,
JSONL storage/import, and historical migrations belong outside this API. Shared
builtin tools provide shell/filesystem operations through `ExecutionEnv`; a
terminal `!` command interface is not part of the headless session surface.

Native values are detached. Cache context freshness and returned router-state changes
use JSON value equality; they do not expose JavaScript object identity. Resource
snapshots supplied with `with_resources` are host-owned immutable input: `/skill:`
expansion uses the captured skill content. Hosts that want filesystem resource
reloads can load a new snapshot and call `set_resources` before the next prompt.

The public source observations and reproduction instructions are in
[`conformance/coding-session-runtime-v1.4.0`](../../conformance/coding-session-runtime-v1.4.0/README.md).
