# Coding-agent Headless Integration

## Inclusion rule

`pi-coding-runtime` owns application-neutral headless composition needed to
produce pi coding-agent behavior over the Rust AI and Agent surfaces. It does not
own CLI/TUI presentation or duplicate generic Harness behavior.

## Included upstream responsibilities

### Model runtime

Corresponding upstream modules:

```text
core/model-runtime.ts
core/model-registry.ts
core/model-config.ts
core/models-store.ts
core/runtime-credentials.ts
core/auth-storage.ts
core/provider-composer.ts
core/remote-catalog-provider.ts
core/model-resolver.ts
```

Rust module owner: `pi_coding_runtime::models`.

It composes built-in and configured providers, credentials, catalog overlays,
availability snapshots, model selection, and product defaults over `pi-ai`.
It does not become the `Provider` or `Models` contract authority.

### Coding tools

Rust module owner: `pi_coding_runtime::tools`.

It adds grep, find, ls, PowerShell, coding-specific prompt metadata, output
accumulation, file-operation classification, and the read/bash/edit/write
implementations. `pi-harness-runtime::tools` owns only generic tool contracts,
registry/validation, progress, cancellation, ordering, and settlement adapters;
it does not own filesystem or process tools. Shared helpers remain internal to
`pi-coding-runtime` rather than creating another crate.

### Messages and system prompt

Rust modules:

```text
messages
system_prompt
resources
```

They own coding-specific custom message roles, conversion/projector policy,
coding guidelines, selected-tool prompt contributions, and normalized resource
snapshots. Filesystem discovery and UI rendering remain host responsibilities.

### Session application lifecycle

Rust modules:

```text
session_lifecycle
legacy_import
builder
```

They own creation/replacement/fork/import orchestration at the application
boundary and construct a Rust `AgentHarness`. Durable tree, entries, migration,
fork semantics, queues, abort, and recovery remain in Agent/Session crates.

The target composition interface is:

```rust
pub struct CodingHarnessBuilder {
    pub session: SessionHandle,
    pub models: Arc<dyn Models>,
    pub execution_env: Arc<dyn ExecutionEnv>,
    pub resources: Resources,
    pub settings: CodingSettingsSnapshot,
    pub extensions: Arc<dyn ExtensionRuntime>,
}

impl CodingHarnessBuilder {
    pub async fn build(self) -> Result<AgentHarness, CodingRuntimeError>;
}
```

This mirrors the intent of upstream `src/server/create-harness.ts`: the
application supplies environment, tools, prompt metadata, and system prompt,
then constructs `AgentHarness`.

## Responsibilities moved out of current AgentSession

| Current concern | Target owner |
|---|---|
| run/turn/checkpoint lifecycle | `pi-harness-core/runtime` |
| steering/follow-up/nextRun | `pi-harness-core` |
| retry/backoff/recovery | `pi-harness-core` |
| abort/close/failure drain | core/runtime |
| compaction state and algorithm | core/runtime compaction modules |
| navigation state and atomic publication | core/runtime |
| tool batch order/replay/terminate | core/runtime |
| session tree/storage/migration | `pi-agent-session` |
| provider/auth/model contract | `pi-ai-*` |
| extension hook protocol | `pi-extension-protocol` |

The current `AgentSession` is a behavior source and migration ledger, not a
permanent Rust class boundary.

## Host-only responsibilities

The following are not part of the 15-crate harness workspace target:

```text
src/cli/**
src/modes/**
interactive components and themes
keybindings and footer components
HTML/session export presentation
clipboard and image UI
self-update/version checks
npm/git package installation
file-backed settings precedence and UI
project trust prompts
legacy stdin/stdout RPC presentation
packages/client/server UI adapters
```

If a full Rust pi CLI is later desired, these belong in a separate
`pi-coding-host` design and MUST consume `pi-coding-runtime` rather than extend
the Harness core.

## Extension loader boundary

Rust owns extension manifest validation, capability registry, ordering, and
effect routing once an authenticated endpoint has been supplied. A JavaScript
language host owns Jiti/module loading, virtual modules, JavaScript handlers, TUI
factories, JS provider SDK objects, process lifecycle, transport authentication,
and any sandbox. `pi-coding-runtime` accepts an `ExtensionEndpoint`; it does not
claim to securely spawn an untrusted peer.

Other languages may implement capabilities directly without a language host.
Applications may connect those peers through stdio, a Unix socket, or another
ordered transport after establishing identity and grants. Integration tests may
spawn reference peers, but that test harness is not a production supervisor.

## Settings and resources

The Harness receives detached normalized values:

```rust
pub struct CodingSettingsSnapshot {
    pub model: ModelRef,
    pub thinking_level: ThinkingLevel,
    pub active_tools: Vec<ToolName>,
    pub retry: RetryPolicy,
    pub compaction: CompactionSettings,
    pub stream: StreamOptionsWire,
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
}
```

Reading and merging settings files is not part of durable runtime state.
Operations capture the relevant normalized subset at their specified
linearization point.
