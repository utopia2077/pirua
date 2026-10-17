# Coding MCP and local CodeMode adapters

`pi_coding_runtime::mcp` and `pi_coding_runtime::codemode` connect the reusable
`pi-mcp` client and `pi-codemode` worker to a `CodingSession`. Both expose ordinary
Agent tools. Calls use the same schema validation, before/after hooks, progress,
cancellation and nested-call accounting as other session tools.

## Register an MCP client

The host configures and initializes a `pi_mcp::McpClient` using its chosen stdio or
Streamable HTTP transport and explicit authentication policy. HTTP/OAuth discovery,
tokens, persistence and any interactive authorization remain host capabilities.
The adapter never starts a browser, finds credentials, or reconnects implicitly.

```rust,ignore
use pi_coding_runtime::{ToolExposure, mcp::{McpServerOptions, McpToolRegistry}};
use pi_agent::CancellationToken;

let mcp = McpToolRegistry::new(session.downgrade());
let mut events = initialized_client.subscribe();
let names = mcp.attach(
    "docs",
    initialized_client.clone(),
    McpServerOptions {
        exposure: ToolExposure::Deferred,
        ..Default::default()
    },
    CancellationToken::new(),
).await?;

// In the host's notification loop:
// mcp.handle_event("docs", events.recv().await?, signal.clone()).await?;
// Explicit discovery refresh is also available:
mcp.refresh("docs", CancellationToken::new()).await?;

// Retire only this registry's registrations; the host still owns the client.
mcp.retire("docs").await?;
```

Names follow the release's `mcp__<server>__<tool>` algorithm: non-ASCII-name
characters become underscores, names are limited to 64 bytes, and long or
colliding sanitized names receive the first eight hex digits of SHA-256 over
`server + NUL + original tool name`. A refresh reuses existing mappings even if
discovery order changes. Foreign final-name collisions and duplicate discovered
tool names reject the entire update. A failed refresh leaves the old loadout
intact. Refresh, retirement and registry drop invalidate old registration
capabilities and cancel their outstanding adapter requests. Dropping a registry
removes only its owned tools; it does not close the host's client.

Discovery awaits outside the registry's short state locks. Retirement also
cancels pending attachment discovery, preventing a late response from installing
tools afterwards. Concurrent lifecycle changes reject a stale discovery result
by registration generation; the adapter does not silently retry it.

The host drives `handle_event` to refresh on `notifications/tools/list_changed`
or retire on client closure. This explicit loop gives the host ownership of task
lifetime and reconnect policy. Cloned clients must not be closed by unrelated
consumers while still in use.

Every MCP tool declares a `CallToolResult` output schema. CodeMode receives the
complete result, including `content`, `structuredContent`, `isError` and unknown
protocol fields, with only top-level `_meta` removed. Content-block metadata is
retained. An MCP `isError` is an error for the model; a script can still inspect
the structured result. Local `$ref` values in the server's output schema are
relocated under `structuredContent`, while schema data such as `const` and
`default` remain data.

Model output contains text and images. Embedded text and image resources are
projected directly; text MIME blobs are decoded as UTF-8. Other binary resources
are saved with an explicit output saver or a uniquely created temporary file.
Resource links describe the URI, title, MIME type and size. Set
`McpResultOptions::resource_reader` only when the host has installed a tool with
that name that can read the server's resources. Audio and unknown blocks get
text placeholders. Text above 20 KiB keeps its beginning and end, and the full
text is saved separately. This limit never truncates the script's structured
result. The host owns retention and cleanup of saved output files.

## Attach the local CodeMode tool

Build and package the matching `pi-codemode-worker` executable alongside the host.
Its absolute or explicit configured path is required; the sandbox does not search
`PATH` for a worker.

```rust,ignore
use pi_codemode::{Sandbox, SandboxOptions};
use pi_coding_runtime::codemode::{CodeModeAdapter, CodeModeOptions};

let sandbox = Sandbox::new(SandboxOptions::new(worker_path))?;
let code = CodeModeAdapter::new(
    session.downgrade(),
    sandbox,
    CodeModeOptions::default(),
)?;
code.attach()?;
```

The model invokes `codemode` with an ordinary JSON object:

```json
{"code":"const result = await tools.mcp__docs__search({query:'transactions'}); text(result); store('last', result);"}
```

This `{code: string}` input and JSON-schema-based tool descriptions are the native
Rust representation. The JavaScript source itself is an async function body,
with top-level `await`, `return` and `exit()`. The adapter does not claim byte
parity with the upstream provider-specific raw-source grammar or TypeScript
declaration printer. `ALL_TOOLS`, `describeTool` and `searchTools` describe the
same actual callable registry using normalized JavaScript names, JSON input and
output schemas, and descriptions. Search uses the release's BM25 ranking and
tokenization, including schema descriptions and property names.

The declaration generated by `attach()` omits deferred tools. Scripts still find
them through `ALL_TOOLS` and the discovery globals. Repeating `attach()` on the
same adapter refreshes its model-facing description after a registry change.
Each execution snapshots the current callable tools. Hidden, inactive Direct
and ModelOnly tools remain unavailable to scripts. `codemode` excludes itself,
and another adapter cannot overwrite an existing owner's `codemode` registration.

A tool with an output schema and present `structuredContent` resolves to that
value, including `null` and structured error results. Other successful tools
resolve to joined text. Other errors reject with an `Error`. `image(block)` can
show an image returned by an ordinary or MCP tool. No classifier or image-model
generation globals are installed by this adapter.

The optional first source line accepts exactly these fields:

```js
// @options: {"max_output_tokens": 2000, "timeout_ms": 30000}
const results = await Promise.allSettled([
  tools.read({path: "README.md"}),
  tools.mcp__docs__search({query: "sessions"})
]);
text(results);
```

`max_output_tokens` is a non-negative JavaScript safe integer. `timeout_ms` is a
positive integer at most 2,147,483,647. A misplaced directive or unknown field is
an error. Defaults are 10,000 output tokens, a five-minute worker deadline and 32
concurrent nested calls. The neutral sandbox separately enforces worker, memory,
stack, JSON, store and output limits. Text budgets use UTF-16 length as in the
release, keeping Unicode scalar boundaries intact when trimming. Multiple text
items are numbered, console output is grouped, and long output is saved before
truncation. Shown images retain image blocks and receive saved-file labels.

## Session state and cancellation

`store(key, value)` and `load(key)` operate on JSON values. Each execution reads
only `codemode-store` custom entries on the selected parent chain. A successful
script commits all its set/delete operations as one canonical custom entry.
Failed and aborted scripts commit no store delta. Tool side effects that happened
before a script error are not undone.

The script captures a session ID and process-local navigation generation.
Normal same-branch appends, including custom entries from nested hooks or tools,
are permitted. Concurrent successful scripts apply their complete deltas in
completion order; later completion wins when both write the same key. Switching,
forking or navigating invalidates the captured cursor, including navigation away
and back to the same leaf. A stale script receives an error instead of writing
to a newly selected branch. Store failures remain observable and do not advance
the session's local canonical snapshot. No automatic conflict retry or script
re-execution occurs.

Reopening or forking reconstructs the matching store from canonical records and
executes no historical JavaScript or tools. Weak session capabilities avoid a
reference cycle between registered tools and their owning session.

Each nested tool runs through `CodingSession::execute_tool`; the parent result
owns the bounded nested-call record and aggregate usage. Progress also includes
CodeMode call rows. Ending or aborting a worker cancels unawaited nested calls.
The adapter waits for admitted callbacks to settle before returning the parent
result, so events and usage cannot arrive afterwards. Host tools must honor the
Agent cancellation token; arbitrary host code is not forcibly terminated. The
worker process itself is killable and reaped before completion.

The local worker has no implicit host file, network or credential bindings.
The native QuickJS process boundary is not an OS permissions sandbox. Its actual
VM fingerprint and the neutral `start`/`deliver`/clock/seed boundary remain
available for the later Durable adapter; this module adds no persistent journal
or suspended-script recovery claim.

## Offline verification

The dedicated `mcp_adapter` and `codemode_adapter` tests cover the production
SQLite store, the actual Rust QuickJS worker and the accepted Python stdio MCP
peer, alongside memory-transport protocol probes. The exact-tag oracle at
`conformance/coding-tool-adapters-v1.4.0` records 19 cases from six source files
read by Git object ID. It requires Node's built-in TypeScript stripper and no
upstream `node_modules` or external service.
