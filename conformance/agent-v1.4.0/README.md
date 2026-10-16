# Pi Agent v1.4.0 source observations

These are independent source expectations for the standalone Agent/loop slice.
They execute the exact upstream release and do not import Pirua production code.
Passing `--check` verifies source identity and fixture reproducibility. It does
not verify or accept Pirua's Rust implementation; Rust must run corresponding
scenarios and compare the observable behavior separately.

## Reproduce

Use Node.js with native TypeScript stripping (tested with Node 26.8.1 and npm
11.19.0). Set `PIRUA_UPSTREAM_ROOT` to a Git checkout containing the release
commit; the checkout's current branch and installed dependencies do not matter.
The commands below run from the repository root:

```sh
export PIRUA_UPSTREAM_ROOT=/path/to/pi-mono
node conformance/agent-v1.4.0/setup.mjs --install
node conformance/agent-v1.4.0/oracle.mjs --check
node conformance/agent-v1.4.0/causal-check.mjs
```

`setup.mjs` reads every pinned source with `git show <SHA>:<path>`, verifies its
SHA-256, and extracts the four unchanged Agent source modules under ignored
`.runtime/`. The oracle imports them directly with Node's TypeScript support.
The source tests are hashed as supporting provenance; they are not run by this
oracle. No scheduler, transcript, validation, or hook implementation is stubbed.

The AI slice supplies two release artifacts, resolved relative to `conformance/`:

- `catalog/v1.4.0/pi-ai-1.4.0.tgz`
- `provider-traces/v1.4.0/pi-telemetry-1.4.0.tgz`

Their hashes are checked before they are copied into ignored `.runtime/`.
`npm ci --ignore-scripts` installs them and their locked dependencies into this
directory's ignored `node_modules/`. Setup can use an alternate conformance
artifact directory through `PIRUA_AI_ARTIFACT_ROOT`; that is useful while the AI
slice is in a separate worktree. Neither environment variable is written into
the fixture or lockfile. The committed npm lock includes integrity values for
all registry tarballs and the local release tarballs. Every check also verifies
the installed transcript, validation, identity, and event-stream helper hashes
against the release artifact, plus the AI, telemetry, and TypeBox versions.
The source pin includes the corresponding AI helper TypeScript hashes for audit.
No upstream working-tree
`node_modules` is used. Installing the complete AI package also installs SDKs
for its other providers; these are oracle dependencies, not Pirua's supported
provider scope, and none are contacted during the run.

To intentionally regenerate after reviewing a changed scenario:

```sh
node conformance/agent-v1.4.0/oracle.mjs --write
node conformance/agent-v1.4.0/oracle.mjs --check
```

## Fixture shape and comparison

`observations.json` contains 26 named scenarios. `runner.mjs` supplies their
exact model, transcript, tool definitions, callback behavior, and gate release
order. Each scenario also has a short declarative `input` description for
orientation; use the runner for the complete inputs.

- `events` preserves each complete Agent event as observed at its awaited sink.
- `requests` preserves each provider model, normalized transcript context, and
  stream options as observed at the provider boundary.
- `timeline` interleaves event/request indexes with queue polls, preparation,
  hooks, execution, and explicit gate observations. An `event` or `request`
  index refers to the corresponding array in the same scenario.
- `messages`, `state`, and `outcomes` record return values or public state when
  applicable. The `runToolCall` scenarios intentionally have no Agent events.

Snapshotting occurs at observation time, so later mutations cannot rewrite
earlier events. JSON follows the upstream object values: undefined properties
are absent, array order is preserved, and null is preserved. `Set` state is
represented as an insertion-ordered array. A stream-option callback is encoded
as `"<callback>"`; its execution, where relevant, has a timeline entry. An
`AbortSignal` is encoded as `{ "aborted": <boolean> }`; object identity is
tested explicitly as `signalActive` in the Agent idle case. These are transport
representations of host objects rather than behavior filters.

Only genuine clock noise is normalized. `Date.now()` is fixed to
`1700000000000`, while explicit input timestamps 11–14 remain unchanged.
Every real monotonic `durationMs` is checked to be a nonnegative integer, then
encoded as `"<durationMs>"`. Its presence or absence is retained, distinguishing
executed calls from immediate validation/blocked/aborted outcomes. The dedicated
synthetic-clock case additionally preserves the numeric measurement as
`measuredExecuteMilliseconds: 7` and the total synthetic elapsed time as 607;
it proves hooks and an awaited progress listener do not inflate execute time.
All model IDs, tool/call IDs, thinking levels, identity fields, error messages,
tool declarations, result metadata, and queue snapshots remain intact.

Rust comparison should preserve all semantic fields and ordering shown here.
Normalize its generated wall-clock/duration values equivalently, then map only
language representation differences (for example callback handles and sets).
Do not remove transcript system messages, event barriers, errors, terminate
hints, or structured content to make a comparison pass. In particular,
`structuredContent` exists on tool outcomes/end events but is omitted from the
tool-result transcript by the target source.

## Discriminating cases

| Cases | Boundary observed |
| --- | --- |
| `finish-continue-{none,tool,steering,follow-up}` | Exactly one extra request; each natural scheduling path can satisfy explicit continuation; queue polling and next preparation order |
| `hard-exit-{normal-end,error,aborted}` | Finalized tool transcript before finish; `end` stops polling/preparation; provider error/abort remains a hard exit despite `continue` |
| `preparation-{repoll,pending}` | First/every request replacement; model/thinking replacement persists; pending input precedes request preparation; no repoll after prepareRequest; post-prepareNextTurn repoll only when earlier steering poll was empty |
| `transcript-tool-delta` | Last pending system anchor wins; stale tool intent is replaced; prompt/sections are preserved; tool removal gets an inserted system message before the pending user |
| `stream-request-boundary` | Transform before conversion before auth resolution/provider request; custom host messages stay out of provider context; start/update/end streaming lifecycle; requested thinking level and explicit identity overrides |
| `tool-order-{parallel,sequential,override}` | Source-order preflight; parallel completion order vs source-order transcript barrier; global sequential and per-tool sequential override; post-run progress ignored |
| `progress-await-and-late` | Accepted update listener blocks tool end; settled invocation updates are ignored while a peer still executes and after the run |
| `tool-validation-and-hooks` | Argument preparation/coercion; before-hook mutation without revalidation; schema/argumentsError/unknown-tool rejection; block, execute throw, reported error, after-hook throw; hook/update ordering |
| `structured-content-overrides` | Content-only replacement invalidates structured content; explicit replacement, preservation, shallow details replacement, false error/termination override |
| `length-truncation` | All calls from a length-truncated response fail without validation/hooks/execute, including schema-valid salvaged arguments; normal model continuation follows |
| `termination-{unanimous,mixed}` | Blocked and executed results participate together; all finalized hints must agree to stop; after hook can set the executed hint |
| `duration-excludes-hooks` | Numeric duration excludes before/after hooks and awaited progress publication |
| `tool-abort-{before-call,before-hook}` | Abort at admission and during the before hook; before hook still sees an already aborted signal; no execute/after hook/duration for immediate abort |
| `agent-assistant-tail-{one-at-a-time,all}` | Stateful continue from assistant tail; steering priority; non-consuming peek; one-at-a-time drain vs all; follow-up scheduling |
| `agent-end-listener-idle-barrier` | Listener subscription order; active signal and isStreaming survive awaited agent_end; prompt/continue/reset reject while listener runs; waitForIdle and prompt settle only after release; end preserves queued messages |

`causal-check.mjs` writes a temporary fixture under ignored `.runtime/` with the
two parallel tool-result transcript ends swapped. It verifies the real source
comparison rejects that ordering corruption, then removes only its own temporary
file. `oracle.mjs --check --fixture /path/to/observations.json` can also compare an
alternate fixture against regenerated source observations. Oracle execution has
a 30-second timeout so an ordering regression cannot hang indefinitely.

This is a compact conformance set, not exhaustive testing. It uses deterministic
fake providers/tools and controlled promises, with no live provider calls,
credentials, filesystem tools, network tools, timing sleeps, or UI. Provider wire
semantics belong to the AI slice. The Rust comparator and independent Agent
implementation review remain controller integration work.

## Rust comparison

The Rust driver is `crates/pi-agent/tests/upstream_v140.rs`. It compares all 26
complete scenario traces. See the Agent crate's
[representation boundaries](../../crates/pi-agent/upstream-coverage.md) for the
narrow native endpoint, schema-diagnostic, typed-busy-error and host-handle mappings.
The source observations themselves remain unchanged by those mappings.
