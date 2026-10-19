# Durable conversation runtime 1.4.0 observations

This is independent evidence preparation for the future Durable runtime slice.
It executes the actual published **1.4.0 Harness**, real built-in conversation,
generation, tool, context, usage, compaction and view modules, and real MemoryStorage
and Node SQLite. Expected values come only from those executions. This bundle makes
no Rust implementation or native runtime acceptance claim.

Run with the existing locked package environment:

```sh
node conformance/durable-v1.4.0/runtime.mjs --check
node conformance/durable-v1.4.0/runtime.mjs --causal
```

Explicit regeneration reads only the JSON input cases:

```sh
node conformance/durable-v1.4.0/runtime.mjs --write
```

`--check` never rewrites expectations. `--fixture <path>` chooses another trace for
negative controls. `--causal` changes the expected identity of the duplicate
submission in a disposable copy, invokes the actual `--check` command, and requires
rejection. Temporary SQLite files and negative fixtures live under the existing
ignored `.runtime` directory and are removed. Checks and generation are offline;
no npm install, package script, upstream rebuild or live provider call is used.

The 18 frozen Session files and nine frozen scheduler files remain byte-identical.
`runtime-source-pin.json` contains all 27 hashes and each run verifies them, the
unchanged lock, the existing Durable/Chord package identities and npm attestation
payloads. It extends the release hash set to **326 JavaScript/manifest files**, adding
201 pi-ai files. All installed pi-ai JavaScript and its manifest were first compared
byte-for-byte with the existing `conformance/catalog/v1.4.0/pi-ai-1.4.0.tgz` archive;
its SHA-256 is recorded. `runtime-ai-attestations.json` preserves the matching npm
SLSA association for the installed AI package. No package was installed for this
slice. Attestation payload/digest association is checked; cryptographic signature
trust is not independently reverified.

`verifyEnvironment({pinPath: runtime-source-pin.json})` verifies those identities
before importing the runner. With `PIRUA_UPSTREAM_ROOT` set to the local upstream Git
object store, it also verifies **77 immutable source/test/spec hashes** with
`git show 4a080252f06caf63a525d7873c5d94d3c23887e0:<path>`. No module is imported from
a mutable upstream checkout. The executed artifact is published JavaScript, not a
TypeScript rebuild. Related test names in `runtime-case-index.json` are source
associations; execution of whole upstream tests or their other scenarios is not claimed.

The fixture contains **18 cases / 30 actual release/backend executions**: 12 cases
on both Memory and SQLite and six SQLite-only live close/reopen cases.

| Family | Actual observations retained |
| --- | --- |
| Admission, deduplication and inbox | Input placement, same-request identity despite changed content, busy rejection, immediate display-only write, ordered follow-up/context write placement, queued withdrawal, receipts and inbox changes |
| Context selection | Raw entries, newest replacement and omission edits, active entries and per-entry contributions, excluded aborted/error/deferred assistants, orphan-result omission, call-ordered real and synthesized missing results, as-of context, and actual provider-normalized request |
| Baseline identity and heads | A newer head retains an older head's omission; an original baseline omitting a retained system delta leads despite an earlier assistant; a value-identical replacement of that baseline stays positional; an earlier cutoff still sees the original baseline |
| Pinned request and partial recovery | A committed partial survives real SQLite close/reopen, becomes an aborted entry, and the request reuses its old model, thinking, stream options, provider identity, cutoff/context and system sections despite changed host settings/agent; before-request hooks rerun |
| Retry and deferred generation | Persisted absolute sleeping checkpoints survive close/reopen; early wakes dispatch no retry/poll; due retry answers; deferred polling sleeps again at a strictly later deadline and then answers; explicit abort calls the actual deferred-cancel adapter |
| Tool pipeline and usage | prepareArguments runs before initial validation, beforeTool sees repaired arguments, its replacement is revalidated, persisted execute intent precedes the local effect, invalid initial/replacement arguments execute no effect, and assistant/tool usage aggregates through actual UsageDoc updates |
| Nested tools | Real owned child tool task, parent/call/key identity, per-key nested document and committed result, same-key reuse without another effect, changed-argument rejection, hook parent attribution, source live slot/progress publications and retirement |
| Committed tool progress and mounted view | Actual ToolApi.details promise resolves after its progress commit; the acknowledgement records persisted LiveDoc. Actual mounted frames retain operations and values; independent Chord replay rebuilds every revision and frame count equals touching source commits |
| Tool interruption policy | Live running tools close and reopen with stored/current safe-safe, safe-unsafe and unsafe-safe policies. Only safe-safe executes again. Interrupted nonreplayed results use durable partial output; all cases continue to a final assistant answer |
| Yield, fork and accounting | An onYield continuation keeps the original input open until the final answer; an as-of fork inherits transcript and old agent instructions, receives independent provider identity and fresh usage, then contributes its own usage to Session totals |
| Compaction | Real manual range selection, actual summarizer request, summary placement/head and kept tail with raw history retained; a reset during the gated summary makes its write stale, creates no summary entry and still counts the provider spend |

## JSON action and trace contract

`fixtures/runtime-schema.json` describes the language-neutral envelope. Cases have
an ID, pinned source basis, JSON input, optional backend list, and backend-keyed
expected output. Omitting backends means Memory and SQLite. No fixture field is
evaluated as JavaScript. Expected output retains ordered host observations, complete
provider requests/options, local tool and hook observations, full source commit
publications, and category/message reports. Undefined host results use
`{"$undefined":true}`. The existing recursive trace codec represents any non-scalar
JavaScript string value as `{"$utf16":[code units...]}`; this reserved marker is trace
encoding, not product JSON. Every output/key is checked for scalar Unicode before
writing or comparison, so the whole file is ordinary Rust-readable JSON.

Host operations call actual conversation submission/status/wait/abort, raw entry
append, agent configure, context/as-of reads, entry reads, built-in document snapshots,
usage, task reads/inspection, resume/idle/wake, settings changes, reset, fork, compaction,
watch and lifecycle APIs. `untilRequests`, `untilEffects` and `untilLive` are bounded
test-host waits for actual observations. `release` controls only an external provider
or local tool gate. `advance` sets the injected scheduler wall clock and resumes;
`fireTimers` invokes recorded foreground host callbacks. Background timer scheduling
is declined by the permitted optional timer boundary. Sleep deadlines are decided by
the real scheduler and its injected wall clock, never by the driver.

Local tool descriptors register actual `defineTool` definitions and real extensions.
Their actions call `api.output`, `api.details`, `api.diagnostic`, `api.executeTool`,
or return literal tool results; gates and throws are controlled local effect behavior.
The driver reads the actual persisted TaskRecord at execute entry, so the trace's
intent evidence precedes the effect in time. Nested result/key identity is decided by
the source API. Tool hooks and generation hooks are actual registrations; the fixture
can supply a literal before-tool decision, append a before-request user message, or
continue once on yield. Sections are actual section registrations. No scheduler,
context normalization, generation checkpoint, replay-policy, usage or compaction
algorithm is implemented in the runner.

References such as `$input`, `$first.id`, `$receipt.state.outcome.result.submissionId`,
`$args.n`, `$local.result`, `$run`, `$taskId`, `$callId` and `$clock` resolve named host
results, actual tool inputs/results and injected time. They are JSON value lookup,
not code. Observations detach immediately from mutable host collections.

The external provider is a real `createProvider` registration in real `createModels`.
Only the transport is scripted: ordered `stream`/`fetch` descriptors emit events
through the actual pi-ai `AssistantMessageEventStream`, including an optional partial,
gate, error or deferred handle. Calls to `cancelDeferred` are captured. Request
normalization, model/auth resolution, pinned request preparation, polling and final
settlement remain source behavior. `fauxProvider` supplies model metadata only; its
random/timed transport never runs. The local provider uses empty test auth and never
reads credentials. Retained requests exclude the live AbortSignal and telemetry
callbacks; all JSON options and the complete normalized context are kept.

## Deterministic host and comparison boundary

The controller approved three narrow deterministic host choices. First, the real
`conversationCreated` callback commits an explicit `ProviderDoc.sessionId` of
`fixture-session-<conversation ID>`. Provider messages carry a fixed injected wall-clock
timestamp and explicit duration zero. Second, scheduler time starts at 1000 and moves
only through explicit `advance` operations. Third, process-local `performance.now`
is a nondecreasing controlled host clock used for tool duration; firing a host timer
can advance it by that delay. It is separate from scheduler wall time. There are no
source module patches. These choices preserve full committed source output while
making it reproducible. Default UUID generation and real elapsed performance are
outside the evidence. A native adapter must provide equivalent controlled host
identities and time before comparing these fields.

Exact source operation batches remain observations. A later native comparison must
exercise actual runtime APIs, retain committed values/receipts and causal ordering,
verify intent before effects, intersect stored/current replay policy, and replay its
own emitted operations against prior snapshots. Different proxy operation spelling
may be replay-equivalent; dropping nonempty structural operations or replacing every
state with a copied final source object would not exercise the contract. View batches
must remain at most one per relevant native Session commit. This document does not
accept a future native adapter or define a new assurance framework.

## Evidence limits

Every runtime recovery case uses a **live source invocation followed by actual normal
Harness.close and SQLite file reopen**. No task records are seeded in this runtime
bundle, and no OS/process crash is claimed. Subscription begins after Harness.open;
reopen reconciliation is observed through actual persisted records/snapshots rather
than invented past publications. Cleanup after the last explicit operation is excluded
from expectations.

The progress case proves **source local ToolApi.details commit/await behavior**. It
provides no evidence for native extension transport credit, framing/grants, MCP
transport, native Coding bindings or a native invocation protocol. Those need native
adapter tests. This suite does not execute CodeMode, JavaScript/Jiti extensions,
provider-specific live networking, migration callbacks, every cancellation/storage
failure race, ambiguous commits, queue mode permutations/steering/reset boundary
combinations, all failure-inbox policies, missing-model recovery, concurrent nested-key
admission or nested-call recovery. It does not cover automatic background/blocking/
overflow compaction, compaction retries/recovery, dynamic tool loading, experimental
agent events, view overflow/backpressure or every document remount case. The existing
Session/scheduler suites retain their separate bounded coverage. None of these gaps
is filled with a simulated successful result. No new source/spec contradiction was
observed in these runtime cases.

The validation/revalidation scenario selects `toolExecution: "sequential"` so its
raw committed entry order is determined by the input. Default parallel tool rounds
may commit sibling results in completion order; the source contracts separately
order model context and `afterTools` results by call order. This fixture does not
assert a fixed completion order for parallel siblings.
