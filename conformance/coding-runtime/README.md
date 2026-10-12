# Coding runtime conformance (S15-M6-B)

This checked-in intermediate corpus contains the twelve M6-A and M6-B rows
that are currently witnessed through assembled operations, with twelve
one-change causal negatives. The six lifecycle and extension rows are retained
as `pendingRows` in `manifest.json` for M6-C.

Every active row runs a fresh assembled `CodingHarnessBuilder`/`AgentHarness`
operation in `crates/pi-coding-runtime/tests/coding_runtime_conformance.rs`
and its M6-B support module. The bridge installs deterministic local adapters,
retains real Harness event payloads, starts the selected operation, waits for
terminal or idle settlement, and reads the session entry projection. The four
closed side-effect fields are
`provider_calls`, `tool_invocations`, `session_publication_events`, and
`durable_entries`; lifecycle, queue, retry, tool, abort, and context details
are additional observations.

The M6-A rows are:

- `model.credentials_catalog`: detached overlay precedence, provider-scoped
  availability transition, fuzzy selection, and the host credential boundary.
  The overlay keeps the host provider/model identity but uses distinct
  validated `name` and public `base_url` metadata; the deterministic provider
  request records those fields so removing the override fails the witness.
- `prompt.system_resources`: detached resources passed through assembled
  prompt admission and observed through the real operation/session path.
- `prompt.settings_snapshot`: detached settings passed through the builder and
  observed through the real operation/session path.
- `capability.required_prestart_failure`: empty composition is admitted to the
  builder preflight, which returns exactly
  `CodingRuntimeError::MissingCapability { capability: "models.resolve" }`;
  provider calls, tool invocations, session publication events, and durable
  entries are all zero before the positive inputs are restored.

The M6-B rows are:

- `tools.validate_progress_settle`: a provider tool call reaches the real
  `read` binding. The provider call arguments match ToolStart, actual
  ToolUpdate progress precedes ToolEnd settlement, and host read data matches
  the result content.
- `tools.coding_operations`: real `read`, `write`, `edit`, `grep`, `find`,
  `ls`, and `bash` bindings run in order against the deterministic environment.
  Structured path/read/write/canonical/metadata/list/process calls retain
  arguments, results, and errors. Provider tool calls, Harness payloads, edited
  file bytes, search/list results, and process output are compared together.
- `queue.steer_during_turn` and `queue.follow_up_after_turn`: the real queue
  admission and drain operations run while an adapter stream is held, with
  QueueUpdate payloads and a public lane snapshot proving message placement.
  The subsequent provider context proves delivery after the first turn, and
  an idle snapshot plus register scans prove that the queue is drained.
- `retry.overflow_precedence`: an implementation-generated context overflow
  response triggers compaction before retry, with three provider requests and
  compaction payloads observed. The real overflow response, summary request
  context, durable CompactionEntry metadata, continued request context, and
  absence of ordinary retry events/sleeps prove the precedence.
- `retry.provider_recovery`: an implementation-generated retryable provider
  response emits the real `retry_scheduled` event, retries once, and reaches a
  terminal recovery response. RetryScheduled fields match the actual error,
  second attempt, and measured clock advancement/sleep; ordinary recovery
  does not invent RetryStart or RetryEnd events.
- `abort.active_stream_cleanup`: an active assembled stream is cancelled,
  records runtime cancellation in the adapter lifecycle trace and returns an
  aborted terminal outcome. After `wait_for_idle`, public lane state and
  operation/pending register scans show cleanup, with zero active streams.
- `compaction.context_projection`: two assembled turns are compacted through
  the public lane operation. Both summary requests for the split turn and
  their actual responses are retained, with parent, summary, retained tail,
  token estimate, usage, and hook source from the durable CompactionEntry.
  A subsequent prompt proves projection of that summary and retained tail.

No live provider, network, credential refresh, transport, protocol/schema, host
ownership, or durable-session implementation is added. The only credential
observation is a host-owned boundary: injected credential inputs are retained
without reads. M6-A causal mutation changes the capability input for the
negative row. The eight M6-B causal mutations each remove or alter one required
operation witness, and semantic validation must reject them before exact
fixture comparison. The self-test preserves fixture and manifest bytes and
the exact twelve-active/six-pending partition.

Run the intermediate target with:

```text
cargo test --offline --locked -p pi-coding-runtime --test coding_runtime_conformance -- --nocapture
```

Run the byte-preserving causal self-test with:

```text
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/coding-runtime/causal.mjs
```

`compare-actual.mjs` rejects unknown envelope, outcome, error, observation, and
event fields, including nested payload and message fields, before exact
comparison. Row-specific checks correlate events, provider requests, host
results, clock facts, and public session state. The fixture authority is pinned to
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
