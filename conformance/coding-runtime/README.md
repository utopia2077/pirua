# Coding runtime conformance (S15-M6-B)

This checked-in intermediate corpus contains the twelve M6-A and M6-B rows
that are currently witnessed through assembled operations, with twelve
one-change causal negatives. The six lifecycle and extension rows are retained
as `pendingRows` in `manifest.json` for M6-C.

Every active row runs a fresh assembled `CodingHarnessBuilder`/`AgentHarness`
operation in `crates/pi-coding-runtime/tests/coding_runtime_conformance.rs`
and its M6-B support module. The bridge installs deterministic local adapters,
subscribes to real Harness event tags, starts the selected operation, waits for
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
  `read` binding, emits progress and terminal tool events, and settles durable
  entries through an injected deterministic environment.
- `tools.coding_operations`: real `write` and `bash` bindings mutate and
  execute against the deterministic environment, with their ordered tool
  events and provider request count captured.
- `queue.steer_during_turn` and `queue.follow_up_after_turn`: the real queue
  admission and drain operations run while an adapter stream is held, with
  queue event ordering and post-operation idle state captured.
- `retry.overflow_precedence`: an implementation-generated context overflow
  response triggers compaction before retry, with three provider requests and
  compaction events observed.
- `retry.provider_recovery`: an implementation-generated retryable provider
  response emits the real `retry_scheduled` event, retries once, and reaches a
  terminal recovery response.
- `abort.active_stream_cleanup`: an active assembled stream is cancelled,
  returns an aborted terminal outcome, and leaves no active adapter streams
  after `wait_for_idle`.
- `compaction.context_projection`: two assembled turns are compacted through
  the public lane operation; provider context message counts and durable entry
  deltas are captured around the compaction.

No live provider, network, credential refresh, transport, protocol/schema, host
ownership, or durable-session implementation is added. The only credential
observation is a host-owned boundary: injected credential inputs are retained
without reads. M6-A causal mutation changes the capability input for the
negative row; M6-B mutations increment one durable side-effect count.

Run the intermediate target with:

```text
cargo test --offline --locked -p pi-coding-runtime --test coding_runtime_conformance -- --nocapture
```

Run the byte-preserving causal self-test with:

```text
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/coding-runtime/causal.mjs
```

`compare-actual.mjs` rejects unknown envelope, outcome, error, observation, and
event fields before exact comparison. The fixture authority is pinned to
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
