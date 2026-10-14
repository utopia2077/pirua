# Coding runtime conformance (S15-M6 closure)

The frozen corpus contains eighteen positive rows and eighteen one-change
causal negatives, with zero pending rows. Every positive keeps the exact S15
scenario identity and source references. The bridge supplies fresh foundation,
operation, lifecycle and extension observations. Independent acceptance and
final whole-workspace closure remain verification obligations.

The [model implementation responsibility table](model-responsibilities.md)
records selected catalog/overlay/default/selection/availability/credential
owners, concrete source and tests, and the precise host and unverified limits.

Every active row runs a fresh assembled `CodingHarnessBuilder`/`AgentHarness`
operation in `crates/pi-coding-runtime/tests/coding_runtime_conformance.rs`
and its M6-B and M6-C support modules. The bridge installs deterministic local adapters,
retains real Harness event payloads, starts the selected operation, waits for
terminal or idle settlement, and reads the session entry projection. The four
closed side-effect fields are
`provider_calls`, `tool_invocations`, `session_publication_events`, and
`durable_entries`; lifecycle, queue, retry, tool, abort, and context details
are additional observations.

The M6-A rows are:

- `model.credentials_catalog`: detached overlay precedence, provider-scoped
  availability transition, configured alias selection, and the host credential boundary.
  Separate focused model tests witness unregistered fuzzy selection and defaults.
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

The M6-C rows are:

- `branch.fork_atomic_publish`: an assembled source turn precedes a real
  `MemorySessionRepo::fork`. The returned destination already contains the full
  branch, leaf, configuration and idle state. `attach_fork` preserves that
  publication, a destination turn uses inherited request context, and a reopened
  source proves destination writes preserve the source.
- `restart.recover_session`: a real JSONL-backed assembled turn is closed and
  reopened on a distinct backend. Configured `restart` recovers the exact
  durable entries, leaf, model and usage before a subsequent provider request
  projects that history and publishes the next turn.
- `legacy_import.format3`: a host-selected format-3 JSONL source preserves its
  bytes during open, remints legacy identities, and retains nonzero usage and
  the assistant signature. `attach_import` triggers checked format-4 adoption;
  the persisted header and `v3-import` usage provenance are read from the file.
  A real assembled prompt projects the imported message and extends its parent
  chain. Opening in place remains the explicit upstream copy-semantics boundary.
- `model_switch.next_run`: the first real request sees distinct attachment
  overlay metadata. `switch_model` persists a model configuration event before
  the next run. The next adapter request, readback and durable configuration all
  select the new host model, proving the old overlay was cleared.
- `extension.participation_ordering`: an authenticated Ready grant is consumed
  in provider/echo-tool/hook registration order. Actual peer requests retain
  capability principal, handler, request identity and operation inputs. A
  before-run hook, remote echo protocol progress/settlement, second before-run hook and
  deterministic remote-provider terminal execute through the assembled Harness.
  Event payloads, provider contexts, session publications and close/unsubscription
  observations are compared together. The provider registration supplies the existing admitted zero-tool payload
  schema and exact host-approved echo-history leaf schemas. This row does not
  claim a payload-transform hook witness.
  Its host grant retains an 8,192-byte frame ceiling and a finite 16,384-byte
  decoded budget for the measured 9,963-byte combined DTO charge. The final
  published assistant identity and nonzero usage come from the actual session
  leaf; no host credentials or HTTP are involved. The actual ToolUpdate retains
  the call, tool, lane, run and turn identity of ToolStart/ToolEnd, publishes the
  peer's partial content/details, and occurs between start and settlement.
  The peer installs one bounded pending terminal before enqueueing its real
  progress frame, then validates the matching request and measured 157-byte
  body refund before sending that zero-credit terminal. This is the normal
  ready-sink and funded-credit witness. Separate driver tests cover coalesced
  terminal, pending sink, cancellation and credited bursts; this peer
  coordination does not change runtime terminal policy.
- `lifecycle.builder_replace`: replacement begins while the original provider
  stream is active. The adapter observes cancellation before destination
  dispatch; the old run settles aborted, its operation/pending registers clear,
  its lease releases, and the destination retains its own lease while a real
  subsequent turn completes.

No live provider, network, credential refresh, transport, protocol/schema, host
ownership, or durable-session implementation is added. The only credential
observation is a host-owned boundary: injected credential inputs are retained
without reads. M6-A causal mutation changes the capability input for the
negative row. The M6-B and M6-C causal mutations each remove or alter one required
operation witness, and semantic validation must reject them before exact
fixture comparison. The self-test preserves fixture and manifest bytes and
the exact eighteen-positive/eighteen-negative/zero-pending denominator.

The default build compares seventeen applicable rows and explicitly reports
that `extension.participation_ordering` requires the `extensions` feature. Its
disabled branch produces no extension observation. Both `--features extensions`
and `--all-features` compare eighteen rows, including the real authenticated
extension operation. No default-mode result can substitute for those enabled
comparisons.

Run the default target with:

```text
cargo test --offline --locked -p pi-coding-runtime --test coding_runtime_conformance -- --nocapture
```

Run both enabled targets with:

```text
cargo test --offline --locked -p pi-coding-runtime --features extensions --test coding_runtime_conformance -- --nocapture
cargo test --offline --locked -p pi-coding-runtime --all-features --test coding_runtime_conformance -- --nocapture
```

The ordinary `coding_runtime_causal_corpus_runs_through_node` Cargo test runs the
full Node causal self-test and requires successful exit, the exact 18/18/0
corpus, fixture/manifest byte restoration, and all persistent probe markers.
The unchanged workspace verification commands reach this test automatically.
It complements the three actual Rust-to-Node comparison modes above.

Run the same byte-preserving causal self-test directly with:

```text
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/coding-runtime/causal.mjs
```

`compare-actual.mjs` rejects unknown envelope, outcome, error, observation, and
event fields, including nested payload and message fields, before exact
comparison. Row-specific checks correlate events, provider requests, host
results, clock facts, and public session state. Lifecycle observation fields
are admitted only on their applicable closed row. The causal self-test also
checks all 32 combinations of the four previously accepted M6-A rows and
eight lifecycle-only fields, requiring semantic and same-mutated comparison
to reject each. Four retained nested-message probes remain in place.
The four foundation rows additionally require their own exact input, outcome,
observation and emitted nested object schemas; inherited operation fields and
other foundation rows' fields cannot enter those envelopes. Their prepared
positive operation counts and true readback/availability flags, and the exact
zero-effect preflight contract, are checked independently of fixture equality.
The self-test runs 380 foundation applicability, required-key, type, nested
shape and semantic probes through both semantic and same-mutated comparison.
These supplemental probes preserve the frozen 18/18/0 corpus.
The fixture authority is pinned to
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
