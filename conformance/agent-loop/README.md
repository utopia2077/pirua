# Legacy Agent loop conformance

This directory is the S14-C source-backed corpus for the transient legacy
Agent/agent-loop surface. Its upstream authority is pinned to
`pi-mono@853a80d26c90a14c1886f0ebb8ffaae133ca2185`; every positive row in
`manifest.json` carries source references to that checkout and to the current
Rust witness or gap report.

Run the focused Rust target with this exact command:

```text
cargo test --offline --locked -p pi-agent --test agent_conformance
```

The bounded actual-observation bridge runs fresh Rust Agent calls and sends
their projected observations over stdin to the Node comparator:

```text
cargo test --offline --locked -p pi-agent --test agent_observation_bridge
```

The bridge covers `single_turn.stop`, `single_turn.transform_context`,
`tool.parallel_completion_source`, `tool.length_truncated`, and
`continue.existing_context`. Each fresh Rust run retains the exact prompt
vector and `LegacyContext` clone passed to the loop, then projects their
message roles/content, context tool names and per-tool execution modes, and
the captured first provider stop reason. The transform marker comes from the
real transform callback wrapper. The bridge also captures every lifecycle
event, the complete applicable outcome (`kind`, `stopReason`, `errorCategory`,
`errorMessage`, `terminal`, and `executed` for tool rows), and test-owned
side-channel observations for provider call count, transform/converter order,
tool completion/source order, truncation no-execution, and continuation input
replay. The target does not read `fixtures.json` to construct actual rows or
compare a fixture to itself.

The bridge deliberately does not manufacture `queue_admit`, `queue_deliver`,
`abort_signal`, or `provider_error` as `AgentEvent` values. Queue, active-stream
abort, and provider-error rows remain explicit skipped/test-unwitnessed
boundaries until a separate process-local observation port can capture them.
The skipped rows are `queue.steer_after_tools`, `queue.follow_up_continue`,
`abort.active_stream`, and `provider.throw`; they remain in the checked-in
12-row corpus and are not counted as bridge passes.
`compare-actual.mjs` rejects those pseudo-event types in an actual observation
and fails closed on unknown envelope, event, outcome, or observation fields.

The optional Node causal self-test is:

```text
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/agent-loop/causal.mjs
```

`fixtures.json` contains closed trace envelopes. The envelopes deliberately
remain a conformance-owned DTO: they do not serialize `AgentEvent` into a
production durable type and they do not reuse Harness v3 `HarnessEvent`,
`RunOutcome`, inbox, register, ledger, or restart state. `comparator.mjs`
validates the envelope and event field allowlists before exact comparison. It
preserves raw observations, applies only row-declared exact normalizers on
clones, checks lifecycle ordering, message pairing, tool start/update/end
pairing, completion-order versus source-order projections, queue kind and
boundary, abort boundary, provider error category, and first-failure paths.

The 12 positive IDs are exactly the frozen S14-C minimum corpus:

- `single_turn.stop`
- `single_turn.transform_context`
- `tool.single_call`
- `tool.parallel_completion_source`
- `tool.sequential_override`
- `tool.length_truncated`
- `tool.block_terminate`
- `queue.steer_after_tools`
- `queue.follow_up_continue`
- `abort.active_stream`
- `provider.throw`
- `continue.existing_context`

Every positive has exactly one named local causal negative. The Node helper
mutates an in-memory clone, requires a comparator mismatch, restores the
original fixture bytes in memory, requires the positive comparison again, and
verifies that the checked-in fixture bytes were never changed. The mutation
reasons cover event order, tool pairing, queue boundary, abort boundary, and
provider error category.

Status is explicit. `source-confirmed` means the cited upstream behavior and
the specific bounded local behavior named by the row have a current Rust
witness. It remains local/source evidence for that positive row; the
Rust-to-Node actual-observation bridge independently covers exactly the five
rows named above. A denominator row that combines a witnessed sub-behavior
with an unwitnessed complete Agent path is therefore `test-unwitnessed`; the
local witness remains in the focused Rust target. `non-applicable` is reserved for
the explicit S14 boundary. The separate U1-U14 denominator in `manifest.json`
accounts for all frozen behavior, including proxy and low-level
stream/iterator rows; proxy and stream parity are not silently omitted.
Harness v3 durable state and coding-runtime composition remain outside this
legacy suite and are marked in the denominator as non-applicable or
test-unwitnessed according to the frozen contract.

The current Rust tests exercise only accepted low-level/public APIs with
witnesses: lifecycle, transform-before-convert, tool batch ordering,
truncation refusal, steering boundary, provider throw terminalization,
continuation, and the public façade's process-local state/queue behavior.
The Node causal command is a corpus/comparator self-test: it compares each
fixture with itself and with one mutated clone, and never runs Rust. The
focused Rust target separately checks local witnesses and manifest
bookkeeping; it does not feed an implementation-generated trace into
`compareTrace`. Rows marked `test-unwitnessed` or `non-applicable` remain
explicit boundary records. No live provider or network access is used.

The actual-observation bridge is a separate Rust-to-Node check: the Rust test
executes deterministic provider/tool callbacks, captures the fresh event list
and message values, and invokes `/usr/bin/node` (with `PATH=/usr/bin:/bin` and
`NODE_NO_WARNINGS=1`) for each covered case. The Node driver loads the matching
checked-in fixture and calls `compareObservation`; it compares the declared
input, projected messages and content, every projected AgentEvent field, the
exact declared outcome field set and values, and every declared observation
field while retaining the full fixture validation/invariant checks. The
length-truncation tool-result text is the exact source-backed Rust message,
including the `echo` tool name. Message updates retain an existing lifecycle
identity, and tool progress retains actual textual progress; unsupported
side-channel pseudo-events remain rejected from actual AgentEvent projections.
