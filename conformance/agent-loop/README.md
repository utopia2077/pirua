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
witness. It does not mean that a Rust-captured trace has been run through the
Node comparator or that full Agent-level parity has been differentially
executed. A denominator row that combines a witnessed sub-behavior with an
unwitnessed complete Agent path is therefore `test-unwitnessed`; the local
witness remains in the focused Rust target. `non-applicable` is reserved for
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
