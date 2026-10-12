# Coding runtime conformance (S15-M6-A)

This checked-in intermediate corpus is the first serial M6 unit. It contains
exactly the four M6-A rows and four one-change causal negatives. The remaining
14 frozen S15 rows are retained as `pendingRows` in `manifest.json`; M6-B/C/D
will replace this intermediate corpus with the final 18-row denominator.

Every M6-A row runs a fresh assembled `CodingHarnessBuilder`/`AgentHarness`
operation in `crates/pi-coding-runtime/tests/coding_runtime_conformance.rs`.
The bridge installs a deterministic local `ApiAdapter`, subscribes to real
Harness event tags, prompts the assembled lane, waits for terminal settlement,
and reads the session entry projection. The four closed side-effect fields are
`provider_calls`, `tool_invocations`, `session_publication_events`, and
`durable_entries`; event count and model/availability details are additional
observations.

The rows are:

- `model.credentials_catalog`: detached overlay precedence, provider-scoped
  availability transition, fuzzy selection, and the host credential boundary.
  The overlay keeps the host provider/model identity but uses distinct
  validated `name` and public `base_url` metadata; the assembled Harness
  readback compares those fields so removing the override fails the witness.
- `prompt.system_resources`: detached resources passed through assembled
  prompt admission and observed through the real operation/session path.
- `prompt.settings_snapshot`: detached settings passed through the builder and
  observed through the real operation/session path.
- `capability.required_prestart_failure`: empty composition is admitted to the
  builder preflight, which returns exactly
  `CodingRuntimeError::MissingCapability { capability: "models.resolve" }`;
  provider calls, tool invocations, session publication events, and durable
  entries are all zero before the positive inputs are restored.

No live provider, network, credential refresh, transport, protocol/schema, host
ownership, or durable-session implementation is added. The only credential
observation is a host-owned boundary: injected credential inputs are retained
without reads. M6-A causal mutation changes the capability input for the
negative row and increments one durable side-effect count for the other rows.

Run the intermediate target with:

```text
cargo test --offline --locked -p pi-coding-runtime --test coding_runtime_conformance
```

Run the byte-preserving causal self-test with:

```text
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/coding-runtime/causal.mjs
```

`compare-actual.mjs` rejects unknown envelope, outcome, error, observation, and
event fields before exact comparison. The fixture authority is pinned to
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
