# Coding session source observations

These fixtures bind the Rust headless CodingSession core to Pi v1.4.0 commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`. `source.mjs` reads immutable Git objects
from a checkout containing that commit; the checkout's current branch and installed
JavaScript packages are irrelevant. It uses Node's bundled TypeScript type stripper,
so no `node_modules`, provider credentials, provider requests, or package installation
are required. Use a Node release that provides `node:module.stripTypeScriptTypes`.

Reproduce the committed expected observations with:

```sh
node conformance/coding-session-runtime-v1.4.0/source.mjs /path/to/pi-mono --check
cargo test -p pi-coding-runtime --test session_source --locked
```

`cases.json` describes inputs. `trace.json` records the upstream commit, SHA-256
of each complete source object, and independently executed source outputs. Omitting
`--check` regenerates that trace; regeneration should be reviewed like a test change.
The Rust test consumes the committed trace and does not calculate expected values
from the implementation under test.

The 39 cases cover UTF-16 text/image estimates, Coding-to-provider message
conversion, assistant usage and trailing-message estimates, stale usage after
context edits, compaction cuts and split turns, recovery omissions, branch summary
budgets/file tracking, conversation serialization, summary failure classification,
cache lifetimes/replay eligibility, virtual selection restoration and scoped router
state, and streamed shell ANSI/binary/UTF-8 sanitation. Source entries use ISO timestamps during source execution and Unix
milliseconds in Rust, matching the accepted native session representation.

The runner evaluates complete stripped source modules in isolated VM contexts.
Imports are removed, and only dependencies needed by the exercised pure functions
are supplied: the pinned message/projection/file helpers, an unused provider-env
lookup returning undefined, and the thinking-level vocabulary. Shell fixtures also supply a deterministic in-memory
process operation and prohibit spill-file creation for these small source cases. It does not emulate
the AgentSession class, provider streaming, JavaScript timers, extension loading, or
filesystem storage. Session lifecycle/event ordering, asynchronous failures,
transaction admission, actual Agent tools, SQLite save/reopen/fork, cancellation,
queues, routed requests, cache refreshes, and compaction are exercised separately by
`tests/session_runtime.rs`, `tests/bash_execution.rs`, and the offline `headless_session` example. MCP, CodeMode,
and cross-language extension adapters have their own source fixtures and runtime
suites. These observations are evidence for the covered functions, not a claim that
the entire upstream package is executed by this runner.
