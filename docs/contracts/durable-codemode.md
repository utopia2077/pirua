# Persistent local CodeMode

`pi_durable::codemode` builds a `ToolRegistration` or an `Extension` for the native
`pi_codemode::Sandbox`. Install `create_codemode_extension(CodeModeOptions::new(sandbox))`
in `DurableOptions.registry` before opening Durable. The extension provides the
model-only, replay-safe `codemode` tool and a prompt section listing callable tools.
The host supplies the matching compiled `pi-codemode-worker` path to the Sandbox;
there is one local executor, with no automatic helper installation or PATH lookup.

The tool accepts `{code: string}`. Code is an async JavaScript function body with
`await`, `return`, `text`, `image`, `console`, `exit`, `store`, `load` and `ALL_TOOLS`.
The optional first line `// @options: {"max_output_tokens":10000,"timeout_ms":60000}`
sets its output budget and absolute timeout. Omitted timeout leaves controlled
execution without a deadline. `CodeModeOptions.globals` explicitly installs host
functions; undefined is `None`, JSON null is `Some(serde_json::Value::Null)`.
Discovery globals searchTools/describeTool/describeNamespace use recorded callable
declarations and configured namespaces, including deferred tools. Native declaration
samples display JSON schemas; they do not compile or load TypeScript.

A task owns `pi.codemode.run` and `pi.codemode.journal`, both document version 1 and
never checkpointed before task retirement. Immutable start includes normalized code,
adapter module version 2, clock, seed, deadline, declarations, and detached initial
store. The first actual Started fingerprint is committed before effects/delivery.
The fingerprint includes the actual helper binary digest, engine, OS/architecture,
heap and stack. Recovery requires equality; rebuilding the helper may refuse an
in-flight script. The native QuickJS-NG fingerprint, randomness and script error
stacks are distinct from upstream QuickJS-WASI. Replay stays within one fingerprint.

Each completed live call is journaled before delivery, in arrival order with its
clock and VM ID. Idle counts commit before new tool/global effects can start.
Tool outcomes come from the existing task-scoped `pi.tool.nested-call` record using
stable `c<VM ID>` keys. Global arguments/outcomes and adapter-only raw-argument validation outcomes are
copied into the journal, as described below.
Every held call's old keyed identity is checked before any held effect launches.
Version, fingerprint, deadline, call/count or terminal During divergence decides a
fatal host error outside JavaScript exception handling. A decided result returns
without starting another VM. Nested replay uses the runtime's stored/current policy
intersection; this is not an exactly-once guarantee for unsafe interrupted effects.

The conversation's `pi.codemode.store` is rewindable/asOf, with a checkpoint after
64 retained deltas. Only successful execution applies store writes, in the same
transaction as the first decided result. Failed scripts leave the prior store.
Forks read the selected historical store. `exit()` is successful and retains its
writes. Undefined and JSON null stay distinct in journal globals and VM results.

A host Sandbox shutdown while the Durable invocation remains live decides no script
outcome. The adapter waits for invocation cancellation; normal Durable close and
same-file SQLite reopen with a new Sandbox and the same helper can recover it.
Conversation abort cancels native nested work. Settled/retired invocations cannot
release queued calls, late global outcomes, deliveries or progress. The existing
sandbox monitor owns killing/reaping workers and releasing admission.

Output retains text, console, returns and images. The adapter applies the script's
token budget within runtime byte/line limits, spills full text and writes shown
images under fixed names. The injected ExecutionEnv reserves a temporary file;
the adapter explicitly retains it and commits a derived output directory before
content writes. `write_file` creates the directory, and reruns reuse its committed
path. The existing local PID/counter temporary-file allocator does not provide an
exclusive cross-process collision guarantee. File failures become output labels.
No implicit VM filesystem/network, remote executor, model catalog/classifier or
image generation helper is added. Coding's separate ephemeral adapter is unchanged.

Build and run the real worker tests from the task's prescribed build environment:

```sh
cargo build -p pi-codemode --bin pi-codemode-worker --locked --offline
cargo test -p pi-durable -p pi-codemode --all-features --locked --offline
```

The CodeMode tests use a private test-only observer around actual Sandbox/Run calls,
real Memory/SQLite and offline native provider tools. Frozen source evidence remains
separate: 17 real-worker cases, one document case and three scripted protocol cases.
Native executable equivalents of the three scripted decisions are labeled as such;
they do not claim the source placeholders executed JavaScript.

The native typed nested API accepts object arguments. Undefined and null become
an empty object. Other scalar/array arguments produce an adapter validation error
that JavaScript can catch, without executing a tool or fabricating a nested task.
Its raw name/arguments/outcome occupy the journal's existing local-outcome slot
(`global`), marked explicitly `target: "tool"`; absent target retains normal global
meaning. Replay checks target, name and raw arguments before reusing that committed
validation outcome. Valid nested tool results remain solely in runtime receipts.
This is a bounded native variance from upstream allocating a failed nested ToolTask
for malformed raw arguments; the shared typed API is unchanged.
