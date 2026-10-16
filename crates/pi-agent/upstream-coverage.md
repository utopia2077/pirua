# Agent source and behavior coverage

Authority: [`earendil-works/pi` v1.4.0](https://github.com/earendil-works/pi/tree/v1.4.0/packages/agent/src),
commit `4a080252f06caf63a525d7873c5d94d3c23887e0`. The public Rust split is
`pi-agent-schema` for detached types and tool callbacks, `pi-agent-loop` for run
and tool execution, and `pi-agent` for mutable session-local state and queues.
None depends on the retired Harness or durable session storage.

| Source responsibility | Rust owner | Evidence |
| --- | --- | --- |
| `types.ts`: messages, tools, events, hooks and request boundaries | `pi-agent-schema`, `pi-agent-loop::AgentLoopConfig` | Typed serialization and all 26 complete source scenario comparisons |
| `agent-loop.ts`: start/continue, declarations, request preparation, stream delivery and scheduling | `pi-agent-loop` | `upstream_v140` preparation, continuation, hard-exit, stream and transcript cases |
| `agent-loop.ts` tool pipeline: validation, hooks, execution, progress, timing and finalized outcomes | `pi-agent-loop::run_tool_call` and internal batch executor | `upstream_v140` tool order, validation, progress, structured content, truncation, termination, duration and abort cases |
| `agent.ts`: state, queues, subscription and active-run ownership | `pi-agent::Agent` | Source queue/idle comparisons; native dropped-future and configuration tests |
| `proxy.ts`: remote stream protocol | Optional `pi-agent-loop::proxy` feature | Separate proxy tests and source observations in `conformance/agent-proxy-v1.4.0` |
| `stream-fn.ts`: host stream selection | Required `StreamFn` constructor parameter | Offline example and injected stream cases; no global compatibility fallback |
| Coding message replacement and nested metadata | Explicit `MessageHooks` | Native message transformation test checks actual tool dispatch, events, state and next provider context |

The complete 26-case source oracle and its provenance live in
[conformance/agent-v1.4.0](../../conformance/agent-v1.4.0/README.md).
The Rust comparison is `tests/upstream_v140.rs`; independent source execution
produces expectations without importing Pirua. Native tests in
`tests/native_lifecycle.rs` cover Rust future ownership, request configuration,
late updates, the numeric execute-time boundary and detached message replacement.
The offline example exercises a complete prompt/tool/response flow.

## Representation boundaries

The comparator retains all semantic arrays, state, request values and event order.
Only generated clocks and explicitly recorded host representation differences are
normalized. Synthetic oracle models use `https://oracle.invalid` as their native
nonempty endpoint, mapped back only for those exact fixture models; no network is
used. Native schema validation checks tool name, offending path/keyword, original
arguments, error state and absence of execution/hooks, while allowing the native
validator's diagnostic English to differ from TypeBox. Malformed tool-argument
messages remain exact. `AgentError::Busy` represents the three upstream
operation-specific busy errors. Callback handles and cancellation tokens are
compared by their registered presence and observable behavior, not pointer identity.

The target allows JavaScript callbacks to mutate objects. Native before-tool and
message transformation results make those updates explicit and role checked.
They are included in the current execution and transcript, not deferred to storage.
State snapshots and event listeners do not expose mutable object aliases. The
optional monotonic clock is native host capability injection, not serialized data.

Old schema/session/Harness policies, durable operations, legacy migrations,
search adapters and SQLite Harness façade tests are removed from these three
crates. They exercised the retired runtime and are not evidence for this Agent.
Ordinary Coding storage has its own owner; the separately upgraded Durable runtime
has its own acceptance. UI, global stream compatibility and a proxy server are not
implemented by the transient Agent.

These checks use controlled providers, tools and local transports. They do not
establish live service availability or exhaustive scheduling across all hosts.
