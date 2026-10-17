# Standalone Agent

The current Agent implements `packages/agent` from Pi v1.4.0. It is independent
of persistent session storage. The experimental Harness previously documented
at this path has been retired; it is not an implementation of the new Durable API.

`pi-agent-schema` holds detached messages, tools, events and callback types.
`pi-agent-loop` drives requests, source-ordered tool preflight, sequential/parallel
execution, queues, request preparation and turn completion. `pi-agent` exposes the
stateful facade, prompt/continue, subscriptions, cancellation and idle state.
The [crate README](../../crates/pi-agent/README.md) describes its native API.

Event sinks are awaited barriers. Parallel tool completion events may arrive in
completion order while tool-result transcript insertion preserves source order.
Progress admitted before a tool settles is drained before finalization; late
updates are retired. Tool execute duration excludes hooks and delayed progress
acknowledgements. Foreign callbacks run outside internal locks.

Message hooks transform public messages before downstream tool dispatch and
context reuse. Nested `run_tool_call` uses the same argument and before/after-tool
lifecycle. Ordinary tool failures become error results; infrastructure failures
such as a rejected progress barrier propagate as errors. Structured tool result
`null` is distinct from absent structured output.

The optional `proxy` feature supplies an injected HTTP-backed stream. Its native
URL/cancellation and partial-content boundaries are documented in
[Agent coverage](../../crates/pi-agent/upstream-coverage.md). The
[offline example](../../crates/pi-agent/examples/offline_agent.rs) runs a complete
prompt/tool/response sequence without credentials.
