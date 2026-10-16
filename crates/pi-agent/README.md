# Agent

`pi-agent` is the transient Agent runtime aligned with Pi v1.4.0. It owns the
current transcript, provider turns, executable tools, steering and follow-up
queues. Supply a `Model` and `StreamFn` to `AgentOptions::new`; the stream function
can use an AI registry, an explicitly configured proxy, or an offline test model.
Enable the `proxy` feature for `ProxyOptions`, `stream_proxy` and `proxy_stream_fn`.
Provider catalogs and persistent session stores are separate owners.

```sh
cargo run -p pi-agent --example offline_agent --locked
```

The example completes a prompt, invokes a local tool and receives a final answer
without credentials or network access. `prompt_text` also accepts image inputs;
`prompt` accepts typed or custom messages. Custom messages are excluded by the
default `convert_to_llm` callback. A host can supply its own conversion.

## Runs and observations

`state()` returns a detached snapshot. Model, thinking level, executable tools
and transcript have explicit mutation methods. System prompt and tool declarations
are replayed from transcript system messages; append a system message to change
the prompt. Changing executable tools announces the corresponding declaration
delta before the next request. `configuration()` and `set_configuration()` control
future runs; an active run retains its original configuration snapshot.

`subscribe` returns an identifier for `unsubscribe`. Listeners run in registration
order and are awaited. `agent_end` is the last event, and the Agent stays busy
until its listeners settle. `wait_for_idle` observes that same boundary. `abort`
cancels the active signal; dropping the prompt future also cancels and releases
native run ownership. Successful completion does not cancel the completed signal.
A listener that captures an Agent clone should unsubscribe when finished to avoid
retaining itself through the shared owner.

Steering messages are polled after the current tool batch. Follow-up messages run
when the loop would otherwise stop. Each queue supports all-at-once or one-at-a-time
draining. `finish_turn`, `prepare_request` and `prepare_next_turn` control explicit
continuation and future request state. Error and aborted provider responses remain
hard exits. `reset` preserves the current system baseline and rejects an active run.

## Tools and native callback values

`AgentTool` separates a serializable declaration from the execution callback.
Arguments are prepared and validated before the before-tool hook. An optional
`BeforeToolCallResult.args` explicitly represents changing those already validated
arguments, without a second validation pass. Before/after hooks can block or replace
results. `run_tool_call` exposes the same pipeline for direct and nested calls
without adding Agent lifecycle events or transcript messages. Tool-domain failures
remain outcomes; a rejected progress acknowledgement propagates as an error before
after-tool hooks or successful completion publication.

Parallel execution preflights calls in source order, publishes completion events
in completion order, and appends tool-result messages in source order after the
batch. A per-tool sequential override serializes the batch. Progress callbacks
begin publication synchronously and their acknowledgements delay tool completion;
callbacks retained beyond execution settlement are ignored. `duration_ms` measures
only execution, excluding before/after hooks and pending progress acknowledgements.
An optional monotonic clock permits deterministic host tests.

`MessageHooks.start/end` represent message transformations explicitly. Replacements
must preserve the original role. Their final values feed event delivery, transcript
state, subsequent provider context and assistant tool dispatch. This allows Coding
extensions to transform a message without requiring JavaScript object identity.
Callbacks receive detached values; event subscribers are observers, not mutable
references into Agent state.

The asynchronous sink-based `run_agent_loop` and `run_agent_loop_continue` are
re-exported from `pi-agent-loop`. Rust uses an explicit stream function and awaited
sink rather than a process-global default stream registry or a JavaScript event
iterator. See [upstream-coverage.md](upstream-coverage.md) for source authority,
verification and native representation boundaries.
