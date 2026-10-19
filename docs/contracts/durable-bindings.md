# Durable ordinary bindings

`pi_durable::bindings` binds ordinary operations to the native Durable runtime.
The runtime performs argument preparation/validation, before-tool hooks and a
second validation, then commits final arguments and replay policy before dispatch.
It owns output limits, final fallbacks, after-tool hooks, structured-output
interpretation, usage, controls, task settlement and recovery. None of these
bindings constructs Agent or CodingSession.

`coding::bind_coding_tool(operation, context)` calls the existing CodingTool
operation directly. The invocation's host-created environment takes precedence
when configured; the explicit context is the fallback. A linked native
cancellation token stops the operation. Synchronous operation output snapshots
enter a bounded 32-item bridge; overlapping text becomes incremental runtime
output and details enters the same committed progress path. The bridge drains
before returning the explicit final output, structured data, details, usage and
termination control. Queue exhaustion is observable and cancels local operation
work. Retained callbacks cannot publish after execute ends. A shared operation
may already omit/spill output; the adapter cannot reconstruct bytes it never
reported. Tool replay defaults to unsafe and remains an explicit host decision.

`mcp::McpToolAttachment::attach` accepts a host-owned initialized McpClient and
discovers tools into one uniquely named native Extension. It does not connect,
authenticate, create another client, or close the supplied client. Hosts observe
client notifications and retire/reattach after discovery changes or reconnects.
Captured callbacks are bound to the actual runtime and conversation, reject after
retirement, and propagate invocation cancellation to the existing MCP request.
Results keep model text/images separate from the complete MCP result for programs;
unknown MCP fields survive, explicit null structuredContent survives, and only
its top-level `_meta` is omitted. Text/image resources are projected, audio is
represented by a notice, and binary resources remain in structured output unless
the host explicitly supplies a saver. Runtime output limits remain authoritative.

`extensions::DurableExtensionAttachment::attach` accepts an already Ready
HostConnection and explicit host-selected registrations. Use
`extensions::grant_scope(&durable, conversation_id)` in host profile approvals.
It installs a uniquely named extension into the runtime's existing registry.
Attachment close/drop retires captured callbacks and its native connection;
callbacks retain weak attachment/runtime handles. The host must select the
extension for its bound conversation; copying its callbacks to another runtime
or conversation grants no authority. Phase registry snapshots remain immutable.

The native wire profile is exactly `durable.v1`, kind `durable`, protocol 1.1:

| Method | Mode | Terminal meaning |
| --- | --- | --- |
| before_request | unary | Optional replacement messages for this attempt only |
| after_response | unary | Empty observation acknowledgement |
| on_yield | unary | Optional continuation user content |
| after_tools | unary | Empty observation acknowledgement |
| before_tool | unary | Optional replacement arguments/block; block wins |
| after_tool | unary | Optional complete execution-result replacement |
| before_compact | unary | Optional decline/summary decision |
| tool.call | stream | Complete native Durable ToolExecutionResult |

All methods are core-to-extension. Hook capability names equal their method;
tool capability names are `tool:<native-tool-name>`. Tool ReplaySafe maps to
native safe and ReplayNever to unsafe; idempotent-handler does not independently
authorize Durable replay. Principal, handler, profile digest, conversation scope,
execution mode and current connection generation come from the authenticated
host grant. Manifests cannot install tools, definitions, schemas or permissions.
There are no peer-to-host document, memo, task, model, environment, transaction,
clock or nested-dispatch actions. Coding session.snapshot remains Coding-only.

Payloads use existing bounded SchemaBoundValue framing and root names
`durable.input:<method>`, `durable.output:<method>`, and
`durable.progress:tool.call`. The bundled descriptor specifies closed wrappers,
result channels and operation variants. Native AI codecs additionally validate
full current message/content/usage DTOs, preserving provider replay metadata.
No Coding or historical Harness payload conversion is performed. Unknown
methods, stages, object fields and ungranted bindings reject before mutation.
Details and structuredOutput preserve absence versus explicit null. Domain
structured-schema failures go through the runtime finalizer, preserving its
model-versus-nested behavior; they are not framing failures.

Enabled progress consists of output text/bytes, details replacement and diagnostic
append operations. Byte chunks use canonical base64 and decoded-size limits.
The runtime owns its one ordered UTF-8 decoder and output accumulator. Tail
skips require the host's output window. Window minIntervalMs is a finite,
nonnegative JSON number: fractional values are preserved exactly. Byte, line
and rate counts are safe unsigned integers. Disabled nested progress emits no
frames and still returns complete terminal results.

`HostConnection::request_with_progress_ack` shares the existing request queues,
IDs, framing, terminal reservation and cancellation path. It awaits
`ToolExecutionApi::report_progress` through the covering persistent Session
commit before returning that frame's stream credit. A terminal waits behind
prior admitted progress. Cancellation seals callback admission and lets an
already admitted commit settle; it never starts buffered callbacks during
cleanup. Persistent failure is causal and cannot become successful tool data.
Remote duration includes ordered acknowledgement delay; peers supply no clock,
lease, registry or duration. Existing Coding synchronous Release/Retain behavior
is unchanged, and Coding continues to use Retain.
