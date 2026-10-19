# Language-neutral extensions

`pi-extension-protocol` defines schema-bound framing, endpoints, authentication
and grants. A host establishes identity and an ordered transport, then grants
specific capabilities and methods. Peer messages cannot create their own
credentials or grant themselves additional access.

The current Coding integration uses a distinct `coding.v1` profile with closed
hook and tool DTOs. It validates native messages, roles, arguments, boundary
drafts and results before applying them through public CodingSession APIs.
Historical experimental Harness profile fixtures are protocol provenance; they
are not granted by the current Coding adapter.

One connection actor owns request correlation, progress credit, first-terminal
handling, cancellation and retirement. Native validation and foreign callbacks
run outside protocol-state locks. Coding owns ordered hook composition, remote
tool registration and session-scoped actions. The neutral protocol host does not
own Agent scheduling, storage or persistent effect identities.

The host owns channel authentication, process/network policy and endpoint
lifecycle. The workspace does not load JavaScript/Jiti extensions or silently
translate old Harness payloads. Public compatibility guarantees for external
releases are separate from this pre-release implementation.

The [current Coding profile](current-coding-extensions.md) specifies the exact
method inventory, projection roots, handle lifetime and finite progress budget.

The [native Durable profile](../contracts/durable-bindings.md) is `durable.v1`,
kind `durable`, at protocol 1.1. It carries seven unary hooks and streaming
`tool.call`, bound by authenticated grants to the actual runtime, conversation
and current connection generation. Callbacks retain weak attachment/runtime
custody. Stream credit follows the covering persistent progress commit; terminal
delivery waits behind earlier admitted progress. Coding continues to use its
existing Retain policy. Durable peers receive no storage, task, model or nested
dispatch RPCs. This is a native composition and makes no upstream JavaScript
extension or wire-parity claim.
