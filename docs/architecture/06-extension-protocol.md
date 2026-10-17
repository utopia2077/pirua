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
