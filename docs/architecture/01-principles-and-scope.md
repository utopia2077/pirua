# Principles and scope

Follow the selected upstream contracts through observable behavior. Native Rust
ownership and typed APIs can differ from JavaScript, but differences must be
explicit and tested where they affect callers. Source mappings identify the
responsible owner; they do not imply a byte-for-byte port of every source file.

State has one authority at each layer. Agent owns a run's transient state.
CodingSession coordinates that Agent with a canonical SessionManager. The session
store owns persistent records; projections are derived. Adapters use public tool
and hook seams rather than mutating stores or transcript state independently.

Foreign callbacks receive detached values. No internal state lock should be held
over a callback, network operation or subprocess wait. Cancellation, drop and
terminal events must have explicit ownership; a callback that performs an external
effect remains responsible for honoring its cancellation signal.

Providers, credentials, transports, tools and extension grants are explicit host
capabilities. Construction does not discover accounts or contact a service.
Runtime-independent tool operations and neutral MCP/CodeMode clients can be reused
by the later Durable implementation without a temporary Harness compatibility
layer. The precise included products are in [scope](../scope.md).
