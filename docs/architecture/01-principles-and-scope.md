# Principles and Scope

## One durable authority

The Rust harness is the only authority for entries, registers, usage, lane
state, operation state, queues, configuration snapshots, retry state,
cancellation, recovery, and terminal outcomes.

An extension, provider, UI, façade, search index, telemetry exporter, generated
catalog, or in-memory snapshot MUST NOT become a second owner of those values.

## Pure planning precedes effects

The durable state machine MUST be expressible as pure operations over detached
inputs:

```rust
fn validate_current(current: &HydratedCurrent) -> Result<(), Corruption>;
fn next_action(state: &OperationState, inputs: &PlannerInputs) -> Action;
fn plan_settlement(
    current: &CurrentOperation,
    effect: &EffectPlan,
    output: &EffectOutput,
) -> Result<SettlementPlan, InvariantError>;
```

Clock, randomness, storage, network, provider, tool, hook, timer, filesystem,
process execution, telemetry export, and UI interaction are effects. Their
results re-enter the core as explicit values.

## Inclusion is not coupling

The repository covers the shared AI runtime, the
[owner-selected provider scope](../scope.md#selected-provider-scope), and
the complete `packages/agent` semantics. Product provider coverage follows that
scope; accepted shared adapters remain available without requiring a factory
for every upstream provider. Cargo features and explicit façade imports keep
core imports side-effect free and provider loading explicit.

## Upstream similarity is semantic

The Rust project SHOULD retain upstream public names and module families where
they express stable responsibilities. It MUST preserve observable contracts,
but MAY use different algorithms, HTTP libraries, storage libraries, async
runtimes, and internal data structures.

Every intentional behavior divergence MUST be named in the upstream map with a
rationale and a replacement conformance rule.

## Open identities remain open

API ids, provider ids, custom message roles, tool names, extension ids, and
capability ids are open strings. Known upstream ids may have constants or typed
helpers, but unknown ids MUST survive parsing and serialization.

## Durable values are detached

Durable and wire values MUST NOT contain functions, pointers, file descriptors,
language objects, cyclic graphs, symbols, process ids, sockets, live streams, or
credentials. Persisted values are validated before commit.

## Failure is modeled, not hidden

- Provider and tool failures are in-band effect outcomes.
- Storage commit failure faults the harness.
- External effects are not exactly once.
- Replay is explicit (`safe` or `never`).
- Cancellation is a durable ordering decision plus cooperative process signal.
- An extension response without both a live invocation binding and matching
  durable effect key cannot mutate current state. An older connection generation
  may finish only an invocation still bound to it.

## Compatibility profiles are isolated

Legacy low-level Agent behavior, old session imports, AI compat entrypoints, and
JavaScript extension adaptation are compatibility profiles. Production core
MUST NOT depend on them.

## Host concerns stay outside the harness

CLI argument parsing, TUI components, themes, clipboard, HTML export, package
installation, self-update, and application-specific resource discovery are host
concerns. They can be implemented in Rust later without changing the durable
harness contract.

Extension process launch/termination, transport authentication, OS sandboxing,
environment/network isolation, and credential brokering are privileged host
concerns. The in-scope endpoint accepts authenticated identity, grants, finite
limits, and an ordered transport. Protocol v1 never treats a self-declared
manifest id as identity and never grants core-managed secret access.

## Target observers

The design provides contracts for:

- Rust applications using the harness directly;
- a coding-agent headless runtime;
- built-in Rust provider and tool adapters;
- multi-language extensions;
- session storage implementations;
- remote clients consuming snapshots and progress;
- maintainers importing upstream changes.
