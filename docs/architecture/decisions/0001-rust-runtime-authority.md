# ADR-0001: Rust is the only durable runtime authority

## Context

The project owner requires a complete Rust Harness implementation with
extensions implemented in multiple languages. Preserving JavaScript callbacks as
the core boundary would keep state and lifecycle ownership split across runtimes.

## Decision

Rust owns durable Agent/Harness/Session state, planning, effect intent,
settlement, cancellation, retry, recovery, and terminal outcomes. Foreign
languages provide explicit effects through a protocol and cannot directly
mutate core state.

## Rationale

This removes shared object identity and callback reentrancy from the durable
contract, gives crash recovery one source of truth, and permits independent
extension implementations.

## Consequences

- Extension APIs become versioned request/result contracts.
- In-place mutation becomes explicit patching.
- UI component objects remain host-local.
- External effects remain potentially repeated; exactly-once is not promised.
- A language host can provide partial source compatibility for existing JS
  extensions without becoming core authority.

## Source

Accepted by the project owner in the design conversation on 2026-09-01.
