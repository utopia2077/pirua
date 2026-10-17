# Conformance and host boundaries

The [verification guide](../verification-plan.md) separates source expectations,
native behavior tests, structural guards and execution receipts. Tests use synthetic
credentials, actual local processes and loopback peers; no paid provider API is
contacted. Real service availability and account access are outside this evidence.

Host callbacks own their side effects and cancellation cooperation. Killing a
CodeMode worker cannot undo a previously completed tool effect. MCP reconnects do
not replay tool calls. Session persistence does not promise exactly-once external
effects. The later Durable runtime will define its own recovery/publication rules.

The local CodeMode worker exposes only explicit tool/JSON capabilities, and runs
with bounded resources and a cleared environment. It does not provide an operating
system sandbox against native VM vulnerabilities. The host supplies any required
OS isolation and owns the worker binary it launches.

Extension peers require host-established authentication and grants. Tokens,
provider auth services, storage, filesystem access and process spawning are explicit
capabilities. Diagnostics must preserve useful classification while avoiding raw
credential exposure. [Publication checks](../development.md#publication-checks)
provide bounded scanning of repository content and history.
