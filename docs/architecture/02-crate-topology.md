# Crate topology

The machine authority is [workspace-topology.toml](../../workspace-topology.toml).
All crates inherit the workspace toolchain, package settings and lints.

| # | Crate | Responsibility |
|---|---|---|
| 1 | `pi-telemetry` | Telemetry contracts and local implementations |
| 2 | `pi-extension-protocol` | Language-neutral framing, grants and host |
| 3 | `pi-ai-core` | AI models, streams, registry and auth contracts |
| 4 | `pi-ai-transport` | Native transport adapters |
| 5 | `pi-ai-providers` | Selected builtin providers and OAuth |
| 6 | `pi-ai` | Public AI facade |
| 7 | `pi-agent-schema` | Detached Agent messages, tools and events |
| 8 | `pi-agent-loop` | Agent execution and proxy stream |
| 9 | `pi-agent` | Standalone Agent facade |
| 10 | `pi-coding-runtime` | Headless Coding SDK and common tools |
| 11 | `pi-sqlite` | SQLite infrastructure |
| 12 | `pi-coding-session` | Session tree records and current storage |
| 13 | `pi-codemode` | Local JavaScript worker |
| 14 | `pi-mcp` | Standalone MCP client |

## Dependency graph

| Consumer | Required internal dependencies | Optional internal dependencies |
|---|---|---|
| `pi-telemetry` | — | — |
| `pi-extension-protocol` | — | — |
| `pi-ai-core` | `pi-telemetry` | — |
| `pi-ai-transport` | `pi-ai-core` | — |
| `pi-ai-providers` | `pi-ai-core`, `pi-ai-transport` | — |
| `pi-ai` | `pi-ai-core` | `pi-ai-providers` |
| `pi-agent-schema` | `pi-ai-core` | — |
| `pi-agent-loop` | `pi-agent-schema`, `pi-ai-core` | `pi-ai-transport` |
| `pi-agent` | `pi-agent-schema`, `pi-agent-loop`, `pi-ai-core` | — |
| `pi-coding-runtime` | `pi-agent-schema`, `pi-ai`, `pi-agent`, `pi-coding-session`, `pi-extension-protocol`, `pi-mcp`, `pi-codemode` | — |
| `pi-sqlite` | — | — |
| `pi-coding-session` | `pi-sqlite` | — |
| `pi-codemode` | — | — |
| `pi-mcp` | — | — |

## Literal schema

The checker rejects hidden local packages, noncanonical dependency paths,
cycles, undeclared edges and unexpected optional-feature activation. Optional
selectors are listed explicitly in the machine map. There are no authorized
separate allocator test packages or development-only internal dependency edges.
