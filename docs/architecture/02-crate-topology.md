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
| 15 | `pi-acp-server` | ACP v2 application host for native Coding sessions |

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
| `pi-acp-server` | `pi-agent`, `pi-ai`, `pi-ai-providers`, `pi-ai-transport`, `pi-coding-runtime`, `pi-coding-session`, `pi-sqlite` | — |

The TypeScript workspaces `packages/pi-tui` and `packages/pi-tui-client` retain
the original terminal renderer and interactive frontend. They connect to the
application host over stdio and are outside the Rust crate graph. See the
[TUI integration](../design/tui-acp-v2.md) for the protocol and ownership boundary.

## Literal schema

The checker rejects hidden local packages, noncanonical dependency paths,
cycles, undeclared edges and unexpected optional-feature activation. Optional
selectors are listed explicitly in the machine map. There are no authorized
separate allocator test packages or development-only internal dependency edges.

`workspace_default = true` marks an optional library edge explicitly selected by
the default workspace application build. The native ACP host enables the selected
providers, so the `pi-ai` → `pi-ai-providers` edge is present in that build. The
`pi-ai` library itself still has empty default features. The proxy transport edge
remains absent until its feature is selected. Both unexpected activation and
missing declared activation are rejected; all-feature edges remain exact.
