# rust-harness

Rust implementation of the pi agent and AI runtime, designed to follow
`earendil-works/pi-mono` continuously while replacing in-process JavaScript
callbacks with explicit Rust traits and a language-neutral extension protocol.

The canonical active work bundle is:

- [Proposal](docs/scope.md)
- [Design hub](docs/architecture/index.md)
- [Execution plan](docs/verification-plan.md)
- [Machine-readable crate topology](workspace-topology.toml)
- [Pinned upstream ownership map](upstream-map.toml)

The proposal records approved intent. The design set remains the target
technical authority. The execution plan and state files are created and owned by
the long-horizon controller.
