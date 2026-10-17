# Coding tool adapter observations

Authority: Pi `v1.4.0`, commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`.

`source.mjs` reads exact release objects with `git show`, records their SHA-256
digests, strips import declarations and TypeScript types, and evaluates the
release's original function bodies in a Node VM. Dependency injection supplies
Node primitives, a deterministic output saver and the unused TypeBox schema
constructors. It does not replace the observed algorithms. No upstream working
tree or `node_modules` is imported.

The 19 cases cover MCP tool naming and collision suffixes, relocated result
schema references, model and script content projection, CodeMode source options,
text/console grouping, structured/error/null script values and BM25 discovery.
Native metadata declaration strings, process timing, engine-specific stacks and
raw-source provider grammar are outside these exact-value observations.

To verify the checked-in observations:

```sh
node conformance/coding-tool-adapters-v1.4.0/source.mjs /path/to/pi-mono --check
```

The dedicated Rust `mcp_adapter` and `codemode_adapter` test targets consume all
19 observations. CodeMode execution cases use the actual local worker. Build
`pi-codemode-worker` first, set `CARGO_TARGET_DIR` to the task's normal shared
build directory, and run the tests. `PI_CODEMODE_TEST_WORKER` can explicitly
select a packaged matching worker for consumer testing.
