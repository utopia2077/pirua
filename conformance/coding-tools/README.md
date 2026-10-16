# Pi v1.4.0 common coding capabilities

Authoritative source: `4a080252f06caf63a525d7873c5d94d3c23887e0` (`v1.4.0`).
`source.mjs` reads exact Git objects, strips TypeScript types and executes the
upstream functions in a detached VM. The checkout's current branch is irrelevant.
The edit oracle verifies the installed upstream `diff` dependency is exactly
`8.0.4`, matching the target package manifest.

Run from the repository root:

```sh
node conformance/coding-tools/source.mjs /absolute/path/to/pi-mono --check
cargo test -p pi-coding-runtime --lib --all-features --locked
cargo test -p pi-coding-runtime --all-features --locked
cargo clippy -p pi-coding-runtime --all-targets --all-features --locked -- -D warnings
```

A source-fixture refresh omits `--check`. It must be reviewed with the source
identity and expected behavior; a passing refresh is not evidence by itself.
`trace.json` records SHA-256 fingerprints and captured observations. Rust tests
execute production entry points and compare their results with these captured
values. The oracle needs Node with `stripTypeScriptTypes` and the target's installed
`diff` package. Rust tests need neither Node nor provider/network access.

| Source at the pinned commit | Rust surface | Evidence |
| --- | --- | --- |
| `core/tools/truncate.ts` | `tools::truncate_head`, `truncate_tail`, tool details | Empty/newline, line cap, UTF-8 byte cap and partial-tail rows |
| `core/tools/edit.ts`, `edit-diff.ts` | Edit preparation and operation, numbered diff/patch | Disjoint edits, touched-line fuzzy normalization, duplicate/empty/overlap/no-change errors, normalized newlines, separate hunks and final-newline markers |
| `core/tools/file-mutation-queue.ts` | Shared write/edit queue | Real canonical alias, in-flight cancellation, queued cancellation and independent-file cases |
| `core/tools/read.ts`, `path-utils.ts`; `utils/paths.ts` | Read/environment paths and vision inputs | Actual `@`, Unicode spaces, dot segments, file URLs, NFD/screenshot variants, host image processing, local BMP conversion, image resizing, model profiles and omission notes |
| `core/tools/bash.ts`, `powershell.ts`, `output-accumulator.ts` | Shell environment operation | Actual process output order, cancellation/child effect prevention, timeout, inherited-pipe idleness and successful background work, full spill and structured output bound; injected PowerShell contract |
| `core/tools/ls.ts`, `find.ts`, `grep.ts` | Local listings/search | Actual filesystem ignore/glob/context/byte/count cases, parent and nested-repository rules, listings above the read line cap, exact continuation/details omission, negated grep glob and zero limits |
| `core/skills.ts`, `prompt-templates.ts`, `resource-loader.ts` | Detached resources and local loader | Captured skill XML/reader hints and argument substitution; real discovery, collision/frontmatter/context/worktree cases |
| `core/system-prompt.ts` | Prompt state/sections/diff | Captured default/hidden/custom/forced/invalid sections, ordered section replacement and null removal |
| `core/settings-defaults.ts`, `settings-manager.ts` | Detached settings/merge | Real global/project/runtime merge, source diagnostics and selected headless defaults |
| `core/model-resolver.ts` | Pure model resolution and composition | Captured exact-before-suffix, named/raw-provider ties, fuzzy alias/latest, lenient warnings, ambiguity and not-found targets; captured preferred/fallback model order; overlay/no-credential-side-effect regressions |

The prompt oracle applies one explicit owner-scope projection: its documentation
line refers to CodeMode scripts and omits the upstream classifier/image-generation
model wording. Those execution capabilities are owner-excluded. Vision inputs
remain tested. Four builtin provider entrances are asserted separately; configured
provider IDs remain open.

The pre-upgrade selected baseline consisted of 14 tool, 8 model-composition,
9 model-default and 3 settings tests. Their relevant effect/selection assertions
remain in the candidate, with explicit source-authoritative changes: four builtins,
open configured IDs, release reference syntax, ranked fuzzy selection, detached
requested thinking, nonzero shell result shape, touched fuzzy-line normalization,
numbered diff metadata, and valid Git reinclude rules. Old Harness lifecycle,
legacy JSONL and extension assembly tests are owned by the Coding integration
slice and are not claims of this common-module slice.

Native Windows/PowerShell execution is not exercised on the Linux fixture host.
Image encoding is a Rust implementation of the target input-processing policy;
byte identity with the upstream Photon encoder is not claimed. The oracle and
local tests are offline and never call a paid/live provider.
