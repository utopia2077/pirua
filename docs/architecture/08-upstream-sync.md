# Upstream Synchronization

## Objective and truthful current claim

The repository follows `pi-mono` by semantic authority, not source-language or
file-layout imitation. Every watched path in the pinned baseline is classified;
every included semantic surface has one primary Rust owner, explicit
collaborators, an accepted authority class, synchronization dimensions,
conformance seams, and lifecycle metadata.

The pinned baseline is exhaustively classified. The local
[comparison and review tool](../upstream-drift.md) also extracts arbitrary
old/new Git snapshots, reports changes and uncertainty, validates explicit
intake and acknowledgements, and emits a guarded candidate-map patch. The
complete verification entry runs baseline comparison and assembly consistency.
Applying a new production pin remains a separate intake decision with reconciled
historical evidence; emitting a candidate does not establish that baseline.

## Upstream-map v2

`upstream-map.toml` is the only ownership authority. Code comments may cite a
surface id but may not duplicate its mapping.

```toml
schema_version = 2

[upstream]
repo = "earendil-works/pi-mono"
checkout = "upstream/pi-mono" # ignored Git object database locator; PIRUA_UPSTREAM_ROOT may override
commit = "853a80d26c90a14c1886f0ebb8ffaae133ca2185"

[[owner]]
id = "pi-agent-search::runtime"
kind = "rust"
crate = "pi-agent-search"

[[owner]]
id = "outside-scope.coding-host"
kind = "outside-scope"
decision_ref = "docs/scope.md#scope"

[[watch]]
id = "ai"
root = "packages/ai"
coverage = "complete"
baseline_path_count = 335
route_digest = "sha256:<computed>"
unmatched = "error"

[[surface]]
id = "agent.search.target"
authority = "target-spec"
primary_owner = "pi-agent-search::runtime"
collaborators = ["pi-agent-schema::search"]
disposition = "included"
status = "spec-only"
sync = ["schema", "query", "ordering", "projection"]
conformance = ["conformance/search"]
divergences = []

[surface.lifecycle]
introduced_at = "853a80d26c90a14c1886f0ebb8ffaae133ca2185"
supersedes = []
superseded_by = []
removal_condition = "accepted upstream removal or replacement decision"
reclassification_condition = "Harness v3 S3 source and tests land"
```

### Owners

An included surface has exactly one registered Rust `primary_owner` down to a
crate/module. Collaborators receive drift notifications but do not share
authority. A host-only surface has one registered `outside-scope` owner. A
disposable surface still has one Rust intake owner responsible for recognizing
when replacement source lands.

### Closed authority taxonomy

`authority` is exactly one of:

| Class | Meaning |
|---|---|
| `target-spec` | future contract; overrides disposable pre-landing scaffold |
| `source-and-tests` | landed source plus observable behavior evidence |
| `generator` | transformation rules and revision-bound inputs |
| `generated-projection` | generated output; never edited or treated as primary |
| `compatibility` | preserved legacy behavior isolated from the target runtime |
| `host-only` | explicitly outside the 15-crate headless runtime |
| `disposable-scaffold` | watched for replacement/landing, never ported as authority |

No ad-hoc compound authority string is valid. A file can be optional evidence
for a surface without changing that surface's authority; current Harness
compaction/scaffold code remains `disposable-scaffold` unless a later accepted
decision promotes a specific algorithm.

### Watches and routes

The ownership map watches complete Git trees:

| Watch | Root | Pinned path count | Baseline rule |
|---|---|---:|---|
| AI | `packages/ai` | 335 | every path included, compatibility, generated, or host-only |
| Agent | `packages/agent` | 86 | every path included, compatibility, generated, or disposable |
| Coding-agent | `packages/coding-agent` | 674 | every path explicitly included/compatibility/host-only/generated/disposable |
| SQLite evidence | `packages/session-backends/sqlite-node` | 37 | disposable until Harness v3 S2 lands |
| Telemetry | `packages/telemetry` | 12 | source/tests or package metadata |

Routes use exact `/`-separated paths and specified `*`/`**` globs. Matching is
unordered: every pinned path MUST match exactly one route. Overlap and no-match
are errors. The same rules route candidate trees, but any added path is always a
pending `new-path` item even when it matches a broad route. Its matched owner is
only the intake reviewer; pin advancement requires an explicit map/manifest
overlay confirming or changing the semantic surface. No route has a hidden
“baseline-only” mode and no matched addition is auto-accepted.

For each watch:

```text
route_digest = sha256(
  sorted(path NUL blob_oid NUL route_id NUL surface_id NUL role LF)
)
```

The validator reads tree paths and blob ids from the pinned Git object, never
from the checkout working tree.

### Selectors for multi-authority files

A route may refer to one whole file or a complete selector partition:

```text
file
markdown-section(start anchor, end anchor, expected heading)
typescript-symbol(symbol, export kind, declaration hash)
json-pointer(pointer)
text-span(start sentinel, end sentinel, content hash)
```

For a final text span, the exact end sentinel `$EOF` selects through the pinned
file's last line. All other sentinels must occur exactly once.

Markdown selectors cover every non-whitespace source line exactly once.
`exclude_headings` subtracts a named nested section, using normal Markdown
heading scope, before another selector owns it. The validator materializes each
line set and rejects overlap, uncovered content, a missing exclusion, or a
sentinel/heading that is not unique.

`packages/agent/docs/harness.md` is partitioned by headings/Part 8 rows rather
than assigned wholesale to several owners. A Markdown section ends at the next
heading of equal or greater level. The pinned heading inventory is itself
checked; additions, removals, and renames are structural drift. Barrels and
package export maps are partitioned by symbol/subpath. A changed hunk spanning
selectors emits one item for each; ambiguous or selectorless content blocks.

Required Harness partitions include:

- storage/session/tree/search sections and Appendix B;
- complete Part 3 serialized state tree;
- Parts 4–5 runtime and public surfaces;
- Part 6 host/client disposition;
- Part 7 migrations;
- each Part 8 Types/S/R row with its own primary owner;
- current `packages/agent/src/harness/**` as disposable scaffold.

## Baseline manifests

Revision-bound generated manifests complement the ownership map:

- AI/Agent `package.json` subpath exports;
- recursively expanded root/wildcard-subpath exported symbols with
  type/value/type-and-value status, origin, and source blob oid;
- provider ids, factory symbols, static/dynamic/image kind, API ids, OAuth flow,
  and catalog presence;
- every watched upstream test path plus semantic evidence surfaces derived from
  pinned relative-import closure and reviewed fallback classification;
- Harness heading and Part 8 slice inventory;
- route path/blob/owner inventory.
- generator roots and recursive relative semantic inputs, including constants
  outside `scripts/**`.

The manifests are projections created from pinned blobs and contain their input
oids/digest. The historical manifests retain their fixed-pin byte formats and
assertions. The local comparison tool independently extracts strict candidate
inventories; it never rewrites those historical projections.

### Model catalog provenance

`ai.catalog.generator` owns `generate-models.ts`,
`generate-image-models.ts`, `model-data.ts`, reasoning-option helpers, and all
imported normalization constants. `ai.catalog.projection` owns
`models.generated.ts`, `image-models.generated.ts`, and provider
`*.models.ts` shards.

The upstream generator reads mutable external APIs and its raw data directory is
not in the pinned Git tree. The historical baseline therefore retains
`provenance_status = "missing-intake-snapshot"` for the absent original text inputs.
The repository's [dated catalog capture](../../conformance/catalog/README.md) and
input/source/output hashes support reproducible captured-value parity. Generated
TypeScript at the pin remains projection evidence and does not reconstruct the
missing historical feeds.

## Assembly consistency

`python3 scripts/check_assembly.py` performs a pure join from the pinned source
projections through `upstream-map.toml` to current public evidence. It reuses the
existing Git-object reader, route and selector validation, seven manifest
projections, and AI inventory validators. It does not evaluate upstream source
or run the conformance commands recorded in [the proof registry](../../conformance/assembly.json).
An optional `--map` or `--registry` selects a temporary input; `--json` includes
every resolved obligation. Exit 0 means zero unresolved mappings; exit 1 names
an invalid source, owner, inventory or proof binding.

The joined universes remain separate: 1,144 watched paths; 699 AI and 675 Agent
export occurrences; 42 provider identities, with six selected text entrances,
one selected image entrance, 34 excluded text entrances and excluded public
Faux compatibility; 327 AI behavior rows and 241 source-scope occurrences;
37 Harness selectors and 18 build rows; legacy U1–U14, twelve corpus rows and
exactly five actual bridge definitions; and eighteen coding operations.
All excluded and disposable source identities remain traceable.

Agent package reexports resolve through watched package names and entrypoints.
Pinned named-export clauses retain aliases such as `TelemetrySpanAttributes`
for `SpanAttributes`; a value reexport may use the value namespace of a class
that also supplies a type. The six TypeBox occurrences retain their explicit
external-schema projection. Scaffold exports route to their intake owner and
do not become source-port obligations.

The registry contains logical suite names, known argv, current evidence anchors
and accepted limitations. It never duplicates map ownership. `executable`
means a concrete current proof definition is reachable; `accepted-limit`
retains public boundary evidence and may also name bounded implemented tests.
Neither status attests a new run or complete parity for an unwitnessed contract.
Proxy ownership is required for its six exports and U12 despite its accepted
unwitnessed implementation/parity boundary. Historical fixture provenance and
source statuses remain unchanged. Pin intake still requires validated new
inventories, decisions and actual applicable proof evidence.

## Normative drift algorithm

`scripts/upstream_drift.py` implements the following local comparison contract.
See [the tool documentation](../upstream-drift.md) for CLI arguments, wire
formats, supported syntax and blocking uncertainty:

1. Require full old/new commit ids present locally; default verification never
   fetches.
2. Read both trees/blobs only through Git object commands.
3. Enumerate the union of watched `(path, mode, blob_oid)` tuples.
4. Emit exact-path `A`, `D`, or `M`. Recognize rename only for a unique
   deleted/added pair with identical blob oid; otherwise retain delete plus add.
   Modified moves require explicit responsibility-migration acknowledgement.
5. Route old and new paths independently. Every addition is pending and requires
   an intake overlay even if a broad route matches. Unmatched, multiply matched,
   missing selectors, changed heading inventories, or changed ownership block.
6. Diff selected Markdown sections, TypeScript symbols, JSON pointers, public
   manifests, provider inventories, and generated provenance.
7. Emit canonical items containing change kind, paths/oids, selector, authority,
   primary owner, collaborators, sync dimensions, conformance suites,
   lifecycle/divergence references, and uncertainty.
8. Never auto-accept semantic, host-only, disposable, compatibility, or
   generated changes.

Exit codes are stable:

```text
0  no unexplained drift, or every item has a valid acknowledgement
2  classified drift remains pending
3  invalid map, route, inventory, selector, manifest, or acknowledgement
4  missing Git object or tool failure
```

## Acknowledgement and pin advancement

Canonical drift item id:

```text
sha256(
  old_commit NUL new_commit NUL kind NUL
  old_path NUL new_path NUL old_oid NUL new_oid NUL
  selector_id NUL surface_id
)
```

An acknowledgement binds that id, commits, surface, primary owner,
disposition, evidence, and decision reference. Allowed dispositions are:

```text
implemented
fixture-updated
intentional-divergence
host-only-reviewed
disposable-reviewed
generated-refresh
responsibility-migration
no-semantic-change
```

Unmatched additions first require an intake overlay with candidate commit,
route/selectors, and proposed surface. The pin advances only when both
inventories validate, all items are acknowledged, referenced conformance passes,
divergence records are complete, generated provenance is valid, and no
uncertainty remains. Required proof receipts bind actual local execution
attestations to commands, source/tool/parser inputs and log/artifact hashes;
the tool validates their bindings without executing review-supplied commands.
Cargo evidence must be active under its declared feature profile, and retained
provider adapter families must have complete applicable proof routes.

The tool emits a separate candidate bundle containing an advanced-map patch,
candidate inventory and advancement record bound to old/new commits, report,
decisions and proof receipts. It never applies the patch, changes the current
ownership map or upstream checkout, or rewrites historical manifests. A real
production pin change additionally requires reconciled fixed-pin source,
fixture and captured-feed evidence. The existing guards continue to enforce
those identities; local attestations are not authenticated execution proof.

## Representative ownership records

| Surface | Authority and primary owner | Required evidence |
|---|---|---|
| `agent.sqlite-node.pre-v3` | disposable; `pi-agent-sqlite::upstream-intake` | all `packages/session-backends/sqlite-node/**`; reclassify only when S2 lands |
| `agent.search.target` | target; `pi-agent-search::runtime` | Harness search section and Part 8 S3 row |
| `agent.search.current-api` | source/tests; `pi-agent-search::compat` | `docs/search.md`, `src/search/**`, exact search tests |
| `agent.legacy-proxy` | compatibility; `pi-agent-loop::proxy` | `proxy.ts`, proxy test, barrel symbols |
| `ai.transport-utils` | source/tests; `pi-ai-transport::utils` | exact abort/header/error/proxy/retry/sleep/sanitize utilities and tests |
| `ai.catalog.generator` / `.projection` | generator/projection; `pi-ai::catalog_gen` / `pi-ai::catalog` | generator inputs versus generated outputs with provenance link |
| `coding.models` | source/tests; `pi-coding-runtime::models` | all nine mapped model files and their exact behavior tests |
| `coding.host` | host-only; `outside-scope.coding-host` | UI/CLI/modes/export/keybinding/update/package/trust/RPC presentation |
| `coding.agent-session-layout` | disposable; `pi-coding-runtime::upstream-intake` | private class layout only; observable tests route to included/compat surfaces |
| `coding.legacy-import` | compatibility; `pi-agent-session::runtime` | `session-manager.ts` open/fork/migrate behavior and exact lifecycle/import tests |

## Harness v3 build-order mapping

| Upstream slice | Primary Rust owner |
|---|---|
| Types | `pi-agent-schema` |
| Session layer/Memory/conformance | `pi-agent-session` |
| S1 JSONL | `pi-agent-session::jsonl` |
| S2 SQLite | `pi-agent-sqlite` |
| S3 Search | `pi-agent-search` |
| S4 Dev TUI/Client | `outside-scope.coding-host` |
| R1 Runtime shell | `pi-harness-runtime` with pure core |
| R2 Minimal no-tool run | `pi-harness-core` + runtime/AI effects |
| R3 Generation recovery/retry | `pi-harness-core::generation/recovery` |
| R4 Tools | `pi-agent-loop` phases + core/runtime adapters |
| R5 Inbox/configuration/writes | `pi-harness-core::inbox/checkpoint` + runtime lane |
| R6 Abort/close/failure drain | `pi-harness-core::control` + runtime |
| R7 Deferred redemption | core/runtime + AI deferred APIs |
| R8 Manual compaction | core/runtime compaction |
| R9 Threshold/overflow compaction | core/runtime compaction |
| R10 Navigation | `pi-harness-core::navigation` + runtime |
| R11 Schema version/migrations | session registry + pure core state mappings |
| R12 Surface completion/backend parity | runtime/façade/conformance |

## Intentional divergence

Every divergence records upstream behavior, Rust behavior, owner/rationale,
compatibility consequence, replacement fixture, and revisit/removal trigger.
Unrecorded divergence is a defect. The map records the protocol projections for
bounded remote events, sanitized response headers, typed provider payloads, and
auth-none providers; the search-required `storeGeneration` repair; durable
header classification; and S03's AI divergences for terminal misuse, generic
stream producer authority, retry-abort messages, session resources, endpoint
custody, construction-time transport/middleware, model-header classification,
credential leases/quarantine, cache ordering, error redaction, value-only
publication, async registry fencing, and exclusive model-store ownership.
`upstream-map.toml` remains the canonical detail and revisit authority.
