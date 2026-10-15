# Local upstream comparison and review

`scripts/upstream_drift.py` compares two immutable commits in an existing local
Git object database. It reads trees and blobs, routes both snapshots through
`upstream-map.toml`, extracts inventories, and reports the responsible owners and
conformance suites. It does not fetch, evaluate upstream modules, run commands
from review documents, apply patches, or advance the repository's pin.

Run the pinned baseline comparison with:

```sh
PIRUA_UPSTREAM_ROOT=/path/to/local/pi-mono python3 scripts/upstream_drift.py
```

Both revisions default to the map's commit. To inspect a candidate:

```sh
python3 scripts/upstream_drift.py --upstream /path/to/local/pi-mono \
  --old FULL_OLD_COMMIT --new FULL_NEW_COMMIT --output report.json
```

The old commit must equal the base map's commit. Rust owners must name a
registered production crate from `workspace-topology.toml`. Refs, abbreviations, tree
objects, and tag objects are invalid. IDs must match the repository object
format (40 hex characters for SHA-1, 64 for SHA-256) and their exact canonical
resolved object id; a 40-character SHA-256 prefix is rejected. Missing commits/blobs and unavailable Git,
Node, or parser tools return 4. Input, route, selector, inventory, review, and
proof errors return 3. Valid classified changes or required intake still awaiting
review return 2. A complete valid review, or a comparison with no drift, returns
0. A valid subset of acknowledgements still returns 2. Structural errors and
uncertainty remain blocking even with acknowledgements for every item.

`--upstream` selects the object database; otherwise the tool uses
`PIRUA_UPSTREAM_ROOT`, then the map's checkout locator relative to the repository.
An unrelated local fixture repository does not need the historical production
commit. `PIRUA_UPSTREAM_ROOT` also locates installed parser dependencies when an
explicit fixture object database is selected. The TypeScript syntax parser must
already exist in `node_modules/typescript`; its installed package version must
match `node_modules/.package-lock.json` with a recorded integrity value. Parser
library bytes and installed lock bytes are bound into reports and receipts.
No package installation or network request is performed.

## Immutable projections and conservative comparison

Every watched path must match exactly one unordered route, on each side.
The original watch counts and route digests are checked on the old snapshot;
the candidate receives newly computed counts and digests. Routes that become
empty require explicit removal in an overlay. Watches cannot be removed or
changed by an overlay. Unsupported modes, Git links, invalid UTF-8 paths, and
ambiguous routing are explicit errors. Changed binary or undecodable file
content receives file uncertainty; a review cannot clear it.

Exact-path changes are A, D, or M, including mode changes with identical blobs.
R is recognized only when one deleted path and one added path uniquely share
the same blob object id. Multiple identical candidates remain D+A. Modified
moves remain D+A and may be linked by a responsibility-migration review.
Destination intake is required for every added path, including renames and
paths already matched by a broad route.

Selections support `file`, `markdown-section`, `text-span`, `text-line`,
`typescript-symbol`, and `json-pointer`. Text partitions cover every nonblank
line once. Sentinels use the established prefix semantics, occur once, and end
before the end sentinel; `$EOF` extends through the final line. Excluded nested
headings must exist uniquely inside the selected parent. Symbol selections
cover complete syntax declaration spans and optionally check `export_kind` and
`declaration_hash`. JSON pointers use RFC 6901 escaping, cover every leaf once,
and cannot overlap. JSON partitions compare selected values including types;
text partitions retain formatting changes for review. Selector changes compare
materialized content on both sides, so line shifts alone do not notify unchanged
sections. Missing selections, overlaps, gaps, and changed heading sequences
block until an explicit valid overlay repairs or reviews the structure.

Inventories retain routes/modes, package subpaths, recursive public exported
occurrences, text/image provider identities, complete watched test paths and
relative import closures, generator roots and their recursive semantic inputs,
and partitioned Markdown heading sequences. Public symbols carry kind, origin
blob, complete declaration hash and source span. Comments cannot manufacture
exports; multiple variable bindings, enums, namespace reexports and multiline
declarations are parsed as syntax. Unsupported export forms, unresolved
relative reexports and conflicting origins block. Cyclic public export graphs
currently require review and are reported as uncertainty. The external public
projection is explicitly bounded to TypeBox's `Static`, `TSchema`, and `Type`;
other external public exports require a supported projection. Workspace package
reexports resolve through immutable package export maps to the actual origin
blob and its mapped owner.

The historical AI manifest contains 699 occurrences, including the default
inside `declare module "*.json"` in `providers/data-json.d.ts`. The strict
inventory retains this occurrence with `ambient_module = "*.json"`. This records
an ambient declaration rather than promising a runtime file export. Namespace
changes affect its evidence hash. The historical Agent inventory retains 675
occurrences. Existing pinned manifests, their byte formats, counts, captures,
SDK identities, source-oracle resolver and generator remain unchanged.

Test import closures retain explicitly marked external evidence outside watches;
this does not assign a new semantic owner outside the map. Generator source
imports outside watches block. External import evidence also binds available
Git package manifests and root lock identity, so dependency metadata changes
notify the test or generator seam. Node-only variable import wrappers are
recognized only for static call sites with a direct parameter or the bounded
`.ts`/`.js` runtime rewrite. Unknown dynamic import expressions block extraction
where the closure is needed, and block changed files.

The absent historical catalog data remains a separate, visible
`historical_limitations` entry. Only the known missing provider data directory
under the mapped `missing-intake-snapshot` projection gets that treatment.
New or changed missing-input references block. A changed generator input or
output notifies both its owner and the generated projection. Advancement then
requires `provenance_status = "complete"` and a `provenance_snapshot` containing
`id`, nonempty `source_ids`, `capture`, `sha256`, `inputs`, and `outputs`.
The capture is an immutable Git path whose raw SHA-256 matches `sha256`;
`inputs` and `outputs` are exact path-to-raw-SHA-256 maps covering the extracted
generator closure, external dependency metadata and every mapped output.
Copying a prior capture identifier without matching bytes is insufficient.

## Deterministic wire format

All JSON documents use `schema_version: 1`. Generated JSON is UTF-8, sorted
object keys, compact separators, and one final LF. Item arrays sort by item id;
decisions sort by item id; proof maps sort by suite id. Inputs retain their
explicit array order. Digests use `sha256:` followed by lowercase hex and hash
these canonical bytes, except raw file hashes and the normative item id.

Each item contains kind, old/new paths, object ids and modes, `selector_id`,
`surface_id`, authority, primary owner, collaborators, sync dimensions,
conformance suites, lifecycle, divergence ids, both ownership maps, reasons,
uncertainty, old/new primary and collaborator registration definitions, and
old/new projection evidence. Missing sides use the empty string.
The whole-file selector id is `file`; ownership changes use `ownership`, other
surface metadata uses `surface-metadata`, generated dependencies use
`provenance`, and extracted identities use `inventory:<family>:<identity>`.
These reserved selector ids are report identities, never extra ownership maps.
Map selector ids cannot use these reserved spellings or the `inventory:` prefix.

Item ids hash exactly the following UTF-8 fields, joined by NUL with no trailing
NUL and no extra salt:

```text
old_commit NUL new_commit NUL kind NUL old_path NUL new_path NUL
old_oid NUL new_oid NUL selector_id NUL surface_id
```

Modes remain visible in the payload. The approval `report_digest` hashes the
complete comparison report before its own digest, decisions, pending-item
presentation and exit code are added. It binds both commits, base/effective map,
parser, overlay, inventory digests, canonical items, errors, pending intake and
limitations. Thus an owner-only map change cannot reuse an old approval even
when an individual item id is unchanged.

## Candidate intake overlays

An overlay supplied by `--overlay overlay.json` has these fields:

```json
{
  "schema_version": 1,
  "old_commit": "FULL_OLD_COMMIT",
  "new_commit": "FULL_NEW_COMMIT",
  "base_map_digest": "sha256:...",
  "replace": {},
  "intake": [
    {"path": "packages/example/new.ts", "route": "route-id", "surface": "surface-id", "selectors": []}
  ]
}
```

`replace` may replace complete `owner`, `surface`, `route`, `selector`,
`divergence`, and `manifest` arrays. Omitted arrays inherit the base map. Each
intake row binds
a unique genuinely new path, its extracted route/surface, and the sorted list of
its selector ids, excluding `file`. An empty selector array confirms a whole-file
route. Declared public-manifest sources must exist in the routed candidate.
Removing
a public package therefore needs explicit manifest-array cleanup in addition to
its deletion review. A declaration for an existing public package cannot be
dropped or redirected while its Git source remains. Candidate declarations are
always re-extracted from actual routed package blobs; they cannot supply invented
export rows. Unknown fields, watch edits, stale commits/digests, duplicate
confirmations
and invented selector bindings are invalid. The old snapshot remains unchanged.

A changed heading inventory additionally needs `inventory_digest` equal to the
re-extracted candidate inventory digest. This is a review binding; it cannot
supply or override inventory rows. Ownership changes require `ownership` rows
with `path`, `old_ownership` and `new_ownership`, each an exact surface-to-owner
map. For a unique rename, the row instead binds both `old_path` and `new_path`;
the source and destination ownership are compared even when their blobs are
identical. A rename whose ownership and metadata remain equal needs only its
normal destination intake and item review. When registered primary or
collaborator definitions differ, each row also
requires `old_registrations` and `new_registrations`, binding the complete
owner-id-to-registration maps. A crate change under an unchanged owner id is
therefore an ownership transition. Unused or duplicate ownership rows are
rejected. The resulting items still
require acknowledgement. Both endpoint metadata and suites remain available;
referenced old and new conformance suites are required for advancement.

## Acknowledgements

Pass `--acknowledgements acknowledgements.json` with:

```json
{
  "schema_version": 1,
  "report_digest": "sha256:...",
  "acknowledgements": [
    {
      "id": "sha256:...",
      "old_commit": "FULL_OLD_COMMIT",
      "new_commit": "FULL_NEW_COMMIT",
      "surface_id": "surface-id",
      "primary_owner": "owner-id",
      "disposition": "implemented",
      "evidence": ["conformance/logical-suite"],
      "decision_ref": "review-reference"
    }
  ]
}
```

Unknown, duplicate, stale, incomplete and wrong-owner decisions return 3.
Allowed dispositions are `implemented`, `fixture-updated`,
`intentional-divergence`, `host-only-reviewed`, `disposable-reviewed`,
`generated-refresh`, `responsibility-migration`, and `no-semantic-change`.
Authority-specific dispositions must match their class. No disposition silently
accepts additions or uncertainty. `intentional-divergence` also binds a
`divergence_id` present in the item's complete map-linked divergence records.

A migration additionally supplies `related_items`, including its own id and all
linked D+A or ownership items, plus `responsibilities` with `old` and `new`
surface-to-owner maps for those items. Ownership transitions require this
migration disposition. Similarity is not evidence of a semantic migration.

## Proof receipts and patch artifacts

For patch emission, pass `--registry`, `--receipts`, `--proof-root`, and
`--emit-patch bundle.json`. The default registry is `conformance/assembly.json`;
it is loaded only for this guarded operation. A fixture may supply an explicit
registry and proof root. The registry resolves logical suite ids to fixed argv
arrays, evidence paths and optional exact anchors, and a status of `executable`
or `accepted-limit`. Missing suites, limitations or missing evidence block a
patch. Evidence anchors support `test:`, `python:`, `heading:`, `json:` and
`text:`; the tool validates the actual source location. Rust `test:` anchors use
the shared source parser for supported macro-generated and qualified test names.
Cargo suites additionally
require the shared assembly checker's pure `check_reference` and `check_command`
validation, including test activation under the declared Cargo arguments. A
missing checker or rejected activation blocks receipt acceptance. The checker is
loaded lazily for these suites; baseline comparison and Python fixture proofs do
not resolve or run a Cargo command. The production provider-traces family also
requires the shared source-derived adapter-family validation against the
proof root's tracked `conformance/ai-domain/inventory.json`, regardless of whether
the registry path is default or explicit. Missing canonical inventory or a
missing applicable family blocks the receipt.

A receipts document contains `receipts`, with one row per required suite:

```text
suite, commands, exit_code, binding, inputs, registry_digest, parser, logs, artifacts
```

`commands` must equal the registered argv arrays. `exit_code` must be integer
zero. `binding` contains old/new commit, report, base/effective-map, and old/new
inventory digests. `inputs.source_inputs` lists every tracked proof-root file
with its relative `path` and raw `sha256`; `inputs.tool_inputs` binds the comparison/extraction files, their shared
selector helper, the production crate topology, and the shared assembly
checker when installed. All suite evidence must be among those tracked
inputs. `registry_digest` hashes the exact suite row; `parser` equals the
report's parser identity. `logs` and `artifacts` are nonempty arrays of relative
paths and raw SHA-256 hashes, read and verified locally. Paths must resolve to
files contained in the proof root. No command in these documents is executed.

Receipts are local execution attestations, not authenticated proof of execution.
The submitter must actually run the registered suite against the bound inputs
and retain its real logs and artifacts. A bare pass flag, existing evidence
filename, old fixed-pin green, failed run, stale tool/source hash, or tampered log
does not meet the receipt contract. An accepted unimplemented contract cannot
be attested as completed parity.

The deterministic bundle contains a unified `patch` for `upstream-map.toml`,
round-trippable `candidate_map` TOML with recomputed watch counts/digests, a
revision-bound `candidate_inventory`, and an `advancement` record binding
commits, approval report, candidate map/inventory, decisions and proof receipt
digests. The tool writes only this separate artifact. It rejects report and patch output
paths that overwrite map/review/registry/receipt inputs, tracked source,
imported tool files, proof logs/artifacts, pinned manifests or upstream source.
Report and patch output paths must also differ. It does not apply the patch or rewrite the seven historical manifests.

An emitted map is a reviewed candidate artifact, not acceptance of a new
production baseline. Applying a real pin change requires separately reconciled
historical source/fixture/capture evidence and legacy manifest intake. Existing
fixed-pin guards intentionally continue to protect that evidence. The focused
Python tests prove positive emission and patch applicability only in isolated
true-Git fixtures with an actually executed local suite; production conformance
and full repository verification remain separate obligations.
