# Pure legacy format-3 normalization fixtures

Authority is pinned `853a80d26c90a14c1886f0ebb8ffaae133ca2185`:
`packages/agent/docs/harness.md:2919–2935`, concrete node shapes in
`packages/coding-agent/src/core/session-manager.ts:32–153`, coding roles in
`packages/coding-agent/src/core/messages.ts:29–77,100–137`, and Usage in
`packages/ai/src/types.ts:382–403,421–464`. The frozen S07 contract owns deterministic
IDs, ISO admission, typed register ordering, unresolved provenance and the virtual
adjustment. These are source-shaped format3 literals, not renamed pre-v3 fixtures.

`all-v3.jsonl` has one explicit version3 header and19 physical nodes covering all
10 accepted tags (nine declared plus active_tools_change). Its direct messages
cover three builtins and four coding roles. The explicit expected table retains13
entries, four registers and one virtual adjustment. It includes two branches,
a final discarded node selecting an earlier leaf, label-parent targeting that
differs from targetId, ECMAScript-trimmed name, empty old summaries inside a kept
tail, a discarded firstKept boundary, arbitrary opaque ID twins, tool-call IDs,
and independently inconsistent token/cost totals. Usage is counted once from
physical nodes, never tail copies; expected totalTokens338 is not the component
sum48, and expected total cost is negative(-0.5).

The expected file was authored from a fixed retained-node/parent/tail table and
Python hashlib integer UUID bit placement, without calling production conversion.
Known typed UsageCost values use floating-point JSON representation; arbitrary
opaque details/data retain their original numbers. The author's generation recipe
is retained as `/tmp/rust-harness-s07-legacy-implementation-fixtures-20260908.py`;
no test regenerates expected results from the Rust implementation.

`id-vectors.json` fixes the framed SHA256 outputs, including UTF-8 byte lengths,
NUL-bearing IDs, domain separation and the first74 digest bits in UUIDv7 positions.
`iso-vectors.json` uses independent Python datetime/civil iteration for epoch,
leap/century, offset, fractional truncation and exact48-bit maximum literals.
The generation recipe is an author oracle, distinct from root/reviewer literals.

Owning tests are `legacy_v3_normalization`, `legacy_v3_ids_iso` and
`legacy_v3_validation`. They exercise actual PreparedLegacy getters and actual
MemoryBacking/RuntimeSession consumers, field validation with permissive caller
schemas, exact source-byte retention, unknown roles, malformed ancestry/bounds,
null/omission, numeric overflow, ID provider faults, zero adjustment, backwards
entry time and stable IDs despite content/position/append changes.

`PreparedLegacy` has no public constructor, public mutable fields or decode/parts
shortcut. Its source/state getters are borrowed immutable values. Callers must
choose coding_message_options explicitly; the pure normalizer never auto-registers
or overrides a role schema. Legacy builtin missing/null content alone repairs to
an empty array; other invalid payloads reject. All node timestamps use the frozen
ISO domain; retained IDs use outer timestamps, not inner message timestamps.

Pure preparation accepts complete UTF-8 JSONL object records, with a final valid
record permitted without LF and trailing ASCII whitespace ignored. Interior blank
records, unknown header/retained-node fields, incomplete JSON and invalid envelopes
reject. Discarded model/thinking/active-tools records need only their structural
envelope; their payload keys do not become required configuration. This unit does
not recover a torn legacy file or access paths. FileRepo owns physical admission.

The virtual adjustment is logical RAM state, even when usage is zero; it does not
prove that source bytes contain a ledger row. File-backed adoption must compare
exact PreparedLegacy bytes and header identity under the path claim and recheck
before first replacement. The later physical consumer must persist the normalized
PRE-snapshot plus the original triggering caller frame once, and prove read-only
open/fork/restart, first-write failure/success and idempotent conversion on real
files. Pure fixture success is not that proof and does not complete S07.
