# Actual Memory repository conformance

The production MemorySessionRepo implements create/open/list/delete/fork using
accepted RuntimeSession, SessionCodec, MemoryBacking and MigrationRegistry.
`repository-contract.json` contains literal source-bound fork membership and seq
expectations. Tests exercise exact copied Entry bytes (including display usage),
new register tokens, facts/configuration/exclusions, idle lanes/zero ledger,
root/view ownership, close/recreate and callbacks during reserved construction.

Internal `repo::tests` use only narrow SYNTHETIC closed-snapshot initialization
and observation/failure hooks around the SAME production record/open adapter.
They run current DTOs through artificial versions1→2→3, actual MemoryBacking
publication, real codec validation and final RuntimeSession exposure. They are
not an alternative store, migration engine or test-only repository. Tests cover
actual version authority, all12 register envelopes, failed steps/resume, pending
outer claim after inner lease drop, sealed old writers, changed header policy,
coherent fork around source commits/delete and realistic maximum-seq exhaustion.

An in-progress record claim yields Busy, including reentrant operations; list
omits unpublished destinations and may return Busy for a published claimed record.
Explicit IDs do not mint; omitted IDs use the accepted checked UUIDv7 helper.
List metadata is detached and ID-sorted; supplied metadata selects by ID only.
Catalog identity fields are immutable; mutable version/generation come from the
actual backing snapshot. Active roots are retained strongly to track surviving
views. Borrowed active storage is never closed by snapshot/open cleanup.

Fork source must already be at the current version: migrate through open first.
Fork itself never mutates source authority. After an acknowledged coherent
snapshot, later source mutation/deletion does not alter that captured destination.
Only retained Entry seqs determine the destination allocator; unretained delete
exhaustion does not transfer. New destination metadata uses explicit cwd only,
new clock time, actual parent, current version and storeGeneration0.

The private actual Memory migration adapter swaps a completely validated backing
only under an outer open claim with no active incompatible writer. It checks
source version and both outer/inner register key/seq envelopes, preserves immutable
rows and allocator, and publishes values+version together. No ordinary Storage
replacement capability is exported. Current production version1 has no historical
mapping; the artificial typed versions prove mechanics only. Persistent async
publication/fencing/drop recovery, historical codecs and S07 are not claimed.

Full acceptance remains `./scripts/verify.sh`, followed by independent fresh
whole-S06 assembly review under independent review. Author unit tests alone do not close S06.
