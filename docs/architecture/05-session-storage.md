# Session and Storage

## Storage is one-session and agent-agnostic

`Storage` knows entries, registers, usage rows, transactions, scans, metadata,
and lifecycle. It MUST NOT know operation transitions, hooks, providers, tools,
lanes as orchestration objects, or application session selection.

```rust
#[async_trait]
pub trait Storage: Send + Sync {
    async fn commit(&self, tx: Transaction) -> Result<CommitResult, StorageError>;
    async fn get_entries(&self, ids: &[EntryId]) -> Result<EntryMap, StorageError>;
    async fn get_register(
        &self,
        namespace: RegisterNamespace,
        key: &str,
    ) -> Result<Option<Register>, StorageError>;
    async fn list_registers(
        &self,
        namespace: RegisterNamespace,
        prefix: Option<&str>,
    ) -> Result<Vec<Register>, StorageError>;
    async fn scan_entries(&self, query: EntryScan) -> Result<Vec<Entry>, StorageError>;
    async fn scan_branch(&self, query: BranchScan) -> Result<Vec<Entry>, StorageError>;
    async fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> Result<Vec<EntryStructure>, StorageError>;
    async fn scan_usage(&self, query: UsageScan) -> Result<Vec<UsageRow>, StorageError>;
    async fn stats(&self) -> Result<SessionStats, StorageError>;
    async fn close(&self) -> Result<(), StorageError>;
}
```

`StorageError` is Rust-only and non-serializable. It is `#[non_exhaustive]` with
stable categories `closed`, `conflict`, `corruption`, `invalid_data`, `busy`,
`io`, `unsupported`, and `backend { open code }`; every variant carries bounded
context and an operator-facing message, never session content or credentials.
Backend-specific sources remain behind `std::error::Error` and are not durable.

## Transaction contract

A transaction contains ordered writes:

```rust
pub enum Write {
    InsertEntry(NewEntry),
    InsertUsage(NewUsageRow),
    SetRegister(NewRegister),
    DeleteRegister(RegisterKey),
}
```

Normative invariants:

- all writes commit or none do;
- committed sequence numbers are strictly increasing in write order;
- entry and usage ids share one session-wide uniqueness domain;
- entries and usage rows are write-once;
- registers are the only mutable values;
- delete of an absent register is a no-op;
- JSON null is data, not deletion;
- a failed validation produces no write;
- a failed admitted commit faults the harness caller.

Conditional operation commits compare register sequence tokens, not serialized
value equality.

## Session layer

`Session` owns runtime schema validation and typed projections:

```rust
pub struct Session<S: Storage> {
    storage: Arc<S>,
    codec: Arc<SessionCodec>,
    ids: Arc<dyn IdGenerator>,
}

pub trait SessionRepo {
    async fn create(&self, options: SessionCreateOptions) -> Result<SessionHandle, SessionError>;
    async fn open(&self, metadata: SessionMetadata) -> Result<SessionHandle, SessionError>;
    async fn list(&self, options: ListOptions) -> Result<Vec<SessionMetadata>, SessionError>;
    async fn delete(&self, metadata: SessionMetadata) -> Result<(), SessionError>;
    async fn fork(&self, source: SessionMetadata, options: ForkOptions)
        -> Result<SessionHandle, SessionError>;
}
```

`SessionTree` provides lane-bound branch views, facts, context projection, and
idle writes. Public reads return detached immutable values.

`SessionError` is likewise Rust-only/non-serializable and non-exhaustive. Stable
categories are `storage`, `schema`, `unknown_custom_role`,
`newer_storage_version`, `migration`, `not_found`, `closed`, and
`invalid_operation`. Public Harness expected errors remain the exact tagged DTOs
in the Agent design; these session/backend categories are not a competing wire
contract.

`SessionMetadata` follows the pinned interface and adds required
`storeGeneration: SafeUnsigned` to resolve the same specification's search and
precise-rewrite invariant. New and imported sessions start at zero; a precise
rewrite increments it atomically with store publication. Search cursor identity
is `(sessionId, storeGeneration)`. This target repair is recorded as an explicit
upstream divergence because the prose/build-order require the counter while the
TypeScript interface accidentally omits it.

## Memory backend

Memory is the reference backend for runtime development and conformance. It
implements the same transaction and query semantics as persistent backends; it
is not allowed to skip validation because it is in process.

## JSONL backend

JSONL lives inside `pi-agent-session` to reduce crate count, but remains an
independent module and conformance implementation.

Format rules:

- header includes format and storage versions;
- one physical line encodes one complete transaction;
- a single-write transaction may use a compact object representation;
- a multi-write transaction is encoded as one array/container line;
- a syntactically torn final line is discarded as an unacknowledged transaction;
- malformed interior data is corruption;
- register set/delete replay yields current state;
- snapshot compaction rewrites live entries, rows, and latest registers through
  temp-file plus atomic rename;
- superseded register bytes may remain until compaction;
- migration forces a compacted current representation.

## SQLite backend

SQLite remains a separate crate because it introduces native/platform
dependencies and an independently testable physical design.

Required properties:

- one database file per session;
- every write transaction begins with `BEGIN IMMEDIATE`;
- writer ownership uses an expiring fenced lease;
- entries/usage rows are never updated or deleted in ordinary runtime;
- register upsert/delete is transactional;
- segmented branch index avoids copying an unbounded shared prefix;
- query-plan guards reject full entry scans and temporary sort plans on the hot
  branch path;
- fork/rewrite uses a coherent read snapshot and atomic destination publication;
- an explicit repair operation rebuilds derived branch state.

## Forks

A fork copies:

- selected entries;
- facts allowed by the selected scope;
- lane leaves/configuration where applicable;
- metadata linking the parent session.

It does not copy:

- open operation registers;
- pending entry registers;
- `lane.lastResult`;
- usage ledger rows.

Destination lanes begin idle and usage begins at zero.

## Legacy coding-agent v3 import

Legacy import belongs to the session codec, not `pi-coding-runtime` state.
Application code selects a source file; session code performs normalization.

The importer MUST preserve the upstream Appendix B rules, including:

- custom message conversion;
- facts extracted from label/session-info nodes;
- discarded model/thinking/tool-change nodes;
- parent-chain repair;
- compaction retained-tail materialization;
- timestamp conversion;
- UUIDv7 id re-minting and known-reference remapping;
- aggregate imported usage adjustment;
- first-write atomic format conversion.

## Schema evolution

- Equal version opens normally.
- Older version runs chained migrations before publication.
- Newer version is refused.
- Every migration maps every reachable current register value, including open
  `OperationState` variants.
- Entry/usage compatibility has a stronger stability obligation than ephemeral
  operation registers.

`pi-agent-session::migration` owns the registry, chain selection, writer lease,
migrate-on-open transaction, and newer-version refusal.
`pi-harness-core::migration` owns pure exhaustive mappings for operation-state
versions defined by `pi-agent-schema`. Migration performs no provider, tool,
hook, timer, id mint, entry insertion, or usage insertion effect.

Every old operation state is classified as `pre_intent`, `effect_pending`, or
`settled`. An `effect_pending` mapping MUST preserve:

- every operation/lane/kind/step/attempt/item field from which the structured
  `EffectKey` is deterministically derived (the key is not separately stored);
- reserved response/result/usage ids;
- attempt, poll, item, and nested request indexes;
- replay policy and persisted effective tool arguments;
- cancellation control and drained pending queues;
- captured model/configuration, stream options, and retry policy.

It MUST NOT map back to ordinary pre-intent. If there is no natural successor,
the mapping produces an explicit `migrated_effect_pending` recovery variant.
That variant can only apply the previous conservative rule: safe replay, a later
numbered provider/summary attempt when permitted, or synthetic
interrupted/error/aborted settlement under the reserved ids. Conformance covers
every reachable old variant, crash after each migration step, idempotent reopen,
zero effects during migration, and post-migration recovery trace equivalence.

Storage version 1 has no historical state variant and therefore does not define
or serialize `migrated_effect_pending`. Before the first v2 schema exists, a new
migration ADR MUST define that variant's complete fields/discriminants and old
decoder. S02 must not invent a future placeholder state.

## Search boundary

Search is a standalone service over repositories. Search indexes are derived and
rebuildable. Repositories MUST NOT know search exists. SQLite FTS or another
index may have its own failure policy, but it cannot become conversation
authority.
