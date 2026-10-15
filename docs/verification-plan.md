---
revision: 253
upstream: 853a80d26c90a14c1886f0ebb8ffaae133ca2185
---

# Verification plan

The coding runtime and its foundations, local upstream drift comparison and
assembly consistency checks are implemented. The contracts below define their
required behavior and proof obligations. Explicit accepted limitations remain
part of the evidence boundary; implementation and a complete verification run
do not establish live-provider parity or a stable public release.

Provider coverage follows [the accepted scope](scope.md#selected-provider-scope).
`scripts/verify.sh` is the complete verification entry point. Focused checks
support individual contracts and do not replace the complete verification.

## Verification contracts

### S01: The architecture and execution baseline is internally auditable

**Depends on**: none

**Outcome**:

- The accepted intent, design hub, 15-crate topology, source authority map,
  and public scope agree with each other.
- Upstream-map v2 exhaustively classifies all pinned paths in the five watched
  trees, with one primary owner per surface, checked selectors/manifests, and no
  unmatched or ambiguous route.
- Independent reviewers have challenged topology, upstream-follow strategy,
  and protocol/state/security boundaries; every material finding is resolved or
  explicitly carried as an open decision.

**Scope**:

- `README.md` — repository entry point.
- `upstream-map.toml` — machine-readable upstream ownership seed.
- `docs/scope.md`, `docs/architecture/**`, `docs/verification-plan.md` —
  accepted scope, architecture, topology, and proof contracts.
- `upstream/manifests/**` — pinned-object route/export/provider/test/heading
  projections and provenance.
- `scripts/validate_docs.py` — deterministic structural/link/map validation.

**Must preserve**:

- Accepted intent and accepted ADR-0001/0002 decisions.
- The accepted fifteen-crate topology and its exact graph remain authoritative.
- The production-crate count is 15, not 26 and not an implicit larger count.

**Exit verification**:

- Command: `python3 scripts/validate_docs.py`
- Expected: exit 0; proposal sections ordered; all relative Markdown links and
  pinned selectors resolve; exactly 15 unique production crates; all five Git
  watches match their baseline counts/digests; every pinned path has exactly one
  route; every semantic surface has one registered primary owner; generated and
  public/test manifests are bound to pinned blob ids.
- Command: `python3 -m unittest scripts/test_validate_docs.py`
- Expected: twelve causal validator tests pass.
- Command: `git diff --check`
- Expected: exit 0.
- Causal check: temporary probes for a 14-crate topology, missing owner,
  unmatched path, ambiguous route, missing Harness heading, and dirty working
  tree all fail except the dirty-tree probe, whose result must remain identical
  because validation reads pinned Git objects. Remove every probe afterward.

### S02: The workspace and complete shared type surface compile as one contract

**Depends on**: S01

**Outcome**:

- A pinned stable Rust toolchain, locked Cargo workspace, and all 15 production
  crates build under one quality gate.
- `pi-ai-core` contains the behavior-free AI message/content/model/usage/error
  DTOs required by Agent schema.
- `pi-agent-schema` contains the complete Harness v3 Types-slice surface:
  entries, registers, usage, complete current/historical operation-state tree,
  transactions, storage/session/tree/repo/search interfaces, Part 5 results,
  errors, events, snapshots, and hooks.
- Workspace dependency rules prevent the forbidden edges in the topology.

**Scope**:

- `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `.gitignore` — reproducible
  workspace baseline.
- `crates/**` — 15 crate manifests plus the complete behavior-free shared type
  implementation in `pi-ai-core` and `pi-agent-schema`.
- `scripts/verify.sh` — canonical whole-workspace quality entry point.
- `conformance/schema/**` — serialization/discriminant/export inventories.

**Must preserve**:

- No effectful dependency enters `pi-harness-core`.
- Production Rust schema/domain require no embedded JavaScript runtime. Offline
  oracle and schema comparison tooling may require documented Node prerequisites.
- The Types slice remains behavior-free; storage and runtime methods may be
  traits but have no fake successful implementation.
- All placeholder APIs are private; empty public stubs do not count as
  implementation.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: formatting, dependency policy, clippy with warnings denied, all
  workspace tests, and documentation validation pass.
- Expected delta: schema inventory matches every Harness v3 Types requirement;
  JSON round-trips preserve field names/discriminants and unknown open ids.
- Causal check: an unknown closed discriminant and an unlisted internal Cargo
  edge are rejected by focused tests/checks.

### S03: The portable AI runtime contracts match `packages/ai`

**Depends on**: S02

**Outcome**:

- `pi-telemetry` supplies the minimal object-safe context/span and no-op
  contract needed by portable request runtime; full telemetry schema/exporter
  behavior remains S13.
- `pi-ai-core` completes the upstream event stream, Models registry, auth
  resolution, stores, validation, message
  transformation, overflow detection, retry classification, and session
  resource semantics.
- Six portable domain units cover 217 behavior rows: message transformation,
  tool validation, failure classifiers, provider/model helpers, portable utilities,
  and session resources. Auth/store and registry units add 58 and 30 rows; streams
  add 22. Deterministic aggregation preserves exactly 327 behavior rows and
  699 export occurrences.
- Auth resolution plus credential/model stores land as a verified unit before
  the registry/publication unit, preserving the exact 58/30 conformance split.
- The registry unit dispatches text/deferred requests only. It consumes an
  already-resolved runtime endpoint for the Azure sentinel; S05 owns the Azure
  algorithm and image registry/dispatch while reusing S03 request custody.
- Golden JSON and behavioral fixtures cover every exported upstream domain
  discriminant and the success/error/cancel stream terminals.

**Scope**:

- `crates/pi-telemetry/**` — minimal executor-neutral context/span contract and
  no-op implementation only.
- `crates/pi-ai-core/**` — complete portable domain/runtime surface.
- `crates/pi-ai/**` — façade projections required to expose that surface.
- `conformance/ai-domain/**` — generated inventories and golden fixtures.

**Must preserve**:

- Provider-neutral DTOs do not depend on built-in provider registries or HTTP.
- Stream terminal settlement occurs exactly once; cancellation is distinguishable
  from provider failure.
- The public generic event stream is consumer-only; assistant production uses a
  validated producer capability and missing/empty terminal error messages fail.
- Secrets never enter serializable model/session DTOs or debug output.
- Dynamic traits are object-safe through explicit boxed futures; credential
  refresh holds an exclusive per-provider mutation lease; registry persistence
  and publication are fenced by one per-provider sequencer.
- Core request runtime has no HTTP transport or callback middleware. Retry in
  this slice is pure classification only; S04 owns HTTP retry and S14 owns the
  legacy whole-assistant loop.
- Borrowed/stored Rust policy traits are trusted process-local capabilities and
  non-Serde. Foreign providers/resources use protocol values and lifecycle
  messages; they never supply Rust or JavaScript callbacks to core.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: all gates pass and the AI export-inventory/golden suite reports no
  missing upstream domain contract.
- Expected: the behavior aggregate has exactly 327 unique rows and the symbol
  inventory has exactly 699 occurrences with totals 146/123/22/163/239/6 for
  S02 DTO/implemented/Rust-nonapplicable/S04/S05/external rows.
- Causal check: duplicate terminal events and unknown required discriminants are
  rejected by focused tests.
- Causal check: 32 concurrent OAuth resolutions refresh once; a blocked stale
  publication drains before a newer generation reads cache; post-spawn
  membership admission remains reentry-safe; a displaced provider epoch cannot
  revive after replacement admission; foreign lifecycle behavior uses tokens/
  messages rather than callback identity.

### S04: Shared AI transports and canonical wire adapters are trace-equivalent

**Depends on**: S03

**Outcome**:

- `pi-ai-transport` owns HTTP, SSE, WebSocket, proxy, timeout, retry transport,
  and redacted diagnostics.
- `pi-ai-providers` implements the shared Anthropic Messages, OpenAI
  Completions/Responses/Codex/Azure, Google Generative AI/Vertex, Bedrock,
  Mistral, and pi-messages wire adapters against recorded fixtures.

**Scope**:

- `crates/pi-ai-transport/**` — transport behavior and mockable boundaries.
- `crates/pi-ai-providers/src/api/**` — canonical wire families.
- `crates/pi-ai-core/src/stream.rs` and focused tests — additive opt-in
  cancellation snapshots preserving received content/usage; existing default
  S03 cancellation remains unchanged.
- `crates/pi-ai-core/src/options.rs` and the registry's header merge only —
  preserve raw header own-key order through decoding, cloning and dispatch.
  Existing BTreeMap construction/lookup APIs, wire shape, exact-key duplicate
  rejection and all accepted S03 proof rows remain intact. Provider consumers
  use the ordered projection where case-variant override order is observable.
- `conformance/ai-transport/**` — transport and construction-time service proof.
- `conformance/provider-traces/**` — request/stream/error trace fixtures with
  sanitized credentials.
- Additive logical Fetch request/custom-Fetch dispatch and generic typed payload
  services — preserve the source input forms needed by configured Cloudflare
  binding and Bedrock binary fields. Existing physical transports, JSON carrier,
  default service type and all accepted family observations remain unchanged.
- AI export mapping/generator/probes and dependency gates — cumulative S04
  evidence, preserving all 699 identities and S03's 327 behavior rows.

**Must preserve**:

- Retry ownership is explicit; one request cannot be retried by two layers.
- Header/query/body credentials are redacted from fixtures and diagnostics.
- Provider stream order and partial tool-call assembly remain observable.
- Raw request header case variants retain source ordering and null-override
  effects; no sorting or input rejection substitutes for those effects.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: all canonical adapter request, stream, timeout, proxy, and error
  traces pass offline.
- Causal check: repairable malformed input follows the family's pinned parser;
  unrecoverable SSE/JSON and mid-stream cancellation settle once with the
  expected typed failure/cancel result and required partial content/usage.
- Expected: real loopback HTTP/proxy/WebSocket proofs exercise the production
  backend, all 161 S04 text/helper export occurrences have concrete verified
  targets, and the two original image occurrences remain explicitly S05.
- Expected: custom Fetch receives source record/constructed-header and text/byte
  body inputs before network Fetch conversion; native Fetch keeps its accepted
  behavior. Bedrock middleware distinguishes typed blobs from JSON replacement,
  while existing JSON middleware implementations and default services compile
  and preserve their source observations.
- Expected: source-bound raw-header permutations survive input decoding and
  reach SDK request construction with distinct override/deletion outcomes;
  unchanged S03 conformance and dependency guards continue to pass.

### S05: Selected providers, OAuth, images, and catalogs are complete

**Depends on**: S04

**Outcome**:

- [选定的 6 个文本入口和 1 个图像入口](scope.md#selected-provider-scope)
  完成工厂、选项映射、必要认证/OAuth、模型目录、图像与注册装配。
  工厂之外的共享 AI/图像运行时和兼容帮助函数仍按既定职责实现。
- 全量上游导出保持可追踪；名单外职责有引用所有者决定的明确排除记录。
  公开 Faux provider 兼容不再要求，离线验证使用内部模拟 provider。
- The stateless `pi-ai` SDK compatibility helper corresponding to
  `RetryCallbacks`/`retryAssistantCall` preserves bounded retry observations
  without becoming Harness retry authority.
- Selected catalog generation is deterministic. Upstream additions remain
  visible as a failing inventory diff until classified, including new entries
  outside the selected product scope.

**Scope**:

- `crates/pi-ai-providers/src/providers/**`, `oauth/**`, `images/**`,
  `catalog/**` — selected provider entries and their required dependencies only.
- `crates/pi-ai/**` — public provider/catalog façade and developer targets.
- `conformance/catalog/**`, `conformance/provider-traces/**`,
  `conformance/ai-compat/**` — inventories, offline traces, and stateless façade
  compatibility behavior.

**Must preserve**:

- No live credentials or paid provider calls are required by the default suite.
- Generated data comes from its generator/provenance, never hand-edited output.
- Provider-specific policy stays outside `pi-ai-core`.
- Built-in registration/catalog selection includes only the chosen entries;
  text and image OpenRouter share their source ID in separate namespaces.
  Provider/OAuth/image `all-*` features cover the selected product set and do
  not restore excluded factories; shared-wire tests keep their accepted breadth.
  Open IDs/custom provider ports retain their accepted semantics.
- 已验收共享 API 不因缺少对应产品工厂而删除或失去回归验证；选定路径必须
  证明完整注册装配，包括已知 registry usage0 / child-lazy usage17 的取消边界。
- Harness and legacy Agent orchestration do not call the stateless SDK retry
  helper; S04 HTTP retry remains a separate request-level policy.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: selected provider/auth/image/catalog and shared facade obligations
  have zero unexplained gaps; full upstream identities retain explicit scope
  decisions. All selected offline traces and 14 S05-owned `retry-loop.*` compatibility
  fixtures pass. Internal simulated providers support offline proof without
  claiming the excluded public Faux compatibility API.
- Expected: registration and catalogs contain exactly the selected entries;
  OpenRouter model routing is not additionally filtered by model vendor.
- Causal check: adding a synthetic upstream provider inventory item without an
  owner causes the inventory test to fail.
- Causal check: removing a selected entry or registering an excluded factory
  fails the scope/assembly check; an exclusion cannot satisfy implementation
  evidence for a selected entry.

### S06: Transactional storage and the conversation tree work in memory

**Depends on**: S03

**Outcome**:

- `pi-agent-schema` and `pi-agent-session` implement Harness v3 entries,
  registers, usage rows, transactions, session/tree/context/repository APIs,
  migrations, forks, and the Memory backend.
- One backend conformance suite proves atomic expected-value commits, stable
  queries, isolation between sessions, and migrate-on-open behavior.

**Scope**:

- `crates/pi-agent-schema/**` — shared durable schemas.
- `crates/pi-agent-session/**` — backend-neutral session semantics and Memory.
- `conformance/session/**` — backend contract.

**Must preserve**:

- Storage remains one-session and agent-agnostic.
- A failed compare-and-set transaction commits no partial entries, registers,
  or usage rows.
- Durable types are detached values, not aliases to backend buffers.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: all Memory backend contract and independently modeled/property
  transaction/query tests pass. Real Session/Repo/context/migration composition
  reaches the actual accepted trait methods; zero-test or name-only success
  cannot satisfy a behavior obligation. Prior S01–S05 source/behavior/catalog/
  dependency/feature evidence remains unchanged.
- Expected: concurrent CAS yields one complete winner, late staged failure and
  condition mismatch preserve the exact snapshot including allocator/stats,
  pending placement validates final state, and detached reads/lifecycle/forks/
  synthetic migration failures preserve their frozen boundaries.
- Causal check: an injected expected-value conflict leaves a byte-for-byte
  equivalent snapshot; removing the atomic condition or final publication guard
  makes the corresponding independently expected trace fail before restoration.

### S07: JSONL persistence and legacy coding-agent sessions survive migration and crashes

**Depends on**: S06

**Outcome**:

- JSONL satisfies the shared storage contract, detects/truncates only an
  incomplete tail according to policy, and preserves committed history.
- Legacy coding-agent v3 files import idempotently into the Harness v3 model
  with provenance and deterministic identifiers.

**Scope**:

- `crates/pi-agent-session/src/jsonl/**`, `legacy_v3/**`, actual file-repository
  modules and the existing generic migration consumer boundary.
- `conformance/session/jsonl/**`, `conformance/session/legacy-v3/**` and owning
  physical/repository/process tests. Only required additive exports/manifest
  entries and behavior-free schema reexports join these modules.

**Must preserve**:

- Existing source files are never destructively rewritten during import.
- Corruption before the final incomplete record is surfaced, not silently
  discarded.
- Reopening or reimporting cannot duplicate committed semantic entries.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: shared backend/model cases execute through actual JSONL; independent
  legacy goldens, complete-invalid/interior/torn-array recovery, descriptor handoff,
  allocator preservation, live-view/fork and real migration/conversion cases pass.
  A separate writer process acknowledges a commit, is terminated without graceful
  close, and another process observes the exact committed state. This establishes
  process-crash behavior only. Read-only and rejected/mismatched first writes leave
  legacy source unchanged; successful conversion preserves one adjustment and the
  complete caller frame; repeat reopen never remints or duplicates it.
- Causal checks: corruption in a non-tail record fails open with a typed error;
  removing a real truncation/publication/identity or migration-value guard makes
  a corresponding independently expected named observation fail, then restore.

### S08: SQLite and standalone search satisfy the independent Track S contracts

**Depends on**: S06

**Outcome**:

- `pi-agent-sqlite` implements Harness v3 S2 and passes the S06 shared
  storage/session contract against the Memory reference, including transactions,
  writer lease/fencing, forks, repair, and physical-format checks. JSONL runs
  the same contract independently in S07.
- `pi-agent-search` implements S3 over `SessionRepo`, with durable cursors,
  catch-up, notify/reconcile/remove, and a reference SQLite FTS5 projection that
  works over every backend without becoming storage authority.

**Scope**:

- `crates/pi-agent-sqlite/**` — S2 backend.
- `crates/pi-agent-search/**` — S3 service and optional FTS implementation.
- `conformance/session/sqlite/**`, `conformance/search/**`, shared reader/admission
  conformance and owning tests. Additive Session helper/read/validation APIs,
  external dependency/lock changes, and the narrow pi-agent search
  feature/trait forward join only as required by the frozen boundaries.

**Must preserve**:

- Repository/session code never depends on search.
- Search never depends on `pi-agent-sqlite` or Harness runtime.
- Search scope cannot expose private data across sessions.
- Pre-v3 `sqlite-node` source remains disposable evidence; the Harness spec and
  shared conformance own semantics.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: Memory and SQLite pass one parametrized contract; SQLite locking,
  fencing, repair and query-plan tests pass; search catch-up, idempotent crash
  recovery, notify/sweep equivalence, removal, multi-process discipline, scope,
  and ranking fixtures pass.
- Causal check: deleting the derived index and rebuilding yields equivalent
  normalized results without changing canonical sessions.

### S09: Harness v3 R1–R4 execute runtime-shell, model, recovery, and tool turns

**Depends on**: S05, S06

**Outcome**:

- R1 provides mutation lines, total-state validation, register-seq CAS tokens,
  snapshots, Effects, scheduler/manual gate, hook/event primitives, bounded
  restore inventory, identity resolution, fault, and close plumbing.
- R2 runs one complete no-tool operation through in-memory session and faux
  provider with exact acceptance, hooks, intent, usage, terminal transaction,
  events, and telemetry.
- R3 recovers/retries every generation state, including unknown effects,
  synthetic caps, stop/error/aborted/deferred classification, and failure-drain
  foundations.
- R4 executes durable complete tool plans using the behavior-compatible
  three-phase loop, context-bound tools, replay policy, ordered finalization,
  hooks, and usage.

**Scope**:

- `crates/pi-harness-core/**` — pure validation and transition/settlement planner;
  with only approved test dependencies.
- `crates/pi-harness-runtime/src/**` and new owning runtime tests — the R1-R4
  owner, public surface, settings/events/watch, request adapter, driver/Effects,
  restore, tools and lifecycle composition. Existing foundation behavior and
  assertions remain fixed; this grants no S10/S11 behavior or dependency autonomy.
- `conformance/harness/r1-r4/**` — state, race, crash, and trace fixtures.
- `crates/pi-agent-schema/src/surface/planner.rs` and owning ABI witnesses —
  reviewed process-local correlation/lifecycle corrections only; durable DTOs,
  existing eight-method inventories and unrelated accepted surfaces stay fixed.
- `crates/pi-agent-session/src/{storage,session,codec,memory,jsonl}/**`,
  `crates/pi-agent-sqlite/src/**` and owning new tests — additive backend-incarnation
  attachment/read-stamp capability and borrowed actual-codec access only.
- `crates/pi-agent-loop/**` — shared tool phases and their compatibility witnesses;
  full legacy Agent remains S14. Preserve the reviewed runtime-to-loop topology
  edge and existing-workspace dependency declarations required for real integration.
- `crates/pi-ai-core` adapter/registry plus the new request-lifecycle module and
  exports; `crates/pi-ai-providers` selected text-adapter service/invocation helpers
  and owning bridge tests — only the separately reviewed request-lifecycle unit.
  Existing RequestRuntime/request/option fields, transport algorithms, dependency
  versions/edges and selected factory scope stay fixed.

**Must preserve**:

- `op.state` is the complete durable program counter.
- Effect output settles only the exact structured `EffectKey` current state
  expects.
- Tool args persist under their documented registers; raw results finalize and
  commit in source order even when execution is parallel.
- Manual drive and automatic drive have identical durable traces.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: every required R1–R4 focused test and operation-state property suite
  passes, including the public no-tool walking skeleton.
- Causal check: stale/wrong-key model or tool settlement cannot mutate current
  state; parking a manual action causes zero writes/effects.

### S10: Harness v3 R5–R7 complete inbox, control, failure drain, and deferred redemption

**Depends on**: S09

**Outcome**:

- R5 implements `nextRun`, steer, follow-up, queued cancellation, durable drain
  markers, checkpoint consumption, immediate config setters, deferred writes,
  and adjustments.
- R6 implements orthogonal abort/close/failure drain, signalling, per-phase
  reconciliation, deferred cancellation, waiters, controlled-crash close,
  terminal cleanup, and external-finalization stop.
- R7 performs exactly one deferred provider poll per resume, preserving copied
  options/config/source lineage, request hooks, fresh unknown-poll intents, and
  newest-source cancellation.

**Scope**:

- `crates/pi-harness-core/src/**` and new R5–R7 tests — pure inbox/control,
  transition/settlement/deferred recipes and additive detached facts; accepted R1–R4
  assertions stay fixed and core retains its sole production schema dependency.
- `crates/pi-harness-runtime/src/**` and new R5–R7 public/model tests — actual
  lane/tree/driver/Effects/request/owner/gate/observation consumers of those recipes.
- `crates/pi-agent-schema/src/surface/planner.rs` and a new transport witness — only
  additive process-local Deferred reconciliation validation; no durable shape or
  existing Effects/Provider/ApiAdapter inventory change.
- `crates/pi-ai-core/src/{request_lifecycle,models_registry,lib}.rs` and new deferred
  bridge tests — optional lifecycle fetch capability and captured fetch/cancel consumers
  through existing registry workers, preserving ordinary routes and selected providers.
- `crates/pi-agent-session/src/{storage,session,memory,jsonl}/**` and narrow shared
  materialization helpers plus new tests; `crates/pi-agent-sqlite/src/{owner,commit}.rs`
  and new tests — backend-owned exact entry preparation/conditional publication only,
  with old ordinary commit semantics, full validation and fresh lease checks preserved.
- `crates/pi-agent/tests/runtime_sqlite_projection.rs` and new dedicated support —
  actual Harness/projector/SQLite composition through the existing `sqlite` feature;
  no manifest, dependency edge or version change. Test-only lease expiry touches only
  its own temporary database through the already-required Python standard library.
- `conformance/harness/r5-r7/**` — deterministic state/race/crash/trace evidence.
- Preserve dependency versions, selected-provider protocols and existing guards.

**Must preserve**:

- Queue/register/entry exclusivity and atomic consume/delete rules.
- Abort is idempotent, generation-aware, and reconciles every state.
- Unknown external outcomes are never guessed; retry/poll budgets and usage do
  not reset after restart.
- One `resume()` grants at most one deferred poll.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: both orders of every R5 queue/config/control race and every R6/R7
  state/crash boundary pass with fake clocks and deterministic effects.
- Causal check: a stale completion after abort/restore is a no-op; a crash after
  consuming one queue item cannot consume the next; one resume cannot poll twice.

### S11: Harness v3 R8–R10 compact and navigate through first-class summary effects

**Depends on**: S10

**Outcome**:

- R8 implements reserved-lane manual compaction, preparation register, total
  structural state, hook/generated sources, nested request intents/usage,
  retained tail, retry/recovery, and abort.
- R9 adds once-per-trigger threshold compaction, overflow classification,
  atomic response/preparation publication, normalized projection, one recovery
  flag, and bounded second failure.
- R10 validates and atomically publishes summarized/unsummarized navigation,
  final leaf/label/terminal writes, and summary-only hook behavior.

**Scope**:

- `crates/pi-harness-core/src/**` and new R8–R10 tests: structural/compaction/navigation
  recipes plus their existing planner/generation/transition/terminal/control joins;
  preserve the sole production schema dependency and all prior assertions.
- `crates/pi-harness-runtime/src/**` and new R8–R10 public tests: actual preparation,
  summary request, lane/driver/Effects/publication/control/observation consumers.
- `crates/pi-agent-schema/src/surface/planner.rs` and new transport tests: only the
  additive non-Serde exact-key summary-suspension carrier required by H1892. Existing
  durable DTOs, SummaryRequestPlan fields and the eight-method Effects inventory stay fixed.
- `crates/pi-agent/tests/runtime_structural_sqlite.rs` and dedicated new support:
  actual structural/overflow Harness composition through the existing sqlite feature,
  exact result metadata and lease-expiry atomic-prefix refusal, no manifest change.
- `conformance/harness/r8-r10/**`; preserve provider protocols and dependency
  versions.
- One source-entailed old fixture exception: in
  `crates/pi-harness-runtime/tests/runtime_r5_r7_deferred.rs`, only the local Length
  response of `actual_pending_handle_equality_checks_every_axis_and_error_never_retries`
  is set to output equal to its captured model limit with consistent total usage.
  All its assertions, other cases and shared builders stay unchanged. Add new explicit
  original-zero/below/equal/above/captured-limit observations in the new S11 tests.

**Must preserve**:

- Nested summary intent, `run_summary_request`, and
  `settle_summary_request` are independently gated crash boundaries.
- Compaction is append-only with explicit lineage and complete usage.
- Navigation cannot orphan an operation or cross session boundaries; its final
  publication is one transaction.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: every R8–R10 normal/state/crash/race test passes, including split
  turns, empty/decline, threshold reopen, all overflow predicates, retained
  tails, and navigation atomicity.
- Causal check: crash at each nested summary boundary produces the documented
  unknown/retry/synthetic path and never resumes partial summary content.

### S12: Language-neutral extensions use the same Rust effect and settlement path

**Depends on**: S11

**Outcome**:

- `pi-extension-protocol` implements protocol/profile negotiation, canonical
  schema digests, authenticated capability references, structured effect/live
  bindings, unary/stream state machines, byte credit, cancellation,
  generations/drain, bounded overload, reentrancy, snapshots/gap-resync, and
  structured failures.
- All eleven `hook.v1` methods, tools, auth-none providers, host actions,
  resources, events, and telemetry adapt into existing Rust Effects; peers never
  write durable state.
- Rust and TypeScript SDK/reference peers plus one minimal Python or Go peer pass
  the same transcripts. No embedded JavaScript runtime is required.

**Scope**:

- `crates/pi-extension-protocol/**` and the existing runtime extension,
  registry, hook, event, owner, effects, driver, tool, provider, resource,
  summary, abort and structural adapters already authorized at revision105.
- Existing SDK/reference peers under `sdk/typescript/**` and `sdk/python/**`,
  their runtime/conformance tests, and `conformance/extension-protocol/**`.
- Receive/native tooling and synthetic source
  boundaries remain in force. The archived reconciliations preserve exact
  source authorizations; archive placement neither broadens nor revokes them.

**Must preserve**

- Settlement requires both an exact live `InvocationBinding` and matching
  durable `EffectKey`.
- First terminal enters `terminal_received`; its binding and validated output
  remain live until durable settlement or definitive no-longer-expected
  classification, then retire exactly once.
- An older generation may settle only an invocation still bound to it; “old”
  alone is not stale.
- Protocol v1 transports no core-managed credential and grants no secret
  resolution, OS, process, network, or filesystem authority.
- Trusted in-process `watch()` keeps upstream guarantees; bounded remote event
  sinks use the recorded gap/resync divergence.
- All original S12 outcomes, exits, causal checks, and the selected provider
  scope remain in force.
- No ninth Effects method, durable origin/spend ledger, credential route,
  whole-process quota, silent schema/method expansion, or public Tool/SDK
  semantic change is introduced.
- The six-profile protocol contract, canonical descriptors and static corpus
  remain the source of truth. Derivative runtime scenarios may expose missing
  fields only when the canonical catalog already contains them.
- Memory and SQLite behavior must agree at every runtime consumer boundary.
- Every child process is bounded and reaped. Source hashes, causal failures
  and restored passing checks must validate the exact candidate.
- A partial track cannot satisfy the assembly proof; all original exits must pass.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: cross-language N/N+1, all hooks, tool/provider stream, live drain,
  crash/rebind, complete resource/host-action/telemetry profiles, grant-ceiling
  intersection, initial/additional byte credit, relative deadlines, cancel/end
  order, gap/resync, schema evolution, malformed/allocation bomb, every queue
  overload, and secret sentinel fixture passes under finite tiny limits.
- Causal check: wrong key/generation, duplicate terminal, forged/cyclic parent,
  ungranted action, and secret request all cause no durable mutation.

### S13: Harness v3 R11–R12 migrate safely and complete every public/backend surface

**Depends on**: S07, S08, S11, S12

**Outcome**:

- R11 performs chained migrate-on-open under the writer lease, including total
  open-operation mappings, lenient old JSONL replay plus mandatory compaction,
  and newer-version refusal on every backend.
- R12 completes snapshots/watch, events/order/filtering, settings/resources,
  public/private surfaces, telemetry, exports, backend parity, and removes any
  remaining private scaffold.

**Scope**:

- `crates/pi-agent-session/src/migration/**` — registry/orchestration.
- `crates/pi-harness-core/src/migration/**` — pure exhaustive state mappings.
- `crates/pi-harness-runtime/src/{settings,resources,watch,surface,telemetry}/**`.
- `crates/pi-telemetry/**`, `crates/pi-agent/**` public façade.
- `conformance/harness/r11-r12/**`, `conformance/telemetry/**`.

**Must preserve**:

- Effect-pending migration preserves uncertainty, ids, attempts, replay policy,
  cancellation, queues, effective args, and captured policy; it never returns to
  ordinary pre-intent or performs an effect.
- Private surfaces cannot be obtained through public projections.
- Observer/exporter failure cannot alter authoritative state.
- Memory, JSONL, and SQLite pass the same final runtime race/crash matrix.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: equal/older/newer, chained/idempotent/crash migration, every old
  operation variant, zero-effect migration, backend parity, snapshot gap,
  attach-in-every-live-state, visibility, event ordering, schema freshness, and
  telemetry redaction/failure tests pass.
- Causal check: migrate a pending non-replayable effect and prove it cannot be
  redispatched; fail a telemetry observer and prove the durable trace is equal.

### S14: The legacy low-level Agent surface remains behavior-compatible

**Depends on**: S05

**Outcome**:

- `pi-agent-loop` implements current `Agent`, agent loop, tool pipeline, proxy,
  queues, abort, steering/follow-up, and event-order behavior separately from
  Harness v3.
- `pi-agent` exposes the intended legacy compatibility façade and the new
  harness façade without conflating their state models.

**Scope**:

- `crates/pi-agent-loop/**` — legacy behavior.
- `crates/pi-agent/**` — façade, compatibility profile, testkit.
- `conformance/agent-loop/**` — translated/differential traces.

**Must preserve**:

- Existing low-level event ordering, tool-call pairing, queue semantics, abort,
  and provider error outcomes.
- Legacy compatibility code cannot become Harness v3 durable authority.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: every mapped upstream Agent/agent-loop behavior test has a passing
  Rust fixture or an explicit reviewed non-applicability record.
- Causal check: a fixture that swaps two normative events fails trace matching.

Current legacy acceptance is bounded by the
[U1–U14 denominator](../conformance/agent-loop/README.md). U4/U5/U6/U8/U10/U12/U13
remain test-unwitnessed as complete contracts; U14 retains its non-applicability
decision. The actual Rust-to-Node bridge covers exactly five named rows, while
the twelve-row corpus also has separate comparator causal checks. The proxy
responsibility remains owned and is not certified by a workspace-wide pass.
These accepted limits qualify the current evidence without removing the
compatibility obligations above.

### S15: The headless coding runtime composes the completed Rust surfaces

**Depends on**: S13, S14

**Outcome**:

- `pi-coding-runtime` implements selected coding-agent model/credential/provider
  composition, coding tools, messages, system prompt, settings snapshot,
  resource inputs, session lifecycle, legacy import, extension bridge, and
  builder/server composition.
- A headless integration harness runs deterministic coding sessions, including
  tools, compaction, restart, model switch, and extension participation.

**Scope**:

- `crates/pi-coding-runtime/**` — headless product composition.
- `conformance/coding-runtime/**` — product traces and private simulated-provider
  scenarios within the selected provider scope.

**Must preserve**:

- Product policy composes core APIs and does not redefine Agent/Harness schema.
- UI/TUI/CLI/package-manager/trust-prompt concerns remain host-owned.
- Coding tools use the same validation, progress, cancellation, and settlement
  contracts as extension tools.
- `retry-loop.overflow-precedence` is checked at the actual coding-session
  caller, as frozen in S03; it is not part of S05's stateless helper.
- Codex session cleanup composes the S05 owned resource scope: successful core
  cleanup consumes its registration, later sessions register a new scope, and
  callers await that scope's close tickets when physical completion is required.
  A synchronous cleanup result or later empty close is not completion of prior work.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: deterministic headless scenarios for prompt, coding tools, queue,
  retry, abort, compaction, branch, restart, legacy import, model switch, and
  external extension all pass.
- Causal check: disabling a required capability makes composition fail before
  the operation starts.

Current coding evidence retains eighteen positive and eighteen negative rows,
with zero pending rows. The default bridge covers seventeen applicable rows;
the extension participation row requires the `extensions` feature. The
[model responsibility table](../conformance/coding-runtime/model-responsibilities.md)
and [coding corpus](../conformance/coding-runtime/README.md) retain the accepted
boundaries for host discovery/credentials, in-place legacy import, ordinary
request model metadata, finite logical progress bounds and measured preflight
side effects. These are bounded observations rather than live-provider,
process-isolation or allocator certification.

### S16: Upstream drift detection and the final assembly audit prove completion

**Depends on**: S02–S15

**Outcome**:

- The upstream ownership map covers all in-scope contract categories and a tool
  can compare a pinned/new pi-mono revision, classify touched authorities, and
  name required Rust owners and conformance suites.
- A clean-room assembly audit finds no unowned in-scope export, provider,
  Harness v3 requirement, low-level Agent behavior, or selected coding-runtime
  responsibility.
- Provider completeness uses the Proposal's selected scope; excluded upstream
  entries remain traceable with explicit decisions and are reported separately
  from implemented coverage. Existing source and behavior identities remain unchanged.
- Repository documentation describes current limitations honestly and supports
  continued Harness v3 intake without source translation guesswork.

**Scope**:

- `upstream-map.toml`, sync tooling, inventories, and all conformance manifests.
- Public API manifests and repository documentation.
- Final independent assembly audit.

**Must preserve**:

- Drift detection reports uncertainty; it never auto-accepts upstream semantic
  changes.
- No live provider calls, publish, or deployment occur in the default proof.
- Proposed publication/security decisions remain visibly unresolved unless a
  human has accepted them.

**Exit verification**:

- Command: `./scripts/verify.sh`
- Expected: clean checkout-equivalent offline run passes formatting, dependency
  policy, lints, unit/property/integration/conformance/doc tests, SDK tests, and
  inventory/drift checks.
- Command: `python3 scripts/upstream_drift.py` and
  `python3 -m unittest scripts/test_upstream_drift.py`.
- Expected: baseline reports zero unexplained drift. True-Git synthetic changes
  name their authority, Rust owner and proof suite, remain nonzero until valid
  intake and complete acknowledgements, and block structural uncertainty.
  Cross-owner renames require explicit responsibility migration. Candidate
  patch emission requires bound actual fixture-suite receipts; stale inputs,
  failed execution or tampered logs/artifacts block it. Positive patch
  applicability is verified in an isolated fixture, separate from production
  pin intake and unchanged historical evidence guards.
- Causal check: remove one owner mapping in a temporary copy; assembly audit
  must fail, then restore the source.
- Command: `python3 scripts/check_assembly.py` and
  `python3 -m unittest scripts/test_check_assembly.py`.
- Expected: all enumerated source obligations resolve to canonical owners and
  concrete current proof definitions or explicit accepted limitations. The
  pure check executes zero registered proof commands. Its temporary-map causal
  removes the proxy owner while retaining the six exports and U12, requires
  named failure, restores exact bytes and requires zero unresolved mappings.
