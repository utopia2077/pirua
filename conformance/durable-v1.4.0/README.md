# Durable Session 1.4.0 observations

This suite runs the published `@earendil-works/pi-durable` and
`@earendil-works/chord` **1.4.0** JavaScript packages. Their preserved npm SLSA
provenance payloads associate the package integrity digests with tag `v1.4.0`,
commit `4a080252f06caf63a525d7873c5d94d3c23887e0`. It executes real MemoryStorage,
Node SQLite storage, Session, and decoded Chord delta functions. It does not build
the upstream monorepo or import modules from a checkout's mutable working tree.

Install once, using Node >=22.19 with built-in SQLite and npm:

```sh
node conformance/durable-v1.4.0/setup.mjs --install
node conformance/durable-v1.4.0/session.mjs --check
node conformance/durable-v1.4.0/causal-check.mjs
```

Setup invokes `npm --prefix <absolute-suite-directory> ci --ignore-scripts
--no-audit --no-fund` after checking that the suite manifest exists. The pinned
lock includes all transitive dependencies and artifact integrity digests. This
single package environment can serve later scheduler, runtime and CodeMode
oracles; those suites are not implemented here. Installation needs package
registry access. Fixture generation and checking need no network and invoke no
model/provider APIs. SQLite files are temporary and removed after each case.

To additionally verify the tagged source/test/spec fingerprints against a local
Git object store, set `PIRUA_UPSTREAM_ROOT` to a checkout containing the target
commit before running setup or the check. Verification uses `git show <commit>:<path>`;
the working-tree revision does not matter. Without that variable the check still
verifies all 125 pinned release JavaScript files/manifests, the lock, and the two
preserved provenance bundles. Bundle payload/digest association is checked;
cryptographic signature trust is not independently reverified by this suite.
No claim of executing the tagged TypeScript or the complete tagged test suite is
made. `source-pin.json` identifies every byte fingerprint and package integrity.

Generation is explicit:

```sh
node conformance/durable-v1.4.0/session.mjs --write
```

It reads `fixtures/session-inputs.json` and writes `fixtures/session.json`.
`--check` reads the frozen expectations, executes their inputs again and compares
all observations; it cannot rewrite expectations. `--fixture <path>` selects an
alternate observation file for review/negative controls. `--pin <path>` selects an
alternate source pin for the freshness control. `causal-check.mjs` demonstrates
failure after changing the fork's final-state expectation, changing the observed
UTF-16 code units, and changing one release source fingerprint. It creates and
removes private temporary fixtures, without altering installed source or committed expectations.

The current fixture contains 38 input cases and 61 executions: 23 cases run on
both Memory and SQLite, three require file reopen and run only on SQLite, and 12
call real Chord decoded delta appliers. The source index is `case-index.json`;
links identify related tagged definitions/tests/spec sections, not claims that
those entire tests were run.

| Family | Observed behavior |
| --- | --- |
| Storage | Detached complete records, task state variants and null/missing fields, request lookup, ascending/descending pagination, cursor-order rejection, fork visibility and heads, base/tail reads, checkpoint replacement, as-of reads, retirement/recreation and independent copies, metadata rollback, nonempty structural no-ops, SQLite reopen |
| Delta | `r/s/d/a/t/p/m`, immutable and mutable replay, object keys versus array indexes, no sparse arrays, unsafe paths, invalid permutations, valid non-BMP/UTF-16 boundaries, and the separately marked unpaired-surrogate result |
| Session | Duplicate document acquisition, staged read visibility, callback and invalid sparse-write rollback, table read-before-write, retained old snapshots/publications, empty-batch suppression, checkpoint decisions/counts/failure, family seed lifetime, version rejection without migration, submissions and same-version reopen |
| Fork | As-of/current/initial policies, final state of the cutoff entry's whole commit, unknown definitions, independent incarnations, inherited entry cutoff, exclusion of tasks/task docs/Session docs |
| Observation | Noncreating acquisition, baseline state deliveries, exact watch frames, incarnation retirement to null, recreation isolation, close retaining last value, and 101 pending frames coalescing to the newest root replacement |

## Trace format and native consumption

`fixtures/schema.json` defines the envelope. Each case has `id`, `family`,
`basis`, `input`, and `expected`. Expected output is keyed by `memory`, `sqlite`
or `chord`. It contains ordered `{label,value}` observations and, for Session
cases, complete retained commit publications, admitted storage batches and
checkpoint predicate calls. Each case starts with a new store. IDs and sequences
are the deterministic numeric results produced by upstream, not renumbered.
Publications are retained as the actual upstream objects until final serialization;
old snapshot aliases retain the actual returned revision, so subsequent changes
exercise prior-revision stability. The JSON output is detached from those objects.

Inputs are JSON data, with no function bodies or executable code strings:

- `as: "parent"` stores a result. `"$parent.id"` resolves it; references inside a
  transaction resolve step by step, including earlier staged creation results.
- `{"$undefined":true}` represents an omitted trace argument or absent observation.
  Ordinary JSON null remains null. `{"$utf16":[56832,120]}` losslessly represents an
  observed JavaScript string containing an unpaired surrogate, using every UTF-16
  code unit of that string (`0xde00`, `0x78` in this example). Valid Unicode scalar
  strings, including ordinary non-BMP characters, remain unchanged. These reserved
  trace forms are adapter syntax, not extra Durable JSON value variants. The UTF-16
  marker does not introduce invalid strings into native product JSON; consumers
  retain it as trace data for the already marked outside-native case.
- `trace-codec.mjs` recursively encodes only non-scalar observed string values.
  `session.mjs` checks all fixture strings and object keys for Unicode scalar
  validity before comparison or writing. Thus the whole shared fixture can be
  parsed by ordinary Rust `serde_json`, including when that native consumer skips
  the outside-native behavior case. The original unencoded observation is retained
  privately for provenance, with its original SHA-256 recorded in `provenance.json`.
- Storage `commit.writes` carries the exact tagged `StorageWrite` DTO shape.
  `read.method/args` selects the named storage method. Context is supplied by the
  runner. `mint`, `reopen`, and `close` are explicit operations.
- Delta `value/ops` supplies exact decoded tuples. Both published mutable `apply`
  and immutable `applyImmutable` run; the previous immutable input is observed.
- Session `transaction.steps` contains creation, append, fork, submission, document,
  retirement, table reads and explicit callback failures. Definition descriptors
  supply JSON initial values, scope, version, history/fork semantics, optional
  family seed field and checkpoint policy (`never`, `always`, delta threshold,
  or a deterministic throwing predicate). `doc.edits` uses `set`, `delete`,
  `append`, `trim`, `splice`, and `reverse` descriptors applied through ordinary
  JavaScript draft access. The driver implements no delta diff algorithm.
- `watch*`, `state*`, and `repeatTransactions` describe observation gates, flushes,
  frame reads, state reads and the bounded overflow schedule. The held first
  callback keeps one in-flight frame separate from the 101 pending commits.
- Errors expose source JavaScript `category` and `message` separately. Native
  adapters map error categories; JavaScript exception spelling is evidence.

Exact source tracker emissions remain recorded in `publications.ops`, storage
`writes` and checkpoint calls. A native adapter whose API uses explicit decoded
operations compares the appropriate semantic projection: exact received decoded
operation replay, values, record metadata, sequences, atomicity, publication
cardinality and incarnation lifetime. The order of separate changes within one
publication is unspecified and native adapters compare those changes by identity;
operation order within one document remains meaningful. Session draft convenience
methods do not
require reproducing the Chord proxy's particular emission spelling. Their native
normalization must still preserve operation order and replayable nonempty
structural changes. Native consumers must independently replay their actual emitted
batches onto the previous snapshot, verify replay produces the new value, and
assert positional removals and structural nonemptiness. Substituting retained final
values or unconditional root replacements would bypass the behavior under test. This suite does not assert pointer identity, proxy behavior,
allocation addresses or native exception strings.

## Deliberate comparison boundaries

Three cases are explicitly marked with `nativeComparison`:

1. `sqlite-allocation-only-reopen-variance` observes IDs `[2,3]`, reopen, then
   `[2,3]` again without an intervening commit. A later commit persists the high
   water mark. The native no-reuse contract reserves allocation durably and must
   differ from this actual release behavior.
2. `delta-utf16-surrogate-split-upstream-domain` trims one UTF-16 unit from `😀x`
   and observes JavaScript's unpaired low surrogate. Its exact result is recorded as
   `{"text":{"$utf16":[56832,120]}}` so the trace file itself contains only Unicode
   scalar strings. That result is outside the native Unicode scalar JSON domain. Native Session must reject the split
   before commit; the valid two-unit trim still returns `x` in both domains.
3. `storage-deferred-invalid-delta-validation-variance` deliberately violates
   tagged spec §10's trusted semantic-write precondition by committing an append
   onto a number. Actual raw Storage accepts the batch, and materialization later
   rejects it as corruption; a following retirement is accepted. This is outside
   common conformance, not an upstream defect or a requirement for raw backend
   revalidation. The required common Session boundary instead rejects the sparse
   draft edit and rolls back its staged table write before storage admission.

`fixtures/native-contract.json` contains independently authored native contract
expectations for allocation reservation, surrogate splitting, and Session
invalid-operation rollback. They are labeled separately and are **not** run as
upstream successes. Native fault-injection tests remain responsible for backend
failure/ambiguity, caller cancellation after admission, failure latch/close races,
and checkpoint/commit interruption. This bounded oracle does not simulate those
storage semantics or claim their proof. Unknown-version conversion/migration,
JSONL, browser/IndexedDB, Cloudflare, paid APIs, scheduling and runtime execution
are excluded.
