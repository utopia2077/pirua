# Bounded operation-state history model

The `pi-harness-runtime` integration target `operation_state_model` complements
S09's public-runtime tests. It runs accepted pure core recipes through actual
MemoryStorage + RuntimeSession + SessionCodec conditional publication. It does not
instantiate the public Harness factory, call Models/tools/hooks, or use the public runtime's
private helpers. Its fixtures are constructed locally from accepted schema types;
no fixed example table or simulated transaction publisher stands in for storage.

## Explored histories

Two deterministic recursive generators explore **251 bounded histories** per profile:

- **154 generation histories:** 11 outcome paths, crossed with all seven wrong-key
  axes and both orders of a lane-token conflict. Attempts are capped at three.
  Each pre-cap attempt can stop, produce a retryable error, or lose its live binding.
  Stop terminates the path; retry/loss advances to another attempt. At attempt three
  the only generated outcomes are stop or ordinary terminal error. Thus the tree
  has 1 + 2 × (1 + 2 × 2) = 11 paths. Retry uses the captured deadline; loss below
  cap advances without inventing a billed response. Each settled attempt tests
  live-key awaiting, wrong identity, and duplicate/stale output rejection.
- **97 tool histories:** all 71 linearizations of source-ordered clearance, arbitrary
  raw readiness, and source-ordered settlement for three prepared calls; plus all
  26 linearizations when the final call returns an immediate clearance result.
  Calls use original sparse assistant-content positions **1, 3, 5**. Prepared
  intents may overlap, and later-ready outputs are explicitly offered too early
  and rejected. Deterministic seed parity covers all-terminating versus ordinary
  continuation and optional tool usage. Selected eligible boundaries lose/rebind
  a safe live tool using saved args. Every immediate-final history races cleanup
  against insertion into its observed-absent args key, rejects, reloads, and commits.

These are finite recipe/receipt histories, not an assertion that every public-driver
schedule or every R1–R4 state has been explored. Seven-axis generation histories
share prefixes and outcomes; the count is not a count of independent guarantees.
The tool generator enumerates admissible receipt interleavings, including choices
between available clearance and settlement work; it does not replicate or establish
the public runtime's scheduling implementation.

## Independent observations

A small reference holds committed entry IDs/order, usage-to-entry associations,
expected saved args, logical attempt budget, source completion prefix, and terminal
ownership. Reserved response/usage IDs are checked against the explicitly supplied
IDs before joining the reference. The reference does not implement the core's
phase transitions or generate production writes.

After every successful acceptance, transition, settlement, valid root pulse,
external args insertion and terminal publication, the test:

1. Obtains a complete real storage snapshot solely as a conformance oracle and
   validates it through the actual codec.
2. Reads actual lane/operation roots; gathers their required entry/register names;
   loads those through Session; invokes accepted current-state validation. All
   deterministic batch args keys are explicitly read for Seq/Absent cleanup facts.
3. Checks committed entry identity/source order and parent chain, R/U associations,
   no early materialization/billing of a pending response, exact saved arguments,
   source-ordered result prefix, and the captured three-attempt budget.
4. Checks the independently expected public/durable terminal category and complete
   operation-register cleanup, then rejects a duplicate terminal candidate.

Every rejected wrong/stale output compares the complete durable snapshot before
and after planning. Every deliberately stale conditional recipe is submitted to
actual Session, must return NotApplied, and must preserve that same complete snapshot
(entries, usage, register values/tokens, sequence watermark, versions and stats).
Recipe predicates are mapped directly, without deduplication or repair. Full snapshot
reads belong only to this test oracle; the modeled hydration path does not consult
ledger or historical results to choose recovery.

## Replay and scope

Default runs execute all 251 histories. An individual failure prints family, seed,
full generated actions, and the completed action prefix. Replay deterministically:

```sh
PI_OPERATION_MODEL_CASE=generation:0 cargo test --offline -p pi-harness-runtime --test operation_state_model
PI_OPERATION_MODEL_CASE=tools:71 cargo test --offline -p pi-harness-runtime --test operation_state_model
```

Generation seeds are `path_index * 14 + wrong_key_axis * 2 + conflict_order`, in
recursive Stop/Retry/Lost traversal; valid range 0–153. Tool seeds are DFS order
(Clear, Ready by source position, Settle); 0–70 are prepared and 71–96 immediate-final.
Out-of-bound or malformed replay values fail rather than reporting an empty success.
Default and `--all-features` run the same bounded histories, offline.

Excluded: public runtime execution, callback concurrency, manual gates,
close/abort/queue APIs, physical close/reopen, deferred polling, structural/overflow
work, live providers, and other storage backends. Configured Session roots and
explicit fixture API/model/IDs avoid all three pending owner defaults. Unknown-at-cap
synthetic factory policy is not chosen; the generated cap error is an ordinary
explicit settled provider fixture. S10/S11 remain outside this model.

Author causal evidence perturbs only the test-owned recipe-to-Session boundary by
omitting Usage writes. The real Session commits the resulting structurally valid
transaction, then the independent R/U invariant fails on replay `generation:0`.
No assertion or production guard is changed; exact source restoration passes.
This target is property evidence for accepted core/Session,
not whole-S09 or public-runtime acceptance.
