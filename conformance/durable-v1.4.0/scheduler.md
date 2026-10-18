# Generic Durable scheduler 1.4.0 observations

This evidence preparation executes the actual published Durable **1.4.0 Harness**
scheduler with custom generic task definitions, real MemoryStorage and real Node
SQLite. It uses no Rust implementation or expected values from a Rust algorithm.
It does not implement or accept a native scheduler. The pinned package environment,
lock, source association and trace codec are the existing Session environment;
all 18 frozen Session files remain byte-identical.

Run after installing the existing shared package environment:

```sh
node conformance/durable-v1.4.0/scheduler.mjs --check
node conformance/durable-v1.4.0/scheduler.mjs --causal
```

Explicit generation reads `fixtures/scheduler-inputs.json`:

```sh
node conformance/durable-v1.4.0/scheduler.mjs --write
```

`--check` does not rewrite expectations. `--fixture <path>` selects another frozen
fixture for negative controls. `--causal` changes one paused inspection's expected
pending task to running, executes the actual `--check` command against a disposable
copy, and requires rejection. SQLite files and negative fixtures live temporarily
under the suite's ignored `.runtime` and are removed. No new installation, package
script, upstream build, network or provider call is needed for any command here.

`scheduler-source-pin.json` reuses the existing package/lock/attestation identity and
125 durable/chord release file hashes, adds the relevant immutable scheduler sources,
and hashes all 18 frozen shared files. The driver uses
`verifyEnvironment({pinPath: scheduler-source-pin.json})`, verifies every shared-file
hash, then executes behavior. With `PIRUA_UPSTREAM_ROOT` set to a local Git object
store, it verifies 47 tagged source/test/spec fingerprints with immutable `git show`;
it never imports that checkout's mutable modules. The existing npm attestation
payloads associate the published package digests with exact commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`. No TypeScript-source execution or complete
upstream test-suite execution is claimed. Signature trust has the same explicit
limit as the shared environment: payload/digest association checked, cryptographic
signature trust not independently reverified.

The frozen fixture has **23 cases / 41 actual release/backend executions**: 18 cases
on Memory and SQLite, plus five SQLite-only cases requiring file reopen.
`scheduler-case-index.json` maps each case to tagged tests, source hashes and spec
sections. Related whole tests are associations, not claims that those tests ran.

| Cases | Observed behavior |
| --- | --- |
| No inspection dispatch | Task stays pending, inspect remains paused, no handler event occurs until resume; three real checkpoint advances complete a typed counter |
| No progress and throwing handlers | Returning without durable progress, an empty commit, and a thrown handler become distinct faulted receipts/reports |
| Memo and terminal lease | First stored memo wins, explicit null differs from absence, live record retains memos, terminal receipt drops them, late invocation commit rejects |
| Abort and stale writes | Actual committed raw task record already carries abortRequested when the run's signal fires; the old run's attempted terminal write rejects, then a fresh abort invocation settles |
| Absolute sleeps | Real sleep state releases the invocation, early/repeated resume before deadline does not dispatch, deadline wake returns actual fake wall clock; sleep survives SQLite reopen |
| Durable joins | Waiting and resumed checkpoints use real runtime.commit; allSettled retains the slow child, while failFast cancels the remaining owned slow child and the waiter resumes with failed/aborted outcomes |
| Completing holds | A decided parent stays completing until its child finishes and never reruns; a separately seeded owner also waits for work in its owned conversation |
| Background boundary | Ordinary idle and conversation abort include foreground work and exclude the background task; explicit task abort later stops it |
| Definition availability | Missing definitions block until registration; newer stored versions and newer definitions lacking migration block without conversion; an explicit abort or fitting registration resolves each case |
| Registry handover | The old same-version phase continues after replacement; the next phase resolves the new registry definition |
| Normal close | Running invocation is signalled without an abort mark, stale write rejects, checkpoint/memo/startedAt remain pending across SQLite reopen, no restart is counted, and the memo winner is reused |
| Seeded reopen | Persisted pending/running/waiting/sleeping/completing/terminal records are read from reopened SQLite; running becomes pending with restart increment, other states retain meaning, cached terminal outcome is reused without handler dispatch |
| Restart guards | Seeded running residue reaches restart limit, stays blocked without dispatch, retry clears the guard and completes; restart-abandoned missing code stays marked until explicit abort orphans it |
| Wait cycles and timers | Real runtime commits reject self-wait and implicit owner/child hold cycles; invocation timers disarm at terminal, and retained host callbacks cannot invoke ended task code |

## Input and observation contract

`fixtures/scheduler-schema.json` defines the envelope. Every case has `id`, `basis`,
`input` and backend-keyed `expected`. The input contains task descriptors, optional
installed definition IDs/settings/seed records, and ordered host operations. There
are no executable function bodies or evaluated JavaScript strings in fixture input.

Task descriptors provide definition `id`, persisted `name`, version, initial checkpoint,
phase action arrays and an optional abort action array. The driver calls real
`defineTask` and installs real `defineExtension` registrations in the real registry.
The default abort handler commits an aborted outcome with reason `oracle`.
No descriptor has a migration callback. Seed records are exact tagged StorageWrite
DTOs; every new case starts with a real raw root conversation 1 commit. This avoids
introducing unrelated Harness conversation document initialization into generic
scheduler IDs. No live concurrent owner is fabricated by the driver.

Host operations create tasks through real Harness Session commits, resume, inspect,
read/get receipts, abort task/conversation, retry, install/uninstall definitions,
release gates, advance the injected wall clock, acquire/join/check in-memory waits,
read handler events/timer dispositions, and close/reopen storage. `until` waits for
an actual committed task status with a bounded test-driver deadline. Waiting for a
receipt and waiting for idle enable scheduling exactly as the real Harness APIs do.
The clock starts at 1000 unless supplied; the fake host's onWake boundary requires
explicit resume and never uses real elapsed time as a task deadline.

Handler actions are a small adapter over real runtime methods:

- `complete` and `next` return literal next state through real `runtime.commit`.
  Source scheduler validation, checkpoint comparison, reservation, holds and
  reconciliation decide what persists. The driver implements none of those rules.
- `counter` increments one checkpoint field within `runtime.commit`; `spawn` creates
  a real owned task through the callback's Tx; `wait` commits a real waiting state;
  `sleep` calls real `runtime.sleepUntil`.
- `memo`, `record`, `ownedTasks`, `outcomes`, and `waitForTask` call their real runtime
  methods. Each observed result is detached, including absent versus JSON null.
- `gate` models a controlled external wait. Abortable gates observe the **actual raw
  persisted task** after the source signal fires; `tryCommitOnSignal` attempts a
  real stale invocation write and records its category/message. Gates implement
  only the fake external service boundary, not scheduler state transitions.
- `timer` uses real runtime.setTimer over the host's recording timer boundary;
  manually firing a retained host callback after disarm tests the runtime guard.
  `noProgressCommit`, `lateCommit`, `observe`, and `throw` are explicit test actions.

References such as `$t`, `$task.id`, `$task.input.owner`, `$local.child` and `$clock`
resolve only JSON values from named host results, phase inputs, local action results,
or the controlled clock. `as` stores those results. Shared `{"$undefined":true}`
and `{"$utf16":[...]}` trace conventions apply; product JSON is not extended.
The shared codec and scalar validation keep the entire file ordinary native-readable
JSON. Error reports contain category/message/type/task attribution, with no stacks
or environment-specific paths.

Expected output retains ordered host observations, phase/abort/signal/handler events,
complete task changes from commit publications, and reports. Publication subscription
begins after Harness.open; reopen reconciliation is observed through actual persisted
record/inspection reads rather than a fabricated past publication. Cleanup after the
last explicit operation is excluded from expected output. Source arrays/order remain
captured as observed; a later native adapter may map native error categories and
compare independent task collections by identity while preserving the required
per-task causal ordering and atomic commits. Native API-specific adapters must call
their actual scheduler/runtime APIs; copying final source records would not exercise
these behaviors.

## Evidence limits

`seededReopen` means valid task records were written to actual SQLite, that connection
was closed, the same file was reopened, and the actual Harness performed recovery.
It is **seeded reopen evidence**, not an observed OS/process crash. The normal-close
case separately exercises a live source invocation through real Harness.close and
SQLite reopen. Native interruption/crash proofs remain the native slice's duty.

The seeded owned-conversation case proves reconciliation/holds over an existing
ownership graph; it does not test creation of that graph through task APIs. This
suite does not execute migrations, migration callbacks, hooks, provider requests,
ordinary tools, inbox admission, CodeMode, task-document retirement, terminal-cleanup
integration for built-in tasks, browser/Cloudflare backends, ambiguous storage faults,
or every cancellation race. Signal-time stale writes and retained timers are the
bounded stale-callback cases actually observed. Memo candidates are tested sequentially
and across reopen, not as concurrent writer races. No stubbed scheduler state machine
or mock successful recovery is used to fill unobserved coverage.
