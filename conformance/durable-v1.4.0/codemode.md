# Durable CodeMode 1.4.0 observations

This independent future evidence bundle executes the published **1.4.0 Durable
CodeMode adapter** through real Harness, ToolTask, owned nested tools, task documents
and real Memory/Node SQLite. It supplies source expectations only; it implements or
accepts no native Durable CodeMode adapter.

~~~sh
node conformance/durable-v1.4.0/codemode.mjs --check
node conformance/durable-v1.4.0/codemode.mjs --causal
~~~

Explicit regeneration reads the separate input file:

~~~sh
node conformance/durable-v1.4.0/codemode.mjs --write
~~~

Checks never rewrite expectations. A disposable trace can be selected with
--fixture <path>. The causal command changes the expected committed first-issued
count observed at effect entry, invokes the actual --check command against that
copy and requires rejection. Temporary SQLite/negative-control files live under the
existing ignored .runtime directory and are removed.

The bundle freezes **21 cases / 29 source/backend executions**:

- **17 real local-worker cases / 23 executions** use the installed published
  CodemodeSandbox, real Node worker threads and its real QuickJS WASM. These
  include the journal-commit rejection case.
- **One document-policy/history/fork case / two executions** calls the actual
  CodeMode document definitions and Session/storage APIs without starting a VM.
- **Three scripted Sandbox protocol cases / four executions** emit controlled
  Started/Call/Idle/terminal/During events and record deliveries. They execute the
  actual Durable adapter, task runtime, journal commits and nested tools. Their
  script text is a descriptive placeholder and no JavaScript VM executes it.

The categories are explicit in codemode-case-index.json. Scripted protocol evidence
does not claim real VM execution. Every source expectation remains detached from
future native implementation algorithms.

## Source and package identity

All **37 prior Session18/scheduler9/runtime10 files are byte-identical** and checked
before every command. The new source pin reuses verifyEnvironment, the unchanged
lock and existing release/attestation identities, adds23 release fingerprints and
16 immutable source/test fingerprints: **349 JavaScript/manifest/WASM hashes** and
**93 tagged source/test/spec hashes** in total.

Installed @earendil-works/pi-codemode@1.4.0 JavaScript/manifest files and
quickjs-wasi@3.6.2 JavaScript/manifest/WASM files were compared byte-for-byte with
their existing npm-cache tarballs. Each tarball's SHA-512 was verified against the
unchanged lock before hashing installed files. The cache's mutable path is not part
of portable provenance; exact versions, registry URLs, lock integrity and archive
SHA-256 are in codemode-source-pin.json. No installation or dependency mutation was
used. One npm attestation metadata response was retrieved from the official registry
and preserved as codemode-attestations.json; its SLSA payload associates the locked
CodeMode package digest with immutable commit
4a080252f06caf63a525d7873c5d94d3c23887e0. Payload/digest association is checked;
cryptographic signature trust is not independently reverified. quickjs-wasi has a
separate locked third-party package identity, not the Pi commit identity.

With PIRUA_UPSTREAM_ROOT set to a Git object store, the checker additionally verifies
all93 source/test/spec hashes with immutable git show TARGET:path; mutable checkout
modules are never imported. CodeMode RunDoc/JournalDoc are imported directly from
the hashed installed release module because its public index omits these tokens.
No source patches, rebuild, complete upstream test suite, live provider request,
image generation, classifier or remote executor is used.

## Actual coverage

| Cases | Observed behavior |
| --- | --- |
| Local basic | Persisted immutable start precedes Sandbox.start; actual tool count commits precede nested effect; journal result precedes delivery; actual terminal During, output, seeded random value and injected VM clock are retained |
| Arrival order and replay | Concurrent calls finish2 then1 under explicit external gates; source journals and delivers2,1,3, reopens SQLite and replays2,1,3 before continuing4; finished unsafe tools run once, safe interrupted hold runs twice; delivery wall clocks and later VM time are retained |
| Globals and null/undefined | Global results are journaled, old globals do not rerun during replay, undefined remains distinct from JSON null and later global calls run live |
| Every held identity | First new held call could run, later held call conflicts with an existing nested key; actual adapter checks them all and admits no new echo effect. Script try/catch cannot convert host divergence to success |
| Module/fingerprint/deadline | Explicit post-close start-state perturbation or expired injected deadline causes source refusal, no new echo call, and no second VM for version/deadline failure |
| Count and terminal divergence | Stored first-issued and issued-after mismatches reject; real worker terminal During after a replayed result rejects premature completion without waiting for another Idle |
| Decided outcome | Actual afterTool hook holds after CodeMode outcome/store commit; SQLite reopen reuses that outcome and starts no second VM |
| Host shutdown and Durable abort | Real sandbox close yields an aborted VM result while the adapter records no outcome; actual Harness close/reopen resumes it. Explicit conversation abort instead ends the Durable invocation and admits no later echo |
| Late global | A real pending global returns after successful VM settlement; it causes no later journal delivery or extra outcome |
| Success-only store and fork | Two successful writes commit with decided results, a throwing script keeps the previous value, historical read and as-of fork see the earlier successful store, and the fork's real worker reads it |
| Output/images | Real VM text/console/return formatting, token-budget spill and image attachment pass through actual formatResult; a controlled file host records exact bytes and the persisted output directory before writes |
| Checkpoints | Actual task document policies keep deltas; store has64 deltas, next update creates a base, historical value32 and as-of fork survive, then a new update adds one delta |
| Journal-commit rejection | Controlled Storage boundary rejects the result-journal write before forwarding to the real backend. Actual Session fails, journal retains no entry, VM receives no delivery and second global never runs |
| Scripted count gate and late callbacks | A Call without Idle acknowledgement causes no effect; after Idle the actual persisted count precedes effect and journal precedes delivery. Call/Idle callbacks emitted after terminal cause no new effect |
| Scripted terminal/held target | Terminal During seq0 with no later Idle still rejects a replay that previously continued; a held global replacing an earlier keyed tool rejects before the global runs |

Full source publications retain document values/operations, task checkpoints,
nested-call documents/retirement, entries, receipts and atomic store/outcome commits.
VM events retain complete start options, fingerprint, calls, Idle acknowledgements,
deliveries with the latest **committed publication** journal, and complete terminal
results. Effect-entry observations retain the actual raw stored nested task and
committed owner start/journal. The driver does not infer journal state or reimplement
count, replay, identity or storage policy decisions.

Existing owned nested tasks can resume independently during reopen reconciliation.
Thus refusal fixtures can show the already-admitted safe hold executing twice,
while proving that the adapter admits no new echo/global call. They do not claim
that refusal retroactively prevents an existing scheduler task's own recovery.

## JSON and host boundary

fixtures/codemode-schema.json defines the language-neutral envelope. Input operations
call real APIs for submit/wait/status, task/document/entry/store/as-of reads, fork,
abort, normal close/reopen and explicit state perturbations. untilEffects,
untilVm, untilJournal and untilDecision wait for actual events/state. The driver
uses a bounded real-time startup deadline only to detect a stuck test; task deadlines
and journal clocks use the separate injected wall clock.

A local input's code is passed verbatim to the actual published VM. JSON operation
descriptors are not evaluated as host JavaScript. Tool/global descriptors implement
controlled external effects and gates; actual nested ToolTask APIs decide identity,
replay and settlement. Scripted descriptors contain ordered literal protocol
events, optionally emitted in response to source Run.deliver. They contain no journal
or replay state machine. References such as $input, $first.answer, $args.gate,
$run, $delivery.outcome.value and $clock perform JSON value lookup only.

patchRun and patchJournal mutate actual document drafts in actual Harness commits
after a live normal close/reopen. These explicit fixture perturbations simulate
incompatible/corrupt stored evidence or altered deterministic start input to discriminate
refusal paths. They are labeled in the index; they are not observed process crashes
or ordinary live user operations. No raw seeded task is presented as live execution.

The controller approved a narrow process-local entropy host: only
crypto.getRandomValues(Uint32Array(1)), the adapter's persisted seed draw, returns
fixture seed424242; other shapes delegate to the original function. The original
property descriptor is restored at case end. performance.now is a nondecreasing
constant-zero test host for tool/VM-host duration and is similarly restored.
Scheduler wall time starts1000 and changes only through explicit advance.
The real conversationCreated callback commits explicit ProviderDoc session IDs;
the scripted external provider supplies fixed message timestamp and duration zero.
No default entropy, seed security, UUID or elapsed-performance property is claimed.

The file environment is an in-memory controlled ExecutionEnv boundary, not a native
filesystem integration test. It assigns deterministic /oracle/code-output paths
and retains exact text/image bytes. Actual outputDir persistence, formatting,
deduplication and writes are performed by the adapter. The fake provider transports
only CodeMode calls and final answers; no network or credentials are involved.

Undefined host results use {"$undefined":true}. The frozen trace codec reserves
{"$utf16":[code units...]} for any non-scalar JavaScript string value. These are
lossless trace markers, not additions to product JSON. Scalar Unicode validation
covers every value/key before writing/comparison. Error reports retain category/
message without host stacks; actual script output, including its deterministic
VM stack where present, remains unchanged.

## Evidence limits and native comparison

All recovery cases first run an actual adapter invocation, perform actual normal
Harness.close and reopen the same SQLite file. The host-shutdown case additionally
closes a real worker sandbox before closing Harness. Some refusal cases explicitly
perturb persisted document drafts after reopen. There is no OS/process-crash claim
and no fabricated successful recovery. Publication subscription begins after open;
reconciliation is observed by actual reads and later publications.

Observed source QuickJS fingerprint bytes are retained. No native QuickJS
fingerprint-byte parity is claimed. Future native tests must prove their own stable
identity/mismatch refusal, journal-before-delivery, count-before-effect, every-held
identity gate, real worker cleanup and atomic successful store through actual APIs.
They must use native emitted operations and independently replay them;
copying final source state would not exercise these behaviors. This bundle does
not accept that future comparison adapter.

Unobserved: complete replay-corruption matrix, every storage/cancellation race,
ambiguous commits, hard process crash, every terminal During/count permutation,
concurrent same-key admission races, worker resource limits/CPU timeout/memory failure,
real filesystem spilling/recovery and cross-process file helper identity, global
search/catalog/reload variations, automatic model catalog/classifier/image-generation
helpers, remote sandbox, CodeMode through native extension transports or Coding
bindings. VM source-error stacks are observed output, not host report stacks.
Separate Session/scheduler/runtime bundles retain their own bounded evidence.
No new source/spec contradiction was found in these CodeMode cases.
