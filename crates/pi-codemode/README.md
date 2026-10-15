# pi-codemode

A Rust library and companion Rust executable for local JavaScript tool orchestration.
Each script runs in one fresh QuickJS-NG process through pinned `rquickjs` 0.14.0.
The crate has no Agent, CodingSession or Durable dependency. Runtime adapters own
schema validation, permission checks and conversion to their tool result types.

Build and package **both** the host library and its matching `pi-codemode-worker`
executable. The host supplies the executable path explicitly; there is no PATH
lookup, bundled Node runtime, downloaded runtime or automatic helper installation.
For a local checkout (use the repository's prescribed build environment first):

```sh
cargo build -p pi-codemode --bin pi-codemode-worker --locked
cargo run -p pi-codemode --example local --locked -- \
  "$CARGO_TARGET_DIR/debug/pi-codemode-worker"
```

The example declares `echo`, evaluates a script, dispatches the detached JSON call,
and prints its typed result. A production packager should install the helper beside
its application and pass the resolved application-relative path to
`SandboxOptions::new`. A missing helper, exhausted admission capacity or invalid
start data returns `BoundaryError` before a run is returned.

`Sandbox::execute` drives host callbacks concurrently, bounded by
`max_concurrent_tools`. The callback receives a `HostCall` and a cooperative
`Cancellation` signal. Tool and global declarations are explicit capabilities;
provide only the callbacks a script is allowed to invoke. Callback arguments,
results and error properties cross a JSON boundary. `Option<Value>::None` represents
JavaScript `undefined`; `Some(Value::Null)` represents JSON `null`.

Scripts call tools through normalized property names or bracket notation for their
original names. Both forms take one argument. Scripts also have a
frozen `ALL_TOOLS` array, declared globals (including `namespace.member` and spread
arguments), `text`, `console.log/info/warn/error/debug`, `image`, `exit`, `store` and
`load`. `image` accepts tool-returned image blocks and base64 data URLs; the actual
PNG/JPEG/GIF/WebP signature determines its MIME type. HTTP image URLs are rejected.
There are no model image generation or classifier helpers.

Output survives script errors. Store snapshots and writes are detached JSON;
`load` returns a fresh copy. Only `ExecutionResult::Success` carries `StoreWrites`.
`store(key, undefined)` deletes a key; persisting writes atomically with a runtime's
successful result remains the caller's responsibility. `exit()` succeeds with an
undefined value and the writes made so far. A script waiting on a promise with no
pending host call fails because there are no timers that could resume it.

The host starts the helper with a cleared environment, only `TZ=UTC`, `LANG=C` and
`LC_ALL=C`, a temporary working directory, piped protocol stdin/stdout and discarded
stderr. The VM installs no filesystem, network, process, credentials, timers,
module loader, `WeakRef` or `FinalizationRegistry` bindings. The bridge stays in a
private closure, and the upstream Pi prelude freezes built-in objects so scripts
cannot change its serialization machinery. This is a capability-restricted native
VM in a separate process; it does **not** provide an OS sandbox against native
engine vulnerabilities. The helper itself remains a trusted application binary.

Cancellation, deadlines, `Sandbox::close` and dropping a `Run` request process
termination. The supervisor kills and reaps the actual helper, including a script
spinning inside a synchronous built-in or a microtask loop. `Run::is_finished`
becomes true when an outcome is decided. `Run::wait` returns after process reaping
and admission release. `close` also waits for all admitted children to exit.
Unawaited calls are marked cancelled at script completion, and host callbacks'
cancellation signal is set. Host callbacks must cooperate with that signal: their
host-owned threads are outside the JavaScript process and cannot be forcibly
stopped by killing the VM.

Default admission is four children per `Sandbox`; cloned sandboxes share it.
Default VM heap is 64 MiB and native stack guard is 256 KiB. The configurable stack
range is 64 KiB–1 MiB. Execute defaults to a 300-second deadline; controlled start
has no deadline unless its `RunOptions` supplies one. Resource limits also include:

| Boundary | Limit |
| --- | --- |
| Text and base64 output | 16 Mi UTF-16 characters, 100,000 items |
| Stored value JSON | 256 Ki UTF-16 characters |
| Total live store keys and JSON | 1 Mi UTF-16 characters |
| Script source, declarations, each tool argument/result | 1 MiB UTF-8 |
| Calls per run | 4,096, tools and globals together |
| Aggregate call arguments and delivered results | 16 MiB UTF-8 each |

These bridge limits bound host-side event and answer buffering as well as VM
memory. Output-limit failures remain terminal if the script catches their error.
Initial store limits are checked before launch. Tool results exceeding a delivery
limit return `BoundaryError` without consuming the waiting call or sequence number.

The native JSON domain permits at most **256 nested arrays/objects** per boundary
value, counting its root container and excluding wire envelopes. This applies to
script return values, stored values, tool/global arguments, initial store values
and delivered outcomes (including an error's properties object). Strings and object
keys must contain Unicode scalar values: a valid UTF-16 surrogate pair becomes its
UTF-8 character, while a lone surrogate is unsupported by Rust/Serde strings. Such
script values raise a `TypeError` whose message starts with `CodeMode JSON domain`.
Validation occurs before store mutation, call ID/pending allocation, or terminal
ownership. A caught invalid call leaves the next valid call's ID unchanged; a failed
return still reports one typed terminal result with earlier output/call records.
Host initial-store and delivery depth violations return `BoundaryError` before
admission or consumption, allowing a corrected delivery for the same call/sequence.

Serde's default decoder limit is 128 levels. Internal decoding replaces that
incidental limit only after an iterative structural depth scan; it does not accept
unbounded recursive values. A consumer decoding the serialized deep result with its
own default Serde decoder must configure a suitable bounded decoder itself. The
256-level domain and Unicode restriction are deliberate native adaptations from
upstream's JavaScript `JSON.parse` boundary; arbitrary JavaScript UTF-16 strings and
unbounded JSON nesting are not promised.

After an execution is admitted, the convenience driver converts a rejected host
callback outcome into a bounded, catchable tool error rather than returning an
admission error and dropping the run. If the remaining aggregate delivery budget
cannot admit even this rejection, the driver settles a typed `sandbox` error. Both
paths retain output/call records, preserve success-only writes, signal cancellation
to outstanding host work and wait for child exit/admission release before returning.

`Sandbox::start` and `Run::deliver` expose the controlled execution seam for a future
Durable adapter. Call `next_event` to consume events in VM order:

1. `Started { fingerprint }` precedes script execution and all host calls. Record or
   compare it before admitting any tool effects.
2. `Call { id, target, name, args }` identifies the next invocation-local call,
   numbered 1, 2, … across tools and globals. Arguments have already crossed JSON.
3. `Idle { seq, issued }` acknowledges a promise-job drain: `seq=0` is the initial
   script, later values are delivered answers, and `issued` is the total call count.

`deliver(seq, now, id, outcome)` accepts one answer for a waiting call and a new
positive sequence number. The helper buffers gaps and applies results in sequence
order even when delivered out of order. The virtual clock starts at `started_at`
and advances to the maximum of its current value and each applied result's `now`.
`Date.now`, no-argument `Date`, and `performance.now` observe that clock. `Math.random`
uses the supplied 32-bit seed and the fingerprinted mulberry32 algorithm. Tool errors
reject with an `Error`; JSON error properties are attached except `name`, `message`
and `stack`. A script's own result reports `during { seq, issued }`, including when
it finishes during the last drain without an `Idle`. Host cancellation/timeouts
carry no `during`. Delivering after completion is a no-op.

Replay consists of starting the same source, declarations, store, `started_at`, seed
and resource limits with the same helper fingerprint, then replaying the same
outcomes with the same sequence numbers and clocks. The fingerprint includes the
SHA-256 of the **actual helper executable**, the backend identity, target OS/arch,
heap and stack limits, and protocol version. It consequently changes on a different
helper build, including bridge/prelude changes; a durable host must refuse a mismatch.
This crate does not write journals, retry effects, resume processes or accept a run
as replay-equivalent on behalf of its caller.

The JavaScript prelude is adapted from Pi v1.4.0
`4a080252f06caf63a525d7873c5d94d3c23887e0`,
`packages/codemode/src/runtime/prelude-source.ts` (MIT). The controlled event and
delivery contracts follow that release's `types.ts`, `runtime/protocol.ts` and
`runtime/vm.ts`, with its Durable consumer consulted only for the journal seam.
This backend differs from upstream QuickJS-WASI: engine-specific error stacks,
arbitrary Date parsing/locale details and random sequences are not interchangeable.
Replay is scoped to the exact local fingerprint. Standard UTC dates, controlled
clock progression and seeded randomness are exercised against real helper processes.
The executor convenience path uses wall time and native randomness, as upstream does.

Dependency behavior was checked against current official rquickjs documentation via
Context7, especially
[`Runtime` limits, interruption and pending jobs](https://github.com/DelSkayn/rquickjs/blob/master/_autodocs/api-reference/runtime.md).
The implementation uses a killable process for cancellation rather than relying on
native interrupt polling. Tests launch the compiled helper and cover actual process
exit, capability boundaries, output/store/heap limits, tool images and errors,
controlled result reordering, deterministic replay inputs and successful write semantics.
