# Typed lazy helper and text factories

Authority is pi-mono853a80d26c90a14c1886f0ebb8ffaae133ca2185, read exclusively
through Git objects. `oracle.mjs` executes the complete pinned `api/lazy.ts` and
`utils/event-stream.ts` bodies after Node's TypeScript erasure, plus all ten text
`*.lazy.ts` factory bodies. The sole family service substitution replaces dynamic
imports with a controlled host module loader (and supplies the source module URL);
no SDK import, live provider, credential discovery or registry runs. Synthetic
clock, models and modules make the trace deterministic.

Reproduce with:

```
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/lazy/oracle.mjs --check
```

`trace.json` retains actual source observations: usage-only cancellation17;
synchronous per-operation loader invocation; advertised capability presence and
missing-method failures; ten factory raw/simple traces; global Bedrock sampling
with a pending fallback, replacements and reset; repeated uncached failure. The
source's broader iterable witnesses are retained as explicit domain exclusions,
not silently converted into terminal events in Rust.

Rust exports `api::lazy::{lazy_stream,lazy_api,LazyApiLoader,LazyApiCapabilities}`
and ten snake-case factory functions in that same module. Every factory accepts
an explicitly configured loader returning the actual trusted `ApiAdapter`; this
keeps availability, module construction and caching with the host. There is no
stub provider, factory registration, native fallback or import cache. Capabilities
are explicit, default false, and exposed through `LazyApi::capabilities` because
the existing object-safe `ApiAdapter` method inventory is fixed. Bedrock's global
setter accepts `None` for the source's undefined/reset selection. Module owners
and loader calls are detached from its mutex before invoking/dropping user code.

The admitted setup domain is an owned future returning a canonical validated
`AssistantStream` wired to the original request cancellation token. The private
outer races cancellation only while setup remains pending. Once polling yields a
child, the child owns cancellation; even a token cancelled during that completing
poll must preserve its usage-only terminal. Typed setup errors use fixed safe
zero-usage text. Setup/load callbacks are trusted, synchronous, bounded and
nonblocking; Rust does not claim arbitrary synchronous panic/JavaScript throw
parity. A setup cancellation wins before polling an already-ready future if the
original token was already observed cancelled. The helper adds no timeout.

There is one setup/forward task per stream and no loader cache, polling task pool
or auxiliary event queue. Core owns the existing stream queue and validated
terminal/result equality. Losing setup, rejected/dropped tasks and forwarded
children release via ordinary ownership. Dropping only the consumer does not
cancel detached setup/forwarding, matching the source; caller cancellation or
host executor shutdown remains the termination authority. Cancel-deferred invokes
load before returning its fallible future; polling/drop follows the existing Rust
future contract, not eager unawaited JavaScript-promise scheduling.

Tests use controlled executor polls and owned futures, with no sleep or live I/O.
They cover both handoff orders, retained prefix, noncooperative setup cancellation,
spawn rejection/drop, all operation delegation, false/true capabilities, repeated
failure, all ten factories, and Bedrock replacement/reset. A causal mutation that
binds the outer to the original token makes the unchanged usage17 test fail;
restoring the private outer makes it pass.

The two image export occurrences remain S05. Arbitrary iterables with optional
results, result-only completion, bare EOF/pending result and thrown values are
outside the typed domain. The current ModelsRegistry wrapper still has the
separately measured usage0 cancellation composition; S05 owns that compatibility
correction. These direct adapter tests do not certify assembled registry snapshot
preservation. Full repository acceptance uses only `./scripts/verify.sh`.
