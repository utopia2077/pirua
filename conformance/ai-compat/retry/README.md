# Stateless assistant retry evidence

Source authority is pi-mono853a80d26c90a14c1886f0ebb8ffaae133ca2185,
`packages/ai/src/utils/retry.ts`, read from Git objects. `source.mjs` executes the
complete TypeScript-stripped module, including its classifier and private sleep.
Only producer, optional callbacks and host timers are deterministic fixtures.
No Faux provider implementation or live provider service is used.

`cases.json` names the exact14 frozen S05 retry-loop rows and10 extra rejection/
cancellation controls. `source-observations.json` records source hash, complete
ordered traces and final response observations. The Rust tests compare every
row; each frozen row has its own executed test. Unknown fixture capability names
fail; cancellation controls assert timer entry and delivered abort, not arbitrary
rejection. `causal.mjs` mutates only a child-loaded source string and proves four
specific comparisons fail for wrong retry count, missing attempt-start ordering,
wrong billing classification and wrong sleep duration. Git source is unchanged.

Raw delay and native effective duration are separate. The oracle runs1025 retries
for bases0,1 and MAX_SAFE_INTEGER and records every scheduled f64 as hexadecimal
bits (NaN/Infinity have explicit tags); scheduled and slept delays agree. Rust
compares all3075 observations. Its non-Serde RetryDelay has no unchecked public
constructor and exposes raw_milliseconds, preserving zero-times-infinity NaN.
Node's actual timer handles establish admitted delays at zero, ordinary values,
fractional milliseconds, INT32_MAX, above INT32_MAX, infinity and NaN. The exact
Node version and internal/timers source hash are retained; that source separately
confirms truncation at insertion. ClockRetrySleeper applies this proven Node rule
and the existing Clock cancellation contract. Scheduling latency is not promised.

Cancellation is observed only at the source sleep stage. Producer or observer
rejection propagates at its original stage. Sleep failure first invokes finish;
a rejected finish supersedes the sleep result. Only typed RetrySleepError::Cancelled
normalizes the previous response to aborted, retaining content/usage and supplying
`Retry backoff aborted` because the inherited strict DTO requires an error message.
The source's omitted message remains visible as null in the observational fixture;
this one explicit field normalization is asserted before comparison. No policy,
classifier, core DTO or overflow caller behavior changes. The fifteenth frozen
`retry-loop.overflow-precedence` remains S15.

Run offline:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-compat/retry/source.mjs
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-compat/retry/causal.mjs
cargo test -p pi-ai --test retry --locked --offline
```

Four-way variance: all14 planned rows implemented; none missing in this unit;
extra failure/numeric/causal controls implemented; no durable retries, provider
registration, foreign callback identity, excluded provider products or overflow
caller implemented. Trusted host panics, live providers, long elapsed backoff,
and non-Node timer semantics remain outside the claim. Services are borrowed and
all waits remain in the returned future; no detached task or retained global state.
