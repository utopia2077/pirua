# Complete provider-request retry

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`, complete
`packages/ai/src/utils/provider-retry.ts`, and OpenAI 6.40.0
`src/client.ts:805-874` / `src/core/api-promise.ts` withResponse stage. Error
response text is read before the SDK promise rejects. The original cap witness
is also retained in the standard Responses wrapper's `retry-cap.json`.

`HttpTransport::retry_provider_request` owns retries around the complete supplied
operation. `ProviderAttemptFailure::Provider` explicitly supplies optional SDK
status and headers with an opaque family error; `Other` is never retried. The
helper returns original attempt failure, cancellation, or policy failure. Policy
failure retains the preceding family value and metadata without formatting it.
Family errors require Send only, with no Clone/Debug/Error/Serde bound. All
container diagnostics are redacted. Callbacks are trusted in-process Rust values.

The operation must call lower HTTP with `RetryProfile::None`, read/classify failed
bodies and then return the typed failure. A successful response/body leaves the
helper permanently. Existing status-only `send` behavior is intentionally
unchanged; the controller will move SDK-backed wrappers onto this owner and
compose Fetch redirect handling within each lower attempt.

`oracle.mjs` executes exact pinned retry policy functions with injected jitter
0.4 and clock zero, retaining 31 status/header/delay/cap/backoff vectors. It also
uses the real installed OpenAI client and controlled local Response body to show
attemptsBeforeErrorBody=1 and attemptsAfterErrorBody=2. The retained cap-stage
probe produces one attempt and the exact message
`Server requested 1s retry delay (max: 1s). 429 context length exceeded`, with
both retry and overflow classifiers true. No expected output comes from Rust.
Run with `env -i PATH=/usr/bin:/bin node conformance/ai-transport/provider-retry/oracle.mjs --check`.
Global fetch throws; every SDK client receives an injected local fetch; the
script rejects provider environment values.

Focused tests consume these fixed vectors and additionally prove missing SDK
metadata, zero default, non-provider failures, maximum safe retry admission,
cap zero/inclusive/exceeded, invalid jitter, opaque ownership/drop, non-cooperative
attempt/backoff cancellation and future drop, post-result cancellation, failed
body read-before-retry, cap preservation of family overflow/retry, and inability
to retry a returned successful body that later fails. The actual lower send/body
ports are composed by a boxed borrowed function with the intended adapter shape.

Retry eligibility and delay calculation reuse existing transport policy. Only
three retry-relevant header names receive Fetch edge-space/tab trimming and
comma joining through the accepted `get_for_fetch` projection. The helper keeps
only those three fields for policy inspection; it does not duplicate Headers.
Explicit clock/date/jitter ports, timer normalization and safe maximum limits
remain the existing accepted transport contract. Cancellation priority before
invocation and after a ready success follows the stronger S04 cancellation
contract rather than JavaScript's cooperative signal behavior.

Raw maxRetries admits the full safe-unsigned maximum, 9007199254740991. It is
not the durable RetryPolicy-to-maxAttempts normalization and does not reserve
one unit for a public one-based attempt count. The oracle executes the exact
pinned helper at MAX with immediate success and one transient failure followed
by success (one/two calls, no long loop). Rust tests consume those observations
and reject MAX+1 before invoking the operation as an out-of-domain raw budget.

Opaque failure retention does not mean reusing the original classifier flags.
Adapters must classify the actual delay-cap-prefixed error shape (or precompute
that projection before discarding raw text). The prefix can enable retry, a
billing/quota veto can still prohibit it, and an anchored overflow pattern can
stop matching. Transport assumes neither retry nor overflow is invariant under
the family error transformation and never formats the opaque failure itself.

Acceptance: `cargo test -p pi-ai-transport --test provider_retry`, the offline
oracle `--check`, and the sole full gate `./scripts/verify.sh`.
