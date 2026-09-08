# Azure OpenAI Responses evidence

## Corrected assembly

The owning feature is rebuilt on accepted base `0eb4120`, including ordered raw
request headers, native semantic response headers, Fetch content composition and
the shared OpenAI SDK string projections. Azure raw headers consume
`iter_for_dispatch`; SDK media/env/custom-record handling consumes the unchanged
private `openai_sdk_strings` helper. Azure's separate pi scoped configuration
fallback remains truthy and untrimmed until its existing field-specific steps.

`correction-oracle.mjs` executes the pinned Azure wrapper and installed SDK for
53 header/media/env cases and13 settled-custom-record cases, in two separate
fixtures. These compare complete requests, callback order, full event snapshots,
both final classifiers and body closure. They include reversed case-distinct raw
keys and final null, all25 ECMAScript whitespace characters, NEL/NBSP media
controls, eager/lazy header validation, exact-key overwrite slots, numeric own
keys, __proto__ omission and invalid values overwritten before validation.
The original75-case trace, original standard evidence and SDK126 expectations
remain byte-unchanged. Expected results are produced only by source execution.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/azure-openai-responses/correction-oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/azure-openai-responses/correction-oracle.mjs --custom-record --check
```

Scripted SDK/custom-fetch fixtures explicitly use Preconstructed response
composition. Native loopback remains RawNetwork and exercises the accepted Fetch
composition; no network defaults are added to the source's injected Responses.
The historical separate-integration caveat below describes the original feature
receipt, not a missing dependency of this corrected assembly.

Authority is pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`, complete
`packages/ai/src/api/azure-openai-responses.ts` and relevant lazy/shared helpers,
plus installed OpenAI6.40.0 `src/azure.ts`, Responses resource, APIPromise,
default parser, error/header/query helpers. Current upstream files and docs
are not substitutes for this pinned behavior.

`oracle.mjs` executes the exact pinned Azure wrapper and actual AzureOpenAI SDK
with synthetic injected Fetch responses. It records75 request cases and332 full
push-time event snapshots, with source helper hashes. Expected data never comes
from Rust. Run:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/azure-openai-responses/oracle.mjs --check
```

The child environment is explicitly empty. Each fixture ambient map is installed
only inside that child and removed after its case; global Fetch is disabled.
Timestamp7, portable Pi User-Agent and omitted SDK platform identity headers are
declared normalizations. Scratch block fields are excluded from owned snapshots.
Error prose/diagnostics are normalized in comparisons only after independently
checking both source retry and overflow classifiers. Other snapshot fields and
opaque signature strings remain compared. SDK numeric variants compare through
core DTOs; raw SSE integral decimal/exponent spellings remain in source inputs.

## Azure-specific obligations

- Raw and simple require an explicit resolved API key; header-only auth is not
  the standard Responses placeholder path. SDK uses api-key, not an invented
  Authorization bearer header. Explicit user headers may still add Authorization.
- Option/env/resource/fallback precedence is source-derived. Scoped empty strings
  fall back to the injected ambient map, but whitespace is truthy before later
  field-specific trimming. API version defaults to v1. Deployment mapping uses
  source comma/equals splitting, trimming, last value and empty-value fallback.
- Effective fallback URL is the nominal RequestEndpoint, never rewritten Model
  data. Azure hosts normalize only the exact source path set, clear query only
  in that branch, and retain fragments. Proxy/existing custom paths preserve
  their query semantics. SDK api-version default query wins after path-query
  parsing. `/responses` is NOT a deployment-rewrite endpoint; deployment selection
  changes payload model only, even with sampling/replacement overrides.
- Prompt cache key remains present with cacheRetention:none; no standard24h
  retention, session-affinity defaults, Copilot headers or xAI include branch is
  invented. Azure sends all tools without standard deferred-placement modes,
  defaults strict support true, preserves grammar input and includes Azure in
  tool-ID replay compatibility. Sampling params remain last.
- No Azure service-tier option or standard tier-pricing policy is added. A wire
  service_tier from sampling and a response priority tier do not multiply costs.
- Original asynchronous API setup/error paths are in-band. The source simple
  synchronous missing-key throw becomes the required nominal in-band setup error.
  The native lazy factory is passive construction; no module-loading capability,
  global registration, client I/O or credential discovery is invented.

Raw Azure options have strict Serde and globally redacted Debug as approved by
the controller. Raw input serialization is NOT authority to persist credential
URLs or pass them through foreign extension grants. Resolved config only holds a
non-Serde RequestEndpoint, never writes back into the option/model, and never
formats raw URL text into errors or telemetry. A synthetic credential-query test
proves exact source URL preservation while model/debug/error views remain clean.
S05 owns credential discovery and provider/facade factory composition; the private
API config/env helpers are fully implemented here, not deferred or new exports.

## Shared execution and proof boundaries

`api/openai_sdk.rs` is a private extraction of the accepted standard execution.
Each feature independently enables the private engine; Azure does not enable the
standard family. Standard public type paths re-export the same SDK option types.
Family payload/config/header/pricing policy remains outside the shared engine.
The private family tag selects actual normal HTTP error prefix and safe diagnostic
label, not another retry/HTTP implementation.

The complete attempt calls transport send_fetch with lower retry disabled and the
SDK600000ms headers-only timeout. Failed body consumption and SDK error formation
precede retry; non-streaming decode/addOutputText/primitive-operand classification
precede onResponse and Start. Decode failures stay Other, no request retry.
Successful SSE is outside retry, uses the accepted OpenAI decoder and shared
Responses reducer, retains partial snapshots, and consumes through EOF/DONE.
Normal/cap errors are separately classified before redaction; Azure does not
append Completions metadata.raw. The original standard68-case decode corpus also
runs through the Azure-only public adapter because the installed SDK resource
and status-less error formatter path are identical.

Focused tests cover raw option admission, passive/default dispatch, simple auth,
source request/snapshot corpus, exact query custody, payload/response cancellation,
usage-only abort/drop, failed-body EOF-before-retry and no successor on cancellation.
Native loopback captures actual Azure POST path/query/api-key and JSON body, feeds
three-byte fragmented chunked SSE, and requires peer EOF/reset on success/abort.
A causal mutant adding `/deployments/incorrect` fails the source default-request
assertion; restoring the normalization restores green tests. Standard source
fixtures and test assertions are unchanged by extraction and remain regressions.

SSE/body/aggregate resource bounds are inherited from the accepted runtime/shared
processor; no second HTTP client, timer or retry owner exists. Shared Fetch codec/
default-header integration is separate controller-owned S04 work, not claimed from
this family loopback. No live Azure/provider/TLS/account interoperability, ADC/OAuth,
provider registration, catalog or S05 facade breadth is established here.

Full acceptance remains `./scripts/verify.sh`. Focused iterations:

```sh
cargo test -p pi-ai-providers --features wire-azure-openai-responses,native --test azure_openai_responses --locked --offline
cargo test -p pi-ai-providers --features wire-openai-responses,wire-azure-openai-responses,native --test openai_responses --test azure_openai_responses --lib --locked --offline
```
