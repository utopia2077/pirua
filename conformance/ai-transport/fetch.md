# Replayable Fetch request composition

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185` plus the
installed SDK/runtime sources recorded below. This is derived transport behavior,
not a new provider or a general browser Fetch implementation.

## Evidence and reproduction

Run `node conformance/ai-transport/fetch-oracle.mjs --check` with the inspected
local pi-mono checkout at `upstream/pi-mono`. No install, credentials,
public endpoint, or provider API is used. The expected JSON is generated only
from those JavaScript oracles, never Rust output. The check asserts provenance
versions/source hashes and exact normalized traces. Synthetic body values and
header names are recorded; random listener ports and credential values are not.

The two independently executed runtimes are Node **26.8.1**, bundled Undici
**8.10.0**, and installed npm Undici **8.9.0**. They agree on this finite corpus;
they are not described as the same package version. The corpus covers all five
redirect statuses with POST/PUT/HEAD; twenty/twenty-one hops; normalized request
method cases; absent/empty/duplicate/relative/query/fragment/invalid/high-byte
Location; origin changes; credentials and unsupported schemes. Header projection
checks include mixed-case Cookie and Set-Cookie, including an actual response.

Installed Undici source anchors:

- `lib/web/fetch/index.js:1252-1393`, `httpRedirectFetch`: hop bound, method/body
  rewrite, five body-field deletions, four cross-origin sensitive-field deletions.
- `lib/web/fetch/index.js:1578`: delete caller Host before network transmission,
  including the first hop. The native backend regenerates the target Host.
- `lib/web/fetch/util.js:25-52`, `responseLocationURL`: joined Location, Latin1
  octet recovery then UTF-8 replacement decode, relative resolution and fragment
  inheritance. Buffer decoding retains a BOM; no text-normalization shortcut.
- `lib/web/fetch/headers.js:236-255`: Cookie joins with `; `, other fields with
  `, `. Raw transport fields are not collapsed or rewritten by the accessor.
- `lib/web/fetch/request.js:146,374-402`: credential/method validation;
  `lib/web/fetch/constants.js:68` lists body headers; its bad-port list is
  reproduced explicitly. `util.js:721` and core normalized method records govern
  method casing, including QUERY and preserved PATCH casing.
- OpenAI **6.40.0** `src/client.ts:750-798` and Anthropic **0.91.1**
  `src/client.ts:650-695`: failures from Fetch are wrapped as SDK connection
  errors. Redirect-generated network failures therefore use `Connection`, while
  arbitrary raw `Protocol`/`InvalidRequest` failures are not made retryable.
- Initial HTTP credential and blocked-port failures also occur inside Fetch:
  Undici `index.js:166-175,583-587`, `request.js:146-153`. Both SDKs wrap these as
  `APIConnectionError`, and the pinned helper invokes Fetch twice with one retry.
  Malformed URL syntax fails SDK buildURL before Fetch (OpenAI `client.ts:716`,
  Anthropic `client.ts:623`), with zero Fetch calls. The retained oracle exercises
  both SDKs against both Fetch runtimes and asserts a rejecting dispatch sentinel
  is never invoked. This is zero network dispatch, not merely a socket failure.
- Pinned `packages/ai/src/utils/provider-retry.ts`: the executed helper accepts
  `Number.MAX_SAFE_INTEGER` retries on initial success, and observes joined
  `x-should-retry`. Pre-change Rust regressions failed both observations. The
  correction is confined to transport request policy, not durable core retry DTOs.

## Boundary

`HttpTransport::send_fetch` has the same request/runtime/retry/timeout arguments
and return type as `send`. `send` and `RawHttpClient` still do not redirect.
`with_fetch_redirects(FetchRedirectMode::Manual)` opts trusted binding transports
out; Follow is the default. Manual does not add a native fallback. Finite request
validation and header projection still apply in Manual mode.

Each attempt owns one acquisition timer across up to twenty redirects, replayable
bytes and selected proxy. Each retry starts at the original URL, headers and body
with a fresh hop count/timer. Every hop is separately cancellation/deadline-raced,
including a non-cooperative injected send. Discarded response bodies are closed
without reading them. Final body failures cannot enter the retry loop. Whole-body
timeout/cancellation remains attached to the original runtime by the existing
managed body; a passed-in replacement runtime cannot detach it.

Normalized-origin changes remove Authorization, Proxy-Authorization, Cookie and
Host, but preserve custom API-key fields. This is source behavior, not a blanket
credential policy. The proxy is explicitly selected once by the host and retained
across hops; no process environment lookup or new network capability is added.

The finite provider domain is HTTP(S), validated method tokens and replayable byte
bodies. Empty `Vec` means absent body for GET/HEAD; it cannot preserve JavaScript's
distinction between absent body and an explicitly supplied empty byte/string body.
Initial URL syntax and canonical-domain preflight failures are `InvalidRequest`.
Parsed HTTP URLs containing credentials or blocked ports instead fail inside the
source Fetch stage and are `Connection`, without invoking the raw client. They
receive one backoff with one configured retry; caller cancellation ends that
backoff or prevents the first attempt. Running before raw send does not itself
make a failure a nonretryable configuration error.
Malformed/forbidden redirect targets and hop exhaustion are Fetch network errors
eligible for a selected shared retry. Errors expose only reviewed kind, no raw URL,
headers or body. The module does not claim arbitrary RequestInit, streaming upload,
browser CORS/cache/referrer/cookie-jar, default header synthesis, decompression or
response-text decoding fidelity. Those remain distinct adapter/backend duties.

## Rust checks

`cargo test -p pi-ai-transport --test fetch --features native --locked --offline`
tests source-derived redirect/header/method cases, both reproduced policy defects,
original-request/hop-count retry, malformed/hop-limit retry, intermediate closure,
shared deadline, non-cooperative cancellation/drop, and real production HTTP and
explicit proxy chains. Real network probes prove original credentials restored on
retry but stripped at the other origin, POST-to-GET rewriting, final-body failure
not replayed, and pending socket release on cancellation/drop. Existing native
HTTP/TLS/H2/proxy/status-head/WS and portable tests remain in the full gate.

Acceptance gate: `./scripts/verify.sh`, offline, default and all features.
