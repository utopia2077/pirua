# Selected OAuth flow conformance

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
The production constructors are `AnthropicOAuth::new(OAuthServices)`,
`OpenAICodexOAuth::new(OAuthServices)`, and `OpenRouterOAuth::new(OAuthServices)` in
`pi_ai_providers::auth::oauth`. Each implements the existing `OAuthAuth` trait.
Construction performs no login, registration, environment lookup, or host action.
The three `oauth-*` features enable only the existing `url` dependency. Explicit
`auth-native` forwarding selects `native` and transport `auth-native`; selecting
an OAuth flow does not implicitly select a native backend or another provider.

## Evidence and source execution

`source.mjs` loads complete unmodified pinned Anthropic, Codex, and OpenRouter
flow bodies plus their actual provider-env, PKCE, callback-page, and device-code
helper modules from Git objects. Runtime type erasure does not extract or replace
function bodies. Only host services are synthetic: HTTP, listener requests and
replies, crypto entropy/SHA256, interaction, wall time, and timers. The configured SHA256
service returns the standard Node digest through an asynchronous boundary; it
does not depend on thread-pool scheduling to settle within the driver turns. Codex's dynamic
Node crypto import is explicitly redirected to the synthetic host; an already
cached real Node crypto module cannot provide unrecorded entropy.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/oauth-selected/source.mjs
```

The committed JSON binds the pin, Node 26.8.1, seven actual module hashes, 49 input
scenarios and their observed outputs. Source reexecution compares the complete
JSON. The Rust differential test separately drives the production flows against
explicit synthetic capabilities and compares ordered fetch headers/body bytes,
body-read presence, prompts, auth/progress events, complete callback HTML and
configured response headers, manual cancellation, listener release, device sleep
intervals, credential fields and request-auth derivation. Auth URL/state/PKCE,
code selection, token request order and callback status are not normalized away.
The source never calls account endpoints or reads ambient credentials.

`oauth_selected_complete_source_trace_parity` owns all 49 traces and the explicit
three inherited numeric admission controls below. It does not claim 49 separate
Rust test functions. `oauth_selected_full_pinned_source_reexecutes_with_exact_hashes`
executes the source driver rather than treating a stored export row as behavior.
All three flow features are required by these cross-flow tests.

## Source stages covered

- Anthropic API subscription login via manual code, URL and callback; wrong and
  empty state, missing code, failed bind; invalid route/state/error and malformed
  target callbacks keep waiting for the valid callback. The malformed-target500
  is plain text for Anthropic and HTML for Codex, matching their separate source
  handlers. Anthropic/Codex accept POST callback methods; OpenRouter rejects POST.
- Codex browser selection, manual and callback login, actual source bind-failure
  fallback, unknown method; device user-code request,403/404 no-body-read pending
  polls, nested pending errors, slow_down and interval growth, hex numeric-string
  interval, ordinary/slow_down deadline errors, and completion after the nominal
  polling deadline. A successful in-flight poll is not reclassified as timeout.
  Device-start404 fails before body consumption, unlike other failed starts.
- OpenRouter manual code and literal hash input; callback route/missing-code/
  method errors; claimed duplicate callback409; claimed exchange vs later manual
  success and later manual error; provider denial and escaped error detail;
  invalid JSON, empty key, empty error-detail precedence, HTTP failure and best-effort error-body failure;
  login and exchange timeouts; permanent key credential and value-preserving
  refresh. A manual error after claim can reject login after callback200, while
  a later manual success cannot start another exchange.
- Anthropic/Codex rotating refresh, timestamp margin vs no margin, Codex accountId
  extraction from the new access token, and all three `to_auth` derivations.
  Codex parses the source's standard-base64/Latin-1 JWT payload form solely to
  extract the account claim; this is not a token signature verification API.

The shared HTTP helper sends no OAuth retries. Anthropic and OpenRouter select
30-second whole-response exchange timeouts; Codex token requests have no local
request timeout. Core still owns the separate request-auth refresh deadline and
credential mutation lease. No flow reads or writes the credential store.

## Native lifetime evidence

The separately selected `auth-native` profile runs:

- `native_composition::oauth_selected_native_callback_reply_and_cleanup_stages`:
  real native listeners and actual TCP replies for all three flows. Held synthetic
  token HTTP proves Anthropic/Codex fetch initiation followed by manual/listener
  cleanup before token completion. OpenRouter sends no callback response while
  exchange is held, then replies200 and releases its listener after exchange.
- `native_composition::oauth_selected_native_cancellation_releases_listener_and_prompt`:
  all three pending browser flows abort, cancel the manual prompt token, acknowledge
  listener shutdown, and permit actual port rebinding.
- `native_composition::oauth_selected_native_codex_bind_fallback_and_anthropic_failure`:
  intentionally occupied real fixed ports produce Codex manual fallback and
  Anthropic failure before any prompt or token HTTP. The fallback also races the
  original cancellation token while the per-prompt token stays independently
  owned; cancellation stops that pending prompt before token HTTP starts.

`native_composition::oauth_selected_native_stalled_primary_ack_retains_cancellation_and_timeout_owners`
adds seven real-listener controls: releasing and cancelling a deliberately
noncooperative primary reply for each provider, plus OpenRouter's existing login
timeout. The owning reply drops exactly once; accepted connections reach EOF and
actual callback ports can rebind. The source's queued response can already finish
while Rust still awaits the host ACK. This is the controller-approved plan 24 /
conventions 44 normalization, not stalled-ACK scheduling parity. Browser token
exchange may consequently wait for that primary ACK. Original cancellation stays
live, and OpenRouter retains its existing timeout owner throughout the ACK wait.
Invalid/duplicate reply futures are instead queued and polled beside login work;
the source invalid-route timeout control also holds the host ACK and still settles.

Fixed-port tests serialize with one test-owned mutex. They do not replace source
ports with selected ephemeral values. OpenRouter naturally requests an ephemeral
port. Callback host resources are still owned by accepted transport capabilities;
no foundation production module is modified. Each test waits for physical EOF,
acknowledged close or actual rebinding, not merely a dispose counter.

## Admission and platform boundaries

Three source-valid values are outside inherited DTO admission: fractional Codex
interval 0.5, fractional final Anthropic milliseconds, and negative final expiry.
The source results remain recorded; Rust rejects them explicitly with
`OAuthFlowErrorKind::InvalidResponse`, preserving the exact admitted prefix and
never rounding an interval event. Fractional token seconds yielding integral safe
milliseconds remain accepted. Credential expiry never exceeds SafeUnsigned.

Source error strings/engine stacks are recorded but are not copied to Rust error
Display/Debug. Typed `OAuthFlowErrorKind` and optional HTTP status preserve the
failure stage; underlying host errors retain an optional source chain. Ordinary
OpenRouter callback error descriptions remain transient, are escaped into the
callback page, and are redacted in error formatting. The source's URLSearchParams
request-body object is represented as its exact encoded text in the existing
finite FetchBody surface; arbitrary JavaScript object identity is not promised.

Native callback framing, resource limits and asynchronous close acknowledgment
inherit the foundation README. The synthetic source listener records only the
first physical release; repeated idempotent close calls do not add releases.
HTTP default headers/wire details remain owned by the accepted transport, while
these tests observe source-level supplied headers and body forms. Arbitrary
JavaScript prototypes, reentrant UI implementations, unhandled Node callback
exceptions, every platform error message, and live-account/browser behavior on
other operating systems are not universal parity claims.

Four-way variance: all planned selected flow bodies and their required selected
runtime behavior are implemented; no factory/catalog/registry work is included;
explicit native forwarding and exact feature guard admission complete the
previously deferred foundation feature seam; no additional dependency version,
core DTO/signature/default, ambient discovery, browser launcher or excluded OAuth
flow is added. The controller's main gate and independent review remain acceptance
requirements after this unit's isolated full gate.

## OpenRouter review correction

Two appended complete-source cases preserve the original 47 observations:
`openrouter-callback-body-read-failure` proves that a successful HTTP response
whose JSON body read rejects yields InvalidResponse and the fixed callback detail
`OpenRouter OAuth returned invalid JSON`. Cancellation and timeout retain priority;
failed-status best-effort body handling and the other flows are unchanged.
`openrouter-callback-exchange-timeout` proves that the callback page retains
`OpenRouter OAuth token exchange timed out`, independently of the primary ACK
normalization. Generic error Display/Debug remain redacted.

The source driver additionally records SHA256 digests of the actual returned
synthetic access and refresh strings. The native-profile test
`oauth_selected_source_credential_values_match_without_logging_tokens` compares
both values independently for all 20 admitted successful credential outcomes,
and checks that changing either value fails equality. It does not log credential
values or add credential fields to durable/public DTOs. The three inherited
numeric admission exclusions retain their source observations and explicit Rust
rejection. The complete source replay also checks the digest map.

The two new exact Rust tests are
`oauth_openrouter_successful_body_read_failure_is_json_admission` and
`oauth_openrouter_callback_timeout_preserves_fixed_page_detail`. Together with
the digest test and six existing tests, the owning all-feature target executes
nine named tests, covering 49 source scenarios plus native lifetime controls.
