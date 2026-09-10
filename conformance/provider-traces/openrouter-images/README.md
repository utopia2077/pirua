# OpenRouter image wire

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`, complete
`packages/ai/src/api/openrouter-images.ts` and its four runtime helpers. This
unit implements configured image wire only. Generic image models/registry,
provider factory, OAuth, catalogs, and compatibility registration remain separate.

`source.mjs` exports original Git blobs into a temporary directory, executes the
complete adapter with installed OpenAI 6.40.0 and constructed fake Fetch responses,
and removes that directory. It never connects to provider endpoints. `trace.json`
records every source-file hash, driver hash, Node version, and a digest of all
installed OpenAI `.mjs`/package metadata files. SDK bytes are versioned installed
provenance, not Git source authority. Check mode compares all fixture bytes:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/openrouter-images/source.mjs --check
```

The normal corpus has 49 complete executions, plus three complete source
numeric-admission controls. It observes payload callbacks, Fetch URL/method/
headers/body form/body text, successful response callbacks, and complete image
results. The corpus includes all finite JSON root alternatives, null versus
missing body, model/header/env precedence, early and late header errors,
SDK media-type/204/content-length-zero decoding, malformed JSON, read failures,
first-choice content, partial fields on error, usage/cache arithmetic, retry
veto/cap/success, callback failure/cancellation, and transient endpoint query
construction. No expected value comes from Rust output.

The source fixture records three Fetch Request constructions separately:
missing body, empty text, and empty bytes. Missing body is exposed as
`FetchBody::Absent` to custom consumers. Physical conversion preserves the
source's empty text Content-Type default and omits it for absent/byte bodies.
Absent and empty bytes have equal zero-byte physical POST bodies; their logical
custom inputs remain different. Existing empty-byte GET/HEAD admission is
unchanged. Cloudflare binding rejects Absent with its existing InvalidRequest
category before parsing; existing fixtures are unchanged.

## Rust contract and normalization

`images::openrouter::OpenRouterImagesApi` implements the unchanged core
`ImagesApiAdapter`. `generate_images` also accepts a nominal resolved image
request plus dedicated `ImageAdapterServices`. Middleware receives the actual
image request and ordered `WirePayload`, not a fabricated text request. All
services are passive, explicitly supplied, non-Serde capabilities. No provider,
OAuth, network backend, or catalog is enabled by default.

The source observes callback cancellation only through a caught failure. Image
middleware is awaited directly and owns its cancellation behavior. In particular,
a successful onResponse which cancels its token still yields successful images;
a non-cooperative response observer is not forcibly settled by the image wire.
Transport acquisition/body cancellation retains the accepted S04 transport owner.
All HTTP retries occur in `HttpTransport::retry_provider_request`, with no lower
HTTP/SDK retry. HTTP error bodies are consumed before retry eligibility, success
parse failures do not retry, and onResponse runs only after successful SDK parsing.
Each attempt's stainless retry count is zero. There is no assistant/Harness retry.

Explicit normalizations:

- Accepted Rust Unicode strings cannot contain unpaired UTF-16 surrogates.
  All admitted string scalars pass unchanged; no surrogate claim is made.
- Core numeric/ID/model DTO admission is unchanged. Usage counts must be safe
  nonnegative integers, costs finite, and response IDs nonempty strings.
  A representable negative prompt count may still produce source-clamped zero
  input. Fractional/negative/unsafe resulting output counts fail at usage
  admission after response ID assignment; the three source controls show that
  upstream itself returns the unrepresentable usage. No rounding is performed.
- The SDK platform/user-agent fingerprint follows S04's platform projection:
  `user-agent: pi-ai-rust`, with platform-specific stainless headers omitted.
  Other headers, null behavior, casing consumption, body form, and wire text
  are compared exactly.
- Provider/SDK/callback error prose may contain secrets. Rust emits fixed
  nonempty stage-specific messages; fixture comparison preserves error-message
  presence, terminal reason, and every partial result field. The synthetic source
  prose remains in the source fixture for audit, but is not copied into runtime
  failures. HTTP status/header retry behavior is preserved independently of prose.
- JSON wire serialization uses the accepted shared one-MiB/depth-128 serializer.
  Successful response buffering defaults to 16 MiB; HTTP error buffering defaults
  to 256 KiB; both response limits are explicit service settings. They are local
  admission bounds, not upstream or aggregate/RSS guarantees.
- Injected clock timestamps project to SafeUnsigned using the accepted S04
  saturation convention. Source fixtures use a fixed in-range timestamp.
- The configured adapter checks its API identity. Registry-owned declared API
  bindings remain a later composition unit.

## Verification

`complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages`
compares all 49 source cases. Numbers compare through core DTO values to avoid
JSON lexical `0` versus `0.0` differences. Separate tests verify inherited usage
admission, response-observer cancellation ownership, blocked Fetch cancellation,
response-body release on bounds, and retained partial output.

`native_image_post_decodes_and_closes_peer_while_adapter_stays_alive` sends a
real POST through the production native HTTP backend, checks the payload at a
loopback peer, decodes images, and requires peer EOF while adapter/transport
owners remain alive. Transport `fetch_body_presence` tests compare logical forms
and native empty-body Content-Type/EOF behavior. The native test features are
explicit; ordinary image wire depends only on the existing transport ports.

```sh
```

The causal script temporarily adds a success-cancellation veto and then folds
Absent into empty Text. Each mutation must fail the unchanged complete-source
comparison at its exact case; each file is restored in `finally`, and the
restored test must pass. Run it without a concurrent build in the same tree.
These focused commands supplement the sole full gate `./scripts/verify.sh`.

The mechanical existing Codex test match arm changes its file SHA256 from
`92dc8ef47923dc3b9179d083fad68f1ff5ef1d1293dc224d79a569a48b963047`
to `200e13e6a35951bf426e4d729d818e4db6ac3a93bc2e9ad3d2df1b72df3e8131`.
The S04 evidence catalog changes only that digest. Its canonical digest changes
from `4dcf95fa00798450afa1dca39668216dd59349cb824cf792a9da331b2c89de78`
to `00bc1ecd6cba6f75b435ef6375c86e7ceaf7cf3189daa76f4acd065dd0ce7a85`.
No original test identity, assertion, source fixture, or S04 ownership changes.

Unverified: live providers/accounts, arbitrary JavaScript objects/prototypes,
non-JSON callback values, universal SDK/platform behavior, and later registry/
factory/catalog/facade assembly. These are not claimed by this wire unit.
