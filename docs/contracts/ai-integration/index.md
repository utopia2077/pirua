# Selected provider composition

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Source and scope

Six text entries and OpenRouter images are the only product factories. Full
source accounting retains 699 export occurrences. Of the old 241 S05 rows,
114 remain implementation obligations (17 selected-specific and 97 shared),
125 have explicit owner scope exclusions, and two have separate Rust-platform
replacement explanations. The 114 rows represent 90 distinct origin/name pairs;
derive target counts after implementation rather than assuming one per row.

Product exclusions must use a new explicit owner-scope disposition, not
`implemented` or `rust-nonapplicable`. They include excluded factories/shards,
their dedicated helpers and public Faux compatibility. The separate two Rust
mechanisms are Bun binary OAuth loader registration and TypeScript JSON module
typing; selected Rust flow construction and validated catalog decoding replace
them. Keep their rationale, replacement, decision reference and revisit trigger.
Preserve the old 22 nonapplicable rows and all 327 S03 behavior rows byte-for-byte.

All 16 legacy wire aliases and `bedrockProviderModule` remain shared wrappers
around accepted adapters; they do not register excluded product factories or
restore excluded discovery/catalogs. Scope guards must distinguish these cases.
The existing S04 161-occurrence proof remains required. Exact callable/trait
probes, required feature profiles, named executed tests and source evidence
are mandatory for new implemented claims; zero-test success is never evidence.

## Ownership and composition

- `pi-ai-core` retains DTOs, auth/store authority, provider-neutral text Models
  and open IDs. No transport/native dependency is added. The only planned core
  behavior extension is opt-in child-owned text cancellation described below.
- `pi-ai-providers` owns selected auth implementations, factory composition,
  catalog intake, image wire and generic image provider/model runtime. This
  matches `ai.oauth-images` ownership in upstream-map, not an assumed analogy
  with the text registry. `pi-ai` owns optional compatibility registries,
  public wrappers, selected builtin composition and stateless retry helper.
- `pi-ai-transport` owns production network/listener/entropy effects behind
  explicit service capabilities. Provider policy, token endpoints and OAuth
  decisions remain in providers. Reuse existing pinned dependencies; any new
  version/edge or broader native feature requires explicit review.
- Default features remain empty. Factory features select their wire/catalog;
  OAuth/image features remain separately selectable. Provider umbrellas contain
  the selected product set only; all-wire regression tests retain S04 breadth.
  Modules do not register providers, read user configuration or open listeners
  at import/construction. Ambient access is an explicit host service operation.
- Factories preserve one configured adapter/module instance through lazy loads,
  including Codex cache/resource ownership. All selected text source factories
  use default lazy capabilities: underlying optional deferred operations must
  not accidentally become factory capabilities. Static catalogs expose no
  dynamic refresh capability. Selected text/image OpenRouter share credential
  provider ID while their model/API namespaces remain distinct.

## Authentication and host effects

Reuse core `AuthContext`, `AuthInteraction`, `ApiKeyAuth`, `OAuthAuth`, credential
store/lease and request-refresh deadline. OAuth flows return credentials; the
existing store/registry commits them. Providers never duplicate rotation or
durable credential authority. Concrete API-key helper and default host context
keep source truthiness versus whitespace-only absence distinct. Anthropic's
stored key, auth-token header, OAuth token and API-key precedence stays exact.
Legacy env helper observations are separate from factory auth resolution.

Provider-owned OAuth services capture transport, clock, configured callback
host, secure entropy/PKCE and an owned callback-listener capability. The generic
listener exposes request method/target and a one-shot reply owner; it does not
know provider/token semantics. Flow code can retain a reply across token exchange.
Real loopback tests must prove listener/connection cleanup and response timing.
No browser launcher or credential-file discovery is required by these flows.

Callback session explicit close is asynchronous and acknowledges listener and
accepted-connection shutdown; Drop initiates cleanup. Receive cancellation does
not implicitly close the session. Native connection/reply admission is bounded
and owned by an explicitly supplied runtime handle. The default-off transport
`auth-native` feature may reuse existing ring0.17.14 for secure entropy/SHA256,
plus existing native/base64 dependencies; this adds no new package version and
does not activate auth effects through existing wire/native feature selectors.

The accepted host's reply completion acknowledges write/shutdown; pinned Node
`res.end()` only queues bytes (`anthropic.ts:145–146`, `openrouter.ts:198–199`).
Primary replies may await the stronger host ACK before definitive close. This
is an explicit host timing normalization: no stalled-ACK scheduling equivalence
or source-completion timestamp claim is made. A stalled primary reply remains
cancellation-responsive and, for OpenRouter, under its existing timeout until
settlement. Prove release of the reply and listener on those paths with controlled
pending hosts. Invalid/duplicate replies are owned and polled alongside login
work, so their ACK cannot block valid callback/manual/exchange/timer progress.
No new timeout constant, credential rule, foundation method or ambient action
is introduced by this refinement.

Freeze the complete pinned three flows, including Codex browser and device
paths. Preserve PKCE/state generation, callback route/state handling, invalid
callbacks that leave waits open, source bind-failure differences, manual-input
parsing, pending/slow-down polling, timeout ownership, and cleanup ordering.
Anthropic/Codex release browser listener/manual wait before returned exchange
completion; OpenRouter claims in the callback handler, exchanges once, replies
after exchange and retains that claim against later manual success. Source
OpenRouter refresh preserves credential values, MAX_SAFE expiry and empty refresh;
Codex refresh derives accountId from the new access token. Lazy OAuth memoizes
load success or failure without requiring module-global JavaScript identity.

Inherited DTO admission remains authoritative: interval events use SafeUnsigned,
credential expiry must be a safe nonnegative integer, and secret/callback values
remain non-Serde/redacted capabilities where already specified. A source-valid
fractional device interval is outside that inherited event domain: reject it
explicitly with sanitized response-admission failure; never round or claim its
fractional event was emitted. Checked expiry rejects unrepresentable results,
including negative/fractional/unsafe/nonfinite values, while accepting fractional
seconds when the final millisecond expiry is integral and safe. Record these
normalizations with complete source controls and preserve core DTO definitions.
Configured fake HTTP/interaction/clock services must exercise all supported
paths, including cancellation and resource release, without account calls.

## Images

Use dedicated `ImageAdapterServices`, image payload and response middleware
borrowing `ImageGenerationRequest`; do not fabricate text requests or change
accepted text middleware signatures. Reuse WirePayload, response-header record,
logical Fetch and selected SDK helpers where their actual image semantics match.
Keep the existing core `ImagesApiAdapter::generate` method. A provider-owned
`ImagesApiBinding` carries declared ImagesApiId and adapter, preserving explicit
registry identity and mismatch rejection without adding a core required method.

Image wire preserves the complete pinned OpenRouter adapter: key required before
payload work; one user message; source modality/input order; any admitted finite
payload replacement; exact header spread/order with null removal before SDK
defaults; constructed Fetch Headers and text body; one outer provider retry
owner with SDK retries zero; callback after successful SDK decode; first choice,
text then data-URL images, optional usage/responseId and partial fields on error.
Do not validate base64 more strictly than the source shape matcher. Reuse SDK
construction/decoding stages only when proven equal; image null Authorization
does not delete the SDK Bearer default. A successful response callback cancelling
the token may still yield success: cancellation changes status only at source
catch stages. Auth-runtime cancellation produces image error, not wire abort.

The contract preserves source-falsy payload request construction: the SDK omits
RequestInit.body for admitted false/zero/empty-string replacements. An additive
`FetchBody::Absent` must reach CustomFetch distinctly from Text("") and Bytes([]).
Only network conversion may project it to physical empty bytes, without a text
content-type default. Existing Text/Bytes and legacy GET/HEAD admission remain.
Cloudflare binding rejects Absent as missing input in its existing typed error
category. Existing exhaustive test matches may gain an arm; their prior expected
observations/JSON fixtures remain unchanged. Any resulting evidence-file hash
refresh records that exact mechanical change, not a new S04 fixture capture.

Generic image runtime preserves ordered replacement/listing, first matching
model, unknown lookups, per-provider refresh failure and all-provider allSettled,
single-flight refresh with old-list retention on failure, and shared auth
resolution. Resolve credential endpoints transiently; never write them into a
public model. Exact-key image header/env assembly is not automatically the text
registry's case-insensitive merge. Factory adapters return in-band image output;
custom image provider operations may have typed rejected-future errors. Preserve
the source no-auth asynchronous rejection path separately from auth-path caught
errors; do not claim universal never-reject behavior. Public/global image lookup
can reject missing API registration. Explicit initialization replaces upstream
import registration as already approved by the design. Image builtin registration
replaces an existing entry; text builtin compatibility registration preserves it.

## Catalogs

Pinned text shards import ignored JSON absent from Git and the inspected local
roots. Capture exact public generator feed bytes with URLs, retrieval time,
length and hashes; execute the complete unchanged pinned generator against an
offline URL map that rejects undeclared requests. Public unauthenticated catalog
GETs are authorized build input collection, not live model/account operations.
No historical Git-time catalog claim may be made from this new capture.

Freeze generator/helper/driver/runtime identities and generated clock metadata.
Keep complete source feed observations when the pinned generator consumes them,
even for excluded providers; filter only the final product catalogs to selected
entries. Record deterministic flat/grouped representations and schema validation.
The separate image source projection and generated capture must have explicit
provenance. Preserve supported negative routing costs and source ordering;
OpenRouter catalog is not additionally filtered by underlying model vendor.
Rust intake must enforce accepted model/header/baseURL DTO invariants and detect
changed bytes/provenance/selection. Capture/update is explicit; default validation
and generation checks are completely offline with checked-in inputs.

## Text cancellation, compatibility and resources

The accepted default ModelsRegistry settles original-token cancellation with
its S03 fallback. Source Models uses lazy child forwarding and preserves usage17
where the current wrapper returns0. Add an explicit constructor/mode for child
ownership after adapter handoff, leaving `ModelsRegistry::new` and existing
options literals/default tests unchanged. Before handoff, cancellation owns
setup; after handoff, drain the validated child to its terminal including usage-
only cancellation. Reuse no provider crate from core. Selected builtin facade
composition enables this mode and proves raw/simple/deferred wrapper behavior
with original-token cancellation, setup failure and spawner rejection controls.

Compatibility wrappers remain optional and process-local. Preserve source API
identity, insertion order/replacement/non-clobber rules, sourceId ownership,
getter behavior and direct versus auth-resolved invocation. Retain handles for
registered session resources and expose owned asynchronous Codex graceful close;
synchronous registry callbacks cannot claim asynchronous physical close completed.
S15 retains lifecycle/cleanup-call ownership. Default imports remain side-effect
free and no excluded factory is registered by legacy wire aliases.

## Stateless assistant retry

The accepted S03 contract assigns **14** `retry-loop.*` rows to S05, with exact
IDs in [S03 conformance](../ai-domain/conformance.md). The fifteenth,
`retry-loop.overflow-precedence`, belongs to the actual S15 coding-session caller.
It must remain tracked there; the helper never becomes overflow/Harness authority.

Use trusted producer, sleeper and optional observer ports in `pi-ai`, preserving
initial produce before policy handling, callback order, bounded retry count,
exponential delay observations, later finish semantics and rejected producer/
observer propagation. Cancellation is observed at the source sleep stage, not
as a new pre-produce or post-success check. Source sleep-abort output has missing
errorMessage; the existing strict AssistantMessage requires a fixed safe aborted
message. Preserve content/usage and record only that inherited normalization.
Source numeric delay and effective timer delay are separate observations; do
not silently saturate exponential values or alter callback order to fit a timer.
Unrepresentable numeric observations need explicit typed representation or a
explicitly reviewed finite admission rule, not a hidden cap.

## Factory handoff

The [factory/builtin contract](factories.md) fixes the selected constructor,
configured module, awaited registration and feature-group seams before consumers
are implemented. It leaves existing compat types independent until their runtime
facade is implemented and does not expand coding-runtime dependency authorization.

The [runtime facade contract](runtime-facade.md) fixes pending registry, legacy
environment, alias and cleanup-ticket composition. The [OAuth CLI contract](oauth-cli.md)
fixes developer input/file/native-host boundaries. Both inherit accepted source,
DTO and lifecycle authority; they do not reopen accepted unit observations.

The [export evidence contract](export-closure.md) binds each retained source
occurrence to actual typed routes, named executed tests and complete source proof;
its separate composition appendix covers behavior beyond the export count.
