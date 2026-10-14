# Generic image runtime

Source authority is pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
The complete `images-models.ts`, `images-api-registry.ts`, `images.ts`, both
OpenRouter image lazy wrappers, shared auth resolver/store/abort helpers, and
OpenRouter wire are executed by `source.mjs` from immutable Git blobs. No helper
body is copied into the oracle. Wire-loader substitution is explicit and limited
to the lazy-wrapper tests; a separate complete runtime-to-real-wire source case
uses installed OpenAI 6.40.0 and synthetic Fetch responses. Source, driver, Node,
and SDK fingerprints are recorded in `trace.json`.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/images-runtime/source.mjs --check
```

Sixteen complete generation cases distinguish no auth, an empty auth object,
ordinary auth, synchronous launch failure, asynchronous rejection, auth failure,
unknown provider, cancellation after auth admission, cancellation after generator
admission, explicit empty key/env, exact-case headers/env, and credential endpoint
selection. The source store implements the real `read` method and delegates to
upstream InMemoryCredentialStore. Before cancellation, the source driver requires
that the intended `auth` or `generate` stage was entered; an unrelated TypeError
cannot satisfy a cancellation fixture. The source's original synthetic error
messages remain recorded, including rejected-future outcomes.

Other complete-source sections cover insertion/replacement/reinsert/list order,
throwing model sources, duplicate model first-match, unknown lookup, shared refresh
promise identity, synchronous-failure memoization versus asynchronous-failure retry, failed refresh retaining old models, successful retry, named
error categories, all-provider allSettled, detached provider refresh after collection
replacement, model-auth header behavior, retained API bindings, unknown global API,
and both lazy modes including failed loads. The runtime-to-wire case requires
read/auth/payload/Fetch/response order, exact request headers/body/endpoint, unchanged
public model, image usage/output, and success after onResponse cancels the token.

## Interfaces and ownership

Generic runtime modules live in `pi-ai-providers::images` and compile with default
features disabled. Concrete wire/factory/catalog/OAuth and overall compatibility
facade construction are separate consumers. No core/wire ABI or dependency changes
are required by this unit.

- `ImagesGenerator::generate` returns `ImagesGenerateCall`: an outer launch
  `Result` containing a future whose output is another `Result`. This preserves
  source synchronous throw versus asynchronously rejected promise. Existing
  in-band-only `ImagesApiAdapter` values enter through
  `images_generator_from_adapter`; no text model/request is manufactured.
- `ImagesProvider` has id/name/auth/models, a genuinely optional borrowed
  `ImagesModelsRefresher`, and generation. `create_images_provider` accepts a
  static catalog and optional `ImagesRefreshConfig { source, spawner }`.
- One provider refresh owns one detached task through the injected spawner.
  Ordinary callers join the current flight. Dropping a waiter
  unregisters its waker but cannot cancel admitted fetch or catalog publication.
  Asynchronous failure leaves the old list and clears the flight; later calls retry.
  A synchronous callback launch failure retains the failed flight, matching the
  pinned `inflightRefresh ??=`/synchronous-finally ordering. The new
  `ImagesModelsFetchCall` outer launch Result keeps these two failures distinct;
  Rust panics are not used as typed source throws. Replacing
  the collection's provider does not revoke the old provider object's image
  refresh, unlike the separate text registry publication/fencing contract.
- `ImagesModelsOptions` requires an explicit auth context/clock; absent credential
  storage creates a private InMemoryCredentialStore. No host discovery occurs.
  `ImagesModels` is concrete; `MutableImagesModels` names the same Rust collection.
  `get_providers` returns a detached Arc snapshot. All-provider model listing
  instead uses a live insertion cursor, preserving finite callback-driven addition,
  replacement, deletion/reinsert, and clear. Empty tombstones retain cursor positions
  until the last live cursor is disposed; only then are they compacted. Provider
  calls and displaced provider destructors execute outside collection locks.
- `ImagesRequestOverrides` exposes only image generation's key/env/headers.
  Standalone `get_auth`/`get_model_auth` accept existing AuthResolutionOverrides
  plus cancellation. Generation forwards key/env/signal, not an invented minimum
  OAuth validity field. Shared credential resolution uses ProviderId with the
  same nonempty bytes as ImagesProviderId; model/API namespaces remain distinct.
- With no resolved auth, launch failures are in-band but asynchronously rejected
  generation futures propagate. With resolved auth, both are in-band image errors.
  Auth cancellation produces image error. After generation handoff, this runtime
  adds no cancellation race or success veto. `get_model_auth` does not merge public
  model headers; the accepted wire consumes them at its source stage.
- Header assembly keeps exact-key replacement slots and distinct case spellings,
  including null. It uses public ordered Serde decoding to reconstruct the core
  ProviderHeaders value; no private core constructor or altered core signature.
  Effective credential URLs live only in RequestEndpoint, never a public model.
- `ImagesApiBinding` holds explicit API identity and a generator; even a retained
  binding checks its own identity after registry replacement. `ImagesApiRegistry`
  provides register/get/generate, with explicit upsert and no global side effect.
  Source IDs are retained as registration metadata. There is no invented required
  image reset/getAll/unregister surface.
- `PerCallImagesApi` invokes a loader on each generation and propagates rejection.
  `CachedImagesApi` retains one independently driven load/failure and catches load
  or generation failures in-band. `ConfiguredImagesApiLoader` retains one actual
  configured generator instance for factory consumers. Lazy wrappers add no
  cancellation policy. Optional global facade initialization remains a later unit.

`refresh(None)` follows `Array.from(map.values(), mapper)`: mapper invocation is
live, then allSettled waits on the admitted future set. Owned async frames retain
the provider and borrowed refresh future. A private barrier lets the collection
invoke each refresher synchronously but prevents it from polling returned work
until enumeration ends. Independently owned provider tasks remain under the
explicit spawner's scheduling; no global JavaScript microtask scheduler is claimed.

Factory refresh has two private states. The source-visible flight is assigned
only after the typed fetch launch returns, so a bounded same-thread callback may
launch a nested refresh first. Every completion clears the current visible flight
unconditionally: no generation/pointer fence suppresses nested or later completion.
The source fixture proves a nested child can clear an outer flight and that an older
pending operation can publish after a newer one. A separate unpublished ThreadId
reservation lets other Rust threads join a launch already in progress while its
callback runs outside locks; same-thread reentry does not see that reservation as
a source flight. This is the explicit concurrent Rust linearization, not a claim
that JavaScript has parallel threads. Finite reentry and simultaneous other-thread
join tests exercise both paths.

Task, provider, generator, source, and waker callbacks/destructors are kept outside
collection/completion locks. Waiter IDs are checked non-wrapping u128 values.
Failed or discarded load tasks settle waiters through completion guards. Async
Rust calls begin on polling; the optional provider refresh method admits its owned
flight when called. This is the explicit Rust future boundary, not JS Promise
object identity. Readonly arrays, module globals, and arbitrary JS object identity
are not portable values.

## Verification and normalized domain

`images_runtime.rs` runs default-feature generic tests plus the feature-selected
runtime-to-wire test. It compares source call/event/result observations, verifies
actual branch admission, checks shared refresh and dropped waiters, live listing/refresh mapper mutation, all-provider
concurrency, nested launch/completion order, cross-thread joins, cursor disposal/compaction, provider replacement, retained bindings, cached load rejection, task
rejection/discard, configured module identity, and waker-drop reentry.

```sh
cargo test -p pi-ai-providers --test images_runtime --features wire-openrouter-images --locked --offline
python3 conformance/provider-traces/images-runtime/causal-check.py
```

The causal script temporarily catches the no-auth rejected future, folds header
names case-insensitively, retries a synchronous refresh throw, caches an asynchronous refresh rejection, substitutes snapshot iteration, polls
returned refresh work before admission ends, joins a same-thread nested launch,
and fences nested completion. Each change must fail the unchanged complete-source
comparison; files restore in `finally`, and the baseline exact test must pass.
The sole full repository gate remains `./scripts/verify.sh`.

Typed ModelsError replaces arbitrary thrown values; normal error formatting remains
redacted. In-band runtime/lazy setup errors contain fixed nonempty text, and fixture
comparison preserves message presence, rejection versus resolution, terminal reason,
identity/output/usage, and catch-time timestamp instead of copying source error prose.
Core DTO admission, safe timestamp saturation, credential-store quarantine and auth
cancellation/deadline semantics remain unchanged. Absent versus empty resolved
header/env records follows the existing core ResolvedAuth representation; explicit
key absence versus empty key and auth env omission versus empty map remain distinct.
The wire integration inherits only the accepted SDK platform-header projection and
numeric DTO comparison, not broader request/result normalization.

Unverified: live providers/accounts, arbitrary JS prototypes/throws/promise identity,
foreign callable objects, production executors beyond their accepted TaskSpawner
contract, and later selected factory/catalog/global facade assembly. These are not
claimed by the generic runtime unit. Internal scripted providers are test
infrastructure, not the excluded public Faux provider surface.
