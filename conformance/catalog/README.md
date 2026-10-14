# Selected catalogs from the 2026-09-07 public capture

This corpus proves reproducible parity for the exact dated public feed bytes in
[capture.json](capture.json), using upstream generator revision
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`. It does **not** reconstruct the
historical text catalog whose ignored input/output files are absent from Git.
All five inputs were captured without account credentials;
this unit independently checked their SHA-256 hashes and copied their original
bytes. No feed was fetched again. Capture metadata retains URLs, effective URLs,
retrieval times, status, lengths, response dates and hashes.

The selected text output has 450 models: OpenAI 39, Google 22, Anthropic 14,
DeepSeek 3, OpenAI Codex 7 and OpenRouter 365. The newly captured image output has
52 OpenRouter models. The separate pinned source projection has 50 models and is
retained only as `generated/historical-images.json`; it is never blended into
or loaded by the Rust built-in catalog. OpenRouter routing models are retained
regardless of underlying vendor, including negative routing prices.

## Reproduction and provenance

Run `python3 scripts/generate_selected_catalogs.py --check` from the repository
root. Remove `--check` only for an explicit regeneration from the approved input
capture. Both modes are entirely offline. The canonical `scripts/verify.sh`
includes this check, the owning Python causal suite and Rust catalog tests.

The runner independently checks every [source-manifest](source-manifest.json)
entry against immutable Git bytes and blob IDs, stages those unmodified source
files in a temporary directory, then executes both complete pinned generators
with a sanitized environment. `offline-preload.mjs` serves only the exact
recorded feed URLs, rejects undeclared URLs/request options, and freezes the
clock to `2026-09-07T06:59:46.000Z`. Text execution uses `--strict --json-output`:
it also generates and validates the upstream grouped data and schema-version-3
manifest through the complete upstream `model-data.ts` path. Image execution
uses its unchanged `--strict` main. The clock is a reproducibility choice within
the capture second, not a claim about historical generation time.

`project.mjs` selects the six text providers only after complete generation and
converts each source object into an ordered model array for Rust intake. It also
exports both distinct image projections. `generated/source-text.json` and
`source-grouped.json` retain complete generated provider observations as build
evidence, including excluded products; only the selected arrays are runtime
assets.

[Generated provenance](generated/provenance.json) binds every output hash and
length, input/source manifest hashes, Python generator and JS driver hashes,
Node executable hash/version, ICU, locale, exact consumed URLs and frozen clock.
A changed input, source identity, selection, output, runtime or provenance causes
`--check` to fail. Runtime upgrades and capture updates require explicit review;
there is no automatic network fallback. Temporary source/output directories are
removed after execution. The generator imports only its pinned runtime modules;
TypeScript type-only imports do not execute provider implementations.

## Rust intake and feature boundary

`pi_ai_providers::catalog` exposes six text functions returning
`Result<Vec<Model>, CatalogError>`: `openai_models`, `google_models`,
`anthropic_models`, `deepseek_models`, `openai_codex_models` and
`openrouter_models`. Each exists only with its matching `catalog-<id>` feature.
`openrouter_image_models() -> Result<Vec<ImagesModel>, CatalogError>` requires
`catalog-openrouter-images`. All seven features are data-only, activate no
other feature/dependency, and remain default-off. No factory, auth behavior,
registration, platform effect or live refresh is implemented by this unit.

`decode_text_catalog(bytes, provider, policy)` and `decode_image_catalog(bytes,
provider, policy)` accept ordered model arrays and validate them using existing
`RawModel`/`RawImagesModel::into_model` conversion. They reject mismatched provider
IDs, duplicate model IDs, unsupported fields, invalid numeric/endpoint/compat
values and unclassified headers; results are atomic owned vectors. Error
Display/Debug are redacted, with optional underlying error sources retained.
These generic decoders validate supplied data and do not assert external data
provenance. Built-in arrays are immutable compiled assets verified by the offline
SHA-256 build checks and then decoded under the default empty header policy.

Complete-source tests compare every selected model field and array order after
Rust round-trip. Numeric JSON spelling (`0` versus `0.0`) and object property
order are not compared because existing core finite-number/map DTOs normalize
those representations. Numeric values, omitted versus null fields, model order,
modality order, cost tiers, compatibility metadata and negative rates remain
exact. No behavioral normalization was added.

## Causal coverage and limits

Python controls regenerate twice, change input bytes and purported capture
hashes, alter source metadata, add/remove selected entries, filter OpenRouter by
vendor, reorder models, change clock provenance, and request undeclared URLs or
options. Rust controls cover all seven complete catalogs plus duplicate IDs,
provider namespace errors, endpoint/numeric/unknown-field admission, public
header policy and credential-header rejection. Workspace controls reject any
catalog feature that activates native/wire/dependency effects or invents an
excluded catalog feature. All earlier guard commands and evidence remain intact.

Catalog counts and values are specific to this capture. Historical text values,
provider registration, OAuth/factory composition, image requests and ongoing
catalog discovery remain outside this unit's claims.
