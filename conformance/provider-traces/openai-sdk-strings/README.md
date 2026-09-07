# OpenAI SDK string boundaries

Authority: pi-mono853a80d26c90a14c1886f0ebb8ffaae133ca2185 full standard
Responses/Completions wrappers and installed OpenAI6.40.0. The oracle executes
those wrappers with injected synthetic Fetch only; no live endpoints or auth
discovery. Dependency Git source hashes are retained in each corpus.

Run from repository root:

```
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/openai-sdk-strings/oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/openai-sdk-strings/oracle.mjs --completions --check
```

Each family has50 executed cases. They cover media-type suffix NEL versus NBSP,
ASCII and plain JSON controls, the SDK's application/json substring condition,
empty/whitespace/wrapped org/project values, all25 ECMAScript trim characters,
custom-header eager/lazy validation with value/null patches, and nonempty
custom headers with invalid organization data that must remain lazily validated.
Ordinary request-header NBSP and untrimmed scoped PI_CACHE_RETENTION controls
prevent changing general header or provider environment semantics.

SDK `src/internal/parse.ts:56–70` trims only the media segment with JS String.trim.
NEL U+0085 is not in that set, whereas NBSP is. Responses addOutputText sees
the text primitive on the NEL +json-lookalike path and rejects before onResponse
or Start; it never enters HTTP retry. Completions already had correct ECMA media
trimming and remains an unchanged regression control.

SDK `src/internal/utils/env.ts:10–15` is `trim() || undefined`.
Client `src/client.ts:412–413,463–477` consumes organization, project and custom
headers. Nonempty custom-header env triggers eager defaultHeaders building;
organization/auth/encoder headers are not part of that eager merge. Pi's scoped
env helper has distinct semantics and is not changed. Ordinary header edge
trimming is unchanged. The new private helper reads only an explicitly captured
ambient map, never process environment, and returns borrowed transient strings.

`tests/openai_sdk_strings.rs` invokes both complete adapters with identical
fixture models, contexts, options, ambient values, bodies and middleware. It
compares full request URL/headers/body, callback order, complete event snapshots,
final retry/overflow observations and exactly one close per dispatched body.
SDK custom-Fetch fixtures use Preconstructed, not native network defaults.
Portable UA/platform headers are normalized as in existing corpora. Date.now=7;
stream snapshots are copied at source push. Numeric snapshots compare by finite
Number value (Rust cost-f64 versus JS integer spelling). Raw exception prose and
Rust diagnostic fields are excluded from snapshot equality, while final source
retry/overflow flags remain independent assertions; no arbitrary exception text
is restored or accepted as input. No prior family fixture/assertion is changed.

The initial23-case matrix reproduced standard media/env failures and Completions
env/eager-stage failures before implementation. After correction, the expanded
100-case corpus and all prior family tests/oracles pass. This unit has no Azure
feature or SDK extraction. Later Azure composition must consume this helper for
its SDK media decode and ambient org/project/custom-header reads/eager trigger;
Azure's own scoped configuration helper must not be normalized by it.

No native networking algorithm, raw header iteration, durable schema, public
feature, manifest or dependency changes. Native family regression tests and the
repository acceptance entry remain required; this is not broad SDK/Fetch parity.

## Custom-env parsed-object correction

Run the same oracle with `--custom-record` (and optionally `--completions`) plus
`--check` to verify the separate13-case/family corpus. The original100cases and
expectations are unchanged. New cases settle exact key replacements before
case-insensitive header application: replacement preserves the first own slot;
array-index keys enumerate numerically before other string keys. A string
assignment to __proto__ creates no own property; constructor/toString and other
ordinary names remain. Parsing trims name/value at the first colon, ignores
colonless lines, retains further colons and allows blank names to reach later
validation. Invalid values overwritten by an exact later key never reach that
validation. No arbitrary prototype execution is modeled.

The oracle additionally executes the exact parsed-object block sliced from
installed SDK client.ts463–474 and records Object.entries before buildHeaders.
Private helper unit assertions consume that source observation, including numeric
own-key ordering that final HTTP header lookup alone would not distinguish.
Complete adapters consume the same finalized record, then preserve eager/lazy
header-validation placement already covered above. The original independent
five-case/family probe is retained externally and passes without assertion edits.
