# Extension OAuth compatibility declarations

The source is pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`:
complete `packages/ai/src/compat/extension-oauth-types.ts`, `src/oauth.ts` and
`src/index.ts` are read from Git objects. These exports are type-only; there is
no executable source login algorithm in this unit. `source.py` validates every
interface member and its order, optionality and callback result, the credential
import/reexport, and the two export entry points. `source-shape.json` binds that
proof to whole-file SHA256 values. Capture never reads the upstream worktree.

Exactly seven source symbols are projected: OAuthPrompt, OAuthAuthInfo,
OAuthDeviceCodeInfo, OAuthSelectOption, OAuthSelectPrompt, OAuthLoginCallbacks,
and the existing nominal core OAuthCredentials reexport. The five new values
are owned, non-Serde and Debug-redacted. OAuthSelectOption has exactly id/label,
not the extra description of core AuthSelectOption. Select options keep order
and repeated IDs. Empty strings, optional booleans and omitted values survive.
The callback trait and its two Rust-only optional capability traits are
process-local, object-safe and Debug-redacted. Required auth/device notification
is synchronous; prompt/select/manual results use existing borrow-scoped BoxFuture
and AuthInteractionError. The progress/manual getters expose absence without
invoking the capability; an optional borrowed signal preserves token identity.

Normalization is explicit: optional interval/expiry seconds reuse SafeUnsigned
from the accepted core AuthEvent domain. Fractional/negative/nonfinite/unsafe
JavaScript numbers are outside this projection; there is no rounding or new
numeric family. Credential serialization remains the existing core contract;
the credential reexport is not a new non-Serde token format. Trusted synchronous
exceptions and async callback rejections become the existing typed interaction
error; Rust panics and foreign JavaScript callback identity are not translated.

Meaningful proof commands, with actual feature admission:

```sh
env -i PATH=/usr/bin:/bin python3 conformance/ai-compat/oauth-types/source.py --check
env -i PATH=/usr/bin:/bin python3 conformance/ai-compat/oauth-types/source.py --causal
cargo test -p pi-ai --features compat --test compat_oauth_types --locked --offline
cargo test -p pi-ai --features compat --doc --locked --offline
```

The seven owning Rust tests exercise required/optional trait-object invocation,
select None versus empty/nonempty results, delayed future polling, signal identity,
typed errors, redaction, nominal credential equality and source proof. Module
doctests include a compiling construction/port admission control plus three
compile-fail capability serialization/deserialization checks. Source controls
admit harmless whitespace/comment changes and reject seven changes to callback
presence/results/timing, prompt optionality, extra fields/order and alias origin.
None writes or changes pinned source or old conformance evidence.

Four-way variance: all seven planned source symbols implemented; none missing;
additional Rust-only optional subports and shape controls implemented; no login
flow, extension adapter, runtime/global facade, provider factory, durable protocol,
new dependency or callback scheduler implemented. `compat=[]` is empty/default-off.
This proof does not establish extension RPC, live OAuth behavior, automatic signal
handling or arbitrary host executor behavior. Cumulative export closure and root
normal-profile verification remain controller-owned.
