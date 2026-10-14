# OAuth developer CLI

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Commands, interaction and file ordering

Selected OAuth entries are anthropic/Anthropic, openai-codex/OpenAI Codex and
openrouter/OpenRouter in source order. No image duplicate or API-key-only entry.
Preserve source no-command/help/--help/-h, list, login, unknown command/provider,
extra-argument behavior, output/order and the literal upstream help usage string.
CLI errors retain already-approved fixed redaction rather than raw provider/IO
error chains. Intentional authentication URLs/device codes are displayed by the
interaction port. No browser launch or new signal/cancellation UX is required.

Provider and method selection use source decimal-prefix parseInt(...,10)
semantics; nonselect input preserves bytes and empty line. Logical provider-choice
and login interfaces close in their source finally stages. Login success precedes
all auth.json reads, updates, writes and saved notice. Failed login never reads or
writes the file. File/write errors close interaction and omit saved notice.

Only cwd/auth.json is addressed. Missing, read error and malformed JSON fall back
to an empty object as in source. Admitted existing object preserves unrelated
arbitrary values and overwrites only the selected key. Serialize the actual
OAuthCredential through existing Serde, retaining refresh/access/expires and
provider extras. Pretty output has two-space indentation and no final newline;
compare parsed values, not JS property insertion order.

Source arrays can falsely report success while discarding the assigned provider
property; primitives/null throw. The Rust developer input domain admits object
roots only. Valid nonobjects or duplicate-key/numeric/string/depth values outside
inherited strict JsonValue admission return a fixed invalid-auth-file failure
without writing. Syntax failure and admission failure must stay distinct; never
silently erase valid but unadmitted data under malformed-JSON fallback. An optional
syntax-only first parse cannot supply the outgoing object or hide duplicates.
This narrows the unsound source file domain explicitly, without changing core DTO
or JSON policy. No real credentials are used for verification.

## Native host and process lifetime

Provider `auth::native_host`, under existing auth-native only, narrowly reexports
NativeHttpClient, TokioClock, NativeCallbackListener, NativeOAuthCrypto,
NativeRetryJitter, HttpTransport and CallbackLimits from accepted transport.
No provider normal Tokio dependency or new internal edge/version is needed.
The binary privately composes OAuthServices with existing native services,
127.0.0.1 callback host, CallbackLimits::default and explicit1MiB token-response
limit. The limit is a native CLI resource choice, not a source constant. Construct
only the chosen accepted concrete flow; do not build all factory wire modules.

Synchronous command planning handles help/list/invalid explicit command/provider
before native service or stdin construction. Runtime/IO may support interactive
selection; only validated login configures the chosen flow. Existing Tokio1.53.1
is optional normal pi-ai dependency for this developer target only; no new feature
like io-std/signal/macros or package version is introduced. A private Handle-based
TaskSpawner uses the accepted task port.

Use one process-lifetime detached std stdin reader thread, requested on demand,
with one active question and identity-bound reply. Never use a join-waited Tokio
blocking task for indefinitely blocked stdin. Render/flush before requesting a
line. Canceled question replies are retired and cannot be routed to a later
question. Closing the logical interface retires waiters; a physically blocked
reader may survive until process exit and is never joined. Async prompt wait
observes the supplied cancellation, so browser completion can finish the CLI.
Do not claim physical interruption of an OS read.

EOF/IO failure returns a fixed interaction failure, distinct from empty line.
Request-driven piped input preserves subsequent selection/method lines. These are
explicit native liveness/pipe timing normalizations: source Node readline can
leave EOF questions pending or discard typeahead between interfaces. Do not claim
universal TTY/readline scheduler parity. Serialize output writes and prevent
retained input/credential diagnostics from exposing secret values.

## Feature boundary and proof

`dev-oauth-cli = ["all-oauth", "auth-native", "dep:tokio"]` selects only the
three local flows and native host profile. Binary pi_ai_cli has path
src/bin/pi_ai_cli.rs and required-features=[dev-oauth-cli]. Reserved topology group
membership already exists in the accepted factory contract. Preserve default
empty, no activation through default/native/all-oauth, no all-providers/catalog
requirement and no new dependency edge. Exact feature/bin/dependency guards must
reject accidental activation and unknown entries. Existing test Tokio remains.

Implementation owns new private binary modules, narrow native reexport module,
additive manifests/auth module exports, exact guards and owning tests/source
conformance. No accepted OAuth/foundation/transport/core/factory algorithm changes.
A private injected runner is permitted for deterministic tests, not a public SDK
or fake CLI UX. Full-source32 command observations prove outputs, prefix selection,
logical close and login/read/write ordering. Add controls for early-read, lost
unrelated values/extras, incorrect parser, finally-close and invalid file domains.

Compose actual accepted flows with CLI AuthInteraction on synthetic services.
Actual isolated-cwd binary smoke covers help/list/errors and piped invalid
selection/method/EOF before transport starts. A synthetic/local child host must
prove browser success exits while its physical stdin reader is still blocked,
without adding a user-facing test command/flag. No real auth.json/account call.
