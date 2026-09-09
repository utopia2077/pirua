# Codex Responses configured adapter

`OpenAICodexResponsesApi` implements raw and simple streaming with typed
`OpenAICodexResponsesOptions`, plus detached WebSocket debug stats, explicit
stats reset and owned graceful session close. `CodexServices` supplies transport,
persistent Node connector availability, UUID generator, date parser, compression
limits and source signal presence. Credentials arrive only in the resolved
request. No OpenAI SDK request client, account lookup, OAuth, registration,
ambient discovery or global cache is created.

The adapter uses the accepted Responses converter/reducer, ordered WirePayload,
logical Fetch and zstd request primitive. Payload observation/replacement happens
once before transport selection. Actual custom Fetch sees constructed Headers,
plain JSON Text or compressed Bytes. Compression runs once before the retry loop;
encoder absence/failure preserves plain JSON. Request byte comparisons include
strict schemas and deferred tool fields with their source insertion order.

`CodexSourceSignalPresence::Present` is the default. Its timeout ends at response
headers while caller cancellation remains effective during reads. `Absent`
retains the source timeout through body consumption and still honors the original
Rust caller token. Successful missing bodies fail before Start and never reenter
request retry; present-empty/incomplete streams retain their distinct missing
terminal classification. Shared completion metadata opts into boolean end_turn
only for Codex; other families keep default false.

The instance owns raw-session/account cache identities, sticky fallback and
statistics. Omitted transport, explicit auto and websocket-cached remain distinct.
Busy cached connections use one-shot exchanges. Continuation compares exact
serialized request/prefix bytes, including equivalent schema values with different
field slots; mismatches send full context. The two source reconnect categories
remain separate, as do pre-output SSE fallback and post-output nonreplay.

Explicit close removes entries and cancels timers before returning an owned async
completion. Polling it awaits1000/debug_close under the configured grace; zero
expires immediately. Dropping that completion, dropping the last public adapter
owner, and emergency abort retire ownership without claiming a flushed frame.
Stats reset and close have separate effects. No IO, host callback, destructor or
waker runs under the cache/slot mutex. Noncooperative waits race cancellation and
bounded grace; failed leases do not return to reuse.

## Source evidence

All pinned TypeScript comes from Git853a80d26c90a14c1886f0ebb8ffaae133ca2185.
`harness.mjs` executes complete Codex/helper/reducer/stream bodies with actual
installed partial-json and Node zstd. Only configured Fetch, persistent WebSocket,
time, UUID, user agent and future resource registration leaves are synthetic.
Source snapshots are copied when published; raw errors/stacks use the accepted
safe normalization for Rust comparisons. Fixtures are source-generated, never
constructed from candidate Rust output.

- `oracle.mjs`:88 complete invocation cases, including typed/simple options,
  strict/grammar/deferred tools, header/endpoint order, retry/body/hook stages,
  logical Fetch forms, zstd/plain bytes, fallback and full events/results. Sleep
  comparisons apply the separately proven Node timer coercion. Four line endings
  and astral/BMP optional-regex-character controls distinguish server delay from
  broad-catch backoff; BOM versus NEL distinguishes ECMAScript URL fallback.
- `cache-oracle.mjs`:22 multi-request sequences covering full/delta, raw/default
  modes, reuse/busy/closed/age/idle, session/account changes, reset/close/sticky,
  previous-response and connection-limit retries, and strict-schema slot identity.
  Check mode compares the JSON persistence projection of optional undefined
  source properties; retained snapshots and wire strings are unchanged.
- `account-oracle.mjs`:19 finite-JSON scenarios. Number1 and string1 have equal
  header spelling but distinct cache keys; object/array claims get fresh per-parse
  identity. atob's Latin1 behavior distinguishes literal UTF8 from escaped Unicode.
  Falsy and property-shadow coercion failures preserve callback/header stages.
- `metadata-oracle.mjs`:20 boolean/missing/nonboolean/default-off controls and
  usage17 cancellation snapshots in the shared opt-in path.
- `payload-oracle.mjs`:18 finite payload cases, including arrays/scalars, cached
  null and UTF16 string spread. Surrogate halves remain escaped wire strings.
- `timeout-oracle.mjs`:3 actual complete-source timing/caller-cancellation cases.
- `native-frame-oracle.mjs`:actual Node persistent raw-DEFLATE messages across two
  Codex responses with no dictionary reset; consumed by the native socket test.

Run every source program with `env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node
<path> --check` (one line). No source check invokes a live provider or credentials.
Focused Rust entry: `cargo test -p pi-ai-providers --features
wire-openai-codex-responses,native --test openai_codex --test openai_codex_native
--locked --offline` (one line). Sixteen runtime tests and four native tests cover
source snapshots/bytes, controlled ownership and real local sockets. Native tests
observe compressed full/delta requests, exact graceful close frame, body timing,
failed upgrade fallback and post-output close1009 without replay. Their final peer
reads require physical EOF; only precise already-closed half-shutdown is tolerated.

## Explicit local bounds and adaptations

Defaults retain at most128 cached sessions,256 stats entries and256 total retained
operation owners (active leases, idle tasks and asynchronous retirements). Tokens
and individual resource keys are limited to8192 bytes. Continuation and decoded
messages default to one MiB each; the existing checked wire serializer also caps
one MiB/depth128. Incoming JSON and configured-token JSON retain the parser's
fixed depth128 admission; finite values outside that parser/resource envelope
are not claimed source-equivalent. The strict-schema helper keeps its own unchanged256KiB/
depth128 input limits. Compression has explicit host-supplied input/output bounds;
its failure selects plain JSON rather than changing shared codec policy.

These are finite local resource bounds, not source timing/allocator equivalence
outside that envelope. Long valid connections have no cumulative byte cap. Timer
owners retain budget until dropped even when a host delays polling cancelled
work. Source globals become instance resources, synchronous source cleanup becomes
owned graceful completion versus emergency abort, and cached-null source failure
retires its failed lease rather than leaving inaccessible ownership. Original
safe-number DTO, source error normalization and S03 terminal/cancellation rules
remain unchanged. Source synchronous simple-setup exceptions become typed stream
setup errors under the existing Rust ApiAdapter return shape. S05 registry/lazy composition and S15 host cleanup integration
remain separate; full acceptance uses only `./scripts/verify.sh`.


## Independent-review corrections

`review-correction-oracle.mjs` retains33 complete source cases for first
WebSocket completion aliases with boolean/missing/nonboolean end_turn, a preceding
created event, and unchanged HTTP Start timing. Only the mapper's endTurn update
precedes first-completion Start; usage and other completion fields are still
interpreted later. Source close-cycle and original-token/full-delta reuse controls
are included. The original corpora remain unchanged.

A retained slot now shares its current operation charge with its active lease or
idle task. Reuse and successful release hand off that charge; cancelled old idle
tasks keep their separate prior charge until reaped. Explicit and internal close
retirements retain the slot's charge until the last owner is released, including
never-polled close futures and multi-slot close(None). Grouping slots does not
merge their charges. Obsolete continuation storage is released before an explicit
close future is returned. Existing limits and source session/statistics effects
are unchanged; charge destruction occurs outside locks.

New controls cover selected/all-session retention, busy owners, noncooperative
close, zero grace, emergency abort and capacity recovery. The native cap4 test
reaps idle tasks by completion acknowledgement, measures held live sockets,
requires strict physical EOF for all eight attempts after failure/drop, and then
proves recovered graceful1000/debug_close/EOF. The original full/delta native test
also retains the independent original-token-cancelled/fresh-second-token control.


The three diagnostic-bearing oracle checks share `source-comparison.mjs`.
Only actual assistant snapshots at recorded `events[*].partial/message/error`
and `result` paths, including `first`/`second` phases, receive diagnostic-stack
comparison. Only the current execution-root URL maps directly to the original
fixture root; no intermediate string marker or third raw URL is equivalent.
Payloads, requests, transcript content and tool arguments are never traversed.
Relative filenames, functions, line/column text, unrelated URLs, diagnostic
messages and every other field remain exact. Input observations and original
JSON fixtures are not rewritten; default source generation still emits raw
stacks. Cache's existing JSON persistence projection remains at its call site.
Run `source-comparison-check.mjs` with the same sanitized Node command: relocated
known root alone passes for standalone cases and first/second corpus phases;
raw comparison notices relocation. Twelve controls reject the original nine
function/line/column/other-URL/wire/event/classifier/message/ordinary-stack changes
plus literal marker aliases, diagnostic-shaped request bodies and diagnostic-shaped
tool content. This makes --check portable without suppressing source diagnostics.
