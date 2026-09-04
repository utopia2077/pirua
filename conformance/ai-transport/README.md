# Transport prerequisite evidence

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
This is a transport prerequisite, not canonical-adapter closure.

`crates/pi-ai-transport/tests/portable.rs` uses source-derived expectations:

| Witness | Pinned authority | Observation |
| --- | --- | --- |
| Retry acquisition/cap/backoff | `packages/ai/src/utils/provider-retry.ts:14-134` | Header overrides, parseFloat prefix, 500 ms initial backoff, inclusive 60-second cap, cancelled backoff |
| No retry on success/stream | `packages/ai/src/api/openai-responses.ts:146-162` | Only failed response acquisition can retry |
| No retry profile | `packages/ai/src/api/pi-messages.ts:382-414` | One attempt even for retryable status |
| Header versus whole lifetime | `packages/ai/src/api/openai-codex-responses.ts:387-405`; `packages/ai/src/api/mistral-conversations.ts:287-316` | Separate acquisition and body deadline ownership |
| Proxy selection | `packages/ai/src/utils/node-http-proxy.ts:13-111` | Scoped/ambient precedence, suffix/exact/port bypass, unsupported proxy rejection |
| SSE profiles | `packages/ai/src/api/pi-messages.ts:266-310`; `openai-codex-responses.ts:765-821`; `mistral-conversations.ts:435-500`; `anthropic-messages.ts:460-487` | First/joined data lines, named events, line endings, trailing-frame and DONE differences |

Cancellation precedence, non-cooperative loser drop, bounded buffers and full
wire redaction are accepted Rust target requirements from S04's implementation
brief, not claims that upstream JS already implements every guard.

The counterexamples deliberately include a successful response carrying
`x-should-retry:true`, a retryable response carrying `x-should-retry:false`, an
uncooperative send/body/clock, a simultaneous deadline/cancellation, and one-byte
frame/body-limit overflows. None can be replaced by an ordinary happy-path test.

The default retry date parser handles RFC HTTP dates, canonical JS `Date.toString`
with numeric GMT offsets (and an optional parenthesized zone label), and standard
ISO years/dates or explicitly zoned ISO date-times. ISO date-times support minutes,
optional seconds and three-digit milliseconds, `Z` or signed `HH:mm` offsets,
and standard signed six-digit extended years. Components, leap days, offsets and
the JavaScript time range are validated. Unzoned date-times require a host timezone
and therefore explicit `RetryDateParser` injection, as do other implementation-
defined V8 legacy forms. Numeric parseFloat header precedence remains upstream-
exact even for strings that could separately parse as dates. Full V8 Date.parse
parity is not claimed. The timer conversion profile maps negative, nonfinite
and host-timer-overflow delays to immediate scheduling. Upstream host-specific
minimum timer clamping is not a portable exact-time promise.

Raw response headers/bodies are deliberately exposed only for transient adapter
classification. They are not public diagnostics. Adapters must classify retry
and overflow before producing reviewed safe text; transport does not infer these
provider-body semantics or synthesize an assistant message.

The canonical SSE interface is `push(chunk)`, `next_frame()`, `end_input()`.
Intake defaults to 1 MiB of queued bytes and checks that budget before accepting
the chunk. The independent frame budget defaults to 256 KiB excluding its
delimiter and the stream-leading BOM (PiMessages first normalizes CRLF).
Potential incomplete delimiters do not consume the frame budget. Up to 1,024
frames may be emitted between pushes; there is no allocated event queue.
Adapters consume one frame at a time and stop immediately at their terminal.
A later overlimit frame in the same accepted chunk cannot erase that terminal.
Limits are configurable and failures never truncate an emitted event. Provider
JSON decoding and tool-argument repair are outside framing.

Independent review counterexamples are permanent regressions: all family BOMs,
bytewise BOM splits, preservation of later BOMs, Anthropic event-only and colonless
fields, every CR/LF delimiter split at exact and one-byte-over frame limits,
terminal-before-invalid-tail, NO_PROXY port overflow and ECMAScript NEL exclusion.

Direct dependencies in this prerequisite are exactly `url = 2.5.8` (WHATWG URL
parsing for proxy targeting) and `httpdate = 1.0.3` (HTTP retry date parsing).
Versions were resolved from Cargo's crates.io metadata; no core dependencies
or accepted internal edges were changed.
