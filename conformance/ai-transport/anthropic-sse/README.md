# Pinned Anthropic private SSE framing

`SseProfile::Anthropic` follows pi-mono
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`, not the imported Anthropic SDK
iterator. `packages/ai/src/api/anthropic-messages.ts:575-583` calls
`.asResponse()`; `:589` consumes `iterateAnthropicEvents`, whose `:471` calls
the private `iterateSseMessages`. Parser authority is `:313-458`, consumer
filtering/JSON handling `:460-499`. The complete pinned
`packages/ai/test/anthropic-sse-parsing.test.ts` was inspected, including raw fake
client responses, unterminated event construction and unknown trailing events.

Source blob: `6c4ff62d08c2082e962ef79fdc913e35826fa4df`.
Source SHA256: `c63b3cbe8afe2504bb5f60f2e7f7b8f773de68486a03cc0cc266c3c76f73f7f6`.
The initial oracle uses portable checkout resolution in `initial-oracle.mjs`,
while retaining the pinned processor and source identities above. Script
SHA256 `f8d491d5d3e7c0dcca0e2721cfb9e509756e8f68b302ce4355b1962ddd26b011`.

## Executed evidence

Run from the workspace, without ambient credentials or SDK installation:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/anthropic-sse/initial-oracle.mjs
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/anthropic-sse/oracle.mjs --check
```

The retained expected `frames.json` is generated exclusively by executing the
pinned private functions loaded from Git objects in a Node VM. Node version and
source digest are checked. It covers seventeen inputs, 577 two-chunk splits and
bytewise feeds: 2,308 per-read/EOF stages. Controlled reader acknowledgement, not
sleep or guessed microtasks, determines each stage. No network is invoked.
The original oracle and the Rust pre-fix regression demonstrate the same causal
counterexample: a complete CRLF frame emits one named JSON frame, whereas a
read ending after `event: message_stop\r` followed by LF emits a named empty-data
frame and an unnamed JSON frame. Bytewise input preserves that source quirk.

Streaming TextDecoder retains incomplete UTF-8 between reads, replaces malformed
sequences, flushes incomplete input at EOF and removes a BOM only once. A leading
empty line also consumes the stream's initial
BOM position, so a later BOM is retained. CR is an
immediate line ending; only a LF already in the same read is paired with it.
Multiline values join with LF; exactly one leading space after colon is removed;
colonless data, event-only frames, unknown names and literal DONE are retained.
Blank lines with neither nonempty event nor any data do not reset source raw-line
state. EOF decodes the final unterminated line and flushes a pending event.

Four consumer observations execute the pinned `iterateAnthropicEvents` and pinned
`utils/json-parse.ts` repair code too: named `message_stop` with `{}` yields `{}`
without inventing a JSON type; unnamed JSON containing `type: message_stop` is
ignored; unknown event with non-JSON data is ignored; an unterminated named typed
event is yielded at EOF. No SSE-name-to-missing-JSON-type fallback exists in this
caller. These are source consumer evidence, not transport JSON interpretation.
Provider reducers still own validation, errors and terminal selection. The
transport frame retains event/data plus bounded transient raw-line context through
`SseFrame::raw_lines_for_adapter(&self) -> Option<&[String]>`. Anthropic returns
Some exact nonempty decoded source lines; other profiles return None. The carrier
is private, non-Serde and non-Display; existing frame Debug remains fully redacted.
Raw lines may affect the provider's own malformed-JSON error classification, so
the trusted adapter must classify source-equivalent text before discarding them.
Transport never classifies, and raw context never belongs in public diagnostics.

## Resources and demand

The private chunk-length queue preserves push boundaries even if callers enqueue
multiple chunks before asking for frames. Parity is for the same supplied read
sequence; different native HTTP runtimes need not partition identical wire bytes
into identical body chunks. Callers must not silently coalesce/split reads when
claiming this source's stage-sensitive behavior. Each nonempty live marker charges
`size_of::<usize>()` against `max_buffer_bytes`; empty pushes add no marker.
The continuous queue charge includes raw bytes, decoded pending line, incomplete
UTF-8 suffix (still raw), normalized pending frame and markers. Checked arithmetic
rejects overflowing/exceeded budgets, including empty intake. Pending normalized
frame charge includes field lines and separating LF, excluding line terminators
and the initial BOM; it preserves raw-line accumulation across empty flushes.

Exact UTF-8 replacement length is checked before allocating decoded text. Parsing
stops at the next requested frame: a terminal can be observed before an oversized
trailing frame is inspected, provided the independent intake budget admits the
raw chunk. Frame/feed/intake/growth failure resets raw, decoded, field, UTF-8 and
chunk-marker ownership and closes the decoder. These are logical payload/marker
charges, not allocator-capacity or secure-zeroization guarantees.

Pending Anthropic state retains each decoded raw line once, with only two booleans
tracking flush eligibility. Event/data fields are derived at emission, not stored
as duplicate queued text. Normalized pending frame bytes retain precisely the old
charge, including comments and ignored/repeated fields across empty flushes. The
returned frame owns its bounded raw context alongside derived event/data; consumer
retention after emission is outside the decoder's queued-byte budget. No allocator
capacity, temporary-copy or global caller-retention bound is claimed.

The additive raw-context oracle executes the pinned private parser unchanged and
captures all three source fields. Its nine inputs cover 1,940 read/EOF stages,
including comments, ignored/colonless/repeated fields, empty-event overwrites,
empty-flush carry, CR read boundaries, BOM, UTF-8 replacement and truncated EOF.
The unchanged 2,308 event/data stages and 903 OpenAI SDK stages still apply.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/anthropic-sse/raw-context-oracle.mjs --check
```

Raw-context tests also verify profile isolation, redacted Debug, clone ownership
beyond decoder failure/drop, exact carried-comment intake/frame thresholds and
complete raw-state cleanup at EOF, frame/feed failures and the old intake/growth
failures. Private assertions follow the changed state representation without
weakening a threshold. No external SseFrame literal existed in the searched
workspace crates/conformance tree before this additive private field.

New tests cover exact queue/frame/feed and decoded-growth boundaries, empty push
metadata, postfailure cleanup, queued CR boundaries and terminal-tail ownership.
The old portable assertions did not require changes. OpenAI's 903 retained SDK
stages and all other profile tests remain unchanged and passing. This corrects
the existing Anthropic profile; no new public variant, provider-local parser,
dependency, or port is added.

Focused checks: `cargo test -p pi-ai-transport --lib --test anthropic_sse --test
openai_sse --test portable --locked --offline`. Full acceptance: `./scripts/verify.sh`.
