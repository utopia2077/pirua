# OpenAI SDK SSE profile

`SseProfile::OpenAI` projects installed OpenAI 6.40.0's Node iterator, not a
generic EventSource interpretation. Authority is `src/core/streaming.ts:224-364`
and the complete `src/internal/decoders/line.ts` and `src/internal/utils/bytes.ts`.
The retained oracle fingerprints both source and executed JavaScript files.

`oracle.mjs` invokes the exact exported `_iterSSEMessages` with a controlled
async body. Each stage waits for the iterator's next body read; no time-based
sleep or guessed microtask count determines release. Twenty inputs cover all
301 two-chunk byte splits, with observations after each chunk and EOF. They
cover the three raw chunk delimiters, delayed mixed endings, trailing CR carry,
missing blank lines, BOMs on successive lines, UTF-8 splits/replacement, empty
data, event-only, comments, and literal DONE data. JSON/DONE semantics belong to
the adapter. No fetch or endpoint is used.

The SDK gates raw chunks on LF-LF, CR-CR or CRLF-CRLF. Its line decoder may
retain a trailing CR after that gate. EOF flush appends one newline to a pending
line, never fabricates a blank SSE line. `TextDecoder.decode` strips a BOM on
each line. In particular `data: x\r\r` emits only at the next released chunk or
EOF; `data: x\n` and `data: x` never emit.

Installed Anthropic 0.91.1 has identical line/byte decoder fingerprints and the
same iterator algorithms. The oracle also executes that SDK and asserts identical
observations for all 301 splits. This proves SDK iterator parity, not which
iterator a pi adapter consumes. The pinned Anthropic adapter calls `.asResponse()`
and uses its own chunk-sensitive parser; it must not select this profile merely
because the installed SDK iterators match. Other profiles remain unchanged here.

Limits remain explicit: total queued raw bytes plus pending normalized frame
bytes are capped continuously, including during UTF-8 replacement growth, not
only at intake. Normalized frame content excludes its blank delimiter and line
BOMs; emitted frames per push are demand-counted. Before allocating decoded line
content, the parser measures the exact valid UTF-8 and replacement-byte lengths
and checks the pending frame plus remaining raw tail. Empty pushes also reject an
already-exceeded or overflowing charge. These are logical byte charges, not an
allocator-capacity, transient-copy, or secure-zeroization guarantee.

A gate-delayed raw sequence remains subject to the cap before parsing; no partial
event is silently truncated. Every limit failure closes decoding and resets all
owned event/data, release, frame and line state, including intake failures after
a CR-held event. The EOF flush uses the SDK's one virtual newline without adding
an allocated raw byte or changing release/blank-line semantics. Pending final
unclosed SSE data is discarded by the SDK EOF rule, not interpreted as a frame.

Resource regressions are source/contract-derived byte arithmetic, separate from
the unchanged 903 SDK-stage observations: `data:` plus four invalid bytes and
CR-CR is eleven raw bytes, then seventeen normalized frame bytes plus one held CR
(eighteen total). Caps17/18/19 prove below/equal/above; cap11 reproduces the
original escape. A second case charges an existing normalized line and waiting
raw tail at20/21/22. Private-state witnesses verify complete cleanup after intake
and normalization failures, and defensive empty-intake overflow rejection.

Verify: `node conformance/ai-transport/openai-sse/oracle.mjs --check` and
`cargo test -p pi-ai-transport --test openai_sse`; full acceptance uses
`./scripts/verify.sh`.
