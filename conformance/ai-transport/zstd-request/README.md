# Bounded zstd request encoding

`compress_zstd_request(&[u8], ZstdRequestLimits)` is a synchronous no-I/O
primitive. Limits have explicit input/output byte counts and no Default or
implicit unlimited mode. It adds no request headers, cancellation, environment,
retry, fallback, thread or network owner. This directory follows the existing
`conformance/ai-transport/` layout.

## Source and reproduction

The executable oracle reads the complete `loadNodeZlib` and
`compressRequestBodyZstd` helpers plus their level constant from
`packages/ai/src/api/openai-codex-responses.ts:193–224` at
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`. It invokes the actual Node26.8.1 builtin
zlib, zstd1.5.7. Builtin JS SHA-256:
`db9aad39b945f269c8ef4af586b7b9a07e792bdff9abecf36178c81090233c50`.
The helper hash and runtime identities are embedded in trace.json. No Rust
output is used to generate expectations.

Node's builtin `zlibBufferSync:181` converts strings to UTF-8 bytes;
`processChunkSync:429` repeatedly drains bounded output chunks using the finish
directive; `zstdDefaultOpts/Zstd:916–969` selects `ZSTD_e_end` and applies explicit
params; `zstdCompressSync:1120` connects the convenience call. The pinned helper
sets compression level3, catches failures and returns null. That null/plain-body
fallback belongs to the later Codex consumer, not this fallible primitive.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/zstd-request/oracle.mjs --check
cargo test -p pi-ai-transport --test request_encoding --locked --offline
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/zstd-request/cross-decode.mjs
```

Fourteen cases include empty input, JSON, multibyte text, repetitive data around
256/16384/131072 boundaries, a262145-byte body, deterministic noise and2000
structured records. With the installed versions, Rust level3 frames match actual
source frames byte-for-byte. Rust also decodes every Node frame; the independent
Node cross-decoder launches the freshly resolved Cargo test artifact and decodes
every Rust output back to the source input. These actual outputs are not stored
as expectations. Exact compressed bytes are version-bound evidence, not a promise
that every compatible encoder/platform must always choose identical blocks.

A level3→1 mutant initially survived the simple repetitive/noise cases; the
source-generated structured-record vector falsifies it. Restoring3 passes the
same exact-byte assertions. This limitation was resolved by adding evidence,
not weakening an assertion.

## Allocation and failure boundary

Input length is checked before output allocation or codec construction; the
borrowed input is never cloned by Rust. A fallible Vec reservation requests
`min(ZSTD_compressBound(actual_input_length), max_output_bytes)`, initializes
that logical slice, and passes only the slice length to the codec. A huge
declared allowance therefore does not cause a huge reservation for a small body.
Allocator rounding/metadata are not a physical-RSS guarantee. Successful returned
length never exceeds the output limit; rejected or incomplete bytes are never
returned. Empty input is valid and needs its9-byte zstd frame, not zero bytes.

A new locked zstd-safe8.0.0/zstd-sys2.1.0+zstd1.5.7 CCtx applies level3 and
executes blocking single-threaded `compress_stream2(e_end)`. End completion and
complete input consumption are checked. The simpler ZSTD_compress entry point
can reject a destination whose final compressed frame would fit; the streaming
end path admits exact frame-sized output capacities without an oversized output
scratch buffer. Every success/error drops the context; no global context cache
or codec reuse after errors exists. Destination/allocation failures map to
LimitExceeded using the locked library's fixed error names; other codec failures
map Protocol, with no raw codec prose in diagnostics.

Codec working storage is additional to the output budget, not silently charged
to it. Locked `clevels.h:31` gives level3 dfast's maximum windowLog21,
chainLog16/hashLog17; actual small inputs select smaller parameter tiers. Native
workspace contains match tables, token/sequence and temporary/block state. Its
buffered path may additionally hold up to window+block input bytes and
compressBound(block)+1 output bytes; window is at most2MiB and block at most128KiB
for these defaults. See `zstd_compress.c:1701–1753,1811–1828,2129–2150`. These fixed
level/no-dictionary/no-worker parameters and admitted input determine codec
storage; max_output_bytes is not a bound on that context, nor is physical RSS,
allocator fault injection or secure zeroization claimed. No new dependency or
feature was enabled, and existing workspace unsafe-code prohibition is retained.

## Unverified consumption

The pinned Codex caller compresses once after its WebSocket path and before the
SSE retry loop (`:368–395`), reuses bytes, sets Content-Encoding only on success,
and falls back to plain JSON on encoder failure. This unit does not implement or
prove those caller effects, nor WebSocket compression or any live backend.
Source browser/unavailable-codec behavior remains a consumer concern. No provider
is enabled by default and no S03 evidence is changed.
