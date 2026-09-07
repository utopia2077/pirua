# Native Fetch content

Authority is actual Node26.8.1's bundled Undici8.10.0, SHA256
`9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d`.
This unit does not substitute the separately installed npm Undici8.9.0.
Every oracle asserts runtime/version/hash and uses only real loopback servers.
Expected encoded and decoded bytes originate in Node, never the Rust decoder.

Run each of `oracle.mjs`, `incremental-oracle.mjs`, `held-body-oracle.mjs`,
`tls-oracle.mjs`, `header-projection-oracle.mjs` with
`env -i PATH=/usr/bin:/bin node <path> --check`.
The main 57-case corpus covers codecs/aliases, reversed stacks, five/six codings,
unknown-chain bypass, null/HEAD/redirect/manual responses, empty/truncated/corrupt
and trailing data, and default/overridden/conditional/Range request headers.
Ten incremental cases include valid compressed empty frames and multi-megabyte
streams; eight held-body cases distinguish frame completion from HTTP EOF.
The TLS oracle uses an OpenSSL-generated one-day synthetic local root in a unique
temporary directory and a clean Node child with only that explicit extra root.
The synthetic material remains in that temporary directory for inspection.
No real credentials, public endpoints, installation scripts or provider calls run.

## Source routines and observations

The complete bundled `fetching`, `appendFetchMetadata`,
`httpNetworkOrCacheFetch`, `httpNetworkFetch` (including dispatch callbacks),
`InflateStream`, `writeH1` and relevant buffer dispatch were inspected.
Bundle offsets: metadata319293, network/cache646394, network653551,
InflateStream338310, writeH1408341, HTTP/1 buffer415724.
Node's bundled `zlib` module `processCallback` explains output publication and
early EOF when a codec leaves trailing input. Source trace independently catches
gzip member+garbage publication before error, and the non-Gunzip early EOF cases.

- Default Accept, Accept-Language, User-Agent, Sec-Fetch-Mode, Content-Length,
  conditional cache fields and HTTP/HTTPS Accept-Encoding are network-only.
  Range appends identity even to an explicit coding list; Host is regenerated.
  Empty PATCH/QUERY/PROPFIND/PROPPATCH lengths include native dispatch behavior.
- gzip/x-gzip and zlib-or-raw deflate use tolerant sync-flush finishing; Brotli
  tolerates incomplete finish. Zstandard requires a complete frame, stops at the
  first frame, and an empty encoded HTTP body is a body error in the executed
  runtime. A valid encoded empty frame is different and succeeds.
- Non-Gunzip codecs with trailing bytes expose EOF without polling a held-open
  upstream again. Exact frame boundaries alone still wait for HTTP EOF/cancel.
- More than five codings rejects at acquisition. Unknown coding anywhere bypasses
  the whole chain. HEAD/null-body and followed redirect bodies skip decoding.
  A manual redirect is decoded normally. Original response headers stay unchanged.

## Ports and ownership

`FetchResponseSource::{RawNetwork,Preconstructed}` and `FetchContentLimits` are
construction configuration via `HttpTransport::with_fetch_response_source` and
`with_fetch_content_limits`. RawNetwork is default. Redirect policy is independent.
Preconstructed means a trusted semantic Response: no network defaults, no second
decoding, and no native fallback. The existing finite validation/projection still
applies. A binding that owns redirects independently selects Manual.

The object-safe additive `RawHttpClient::send_fetch_hop(request,runtime)` has the
same borrowed lifetime and boxed response future as `send`, and defaults to
delegating to `send`. RawNetwork uses this one-hop hook; Preconstructed and raw
`HttpTransport::send` use `send`. Native overrides only the protocol-aware default:
missing HTTP/1 Connection becomes close for HEAD, keep-alive otherwise; explicit
caller values survive. Negotiated HTTP/2 receives no added Connection field.
No hidden header metadata, request literal change, retry owner or driver is added.

Successful body decoding remains outside acquisition retry. Each codec reads only
on downstream demand; the original managed body retains caller cancellation and
timeout custody. Error/EOF/drop closes the nested body/codec chain; cancellation
races noncooperative reads. Unknown/unencoded streams remain raw.

## Resource and dependency decisions

Defaults are 1MiB pending bytes per layer, 32KiB output per call and 32MiB logical
codec storage per layer, with at most five layers. Pending means live unconsumed
encoded input plus the incoming chunk, never cumulative traffic. Each layer's
output buffer is separately bounded. A caller-selected output chunk must be
nonzero and fit the pending budget. Native transport framing and an injected raw
client's already allocated returned Vec are outside decoder allocation custody;
oversized chunks are rejected before adoption/concatenation. Allocator rounding,
upstream allocations, physical RSS and secure zeroization are not promised.

- flate2=1.1.10 uses zlib-rs0.6.7, not a host zlib. Its inflate allocation is fixed:
  State plus a 32768+64-byte window and 64-byte alignment padding
  (`inflate.rs:2183-2209,2267`). The decoder reserves a conservative 1MiB per
  layer before construction; no untrusted window allocation follows.
- brotli-decompressor=6.0.0 uses strict standard-window mode and a shared bounded
  allocator for byte/u32/Huffman cells. Checked length multiplication and total
  admission precede reserve/resize; state size is included. Allocation rejection
  is checked during construction and decoding. Ring/context/Huffman allocations
  are checked by the inspected library before access (`decode.rs:1312,1924,3312,
  3508,3560`).
- zstd-safe=8.0.0 / zstd-sys2.1.0+zstd1.5.7 use vendored C with pregenerated
  bindings, no bindgen/legacy/dictionary-builder/MT feature. WindowLogMax is
  derived from budget minus 1MiB fixed overhead, before decoding. Zstd rejects
  oversized windows before allocating its buffers (`zstd_decompress.c:2230,2263`);
  DCtx + input<=128KiB + output<=window+2*128KiB+padding fits that reservation.
  The native version test requires10507. Build scripts use the normal C toolchain;
  an explicit build-host ZSTD_SYS_USE_PKG_CONFIG can override vendoring and is not
  a supported reproducible build configuration for this evidence.

All direct codec versions are exact pins; transitive source/checksums are locked.
No new production crate, internal edge or provider/default feature activation.
Runtime portability was executed on Linux x86_64 only; other targets need their
normal C toolchain for Zstd. Code has no local unsafe implementation.

## Tests and finite non-claims

`cargo test -p pi-ai-transport --features native --test fetch_content --locked --offline`
covers complete vectors, bounded incremental output, working-window rejection,
noncooperative EOF/cancellation/drop, retry-stage composition, original runtime,
real HTTP and verified TLS H1/H2, raw/preconstructed controls and Hyper warning
absence. Original Fetch/native/portable and provider tests remain unweakened.
The Pi, Mistral, Anthropic, standard Responses and Completions fixture constructors
explicitly identify preconstructed custom-fetch responses; their native overrides
remain RawNetwork and all assertions remain. The last three were added during
main assembly after the exact SDK-header comparisons exposed the different
observation stage; no fixture expectation or native constructor was changed.
The original Fetch method/header test stays RawNetwork: its former three-field
count is upgraded to the seven-case Node oracle's exact Fetch header maps, with
physical Host/Connection also retained separately. Cookie/Set-Cookie assertions
are unchanged. Node's lowercase-patch advisory warning is not reproduced by Rust.

Causal guard witness: disabling only the pending-input admission conditional made
`codec_limits_cancellation_and_drop_release_without_retry` fail (exit101): the
8-byte budget yielded decoded bytes instead of LimitExceeded. Restoring the guard
made that same unchanged test pass (exit0).

This is not arbitrary RequestInit, streaming uploads, browser CORS/cache/cookie
jar, new authentication, pooling, protocol fallback or Fetch's separate 421/407
workflow. No arbitrary external exception text or exact asynchronous chunk-size
identity is claimed. Codec error categories are typed/redacted Connection;
configured limits and caller cancellation retain their distinct safe categories.
Acceptance is the unchanged offline `./scripts/verify.sh` gate.

## Gzip publication correction

The independent review found that output chunk size was incorrectly determining
bytes visible before a trailing gzip error. Actual Node Gunzip uses a persistent
16-KiB output allocation; each native write is validated before processCallback
publishes its output. Full output does not suppress remaining-input validation:
after StreamEnd, even zero output room still permits a next member/header error.
Thus an A-member of16384 bytes followed by garbage exposes0 bytes;16385 exposes
16384;32768 exposes16384. This is a total-prefix difference, not chunk identity.

Gzip now tracks the source allocation offset across input callbacks, validates
the complete current native operation, and only then commits bytes. The public
output_chunk_bytes ceiling splits this already-committed prefix; it cannot alter
which bytes precede an error. One private <=16-KiB semantic output/commit buffer
is explicitly charged inside the existing conservative1-MiB gzip working-state
reservation. The configured codec budget, encoded pending-input budget and
public per-call output budget keep their existing numeric defaults. No full
response collection, additional input demand, retry owner or port is introduced.

`gzip-boundary-oracle.mjs --check` separately reproduces104 actual Node Gunzip
write/error traces (8 variants at13 sizes, including multiples of16384) and13
real Fetch HTTP confirmations. Its run-length records encode every exposed byte,
including A/B member ordering, rather than prescribing asynchronous chunk shapes.
Member/garbage and trailer/garbage split feeds, persistent offsets, valid/truncated
streams, empty/interleaved members and full-output validation are retained.
The original five oracles and fixtures are unchanged. Rust compares every input
step under output ceilings17/128/16384/32768, plus actual native HTTP prefixes
under17/32768; old long-stream, working-memory, cancellation and release tests remain.
