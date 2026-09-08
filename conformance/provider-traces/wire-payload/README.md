# Ordered middleware payload custody

`WirePayload` is the non-Serde, redacted-Debug, trusted provider carrier over the
existing ordered `serde_json::Value`. Construction, cloning, borrowing, mutation
and extraction do not pass through core `JsonValue` or serialize the payload.
The seven existing families retain the logical value through middleware and their
own SDK converters; the supplied Anthropic client receives the same carrier.
This does not change core DTOs, persistent object ordering, response headers,
callback cancellation, retry ownership or any existing fixture.

## Source and reproduction

Both oracles load complete pinned adapters and helpers from pi-mono Git object
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`. Google also executes installed
`@google/genai` 1.52.0, with the exact SDK SHA-256 in `google.json`. Node is
26.8.1. All requests use synthetic injected Fetch and constructed Responses;
there is no host credential discovery or live provider access.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/wire-payload/google-oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/wire-payload/pi-oracle.mjs --check
cargo test -p pi-ai-providers --all-features --test wire_payload --locked --offline
./scripts/verify.sh
```

Google's six complete observations cover forward/reverse case-variant headers in
replacement and in-place mutation, no-op logical key order, numeric-key and Number
controls, complete SDK requests, every event/result, and middleware order. The SDK
joins lower-then-upper `x-order` values as `lower, upper`; the reverse order yields
`upper, lower`. A deliberately reintroduced core-JSON roundtrip fails the former
request with `upper, lower`. The pre-correction complete adapter independently
reproduces the same failure. The original separate Vertex witness is not modified;
this base has no Vertex consumer and its later composition is not claimed here.

Pi's nine complete observations compare final bytes (not parsed JSON alone),
events/results and callback order. They include numeric index boundaries
4294967294/4294967295, leading-zero keys, insertion order, `1e20`, negative zero,
and rounding 9007199254740993 through JavaScript Number. Large fixtures retain a
source SHA-256/length instead of a megabyte-long string; the Rust test additionally
compares every byte to the source-defined repeated-character payload. Requests
above one MiB and at depth 128/129/256 succeed; a hook reduces an initially large
context before consumption. Large/deep and shrink controls also pass against the
old Pi carrier/serializer, independently establishing that no new cap is added.

## Limits and admitted variance

The public checked serializer and the six existing checked consumers retain their
one-MiB and depth-128 limits at their existing consuming stage. Carrier construction
does not add a pre-hook rejection. Pi alone uses a private no-new-fixed-limit
policy in the **same** serializer algorithm, preserving its prior domain while
correcting JavaScript key and Number spelling. Pi still allocates its full outgoing
body; ordinary allocator failure and recursive-stack limits are not converted to
new typed byte/depth rejections. This is not a physical-memory, arbitrary-depth or
recoverable-OOM guarantee. Existing incoming/stream/resource limits are unchanged.

Finite ordered JSON remains distinct from arbitrary JavaScript objects: no
undefined, prototypes, executable getters/toJSON, functions, BigInt, nonfinite
numbers or lone UTF-16 surrogates. The wrapper cannot recover ordering already
lost before construction. Hosts choose their mutations explicitly; for JS-like
deletion preserving subsequent insertion order use the ordered map's
`shift_remove`, not its swap-removal operation. Numeric keys are enumerated in JS
index order during wire projection, not by globally sorting ordinary keys.

Compile-fail documentation verifies both missing Serde directions. Debug tests
verify payload content is redacted; accessor use is deliberately trusted and does
not claim secret zeroization. Complete snapshot comparisons decode core event and
message types so JavaScript zero and finite-f64 zero remain the same value.
SDK runtime identity remains the existing documented `pi-ai-rust` normalization.
