# Ordered transient provider headers

`oracle.mjs` executes the complete five pinned provider modules and their pinned
helpers from Git object `853a80d26c90a14c1886f0ebb8ffaae133ca2185`, with installed
OpenAI 6.40.0 / Anthropic 0.91.1 and Node 26.8.1. `trace.json` is its output,
not Rust output. Complete input-source SHA-256 values are embedded in the trace.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/provider-header-order/oracle.mjs --check
cargo test -p pi-ai-core --test provider_headers_order --locked --offline
cargo test -p pi-ai-core --lib registry_merge_matches_pinned_order_spelling_and_null_projection --locked --offline
cargo test -p pi-ai-providers --all-features --test provider_header_order --locked --offline
```

The 95 actual adapter requests cover all six case-alias permutations with and
without null, both null orders, Accept/default-slot interactions, and numeric
own-key boundaries. The SDK/custom-fetch seam uses Preconstructed responses;
no network requests, ambient credentials, or native profile substitutions occur.
HTTP 400 terminates each attempt without retries while retaining each family's
actual payload/send/response callback stage. The common user-agent/platform
identity is normalized to `pi-ai-rust`; it is not compared by this fixture.
Only the ordering-sensitive custom names, Accept, and numeric names are compared;
this is not an independent re-audit of all unrelated default headers.

Three additional cases execute the pinned registry `mergeHeaders` function and
compare the Rust registry's actual private merge function: later case-insensitive
replacement removes earlier aliases, retains latest spelling/null, and moves the
replacement to its new own-key slot. Existing registry ASCII-insensitive policy
is unchanged. Prototype emulation and general JSON-object ordering are not added.

`ProviderHeaders` decodes the original object with a visitor, preserves ordinary
own-key order and projects canonical array indices first. Exact duplicate JSON
keys still reject, unlike permissive JavaScript JSON parsing; that is the retained
S03 admission boundary. Cloning, equality, and direct serialization retain the
observable order. `as_map` remains exact-key lookup; `new(BTreeMap)` retains only
its known deterministic order. Neither a prior sorted map nor a prior lossy
`serde_json::Value` can recover original insertion history. Registry construction
is crate-private and never uses a Serde round trip. No new public builder/error
or durable DTO relaxation is part of this correction.

Causal checks: before the carrier change the direct serialize-order regression
failed (uppercase sorted ahead of lowercase). Replacing only Mistral dispatch
with the old `as_map()` iterator makes the first three-alias request send `lower`
instead of pinned-source `caps`; restoring the projection passes. The old S03
rows, fixtures and assertions are not modified. Azure is deliberately absent:
its separate candidate must adopt this accessor and rerun its original F1 probe.
