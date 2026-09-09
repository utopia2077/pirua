# Configured Cloudflare gateway binding

The four URL templates and auth sentinel retain exact pinned source bytes. Source
is pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`; oracles read Git objects,
not the mutable source checkout. All effects use configured synthetic bindings,
with no account, credential discovery, SDK installation or network fallback.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/cloudflare-gateway-binding/oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/cloudflare-gateway-binding/pi-oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/cloudflare-gateway-binding/openai-oracle.mjs --check
cargo test -p pi-ai-providers --features cloudflare-gateway-binding,wire-pi-messages,wire-openai-completions --test cloudflare_gateway_binding --locked --offline
```

The direct 50-case oracle executes the complete binding helper and covers prefix
normalization, query/fragment, provider/endpoint splitting, method, JSON null and
other finite JSON values, duplicate keys/Number rounding and root/nested numeric own-key enumeration at
the binding query itself, byte replacement/BOM
versus text BOM, depth129/256, a value above one MiB, ordered record versus actual
Headers input, exact strip set and untouched preconstructed redirect/error bodies.
Failed validation has no gateway/run effects. Header __proto__ string assignment
creates no own entry in the source plain object; it is not an extra HTTP filter.

The Pi oracle executes the complete pinned adapter and binding together. Three
original controls distinguish forward/reverse alias records and a literal comma
value; two additional complete cases retain middleware-produced depth129/256
through Pi serialization and binding JSON.parse; a sixth case proves direct helper
rejection classification. The OpenAI oracle executes the
complete pinned Completions adapter, actual installed OpenAI6.40.0 and complete
binding with a synthetic leaf. Actual SDK input is Headers plus string; explicit
null auth fields remove the placeholder and the shim strips only gateway auth.
All requests, query own-key order, full event sequences and final results are
compared. A fourth SDK case rejects outside the configured prefix with zero
binding effects and preserves source retry classification; fixed typed error text
and the existing Rust diagnostic projection are compared separately. Only the pre-existing SDK platform-header normalization is applied after
actual binding observation. The supplied response already contains decoded SSE
with a gzip metadata header, proving no second content decode occurs.

## Rust boundary

Enable the default-off `cloudflare-gateway-binding` feature. Construct
GatewayBindingFetchOptions from an explicit AiGatewayBinding, base URL and gateway
name, then create_gateway_binding_fetch and into_transport with an explicit retry
jitter capability. The gateway selector is bounded and synchronous. Each validated
request selects one gateway and invokes its run once with the original runtime.
No selector/runner is invoked before URL/method/body/header conversion succeeds.
Custom Fetch rejection retains its original closed error kind and gains an
explicit rejection-stage annotation. OpenAI acquisition applies its SDK Connection
wrapper; direct Pi keeps nonretryable helper failure. Constructor errors and later
body errors never gain that marker, and Cancelled/Timeout remain distinct. Paired
complete source prefix failures prove the different Pi/OpenAI classifiers. Separate
actual-source checks establish existing Anthropic/Mistral/Google wrappers require
no change (Google uses its source-allowed global-Fetch placement). The transport
owns selected timeout/retry and preempts noncooperative run/body
futures on cancellation; the binding introduces no default policy. Unsupported
raw operations fail with a typed error and cannot fall back to networking.

Transport FetchRequest retains exact record/constructed Headers and text/byte
forms. Pi now submits its original spread record and JSON text before conversion.
Native Fetch still performs its existing validation, joining, defaults and hops;
custom Fetch consumes original logical input. Completions reserves its source
messages slot before stream, with its existing later assignment unchanged. The
binding never tries to reverse a joined comma or reorder a finished SDK payload.

All capability/request/error Debug output is redacted; data is available only to
trusted typed consumers. The universal query uses ordered WirePayload and JS
Number projection, not durable core safe-integer admission. The existing
serde_json/unbounded_depth feature is activated only by binding; only this parser
instance disables its default recursion check. Other decoders retain their checks
under feature unification, covered by an explicit test and full repository gate.

## Limits

The admitted input is the finite Rust record/Headers, text/byte and ordered JSON
projection, not arbitrary Request/RequestInit unions, executable getters, stream
uploads, lone UTF16 surrogates or nonfinite JavaScript Number objects. JSON input
is already owned and parsing holds proportional allocations; no new fixed
one-MiB/depth128 rejection is imposed. Recursive parsing/normalization/drop and
ordinary allocator limits remain, with no arbitrary-depth, RSS, zeroization or
recoverable-OOM promise. Gateway name/prefix matching follows the helper's actual
validation, not additional inferred configuration rules. Existing provider-native
limits remain at their prior consuming boundaries. Full acceptance is only
`./scripts/verify.sh`; no live Cloudflare interoperability is claimed.
