# Native Fetch header values

Authority: Node26.8.1, bundled Undici8.10.0 (not installed npm8.9.0), and
pi-mono853a80d26c90a14c1886f0ebb8ffaae133ca2185 `utils/headers.ts` and
`utils/provider-retry.ts`. All endpoints/keys are synthetic local loopback.

The original full Pi/Mistral wrappers observe a difference between network
`X-Trace: first ` plus `x-trace: second` and constructed Headers: network yields
`first , second`, constructed yields `first, second`. Hyper's raw HeaderMap has
already removed trailing OWS, so a callback-only trim change cannot repair it.

`oracle.mjs --check` executes eight field-value groups through actual Fetch and
constructed Headers, across ordinary/Cookie/Set-Cookie fields, and invokes the
pinned own-record helper. Network parsing removes leading SP/HTAB but retains
trailing bytes; HeadersList append/get do not normalize again. Cookie joins with
`; `, ordinary fields and Set-Cookie get with `, `; Set-Cookie iteration remains
repeated and the callback record keeps its last value. Empty values, multiple
SP and HTAB distinguish this from trim-after-join. Source builtin anchors:
appendHeader11938, HeadersList11990, appendHeadersListFromResponseHeaders13789,
onResponseStart14763; the script asserts the bundled-source SHA256.

`physical-oracle.mjs --check` records11 actual HTTP/1 admissions. CRLF fields,
leading empty CRLF/LF prefixes, final fields after103 and Latin1 are accepted;
bare-LF status/header/terminator, obs-fold and space-before-colon are rejected.
The source observations do not imply a general second HTTP parser. Hyper remains
primary validation authority and raw send keeps its earlier Hyper behavior.

`h2-oracle.mjs --check` uses a fresh synthetic root and separately negotiated
TLS/ALPN h2 with actual Node Fetch. Five cases preserve legal duplicates/Latin1,
omit invalid leading/trailing SP/HTAB fields and retain non-ASCII NBSP. Actual
Node Fetch was additionally run against the same Rust h2 server used by the Rust
test; all five observations match, so omission is receiver-side, not a Node
server serialization artifact. Only that source-confirmed invalid edge-OWS class
is omitted from native Fetch-hop H2's two admitted views. Raw send retains its
original fields; no Unicode trim or general header sanitization is introduced.

`retry-oracle.mjs --check` executes the complete pinned retry helper and four
actual-attempt controls. Network500 + `x-should-retry: false ` retries; network400
+ `true ` does not. Constructed Headers trims and reverses both outcomes. Lower
HTTP retries remain disabled; copied policy fields retain the already observed
Fetch values, rather than becoming a second Headers construction.

Run each script from the repository root with
`env -i PATH=/usr/bin:/bin node --disable-warning=ExperimentalWarning <script> --check`.
Expected fixtures originate only from those source executions. Runtime tests
consume fixtures offline and need neither Node nor OpenSSL; the optional H2
oracle uses OpenSSL only to generate an ephemeral local trust root.

## Ownership, bounds and declared limits

HttpHeaders carries explicit raw and optional semantic field data, not an origin
flag or fake metadata header. `with_fetch_response_fields` checks per-name
cardinality and Latin1 domain, allows ordering differences, and retains raw
fields. `iter_for_fetch`/`get_for_fetch` preserve supplied semantic values;
existing constructors keep their original constructed-value projection.
Append/set normalize only the newly constructed value, remove affects both,
clone/equality include both views. Debug stays redacted and no Serde is added.
Response middleware preserves the admitted own-record values through both
accessors; original raw repeated headers are not mutated.

Only native Fetch HTTP/1 stores extra field octets in the existing post-TLS
ObserveHead path. Captured field octets are a subset of the unchanged cumulative
head limit(default65536), including informational status/header bytes. Final
metadata is released only after Hyper accepts the head; informational fields are
discarded without refunding their byte charge. Capture stops before body bytes.
All split and byte-budget tests retain the original status/reason behavior.

The input cap is not an allocator/RSS promise. Current-line capacity, name/value
allocations, per-field Vec/String metadata, raw HeaderMap, Latin1 UTF8 expansion
(at most two bytes per admitted octet) and overlap while building explicit views
are all O(head_limit); small fields can have proportionally larger metadata.
No unbounded aggregate body counter or additional network reader/task/retry
owner is added. Cancellation, drop, validation and budget failure release the
same connection/capture owner. HTTP/2 retains the existing library head-admission
limits; the HTTP/1 cumulative octet cap is not claimed to bound HPACK memory.

No complete browser Fetch, exhaustive malformed-HTTP/HPACK compatibility,
physical allocation/zeroization or cross-target claim. These changes leave codec,
SSE, provider payload, core and dependency responsibilities unchanged. Full
acceptance remains `./scripts/verify.sh` plus independent review.
