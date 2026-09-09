# Logical Fetch input selection

The explicit FetchRequest carries ordered ordinary string record or constructed
Headers data, and text or owned bytes. All are transient and Debug-redacted. Exact
record replacement retains the first own slot; numeric keys enumerate as JS
Object.entries. HTTP validation, ASCII case folding and value trimming do not run
at record construction.

An explicitly configured CustomFetch sees these values before network URL/method/
header/body preparation. Its response is preconstructed: no redirect, second
content decoding, network defaults or fallback occurs even if ordinary network
construction settings are selected. Raw send retains its explicit raw backend;
new_custom has no raw capability and reports UnsupportedOperation. Cancellation,
selected deadlines and explicit retry remain owned by the existing transport.
Only rejection of the configured send gains a closed custom-Fetch-stage marker,
while its original kind is retained. SDK acquisition owners may apply their own
wrapper; direct callers do not inherit SDK Connection classification. Later body
reads and native/raw failures are unmarked, and Cancelled/Timeout stay distinct.

Without custom Fetch, logical inputs become the existing physical request and
continue through the unchanged network Fetch pipeline. Text contributes its
source content-type default when absent; legacy byte requests preserve their
old body convention, including empty GET/HEAD bytes meaning absence. Existing
send_fetch wraps selected physical headers/bytes for custom consumption, and
keeps the original network path otherwise. It is not an arbitrary RequestInit
presence/override union or a new browser Fetch implementation.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/fetch-request/oracle.mjs --check
cargo test -p pi-ai-transport --test fetch_request --locked --offline
cargo test -p pi-ai-transport --features native --test fetch_request --locked --offline
```

The 15-case Node26.8.1 oracle records actual Request-constructor selection for
record aliases, literal commas, text versus bytes, BOM bytes, absent content-type,
empty bodies, whitespace, UTF8 rejection and invalid names/values/method-body
combinations. Tests compare its full selected header values and body bytes. A
separate custom-capability test preserves even non-network URL/method/UTF8 values,
checks explicit original forms and proves no redirect/decoder/raw fallback.
A real native logical-request socket test also proves text content-type/defaults,
alias joining, bytes and response release. The peer must observe actual EOF while
the response, transport and runtime remain alive; a read error fails the proof.
A held-open peer control rejects timeout as EOF, then proves that the same peer
can still send data before its retirement produces EOF. Read deadlines only bound
failure; they never count as release. Existing native/Fetch tests remain unchanged
and retain their socket coverage.
