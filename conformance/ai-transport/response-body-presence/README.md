# Immutable Fetch body presence

`oracle.mjs --check` executes actual Node 26.8.1 Fetch against synthetic loopback
responses: POST 200/204/205/304 and HEAD 200. It also records constructed 200 null
versus present-empty bodies. Presence stays unchanged after text consumption.
No provider endpoint or credential is used.

`cargo test -p pi-ai-transport --features native --test response_body_presence
--locked --offline` (one line) checks immutable presence through PolicyBody,
custom Fetch, EOF and close, raw/preconstructed preservation versus network body
suppression, and all five actual native request/status cases. Existing present
body implementations require no migration. AbsentResponseBody is explicit and
has no bytes or read effect; ownership still follows the existing close/drop
contract. Finite HTTP profiles remain unchanged; this is metadata custody only.

The native peer remains open after writing every response. It must receive actual
EOF, with timeout/errors failing the test. The client awaits that observation
while its response, transport and runtime remain alive, then rechecks presence.
This distinguishes client body retirement from the server simply closing itself.
