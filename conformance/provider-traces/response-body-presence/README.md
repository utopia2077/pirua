# Response body presence

The complete pinned Pi, Anthropic, Mistral, OpenAI Completions, OpenAI Responses
and Azure Responses adapters, plus Google GenAI and configured Vertex, execute
with synthetic configured Fetch and actual installed SDKs. The retained 34-case
trace covers the first six families receiving present-empty 200, absent 200, absent 204/205, and absent 400.
Google GenAI/Vertex add present-empty versus absent 200 controls through actual
SDK1.52.0 and source-allowed global Fetch, with an explicit synthetic API key
for Vertex and no discovery. Google also rejects absence after Start. All
invocations set maxRetries=2 yet perform one send, preserving the post-success
no-replay boundary. Source hashes, Node and SDK versions are in the fixture.
Children must run with `env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1`.

Absence is a construction fact, not EOF. Pi and Mistral reject absent successful
bodies before Start; Anthropic and OpenAI SDK streaming reject after Start.
Unsuccessful responses retain their existing header/callback and retry ownership.
All present-empty controls keep their source missing-terminal classification;
absent controls preserve their distinct nonretryable failure. Error text and Rust
only diagnostics use the existing typed normalization, while every remaining
message/event field, classifier, callback stage and body close/drop is checked.
The successful-absent body capability fails immediately if polled; the400 error
path still permits its existing empty-body consumption.

`ResponseBodyPresence` defaults to Present for existing body implementations.
`AbsentResponseBody` explicitly represents a trusted absent body. Policy wrappers
forward the immutable fact and native Fetch marks its existing null-body
method/status suppression as Absent. EOF and close do not change presence. Raw
send and preconstructed selection keep the caller's explicit presence. No header
marker or inference from zero bytes is used, and no Request/Response host union,
new body/resource limit, provider registration or live account access is added.

Run `cargo test -p pi-ai-providers --all-features --test response_body_presence
--locked --offline` (one line). The transport has separate portable and actual
native/Node presence tests in `tests/response_body_presence.rs`. Temporarily
removing only policy presence forwarding causes the unchanged complete-family
test to fail on absent 200 Pi classification; restoring forwarding passes.
