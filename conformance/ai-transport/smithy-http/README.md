# Smithy native HTTP and provider response records

Authority: pi-mono853a80d26c90a14c1886f0ebb8ffaae133ca2185 Bedrock's
toProviderResponse/custom-header/handler selection, installed Smithy
node-http-handler4.7.3, Node26.8.1, h2=0.4.19 and http=1.5.0. The new operation
is one explicit native H1/H2 attempt, not Fetch or a negotiated fallback.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/smithy-http/oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/smithy-http/goaway-oracle.mjs --check
cargo test -p pi-ai-transport --features native --test smithy_http --lib --locked --offline
cargo test -p pi-ai-providers --test smithy_response_record --test runtime_response_head --locked --offline
```

The source oracles use actual installed handlers and localhost peers; the record
projection executes the exact pinned helper. Expected records never originate
in the Rust implementation. Raw field order and eighty additional names exercise
growth; Cookie, Set-Cookie, Latin1/NBSP and numeric :status retain source types and
own-key order. Source rejects LF/folded H1 fields; the native profile rejects them
before head publication. H2 SP/HTAB edge omission additionally follows the
accepted Node H2 admission evidence in native-fetch-headers; NBSP remains data.

## Explicit request policy

SmithyProtocol selects forced H1 or H2 prior knowledge/TLS without H1 fallback.
H2 authority and optional scheme are explicit profile data, never physical colon
headers. The caller first settles its ordered SDK header record and signs it;
ordinary fields then use H1 case-insensitive replacement or H2 admission/joining.
Strict physical HttpHeaders stays unchanged. Unsupported injected clients fail
with UnsupportedProfile instead of falling back to generic send.

SmithyBodyAdmission is separately derived from the source's exact Expect/expect
lookup before lowercasing. Immediate covers mixed spelling and source external
agents; WaitForContinue uses the source fixed6000ms grace,100,final head or error.
The grace runs concurrently with connection acquisition, not six fresh seconds
after TCP/TLS. It is not an implicit neutral timeout. SmithyH1Connection separately
selects the default Connection value, since source uses nullish lookup for its
agent choice but truthy lookup for body admission. Explicit physical Connection
still wins. No connection pool or agent handle is added.

The transport wrapper races original cancellation before even invoking the port,
checks it again after completion, and owns body close/drop. There are no retries,
redirects, Fetch defaults, Accept additions, content decoding or request timeout.
Native tests retain the original generic/Fetch assertions and test no H1 successor,
forced H1 TLS, actual request bodies,100/final/grace gates, cancellation/drop and
runtime loss. Platform/TLS implementation identity is not a Node fingerprint claim.

## Records and limits

ResponseHeaderRecord is an ordered non-Serde, Debug-redacted record, not a header
bag. Exact replacement preserves its first slot; array-index keys enumerate first.
Text and finite Number are distinct; get_str never coerces numeric :status.
from_http retains accepted Fetch observations. from_smithy applies Node's separate
singleton sets and grouping, bare-comma Set-Cookie and H2-only numeric :status.
Both omit the source plain object's non-own string __proto__ assignment.
Existing callback tests change accessors only, preserving expected values/order.

Checked record construction and Smithy projection are bounded to8192 own entries
and1MiB UTF-8 logical key/value data; numbers charge8 bytes. The existing Fetch
from_http projection inherits already admitted trusted HttpHeaders without adding
aggregate rejection. Its private insertion path retains exact byte accounting.
Public edits to a larger inherited record may preserve or reduce current excess,
but cannot increase it; after shrinking within a limit, the ordinary cap applies.
Clones retain accounting and failed changes are atomic. These are explicit local
limits on new records, not upstream unlimited-object equivalence.

`fetch-record-admission-oracle.mjs --check` executes pinned headersToRecord on
actual constructed Headers, preserving successful1MiB value and8193-entry cases.
The native physical head limits do not constrain this trusted preconstructed
response-port domain. Tests retain Fetch success, source order/strings and clone
isolation while separately requiring Smithy/check-construction rejection.

Native H1 retains the existing cumulative head-byte cap including informational
heads. Smithy H2 uses that head limit for both compressed header-envelope bytes and
cumulative decoded acquisition headers, plus at most128 stream1 header blocks. All9 frame
header bytes and empty CONTINUATION frames are charged. h2 independently caps each
decoded block before constructing its HeaderMap. Envelope observation uses fixed
9-byte state, never HPACK decoding or header interpretation. Queued initial1xx
working storage is bounded proportionally to128 times the configured decoded
block limit, including HeaderMap/String bookkeeping, not an exact RSS bound.
At the default65536-byte cap, this can exceed the logical wire cap; no fixed-RSS
claim is made. h2's flow-controlled DATA buffering and16KiB upload chunks remain
separate. A second lifetime budget in that same IO owner bounds unconsumed
PUSH_PROMISE and non-stream1 HEADERS to the configured head-limit wire bytes and
128 initiating blocks, before and after final-head acceptance. Their CONTINUATION
frames include all9 envelope bytes, even empty continuations; promise continuations
keep this budget although their parent stream id is1. Request-stream acquisition
accounting ends at final-head acceptance and never resets the lifetime budget.
Normal DATA, SETTINGS and PING do not consume either metadata budget. h2 still
validates framing/HPACK and applies its decoded per-block cap. These two bounded
classes can retain proportional metadata up to256 times the configured decoded
block limit plus allocator/bookkeeping overhead, not an exact memory-size promise.
The locked h2 source queues promises at proto/streams/streams.rs:912–918 and clears
them when the parent reference count reaches zero at1670–1682. Emitting GOAWAY
alone does not update h2's internal close state or release this retained queue.
Exceeding either budget closes the IO; body reads report typed LimitExceeded and
retire response/driver/upload owners. A live outer response retains no body after
a failed read. No application push-stream receiver is exposed.

Replayable request bytes are already owned; native preparation can hold
proportional transient copies rather than a second unbounded upload queue.

## Head-time GOAWAY and ownership

Actual NodeHttp2Handler closes its isolated session at the response head. Node's
session.close calls goaway(NO_ERROR,0), then allows the active response body to
finish. The retained source peer waits for GOAWAY before sending body data.
The native same-peer test waits for the control before polling the body, proving
the idle IO driver is woken independently of consumer demand.

h2's client API does not expose graceful shutdown. The approved Smithy-only IO
owner therefore has one fixed17-byte GOAWAY pending control. It tracks the24-byte
client preface and outbound frame envelopes, finishes already offered caller
bytes, and starts control only at a complete frame boundary on read/flush. A
partial control finishes before later caller bytes; control bytes never count
as consumed h2 input. Short writes/flush backpressure cannot block inbound reads.
No new background task, HPACK decoder or arbitrary control queue is added.
The scope is this isolated single request with no application-processed server
streams, not a general H2 connection API. Cancellation/drop/runtime loss may
retire unflushed control bytes and release the sole IO/driver/body owners.

Control tests cover every preface/header/DATA split, offered-byte ordering,
partial17-byte writes, blocked flush, concurrent read progress, idle wake and
drop. Bounds tests cover compressed repeated1xx,129 blocks and exact-limit empty
CONTINUATION envelopes. Additional native peers wait for GOAWAY, then prove valid
promises at247 bytes (13 promises) succeed and266 bytes fail, and128 blocks succeed
while129 fail at65536 bytes. Successful bodies deliver32768 DATA bytes with a247-byte
metadata budget; failures prove socket release while the outer response stays alive.
Private IO tests cross every input split and final-head boundary, retain promise
versus pushed-response continuation ownership, and exclude subsequent request
trailers, DATA, SETTINGS and PING from the lifetime charge. The pre-fix native
regression fails because the over-budget body read succeeds; the fixed test passes.
Causal mutants that add Accept, change cookie joining or
omit head-time GOAWAY fail the retained source/native assertions and are restored.

Bedrock auth/discovery, signing, SDK retry and binary event interpretation are
separate consumers. No live provider or credential discovery is exercised.
The only full-repository acceptance entry is ./scripts/verify.sh.
