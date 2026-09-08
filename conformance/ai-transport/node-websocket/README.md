# Persistent Node26 WebSocket profile

Authority: main S04 Persistent Node WebSocket contract; pi-mono Git object
`853a80d26c90a14c1886f0ebb8ffaae133ca2185` Codex WS/cache/parser caller; actual
Node26.8.1 bundled Undici8.10.0, SHA-256
`9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d`.
This is separate from generic WebSocket/tungstenite behavior, npm Undici8.9.0,
and Bun/browser implementations. No live provider or credentials are used.

## Source observations and reproduction

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/node-websocket/oracle.mjs --check
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/node-websocket/inflight-oracle.mjs --check
cargo test -p pi-ai-transport --features native --test node_websocket --lib --locked --offline
```

The46-case `trace.json` comes from the actual global Node WebSocket with local
HTTP upgrade servers. It retains request headers (excluding random key and
ephemeral Host), application text/binary, successful Close code/reason and source
error observations. It covers compressed/uncompressed fragments and controls,
window8–15/08/empty/invalid, persistent dictionary including declared no-context,
UTF8/BOM, EOF/protocol errors, informational upgrade heads, custom header defaults,
Range/Host/cache behavior and offered/quoted protocols. A source Error followed
by abnormal Close1006 is projected to one typed transport failure; successful
Close1005 remains observable. Expected fixtures are never Rust-generated.

Valid compressed sessions wait for the client's Close reply before sending TCP
FIN. This causal handshake matters: asynchronous Node inflater completion can
lose to an independently observed socket close. `early-fin-oracle.mjs` and
`early-fin.json` retain the initial, unmodified immediate-FIN experiment;
`early-fin-observations.json` records three further actual runs. These are source
reachable observations, not a promise of identical unsynchronized scheduler order.
They are **not** claimed fixed by a successful orderly-close corpus.

`inflight-oracle.mjs` executes the complete fingerprinted bundled source with one
dependency seam: the real Node raw inflater runs, but its completed flush callback
is held until an explicit barrier releases it. No fake decompressed output or
sleep chooses the result. Already-admitted plaintext remains before failure;
Closed-before-completion suppresses the later compressed message, while
completion-before-close admits it. Rust's same-owner driver has a private test
gate and real TCP peers proving both orders. There is no extra inflater task,
thread, callback inlet or public timing control. Its production work is bounded
and cooperative; a particular yield schedule is not Node global timing parity.

Isolated negative source-crash witness:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/node-websocket/oracle.mjs --unrequested-protocol
```

This deliberately exits1: bundled connection15608 calls `includes` on null when
a response protocol exists but no request protocol was supplied. Controller
approved typed Protocol + resource release for this invalid source-fatal input.
It is not a fabricated normal Node error/close golden, and does not authorize raw
exception prose or a process crash. A dedicated native test proves the safe
failure. Valid protocol controls still use actual source traces.

## Ownership and observable boundaries

`Node26WebSocketConnector` explicitly implements PersistentWebSocketConnector.
Old generic ports, tests and defaults are unchanged; only their exact TCP/proxy
opening code is extracted for shared use. Existing TLS configuration stays
explicit. Node application events have no Ping/Pong; controls neither enter the
queue nor reset receiver idle timing. Close codes are observations, not permission
to emit reserved codes. All raw-bearing public types are Debug-redacted/non-Serde.

The connection owns one driver task, framing/inflater/IO cursors and private
lifetime abort. It retains a clock, not its first caller cancellation token.
Send admission and receiver epoch share one transition. The driver acknowledges
send-queue acceptance, not remote delivery or write completion. Later write errors
belong to the attached receiver. Dropping uncertain admitted-send wait retires;
dropping only next unregisters its waiter; dropping a receiver discards its epoch
queue without resetting dictionary/framing. Idle messages decode/validate and
advance dictionary but have no application mailbox. Fragments route on completion.
No source Codex cache, continuation, fallback/statistics or timers are implemented.

Already-admitted messages precede terminal observations. State independently
changes when IO observes close/error. Caller cancellation wins queued delivery.
An initial queued event needs no idle timer; Some(Duration::ZERO) disables receiver
idle timeout, matching the Codex parser. In-flight decompression does not publish
after observed closure. Waiters/wakers, commands, acknowledgements, IO, codec and
clock callbacks are invoked/disposed outside the shared-state mutex.

Local shutdown validates Node's1000 or3000–4999 API code domain and at most123
UTF8 reason bytes; peer wire-code validation is distinct. Shutdown flushes its
reserved Close and awaits physical closure under caller cancellation and explicit
Duration grace. Zero expires immediately. Peer Close replies contain code only,
then await actual closure under the configurable local peer grace. Grace expiry
is a typed local resource timeout, not a claimed Node default. Abort, operation
abandonment and runtime loss retire IO/codec/task ownership; no orphan task remains.

## Resource accounting and implementation limits

Limits are construction-owned and explicit; defaults are local safety budgets,
not Node's unbounded dispatcher defaults:

- Handshake input/output:65536 bytes; read chunk8192 (configurable1..65536).
  Borrowed endpoint/proxy/header input is admitted before cloning. Header values
  are checked as Latin1. Informational response heads share the cumulative budget.
  Physical read-ahead is at most one read chunk and retained across upgrade.
- Physical frame and decoded-message limits come independently from the existing
  WebSocketRequest. Parser pending input is at most frame+read chunk+14 bytes;
  in-flight compressed frame adds at most frame+4 tail bytes. Vec capacities and
  temporary decoded text copies can add a constant factor; these are not RSS caps.
- Fragment count4096; one fragmented message at a time. Inflater processing consumes
  at most8192 input and produces at most8192 output bytes per step. The driver
  handles at most32 ordinary frame steps per poll and yields an in-flight step
  before further completion, while polling read/write and commands fairly.
- Application queue128 messages/16MiB; terminal observation has a separate bounded
  slot (peer Close reason≤123 bytes). Exhaustion retires rather than blocking
  physical receive indefinitely. Idle messages do not accumulate a queue.
- Pending application commands2 /16MiB+14 bytes; charge remains until corresponding
  writes flush. Aggregate pending+outbound application bytes also obey the separate
  outbound limit16MiB+14. A single graceful Close is independently reserved.
- Physical control reserve4096 bytes, including a131-byte Close reservation.
  Every encoded control costs at least6 bytes, also bounding control-count
  metadata. Data cannot consume this reserve. Peer close grace defaults5 seconds
  as an explicitly declared local resource bound; caller shutdown grace has no
  implicit/unbounded default.

Header Vec/String and per-command/message metadata is O(admitted field/count
budgets), separately from payload byte counts. The inflater has up to32KiB history
plus fixed decode tables/state/allocator overhead; safe zlib-rs caller-owned output
slices bound growth. No fixed allocator RSS/zeroization claim. Compression history
persists even under server_no_context_takeover, as the actual source does.
Long valid sessions do not fail merely by cumulative length;100 sequential native
messages exceed a four-byte queue budget without accumulating it.

Only direct native-only zlib-rs=0.6.7, defaults off/std, is added to the existing
locked package closure. Its8-bit safe API is needed because flate2's public
configurable inflater admits9–15 only. Existing Fetch flate2 remains unchanged;
no c-allocator/default/debug/internal API feature added. Native dependencies may
contain their already accepted unsafe internals; this crate still forbids unsafe.

## Verification qualifications

Native tests exercise actual TCP upgrade/message/Close, same-packet tail, idle
dictionary and epoch barriers, partial/blocked reads and writes, caller cancellation,
runtime loss, reentrant waker disposal, exact outgoing budget, independent queue
limits, TLS/private trust, explicit proxy and Close grace. Pure codec tests compare
every byte split to source observations and test independent frame/decoded/fragment
budgets. Original generic native tests remain unchanged and pass.

Causal guard check: disable only the driver's EOF/in-flight termination guard;
the real-TCP Closed-first barrier test fails at its two-second completion bound.
Restore it and the same test passes. No mutant remains. Full acceptance is solely
`./scripts/verify.sh`, not these focused commands.

Bun/browser/custom runtime, HTTP2 WebSocket and live Codex/account behavior are
not claimed. No source API is replicated for unused Blob identity/coercion hooks,
raw platform exception prose or upgrade-head delivery. Configured limits and
source-crash normalization are explicit deviations, not silent fidelity reductions.
Provider/cache integration and S05 registration remain later independent units.

## INFO-stage regression evidence

`info-stage-oracle.mjs` executes the fingerprinted installed Node source against
24 loopback cases; `info-stage.json` retains its output. Run with `--check` under
the same sanitized environment as `oracle.mjs`. The eight independent review
cases preserve finished-DEFLATE, HTTP-whitespace and nonminimal-data controls.
The added cases cover all three controls with raw length markers126/127, invalid
opcode/mask/RSV/fragmentation/continuation headers, and valid nonminimal64-bit data.
Invalid header-only cases retain an admitted text prefix and withhold all further
bytes and physical EOF. Native tests require Protocol/Closed and peer release
without a receiver idle timeout; the outer three-second watchdog is test-only.
Codec tests include every split of these source bytes alongside the original634
split observations. Node validates INFO before awaiting extended length or mask;
checking an expanded control length loses both the raw marker and this stage.

## Profile peer teardown scheduling

The source corpus server may attempt its local write-half shutdown after the
native receiver has already retired a source-fatal connection. Test cleanup
accepts only `NotConnected` from that shutdown; every other shutdown error still
propagates. Its read drain must observe EOF or peer `ConnectionReset`, rather than
silently ignoring arbitrary read errors. All46 source request/frame/event/Close
assertions are retained. This corrects a server-test teardown assumption and does
not change the production adapter or accepted provider behavior.

The Linux regression `profile_cleanup_handles_peer_closure_before_local_shutdown`
selects the ordering with channels and a peer `peek`: wire bytes are confirmed
available but left unread, the peer closes, and the server consumes the reset
before local shutdown. Actual Linux shutdown reports `NotConnected` (OS107).
The old strict cleanup fails deterministically; the corrected classifier succeeds,
and a subsequent EOF proves physical release. BrokenPipe, TimedOut,
ConnectionReset and Other shutdown errors remain failures. No sleeps or retries
select this schedule; three-second socket/channel deadlines only bound a failure.
