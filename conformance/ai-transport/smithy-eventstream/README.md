# Bounded Smithy binary event stream

`source.mjs` executes installed Smithy core 3.24.3. It copies every message into
its exact owned Uint8Array before decoding, avoiding unrelated pooled Buffer
backing memory. Seventeen binary cases cover all ten header tags, CRC failures,
duplicate replacement, non-own `__proto__`, UTF-8 replacement and declared header
section versus complete-message extent. Five EOF cases and 212 splits of the
principal message are source assertions; Rust tests run every split of every
case.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-transport/smithy-eventstream/source.mjs
cargo test -p pi-ai-transport --features native --test smithy_eventstream --locked --offline
```

The decoder owns at most its configured intake buffer. Default bounds are 2 MiB
buffer, 1 MiB message, 65,536 declared/logical header bytes and 1,024 wire header
entries; duplicates and discarded names count. It returns one message on demand
and retains no decoded-message queue. A failed or explicitly cleared decoder
releases its input and closes intake. Test bounds are reduced to exact adjacent
values, with a 10,000-message control proving there is no cumulative DATA charge.

Iteration/payload splitting follow the declared header length. Source typed
views for string/binary/long/timestamp/UUID may reach into payload and CRC bytes
within the same admitted message. Primitive byte/short/integer reads remain
bounded by the declared header section. Values beyond the owned message fail;
no out-of-message access or allocation is reproduced. Long/timestamp/UUID retain
wire bytes; callers perform their own typed interpretation. Duplicate replacement
retains the first own slot and numeric keys enumerate first.

One to three trailing length-prefix bytes at EOF are ignored, matching the
source framer. Four or more bytes of an incomplete message fail. The provider
independently requires an assistant terminal state. No provider event names,
credential handling, compression, native I/O policy or cumulative response-body
collection are part of this helper. CRC32 is an integrity checksum, not custom
cryptography. Full acceptance remains `./scripts/verify.sh`.
