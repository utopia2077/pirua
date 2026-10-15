# Coding session projection oracle

`source.mjs` extracts and executes named functions unchanged from the Pi v1.4.0
SessionManager and Coding messages source at `4a080252f06caf63a525d7873c5d94d3c23887e0`.
It loads no provider, credential or runtime. The fixture records complete source hashes.

```sh
node conformance/coding-session/source.mjs /path/to/pi-checkout --check
cargo test -p pi-coding-session --test upstream_projection --locked
```

The checkout needs TypeScript installed. Omitting `--check` regenerates `trace.json`.
The Rust native entry timestamp is Unix milliseconds; the peer converts only entry
timestamps to/from upstream ISO strings. Message timestamps and all projected values
are preserved. These fixtures cover projection, not every SessionManager method.
Store transactions, branch persistence, fork labels and version rejection have separate
Rust public-API tests. No JSONL backend or historical input migration is implemented.

Upstream source is MIT licensed; attribution is retained in the repository LICENSE.
