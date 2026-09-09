# Codex direct request retry mechanism

Transport owns inclusive attempts and original-token cancellation/backoff;
provider failures stay opaque. The provider supplies Terminal, Backoff or
ServerDelay observations after its source status/body/hook classification.
Successful response acquisition leaves this owner permanently, so later body
failure cannot replay a successful request. Existing shared SDK retry is unchanged.

The pinned source oracle covers42 header/date coercions and18 server-cap cases.
It executes the complete Codex module's private helpers through the provider
harness. Number consumes the whole string, including supported radix spelling;
retry-after-ms precedes seconds/date. Missing cap means60000ms and zero disables
only that cap. Node timers independently truncate valid delays and clamp values
outside1..2147483647 to1ms; exponential backoff has no jitter. Date parsing uses
an explicit existing deterministic RetryDateParser capability.

`cargo test -p pi-ai-transport --test codex_retry --locked --offline` runs three
focused tests: source delay/cap controls, attempt/timer choices and cancellation
that drops a noncooperative attempt or backoff. Run `oracle.mjs --check` with
`env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node` to verify retained source data.
No HTTP/provider payload classification, new dependency, live endpoint, ambient
credential or unbounded retry owner is introduced. Counts are case counts within
tests; full repository acceptance remains `./scripts/verify.sh`.
