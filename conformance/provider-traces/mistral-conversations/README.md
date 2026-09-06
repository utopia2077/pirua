# Mistral canonical wire evidence

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`, read through Git
objects. Complete `packages/ai/src/api/mistral-conversations.ts` and all four
`packages/ai/test/mistral-{http-transport,raw-stop-reason,reasoning-mode,tool-schema}.test.ts`
were inspected. No provider network calls or credentials were used.

`oracle.mjs` executes the pinned reducer, hash, JSON-repair/partial parser,
sanitizer and cost function under Node v26.8.1, with only the clock fixed to 7.
It produces `trace.json`: mixed reasoning/text/tool fragments, every event's
owned snapshot, first response identity and cached usage. The only event
normalization removes `partialArgs`, a source-private scratch property forbidden
in core durable content. The oracle uses the installed upstream `partial-json`
dependency; it is not part of the Rust build or default verification.

`cache-oracle.mjs` executes the pinned reducer/cost function to produce
`cache.json`. `coreAdmitted` separately labels the existing safe-integer DTO
boundary; cached alias selection and provider clamps run before this admission.
In particular negative, enormous or fractional raw cached counts can normalize
to valid final counts. A fractional final count remains a protocol rejection.

The Rust tests derive request/remapping, replay, tool-schema, reasoning, headers,
EOF, stop mapping and timeout assertions from the named pinned source/tests.
Node `AbortSignal.timeout` probes in the trace record the unsigned-32-bit
admission boundary; actual Node warnings confirm values above signed-32-bit
range schedule 1 ms. Core neutral timeout DTOs already exclude negative and
fractional caller values. Platform identity is deliberately normalized to
`pi-ai-rust`; a trusted host may supply the source OS-derived user-agent snapshot
without adapter ambient discovery.

Safety boundaries: HTTP body/status text are transiently formatted as pinned,
classified before redaction, then discarded; default errors retain control
classification rather than provider text. Unknown raw stop strings remain in
the explicit raw-stop field, not copied into default error text. Count, tool-ID,
object-only argument, UTF-8 string and bounded JSON/stream limits are the
accepted Rust-core boundaries, not claims to admit arbitrary JavaScript values.
Middleware is a trusted Rust capability raced against caller cancellation.
Cancellation snapshots retain source partial content/usage/identity; timeout
produces an error, not an aborted result. Retry options are intentionally ignored.

`HttpTransport::send_fetch` now composes the accepted native Fetch redirect path,
with no retries and the same WholeResponse timeout. A real loopback chain sends
307 with a relative Location, then 302 to a different local origin. The tests
capture retained POST body/auth on the relative hop, GET/empty body/content-header
deletion and auth/cookie removal across origins, while preserving custom headers
and affinity. Incomplete redirect bodies must close without waiting for EOF.
The final fragmented SSE produces the complete existing trace.json snapshots;
separate runs cancel during a redirect-hop wait and during the final SSE body.
Every active socket must report EOF/reset; read timeout is never accepted as
release evidence. One 60-second whole-response timer covers the chain, and only
the final response reaches response middleware. No adapter-local redirect loop,
retry helper, new default activation or backend was introduced.

`error-oracle.mjs` executes the pinned full Mistral error stream and final S03
classifiers with an empty transcript and injected synthetic Responses. Its first
ten rows reproduce the independent status/veto witness exactly; six additional
controls cover ASCII/astral UTF-16 truncation, surviving overflow/billing text,
and simultaneous overflow/retry vetoes. `errors.json` expectations are source
output, never Rust output. Run with Node `--disable-warning=ExperimentalWarning`
and `--check` to re-execute the source comparison offline.

Final HTTP error text now contains only the selected safe semantic category.
Reintroducing status 429/503 after discarding billing/quota text would reverse
the source's retry veto. Status and both independent classification observations
are instead retained in `mistral_http_failure` structured diagnostics. The old
status-preservation test now checks that structured field; the new regression
calls both classifiers on the final AssistantMessage for all 16 rows. Trimming
and 4000-UTF-16-unit truncation still precede classification; raw text is never
restored. Retained 204/205 tests also require response observation, no Start,
no body polling, and exactly one resource close.
