# Simple options evidence

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`,
`packages/ai/src/api/simple-options.ts:16-52,56-113`.
The six functions and two constants are projected into `api::simple_options`.
`build_base_options` handles the common wire fields; nominal requests retain
runtime, auth, headers, environment and middleware separately. It does not
re-serialize or drop those process-local capabilities.

`oracle.mjs` executes the exact pinned thinking helpers offline. Its immutable
`vectors.json` covers missing versus zero base, the equality branch, answer room,
and an intermediate sum above the safe-integer boundary with a safe final cap.
Run `node conformance/provider-traces/simple-options/oracle.mjs --check`.
Other test expectations are source-confirmed: one four-character user message
estimates one token, so window 10000 reserves 4096 and yields 5903 available.
Wire fields remain exact; model sampling keys precede request overrides.

Counts use the accepted safe-unsigned input domain. Negative/fractional token
counts are already rejected by core admission. Estimation failure is typed.
