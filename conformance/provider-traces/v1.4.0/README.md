# Pi v1.4.0 offline provider differential evidence

Authority is tag `v1.4.0`, commit `4a080252f06caf63a525d7873c5d94d3c23887e0`.
The catalog provenance folder retains the exact published AI archive, registry
integrity, and SLSA source association. This folder retains the exact telemetry
1.4.0 archive needed to load that AI release.

`oracle.mjs` loads the complete published release adapter modules with the SDK
versions declared by that release: OpenAI 7.19.0, Anthropic 0.129.0, Google GenAI
2.21.0, TypeBox 1.3.27, and partial-json 0.1.7. `package-lock.json` fixes dependency
URLs and integrity. Install once with `npm ci --prefix
conformance/provider-traces/v1.4.0 --ignore-scripts --no-audit --no-fund`.
Subsequent oracle executions are offline: a fake Fetch peer returns synthetic
SSE/error bodies, and no provider account or paid endpoint is contacted.

Run `node conformance/provider-traces/v1.4.0/oracle.mjs` to reproduce the checked
observations. `--write` regenerates them deliberately. `PIRUA_UPSTREAM_ROOT`
selects the Git checkout containing the exact tag (default: `upstream/pi-mono`
inside this repository); its installed dependencies
are never used. The executable Rust test also reruns this oracle.

The 32 main cases cover all four retained protocols, collapsed and anchored
system changes, ordered prompt sections, tool additions/removals, vision input,
managed thinking, sampling, ChatGPT and Anthropic token request behavior,
Anthropic fallback betas, Responses tool search, and malformed completed tool
arguments. Rust compares logical payloads, physical bodies, protocol/auth
headers, raw parsed provider events, public assistant events, and terminal
messages. A further release execution projects the request expectations of
326 retained historical inputs (50 Responses, 100 Completions, 27 Anthropic,
149 Google); old `addedToolNames` inputs are retired, with anchored transcript
cases replacing that removed contract. Pure helper observations cover ordered
transcript replay, explicit-null client attribution, capped retry delays, and
Google thinking mappings.

Snapshot normalizations are explicit: elapsed duration is measured independently
in Rust, JavaScript Number spelling is compared numerically, and private SDK
content fields `index`, `partialJson`, `partialArgs`, and `streamIndex` are omitted.
During an unfinished tool call, non-object arguments project to an empty object
at the Rust typed boundary; final malformed text produces the exact release's
`argumentsError` for non-object inputs. Raw provider observer events retain
those original JSON values. Native User-Agent runtime details identify Rust;
Google's SDK protocol version marker uses the release version with the Rust
runtime marker. Historical parser snapshots retain their original provenance;
the current request observations are kept separately and never overwrite them.

`oauth-oracle.mjs` executes complete unchanged target-tag Anthropic and ChatGPT
auth source bodies with injected deterministic host capabilities. Its 14 cases
exercise browser/copy-code/ephemeral/manual Anthropic login, refresh, ChatGPT
dynamic registration and refresh, scope/client/state/origin failures, listener
failure, and missing device UUID. `oauth_selected.rs` compares host operations,
request bodies, callback cleanup, and credentials against these observations.
No OAuth network exchange or browser sign-in is performed.


`review-oracle.mjs` runs 22 additional source-backed regressions discovered in
independent review. It reproduces the six original findings and adds Anthropic
historical effort admission/association with empty assistants and pending system
updates, error-tool image grouping and final cache placement, Responses same
model/API/provider and nonempty-output effort eligibility, and bounded preferred
strict fallback controls. All three selected invalid-request 400 limit messages
are covered. Required strict, unrelated errors, a wrong error type, absent
preferred strict, a repeated rejection, and a started stream do not gain another
fallback. Every request rebuild re-invokes the payload callback; logical builds,
physical requests, terminals and fallback diagnostic metadata match the release.
Run `node conformance/provider-traces/v1.4.0/review-oracle.mjs`; the Rust regression
also reexecutes it. Observations are in `review-observations.json`.

The native strict fallback diagnostic retains the exact
`anthropic_strict_tools_fallback` code and `strict_tool_limits_exceeded` reason,
but its error message contains only one bounded recognized limit phrase. Raw SDK
response text remains redacted; a source message with a synthetic sensitive suffix
proves the distinction. This deliberate native diagnostic wording boundary does
not change request fallback eligibility or payload behavior.
