# Registry child-owned cancellation

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
`oracle.mjs` executes complete pinned `models.ts` (including createProvider and
Models stream/simple/deferred dispatch), auth resolution, abort utilities,
throwable diagnostics, lazy forwarding and event-stream bodies. Only imports,
re-export syntax and TypeScript types are erased. All six complete source SHA256
fingerprints are frozen in `trace.json`. No SDK is involved in this boundary.
Injected credential/model stores, auth context, synthetic key/model, clock and
child streams prevent host discovery or network activity. Authentication and
registry dispatch execute their actual source bodies rather than replacements.

Reproduce the exact source observations:

```
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/ai-compat/registry-cancellation/oracle.mjs --check
```

Raw, simple and deferred operations preserve child content and usage17 both when
cancellation follows handoff and when the child cancels the original signal
inside its returning call. The progress event still carries usage0. The complete
source noncooperative-auth witness enters `credentials.read` exactly once with
the original signal, stays unsettled before abort, and then settles error/zero
without child dispatch. Its error message must equal the explicit synthetic
abort reason, so a missing-method TypeError cannot satisfy this observation.
The trace records read count and settlement state before and after abort.

`ModelsRegistry::new_with_child_cancellation(options)` opts into the child
terminal after synchronous adapter handoff. The normal `new` constructor and
all options literals retain accepted S03 cancellation (empty aborted/zero).
The new registry's outer uses a private settlement token, while the original
request token is passed unchanged to the trusted child. Setup still observes
original cancellation and cancels the private fallback. After handoff the
existing pump drains the canonical child; it cannot reconstruct late usage from
progress snapshots. No second result or terminal owner is added.

Inherited normalization: setup cancellation remains Rust aborted/zero, whereas
pinned lazy rejection is source error/zero. Controller explicitly retained that
existing safe fallback; this change concerns post-handoff content/usage only.
Child error text uses the existing fixed Rust cancellation message, not source
JavaScript exception text. Canonical validated children and synchronous bounded
trusted adapter calls remain the admitted domain. Rust does not claim parity
for arbitrary iterables, missing terminals, synchronous throws or executor loss.
The trusted child must observe its supplied original token and eventually settle;
no new timeout or forced post-handoff settlement is introduced.

The source proof's negative controls run the same assertions with
`--control=wrong-method` (deliberately replacing read with the original erroneous
get fixture) and `--control=no-setup-abort`. Both must exit1 at their designated
assertions: missing read entry and missing abort-driven settlement respectively.
The latter remains pending through the bounded microtask drain; it is rejected
before awaiting an unresolvable result. An owning Rust test requires both exact
failure diagnostics, so either unrelated failure or silent success is rejected.

The owning `registry_child_cancellation` core tests execute the source program in
a cleared environment, compare Rust against the six source cases, discriminate
new usage17 from default usage0, check terminal/result equality and uniqueness,
noncooperative auth cleanup, pre-poll cancellation, rejected/dropped dispatch,
unsupported fetch and consumer drop. The task executor is deterministic and
uses no sleeps. Consumer drop does not cancel the independently owned dispatch;
original cancellation permits it to drain and exit. Host task drop retains the
existing last-producer setup error behavior.

Run `cargo test -p pi-ai-core --test registry_child_cancellation --offline` for
focused checks. Only `./scripts/verify.sh` provides full repository acceptance.
Selected builtin facade opt-in and full product composition remain later units.
