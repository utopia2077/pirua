# Summary content source evidence

`source.mjs` loads nine **complete**, byte-identical modules from pinned pi-mono
853a80d26c90a14c1886f0ebb8ffaae133ca2185; their hashes are embedded in `golden.json`.
The module loader supplies only typed dependency adapters: Result/error constructors,
one-shot retry callback, deterministic routing identity/clock, and fake Models that
records actual contexts/options and returns specified responses. Complete text,
conversion, preparation, cut, transcript, prompt, generator and combination bodies
execute unchanged. No real credentials, provider, network, or upstream write.

Run with `env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node
conformance/harness/r8-r10/content/source.mjs --check`. Generation omits `--check`.

Finite oracle inventory: four preparation histories (whole turn, split/prefix-only,
previous compaction/tail/details, tool-result boundary), their four complete generated
compactions, five branch budgets, one generated branch and one empty branch,
and one Unicode/thinking/text/tool-call/truncated-result transcript with token counts.
Four exact sanitizer boundary cases and one full generated compaction prompt retain
pre-sanitization UTF16 length; complete source estimation/clamping records the
90-versus91 context-budget counterexample.
Rust comparisons decode expected durable DTOs so JSON's one numeric domain is not
confused with serde_json's integer/float representation of cost zero.

These fixture tool argument objects use the existing Rust JsonObject canonical map
order before entering the source helper. JavaScript numeric keys still sort as JS
Object.entries/JSON.stringify require. Arbitrary original JavaScript insertion order
cannot be recovered after conversion to the already-frozen Rust BTreeMap DTO. The
initial noncanonical fixture and its distinguishing failed prompt comparison remain
in E/content710/insertion-order-original-{source.mjs,golden.json} and focused-04.log;
this is an explicit representation limit, not an assertion of insertion-order parity.

Separate Rust tests exercise v3 projection: exact committed-row source identity,
logical retained/projector positions, stored assistant filtering without refiltering
projector output, a prospective filtered response boundary, signed threshold
subtraction, timestamp-independent usage estimate, numeric overflow and JS file-list
ordering. These are v3 adaptations, not claims that the old raw-entry helper implements
v3. No obsolete configuration-entry variants are invented.

`summary_content` is pure. Its builders accept owner-converted history/prefix messages,
return prompt text without timestamps, and combine only already-settled receipts.
The owner must stamp each actual nested request, perform all hooks and the three
nested Effects gates, bind exact task/attempt/index/usage identity, and persist usage.
No helper starts a request, retries, changes a register, selects a provider, or claims
public R8–R10 completion.

The additive request specialization uses the actual captured Models registration,
unchanged model identity and a real SimpleStreamOptionsWire max_tokens. It removes
tools, forces deferred=false after effective patches, preserves payload/response
hooks, and suppresses public assistant stream callbacks. Cache isolation is seeded
before the owner's before_request patch; other legal patch values retain precedence.
A summary-only worker wrapper catches provider-worker panic and drops its producer,
letting the existing stream machinery settle its setup error without another request.

UTF16 normalization preserves original valid inputs and preparation. Only a derived
cut-created lone half is removed when materializing Rust scalar prompt text, matching
the selected provider converters' pinned sanitizeSurrogates result and preserving
the original truncation suffix. JavaScript Models callbacks/custom providers can
observe that transient lone half before conversion; the Rust scalar-string boundary
cannot, and arbitrary JavaScript callback equivalence is not claimed. Logical raw
prompt units are retained for request-budget equivalence, not persisted surrogate data.

The logical count binds one synthetic User text and the unchanged system prompt.
For a removed-unit count difference, ordinary/adaptive routes bind the raw-context
clamp. Non-adaptive Anthropic reasoning computes both original clamps and the actual
effective thinking budget, then sends R-B_eff with an explicit active budget bucket
(including zero). Desired template max_tokens stays separate. Thirty-two complete-
source helper composition cases are checked against actual captured Models adapter
inputs; adaptive/off/other/nonreasoning are distinct controls. This proves the local
request seam, not an execution of every selected SDK's final HTTP payload; root owns
the independent native-adapter composition probe. Existing provider helpers and
captured Model are unchanged.
