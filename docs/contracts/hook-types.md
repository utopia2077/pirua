# Hook type and error contracts

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

The S14 free `execute_tool_batch` function remains deliberately deferred. C2
owns its non-Serde DTOs, object-safe phase port, and accepted signature only;
adding a fake or panicking function to `pi-agent-loop` is forbidden.

## Required corrections

### Pending-only planner inputs

Create non-Serde owned projections for the exact `effect_pending` fields of
`Generation`, `SummaryGeneration`, and `Deferred`. Each has a fallible
conversion from the complete durable enum and rejects every other status.
`AssistantEffectPlan`, `SummaryEffectPlan`, and `DeferredEffectPlan` accept only
those projections. Tests construct every non-pending source status and prove it
cannot become the corresponding plan input.

### Metadata and codec contracts

Add an open `SessionMetadataRecord` trait exposing `&SessionMetadata`; implement
it for `SessionMetadata`, and require it on `Session::Metadata` and
`SessionRepo::Metadata`. This preserves repository-specific metadata extension
without allowing `()` to erase required fields.

Add `SessionCodecOptions` containing a deterministic custom-role to `JsonSchema`
registry. Built-in roles cannot be registered as custom roles. Later Memory,
JSONL, and SQLite repository constructors must accept these options; C2 defines
no fake repository factory.

### Exact hook patch semantics

Replace whole-object metadata patching with a validated language-neutral
`MetadataPatch { set, delete }`. `set` accepts JSON null as data. `delete` is
sorted/unique and disjoint from `set`. Outer field omission means keep; outer
null means clear all. Provide helpers for JavaScript adapters to map per-key
`undefined` into delete operations.

Structural hook results preserve five meaningful inputs: omitted root continue,
an explicitly present empty `{}` continue object, explicit `decline:false`,
`decline:true`, and a supplied compaction/summary. Empty and false round-trip
distinctly. False may coexist with a supplied result; true may not. Safe
construction and Serde both enforce the rule.

Optional `HookResult` payloads omit the `result` field when absent. Explicit
`result:null` is rejected. `before_resume` has no result field at all.

### Policy-aware hook roots

Add `RawHookEvent` and `RawHookResult` (or equivalent complete raw seeds) with
policy-aware conversion for every nested classified header location:

- `before_request`: raw model plus raw Harness stream options;
- `before_payload`: raw model;
- `before_request` result: raw stream-options patch.

A typed header-bearing hook value serialized and reopened with the same model
and provider-header policies succeeds. Default ordinary decode remains deny.
Hard-denied header names fail at the containing root. There is no unchecked
public constructor.

### Hook pairing and closed outcome helpers

Expose a validated, non-Serde hook effect output wrapper. Its constructor takes
the dispatched hook plan/event and a `HookResult`, verifies equal closed hook
names, and only then permits inclusion in effect/settlement output. A mismatched
pair is not constructible through public fields.

Remove the public generic Serde helpers for compaction/navigation unsuccessful
outcomes and resume wrappers. Replace them with concrete legal public shapes or
otherwise seal them so downstream crates cannot decode or construct crossed
operation/outcome combinations. `ResumeOutcome` retains exactly 18 legal schema
alternatives and the nineteenth summary-present witness.

### Callback lifetimes and errors

Every callback future that borrows an invocation argument declares a shared
`'a` on `&'a self`, each borrowed argument used asynchronously, and
`BoxFuture<'a, _>`. At minimum compile natural implementations that await the
cancellation observer in `AgentContextTransform` and borrow the model in
`ProviderPayloadTransform`.

`RunWhenIdleTask` returns `Result<(), CallbackError>`. The public method uses
this exact single-channel ABI:

```rust
pub type RunWhenIdleCall = Result<(), RunWhenIdleError>;

#[non_exhaustive]
pub enum RunWhenIdleError {
    Callback(CallbackError),
    Runtime(HarnessRuntimeError),
}
```

`CallbackError` retains a redacted shared source. Callback failure does not
fault the Harness and this is not a nested expected-result channel.

### Error causality and map construction

Storage/session/Harness errors retain optional shared error sources and return
them through `std::error::Error::source`. `SessionError::Storage` chains to its
nested storage error. Manual Debug for every wrapper remains category-only or
redacted; source messages never appear in Debug.

Provide validated constructors for raw header patches and response headers.
Response headers reject exact/ASCII-case-insensitive duplicates and the seven
credential header names before becoming the sanitized `ResponseHeaders` type.
String-only/null rejection remains exact. Runtime sanitization drops unsafe
headers or omits the optional map; credential-header rejection must not convert
an otherwise valid provider response into provider failure.

## Required causal evidence

In addition to preserving all 42 passing C2 focused tests, add tests/probes for:

1. every non-pending generation/summary/deferred status rejected by plan input;
2. metadata associated type cannot be `()` and extended metadata can expose its
   base record;
3. built-in custom-role registration rejection;
4. metadata set-null versus delete, overlap/ordering rejection, and clear-all;
5. structural decline omitted/false/true/supplied and true-plus-result rejection;
6. optional hook result omission versus explicit-null rejection;
7. same-policy raw HookEvent/HookResult reopen plus hard/default denial;
8. matched/mismatched hook plan/result construction;
9. downstream inability to name/use generic crossed outcome helpers;
10. callback implementations that borrow cancellation/model compile;
11. run-when-idle callback versus runtime error distinction;
12. source-chain availability plus Debug secret-sentinel absence;
13. response-header constructors reject credential and case-duplicate names;
14. positive search query/hits and all before-resume/request-step/structural-hook
    alternatives;
15. exact source inventories for SessionTree, Session own methods, SessionRepo,
    Search, Effects, AgentLane, Harness, and its factory/properties.
16. compile-fail doctests or equivalent causal trait-negative guards proving
    planner plans/actions/pending projections, matched hook outputs, callback
    ports/tool bindings/options, and callback/runtime error wrappers do not
    implement Serde;
17. the general omission/presence/null matrix for optional event, hook,
    snapshot, resource, scan, and outcome fields retained from revision 20;
18. exact field/method/object-safety inventory for every agent-loop tool-batch
    DTO/port and stream-assistant port, plus a guard that C2 has no fake free
    `execute_tool_batch` implementation while its documented S14 signature is
    unchanged;
19. all eleven sealed `HookSpec::{Event, Result, NAME}` associations checked at
    compile time.
20. exact round trips for omitted structural root result, present `result:{}`,
    false, true, supply, false+supply, and true+supply rejection for both
    structural hooks;
21. independent anti-masking compile-fail guards for both `Serialize` and
    `Deserialize` on every representative non-Serde family, including
    `PendingGeneration`;
22. a source-linked exhaustive match over every `HarnessEvent` arm and closed
    nested selector enum used by the 43/53/45/55 count, so adding a same-tag
    alternative fails even if the inventory JSON is not updated.
