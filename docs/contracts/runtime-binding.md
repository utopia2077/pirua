# Runtime binding and observable effects

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Full-domain effect correlation

`EffectKey` remains an owned, ordered/hashable, non-Serde value with exactly its
existing identity components. Change only these two counter types:

```rust
pub logical_attempt: SafeUnsigned,
pub item_index: Option<SafeUnsigned>,
```

Use the existing pi-ai-core SafeUnsigned, already imported by the schema crate.
Its 0..=9_007_199_254_740_991 domain represents every accepted durable counter;
OneBasedSafeUnsigned attempts retain their own stronger lower bound in state.
Zero remains available for effect kinds with no numbered attempt; the key type
itself does not invent a semantic restriction shared by every effect kind.

No conversion through u32/usize, hashing of counter values, saturation, modulo,
or new retry cap can substitute for exact identity. Conversion from a valid
one-based durable number to the key preserves its value exactly. All other key
fields and pending-only projections stay unchanged. This corrects the narrower
u32 illustration in design04; after acceptance that current target view must link
to this contract and cover the complete domain.

## Effects expose lifecycle rejection separately from in-band output

Retain all eight Effects method names, arguments, explicit BoxFuture lifetimes,
generic telemetry C, object safety, and method inventory. The target outputs are:

| Method | BoxFuture output |
|---|---|
| commit_transition | Result<Option<CurrentOperation>, HarnessRuntimeError> |
| commit_settlement | Result<Option<SettlementResult<C>>, HarnessRuntimeError> |
| commit_terminal | Result<Option<CurrentOperation>, HarnessRuntimeError> |
| run | Result<EffectOutput, HarnessRuntimeError> |
| finalize_tool | Result<ToolSettlementOutput, HarnessRuntimeError> |
| run_summary_request | Result<SummaryRequestOutput, HarnessRuntimeError> |
| settle_summary_request | Result<Option<CurrentOperation>, HarnessRuntimeError> |
| sleep | Result<(), HarnessRuntimeError> |

Use the existing non-Serde, redacted HarnessRuntimeError::{Closed,Fault}; do not
add a new failure taxonomy. Ordinary provider/tool/hook/cancel-deferred failures
still produce their existing in-band outputs. Do not convert close into a provider
error/retry, an absent response, or a synthetic result under running control.
Admitted commit success is a definitive result: a later close cannot rewrite it
as failure. The future concrete Effects owner must shield admitted commit work
and expose close only where admission/start was prevented or an accepted wait is
rejected. The ABI permits that policy; this type unit does not prove its execution.

A successful settlement still has a complete CurrentOperation and optional
follow-up dispatch/suspension. Its outer Ok(None) means no settlement was admitted
against the supplied current ownership/key. It creates no replacement register,
usage row, follow-up dispatch or consumed resume permit. The caller must reload
under the runtime's lane discipline: an extant newer operation state causes
replanning; absent owned op registers with a cleared currentOperationId cause
ordinary external-finalization reconciliation from lane.lastResult. None alone
is not a corruption/fault claim or evidence that the operation completed.
Transition/terminal Option retain their conditional no-commit meaning. The nested
summary settlement receives the same no-current representation for consistency
with a parent operation that can close or be finalized while a nested request runs.

## Observable ABI obligations

- Keys differing only in attempts above u32::MAX remain distinct in equality,
  ordering and map lookup; safe maximum and optional source/request indexes retain
  exact values. Existing invalid durable-number cases remain rejected.
- Dynamic Effects consumers can receive and distinguish Closed and Fault from
  every method, while an ordinary in-band output remains an Ok value. The existing
  redacted error/source-chain behavior and no-Serde boundary remain intact.
- Conditional no-commit, successful settlement and lifecycle failure are separately
  representable; successful SettlementResult still requires its complete current.
- All eight methods remain object safe with opaque generic telemetry; no executor,
  future/handle persistence, production crate, dependency version or internal edge.

Compiler/trait and key-value witnesses establish only this ABI. Actual parked
zero-effect behavior, admission races, bounded restore, exact settlement against
current state and no-tool/tool walking traces remain runtime proof obligations.
Existing tests may adapt construction/signature syntax to the corrected types;
no old expected behavior, fixture, assertion or fixed inventory may be weakened.
