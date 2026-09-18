# Independent structural controls model

This target has5 Rust test functions and13 finite public histories. It complements
711 basic source/validation/threshold histories; no exhaustive scheduling claim.

| Function | Histories | Distinguishing outcome |
|---|---:|---|
| nested_crash_prefixes |7| Compaction before-intent/before-dispatch/before-usage below cap; dispatch/usage atcap; paid-between cap; navigation dispatch cap |
| abort_nested_orders |2| Explicit NotStarted has no usage; already-returned response settles actual11 once after abort |
| paid_split_request_model_loss |1| Actual paid request0 causes missing-model lookup; owning Run suspension has no write; restored catalog starts whole attempt |
| post_intent_adapter_loss |1| Real captured dispatcher missing adapter is failed settlement with reserved zero usage, never suspension |
| terminal_preparation_stale |2| Hook compaction/navigation preserve full actual row metadata after legitimate fact-write drift, offsets2/3 after drift seq |

All histories use actual RuntimeFactory/public lanes, ModelsRegistry/provider, real
MemorySessionRepo and manual Effects gates. Crash histories physically close/open only
after reaching the specified actual committed prefix. Gate names commit_transition,
run_summary_request and settle_summary_request follow the controller-frozen existing
Effects inventory; tests ALSO match structural status/attempt/index/usage IDs and
provider start/response counters. No arbitrary durable operation is inserted.

The missing-model fixture implements public Provider.models(), reading actual Memory
SessionStats; it returns an empty catalog only after the first nested usage has really
committed. It retains request0's captured registration and does not execute or pretend
to execute delete_provider. Its first missing-catalog observation snapshots public
entries/registers/stats; the returned suspension must leave that snapshot unchanged.
The Run is admitted through legal public PromptInput::Messages including an assistant
message to establish a split turn, not through forged operation metadata.

Unknown below-cap reservations remain absent; cap synthetic zero uses only an actual
outstanding usage ID; paid-between cap invents no extra row. Usage observations combine
actual events and maintained stats, not direct ledger scans. New model visibility leads
to whole-attempt retry, never lost partial request2. Queued nextRun survives control.

The stale-terminal fixture advances injected clock and makes a legitimate unrelated
Session fact write while commit_terminal is parked. The returned full entry equals
stored and event rows, has new timestamp, and sequence counts optional hook usage and
navigation target movement. Hook callbacks do not rerun; structural hook usage omits
future entryId. This is exact outcome/ordinal evidence, not a raw ConditionalCommit log.

Initial compiling public-stub failures are retained before frozen candidate import.
No source/old assertion changes are allowed merely to make these histories green.
