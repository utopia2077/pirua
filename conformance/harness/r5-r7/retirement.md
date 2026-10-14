# Independent retirement boundaries

Exactly2 finite cases, actual public runtime/Models and actual MemoryBacking/Session:

- `FAULT-PROJECTOR-PENDING`: public custom write is pending, provider settles,
  actual projector callback starts once and is held. A second lane's public prompt
  invokes a trusted system prompt returning Fault; its call count and owner admission
  rejection prove the seal. The original accepted call must reject Fault, the projector
  future must be dropped, and no custom placement or later write/request may occur.
  This retains a separate close variant.
- `EXTERNAL-TERMINAL-FINALIZE-GATE`: real tool executes once and returns raw result;
  manual finalize_tool gate parks before after_tool. A legitimate Session transaction
  publishes that raw tool result/usage under its reserved ID, exact old RunAborted result
  and full owned-register cleanup. Validate resulting snapshot/lastResult/absent args,
  then release old finalization. Original call resolves its own durable lastResult,
  with no new provider/tool/hook call, write, Fault or argument recreation.

No fabricated accepted operation or fake backend. External terminal publication is
explicitly the tested authorized Session action, not an asserted public prompt result.
Bounded pump fairness uses positive callback/raw/tool/Fault markers rather than wall-clock
sleep. These two joins do not claim full runtime assembly coverage.
