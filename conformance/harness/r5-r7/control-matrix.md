# Independent public R6 control matrix695

Initial frozen catalog22 histories in4 functions, extended only by authority698 to23 histories in5 functions:

- phase matrix12: checkpoint-need/manual; checkpoint-may/manual;
  assistant-ready/manual; before-request-waiting/manual;
  before-request-running/automatic; assistant-pending-live/automatic;
  assistant-pending-unknown/manual+actual-reopen; retry-wait/automatic;
  failure-drain/manual; deferred-suspended/manual;
  deferred-pending-live/automatic; deferred-pending-unknown/manual+actual-reopen.
- tools5: planned/manual; unknown-safe/manual+reopen; unknown-never/manual+reopen;
  mixed-completed/manual (first completed, later pending/planned);
  mixed-live/automatic (three calls, reverse raw completion after abort).
- drained-marker1: actual public abort marker and surviving payloads, physical
  MemoryRepo close/open, cancellation-only completion, nextRun retained.
- post-hook4: after-response and after-tool each abort-before-start versus
  started-hook-before-abort, with actual held callbacks and transformed result.

Every prefix is reached by actual public prompt/resume/hooks/tools/provider and
manual release where needed; no operation state or pending payload is forged.
Read-only point/current-state inspection selects reached boundaries. Independent
expectations concern reserved IDs, final tool result order/usage/termination,
no new ordinary start after abort, accepted writes before terminal, and terminal
ownership. Automatic cases need not pretend repeated abort beats concurrently
completed terminal; exact repeat and parked no-side-effect assertions use manual
boundaries. Existing two order checkpoint/provider phases supply response/abort
ordering. No S11 structural execution. Full close/fault/R5/R7/raw-transaction matrices
remain runtime687's scope, not inferred from these cases.

Source/fixture details, empirical reachability corrections, exact executed case IDs,
failed original receipts and final candidate identities are recorded in E/control695.
This catalog states finite bounds, not exhaustive host schedules. Usage event rows and
complete publicly readable state are observations, not raw ordered-write request logs.

Authority698 / plan50 / conventions77 / runtime3 corrects the original derived
termination rule: abort-first keeps raw result with terminate:false; hook-first
keeps the complete finalized value, including an omitted-patch inherited true.
Original failed assertion/source/receipt remain in E/control695; only that new
hook-first assertion is corrected, preserving content/usage/RunAborted expectations.

23rd case `after-tool-gate-abort-first/manual`: execute returns raw terminate:true,
finalize_tool starts while running, then actual after_tool hook gate parks before
callback entry. Public abort wins there: callback count0, raw details/usage retained,
termination false. This distinguishes hook admission from an earlier run_after sample.
