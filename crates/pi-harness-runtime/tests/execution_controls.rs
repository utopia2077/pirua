use pi_agent_schema::{ActionInfo, DriveMode, ErrorContext, HarnessFault, HarnessRuntimeError};
use pi_ai_core::Presence;
use pi_harness_runtime::{
    manual_gate::ManualGate,
    scheduler::{Admission, MutationLine},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn ready<T>(value: Poll<T>) -> T {
    match value {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
fn fault(message: &str) -> HarnessFault {
    HarnessFault::new(ErrorContext::new(message).unwrap())
}
fn action(kind: &str) -> ActionInfo {
    ActionInfo {
        kind: kind.into(),
        description: kind.into(),
        details: Presence::Missing,
    }
}
fn closed<T>(result: Result<T, HarnessRuntimeError>) {
    assert!(matches!(result, Err(HarnessRuntimeError::Closed(_))));
}

#[derive(Default)]
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn admission_close_first_and_line_only_invoke_no_storage() {
    let admission = Admission::new();
    let line = MutationLine::new(admission.clone());
    let permit = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
    admission.seal_close();
    let writes = AtomicUsize::new(0);
    if let Ok(_commit) = admission.try_admit() {
        writes.fetch_add(1, Ordering::SeqCst);
    }
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    closed(admission.try_admit());
    closed(ready(poll(Box::pin(line.acquire()).as_mut())));
    drop(permit);
    assert!(poll(Box::pin(admission.drained()).as_mut()).is_ready());
}

#[test]
fn retained_commit_survives_close_until_outcome_and_publication() {
    let admission = Admission::new();
    let mut drain = Box::pin(admission.drained());
    assert!(
        poll(drain.as_mut()).is_pending(),
        "zero while open is not drained"
    );
    let permit = admission.try_admit().unwrap();
    let finish = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let mut retained = Box::pin({
        let finish = finish.clone();
        let writes = writes.clone();
        let published = published.clone();
        async move {
            // This test owner deliberately retains and drives both future and permit.
            std::future::poll_fn(|_| {
                if finish.load(Ordering::SeqCst) == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            writes.fetch_add(1, Ordering::SeqCst);
            published.store(1, Ordering::SeqCst);
            drop(permit);
            Ok::<_, HarnessRuntimeError>(7)
        }
    });
    assert!(poll(retained.as_mut()).is_pending());
    admission.seal_close();
    assert!(poll(drain.as_mut()).is_pending());
    finish.store(1, Ordering::SeqCst);
    assert_eq!(ready(poll(retained.as_mut())).unwrap(), 7);
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(published.load(Ordering::SeqCst), 1);
    assert!(poll(drain.as_mut()).is_ready());
}

#[test]
fn first_fault_survives_close_and_admitted_failure_promotes_close() {
    for close_first in [false, true] {
        let admission = Admission::new();
        let commit = admission.try_admit().unwrap();
        let line = MutationLine::new(admission.clone());
        let held = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
        let mut queued = Box::pin(line.acquire());
        assert!(poll(queued.as_mut()).is_pending());
        if close_first {
            admission.seal_close();
            closed(admission.check());
        }
        admission.seal_fault(fault("first"));
        admission.seal_close();
        admission.seal_fault(fault("second"));
        for result in [
            admission.check(),
            admission.try_admit().map(drop),
            ready(poll(queued.as_mut())).map(drop),
        ] {
            assert!(
                matches!(result, Err(HarnessRuntimeError::Fault(ref f)) if f.to_string() == "first")
            );
        }
        assert!(poll(Box::pin(admission.drained()).as_mut()).is_pending());
        drop(commit);
        drop(held);
        assert!(poll(Box::pin(admission.drained()).as_mut()).is_ready());
    }
}

#[test]
fn fifo_first_poll_drop_head_middle_and_notified_head() {
    let line = MutationLine::new(Admission::new());
    let mut created_first = Box::pin(line.acquire());
    let held = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
    let mut head = Box::pin(line.acquire());
    let mut middle = Box::pin(line.acquire());
    let mut tail = Box::pin(line.acquire());
    assert!(poll(head.as_mut()).is_pending());
    assert!(poll(middle.as_mut()).is_pending());
    assert!(poll(tail.as_mut()).is_pending());
    assert!(poll(created_first.as_mut()).is_pending());
    assert!(poll(tail.as_mut()).is_pending());
    drop(middle);
    drop(head);
    drop(held); // Tail is notified, but has not consumed its place.
    let mut newcomer = Box::pin(line.acquire());
    assert!(poll(newcomer.as_mut()).is_pending());
    drop(tail); // Ownership must pass to surviving predecessor, not newcomer.
    assert!(poll(newcomer.as_mut()).is_pending());
    let next = ready(poll(created_first.as_mut())).unwrap();
    assert!(poll(newcomer.as_mut()).is_pending());
    drop(next);
    drop(ready(poll(newcomer.as_mut())).unwrap());
    drop(ready(poll(Box::pin(line.acquire()).as_mut())).unwrap());
}

#[test]
fn shared_seal_wakes_independent_lines_and_gate() {
    let admission = Admission::new();
    let left = MutationLine::new(admission.clone());
    let right = MutationLine::new(admission.clone());
    let gate = ManualGate::new(admission.clone(), DriveMode::Manual);
    let left_held = ready(poll(Box::pin(left.acquire()).as_mut())).unwrap();
    let right_held = ready(poll(Box::pin(right.acquire()).as_mut())).unwrap();
    let mut left_wait = Box::pin(left.acquire());
    let mut right_wait = Box::pin(right.acquire());
    let mut park = Box::pin(gate.park(action("park")));
    let count = Arc::new(CountWake::default());
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(left_wait.as_mut().poll(&mut cx).is_pending());
    assert!(right_wait.as_mut().poll(&mut cx).is_pending());
    assert!(park.as_mut().poll(&mut cx).is_pending());
    admission.seal_close();
    assert_eq!(count.0.load(Ordering::SeqCst), 3);
    closed(ready(poll(left_wait.as_mut())));
    closed(ready(poll(right_wait.as_mut())));
    closed(ready(poll(park.as_mut())));
    assert!(gate.peek().is_none());
    assert!(gate.release_next().is_none());
    drop(left_held);
    drop(right_held);
}

#[test]
fn manual_park_peek_release_and_nested_parent_have_zero_early_effects() {
    fn run(mode: DriveMode) -> Vec<String> {
        let gate = ManualGate::new(Admission::new(), mode);
        let trace = Arc::new(Mutex::new(Vec::new()));
        let mut parent = Box::pin({
            let gate = gate.clone();
            let trace = trace.clone();
            async move {
                let _permission = gate.park(action("parent")).await.unwrap();
                trace.lock().unwrap().push("parent-start".into());
                let _child_permission = gate.park(action("child")).await.unwrap();
                trace.lock().unwrap().push("child".into());
                trace.lock().unwrap().push("parent-end".into());
            }
        });
        if mode == DriveMode::Manual {
            assert!(poll(parent.as_mut()).is_pending());
            assert!(poll(parent.as_mut()).is_pending());
            assert!(trace.lock().unwrap().is_empty());
            for _ in 0..3 {
                assert_eq!(gate.peek().unwrap().kind, "parent");
            }
            assert_eq!(gate.release_next().unwrap().kind, "parent");
            assert!(gate.release_next().is_none());
            assert!(poll(parent.as_mut()).is_pending());
            assert_eq!(*trace.lock().unwrap(), ["parent-start"]);
            assert_eq!(gate.peek().unwrap().kind, "child");
            assert_eq!(gate.release_next().unwrap().kind, "child");
        }
        assert!(poll(parent.as_mut()).is_ready());
        assert!(gate.peek().is_none());
        trace.lock().unwrap().clone()
    }
    assert_eq!(run(DriveMode::Manual), run(DriveMode::Automatic));
}

#[test]
fn gate_close_before_release_and_before_consumption_starts_no_consumer() {
    for release in [false, true] {
        let admission = Admission::new();
        let gate = ManualGate::new(admission.clone(), DriveMode::Manual);
        let writes = AtomicUsize::new(0);
        let mut consumer = Box::pin(async {
            let _permission = gate.park(action("effect")).await?;
            writes.fetch_add(1, Ordering::SeqCst);
            Ok::<_, HarnessRuntimeError>(())
        });
        assert!(poll(consumer.as_mut()).is_pending());
        if release {
            assert!(gate.release_next().is_some());
        }
        admission.seal_close();
        closed(ready(poll(consumer.as_mut())));
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        assert!(gate.peek().is_none());
    }
    for mode in [DriveMode::Automatic, DriveMode::Manual] {
        let admission = Admission::new();
        let gate = ManualGate::new(admission.clone(), mode);
        let mut park = Box::pin(gate.park(action("consumed")));
        if mode == DriveMode::Manual {
            assert!(poll(park.as_mut()).is_pending());
            gate.release_next();
        }
        let _permission = ready(poll(park.as_mut())).unwrap();
        admission.seal_close();
        closed(admission.try_admit());
        closed(ready(poll(Box::pin(gate.park(action("future"))).as_mut())));
    }
}

#[test]
fn unpolled_and_dropped_gate_waiters_retain_no_queue_or_waker() {
    let gate = ManualGate::new(Admission::new(), DriveMode::Manual);
    drop(gate.park(action("unpolled")));
    assert!(gate.peek().is_none());
    let count = Arc::new(CountWake::default());
    let weak = Arc::downgrade(&count);
    let waker = Waker::from(count);
    let mut cx = Context::from_waker(&waker);
    let mut first = Box::pin(gate.park(action("first")));
    let mut second = Box::pin(gate.park(action("second")));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    drop(first);
    assert_eq!(gate.peek().unwrap().kind, "second");
    drop(second);
    drop(waker);
    assert!(weak.upgrade().is_none());
    assert!(gate.peek().is_none());
}

struct ReenterDrop {
    line: MutationLine,
    gate: ManualGate,
    drops: Arc<AtomicUsize>,
}
#[allow(clippy::manual_noop_waker)] // The destructor is the reentry stimulus.
impl Wake for ReenterDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for ReenterDrop {
    fn drop(&mut self) {
        let mut acquire = Box::pin(self.line.acquire());
        let _ = poll(acquire.as_mut());
        let _ = self.gate.peek();
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn replacing_and_removing_wakers_runs_destructors_outside_control_locks() {
    let admission = Admission::new();
    let line = MutationLine::new(admission.clone());
    let gate = ManualGate::new(admission, DriveMode::Manual);
    let held = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    for replace in [false, true] {
        let waker = Waker::from(Arc::new(ReenterDrop {
            line: line.clone(),
            gate: gate.clone(),
            drops: drops.clone(),
        }));
        let mut acquire = Box::pin(line.acquire());
        assert!(
            acquire
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        if replace {
            assert!(poll(acquire.as_mut()).is_pending());
        }
        drop(acquire);
        let waker = Waker::from(Arc::new(ReenterDrop {
            line: line.clone(),
            gate: gate.clone(),
            drops: drops.clone(),
        }));
        let mut park = Box::pin(gate.park(action("reenter")));
        assert!(
            park.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        if replace {
            assert!(poll(park.as_mut()).is_pending());
        }
        drop(park);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    drop(held);
}

struct BarrierWake {
    entered: Arc<Barrier>,
    leave: Arc<Barrier>,
}
impl Wake for BarrierWake {
    fn wake(self: Arc<Self>) {
        self.entered.wait();
        self.leave.wait();
    }
}

#[test]
fn cross_thread_handoff_allows_poll_and_cancel_during_wake_callback() {
    let line = MutationLine::new(Admission::new());
    let held = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let leave = Arc::new(Barrier::new(2));
    let waker = Waker::from(Arc::new(BarrierWake {
        entered: entered.clone(),
        leave: leave.clone(),
    }));
    let mut head = Box::pin(line.acquire());
    assert!(
        head.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut tail = Box::pin(line.acquire());
    assert!(poll(tail.as_mut()).is_pending());
    let worker = std::thread::spawn(move || drop(held));
    entered.wait();
    assert!(poll(tail.as_mut()).is_pending());
    drop(head); // Cancel the notified-but-unpolled owner during its callback.
    let permit = ready(poll(tail.as_mut())).unwrap();
    leave.wait();
    worker.join().unwrap();
    drop(permit);
}

#[test]
fn cross_thread_seal_wake_can_repoll_without_lost_notification() {
    let admission = Admission::new();
    let gate = ManualGate::new(admission.clone(), DriveMode::Manual);
    let entered = Arc::new(Barrier::new(2));
    let leave = Arc::new(Barrier::new(2));
    let waker = Waker::from(Arc::new(BarrierWake {
        entered: entered.clone(),
        leave: leave.clone(),
    }));
    let mut park = Box::pin(gate.park(action("close")));
    assert!(
        park.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let worker = std::thread::spawn(move || admission.seal_close());
    entered.wait();
    closed(ready(poll(park.as_mut())));
    leave.wait();
    worker.join().unwrap();
}

struct SealOnDrop(Admission);
#[allow(clippy::manual_noop_waker)] // The destructor seals during registration replacement.
impl Wake for SealOnDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for SealOnDrop {
    fn drop(&mut self) {
        self.0.seal_close();
    }
}

#[test]
fn seal_during_signal_waker_replacement_cannot_grant_line_or_gate() {
    // After notification only the cancellation registration owns this waker.
    // Replacing it seals from its destructor, inside the next poll before the
    // queue transition. This deterministically exercises reentry in that window.
    for manual in [false, true] {
        let admission = Admission::new();
        let waker = Waker::from(Arc::new(SealOnDrop(admission.clone())));
        if manual {
            let gate = ManualGate::new(admission.clone(), DriveMode::Manual);
            let mut waiter = Box::pin(gate.park(action("seal-in-poll")));
            assert!(
                waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            gate.release_next();
            closed(ready(poll(waiter.as_mut())));
        } else {
            let line = MutationLine::new(admission.clone());
            let held = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
            let mut waiter = Box::pin(line.acquire());
            assert!(
                waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            drop(held);
            closed(ready(poll(waiter.as_mut())));
        }
        closed(admission.check());
    }
}

#[test]
fn concurrent_gate_release_is_one_use_and_fault_rejects_unconsumed_permission() {
    let admission = Admission::new();
    let gate = ManualGate::new(admission.clone(), DriveMode::Manual);
    let mut park = Box::pin(gate.park(action("once")));
    assert!(poll(park.as_mut()).is_pending());
    let start = Arc::new(Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let gate = gate.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                gate.release_next().is_some()
            })
        })
        .collect();
    start.wait();
    let released = workers
        .into_iter()
        .map(|worker| usize::from(worker.join().unwrap()))
        .sum::<usize>();
    assert_eq!(released, 1);
    admission.seal_fault(fault("gate-fault"));
    assert!(
        matches!(ready(poll(park.as_mut())), Err(HarnessRuntimeError::Fault(ref f)) if f.to_string() == "gate-fault")
    );
    assert!(gate.peek().is_none());
}

#[test]
fn drain_and_line_notification_use_latest_waker_and_last_commit() {
    let admission = Admission::new();
    let first = admission.try_admit().unwrap();
    let second = admission.try_admit().unwrap();
    let old = Arc::new(CountWake::default());
    let latest = Arc::new(CountWake::default());
    let old_waker = Waker::from(old.clone());
    let latest_waker = Waker::from(latest.clone());
    let mut drain = Box::pin(admission.drained());
    assert!(
        drain
            .as_mut()
            .poll(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        drain
            .as_mut()
            .poll(&mut Context::from_waker(&latest_waker))
            .is_pending()
    );
    admission.seal_close();
    drop(first);
    assert_eq!(latest.0.load(Ordering::SeqCst), 0);
    assert!(poll(drain.as_mut()).is_pending());
    assert!(
        drain
            .as_mut()
            .poll(&mut Context::from_waker(&latest_waker))
            .is_pending()
    );
    drop(second);
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(latest.0.load(Ordering::SeqCst), 1);
    assert!(poll(drain.as_mut()).is_ready());
    let line = MutationLine::new(Admission::new());
    let held = ready(poll(Box::pin(line.acquire()).as_mut())).unwrap();
    let mut queued = Box::pin(line.acquire());
    assert!(
        queued
            .as_mut()
            .poll(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        queued
            .as_mut()
            .poll(&mut Context::from_waker(&latest_waker))
            .is_pending()
    );
    drop(held);
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(latest.0.load(Ordering::SeqCst), 2);
    drop(ready(poll(queued.as_mut())).unwrap());
}
