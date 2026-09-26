//! A test-only finite physical-host witness. It has no live Route or pump.

use std::array;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use pi_extension_protocol::wire::framing::EncodedFrame;

use super::critical_port::{OwnedRoute, OwnedWriter};
use super::transport::{BoundedWriterChannel, ChannelError, ExactWriterClock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JobId {
    route_slot: usize,
    route_generation: u64,
    sequence: u64,
}

struct JobCredit {
    slot: usize,
    id: JobId,
}

struct WriteJob {
    credit: JobCredit,
    frame: EncodedFrame,
    original_deadline: u64,
}

enum JobCell {
    Free,
    Queued(WriteJob),
    InFlight(JobId),
    InCleanup(JobId),
}

struct FixedJobs {
    cells: [JobCell; 2],
    charged_capacity: [usize; 2],
    next_sequence: u64,
    route_slot: usize,
    generation: u64,
    sealed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteOutcome {
    Complete,
    TimedOutBeforeStart,
    TimedOut,
    Channel(ChannelError),
    ClockSeatUnavailable,
    Reaped,
}

struct RetiredJob {
    credit: JobCredit,
    outcome: WriteOutcome,
    never_started: Option<EncodedFrame>,
}

trait CreditOwner: Send + Sync {
    fn release_credit_after_drop(&self, credit: JobCredit);
}

struct CoreCleanup {
    disposal: [Option<RetiredJob>; 2],
    // This keeps the exact generation's ledger and OwnedRoute alive until
    // every retired frame and credit has been disposed of.
    owner: Arc<dyn CreditOwner>,
}

impl CoreCleanup {
    fn new(owner: Arc<dyn CreditOwner>) -> Self {
        Self {
            disposal: array::from_fn(|_| None),
            owner,
        }
    }
}

impl Drop for CoreCleanup {
    fn drop(&mut self) {
        for retired in &mut self.disposal {
            if let Some(retired) = retired.take() {
                drop(retired.never_started);
                self.owner.release_credit_after_drop(retired.credit);
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum QueueError {
    Full,
    TooLarge,
    Stopped,
    SequenceExhausted,
}

// The private key handoff is provided only by the concrete scripted clock.
// Its actual future still implements the repository's ExactWriterClock GAT.
trait KeyedWriterClock: ExactWriterClock {
    fn prepare_key(&self, key: JobId);
}

struct FiniteAdapter<C, K> {
    channel: Arc<C>,
    clock: Arc<K>,
    route: OwnedRoute,
    writer: OwnedWriter,
}

impl<C, K> FiniteAdapter<C, K>
where
    C: BoundedWriterChannel + Send + Sync + 'static,
    K: KeyedWriterClock + Send + Sync + 'static,
    for<'a> C::Write<'a>: Send,
    for<'a> K::WriterSleep<'a>: Send,
{
    fn run(
        self: Arc<Self>,
        id: JobId,
        frame: EncodedFrame,
        deadline: u64,
    ) -> Pin<Box<dyn Future<Output = WriteOutcome> + Send + 'static>> {
        Box::pin(async move {
            if self.clock.now_ms() >= deadline {
                return WriteOutcome::TimedOutBeforeStart;
            }
            let Ok(seat) = self.route.writer_clock_seat() else {
                return WriteOutcome::ClockSeatUnavailable;
            };
            self.clock.prepare_key(id);
            let mut sleep = Box::pin(self.clock.writer_sleep_until(seat.into_raw(), deadline));
            let mut write = Box::pin(self.channel.write_frame_bounded(frame));
            enum Race {
                Timer,
                Write(Result<(), ChannelError>),
            }
            let race = poll_fn(|cx| {
                if sleep.as_mut().poll(cx).is_ready() {
                    Poll::Ready(Race::Timer)
                } else {
                    write.as_mut().poll(cx).map(Race::Write)
                }
            })
            .await;
            let outcome = match race {
                Race::Timer => WriteOutcome::TimedOut,
                Race::Write(_) if self.clock.now_ms() >= deadline => WriteOutcome::TimedOut,
                Race::Write(Ok(())) => WriteOutcome::Complete,
                Race::Write(Err(error)) => WriteOutcome::Channel(error),
            };
            drop(sleep);
            drop(write);
            outcome
        })
    }
}

struct CoreControl<C, K> {
    adapter: Arc<FiniteAdapter<C, K>>,
    jobs: Mutex<FixedJobs>,
    reap_pending: AtomicBool,
}

impl<C, K> CoreControl<C, K>
where
    C: BoundedWriterChannel + Send + Sync + 'static,
    K: KeyedWriterClock + Send + Sync + 'static,
    for<'a> C::Write<'a>: Send,
    for<'a> K::WriterSleep<'a>: Send,
{
    fn new(adapter: Arc<FiniteAdapter<C, K>>) -> Arc<Self> {
        Arc::new(Self {
            jobs: Mutex::new(FixedJobs {
                cells: array::from_fn(|_| JobCell::Free),
                charged_capacity: [0; 2],
                next_sequence: 0,
                route_slot: adapter.route.id().slot(),
                generation: adapter.route.id().generation(),
                sealed: false,
            }),
            adapter,
            reap_pending: AtomicBool::new(false),
        })
    }

    fn try_enqueue(
        &self,
        frame: EncodedFrame,
        original_deadline: u64,
    ) -> Result<JobId, (QueueError, EncodedFrame)> {
        if frame.bytes.capacity() > 64 {
            return Err((QueueError::TooLarge, frame));
        }
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.sealed || self.reap_pending.load(Ordering::Acquire) {
            return Err((QueueError::Stopped, frame));
        }
        let Some(slot) = jobs
            .cells
            .iter()
            .position(|cell| matches!(cell, JobCell::Free))
        else {
            return Err((QueueError::Full, frame));
        };
        let Some(sequence) = jobs.next_sequence.checked_add(1) else {
            return Err((QueueError::SequenceExhausted, frame));
        };
        jobs.next_sequence = sequence;
        let id = JobId {
            route_slot: jobs.route_slot,
            route_generation: jobs.generation,
            sequence,
        };
        jobs.charged_capacity[slot] = frame.bytes.capacity();
        jobs.cells[slot] = JobCell::Queued(WriteJob {
            credit: JobCredit { slot, id },
            frame,
            original_deadline,
        });
        drop(jobs);
        self.adapter.writer.signal_writer();
        Ok(id)
    }

    fn request_reap(&self) {
        self.reap_pending.store(true, Ordering::Release);
        self.adapter.writer.signal_writer();
    }

    fn charged_capacity(&self) -> usize {
        self.jobs.lock().unwrap().charged_capacity.iter().sum()
    }

    fn release_credit_after_drop(&self, credit: JobCredit) {
        let mut jobs = self.jobs.lock().unwrap();
        assert!(matches!(jobs.cells[credit.slot], JobCell::InCleanup(id) if id == credit.id));
        jobs.cells[credit.slot] = JobCell::Free;
        jobs.charged_capacity[credit.slot] = 0;
    }
}

impl<C, K> CreditOwner for CoreControl<C, K>
where
    C: BoundedWriterChannel + Send + Sync + 'static,
    K: KeyedWriterClock + Send + Sync + 'static,
    for<'a> C::Write<'a>: Send,
    for<'a> K::WriterSleep<'a>: Send,
{
    fn release_credit_after_drop(&self, credit: JobCredit) {
        CoreControl::release_credit_after_drop(self, credit);
    }
}

struct ActiveJob {
    credit: JobCredit,
    future: Pin<Box<dyn Future<Output = WriteOutcome> + Send + 'static>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DispatchResult {
    Idle,
    Rescheduled,
    Pending,
    Retired(WriteOutcome),
}

struct CoreWorker<C, K> {
    control: Arc<CoreControl<C, K>>,
    cleanup: Arc<Mutex<CoreCleanup>>,
    active: Option<ActiveJob>,
}

impl<C, K> CoreWorker<C, K>
where
    C: BoundedWriterChannel + Send + Sync + 'static,
    K: KeyedWriterClock + Send + Sync + 'static,
    for<'a> C::Write<'a>: Send,
    for<'a> K::WriterSleep<'a>: Send,
{
    fn new(control: Arc<CoreControl<C, K>>, cleanup: Arc<Mutex<CoreCleanup>>) -> Self {
        Self {
            control,
            cleanup,
            active: None,
        }
    }

    fn retire(&self, retired: RetiredJob) {
        let slot = retired.credit.slot;
        let id = retired.credit.id;
        let mut jobs = self.control.jobs.lock().unwrap();
        assert!(matches!(jobs.cells[slot], JobCell::InFlight(old) if old == id));
        jobs.cells[slot] = JobCell::InCleanup(id);
        drop(jobs);
        let mut cleanup = self.cleanup.lock().unwrap();
        assert!(cleanup.disposal[slot].is_none());
        cleanup.disposal[slot] = Some(retired);
    }

    fn retire_queued(&self, outcome: WriteOutcome) {
        let queued = {
            let mut jobs = self.control.jobs.lock().unwrap();
            array::from_fn::<_, 2, _>(|slot| {
                match std::mem::replace(&mut jobs.cells[slot], JobCell::Free) {
                    JobCell::Queued(job) => {
                        jobs.cells[slot] = JobCell::InFlight(job.credit.id);
                        Some(job)
                    }
                    other => {
                        jobs.cells[slot] = other;
                        None
                    }
                }
            })
        };
        for job in queued.into_iter().flatten() {
            self.retire(RetiredJob {
                credit: job.credit,
                outcome,
                never_started: Some(job.frame),
            });
        }
    }

    fn reap(&mut self) -> DispatchResult {
        if let Some(active) = self.active.take() {
            drop(active.future); // only this independent worker drops a pending host future
            self.retire(RetiredJob {
                credit: active.credit,
                outcome: WriteOutcome::Reaped,
                never_started: None,
            });
        }
        self.retire_queued(WriteOutcome::Reaped);
        self.control.jobs.lock().unwrap().sealed = true;
        DispatchResult::Retired(WriteOutcome::Reaped)
    }

    fn dispatch_once(&mut self) -> DispatchResult {
        self.dispatch_once_with_empty(|| {})
    }

    fn dispatch_once_with_empty(&mut self, on_empty: impl FnOnce()) -> DispatchResult {
        if !self.control.adapter.writer.take_scheduled() {
            return DispatchResult::Idle;
        }
        if self.control.reap_pending.load(Ordering::Acquire) {
            return self.reap();
        }
        if self.active.is_none() {
            let epoch = self.control.adapter.writer.epoch();
            let head = {
                let mut jobs = self.control.jobs.lock().unwrap();
                let index = jobs
                    .cells
                    .iter()
                    .enumerate()
                    .filter_map(|(i, cell)| {
                        if let JobCell::Queued(job) = cell {
                            Some((i, job.credit.id.sequence))
                        } else {
                            None
                        }
                    })
                    .min_by_key(|(_, sequence)| *sequence)
                    .map(|(i, _)| i);
                index.map(|slot| {
                    let JobCell::Queued(job) =
                        std::mem::replace(&mut jobs.cells[slot], JobCell::Free)
                    else {
                        unreachable!()
                    };
                    jobs.cells[slot] = JobCell::InFlight(job.credit.id);
                    job
                })
            };
            let Some(job) = head else {
                on_empty();
                return if self.control.adapter.writer.changed_since(epoch) {
                    DispatchResult::Rescheduled
                } else {
                    DispatchResult::Idle
                };
            };
            let future = Arc::clone(&self.control.adapter).run(
                job.credit.id,
                job.frame,
                job.original_deadline,
            );
            self.active = Some(ActiveJob {
                credit: job.credit,
                future,
            });
        }
        if self.control.reap_pending.load(Ordering::Acquire) {
            return self.reap();
        }
        let wake = self.control.adapter.writer.owned_waker();
        let mut cx = Context::from_waker(&wake);
        let poll = self.active.as_mut().unwrap().future.as_mut().poll(&mut cx);
        drop(wake);
        if self.control.reap_pending.load(Ordering::Acquire) {
            return self.reap();
        }
        match poll {
            Poll::Pending => DispatchResult::Pending,
            Poll::Ready(outcome) => {
                let active = self.active.take().unwrap();
                drop(active.future);
                self.retire(RetiredJob {
                    credit: active.credit,
                    outcome,
                    never_started: None,
                });
                if matches!(
                    outcome,
                    WriteOutcome::Complete | WriteOutcome::TimedOutBeforeStart
                ) {
                    if self
                        .control
                        .jobs
                        .lock()
                        .unwrap()
                        .cells
                        .iter()
                        .any(|cell| matches!(cell, JobCell::Queued(_)))
                    {
                        self.control.adapter.writer.signal_writer();
                    }
                } else {
                    self.control.jobs.lock().unwrap().sealed = true;
                    self.retire_queued(WriteOutcome::Reaped);
                }
                DispatchResult::Retired(outcome)
            }
        }
    }
}

fn cleanup_one(cleanup: &Mutex<CoreCleanup>, slot: usize) -> Option<WriteOutcome> {
    let (retired, owner) = {
        let mut cleanup = cleanup.lock().unwrap();
        (cleanup.disposal[slot].take()?, Arc::clone(&cleanup.owner))
    };
    let outcome = retired.outcome;
    let RetiredJob {
        credit,
        never_started,
        ..
    } = retired;
    drop(never_started);
    owner.release_credit_after_drop(credit);
    Some(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize};
    use std::task::Waker;
    use std::thread;

    use pi_ai_core::BoxFuture;

    use crate::extensions::critical_port::{
        CriticalRoutePort, DrainArmResult, DrainKey, DrainSubmitter, HostArmResult,
        OwnedDrainTicket, PortError, PortLimits, RouteId, WriterNotify,
    };
    use crate::extensions::transport::{
        MonotonicClock, OrderedByteChannel, QuietClose, QuietOrderedByteChannel,
    };

    struct CountWake(AtomicUsize);
    impl std::task::Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn port() -> Arc<CriticalRoutePort> {
        Arc::new(
            CriticalRoutePort::try_new(PortLimits {
                routes: 2,
                bindings: 1,
                ordinary_timers_per_route: 0,
                tasks_per_route: 1,
                observers_per_route: 1,
            })
            .unwrap(),
        )
    }

    fn frame(value: u8) -> EncodedFrame {
        let mut bytes = Vec::with_capacity(64);
        bytes.push(value);
        EncodedFrame { bytes, body_len: 1 }
    }

    #[derive(Clone, Copy)]
    enum WriteStep {
        Pending,
        Ready(Result<(), ChannelError>, Option<u64>),
    }

    struct ScriptChannel {
        script: Mutex<usize>, // test-side step preparation only
        steps: [AtomicU8; 8],
        advances: [AtomicU64; 8],
        advance_present: [AtomicBool; 8],
        step_len: AtomicUsize,
        step_next: AtomicUsize,
        permit: AtomicU8,
        pending: AtomicBool,
        constructed: AtomicUsize,
        polled: AtomicUsize,
        dropped: AtomicUsize,
        values: [AtomicU8; 8],
        values_len: AtomicUsize,
        dropped_on_fixture_thread: AtomicBool,
        fixture_thread: thread::ThreadId,
        wake: Waker,
        clock: Arc<ScriptClock>,
    }

    impl ScriptChannel {
        fn new(clock: Arc<ScriptClock>, wake: Waker) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(0),
                steps: array::from_fn(|_| AtomicU8::new(0)),
                advances: array::from_fn(|_| AtomicU64::new(0)),
                advance_present: array::from_fn(|_| AtomicBool::new(false)),
                step_len: AtomicUsize::new(0),
                step_next: AtomicUsize::new(0),
                permit: AtomicU8::new(0),
                pending: AtomicBool::new(false),
                constructed: AtomicUsize::new(0),
                polled: AtomicUsize::new(0),
                dropped: AtomicUsize::new(0),
                values: array::from_fn(|_| AtomicU8::new(0)),
                values_len: AtomicUsize::new(0),
                dropped_on_fixture_thread: AtomicBool::new(false),
                fixture_thread: thread::current().id(),
                wake,
                clock,
            })
        }
        fn step(&self, step: WriteStep) {
            let mut length = self.script.lock().unwrap();
            assert!(*length < self.steps.len());
            let (code, advance) = match step {
                WriteStep::Pending => (1, None),
                WriteStep::Ready(Ok(()), advance) => (2, advance),
                WriteStep::Ready(Err(ChannelError::Failed), advance) => (3, advance),
                WriteStep::Ready(Err(ChannelError::Closed), advance) => (4, advance),
            };
            self.advances[*length].store(advance.unwrap_or(0), Ordering::Relaxed);
            self.advance_present[*length].store(advance.is_some(), Ordering::Relaxed);
            self.steps[*length].store(code, Ordering::Relaxed);
            *length += 1;
            self.step_len.store(*length, Ordering::Release);
        }
        fn allow(&self, result: Result<(), ChannelError>) {
            assert!(self.pending.load(Ordering::Acquire));
            let code = match result {
                Ok(()) => 1,
                Err(ChannelError::Failed) => 2,
                Err(ChannelError::Closed) => 3,
            };
            assert!(
                self.permit
                    .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            );
            self.wake.wake_by_ref();
        }
        fn counts(&self) -> (usize, usize, usize, bool) {
            (
                self.constructed.load(Ordering::Acquire),
                self.polled.load(Ordering::Acquire),
                self.dropped.load(Ordering::Acquire),
                self.pending.load(Ordering::Acquire),
            )
        }
        fn values(&self) -> Vec<u8> {
            (0..self
                .values_len
                .load(Ordering::Acquire)
                .min(self.values.len()))
                .map(|i| self.values[i].load(Ordering::Acquire))
                .collect()
        }
    }

    struct ScriptWrite<'a> {
        channel: &'a ScriptChannel,
        frame: Option<EncodedFrame>,
        started: bool,
    }

    impl Future for ScriptWrite<'_> {
        type Output = Result<(), ChannelError>;
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.channel.polled.fetch_add(1, Ordering::AcqRel);
            if !self.started {
                self.started = true;
                let index = self.channel.values_len.fetch_add(1, Ordering::AcqRel);
                if index < self.channel.values.len() {
                    let first = self
                        .frame
                        .as_ref()
                        .unwrap()
                        .bytes
                        .first()
                        .copied()
                        .unwrap_or(0);
                    self.channel.values[index].store(first, Ordering::Release);
                }
                let next = self.channel.step_next.load(Ordering::Acquire);
                let (code, advance) = if next < self.channel.step_len.load(Ordering::Acquire) {
                    self.channel.step_next.store(next + 1, Ordering::Release);
                    (
                        self.channel.steps[next].load(Ordering::Acquire),
                        self.channel.advance_present[next]
                            .load(Ordering::Acquire)
                            .then(|| self.channel.advances[next].load(Ordering::Acquire)),
                    )
                } else {
                    (2, None)
                };
                if code == 1 {
                    self.channel.pending.store(true, Ordering::Release);
                    Poll::Pending
                } else {
                    if let Some(advance) = advance {
                        // This scripted transition is inside the bounded host
                        // poll, so it only changes the sampled time atomically.
                        self.channel.clock.advance_from_poll(advance);
                    }
                    Poll::Ready(match code {
                        2 => Ok(()),
                        3 => Err(ChannelError::Failed),
                        4 => Err(ChannelError::Closed),
                        _ => unreachable!(),
                    })
                }
            } else {
                match self.channel.permit.swap(0, Ordering::AcqRel) {
                    0 => Poll::Pending,
                    code => {
                        self.channel.pending.store(false, Ordering::Release);
                        Poll::Ready(match code {
                            1 => Ok(()),
                            2 => Err(ChannelError::Failed),
                            3 => Err(ChannelError::Closed),
                            _ => unreachable!(),
                        })
                    }
                }
            }
        }
    }

    impl Drop for ScriptWrite<'_> {
        fn drop(&mut self) {
            self.channel.pending.store(false, Ordering::Release);
            self.channel.permit.store(0, Ordering::Release);
            self.channel.dropped.fetch_add(1, Ordering::AcqRel);
            if thread::current().id() == self.channel.fixture_thread {
                self.channel
                    .dropped_on_fixture_thread
                    .store(true, Ordering::Release);
            }
            drop(self.frame.take());
        }
    }

    impl OrderedByteChannel for ScriptChannel {
        fn max_read_scratch_bytes(&self) -> usize {
            0
        }
        fn read_chunk<'a>(
            &'a self,
            _: &'a mut [u8],
        ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
            Box::pin(async { Err(ChannelError::Failed) })
        }
        fn write_frame(&self, _: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
            Box::pin(async { Err(ChannelError::Failed) })
        }
        fn close(&self) {}
    }
    impl QuietOrderedByteChannel for ScriptChannel {
        fn close_quiet(&self) -> QuietClose {
            QuietClose::new(None, None)
        }
    }
    impl BoundedWriterChannel for ScriptChannel {
        type Write<'a> = ScriptWrite<'a>;
        fn write_frame_bounded(&self, frame: EncodedFrame) -> Self::Write<'_> {
            self.constructed.fetch_add(1, Ordering::AcqRel);
            ScriptWrite {
                channel: self,
                frame: Some(frame),
                started: false,
            }
        }
    }

    // Every timer and callback-capable field is anchored by the collector.
    // Clients borrow these prewarmed seats; their Drop only publishes atomics.
    struct BaseTimer {
        deadline: AtomicU64,
        generation: AtomicU64,
        phase: AtomicU8, // 0 free, 1 live, 2 dead, 3 draining
        wake: Mutex<Option<Waker>>,
        observer: Arc<ClockObserver>,
    }
    impl Drop for BaseTimer {
        fn drop(&mut self) {
            assert!(
                self.wake
                    .get_mut()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .is_none()
            );
            self.observer.timer_drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct BaseSleep<'a> {
        clock: &'a ScriptClock,
        slot: usize,
        generation: u64,
    }
    impl Future for BaseSleep<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let timer = &self.clock.timers[self.slot];
            assert_eq!(timer.generation.load(Ordering::Acquire), self.generation);
            if self.clock.now_ms() >= timer.deadline.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            // Only admitted, finite, non-unwinding fixture Wakers are eligible.
            assert!(
                self.clock
                    .eligible
                    .lock()
                    .unwrap()
                    .iter()
                    .flatten()
                    .any(|w| w.will_wake(cx.waker())),
                "unadmitted private fixture Waker"
            );
            let replacement = cx.waker().clone();
            let old = timer
                .wake
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .replace(replacement);
            drop(old); // never destroy an opaque callback while a line is held
            if self.clock.now_ms() >= timer.deadline.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for BaseSleep<'_> {
        fn drop(&mut self) {
            let timer = &self.clock.timers[self.slot];
            assert_eq!(timer.generation.load(Ordering::Acquire), self.generation);
            timer.phase.store(2, Ordering::Release);
            self.clock.sleeps.fetch_sub(1, Ordering::AcqRel);
        }
    }
    struct ScriptClock {
        state: Arc<ClockState>,
    }
    impl std::ops::Deref for ScriptClock {
        type Target = ClockState;
        fn deref(&self) -> &ClockState {
            &self.state
        }
    }
    impl Drop for ScriptClock {
        fn drop(&mut self) {
            self.client.store(false, Ordering::Release);
        }
    }
    struct ClockState {
        now: AtomicU64,
        next_slot: AtomicUsize,
        next_generation: AtomicU64,
        next_sequence: AtomicU64,
        registration_serial: AtomicU64,
        registration_state: AtomicU64, // serial << 2 | 0=free, 1=pending, 2=due
        registration_slot: AtomicUsize,
        registration_generation: AtomicU64,
        registration_sequence: AtomicU64,
        registration_deadline: AtomicU64,
        writer_wake: Mutex<Option<WriterNotify>>, // weak, test-side due delivery only
        base: Mutex<()>,
        timers: [BaseTimer; 2],
        eligible: Mutex<[Option<Waker>; 2]>,
        pending: AtomicBool,
        client: AtomicBool,
        sleeps: AtomicUsize,
        root_alive: AtomicBool,
        joined: AtomicBool,
        panic_service: AtomicBool,
        observer: Arc<ClockObserver>,
        collector: thread::ThreadId,
        removed: AtomicUsize,
    }
    impl Drop for ClockState {
        fn drop(&mut self) {
            assert_eq!(thread::current().id(), self.collector);
            assert!(self.joined.load(Ordering::Acquire));
            assert!(!self.client.load(Ordering::Acquire));
            assert_eq!(self.sleeps.load(Ordering::Acquire), 0);
            assert!(
                self.eligible
                    .get_mut()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .iter()
                    .all(Option::is_none)
            );
            if self.observer.debt.load(Ordering::Acquire) == 0 {
                self.observer.early_debt.store(true, Ordering::Release);
            }
            self.observer.state_drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    impl ClockState {
        fn new(now: u64, observer: Arc<ClockObserver>) -> Arc<Self> {
            Arc::new(Self {
                now: AtomicU64::new(now),
                next_slot: AtomicUsize::new(0),
                next_generation: AtomicU64::new(0),
                next_sequence: AtomicU64::new(0),
                registration_serial: AtomicU64::new(0),
                registration_state: AtomicU64::new(0),
                registration_slot: AtomicUsize::new(0),
                registration_generation: AtomicU64::new(0),
                registration_sequence: AtomicU64::new(0),
                registration_deadline: AtomicU64::new(0),
                writer_wake: Mutex::new(None),
                base: Mutex::new(()),
                timers: array::from_fn(|_| BaseTimer {
                    deadline: AtomicU64::new(0),
                    generation: AtomicU64::new(0),
                    phase: AtomicU8::new(0),
                    wake: Mutex::new(None),
                    observer: Arc::clone(&observer),
                }),
                eligible: Mutex::new(array::from_fn(|_| None)),
                pending: AtomicBool::new(false),
                client: AtomicBool::new(true),
                sleeps: AtomicUsize::new(0),
                root_alive: AtomicBool::new(true),
                joined: AtomicBool::new(false),
                panic_service: AtomicBool::new(false),
                observer,
                collector: thread::current().id(),
                removed: AtomicUsize::new(0),
            })
        }
        // Every turn performs maintenance, even when the publication bit is empty.
        fn drain_turn(&self, closing: bool) {
            let mut bag: [Option<Waker>; 2] = array::from_fn(|_| None);
            let mut dead = [false; 2];
            {
                let _base = self
                    .base
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                let now = self.now.load(Ordering::Acquire);
                for (slot, timer) in self.timers.iter().enumerate() {
                    let phase = timer.phase.load(Ordering::Acquire);
                    dead[slot] = phase == 2 || closing;
                    if dead[slot] {
                        timer.phase.store(3, Ordering::Release);
                    }
                    if dead[slot] || (phase == 1 && now >= timer.deadline.load(Ordering::Acquire)) {
                        bag[slot] = timer
                            .wake
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .take();
                    }
                }
            }
            for (slot, wake) in bag.into_iter().enumerate() {
                if dead[slot] {
                    drop(wake);
                } else if let Some(wake) = wake {
                    self.observer.cache.fetch_add(1, Ordering::AcqRel);
                    wake.wake();
                    self.observer.cache.fetch_sub(1, Ordering::AcqRel);
                    self.observer.cache_finished.fetch_add(1, Ordering::AcqRel);
                }
                // Reuse only after the old callback was actually released.
                if dead[slot] {
                    self.timers[slot].phase.store(0, Ordering::Release);
                }
            }
            if closing {
                let callbacks = {
                    let mut eligible = self
                        .eligible
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    array::from_fn::<_, 2, _>(|slot| eligible[slot].take())
                };
                drop(callbacks);
                self.pending.store(false, Ordering::Release);
            }
        }
    }
    impl ScriptClock {
        fn check_fixture_waker(&self, wake: &Waker) {
            let mut eligible = self
                .eligible
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if eligible.iter().flatten().any(|old| old.will_wake(wake)) {
                return;
            }
            let slot = eligible
                .iter()
                .position(Option::is_none)
                .expect("two eligible callbacks");
            eligible[slot] = Some(wake.clone());
        }
        fn set_writer_wake(&self, wake: WriterNotify) {
            *self
                .writer_wake
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(wake);
        }
        fn publish_advance(&self, now: u64) {
            self.now.fetch_max(now, Ordering::AcqRel);
            self.pending.store(true, Ordering::Release);
        }
        fn advance_from_poll(&self, now: u64) {
            self.publish_advance(now);
        }
        fn set(&self, now: u64) {
            self.publish_advance(now);
            if let Some(key) = self.key() {
                self.fire(key);
            }
        }
        fn key(&self) -> Option<JobId> {
            loop {
                let state = self.registration_state.load(Ordering::Acquire);
                if state & 3 == 0 {
                    return None;
                }
                let key = JobId {
                    route_slot: self.registration_slot.load(Ordering::Acquire),
                    route_generation: self.registration_generation.load(Ordering::Acquire),
                    sequence: self.registration_sequence.load(Ordering::Acquire),
                };
                if self.registration_state.load(Ordering::Acquire) == state {
                    return Some(key);
                }
            }
        }
        fn fire(&self, key: JobId) -> bool {
            loop {
                let state = self.registration_state.load(Ordering::Acquire);
                if state & 3 == 0 {
                    return false;
                }
                if self.registration_slot.load(Ordering::Acquire) != key.route_slot
                    || self.registration_generation.load(Ordering::Acquire) != key.route_generation
                    || self.registration_sequence.load(Ordering::Acquire) != key.sequence
                    || self.now_ms() < self.registration_deadline.load(Ordering::Acquire)
                {
                    return false;
                }
                if state & 3 == 2 {
                    return true;
                }
                if self
                    .registration_state
                    .compare_exchange(state, (state & !3) | 2, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    if let Some(wake) = self
                        .writer_wake
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .as_ref()
                    {
                        wake.signal();
                    }
                    return true;
                }
            }
        }
    }
    struct ScriptSleep<'a> {
        clock: &'a ScriptClock,
        key: JobId,
        serial: u64,
        seat: Option<crate::extensions::critical_port::SeatLease<'a>>,
    }
    impl Future for ScriptSleep<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            let state = self.clock.registration_state.load(Ordering::Acquire);
            assert_eq!(state >> 2, self.serial);
            assert_ne!(state & 3, 0);
            assert_eq!(self.clock.key(), Some(self.key));
            if state & 3 == 2
                || self.clock.now_ms() >= self.clock.registration_deadline.load(Ordering::Acquire)
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for ScriptSleep<'_> {
        fn drop(&mut self) {
            loop {
                let state = self.clock.registration_state.load(Ordering::Acquire);
                assert_eq!(state >> 2, self.serial);
                assert_ne!(state & 3, 0);
                if self
                    .clock
                    .registration_state
                    .compare_exchange(state, self.serial << 2, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }
            self.clock.removed.fetch_add(1, Ordering::AcqRel);
            drop(self.seat.take()); // exact deregistration precedes port seat release
        }
    }
    impl MonotonicClock for ScriptClock {
        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::Acquire)
        }
        fn sleep_until(&self, deadline: u64) -> BoxFuture<'_, ()> {
            let _base = self.base.lock().unwrap();
            let slot = self
                .timers
                .iter()
                .position(|timer| timer.phase.load(Ordering::Acquire) == 0)
                .expect("scripted base clock has two timer seats");
            let timer = &self.timers[slot];
            let generation = timer
                .generation
                .load(Ordering::Acquire)
                .checked_add(1)
                .expect("timer generation exhausted");
            timer.generation.store(generation, Ordering::Release);
            timer.deadline.store(deadline, Ordering::Release);
            timer.phase.store(1, Ordering::Release);
            self.sleeps.fetch_add(1, Ordering::AcqRel);
            Box::pin(BaseSleep {
                clock: self,
                slot,
                generation,
            })
        }
    }
    impl ExactWriterClock for ScriptClock {
        type WriterSleep<'a> = ScriptSleep<'a>;
        fn writer_sleep_until<'a>(
            &'a self,
            seat: crate::extensions::critical_port::SeatLease<'a>,
            deadline: u64,
        ) -> Self::WriterSleep<'a> {
            let key = JobId {
                route_slot: self.next_slot.load(Ordering::Acquire),
                route_generation: self.next_generation.load(Ordering::Acquire),
                sequence: self.next_sequence.swap(0, Ordering::AcqRel),
            };
            assert_ne!(key.sequence, 0);
            assert_eq!(self.registration_state.load(Ordering::Acquire) & 3, 0);
            let serial = self
                .registration_serial
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                    (old < (u64::MAX >> 2)).then_some(old + 1)
                })
                .unwrap()
                + 1;
            self.registration_generation
                .store(key.route_generation, Ordering::Release);
            self.registration_slot
                .store(key.route_slot, Ordering::Release);
            self.registration_sequence
                .store(key.sequence, Ordering::Release);
            self.registration_deadline
                .store(deadline, Ordering::Release);
            self.registration_state
                .store((serial << 2) | 1, Ordering::Release);
            ScriptSleep {
                clock: self,
                key,
                serial,
                seat: Some(seat),
            }
        }
    }
    impl KeyedWriterClock for ScriptClock {
        fn prepare_key(&self, key: JobId) {
            assert_eq!(self.next_sequence.load(Ordering::Acquire), 0);
            self.next_slot.store(key.route_slot, Ordering::Release);
            self.next_generation
                .store(key.route_generation, Ordering::Release);
            self.next_sequence.store(key.sequence, Ordering::Release);
        }
    }

    #[derive(Default)]
    struct ClockObserver {
        admissions: AtomicUsize,
        debt: AtomicUsize,
        early_debt: AtomicBool,
        failure_drops: AtomicUsize,
        failure_rescue: AtomicBool,
        peak: AtomicUsize,
        service_starts: AtomicUsize,
        service_exits: AtomicUsize,
        service_joins: AtomicUsize,
        body_joins: AtomicUsize,
        state_drops: AtomicUsize,
        timer_drops: AtomicUsize,
        control_drops: AtomicUsize,
        replies: AtomicUsize,
        abandoned: AtomicUsize,
        empty_turns: AtomicUsize,
        turns: AtomicUsize,
        cache: AtomicUsize,
        cache_finished: AtomicUsize,
        pause_after_scan: AtomicBool,
        at_scan: AtomicBool,
        pause_after_swap: AtomicBool,
        at_swap: AtomicBool,
        pause_empty: AtomicBool,
        at_empty: AtomicBool,
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RunFailure {
        Closed,
        Full,
        SetupRejected,
        ThreadRejected,
        Body,
        Service,
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct RunReport {
        failure: Option<RunFailure>,
    }
    type FailedBody = Box<dyn std::any::Any + Send>;
    type ClockPair = (ClockRoot, Arc<ScriptClock>);
    #[derive(Clone, Copy)]
    enum SetupMode {
        Normal,
        Reject,
        FailedPair,
        ThreadRejected,
    }
    enum SetupStatus {
        Free,
        Requested(u64, SetupMode),
        Building,
        Reply(Result<ClockPair, RunFailure>),
    }
    struct SetupCell {
        line: Mutex<SetupStatus>,
        waiting: AtomicBool,
    }
    struct SetupMailbox {
        closed: AtomicBool,
        cells: [SetupCell; 2],
        observer: Arc<ClockObserver>,
        collector: thread::ThreadId,
    }
    impl Drop for SetupMailbox {
        fn drop(&mut self) {
            assert_eq!(thread::current().id(), self.collector);
            assert!(self.cells.iter_mut().all(|cell| {
                matches!(
                    cell.line
                        .get_mut()
                        .unwrap_or_else(|poison| poison.into_inner()),
                    SetupStatus::Free
                )
            }));
            self.observer.control_drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[derive(Clone)]
    struct ClockFactory {
        mailbox: Arc<SetupMailbox>,
    }
    struct SetupWait {
        mailbox: Arc<SetupMailbox>,
        slot: usize,
        active: bool,
    }
    impl Drop for SetupWait {
        fn drop(&mut self) {
            if self.active {
                self.mailbox.cells[self.slot]
                    .waiting
                    .store(false, Ordering::Release);
            }
        }
    }
    impl SetupWait {
        fn take(mut self) -> Result<ClockPair, RunFailure> {
            loop {
                let cell = &self.mailbox.cells[self.slot];
                let response = {
                    let mut line = cell
                        .line
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    if matches!(*line, SetupStatus::Reply(_)) {
                        let SetupStatus::Reply(response) =
                            std::mem::replace(&mut *line, SetupStatus::Free)
                        else {
                            unreachable!()
                        };
                        cell.waiting.store(false, Ordering::Release);
                        Some(response)
                    } else {
                        None
                    }
                };
                if let Some(response) = response {
                    self.active = false;
                    return response;
                }
                if self.mailbox.closed.load(Ordering::Acquire) {
                    return Err(RunFailure::Closed);
                }
                thread::yield_now();
            }
        }
    }
    impl ClockFactory {
        fn begin(&self, now: u64, mode: SetupMode) -> Result<SetupWait, RunFailure> {
            if self.mailbox.closed.load(Ordering::Acquire) {
                return Err(RunFailure::Closed);
            }
            for (slot, cell) in self.mailbox.cells.iter().enumerate() {
                let mut line = cell
                    .line
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                if self.mailbox.closed.load(Ordering::Acquire) {
                    return Err(RunFailure::Closed);
                }
                if matches!(*line, SetupStatus::Free) {
                    cell.waiting.store(true, Ordering::Release);
                    *line = SetupStatus::Requested(now, mode);
                    return Ok(SetupWait {
                        mailbox: Arc::clone(&self.mailbox),
                        slot,
                        active: true,
                    });
                }
            }
            Err(RunFailure::Full)
        }
        fn setup(&self, now: u64) -> Result<ClockPair, RunFailure> {
            self.begin(now, SetupMode::Normal)?.take()
        }
        fn clock(&self, now: u64) -> Arc<ScriptClock> {
            let pair = loop {
                match self.setup(now) {
                    Err(RunFailure::Full) => thread::yield_now(),
                    other => break other.unwrap(),
                }
            };
            let (root, clock) = pair;
            drop(root);
            clock
        }
    }
    struct ClockRoot {
        state: Option<Arc<ClockState>>,
        receipt: bool,
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Finish {
        Busy,
        Collecting,
        Joined,
    }
    impl ClockRoot {
        fn finish(&mut self) -> Finish {
            let Some(state) = self.state.as_ref() else {
                return Finish::Joined;
            };
            if state.client.load(Ordering::Acquire) {
                return Finish::Busy;
            }
            if !state.joined.load(Ordering::Acquire) {
                return Finish::Collecting;
            }
            state.root_alive.store(false, Ordering::Release);
            drop(self.state.take()); // collector still anchors all callback-capable data
            self.receipt = true;
            Finish::Joined
        }
    }
    impl Drop for ClockRoot {
        fn drop(&mut self) {
            if let Some(state) = &self.state {
                state.root_alive.store(false, Ordering::Release);
            }
        }
    }
    struct ClockEntry {
        state: Arc<ClockState>,
        join: Option<thread::JoinHandle<()>>,
    }
    struct ClockHost;
    impl ClockHost {
        fn run<F>(observer: Arc<ClockObserver>, body: F) -> RunReport
        where
            F: FnOnce(ClockFactory) -> Result<(), FailedBody> + Send + 'static,
        {
            Self::run_with_body_stack(observer, body, None)
        }
        fn run_with_body_stack<F>(
            observer: Arc<ClockObserver>,
            body: F,
            stack: Option<usize>,
        ) -> RunReport
        where
            F: FnOnce(ClockFactory) -> Result<(), FailedBody> + Send + 'static,
        {
            let mailbox = Arc::new(SetupMailbox {
                closed: AtomicBool::new(false),
                cells: array::from_fn(|_| SetupCell {
                    line: Mutex::new(SetupStatus::Free),
                    waiting: AtomicBool::new(false),
                }),
                observer: Arc::clone(&observer),
                collector: thread::current().id(),
            });
            let factory = ClockFactory {
                mailbox: Arc::clone(&mailbox),
            };
            // This invocation owns the body Join from the moment spawn returns.
            let mut builder = thread::Builder::new().name("private-clock-body".into());
            if let Some(stack) = stack {
                builder = builder.stack_size(stack);
            }
            let mut body = match builder.spawn(move || body(factory)) {
                Ok(join) => Some(join),
                Err(error) => {
                    drop(error);
                    mailbox.closed.store(true, Ordering::Release);
                    drop(mailbox);
                    return RunReport {
                        failure: Some(RunFailure::ThreadRejected),
                    };
                }
            };
            let mut entries: [Option<ClockEntry>; 2] = array::from_fn(|_| None);
            let mut failure = None;
            let mut serial = 0_u64;
            loop {
                // A failed body output/panic can retain a client. Reduce and
                // actually destroy it before waiting for any service client0.
                if body.as_ref().is_some_and(thread::JoinHandle::is_finished) {
                    let output = body.take().unwrap().join();
                    observer.body_joins.fetch_add(1, Ordering::AcqRel);
                    mailbox.closed.store(true, Ordering::Release);
                    match output {
                        Ok(Ok(())) => {}
                        Ok(Err(payload)) | Err(payload) => {
                            failure.get_or_insert(RunFailure::Body);
                            reduce_failure_payload(payload);
                        }
                    }
                }
                for cell in &mailbox.cells {
                    let mut discarded = None;
                    let request = {
                        let mut line = cell
                            .line
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner());
                        if (mailbox.closed.load(Ordering::Acquire)
                            || !cell.waiting.load(Ordering::Acquire))
                            && matches!(*line, SetupStatus::Reply(_))
                        {
                            let SetupStatus::Reply(reply) =
                                std::mem::replace(&mut *line, SetupStatus::Free)
                            else {
                                unreachable!()
                            };
                            discarded = Some(reply);
                            None
                        } else if let SetupStatus::Requested(now, mode) = *line {
                            *line = SetupStatus::Building;
                            Some((now, mode))
                        } else {
                            None
                        }
                    };
                    if let Some(reply) = discarded {
                        drop(reply); // opaque abandoned pair drops outside the mailbox
                        observer.abandoned.fetch_add(1, Ordering::AcqRel);
                    }
                    if let Some((now, mode)) = request {
                        let result = if mailbox.closed.load(Ordering::Acquire) {
                            Err(RunFailure::Closed)
                        } else if matches!(mode, SetupMode::Reject) {
                            failure.get_or_insert(RunFailure::SetupRejected);
                            Err(RunFailure::SetupRejected)
                        } else if let Some(slot) = entries.iter().position(Option::is_none) {
                            if let Some(next) = serial.checked_add(1) {
                                serial = next;
                                let state = ClockState::new(now, Arc::clone(&observer));
                                observer.admissions.fetch_add(1, Ordering::AcqRel);
                                let debt = observer.debt.fetch_add(1, Ordering::AcqRel) + 1;
                                observer.peak.fetch_max(debt, Ordering::AcqRel);
                                let service_state = Arc::clone(&state);
                                let mut builder = thread::Builder::new()
                                    .name(format!("private-clock-service-{serial}"));
                                if matches!(mode, SetupMode::ThreadRejected) {
                                    // Actual native spawn refusal on this selected 64-bit
                                    // fixture: the requested stack exceeds its virtual address space.
                                    builder = builder.stack_size(usize::MAX / 4);
                                }
                                let started = builder.spawn(move || Self::service(service_state));
                                match started {
                                    Ok(join) => {
                                        // The fixed collector entry owns the sole
                                        // actual Join before either claim escapes.
                                        entries[slot] = Some(ClockEntry {
                                            state: Arc::clone(&state),
                                            join: Some(join),
                                        });
                                        let pair = (
                                            ClockRoot {
                                                state: Some(Arc::clone(&state)),
                                                receipt: false,
                                            },
                                            Arc::new(ScriptClock { state }),
                                        );
                                        if matches!(mode, SetupMode::FailedPair) {
                                            let opaque: FailedBody = Box::new(pair);
                                            failure.get_or_insert(RunFailure::SetupRejected);
                                            reduce_failure_payload(opaque);
                                            Err(RunFailure::SetupRejected)
                                        } else {
                                            Ok(pair)
                                        }
                                    }
                                    Err(error) => {
                                        drop(error);
                                        state.client.store(false, Ordering::Release);
                                        state.root_alive.store(false, Ordering::Release);
                                        state.joined.store(true, Ordering::Release);
                                        entries[slot] = Some(ClockEntry { state, join: None });
                                        failure.get_or_insert(RunFailure::ThreadRejected);
                                        Err(RunFailure::ThreadRejected)
                                    }
                                }
                            } else {
                                Err(RunFailure::SetupRejected)
                            }
                        } else {
                            Err(RunFailure::Full)
                        };
                        let mut result = Some(result);
                        {
                            let mut line = cell
                                .line
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner());
                            if cell.waiting.load(Ordering::Acquire)
                                && !mailbox.closed.load(Ordering::Acquire)
                            {
                                *line = SetupStatus::Reply(result.take().unwrap());
                                observer.replies.fetch_add(1, Ordering::AcqRel);
                            } else {
                                *line = SetupStatus::Free;
                            }
                        }
                        drop(result); // never hold a rejected/abandoned client in a failure report
                    }
                }
                for entry in &mut entries {
                    if let Some(live) = entry {
                        if live
                            .join
                            .as_ref()
                            .is_some_and(thread::JoinHandle::is_finished)
                        {
                            let outcome = live.join.take().unwrap().join();
                            observer.service_joins.fetch_add(1, Ordering::AcqRel);
                            if let Err(payload) = outcome {
                                failure.get_or_insert(RunFailure::Service);
                                reduce_failure_payload(payload);
                            }
                            live.state.joined.store(true, Ordering::Release);
                        }
                        if live.join.is_none() && !live.state.client.load(Ordering::Acquire) {
                            // A failed service can leave timer/callback payloads.
                            live.state.drain_turn(true);
                            // No raw Arc/Weak escapes; count1 closes the exact
                            // graph, including retained Roots and failed outputs.
                            if Arc::strong_count(&live.state) == 1 {
                                let retired = entry.take().unwrap();
                                drop(retired); // actual state + two timer destruction
                                observer.debt.fetch_sub(1, Ordering::AcqRel); // only afterward admit reuse
                            }
                        }
                    }
                }
                let empty = mailbox.cells.iter().all(|cell| {
                    matches!(
                        *cell
                            .line
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner()),
                        SetupStatus::Free
                    )
                });
                if body.is_none()
                    && entries.iter().all(Option::is_none)
                    && empty
                    && Arc::strong_count(&mailbox) == 1
                {
                    break;
                }
                thread::yield_now();
            }
            drop(mailbox); // actual control allocation destruction precedes report
            RunReport { failure }
        }
        fn service(state: Arc<ClockState>) {
            state.observer.service_starts.fetch_add(1, Ordering::AcqRel);
            loop {
                let pending = state.pending.swap(false, Ordering::AcqRel);
                let observer = &state.observer;
                observer.turns.fetch_add(1, Ordering::AcqRel);
                if observer.pause_after_swap.load(Ordering::Acquire) {
                    observer.at_swap.store(true, Ordering::Release);
                    while observer.pause_after_swap.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                }
                if !pending {
                    observer.empty_turns.fetch_add(1, Ordering::AcqRel);
                    if observer.pause_empty.load(Ordering::Acquire) {
                        observer.at_empty.store(true, Ordering::Release);
                        while observer.pause_empty.load(Ordering::Acquire) {
                            thread::yield_now();
                        }
                    }
                }
                assert!(
                    !state.panic_service.load(Ordering::Acquire),
                    "private service failure"
                );
                let closing = !state.client.load(Ordering::Acquire);
                state.drain_turn(closing);
                if closing {
                    break;
                }
                if observer.pause_after_scan.load(Ordering::Acquire) {
                    observer.at_scan.store(true, Ordering::Release);
                    while observer.pause_after_scan.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                }
                // Always runnable; a false bit or a publication racing a turn
                // cannot remove the guaranteed next service turn.
                thread::yield_now();
            }
            state.observer.service_exits.fetch_add(1, Ordering::AcqRel);
            drop(state); // release actor's strong state before actual Join returns
        }
    }
    // Opaque failure destructors can unwind too. Keep the collector admitted,
    // reduce each resulting payload, and retain no client-bearing error box.
    fn reduce_failure_payload(mut payload: FailedBody) {
        loop {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))) {
                Ok(()) => break,
                Err(next) => payload = next,
            }
        }
    }
    fn wait_for(mut predicate: impl FnMut() -> bool) -> bool {
        let start = std::time::Instant::now();
        while !predicate() {
            if start.elapsed() >= std::time::Duration::from_secs(1) {
                return false;
            }
            thread::yield_now();
        }
        true
    }
    fn with_host(body: impl FnOnce(ClockFactory) + Send + 'static) {
        let observer = Arc::new(ClockObserver::default());
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            body(factory);
            Ok(())
        });
        assert_eq!(report.failure, None);
        assert_eq!(observer.debt.load(Ordering::Acquire), 0);
        assert!(!observer.early_debt.load(Ordering::Acquire));
        assert_eq!(
            observer.admissions.load(Ordering::Acquire),
            observer.service_joins.load(Ordering::Acquire)
        );
        assert_eq!(
            observer.state_drops.load(Ordering::Acquire),
            observer.service_joins.load(Ordering::Acquire)
        );
        assert_eq!(
            observer.timer_drops.load(Ordering::Acquire),
            2 * observer.state_drops.load(Ordering::Acquire)
        );
        assert_eq!(observer.body_joins.load(Ordering::Acquire), 1);
        assert_eq!(observer.control_drops.load(Ordering::Acquire), 1);
    }

    type Control = CoreControl<ScriptChannel, ScriptClock>;
    type Worker = CoreWorker<ScriptChannel, ScriptClock>;
    struct Fixture {
        clock: Arc<ScriptClock>,
        channel: Arc<ScriptChannel>,
        control: Arc<Control>,
        cleanup: Arc<Mutex<CoreCleanup>>,
        worker: Worker,
    }
    fn fixture(factory: &ClockFactory, now: u64) -> Fixture {
        fixture_on(port(), factory.clock(now))
    }
    fn fixture_on(port: Arc<CriticalRoutePort>, clock: Arc<ScriptClock>) -> Fixture {
        let route = port.try_reserve_route_owned().unwrap();
        let writer = route.try_reserve_writer().unwrap();
        clock.set_writer_wake(writer.weak_notify());
        let channel = ScriptChannel::new(Arc::clone(&clock), writer.owned_waker());
        let adapter = Arc::new(FiniteAdapter {
            channel: Arc::clone(&channel),
            clock: Arc::clone(&clock),
            route,
            writer,
        });
        let control = CoreControl::new(adapter);
        let cleanup = Arc::new(Mutex::new(CoreCleanup::new(
            Arc::clone(&control) as Arc<dyn CreditOwner>
        )));
        let worker = Worker::new(Arc::clone(&control), Arc::clone(&cleanup));
        Fixture {
            clock,
            channel,
            control,
            cleanup,
            worker,
        }
    }
    impl Fixture {
        fn clean(&self, slot: usize) -> Option<WriteOutcome> {
            cleanup_one(&self.cleanup, slot)
        }
    }

    #[test]
    fn coalesced_fifo_self_schedules_second_head_and_waker_schedules_pending() {
        with_host(|factory| {
            let mut f = fixture(&factory, 10);
            let j1 = f.control.try_enqueue(frame(1), 100).unwrap();
            let j2 = f.control.try_enqueue(frame(2), 100).unwrap();
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(f.channel.counts().2, 1);
            assert_eq!(f.clock.key(), None);
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Idle);
            assert_eq!(f.channel.values(), [1, 2]);
            assert_eq!(f.clock.key(), None);
            assert_eq!(
                f.clean(j1.sequence as usize - 1),
                Some(WriteOutcome::Complete)
            );
            assert_eq!(
                f.clean(j2.sequence as usize - 1),
                Some(WriteOutcome::Complete)
            );

            f.channel.step(WriteStep::Pending);
            let j3 = f.control.try_enqueue(frame(3), 100).unwrap();
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Idle);
            f.channel.allow(Ok(()));
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(
                f.clean((j3.sequence as usize - 1) % 2),
                Some(WriteOutcome::Complete)
            );
        });
    }

    #[test]
    fn empty_check_rechecks_a_racing_enqueue_before_parking() {
        with_host(|factory| {
            let mut f = fixture(&factory, 1);
            f.control.adapter.writer.signal_writer();
            let control = Arc::clone(&f.control);
            assert_eq!(
                f.worker.dispatch_once_with_empty(|| {
                    control.try_enqueue(frame(7), 100).unwrap();
                }),
                DispatchResult::Rescheduled
            );
            assert_eq!(f.channel.counts().1, 0);
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(f.clean(0), Some(WriteOutcome::Complete));
        });
    }

    #[test]
    fn final_clock_sample_and_prewrite_expiry_seal_without_late_write() {
        with_host(|factory| {
            let mut f = fixture(&factory, 99);
            f.channel.step(WriteStep::Ready(Ok(()), Some(100)));
            let j1 = f.control.try_enqueue(frame(1), 100).unwrap();
            let j2 = f.control.try_enqueue(frame(2), 200).unwrap();
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::TimedOut)
            );
            assert_eq!(f.channel.counts().0, 1);
            assert_eq!(f.channel.counts().2, 1);
            assert!(!f.channel.counts().3);
            assert_eq!(f.clock.key(), None);
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Idle);
            assert_eq!(f.clean(0), Some(WriteOutcome::TimedOut));
            assert_eq!(f.clean(1), Some(WriteOutcome::Reaped));
            assert_eq!(f.control.charged_capacity(), 0);
            assert_eq!(j1.sequence, 1);
            assert_eq!(j2.sequence, 2);

            let mut expired = fixture(&factory, 100);
            let job = expired.control.try_enqueue(frame(3), 100).unwrap();
            assert_eq!(
                expired.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::TimedOutBeforeStart)
            );
            assert_eq!(expired.channel.counts().0, 0);
            assert_eq!(expired.clock.key(), None);
            assert_eq!(
                expired.clean(job.sequence as usize - 1),
                Some(WriteOutcome::TimedOutBeforeStart)
            );
            assert_eq!(expired.control.charged_capacity(), 0);
        });
    }

    #[test]
    fn two_credits_cover_queued_inflight_and_cleanup() {
        with_host(|factory| {
            let mut f = fixture(&factory, 1);
            f.channel.step(WriteStep::Pending);
            let j1 = f.control.try_enqueue(frame(1), 100).unwrap();
            let _j2 = f.control.try_enqueue(frame(2), 100).unwrap();
            assert_eq!(f.control.charged_capacity(), 128);
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            let refused = f.control.try_enqueue(frame(3), 100).unwrap_err();
            assert_eq!(refused.0, QueueError::Full);
            assert_eq!(refused.1.bytes.capacity(), 64);
            f.channel.allow(Ok(()));
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            let refused = f.control.try_enqueue(refused.1, 100).unwrap_err();
            assert_eq!(refused.0, QueueError::Full);
            assert_eq!(f.clean(0), Some(WriteOutcome::Complete));
            let j3 = f.control.try_enqueue(refused.1, 100).unwrap();
            assert!(f.control.charged_capacity() <= 128);
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(f.clean(1), Some(WriteOutcome::Complete));
            assert_eq!(f.clean(0), Some(WriteOutcome::Complete));
            assert_eq!(f.clean(0), None);
            assert_ne!(j1, j3);
            assert_eq!(f.control.charged_capacity(), 0);
            assert!(!f.channel.counts().3);
        });
    }

    #[test]
    fn retained_cleanup_fences_old_route_across_generation_reuse() {
        with_host(|factory| {
            let port = port();
            let mut old = fixture_on(Arc::clone(&port), factory.clock(1));
            let old_key = old.control.try_enqueue(frame(1), 100).unwrap();
            old.control.try_enqueue(frame(2), 100).unwrap();
            old.control.request_reap();
            assert_eq!(
                old.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Reaped)
            );
            let cleanup = Arc::clone(&old.cleanup);
            drop(old);

            let old_capacity: usize = cleanup
                .lock()
                .unwrap()
                .disposal
                .iter()
                .flatten()
                .map(|job| job.never_started.as_ref().unwrap().bytes.capacity())
                .sum();
            assert_eq!(old_capacity, 128);
            let mut fresh = fixture_on(Arc::clone(&port), factory.clock(1));
            let fresh_key = fresh.control.try_enqueue(frame(3), 100).unwrap();
            fresh.control.try_enqueue(frame(4), 100).unwrap();
            assert_ne!(
                old_key, fresh_key,
                "simultaneous route slots need distinct timer keys"
            );
            assert_eq!(old_capacity + fresh.control.charged_capacity(), 256);
            assert_eq!(
                port.try_reserve_route_owned().err(),
                Some(PortError::Exhausted),
                "old cleanup still owns one of the two route slots"
            );
            drop(cleanup);
            let third = fixture_on(Arc::clone(&port), factory.clock(1));
            assert_eq!(fresh.control.charged_capacity(), 128);
            assert_eq!(third.control.charged_capacity(), 0);
            fresh.control.request_reap();
            assert_eq!(
                fresh.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Reaped)
            );
            assert_eq!(fresh.clean(0), Some(WriteOutcome::Reaped));
            assert_eq!(fresh.clean(1), Some(WriteOutcome::Reaped));
            drop(third);
        });
    }

    #[test]
    fn scripted_host_dispatch_does_not_wait_for_test_control_locks() {
        with_host(|factory| {
            let f = fixture(&factory, 1);
            f.control.try_enqueue(frame(1), 100).unwrap();
            let held = f.channel.script.lock().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let mut worker = f.worker;
            let handle = thread::spawn(move || tx.send(worker.dispatch_once()).unwrap());
            let finished = rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok();
            drop(held);
            handle.join().unwrap();
            assert!(
                finished,
                "the bounded channel method waited on a control lock"
            );
        });
    }

    #[test]
    fn exact_writer_clock_methods_do_not_wait_for_due_delivery_lock() {
        with_host(|factory| {
            let f = fixture(&factory, 1);
            f.control.try_enqueue(frame(1), 100).unwrap();
            let held = f.clock.writer_wake.lock().unwrap();
            let held_base = f.clock.base.lock().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let mut worker = f.worker;
            let handle = thread::spawn(move || tx.send(worker.dispatch_once()).unwrap());
            let finished = rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok();
            drop(held_base);
            drop(held);
            handle.join().unwrap();
            assert!(
                finished,
                "exact timer construction, poll or Drop waited for due delivery"
            );
        });
    }

    #[test]
    fn base_script_sleep_waits_until_due_and_wakes_independently() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let mut f = fixture_on(port(), Arc::clone(&clock));
            f.channel.step(WriteStep::Pending);
            f.control.try_enqueue(frame(1), 100).unwrap();
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut cx = Context::from_waker(&waker);
            clock.check_fixture_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            clock.set(9);
            assert_eq!(wake.0.load(Ordering::Acquire), 0);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            clock.set(8);
            assert_eq!(clock.now_ms(), 9);
            clock.set(10);
            wait_for(|| wake.0.load(Ordering::Acquire) == 1);
            assert_eq!(wake.0.load(Ordering::Acquire), 1);
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
            assert!(f.channel.counts().3);
            f.control.request_reap();
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Reaped)
            );
            assert_eq!(f.clean(0), Some(WriteOutcome::Reaped));
        });
    }

    #[test]
    fn in_poll_clock_advance_wakes_a_parked_base_timer() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let mut f = fixture_on(port(), Arc::clone(&clock));
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut cx = Context::from_waker(&waker);
            clock.check_fixture_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            f.channel.step(WriteStep::Ready(Ok(()), Some(10)));
            f.control.try_enqueue(frame(1), 20).unwrap();
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(clock.now_ms(), 10);
            wait_for(|| wake.0.load(Ordering::Acquire) == 1);
            assert_eq!(wake.0.load(Ordering::Acquire), 1);
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
            assert_eq!(f.clean(0), Some(WriteOutcome::Complete));
        });
    }

    #[test]
    fn in_poll_advance_keeps_bounded_write_poll_nonwaiting_and_callback_free() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let f = fixture_on(port(), Arc::clone(&clock));
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut cx = Context::from_waker(&waker);
            clock.check_fixture_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            f.channel.step(WriteStep::Ready(Ok(()), Some(10)));
            let held = clock.base.lock().unwrap();
            let channel = Arc::clone(&f.channel);
            let (tx, rx) = std::sync::mpsc::channel();
            let handle = thread::spawn(move || {
                let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
                let mut cx = Context::from_waker(&waker);
                let mut write = Box::pin(channel.write_frame_bounded(frame(1)));
                tx.send(write.as_mut().poll(&mut cx)).unwrap();
            });
            let finished = rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok();
            assert_eq!(wake.0.load(Ordering::Acquire), 0);
            drop(held);
            handle.join().unwrap();
            assert!(
                finished,
                "bounded write poll waited for the base control lock"
            );
            wait_for(|| wake.0.load(Ordering::Acquire) == 1);
            wait_for(|| wake.0.load(Ordering::Acquire) == 1);
            assert_eq!(wake.0.load(Ordering::Acquire), 1);
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
        });
    }

    #[test]
    fn due_base_timer_wakes_while_writer_delivery_lock_is_held() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let mut f = fixture_on(port(), Arc::clone(&clock));
            f.channel.step(WriteStep::Pending);
            f.control.try_enqueue(frame(1), 10).unwrap();
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut cx = Context::from_waker(&waker);
            clock.check_fixture_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            let held = clock.writer_wake.lock().unwrap();
            let moving_clock = Arc::clone(&clock);
            let handle = thread::spawn(move || moving_clock.set(10));
            let start = std::time::Instant::now();
            while clock.registration_state.load(Ordering::Acquire) & 3 != 2
                && start.elapsed() < std::time::Duration::from_secs(1)
            {
                thread::yield_now();
            }
            let advanced =
                clock.now_ms() == 10 && clock.registration_state.load(Ordering::Acquire) & 3 == 2;
            wait_for(|| wake.0.load(Ordering::Acquire) == 1);
            let woke_before_writer_delivery = wake.0.load(Ordering::Acquire) == 1;
            drop(held);
            handle.join().unwrap();
            assert!(
                advanced,
                "clock did not advance while writer delivery was held"
            );
            assert!(
                woke_before_writer_delivery,
                "due base timer waited for the writer delivery lock"
            );
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
            f.control.request_reap();
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Reaped)
            );
            assert_eq!(f.clean(0), Some(WriteOutcome::Reaped));
        });
    }

    #[test]
    fn exact_clock_key_and_seat_survive_stale_due_event_and_reuse() {
        with_host(|factory| {
            let mut f = fixture(&factory, 1);
            f.channel.step(WriteStep::Pending);
            let old = f.control.try_enqueue(frame(1), 10).unwrap();
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            assert_eq!(f.clock.key(), Some(old));
            f.channel.allow(Ok(()));
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Complete)
            );
            assert_eq!(f.clock.key(), None);
            assert_eq!(f.clean(0), Some(WriteOutcome::Complete));
            f.channel.step(WriteStep::Pending);
            let fresh = f.control.try_enqueue(frame(2), 20).unwrap();
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            assert_eq!(f.clock.key(), Some(fresh));
            f.clock.set(11);
            assert!(!f.clock.fire(old));
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Idle);
            f.clock.set(20);
            assert!(f.clock.fire(fresh));
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::TimedOut)
            );
            assert_eq!(f.clock.key(), None);
            assert_eq!(f.clean(0), Some(WriteOutcome::TimedOut));
            assert_eq!(f.clock.removed.load(Ordering::Acquire), 2);
        });
    }

    #[test]
    fn old_due_key_cannot_expire_a_new_writer_generation() {
        with_host(|factory| {
            let port = port();
            let clock = factory.clock(1);
            let old_key = {
                let mut old = fixture_on(Arc::clone(&port), Arc::clone(&clock));
                old.channel.step(WriteStep::Pending);
                let key = old.control.try_enqueue(frame(1), 10).unwrap();
                assert_eq!(old.worker.dispatch_once(), DispatchResult::Pending);
                old.control.request_reap();
                assert_eq!(
                    old.worker.dispatch_once(),
                    DispatchResult::Retired(WriteOutcome::Reaped)
                );
                assert_eq!(old.clean(0), Some(WriteOutcome::Reaped));
                key
            };
            assert_eq!(clock.key(), None);
            let mut fresh = fixture_on(port, Arc::clone(&clock));
            fresh.channel.step(WriteStep::Pending);
            let fresh_key = fresh.control.try_enqueue(frame(2), 20).unwrap();
            assert_ne!(old_key, fresh_key);
            assert_eq!(fresh.worker.dispatch_once(), DispatchResult::Pending);
            clock.set(11);
            assert!(!clock.fire(old_key));
            assert_eq!(fresh.worker.dispatch_once(), DispatchResult::Idle);
            clock.set(20);
            assert!(clock.fire(fresh_key));
            assert_eq!(
                fresh.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::TimedOut)
            );
            assert_eq!(fresh.clean(0), Some(WriteOutcome::TimedOut));
            assert_eq!(clock.removed.load(Ordering::Acquire), 2);
        });
    }

    #[test]
    fn parked_and_running_stop_reap_on_worker_stack() {
        with_host(|factory| {
            let mut parked = fixture(&factory, 1);
            let j = parked.control.try_enqueue(frame(1), 100).unwrap();
            parked.control.request_reap();
            assert_eq!(
                parked.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Reaped)
            );
            assert_eq!(parked.channel.counts().0, 0);
            assert_eq!(parked.clean(0), Some(WriteOutcome::Reaped));
            assert_eq!(j.sequence, 1);

            let mut f = fixture(&factory, 1);
            f.channel.step(WriteStep::Pending);
            f.control.try_enqueue(frame(1), 100).unwrap();
            assert_eq!(f.worker.dispatch_once(), DispatchResult::Pending);
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let worker_barrier = Arc::clone(&barrier);
            let mut worker = f.worker;
            let handle = thread::spawn(move || {
                worker_barrier.wait(); // pause the worker outside the host poll
                worker.dispatch_once()
            });
            f.control.request_reap();
            barrier.wait();
            assert_eq!(
                handle.join().unwrap(),
                DispatchResult::Retired(WriteOutcome::Reaped)
            );
            assert_eq!(cleanup_one(&f.cleanup, 0), Some(WriteOutcome::Reaped));
            assert!(!f.channel.dropped_on_fixture_thread.load(Ordering::Acquire));
        });
    }

    #[test]
    fn owned_and_public_seats_exclude_each_other_across_generations() {
        with_host(|_factory| {
            let port = port();
            let old = port.try_reserve_route_owned().unwrap();
            let old_id = old.id();
            let borrowed = old.borrowed_view();
            let binding = old.try_reserve_binding().unwrap();
            assert!(port.try_reserve_binding(&borrowed).is_err());
            let writer = old.try_reserve_writer().unwrap();
            assert!(borrowed.try_reserve_writer().is_err());
            assert_eq!(old.try_reserve_writer().err(), Some(PortError::Exhausted));
            let raw_clock = borrowed.try_reserve_writer_clock().unwrap();
            assert!(old.writer_clock_seat().is_err());
            drop(raw_clock);
            let owned_clock = old.writer_clock_seat().unwrap();
            assert!(borrowed.try_reserve_writer_clock().is_err());
            drop(owned_clock);
            let ticket = old.try_reserve_drain().unwrap();
            assert!(borrowed.try_reserve_drain().is_err());
            let wake = writer.owned_waker();
            drop(writer);
            drop(borrowed);
            drop(old);
            let second = port.try_reserve_route_owned().unwrap();
            assert_ne!(second.id().slot(), old_id.slot());
            let second_writer = second.try_reserve_writer().unwrap();
            second_writer.signal_writer();
            let public = port.try_reserve_route().err();
            assert_eq!(public, Some(PortError::Exhausted));
            drop(binding);
            drop(ticket);
            drop(wake);
            let public = port.try_reserve_route().unwrap();
            assert_eq!(public.id().slot(), old_id.slot());
            assert!(public.id().generation() > old_id.generation());
            let raw_clock = public.try_reserve_writer_clock().unwrap();
            let owned_other_clock = second.writer_clock_seat().unwrap();
            assert!(public.try_reserve_writer_clock().is_err());
            assert!(second.writer_clock_seat().is_err());
            drop(raw_clock);
            drop(owned_other_clock);
            drop(second_writer);
            drop(public);
            drop(second);
            assert!(port.try_reserve_route_owned().is_ok());
        });
    }

    #[test]
    fn oversized_frame_and_channel_failure_preserve_or_retire_both_credits() {
        with_host(|factory| {
            let mut f = fixture(&factory, 1);
            let too_large = EncodedFrame {
                bytes: Vec::with_capacity(65),
                body_len: 0,
            };
            let rejected = f.control.try_enqueue(too_large, 100).unwrap_err();
            assert_eq!(rejected.0, QueueError::TooLarge);
            assert_eq!(rejected.1.bytes.capacity(), 65);
            assert_eq!(f.control.charged_capacity(), 0);
            f.channel
                .step(WriteStep::Ready(Err(ChannelError::Failed), None));
            f.control.try_enqueue(frame(1), 100).unwrap();
            f.control.try_enqueue(frame(2), 100).unwrap();
            assert_eq!(
                f.worker.dispatch_once(),
                DispatchResult::Retired(WriteOutcome::Channel(ChannelError::Failed))
            );
            assert_eq!(f.channel.counts().0, 1);
            assert_eq!(
                f.clean(0),
                Some(WriteOutcome::Channel(ChannelError::Failed))
            );
            assert_eq!(f.clean(1), Some(WriteOutcome::Reaped));
            assert_eq!(f.control.charged_capacity(), 0);
        });
    }

    struct TicketArmState {
        port: Arc<CriticalRoutePort>,
        key: DrainKey,
        claimed: AtomicBool,
        callback_finished: AtomicBool,
    }
    impl TicketArmState {
        fn claim(&self) -> bool {
            let won = self.port.tick(self.key);
            if won {
                self.claimed.store(true, Ordering::Release);
            }
            won
        }
        fn finish_callback(&self) {
            self.callback_finished.store(true, Ordering::Release);
        }
    }
    struct TicketRetireCell {
        arm: Arc<TicketArmState>,
        ticket: Mutex<Option<OwnedDrainTicket>>,
        submit_returned: AtomicBool,
        rejected: AtomicBool,
    }
    impl TicketRetireCell {
        fn new(port: Arc<CriticalRoutePort>, route: &OwnedRoute) -> Self {
            let ticket = route.try_reserve_drain().unwrap();
            let key = ticket.key();
            Self {
                arm: Arc::new(TicketArmState {
                    port,
                    key,
                    claimed: AtomicBool::new(false),
                    callback_finished: AtomicBool::new(false),
                }),
                ticket: Mutex::new(Some(ticket)),
                submit_returned: AtomicBool::new(false),
                rejected: AtomicBool::new(false),
            }
        }
        fn submit(&self, host: &impl DrainSubmitter) -> DrainArmResult {
            let mut guard = self.ticket.lock().unwrap().take().unwrap();
            let result = guard.submit(host, 100);
            self.rejected
                .store(result == DrainArmResult::Rejected, Ordering::Release);
            self.ticket.lock().unwrap().replace(guard);
            self.submit_returned.store(true, Ordering::Release);
            result
        }
        fn resolved(&self) -> bool {
            if !self.submit_returned.load(Ordering::Acquire) {
                return false;
            }
            if !self.arm.callback_finished.load(Ordering::Acquire)
                && !(self.rejected.load(Ordering::Acquire)
                    && !self.arm.claimed.load(Ordering::Acquire))
            {
                return false;
            }
            true
        }
        fn cleanup(&self) -> bool {
            if !self.resolved() {
                return false;
            }
            let ticket = self.ticket.lock().unwrap().take();
            let existed = ticket.is_some();
            drop(ticket);
            existed
        }
    }
    struct Reject;
    impl DrainSubmitter for Reject {
        fn submit_drain(
            &self,
            _: &CriticalRoutePort,
            _: DrainKey,
            _: RouteId,
            _: u64,
        ) -> HostArmResult {
            HostArmResult::Rejected
        }
    }
    struct Accept;
    impl DrainSubmitter for Accept {
        fn submit_drain(
            &self,
            _: &CriticalRoutePort,
            _: DrainKey,
            _: RouteId,
            _: u64,
        ) -> HostArmResult {
            HostArmResult::Accepted
        }
    }
    struct InlineReject(Arc<TicketArmState>);
    impl DrainSubmitter for InlineReject {
        fn submit_drain(
            &self,
            _: &CriticalRoutePort,
            _: DrainKey,
            _: RouteId,
            _: u64,
        ) -> HostArmResult {
            assert!(self.0.claim());
            HostArmResult::Rejected
        }
    }
    #[test]
    fn ticket_claim_and_callback_finish_are_distinct_and_stale_keys_fail() {
        with_host(|_factory| {
            let port = port();
            let route = port.try_reserve_route_owned().unwrap();
            let old_generation = route.id().generation();
            let rejected = TicketRetireCell::new(Arc::clone(&port), &route);
            let old = rejected.arm.key;
            assert_eq!(rejected.submit(&Reject), DrainArmResult::Rejected);
            assert!(!rejected.arm.claim());
            assert!(rejected.cleanup());
            let inline = TicketRetireCell::new(Arc::clone(&port), &route);
            assert_eq!(
                inline.submit(&InlineReject(Arc::clone(&inline.arm))),
                DrainArmResult::TickedInline
            );
            assert!(!inline.resolved());
            assert!(!inline.cleanup());
            inline.arm.finish_callback();
            assert!(inline.resolved());
            assert!(inline.cleanup());
            let due = TicketRetireCell::new(Arc::clone(&port), &route);
            assert_eq!(due.submit(&Accept), DrainArmResult::Armed);
            let claimed = Arc::new(std::sync::Barrier::new(2));
            let finish = Arc::new(std::sync::Barrier::new(2));
            let arm = Arc::clone(&due.arm);
            let claimed_callback = Arc::clone(&claimed);
            let finish_callback = Arc::clone(&finish);
            let callback = thread::spawn(move || {
                assert!(arm.claim());
                claimed_callback.wait();
                finish_callback.wait();
                arm.finish_callback();
            });
            claimed.wait(); // tick CAS won, callback work is still paused
            assert!(!due.resolved());
            assert!(!due.cleanup());
            finish.wait();
            callback.join().unwrap();
            assert!(due.resolved());
            assert!(due.cleanup());
            drop(route);
            let reused = port.try_reserve_route_owned().unwrap();
            assert!(!port.tick(old));
            assert_ne!(reused.id().generation(), old_generation);
        });
    }
    #[derive(Debug)]
    struct FullProgress {
        first: DispatchResult,
        first_deregistered: bool,
        second: DispatchResult,
        second_registered: bool,
        reaped: DispatchResult,
        final_deregistered: bool,
        removed: usize,
        charged: usize,
        cleanup: [Option<WriteOutcome>; 2],
        final_charge: usize,
        writes: usize,
        write_drops: usize,
    }
    fn full_progress(mut worker: Worker) -> FullProgress {
        let first = worker.dispatch_once();
        let first_deregistered = worker.control.adapter.clock.key().is_none();
        let second = worker.dispatch_once();
        let second_registered = worker.control.adapter.clock.key().is_some();
        worker.control.request_reap();
        let reaped = worker.dispatch_once();
        let final_deregistered = worker.control.adapter.clock.key().is_none();
        let removed = worker.control.adapter.clock.removed.load(Ordering::Acquire);
        let charged = worker.control.charged_capacity();
        let cleanup = array::from_fn(|slot| cleanup_one(&worker.cleanup, slot));
        FullProgress {
            first,
            first_deregistered,
            second,
            second_registered,
            reaped,
            final_deregistered,
            removed,
            charged,
            cleanup,
            final_charge: worker.control.charged_capacity(),
            writes: worker.control.adapter.channel.counts().0,
            write_drops: worker.control.adapter.channel.counts().2,
        }
    }
    fn assert_full_progress(progress: FullProgress) {
        assert_eq!(
            progress.first,
            DispatchResult::Retired(WriteOutcome::Complete)
        );
        assert!(progress.first_deregistered);
        assert_eq!(progress.second, DispatchResult::Pending);
        assert!(progress.second_registered);
        assert_eq!(
            progress.reaped,
            DispatchResult::Retired(WriteOutcome::Reaped)
        );
        assert!(progress.final_deregistered);
        assert_eq!(progress.removed, 2);
        assert_eq!(progress.charged, 128);
        assert_eq!(
            progress.cleanup,
            [Some(WriteOutcome::Complete), Some(WriteOutcome::Reaped)]
        );
        assert_eq!(progress.final_charge, 0);
        assert_eq!(progress.writes, 2);
        assert_eq!(progress.write_drops, 2);
    }
    #[test]
    fn full_static_j1_j2_reap_and_credit_cleanup_finish_with_base_held() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let f = fixture_on(port(), Arc::clone(&clock));
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            f.channel.step(WriteStep::Ready(Ok(()), Some(10)));
            f.channel.step(WriteStep::Pending);
            f.control.try_enqueue(frame(1), 20).unwrap();
            f.control.try_enqueue(frame(2), 20).unwrap();
            let held = clock.base.lock().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let worker = f.worker;
            let join = thread::spawn(move || tx.send(full_progress(worker)).unwrap());
            let before_release = rx.recv_timeout(std::time::Duration::from_secs(1));
            let wake_before_release = wake.0.load(Ordering::Acquire);
            drop(held);
            join.join().unwrap();
            assert_full_progress(
                before_release.expect("full composed writer waited for base line"),
            );
            assert_eq!(wake_before_release, 0);
            assert!(wait_for(|| wake.0.load(Ordering::Acquire) == 1));
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
        });
    }
    // Checked eligibility: no clock/root/host back-edge, finite non-unwinding
    // wake/clone/Drop after the external release; only atomics are captured.
    struct BlockingWake {
        entered: AtomicBool,
        release: AtomicBool,
        calls: AtomicUsize,
    }
    impl std::task::Wake for BlockingWake {
        fn wake(self: Arc<Self>) {
            self.entered.store(true, Ordering::Release);
            while !self.release.load(Ordering::Acquire) {
                thread::yield_now();
            }
            self.calls.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn blocking_wake() -> Arc<BlockingWake> {
        Arc::new(BlockingWake {
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        })
    }
    #[test]
    fn full_static_j1_j2_reap_and_credit_cleanup_finish_inside_entered_waker() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let f = fixture_on(port(), Arc::clone(&clock));
            let wake = blocking_wake();
            let waker = Waker::from(Arc::clone(&wake));
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            clock.set(10);
            let entered = wait_for(|| wake.entered.load(Ordering::Acquire));
            f.channel.step(WriteStep::Ready(Ok(()), Some(10)));
            f.channel.step(WriteStep::Pending);
            f.control.try_enqueue(frame(1), 20).unwrap();
            f.control.try_enqueue(frame(2), 20).unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let worker = f.worker;
            let join = thread::spawn(move || tx.send(full_progress(worker)).unwrap());
            let before_release = rx.recv_timeout(std::time::Duration::from_secs(1));
            let callback_was_held =
                !wake.release.load(Ordering::Acquire) && wake.calls.load(Ordering::Acquire) == 0;
            wake.release.store(true, Ordering::Release);
            join.join().unwrap();
            assert!(entered, "callback was not entered before writer dispatch");
            assert!(callback_was_held);
            assert_full_progress(
                before_release.expect("full composed writer waited for entered callback"),
            );
            assert!(wait_for(|| wake.calls.load(Ordering::Acquire) == 1));
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
        });
    }
    #[test]
    fn automatic_empty_now_bit_after_swap_and_after_scan_turns() {
        with_host(|factory| {
            let clock = factory.clock(1);
            let observer = Arc::clone(&clock.observer);
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            // A publication while a proved empty turn is paused.
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            observer.pause_empty.store(true, Ordering::Release);
            let empty = wait_for(|| observer.at_empty.load(Ordering::Acquire));
            clock.publish_advance(10);
            observer.pause_empty.store(false, Ordering::Release);
            assert!(empty);
            assert!(wait_for(|| wake.0.load(Ordering::Acquire) == 1));
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
            drop(sleep);
            assert!(wait_for(
                || clock.timers[0].phase.load(Ordering::Acquire) == 0
            ));
            // Control-only now/bit gap. Empty turns still maintain due timers.
            let mut sleep = clock.sleep_until(20);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            let turns = observer.empty_turns.load(Ordering::Acquire);
            clock.now.fetch_max(20, Ordering::AcqRel);
            let automatic = wait_for(|| {
                wake.0.load(Ordering::Acquire) == 2
                    && observer.empty_turns.load(Ordering::Acquire) > turns
            });
            clock.pending.store(true, Ordering::Release);
            assert!(automatic, "now/bit interval lost automatic maintenance");
            drop(sleep);
            assert!(wait_for(
                || clock.timers[0].phase.load(Ordering::Acquire) == 0
            ));
            // Publication after the service has consumed the bit.
            let mut sleep = clock.sleep_until(30);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            observer.pause_after_swap.store(true, Ordering::Release);
            let swapped = wait_for(|| observer.at_swap.load(Ordering::Acquire));
            clock.publish_advance(30);
            observer.pause_after_swap.store(false, Ordering::Release);
            assert!(swapped);
            assert!(wait_for(|| wake.0.load(Ordering::Acquire) == 3));
            drop(sleep);
            assert!(wait_for(
                || clock.timers[0].phase.load(Ordering::Acquire) == 0
            ));
            // A publication after this turn's scan requires another real turn.
            let mut sleep = clock.sleep_until(40);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            observer.pause_after_scan.store(true, Ordering::Release);
            let scanned = wait_for(|| observer.at_scan.load(Ordering::Acquire));
            clock.publish_advance(40);
            observer.pause_after_scan.store(false, Ordering::Release);
            assert!(scanned);
            assert!(wait_for(|| wake.0.load(Ordering::Acquire) == 4));
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
        });
    }
    #[test]
    fn joined_root_metadata_stays_charged_until_actual_state_drop_before_reuse() {
        with_host(|factory| {
            let (mut root, clock) = factory.setup(1).unwrap();
            let observer = Arc::clone(&clock.observer);
            assert_eq!(root.finish(), Finish::Busy);
            assert!(clock.client.load(Ordering::Acquire));
            let (second_root, second) = factory.setup(2).unwrap();
            drop(clock);
            assert!(wait_for(
                || observer.service_joins.load(Ordering::Acquire) == 1
            ));
            assert_eq!(observer.state_drops.load(Ordering::Acquire), 0);
            assert_eq!(observer.timer_drops.load(Ordering::Acquire), 0);
            assert_eq!(observer.debt.load(Ordering::Acquire), 2);
            assert!(matches!(factory.setup(3), Err(RunFailure::Full)));
            assert_eq!(root.finish(), Finish::Joined);
            assert!(root.receipt);
            assert!(wait_for(|| observer.state_drops.load(Ordering::Acquire)
                == 1
                && observer.timer_drops.load(Ordering::Acquire) == 2
                && observer.debt.load(Ordering::Acquire) == 1));
            let (third_root, third) = factory.setup(3).unwrap();
            assert_eq!(observer.admissions.load(Ordering::Acquire), 3);
            assert_eq!(observer.peak.load(Ordering::Acquire), 2);
            drop((second, second_root, third, third_root));
        });
    }
    #[test]
    fn static_client_and_sleep_outlive_early_root_fixture_and_body() {
        let observer = Arc::new(ClockObserver::default());
        let external_observer = Arc::clone(&observer);
        let (tx, rx) = std::sync::mpsc::sync_channel::<Arc<ScriptClock>>(1);
        // This caller owns the external actor Join from creation as well.
        let external = thread::spawn(move || {
            let clock = rx.recv().unwrap();
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            assert!(wait_for(|| external_observer
                .body_joins
                .load(Ordering::Acquire)
                == 1));
            assert_eq!(external_observer.service_joins.load(Ordering::Acquire), 0);
            assert_eq!(external_observer.debt.load(Ordering::Acquire), 1);
            clock.set(10);
            assert!(wait_for(|| wake.0.load(Ordering::Acquire) == 1));
            assert!(sleep.as_mut().poll(&mut cx).is_ready());
            drop(sleep);
            drop(clock);
        });
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let (mut root, clock) = factory.setup(1).unwrap();
            let f = fixture_on(port(), Arc::clone(&clock));
            assert_eq!(root.finish(), Finish::Busy);
            drop(root);
            drop(f);
            tx.send(clock).unwrap();
            Ok(())
        });
        external.join().unwrap();
        assert_eq!(report.failure, None);
        assert_collected(&observer, 1);
    }
    fn assert_collected(observer: &ClockObserver, admitted: usize) {
        assert_eq!(observer.admissions.load(Ordering::Acquire), admitted);
        assert_eq!(observer.service_joins.load(Ordering::Acquire), admitted);
        assert_eq!(observer.state_drops.load(Ordering::Acquire), admitted);
        assert_eq!(observer.timer_drops.load(Ordering::Acquire), 2 * admitted);
        assert_eq!(observer.body_joins.load(Ordering::Acquire), 1);
        assert_eq!(observer.control_drops.load(Ordering::Acquire), 1);
        assert_eq!(observer.debt.load(Ordering::Acquire), 0);
        assert!(!observer.early_debt.load(Ordering::Acquire));
    }
    #[test]
    fn last_sleep_client_and_cached_callback_wait_for_actual_join_and_destruction() {
        let observer = Arc::new(ClockObserver::default());
        let wake = blocking_wake();
        let release_wake = Arc::clone(&wake);
        let release_observer = Arc::clone(&observer);
        let releaser = thread::spawn(move || {
            let body_finished =
                wait_for(|| release_observer.body_joins.load(Ordering::Acquire) == 1);
            let retained = release_observer.cache.load(Ordering::Acquire) == 1
                && release_observer.service_joins.load(Ordering::Acquire) == 0
                && release_observer.state_drops.load(Ordering::Acquire) == 0
                && release_observer.timer_drops.load(Ordering::Acquire) == 0
                && release_observer.debt.load(Ordering::Acquire) == 1;
            release_wake.release.store(true, Ordering::Release);
            (body_finished, retained)
        });
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let (root, clock) = factory.setup(1).unwrap();
            let waker = Waker::from(Arc::clone(&wake));
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            clock.set(10);
            let entered = wait_for(|| wake.entered.load(Ordering::Acquire));
            // These final releases are fixed atomics while callback remains entered.
            drop(sleep);
            drop(clock);
            drop(root);
            assert!(entered);
            Ok(())
        });
        let (body_finished, retained) = releaser.join().unwrap();
        assert!(body_finished);
        assert!(retained, "cached callback was falsely treated as collected");
        assert_eq!(report.failure, None);
        assert_eq!(observer.cache_finished.load(Ordering::Acquire), 1);
        assert_collected(&observer, 1);
    }
    #[test]
    fn abandoned_setup_response_with_client_is_destroyed_before_collection_wait() {
        let observer = Arc::new(ClockObserver::default());
        let body_observer = Arc::clone(&observer);
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let wait = factory.begin(1, SetupMode::Normal).unwrap();
            assert!(wait_for(
                || body_observer.replies.load(Ordering::Acquire) == 1
            ));
            drop(wait); // the opaque response still contains its client/root
            assert!(
                wait_for(|| body_observer.service_joins.load(Ordering::Acquire) == 1
                    && body_observer.state_drops.load(Ordering::Acquire) == 1),
                "abandoned response retained its client until body close"
            );
            Ok(())
        });
        assert_eq!(report.failure, None);
        assert_eq!(observer.abandoned.load(Ordering::Acquire), 1);
        assert_collected(&observer, 1);
    }
    #[test]
    fn setup_rejection_and_failed_pair_collect_all_created_services() {
        let observer = Arc::new(ClockObserver::default());
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let pair = factory.setup(1).unwrap();
            assert!(matches!(
                factory.begin(2, SetupMode::Reject).unwrap().take(),
                Err(RunFailure::SetupRejected)
            ));
            assert!(matches!(
                factory.begin(2, SetupMode::FailedPair).unwrap().take(),
                Err(RunFailure::SetupRejected)
            ));
            drop(pair);
            Ok(())
        });
        assert_eq!(report.failure, Some(RunFailure::SetupRejected));
        assert_collected(&observer, 2);
    }
    #[test]
    fn failed_body_output_and_panic_payload_clients_are_dropped_before_waiting() {
        for panic in [false, true] {
            let observer = Arc::new(ClockObserver::default());
            let report = ClockHost::run(Arc::clone(&observer), move |factory| {
                let pair = factory.setup(1).unwrap();
                if panic {
                    std::panic::panic_any(pair);
                }
                Err(Box::new(pair))
            });
            assert_eq!(report.failure, Some(RunFailure::Body));
            assert_collected(&observer, 1);
        }
    }
    #[test]
    fn failed_service_join_keeps_live_client_state_and_drains_residual_callbacks() {
        let observer = Arc::new(ClockObserver::default());
        let body_observer = Arc::clone(&observer);
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let (root, clock) = factory.setup(1).unwrap();
            let wake = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            clock.panic_service.store(true, Ordering::Release);
            assert!(wait_for(|| body_observer
                .service_joins
                .load(Ordering::Acquire)
                == 1));
            assert_eq!(body_observer.debt.load(Ordering::Acquire), 1);
            assert_eq!(body_observer.state_drops.load(Ordering::Acquire), 0);
            assert_eq!(body_observer.timer_drops.load(Ordering::Acquire), 0);
            assert_eq!(wake.0.load(Ordering::Acquire), 0);
            drop(sleep);
            drop(clock);
            drop(root);
            Ok(())
        });
        assert_eq!(report.failure, Some(RunFailure::Service));
        assert_eq!(observer.service_exits.load(Ordering::Acquire), 0);
        assert_collected(&observer, 1);
    }
    #[test]
    fn actual_body_and_service_thread_rejection_collect_created_handles() {
        assert_eq!(
            usize::BITS,
            64,
            "selected native rejection fixture is 64-bit"
        );
        let observer = Arc::new(ClockObserver::default());
        let report =
            ClockHost::run_with_body_stack(Arc::clone(&observer), |_| Ok(()), Some(usize::MAX / 4));
        assert_eq!(report.failure, Some(RunFailure::ThreadRejected));
        assert_eq!(observer.body_joins.load(Ordering::Acquire), 0);
        assert_eq!(observer.control_drops.load(Ordering::Acquire), 1);
        assert_eq!(observer.admissions.load(Ordering::Acquire), 0);
        let observer = Arc::new(ClockObserver::default());
        let report = ClockHost::run(Arc::clone(&observer), |factory| {
            let pair = factory.setup(1).unwrap();
            assert!(matches!(
                factory.begin(2, SetupMode::ThreadRejected).unwrap().take(),
                Err(RunFailure::ThreadRejected)
            ));
            drop(pair);
            Ok(())
        });
        assert_eq!(report.failure, Some(RunFailure::ThreadRejected));
        assert_eq!(observer.admissions.load(Ordering::Acquire), 2);
        assert_eq!(observer.service_joins.load(Ordering::Acquire), 1);
        assert_eq!(observer.state_drops.load(Ordering::Acquire), 2);
        assert_eq!(observer.timer_drops.load(Ordering::Acquire), 4);
        assert_eq!(observer.body_joins.load(Ordering::Acquire), 1);
        assert_eq!(observer.control_drops.load(Ordering::Acquire), 1);
        assert_eq!(observer.debt.load(Ordering::Acquire), 0);
        assert!(!observer.early_debt.load(Ordering::Acquire));
    }
    struct UnwindingFailure {
        client: Arc<ScriptClock>,
    }
    impl Drop for UnwindingFailure {
        fn drop(&mut self) {
            std::panic::panic_any(FailureDropProbe {
                client: Some(Arc::clone(&self.client)),
                observer: Arc::clone(&self.client.observer),
            });
        }
    }
    #[test]
    fn secondary_failure_payload_unwind_keeps_collector_and_drains_client() {
        let observer = Arc::new(ClockObserver::default());
        let witness_observer = Arc::clone(&observer);
        let witness = thread::spawn(move || {
            let joined = wait_for(|| witness_observer.body_joins.load(Ordering::Acquire) == 1);
            let dropped = wait_for(|| witness_observer.failure_drops.load(Ordering::Acquire) == 1);
            witness_observer
                .failure_rescue
                .store(true, Ordering::Release);
            (joined, dropped)
        });
        let report = ClockHost::run(Arc::clone(&observer), |factory| {
            let (root, clock) = factory.setup(1).unwrap();
            drop(root);
            Err(Box::new(UnwindingFailure { client: clock }))
        });
        let (joined, dropped) = witness.join().unwrap();
        assert!(joined);
        assert!(
            dropped,
            "secondary opaque failure retained its client before drain"
        );
        assert_eq!(report.failure, Some(RunFailure::Body));
        assert_collected(&observer, 1);
    }
    #[test]
    fn body_unwind_with_held_base_line_still_collects_all_debt() {
        let observer = Arc::new(ClockObserver::default());
        let report = ClockHost::run(Arc::clone(&observer), |factory| {
            let clock = factory.clock(1);
            let _held = clock.base.lock().unwrap();
            panic!("private body failure with base line");
        });
        assert_eq!(report.failure, Some(RunFailure::Body));
        assert_eq!(observer.service_exits.load(Ordering::Acquire), 1);
        assert_collected(&observer, 1);
    }
    struct DropWake(Arc<BlockingWake>);
    impl std::task::Wake for DropWake {
        fn wake(self: Arc<Self>) {
            self.0.calls.fetch_add(1, Ordering::AcqRel);
        }
    }
    impl Drop for DropWake {
        fn drop(&mut self) {
            self.0.entered.store(true, Ordering::Release);
            while !self.0.release.load(Ordering::Acquire) {
                thread::yield_now();
            }
            self.0.calls.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn final_actual_waker_destruction_precedes_service_join_and_state_debt_zero() {
        let observer = Arc::new(ClockObserver::default());
        let flags = blocking_wake();
        let release_flags = Arc::clone(&flags);
        let release_observer = Arc::clone(&observer);
        let releaser = thread::spawn(move || {
            let entered = wait_for(|| release_flags.entered.load(Ordering::Acquire));
            let body_joined = wait_for(|| release_observer.body_joins.load(Ordering::Acquire) == 1);
            let retained = release_observer.service_joins.load(Ordering::Acquire) == 0
                && release_observer.state_drops.load(Ordering::Acquire) == 0
                && release_observer.timer_drops.load(Ordering::Acquire) == 0
                && release_observer.debt.load(Ordering::Acquire) == 1;
            release_flags.release.store(true, Ordering::Release);
            (entered, body_joined, retained)
        });
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let clock = factory.clock(1);
            let waker = Waker::from(Arc::new(DropWake(flags)));
            // DropWake captures only independent finite release atomics.
            clock.check_fixture_waker(&waker);
            let mut cx = Context::from_waker(&waker);
            let mut sleep = clock.sleep_until(10);
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
            drop(sleep);
            drop(waker);
            drop(clock);
            Ok(())
        });
        let (entered, body_joined, retained) = releaser.join().unwrap();
        assert!(entered);
        assert!(body_joined);
        assert!(retained);
        assert_eq!(report.failure, None);
        assert_collected(&observer, 1);
    }
    struct FailureDropProbe {
        client: Option<Arc<ScriptClock>>,
        observer: Arc<ClockObserver>,
    }
    impl Drop for FailureDropProbe {
        fn drop(&mut self) {
            assert_eq!(
                thread::current().id(),
                self.client.as_ref().unwrap().collector
            );
            drop(self.client.take());
            self.observer.failure_drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn opaque_failure_actual_drop_precedes_wait_for_service_client_zero() {
        let observer = Arc::new(ClockObserver::default());
        let witness_observer = Arc::clone(&observer);
        let witness = thread::spawn(move || {
            let body_joined = wait_for(|| witness_observer.body_joins.load(Ordering::Acquire) == 1);
            let dropped_before_wait =
                wait_for(|| witness_observer.failure_drops.load(Ordering::Acquire) == 1);
            // A control-only release for the causal wrong-order variant. This
            // never steps service and carries no client/state/Join capability.
            witness_observer
                .failure_rescue
                .store(true, Ordering::Release);
            (body_joined, dropped_before_wait)
        });
        let payload_observer = Arc::clone(&observer);
        let report = ClockHost::run(Arc::clone(&observer), move |factory| {
            let clock = factory.clock(1);
            Err(Box::new(FailureDropProbe {
                client: Some(clock),
                observer: payload_observer,
            }))
        });
        let (body_joined, dropped_before_wait) = witness.join().unwrap();
        assert!(body_joined);
        assert!(
            dropped_before_wait,
            "opaque failure retained client while collector waited for client0"
        );
        assert_eq!(report.failure, Some(RunFailure::Body));
        assert_collected(&observer, 1);
    }
}
