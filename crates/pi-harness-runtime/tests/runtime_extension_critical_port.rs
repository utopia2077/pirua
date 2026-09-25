#![cfg(feature = "extensions")]

use std::cell::RefCell;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use pi_ai_core::BoxFuture;
use pi_extension_protocol::wire::framing::EncodedFrame;
use pi_harness_runtime::extensions::critical_port::{
    CriticalRoutePort, DrainArmResult, DrainKey, DrainSubmitter, HostArmResult, NotificationKind,
    NotificationPost, NotificationPoster, PortError, PortLimits, RouteId, SeatLease,
};
use pi_harness_runtime::extensions::transport::{
    BoundedWriterChannel, ChannelError, ExactWriterClock, MonotonicClock, OrderedByteChannel,
    QuietClose, QuietOrderedByteChannel,
};

fn port(routes: usize, bindings: usize) -> CriticalRoutePort {
    CriticalRoutePort::try_new(PortLimits {
        routes,
        bindings,
        ordinary_timers_per_route: 1,
        tasks_per_route: 1,
        observers_per_route: 1,
    })
    .unwrap()
}

#[test]
fn finite_reservations_charge_overlapping_generations_and_cleanup() {
    let port = port(2, 1);
    let old = port.try_reserve_route().unwrap();
    let binding = port.try_reserve_binding(&old).unwrap();
    assert!(matches!(
        port.try_reserve_binding(&old),
        Err(PortError::Exhausted)
    ));

    let settings = old.try_reserve_settings().unwrap();
    let denial = old.try_reserve_denial_timer().unwrap();
    let ordinary = old.try_reserve_ordinary_timer().unwrap();
    let stop = old.try_reserve_task_stop().unwrap();
    let cleanup = old.try_reserve_task_cleanup().unwrap();
    let observer = old.try_reserve_observer().unwrap();
    assert!(matches!(
        old.try_reserve_settings(),
        Err(PortError::Exhausted)
    ));
    assert!(matches!(
        old.try_reserve_denial_timer(),
        Err(PortError::Exhausted)
    ));
    assert!(matches!(
        old.try_reserve_ordinary_timer(),
        Err(PortError::Exhausted)
    ));
    assert!(matches!(
        old.try_reserve_task_stop(),
        Err(PortError::Exhausted)
    ));
    assert!(matches!(
        old.try_reserve_task_cleanup(),
        Err(PortError::Exhausted)
    ));
    assert!(matches!(
        old.try_reserve_observer(),
        Err(PortError::Exhausted)
    ));

    let replacement = port.try_reserve_route().unwrap();
    assert_ne!(old.id(), replacement.id());
    assert!(matches!(
        port.try_reserve_route(),
        Err(PortError::Exhausted)
    ));
    drop((settings, denial, ordinary, stop, observer));
    assert!(matches!(
        port.try_reserve_route(),
        Err(PortError::Exhausted)
    ));
    drop(cleanup);
    drop(old);
    assert!(matches!(
        port.try_reserve_route(),
        Err(PortError::Exhausted)
    ));
    drop(binding);
    let reused = port.try_reserve_route().unwrap();
    assert_ne!(reused.id(), replacement.id());
    assert!(port.try_reserve_binding(&reused).is_ok());
}

#[test]
fn invalid_capacity_and_foreign_route_reject_before_claim() {
    assert!(matches!(
        CriticalRoutePort::try_new(PortLimits {
            routes: 1,
            bindings: usize::MAX,
            ordinary_timers_per_route: 1,
            tasks_per_route: 1,
            observers_per_route: 1,
        }),
        Err(PortError::InvalidLimits)
    ));
    let first = port(1, 1);
    let second = port(1, 1);
    let route = first.try_reserve_route().unwrap();
    assert!(matches!(
        second.try_reserve_binding(&route),
        Err(PortError::WrongPort)
    ));
    assert!(first.try_reserve_binding(&route).is_ok());
}

#[derive(Clone, Copy)]
enum DrainMode {
    Accept,
    Reject,
    TickThenReject,
}

struct FakeDrain {
    mode: DrainMode,
    calls: AtomicUsize,
    inline_wins: AtomicUsize,
}

impl FakeDrain {
    fn new(mode: DrainMode) -> Self {
        Self {
            mode,
            calls: AtomicUsize::new(0),
            inline_wins: AtomicUsize::new(0),
        }
    }
}

impl DrainSubmitter for FakeDrain {
    fn submit_drain(
        &self,
        port: &CriticalRoutePort,
        key: DrainKey,
        _route: RouteId,
        _absolute_deadline_ms: u64,
    ) -> HostArmResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            DrainMode::Accept => HostArmResult::Accepted,
            DrainMode::Reject => HostArmResult::Rejected,
            DrainMode::TickThenReject => {
                self.inline_wins
                    .store(usize::from(port.tick(key)), Ordering::SeqCst);
                HostArmResult::Rejected
            }
        }
    }
}

#[test]
fn drain_ticket_is_one_shot_and_rejection_fences_late_tick() {
    let port = port(1, 1);
    let route = port.try_reserve_route().unwrap();
    let mut ticket = route.try_reserve_drain().unwrap();
    let reject = FakeDrain::new(DrainMode::Reject);
    let key = ticket.key();
    assert!(!port.tick(key));
    assert_eq!(ticket.submit(&reject, 250), DrainArmResult::Rejected);
    assert_eq!(
        ticket.submit(&reject, 250),
        DrainArmResult::AlreadySubmitted
    );
    assert_eq!(reject.calls.load(Ordering::SeqCst), 1);
    assert!(!port.tick(key));
    drop(ticket);
    assert!(!port.tick(key));

    let mut ticket = route.try_reserve_drain().unwrap();
    let inline = FakeDrain::new(DrainMode::TickThenReject);
    let inline_key = ticket.key();
    assert_eq!(ticket.submit(&inline, 250), DrainArmResult::TickedInline);
    assert_eq!(inline.inline_wins.load(Ordering::SeqCst), 1);
    assert!(!port.tick(inline_key));
    drop(ticket);
    assert!(!port.tick(inline_key));

    let mut ticket = route.try_reserve_drain().unwrap();
    let accept = FakeDrain::new(DrainMode::Accept);
    let accepted_key = ticket.key();
    assert_eq!(ticket.submit(&accept, 250), DrainArmResult::Armed);
    assert!(!port.tick(inline_key));
    assert!(!port.tick(key));
    assert!(port.tick(accepted_key));
    assert!(!port.tick(accepted_key));
    drop(ticket);
    assert!(!port.tick(accepted_key));
}

struct ProbeWake {
    critical: Arc<AtomicBool>,
    wakes: AtomicUsize,
}

impl Wake for ProbeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            !self.critical.load(Ordering::SeqCst),
            "woke on critical stack"
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

struct FakeChannel {
    state: Mutex<ChannelState>,
}

struct ChannelState {
    closed: bool,
    read: Option<Waker>,
    write: Option<Waker>,
}

impl FakeChannel {
    fn new(read: Waker, write: Waker) -> Self {
        Self {
            state: Mutex::new(ChannelState {
                closed: false,
                read: Some(read),
                write: Some(write),
            }),
        }
    }

    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }
}

impl OrderedByteChannel for FakeChannel {
    fn max_read_scratch_bytes(&self) -> usize {
        0
    }

    fn read_chunk<'a>(
        &'a self,
        _buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
        Box::pin(async { Ok(None) })
    }

    fn write_frame(&self, _frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(async { Ok(()) })
    }

    fn close(&self) {
        // A pump Drop may repeat physical closure before the independent
        // notification lane receives the detached tokens.
        self.state.lock().unwrap().closed = true;
    }
}

impl QuietOrderedByteChannel for FakeChannel {
    fn close_quiet(&self) -> QuietClose {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        QuietClose::new(state.read.take(), state.write.take())
    }
}

impl BoundedWriterChannel for FakeChannel {
    type Write<'a> = Ready<Result<(), ChannelError>>;

    fn write_frame_bounded(&self, _frame: EncodedFrame) -> Self::Write<'_> {
        ready(Ok(()))
    }
}

struct FakePoster<'a> {
    queued: RefCell<[Option<NotificationPost<'a>>; 2]>,
}

impl<'a> FakePoster<'a> {
    fn new() -> Self {
        Self {
            queued: RefCell::new([None, None]),
        }
    }

    fn deliver_all(&self) {
        for slot in self.queued.borrow_mut().iter_mut() {
            if let Some(post) = slot.take() {
                post.deliver();
            }
        }
    }
}

impl<'a> NotificationPoster<'a> for FakePoster<'a> {
    fn post_notification(&self, post: NotificationPost<'a>) {
        let mut queued = self.queued.borrow_mut();
        let slot = queued.iter_mut().find(|slot| slot.is_none()).unwrap();
        *slot = Some(post);
    }
}

#[test]
fn quiet_close_detaches_once_and_posts_after_physical_close() {
    let port = port(1, 1);
    let route = port.try_reserve_route().unwrap();
    let critical = Arc::new(AtomicBool::new(true));
    let read = Arc::new(ProbeWake {
        critical: critical.clone(),
        wakes: AtomicUsize::new(0),
    });
    let write = Arc::new(ProbeWake {
        critical: critical.clone(),
        wakes: AtomicUsize::new(0),
    });
    let channel = FakeChannel::new(Waker::from(read.clone()), Waker::from(write.clone()));
    let read_seat = route
        .try_reserve_notification(NotificationKind::Read)
        .unwrap();
    let write_seat = route
        .try_reserve_notification(NotificationKind::Write)
        .unwrap();
    let posts = channel.close_quiet().into_posts(read_seat, write_seat);
    assert!(channel.is_closed());
    channel.close();
    channel.close();
    assert!(matches!(
        route.try_reserve_notification(NotificationKind::Read),
        Err(PortError::Exhausted)
    ));
    assert_eq!(read.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(write.wakes.load(Ordering::SeqCst), 0);
    let poster = FakePoster::new();
    for post in posts.into_iter().flatten() {
        poster.post_notification(post);
    }
    let repeat = channel.close_quiet();
    let read_seat = route.try_reserve_notification(NotificationKind::Read);
    assert!(matches!(read_seat, Err(PortError::Exhausted)));
    drop(repeat);
    critical.store(false, Ordering::SeqCst);
    poster.deliver_all();
    assert_eq!(read.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(write.wakes.load(Ordering::SeqCst), 1);
    assert!(
        route
            .try_reserve_notification(NotificationKind::Read)
            .is_ok()
    );
    assert!(
        route
            .try_reserve_notification(NotificationKind::Write)
            .is_ok()
    );
}

struct FakeClock {
    now: AtomicU64,
    active: AtomicBool,
    next_registration: AtomicU64,
    retired: AtomicU64,
    wake: Mutex<Option<Waker>>,
}

impl FakeClock {
    fn new() -> Self {
        Self {
            now: AtomicU64::new(0),
            active: AtomicBool::new(false),
            next_registration: AtomicU64::new(0),
            retired: AtomicU64::new(0),
            wake: Mutex::new(None),
        }
    }

    fn advance(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
        if let Some(wake) = self.wake.lock().unwrap().take() {
            wake.wake();
        }
    }
}

impl MonotonicClock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn sleep_until(&self, _deadline_ms: u64) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

struct FakeWriterSleep<'a> {
    clock: &'a FakeClock,
    _seat: SeatLease<'a>,
    registration: u64,
    deadline: u64,
}

impl Future for FakeWriterSleep<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.clock.now_ms() >= self.deadline {
            Poll::Ready(())
        } else {
            *self.clock.wake.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for FakeWriterSleep<'_> {
    fn drop(&mut self) {
        let mut wake = self.clock.wake.lock().unwrap();
        wake.take();
        assert_eq!(
            self.clock.next_registration.load(Ordering::SeqCst),
            self.registration
        );
        assert!(self.clock.active.swap(false, Ordering::SeqCst));
        self.clock.retired.fetch_add(1, Ordering::SeqCst);
    }
}

impl ExactWriterClock for FakeClock {
    type WriterSleep<'a> = FakeWriterSleep<'a>;

    fn writer_sleep_until<'a>(
        &'a self,
        seat: SeatLease<'a>,
        original_deadline_ms: u64,
    ) -> Self::WriterSleep<'a> {
        assert!(!self.active.swap(true, Ordering::SeqCst));
        let registration = self.next_registration.fetch_add(1, Ordering::SeqCst) + 1;
        FakeWriterSleep {
            clock: self,
            _seat: seat,
            registration,
            deadline: original_deadline_ms,
        }
    }
}

#[test]
fn owned_writer_waker_coalesces_and_clock_slot_recycles_exactly() {
    let port = port(1, 1);
    let route = port.try_reserve_route().unwrap();
    let writer = route.try_reserve_writer().unwrap();
    let wake = writer.owned_waker();
    assert_eq!(writer.signal_writer(), 1);
    assert_eq!(writer.signal_writer(), 2);
    let observed = writer.take_ready().unwrap();
    assert_eq!(observed, 2);
    assert!(!writer.changed_since(observed));
    wake.wake_by_ref();
    assert!(writer.changed_since(observed));
    assert_eq!(writer.take_ready(), Some(3));
    assert!(!writer.changed_since(3));

    let clock = FakeClock::new();
    for deadline in 1..=8 {
        let seat = route.try_reserve_writer_clock().unwrap();
        let mut sleep = clock.writer_sleep_until(seat, deadline);
        let mut cx = Context::from_waker(&wake);
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
        assert!(matches!(
            route.try_reserve_writer_clock(),
            Err(PortError::Exhausted)
        ));
        clock.advance(deadline);
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_ready());
        drop(sleep);
        assert!(!clock.active.load(Ordering::SeqCst));
        assert_eq!(clock.retired.load(Ordering::SeqCst), deadline);
    }
    drop(wake);
    drop(writer);
    drop(route);
    assert!(port.try_reserve_route().is_ok());
}

#[test]
fn outstanding_writer_waker_prevents_route_slot_reuse() {
    let port = port(1, 1);
    let route = port.try_reserve_route().unwrap();
    let writer = route.try_reserve_writer().unwrap();
    let wake = writer.owned_waker();
    drop(writer);
    drop(route);
    assert!(matches!(
        port.try_reserve_route(),
        Err(PortError::Exhausted)
    ));
    wake.wake_by_ref();
    drop(wake);
    assert!(port.try_reserve_route().is_ok());
}
