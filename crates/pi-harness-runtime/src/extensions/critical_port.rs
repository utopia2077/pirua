//! Opt-in, finite resources for a future first-route drain service.
//!
//! This port does not activate a route or run a drain decision. A host using it
//! must additionally attest that its channel, clock and independent role lanes
//! meet the contracts below. The generic spawner/channel path is not such an
//! attestation. All reservation, release, signal and ticket operations here are
//! fixed-capacity atomic work. `submit` crosses only the attested scalar host
//! boundary; no opaque future is invoked here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Wake, Waker};

const FREE: u8 = 0;
const RESERVED: u8 = 1;
const SUBMITTING: u8 = 2;
const ARMED: u8 = 3;
const TICKED: u8 = 4;
const REJECTED: u8 = 5;

fn word(incarnation: u64, state: u8) -> u64 {
    (incarnation << 8) | u64::from(state)
}

fn word_state(value: u64) -> u8 {
    value as u8
}

fn word_incarnation(value: u64) -> u64 {
    value >> 8
}

/// Capacity charged simultaneously to old and replacement generations.
#[derive(Clone, Copy, Debug)]
pub struct PortLimits {
    pub routes: usize,
    pub bindings: usize,
    pub ordinary_timers_per_route: usize,
    pub tasks_per_route: usize,
    pub observers_per_route: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortError {
    InvalidLimits,
    Allocation,
    Exhausted,
    WrongPort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteId {
    slot: usize,
    generation: u64,
}

impl RouteId {
    pub fn slot(self) -> usize {
        self.slot
    }

    pub fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Default)]
struct WriterSignal {
    epoch: AtomicU64,
    ready: AtomicBool,
}

impl WriterSignal {
    fn signal(&self) -> u64 {
        // The ready bit is the wake obligation. Epoch wrap cannot erase a
        // signal because only the sole writer clears that bit on take_ready.
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        self.ready.store(true, Ordering::Release);
        epoch
    }
}

impl Wake for WriterSignal {
    fn wake(self: Arc<Self>) {
        self.signal();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}

struct RouteSeat {
    refs: AtomicUsize,
    generation: AtomicU64,
    writer_signal: Arc<WriterSignal>,
    settings: AtomicBool,
    writer: AtomicBool,
    denial_timer: AtomicBool,
    writer_clock: AtomicBool,
    read_notification: AtomicBool,
    write_notification: AtomicBool,
    drain_word: AtomicU64,
    ordinary_timers: Vec<AtomicBool>,
    task_stops: Vec<AtomicBool>,
    task_cleanups: Vec<AtomicBool>,
    observers: Vec<AtomicBool>,
}

impl RouteSeat {
    fn new(limits: PortLimits) -> Result<Self, PortError> {
        Ok(Self {
            refs: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            writer_signal: Arc::new(WriterSignal::default()),
            settings: AtomicBool::new(false),
            writer: AtomicBool::new(false),
            denial_timer: AtomicBool::new(false),
            writer_clock: AtomicBool::new(false),
            read_notification: AtomicBool::new(false),
            write_notification: AtomicBool::new(false),
            drain_word: AtomicU64::new(word(0, FREE)),
            ordinary_timers: atomic_slots(limits.ordinary_timers_per_route)?,
            task_stops: atomic_slots(limits.tasks_per_route)?,
            task_cleanups: atomic_slots(limits.tasks_per_route)?,
            observers: atomic_slots(limits.observers_per_route)?,
        })
    }
}

fn atomic_slots(count: usize) -> Result<Vec<AtomicBool>, PortError> {
    let mut slots = Vec::new();
    slots
        .try_reserve_exact(count)
        .map_err(|_| PortError::Allocation)?;
    for _ in 0..count {
        slots.push(AtomicBool::new(false));
    }
    Ok(slots)
}

/// Resource ledger. Construct it before entering owner/settings/queue lines.
/// No live route may use this alone as proof that a generic host is conformant.
pub struct CriticalRoutePort {
    routes: Vec<RouteSeat>,
    bindings: Vec<AtomicBool>,
}

impl CriticalRoutePort {
    pub fn try_new(limits: PortLimits) -> Result<Self, PortError> {
        if limits.routes == 0 || limits.bindings == 0 {
            return Err(PortError::InvalidLimits);
        }
        // At most every binding can belong to one route. Seven fixed child
        // seats are settings, writer, denial timer, writer clock, two channel
        // notifications and the drain ticket. This bound makes every refs
        // increment finite even for an adversarial but accepted configuration.
        let refs_ceiling = limits
            .bindings
            .checked_add(8)
            .and_then(|value| value.checked_add(limits.ordinary_timers_per_route))
            .and_then(|value| value.checked_add(limits.tasks_per_route.checked_mul(2)?))
            .and_then(|value| value.checked_add(limits.observers_per_route));
        if refs_ceiling.is_none() {
            return Err(PortError::InvalidLimits);
        }
        let mut routes = Vec::new();
        routes
            .try_reserve_exact(limits.routes)
            .map_err(|_| PortError::Allocation)?;
        for _ in 0..limits.routes {
            routes.push(RouteSeat::new(limits)?);
        }
        Ok(Self {
            routes,
            bindings: atomic_slots(limits.bindings)?,
        })
    }

    /// A route slot remains charged while any child lease or owned writer Waker
    /// survives. Exhaustion must be handled before publishing a replacement.
    pub fn try_reserve_route(&self) -> Result<RouteLease<'_>, PortError> {
        for (index, seat) in self.routes.iter().enumerate() {
            if Arc::strong_count(&seat.writer_signal) != 1 {
                continue;
            }
            if seat
                .refs
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let Some(generation) = seat.generation.load(Ordering::Acquire).checked_add(1)
                else {
                    seat.refs.store(0, Ordering::Release);
                    continue;
                };
                seat.generation.store(generation, Ordering::Release);
                seat.writer_signal.epoch.store(0, Ordering::Release);
                seat.writer_signal.ready.store(false, Ordering::Release);
                return Ok(RouteLease {
                    port: self,
                    id: RouteId {
                        slot: index,
                        generation,
                    },
                });
            }
        }
        Err(PortError::Exhausted)
    }

    /// Claim one exact waiter before publishing its Request/ID.
    pub fn try_reserve_binding<'a>(
        &'a self,
        route: &RouteLease<'a>,
    ) -> Result<BindingLease<'a>, PortError> {
        if !std::ptr::eq(self, route.port) {
            return Err(PortError::WrongPort);
        }
        for slot in &self.bindings {
            if claim(slot) {
                self.routes[route.id.slot]
                    .refs
                    .fetch_add(1, Ordering::AcqRel);
                return Ok(BindingLease {
                    port: self,
                    route: route.id,
                    slot,
                });
            }
        }
        Err(PortError::Exhausted)
    }

    /// The host calls this at the due time, possibly inside `submit_drain`.
    /// Returning `true` transfers the sole tick decision to the caller. A
    /// rejected, released or stale ticket never ticks later.
    pub fn tick(&self, key: DrainKey) -> bool {
        let Some(seat) = self.routes.get(key.route.slot) else {
            return false;
        };
        if seat.generation.load(Ordering::Acquire) != key.route.generation {
            return false;
        }
        let mut current = seat.drain_word.load(Ordering::Acquire);
        loop {
            if word_incarnation(current) != key.incarnation
                || !matches!(word_state(current), SUBMITTING | ARMED)
            {
                return false;
            }
            match seat.drain_word.compare_exchange(
                current,
                word(key.incarnation, TICKED),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

fn claim(slot: &AtomicBool) -> bool {
    slot.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

#[must_use = "keep the route charged through all child work and cleanup"]
pub struct RouteLease<'a> {
    port: &'a CriticalRoutePort,
    id: RouteId,
}

impl RouteLease<'_> {
    pub fn id(&self) -> RouteId {
        self.id
    }

    fn seat(&self) -> &RouteSeat {
        &self.port.routes[self.id.slot]
    }

    fn reserve<'a>(&'a self, slot: &'a AtomicBool) -> Result<SeatLease<'a>, PortError> {
        if !claim(slot) {
            return Err(PortError::Exhausted);
        }
        self.seat().refs.fetch_add(1, Ordering::AcqRel);
        Ok(SeatLease {
            route: self.seat(),
            slot,
        })
    }

    pub fn try_reserve_settings(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve(&self.seat().settings)
    }

    pub fn try_reserve_writer(&self) -> Result<WriterLease<'_>, PortError> {
        Ok(WriterLease {
            seat: self.reserve(&self.seat().writer)?,
            signal: &self.seat().writer_signal,
        })
    }

    pub fn try_reserve_denial_timer(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve(&self.seat().denial_timer)
    }

    pub fn try_reserve_writer_clock(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve(&self.seat().writer_clock)
    }

    pub fn try_reserve_notification(
        &self,
        kind: NotificationKind,
    ) -> Result<SeatLease<'_>, PortError> {
        let slot = match kind {
            NotificationKind::Read => &self.seat().read_notification,
            NotificationKind::Write => &self.seat().write_notification,
        };
        self.reserve(slot)
    }

    pub fn try_reserve_ordinary_timer(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve_first(&self.seat().ordinary_timers)
    }

    pub fn try_reserve_task_stop(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve_first(&self.seat().task_stops)
    }

    pub fn try_reserve_task_cleanup(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve_first(&self.seat().task_cleanups)
    }

    pub fn try_reserve_observer(&self) -> Result<SeatLease<'_>, PortError> {
        self.reserve_first(&self.seat().observers)
    }

    fn reserve_first<'a>(&'a self, slots: &'a [AtomicBool]) -> Result<SeatLease<'a>, PortError> {
        for slot in slots {
            if let Ok(lease) = self.reserve(slot) {
                return Ok(lease);
            }
        }
        Err(PortError::Exhausted)
    }

    pub fn try_reserve_drain(&self) -> Result<DrainTicket<'_>, PortError> {
        let seat = self.seat();
        let current = seat.drain_word.load(Ordering::Acquire);
        if word_state(current) != FREE {
            return Err(PortError::Exhausted);
        }
        let incarnation = word_incarnation(current)
            .checked_add(1)
            .filter(|next| *next <= (u64::MAX >> 8))
            .ok_or(PortError::Exhausted)?;
        seat.drain_word
            .compare_exchange(
                current,
                word(incarnation, RESERVED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| PortError::Exhausted)?;
        seat.refs.fetch_add(1, Ordering::AcqRel);
        Ok(DrainTicket {
            port: self.port,
            key: DrainKey {
                route: self.id,
                incarnation,
            },
        })
    }
}

impl Drop for RouteLease<'_> {
    fn drop(&mut self) {
        self.seat().refs.fetch_sub(1, Ordering::AcqRel);
    }
}

#[must_use = "the binding keeps its route charged until exact retirement"]
pub struct BindingLease<'a> {
    port: &'a CriticalRoutePort,
    route: RouteId,
    slot: &'a AtomicBool,
}

impl Drop for BindingLease<'_> {
    fn drop(&mut self) {
        self.slot.store(false, Ordering::Release);
        self.port.routes[self.route.slot]
            .refs
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// A scalar reserved seat. It may be transferred to a role and is released
/// only when that role finishes its callback or destructor.
#[must_use = "retain the seat through its independent role"]
pub struct SeatLease<'a> {
    route: &'a RouteSeat,
    slot: &'a AtomicBool,
}

impl Drop for SeatLease<'_> {
    fn drop(&mut self) {
        self.slot.store(false, Ordering::Release);
        self.route.refs.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotificationKind {
    Read,
    Write,
}

/// Sole writer role. Its Waker only coalesces a scalar signal; the host must
/// poll the writer on an independent lane and recheck `changed_since` after
/// observing an empty queue. The port retains a base Arc until slot reuse.
pub struct WriterLease<'a> {
    seat: SeatLease<'a>,
    signal: &'a Arc<WriterSignal>,
}

impl WriterLease<'_> {
    pub fn signal_writer(&self) -> u64 {
        self.signal.signal()
    }

    pub fn owned_waker(&self) -> Waker {
        Waker::from(Arc::clone(self.signal))
    }

    pub fn take_ready(&self) -> Option<u64> {
        self.signal
            .ready
            .swap(false, Ordering::AcqRel)
            .then(|| self.signal.epoch.load(Ordering::Acquire))
    }

    pub fn changed_since(&self, epoch: u64) -> bool {
        self.signal.epoch.load(Ordering::Acquire) != epoch
            || self.signal.ready.load(Ordering::Acquire)
    }

    pub fn seat(&self) -> &SeatLease<'_> {
        &self.seat
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainKey {
    route: RouteId,
    incarnation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostArmResult {
    Accepted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainArmResult {
    Armed,
    TickedInline,
    Rejected,
    AlreadySubmitted,
}

/// Trusted host boundary: scalar, bounded, nonwaiting, callback-free except
/// that it may call `port.tick(key)` inline. It must not unwind. Rejection must
/// leave no scheduled later tick; the ticket also fences any violating late
/// call. An accepted ticket must remain owned until its callback finishes or
/// the host synchronously retires the exact timer. This external retirement
/// fact is a host attestation, not enforced by `DrainTicket`'s Rust type.
pub trait DrainSubmitter {
    fn submit_drain(
        &self,
        port: &CriticalRoutePort,
        key: DrainKey,
        route: RouteId,
        absolute_deadline_ms: u64,
    ) -> HostArmResult;
}

#[must_use = "submit or release this reserved drain timer exactly once"]
pub struct DrainTicket<'a> {
    port: &'a CriticalRoutePort,
    key: DrainKey,
}

impl DrainTicket<'_> {
    pub fn key(&self) -> DrainKey {
        self.key
    }

    pub fn submit(
        &mut self,
        host: &impl DrainSubmitter,
        absolute_deadline_ms: u64,
    ) -> DrainArmResult {
        let seat = &self.port.routes[self.key.route.slot];
        if seat
            .drain_word
            .compare_exchange(
                word(self.key.incarnation, RESERVED),
                word(self.key.incarnation, SUBMITTING),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return DrainArmResult::AlreadySubmitted;
        }
        let result = host.submit_drain(self.port, self.key, self.key.route, absolute_deadline_ms);
        let (next, result) = match result {
            HostArmResult::Accepted => (ARMED, DrainArmResult::Armed),
            HostArmResult::Rejected => (REJECTED, DrainArmResult::Rejected),
        };
        if seat
            .drain_word
            .compare_exchange(
                word(self.key.incarnation, SUBMITTING),
                word(self.key.incarnation, next),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            result
        } else {
            DrainArmResult::TickedInline
        }
    }
}

impl Drop for DrainTicket<'_> {
    fn drop(&mut self) {
        // The opt-in host must have finished/deregistered an accepted timer
        // before this release. No generic MonotonicClock contract proves it.
        let seat = &self.port.routes[self.key.route.slot];
        let mut current = seat.drain_word.load(Ordering::Acquire);
        while word_incarnation(current) == self.key.incarnation {
            match seat.drain_word.compare_exchange(
                current,
                word(self.key.incarnation, FREE),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        seat.refs.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One detached channel notification plus its pre-reserved independent post
/// seat. Constructing and posting it only moves ownership. `deliver` must run
/// on the notification role, never on the cutoff or inline-tick stack. Rust
/// still permits abandonment; the trusted host must not Drop it there.
#[must_use = "post this notification to its independent role"]
pub struct NotificationPost<'a> {
    wake: Waker,
    _seat: SeatLease<'a>,
}

impl<'a> NotificationPost<'a> {
    pub fn new(wake: Waker, seat: SeatLease<'a>) -> Self {
        Self { wake, _seat: seat }
    }

    pub fn deliver(self) {
        self.wake.wake_by_ref();
    }
}

/// Trusted independent notification lane. It must accept a reserved post in
/// bounded time without waking or dropping it inline.
pub trait NotificationPoster<'a> {
    fn post_notification(&self, post: NotificationPost<'a>);
}
