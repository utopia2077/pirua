//! Inert, exclusive caller backing for two readers. No live route uses it.
//!
//! A seat charges the whole fixed inventory, including explicitly unpriced
//! control storage. Known backing limits are not physical/whole-profile bounds.
//! Reservation precedes the separate constructor in source; compiled ordering,
//! heap/stack upper bounds and a trusted warmup observer remain unverified.
//! Box/Arc allocation failure may abort; ordinary rollback does not recover OOM.

use std::alloc::Layout;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use pi_extension_protocol::wire::echo_bounded::IndexStorage;
use pi_extension_protocol::wire::framing::RawFrameReader;

use super::index_pool::{IndexLease, IndexPool};

type TypedSlot = IndexStorage<4096, 4097>;
const BUNDLES: usize = 2;
const BODY: usize = 4096;
const READ: usize = 8192;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackingProfile {
    pub(super) frame: usize,
    pub(super) pre_ready: usize,
    pub(super) provisional: usize,
    pub(super) read: usize,
    pub(super) readers: usize,
}

pub(super) const PROFILE: BackingProfile = BackingProfile {
    frame: BODY,
    pre_ready: BODY,
    provisional: BODY,
    read: READ,
    readers: BUNDLES,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BackingError {
    Limit,
    ProfileMismatch,
    Occupied,
    Exhausted,
    UnreadTail,
    ConstructionInterrupted,
    ObservationMismatch,
}

/// Prices only the named typed/byte backing, never the whole profile.
#[derive(Clone, Copy)]
pub(super) struct KnownBackingLimits {
    pub(super) slot: usize,
    pub(super) total: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnpricedTerm {
    OriginalPoolControl,
    PairedArcControl,
    RootArcControlAndSeparatePrecharge,
    ConstructorHeapOverlap,
    StackUpperBoundAndCompiledAdmission,
    AllocatorBookkeepingAndPhysicalStorage,
}

/// Descriptive layouts; inline members must not be added again to heap charge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InlineLayouts {
    pub(super) original_pool_handle: usize,
    pub(super) index_lease: usize,
    pub(super) paired_mutex_value: usize,
    pub(super) root_mutex_value: usize,
    pub(super) owner: usize,
    pub(super) reader_lease: usize,
    pub(super) startup: usize,
    pub(super) hold: usize,
    pub(super) raw_reader: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Inventory {
    pub(super) profile: BackingProfile,
    pub(super) slot_bytes: usize,
    pub(super) typed_bytes: usize,
    pub(super) byte_boxes_bytes: usize,
    pub(super) known_backing_bytes: usize,
    pub(super) inline: InlineLayouts,
    pub(super) unpriced: [UnpricedTerm; 6],
}

impl Inventory {
    pub(super) fn checked(
        profile: BackingProfile,
        limits: KnownBackingLimits,
    ) -> Result<Self, BackingError> {
        let frame = NonZeroUsize::new(profile.frame).ok_or(BackingError::Limit)?;
        let slot_bytes =
            TypedSlot::checked_slot_bytes(frame, limits.slot).map_err(|_| BackingError::Limit)?;
        let typed_bytes = slot_bytes
            .checked_mul(profile.readers)
            .ok_or(BackingError::Limit)?;
        let byte_boxes_bytes = profile
            .pre_ready
            .checked_add(profile.provisional)
            .and_then(|n| n.checked_add(profile.read))
            .and_then(|n| n.checked_mul(profile.readers))
            .ok_or(BackingError::Limit)?;
        let known_backing_bytes = typed_bytes
            .checked_add(byte_boxes_bytes)
            .filter(|n| *n <= limits.total)
            .ok_or(BackingError::Limit)?;
        if profile != PROFILE {
            return Err(BackingError::ProfileMismatch);
        }
        Layout::array::<u8>(BODY).map_err(|_| BackingError::Limit)?;
        Layout::array::<u8>(READ).map_err(|_| BackingError::Limit)?;
        Ok(Self {
            profile,
            slot_bytes,
            typed_bytes,
            byte_boxes_bytes,
            known_backing_bytes,
            inline: InlineLayouts {
                original_pool_handle: Layout::new::<IndexPool>().size(),
                index_lease: Layout::new::<IndexLease>().size(),
                paired_mutex_value: Layout::new::<Mutex<PairState>>().size(),
                root_mutex_value: Layout::new::<Mutex<SeatState>>().size(),
                owner: Layout::new::<ReceiveBackingOwner>().size(),
                reader_lease: Layout::new::<ReaderBackingLease>().size(),
                startup: Layout::new::<StartupPermit>().size(),
                hold: Layout::new::<DebtHold>().size(),
                raw_reader: Layout::new::<RawFrameReader<'_>>().size(),
            },
            unpriced: [
                UnpricedTerm::OriginalPoolControl,
                UnpricedTerm::PairedArcControl,
                UnpricedTerm::RootArcControlAndSeparatePrecharge,
                UnpricedTerm::ConstructorHeapOverlap,
                UnpricedTerm::StackUpperBoundAndCompiledAdmission,
                UnpricedTerm::AllocatorBookkeepingAndPhysicalStorage,
            ],
        })
    }
}

#[derive(Default)]
struct SeatState {
    inventory: Option<Inventory>,
    holds: usize,
}

/// This control allocation has a separate inert precharge; it is not priced by
/// KnownBackingLimits. Holds retain it if the root handle drops early.
pub(super) struct BackingRoot {
    seat: Arc<Mutex<SeatState>>,
}

impl BackingRoot {
    pub(super) fn new() -> Self {
        Self {
            seat: Arc::new(Mutex::new(SeatState::default())),
        }
    }

    pub(super) fn prewarm(
        &self,
        profile: BackingProfile,
        limits: KnownBackingLimits,
    ) -> Result<ReceiveBackingOwner, BackingError> {
        self.prepare(profile, limits, Audit::default())
    }

    fn prepare(
        &self,
        profile: BackingProfile,
        limits: KnownBackingLimits,
        audit: Audit,
    ) -> Result<ReceiveBackingOwner, BackingError> {
        let startup = self.reserve(profile, limits, audit)?;
        construct(startup)
    }

    /// Small logical entry. No box construction or callback under this line.
    fn reserve(
        &self,
        profile: BackingProfile,
        limits: KnownBackingLimits,
        audit: Audit,
    ) -> Result<StartupPermit, BackingError> {
        let inventory = Inventory::checked(profile, limits)?;
        let mut seat = self.seat.lock().unwrap_or_else(|p| p.into_inner());
        if seat.inventory.is_some() {
            return Err(BackingError::Occupied);
        }
        seat.inventory = Some(inventory);
        seat.holds = 1;
        drop(seat);
        Ok(StartupPermit {
            hold: Some(DebtHold {
                seat: Arc::clone(&self.seat),
                audit,
            }),
            inventory,
        })
    }

    pub(super) fn debt(&self) -> (Option<Inventory>, usize) {
        let seat = self.seat.lock().unwrap_or_else(|p| p.into_inner());
        (seat.inventory, seat.holds)
    }
}

struct DebtHold {
    seat: Arc<Mutex<SeatState>>,
    audit: Audit,
}

impl DebtHold {
    fn try_for_reader(&self) -> Result<Self, BackingError> {
        let mut seat = self.seat.lock().unwrap_or_else(|p| p.into_inner());
        // A returned bundle may be visible while its old hold is still retiring.
        // That hold still consumes capacity; refuse before touching the bundle.
        if seat.inventory.is_none() || seat.holds > BUNDLES {
            return Err(BackingError::Exhausted);
        }
        seat.holds += 1;
        drop(seat);
        Ok(Self {
            seat: Arc::clone(&self.seat),
            audit: self.audit.clone(),
        })
    }
}

impl Drop for DebtHold {
    fn drop(&mut self) {
        let mut seat = self.seat.lock().unwrap_or_else(|p| p.into_inner());
        assert!(seat.holds > 0);
        seat.holds -= 1;
        let retired = seat.holds == 0;
        if retired {
            seat.inventory = None;
        }
        drop(seat);
        if retired {
            self.audit.record(Event::DebtRetired);
        }
    }
}

struct StartupPermit {
    hold: Option<DebtHold>,
    inventory: Inventory,
}

/// No certificate or production permit can be derived from this record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct UnverifiedWarmup {
    pub(super) inventory: Inventory,
}

// Field order releases partial storage before the startup hold on every exit.
struct Building {
    pool: Option<IndexPool>,
    bundles: [Option<Bundle>; BUNDLES],
    paired: Option<Arc<Mutex<PairState>>>,
    startup: StartupPermit,
}

/// Separate large construction path. This annotation is not a compiled stack
/// bound or proof that an optimizer preserves pre-admission in a final binary.
#[inline(never)]
fn construct(startup: StartupPermit) -> Result<ReceiveBackingOwner, BackingError> {
    let mut building = Building {
        pool: None,
        bundles: std::array::from_fn(|_| None),
        paired: None,
        startup,
    };
    let audit = building
        .startup
        .hold
        .as_ref()
        .expect("startup hold")
        .audit
        .clone();
    audit.observe(Event::BeforeConstruction)?;
    let inventory = building.startup.inventory;
    building.pool = Some(
        IndexPool::prewarm(inventory.slot_bytes, inventory.typed_bytes)
            .map_err(|_| BackingError::Limit)?,
    );
    audit.observe(Event::IndexPoolBuilt)?;
    for number in 0..BUNDLES {
        let index = building
            .pool
            .as_ref()
            .expect("pool built")
            .checkout(NonZeroUsize::new(BODY).unwrap())
            .map_err(|_| BackingError::Limit)?;
        building.bundles[number] = Some(Bundle {
            index,
            current: ByteBox::new(audit.clone())?,
            provisional: ProvisionalBody {
                bytes: ByteBox::new(audit.clone())?,
                len: 0,
            },
            read: ReadChunk {
                bytes: ByteBox::new(audit.clone())?,
                filled: 0,
                offset: 0,
            },
            _index_release: ReleaseNote {
                audit: audit.clone(),
                event: Event::BundleReleased,
            },
        });
    }
    // Both IndexLeases retain the unchanged pool after its handle disappears.
    drop(building.pool.take());
    let free = std::array::from_fn(|number| building.bundles[number].take());
    building.paired = Some(Arc::new(Mutex::new(PairState { free })));
    audit.observe(Event::ObservationReady)?;
    Ok(ReceiveBackingOwner {
        state: building.paired.take(),
        hold: building.startup.hold.take(),
        warmup: UnverifiedWarmup { inventory },
    })
}

struct ByteBox<const N: usize> {
    bytes: Box<[u8; N]>,
    _release: ReleaseNote,
}

impl<const N: usize> ByteBox<N> {
    fn new(audit: Audit) -> Result<Self, BackingError> {
        audit.observe(Event::ByteBoxRequested(N))?;
        let result = Self {
            bytes: Box::new([0; N]),
            _release: ReleaseNote {
                audit: audit.clone(),
                event: Event::ByteBoxReleased(N),
            },
        };
        audit.observe(Event::ByteBoxBuilt(N))?;
        Ok(result)
    }
}

pub(super) struct ProvisionalBody {
    bytes: ByteBox<BODY>,
    len: usize,
}

impl ProvisionalBody {
    pub(super) fn store(&mut self, body: &[u8]) -> Result<(), BackingError> {
        if body.len() > BODY {
            return Err(BackingError::Limit);
        }
        self.bytes.bytes[..body.len()].copy_from_slice(body);
        self.len = body.len();
        Ok(())
    }

    pub(super) fn body(&self) -> &[u8] {
        &self.bytes.bytes[..self.len]
    }
}

pub(super) struct ReadChunk {
    bytes: ByteBox<READ>,
    filled: usize,
    offset: usize,
}

impl ReadChunk {
    pub(super) fn fill(&mut self, chunk: &[u8]) -> Result<(), BackingError> {
        if self.offset != self.filled {
            return Err(BackingError::UnreadTail);
        }
        if chunk.len() > READ {
            return Err(BackingError::Limit);
        }
        self.bytes.bytes[..chunk.len()].copy_from_slice(chunk);
        self.filled = chunk.len();
        self.offset = 0;
        Ok(())
    }

    pub(super) fn tail(&self) -> &[u8] {
        &self.bytes.bytes[self.offset..self.filled]
    }

    pub(super) fn advance(&mut self, consumed: usize) -> Result<(), BackingError> {
        let offset = self
            .offset
            .checked_add(consumed)
            .filter(|n| *n <= self.filled)
            .ok_or(BackingError::Limit)?;
        self.offset = offset;
        Ok(())
    }

    pub(super) fn pending_read_buffer(&mut self) -> Result<&mut [u8], BackingError> {
        if self.offset != self.filled {
            return Err(BackingError::UnreadTail);
        }
        Ok(&mut self.bytes.bytes[..])
    }
}

struct Bundle {
    index: IndexLease,
    current: ByteBox<BODY>,
    provisional: ProvisionalBody,
    read: ReadChunk,
    // Runs after the actual index/byte fields have been destroyed.
    _index_release: ReleaseNote,
}

struct PairState {
    free: [Option<Bundle>; BUNDLES],
}

pub(super) struct ReceiveBackingOwner {
    state: Option<Arc<Mutex<PairState>>>,
    hold: Option<DebtHold>,
    warmup: UnverifiedWarmup,
}

impl ReceiveBackingOwner {
    pub(super) fn warmup(&self) -> UnverifiedWarmup {
        self.warmup
    }

    pub(super) fn try_lease(
        &self,
        profile: BackingProfile,
        selected: NonZeroUsize,
    ) -> Result<ReaderBackingLease, BackingError> {
        if profile != self.warmup.inventory.profile {
            return Err(BackingError::ProfileMismatch);
        }
        TypedSlot::checked_slot_bytes(selected, self.warmup.inventory.slot_bytes)
            .map_err(|_| BackingError::Limit)?;
        // Reserve the hold before taking a bundle. Controller and paired lines
        // are never held together, including the no-bundle rollback path.
        let hold = self.hold.as_ref().expect("owner hold").try_for_reader()?;
        let state_reference = Arc::clone(self.state.as_ref().expect("owner backing"));
        let mut state = state_reference.lock().unwrap_or_else(|p| p.into_inner());
        let available = state
            .free
            .iter_mut()
            .enumerate()
            .find_map(|(number, free)| free.take().map(|backing| (number, backing)));
        let (number, mut backing) = match available {
            Some(pair) => pair,
            None => {
                drop(state);
                drop(state_reference);
                #[cfg(test)]
                hold.audit.record(Event::ReaderReservationRolledBack);
                drop(hold);
                return Err(BackingError::Exhausted);
            }
        };
        backing.provisional.len = 0;
        backing.read.filled = 0;
        backing.read.offset = 0;
        drop(state);
        Ok(ReaderBackingLease {
            state: Some(state_reference),
            backing: Some(backing),
            number,
            selected,
            hold: Some(hold),
        })
    }
}

impl Drop for ReceiveBackingOwner {
    fn drop(&mut self) {
        // Arc control deallocation completes before the separate debt hold.
        drop(self.state.take());
        self.hold
            .as_ref()
            .expect("owner hold")
            .audit
            .record(Event::BackingReferenceReleased);
        drop(self.hold.take());
    }
}

pub(super) struct ReaderBackingLease {
    state: Option<Arc<Mutex<PairState>>>,
    backing: Option<Bundle>,
    number: usize,
    selected: NonZeroUsize,
    hold: Option<DebtHold>,
}

pub(super) struct ReaderParts<'a> {
    pub(super) index: &'a mut TypedSlot,
    pub(super) current: &'a mut [u8],
    pub(super) provisional: &'a mut ProvisionalBody,
    pub(super) read: &'a mut ReadChunk,
}

impl ReaderBackingLease {
    pub(super) fn selected_frame(&self) -> NonZeroUsize {
        self.selected
    }

    pub(super) fn parts(&mut self) -> ReaderParts<'_> {
        let bundle = self.backing.as_mut().expect("complete bundle");
        ReaderParts {
            index: bundle.index.storage_mut(),
            current: &mut bundle.current.bytes[..],
            provisional: &mut bundle.provisional,
            read: &mut bundle.read,
        }
    }
}

impl Drop for ReaderBackingLease {
    fn drop(&mut self) {
        if let Some(backing) = self.backing.take() {
            let mut state = self
                .state
                .as_ref()
                .expect("lease backing")
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if state.free[self.number].is_none() {
                state.free[self.number] = Some(backing);
            } else {
                // Safe checkout cannot reach this branch. Still destroy the
                // unexpected bundle outside the line, never under the mutex.
                drop(state);
                drop(backing);
            }
        }
        drop(self.state.take());
        self.hold
            .as_ref()
            .expect("reader hold")
            .audit
            .record(Event::BackingReferenceReleased);
        drop(self.hold.take());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    BeforeConstruction,
    IndexPoolBuilt,
    ByteBoxRequested(usize),
    ByteBoxBuilt(usize),
    ObservationReady,
    ByteBoxReleased(usize),
    BundleReleased,
    BackingReferenceReleased,
    #[cfg(test)]
    ReaderReservationRolledBack,
    DebtRetired,
}

// Test-only observations can fail/inspect the real path; no caller observer
// exists in production, and none of these records can mint certification.
#[cfg(test)]
type TestObservation = dyn Fn(Event) -> Result<(), BackingError> + Send + Sync;

#[derive(Clone, Default)]
struct Audit {
    #[cfg(test)]
    callback: Option<Arc<TestObservation>>,
}

impl Audit {
    fn observe(&self, _event: Event) -> Result<(), BackingError> {
        #[cfg(test)]
        if let Some(callback) = &self.callback {
            return callback(_event);
        }
        Ok(())
    }

    fn record(&self, event: Event) {
        // Drop cannot propagate an injected ordinary error; a test assertion
        // may still expose bad destruction order by unwinding.
        let _ = self.observe(event);
    }
}

struct ReleaseNote {
    audit: Audit,
    event: Event,
}

impl Drop for ReleaseNote {
    fn drop(&mut self) {
        self.audit.record(self.event);
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use pi_extension_protocol::wire::echo_bounded::ScanError;

    use super::*;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn limits() -> KnownBackingLimits {
        let slot = Layout::new::<TypedSlot>().size();
        KnownBackingLimits {
            slot,
            total: slot * BUNDLES + BUNDLES * (BODY + BODY + READ),
        }
    }

    fn on_stack(test: impl FnOnce() + Send + 'static) {
        // This fixture stack permits large safe construction; it is not a
        // production stack bound or a certified construction-peak observation.
        let (done, completion) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(test));
                done.send(result).unwrap();
            })
            .unwrap();
        let result = completion
            .recv_timeout(Duration::from_secs(20))
            .expect("bounded, non-deadlocking backing fixture");
        thread.join().unwrap();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }

    #[test]
    fn whole_inventory_limits_and_profiles_refuse_before_construction() {
        on_stack(|| {
            let root = BackingRoot::new();
            let attempts = Arc::new(AtomicUsize::new(0));
            let count = attempts.clone();
            let audit = Audit {
                callback: Some(Arc::new(move |event| {
                    if event == Event::BeforeConstruction {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(())
                })),
            };
            let exact = limits();
            for rejected in [
                KnownBackingLimits {
                    slot: exact.slot - 1,
                    ..exact
                },
                KnownBackingLimits {
                    total: exact.total - 1,
                    ..exact
                },
            ] {
                assert!(matches!(
                    root.prepare(PROFILE, rejected, audit.clone()),
                    Err(BackingError::Limit)
                ));
                assert_eq!(root.debt(), (None, 0));
            }
            for profile in [
                BackingProfile {
                    read: READ - 1,
                    ..PROFILE
                },
                BackingProfile {
                    pre_ready: usize::MAX,
                    ..PROFILE
                },
                BackingProfile {
                    readers: usize::MAX,
                    ..PROFILE
                },
                BackingProfile {
                    frame: 4097,
                    ..PROFILE
                },
            ] {
                assert!(root.prepare(profile, exact, audit.clone()).is_err());
                assert_eq!(root.debt(), (None, 0));
            }
            assert_eq!(attempts.load(Ordering::SeqCst), 0);
            let owner = root.prepare(PROFILE, exact, audit).unwrap();
            let inventory = owner.warmup().inventory;
            assert_eq!(inventory.known_backing_bytes, exact.total);
            assert_eq!(inventory.byte_boxes_bytes, 32768);
            assert_eq!(inventory.unpriced.len(), 6);
            assert_eq!(root.debt(), (Some(inventory), 1));
            assert!(matches!(
                root.prewarm(PROFILE, exact),
                Err(BackingError::Occupied)
            ));
            for selected in [1, 137, 4096] {
                let mut lease = owner.try_lease(PROFILE, nz(selected)).unwrap();
                assert_eq!(lease.selected_frame(), nz(selected));
                let parts = lease.parts();
                assert_eq!(
                    parts
                        .index
                        .borrow(nz(selected), exact.slot)
                        .unwrap()
                        .slot_bytes(),
                    exact.slot
                );
                assert_eq!(parts.current.len(), BODY);
                assert_eq!(parts.read.pending_read_buffer().unwrap().len(), READ);
                assert_eq!(root.debt(), (Some(inventory), 2));
            }
            assert!(matches!(
                owner.try_lease(PROFILE, nz(4097)),
                Err(BackingError::Limit)
            ));
            assert!(matches!(
                owner.try_lease(PROFILE, nz(usize::MAX)),
                Err(BackingError::Limit)
            ));
            assert!(matches!(
                owner.try_lease(BackingProfile { read: 1, ..PROFILE }, nz(1)),
                Err(BackingError::ProfileMismatch)
            ));
            assert_eq!(root.debt(), (Some(inventory), 1));
            drop(owner);
            assert_eq!(root.debt(), (None, 0));
        });
    }

    #[test]
    fn complete_pairs_are_exclusive_and_exhaustion_does_not_allocate_or_damage() {
        on_stack(|| {
            let root = BackingRoot::new();
            let attempts = Arc::new(AtomicUsize::new(0));
            let count = attempts.clone();
            let audit = Audit {
                callback: Some(Arc::new(move |event| {
                    if matches!(
                        event,
                        Event::BeforeConstruction | Event::ByteBoxRequested(_)
                    ) {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(())
                })),
            };
            let owner = root.prepare(PROFILE, limits(), audit).unwrap();
            let warm_requests = attempts.load(Ordering::SeqCst);
            assert_eq!(warm_requests, 7);
            let mut first = owner.try_lease(PROFILE, nz(137)).unwrap();
            let mut second = owner.try_lease(PROFILE, nz(4096)).unwrap();
            let first_slot = first.parts().index as *mut TypedSlot as usize;
            let second_slot = second.parts().index as *mut TypedSlot as usize;
            assert_ne!(first_slot, second_slot);
            first.parts().current[0] = 17;
            second.parts().current[0] = 29;
            assert!(matches!(
                owner.try_lease(PROFILE, nz(1)),
                Err(BackingError::Exhausted)
            ));
            assert_eq!(first.parts().current[0], 17);
            assert_eq!(second.parts().current[0], 29);
            assert_eq!(attempts.load(Ordering::SeqCst), warm_requests);
            assert_eq!(root.debt().1, 3);
            drop(first);
            let mut reused = owner.try_lease(PROFILE, nz(137)).unwrap();
            assert_eq!(reused.parts().index as *mut TypedSlot as usize, first_slot);
            let mut arena = reused.parts().index.borrow(nz(137), limits().slot).unwrap();
            assert!(matches!(
                arena.scan_envelope(&[0]),
                Err(ScanError::Malformed)
            ));
            assert_eq!(arena.scan_envelope(&[0x2a, 0]).unwrap().arm(), Some(5));
            assert_eq!(attempts.load(Ordering::SeqCst), warm_requests);
        });
    }

    #[test]
    fn raw_fragmentation_coalesced_tail_and_provisional_overlap_use_real_backing() {
        on_stack(|| {
            let root = BackingRoot::new();
            let owner = root.prewarm(PROFILE, limits()).unwrap();
            let mut lease = owner.try_lease(PROFILE, nz(137)).unwrap();
            let parts = lease.parts();
            let mut raw = RawFrameReader::new(nz(BODY), parts.current).unwrap();
            parts.read.fill(&[2, 0x2a]).unwrap();
            let step = raw.read_one(parts.read.tail()).unwrap();
            assert!(step.frame.is_none());
            parts.read.advance(step.consumed).unwrap();
            parts.read.fill(&[0, 2, 0x32, 0]).unwrap();
            let step = raw.read_one(parts.read.tail()).unwrap();
            parts.read.advance(step.consumed).unwrap();
            let frame = step.frame.unwrap();
            assert_eq!(frame.body, &[0x2a, 0]);
            parts.provisional.store(frame.body).unwrap();
            {
                let mut arena = parts.index.borrow(nz(137), limits().slot).unwrap();
                assert_eq!(arena.scan_envelope(frame.body).unwrap().arm(), Some(5));
            }
            assert_eq!(parts.read.tail(), &[2, 0x32, 0]);
            assert_eq!(parts.read.fill(&[]), Err(BackingError::UnreadTail));
            raw.use_negotiated_limit(nz(137)).unwrap();
            let next = raw.read_one(parts.read.tail()).unwrap();
            parts.read.advance(next.consumed).unwrap();
            assert_eq!(next.frame.unwrap().body, &[0x32, 0]);
            assert_eq!(parts.provisional.body(), &[0x2a, 0]);
            assert!(parts.read.tail().is_empty());
            assert_eq!(parts.read.advance(usize::MAX), Err(BackingError::Limit));
            assert_eq!(
                parts.provisional.store(&[0; BODY + 1]),
                Err(BackingError::Limit)
            );
            assert_eq!(parts.provisional.body(), &[0x2a, 0]);
            raw.finish().unwrap();
        });
    }

    fn watched(root: Arc<BackingRoot>, events: Arc<Mutex<Vec<Event>>>) -> Audit {
        Audit {
            callback: Some(Arc::new(move |event| {
                // Reentrant locking is intentional: these observations and final
                // destruction must run outside the seat/pool lines.
                let (inventory, holds) = root.debt();
                if event == Event::DebtRetired {
                    assert!(inventory.is_none());
                    assert_eq!(holds, 0);
                    let events = events.lock().unwrap();
                    assert_eq!(
                        events
                            .iter()
                            .filter(|e| matches!(e, Event::ByteBoxReleased(_)))
                            .count(),
                        6
                    );
                    assert_eq!(
                        events
                            .iter()
                            .filter(|e| **e == Event::BundleReleased)
                            .count(),
                        2
                    );
                    assert!(events.contains(&Event::BackingReferenceReleased));
                } else {
                    assert!(
                        inventory.is_some(),
                        "whole debt lives at actual storage event {event:?}"
                    );
                    assert!(holds > 0);
                }
                events.lock().unwrap().push(event);
                Ok(())
            })),
        }
    }

    #[test]
    fn storage_and_arc_release_precede_final_debt_after_early_owner_drop() {
        on_stack(|| {
            let root = Arc::new(BackingRoot::new());
            let events = Arc::new(Mutex::new(Vec::new()));
            let owner = root
                .prepare(PROFILE, limits(), watched(root.clone(), events.clone()))
                .unwrap();
            let first = owner.try_lease(PROFILE, nz(1)).unwrap();
            let second = owner.try_lease(PROFILE, nz(137)).unwrap();
            drop(owner);
            assert_eq!(root.debt().1, 2);
            assert!(matches!(
                root.prewarm(PROFILE, limits()),
                Err(BackingError::Occupied)
            ));
            drop(first);
            assert_eq!(root.debt().1, 1);
            assert!(!events.lock().unwrap().contains(&Event::DebtRetired));
            drop(second);
            assert_eq!(root.debt(), (None, 0));
            assert_eq!(events.lock().unwrap().last(), Some(&Event::DebtRetired));
        });
    }

    #[test]
    fn return_observation_reenters_pair_and_controller_outside_both_lines() {
        on_stack(|| {
            let root = Arc::new(BackingRoot::new());
            let access = Arc::new(Mutex::new(None::<Arc<ReceiveBackingOwner>>));
            let recovered = Arc::new(Mutex::new(None::<ReaderBackingLease>));
            let once = Arc::new(AtomicBool::new(false));
            let observed_root = root.clone();
            let observed_access = access.clone();
            let observed_recovered = recovered.clone();
            let observed_once = once.clone();
            let audit = Audit {
                callback: Some(Arc::new(move |event| {
                    let debt = observed_root.debt();
                    assert_eq!(debt.0.is_none(), event == Event::DebtRetired);
                    if event == Event::BackingReferenceReleased
                        && !observed_once.swap(true, Ordering::SeqCst)
                    {
                        let owner = observed_access.lock().unwrap().as_ref().cloned();
                        let lease = owner
                            .expect("temporary fixture owner handle")
                            .try_lease(PROFILE, nz(137))
                            .unwrap();
                        *observed_recovered.lock().unwrap() = Some(lease);
                    }
                    Ok(())
                })),
            };
            let owner = Arc::new(root.prepare(PROFILE, limits(), audit).unwrap());
            *access.lock().unwrap() = Some(owner.clone());
            let first = owner.try_lease(PROFILE, nz(1)).unwrap();
            drop(first);
            assert!(once.load(Ordering::SeqCst));
            assert_eq!(root.debt().1, 2);
            // Break the temporary fixture reference before final destruction;
            // no backing/control Arc or Weak is exposed by the owner API.
            *access.lock().unwrap() = None;
            let lease = recovered.lock().unwrap().take().unwrap();
            drop(owner);
            assert_eq!(root.debt().1, 1);
            drop(lease);
            assert_eq!(root.debt(), (None, 0));
        });
    }

    #[test]
    fn returned_pair_waits_for_its_old_hold_when_another_reader_is_live() {
        on_stack(|| {
            let root = Arc::new(BackingRoot::new());
            let access = Arc::new(Mutex::new(None::<Arc<ReceiveBackingOwner>>));
            let once = Arc::new(AtomicBool::new(false));
            let observed_root = root.clone();
            let observed_access = access.clone();
            let observed_once = once.clone();
            let audit = Audit {
                callback: Some(Arc::new(move |event| {
                    if event == Event::BackingReferenceReleased
                        && !observed_once.swap(true, Ordering::SeqCst)
                    {
                        assert_eq!(observed_root.debt().1, 3);
                        let owner = observed_access.lock().unwrap().as_ref().cloned().unwrap();
                        assert!(matches!(
                            owner.try_lease(PROFILE, nz(137)),
                            Err(BackingError::Exhausted)
                        ));
                        assert_eq!(observed_root.debt().1, 3);
                    }
                    Ok(())
                })),
            };
            let owner = Arc::new(root.prepare(PROFILE, limits(), audit).unwrap());
            *access.lock().unwrap() = Some(owner.clone());
            let mut first = owner.try_lease(PROFILE, nz(137)).unwrap();
            let mut second = owner.try_lease(PROFILE, nz(137)).unwrap();
            let first_slot = first.parts().index as *mut TypedSlot as usize;
            second.parts().current[0] = 31;
            drop(first);
            assert!(once.load(Ordering::SeqCst));
            assert_eq!(root.debt().1, 2);
            assert_eq!(second.parts().current[0], 31);
            let mut reused = owner.try_lease(PROFILE, nz(137)).unwrap();
            assert_eq!(reused.parts().index as *mut TypedSlot as usize, first_slot);
            assert_eq!(root.debt().1, 3);
            *access.lock().unwrap() = None;
            drop(reused);
            drop(second);
            drop(owner);
            assert_eq!(root.debt(), (None, 0));
        });
    }

    #[test]
    fn empty_pool_rolls_back_reserved_hold_with_both_lines_reentrant() {
        on_stack(|| {
            let root = Arc::new(BackingRoot::new());
            let access = Arc::new(Mutex::new(None::<Arc<ReceiveBackingOwner>>));
            let once = Arc::new(AtomicBool::new(false));
            let observed_root = root.clone();
            let observed_access = access.clone();
            let observed_once = once.clone();
            let audit = Audit {
                callback: Some(Arc::new(move |event| {
                    if event == Event::ReaderReservationRolledBack
                        && !observed_once.swap(true, Ordering::SeqCst)
                    {
                        assert_eq!(observed_root.debt().1, 2);
                        let owner = observed_access.lock().unwrap().as_ref().cloned().unwrap();
                        assert!(matches!(
                            owner.try_lease(PROFILE, nz(137)),
                            Err(BackingError::Exhausted)
                        ));
                        assert_eq!(observed_root.debt().1, 2);
                    }
                    Ok(())
                })),
            };
            let owner = Arc::new(root.prepare(PROFILE, limits(), audit).unwrap());
            *access.lock().unwrap() = Some(owner.clone());
            // Controlled private empty-pool fault: the actual boxes remain
            // owned here and covered by the owner's whole-inventory seat.
            let mut removed: [Option<Bundle>; BUNDLES] = {
                let mut state = owner.state.as_ref().unwrap().lock().unwrap();
                std::array::from_fn(|number| state.free[number].take())
            };
            assert!(matches!(
                owner.try_lease(PROFILE, nz(137)),
                Err(BackingError::Exhausted)
            ));
            assert!(once.load(Ordering::SeqCst));
            assert_eq!(root.debt().1, 1);
            {
                let mut state = owner.state.as_ref().unwrap().lock().unwrap();
                for (free, backing) in state.free.iter_mut().zip(&mut removed) {
                    *free = backing.take();
                }
            }
            drop(owner.try_lease(PROFILE, nz(137)).unwrap());
            *access.lock().unwrap() = None;
            drop(owner);
            assert_eq!(root.debt(), (None, 0));
        });
    }

    #[test]
    fn root_handle_drop_keeps_seat_and_boxes_live_through_last_reader() {
        on_stack(|| {
            let root = BackingRoot::new();
            let released = Arc::new(AtomicUsize::new(0));
            let count = released.clone();
            let audit = Audit {
                callback: Some(Arc::new(move |event| {
                    if let Event::ByteBoxReleased(_) = event {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                    if event == Event::DebtRetired {
                        assert_eq!(count.load(Ordering::SeqCst), 6);
                    }
                    Ok(())
                })),
            };
            let owner = root.prepare(PROFILE, limits(), audit).unwrap();
            let mut lease = owner.try_lease(PROFILE, nz(137)).unwrap();
            drop(root);
            drop(owner);
            assert_eq!(released.load(Ordering::SeqCst), 0);
            lease.parts().provisional.store(&[9]).unwrap();
            assert_eq!(lease.parts().provisional.body(), &[9]);
            drop(lease);
            assert_eq!(released.load(Ordering::SeqCst), 6);
        });
    }

    #[test]
    fn partial_observation_and_unwind_rollback_release_real_storage_before_seat() {
        on_stack(|| {
            for failure in [
                Event::ByteBoxBuilt(BODY),
                Event::ObservationReady,
                Event::IndexPoolBuilt,
            ] {
                let root = Arc::new(BackingRoot::new());
                let live_boxes = Arc::new(AtomicUsize::new(0));
                let seen = live_boxes.clone();
                let observed_root = root.clone();
                let audit = Audit {
                    callback: Some(Arc::new(move |event| {
                        if event == Event::BeforeConstruction {
                            assert_eq!(observed_root.debt().1, 1);
                            assert_eq!(
                                observed_root.debt().0.unwrap().known_backing_bytes,
                                limits().total
                            );
                        }
                        match event {
                            Event::ByteBoxBuilt(_) => {
                                seen.fetch_add(1, Ordering::SeqCst);
                            }
                            Event::ByteBoxReleased(_) => {
                                assert!(observed_root.debt().0.is_some());
                                seen.fetch_sub(1, Ordering::SeqCst);
                            }
                            Event::DebtRetired => {
                                assert_eq!(seen.load(Ordering::SeqCst), 0);
                            }
                            _ => {}
                        }
                        if event == failure {
                            if failure == Event::IndexPoolBuilt {
                                panic!("ordinary construction unwind");
                            }
                            return Err(if failure == Event::ObservationReady {
                                BackingError::ObservationMismatch
                            } else {
                                BackingError::ConstructionInterrupted
                            });
                        }
                        Ok(())
                    })),
                };
                let result =
                    catch_unwind(AssertUnwindSafe(|| root.prepare(PROFILE, limits(), audit)));
                if failure == Event::IndexPoolBuilt {
                    assert!(result.is_err());
                } else {
                    assert!(matches!(
                        result,
                        Ok(Err(BackingError::ConstructionInterrupted
                            | BackingError::ObservationMismatch))
                    ));
                }
                assert_eq!(live_boxes.load(Ordering::SeqCst), 0);
                assert_eq!(root.debt(), (None, 0));
                // Reuse proves the real seat was returned after rollback.
                drop(root.prewarm(PROFILE, limits()).unwrap());
            }
        });
    }

    struct PendingRead<'a> {
        buffer: &'a mut [u8],
        cancelled: &'a AtomicBool,
    }

    impl Future for PendingRead<'_> {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.buffer[0] = u8::from(self.cancelled.load(Ordering::SeqCst));
            Poll::Pending
        }
    }

    #[test]
    fn cancelling_pending_read_does_not_return_pair_until_future_destruction() {
        on_stack(|| {
            let root = BackingRoot::new();
            let owner = root.prewarm(PROFILE, limits()).unwrap();
            let cancelled = AtomicBool::new(false);
            let mut first = owner.try_lease(PROFILE, nz(137)).unwrap();
            let second = owner.try_lease(PROFILE, nz(1)).unwrap();
            let mut reader = Box::pin(async {
                let parts = first.parts();
                PendingRead {
                    buffer: parts.read.pending_read_buffer().unwrap(),
                    cancelled: &cancelled,
                }
                .await;
                drop(first);
            });
            let mut cx = Context::from_waker(Waker::noop());
            assert!(reader.as_mut().poll(&mut cx).is_pending());
            cancelled.store(true, Ordering::SeqCst);
            assert!(reader.as_mut().poll(&mut cx).is_pending());
            assert!(matches!(
                owner.try_lease(PROFILE, nz(1)),
                Err(BackingError::Exhausted)
            ));
            assert_eq!(root.debt().1, 3);
            drop(reader);
            assert_eq!(root.debt().1, 2);
            let third = owner.try_lease(PROFILE, nz(137)).unwrap();
            drop(owner);
            drop(second);
            assert_eq!(root.debt().1, 1);
            drop(third);
            assert_eq!(root.debt(), (None, 0));
        });
    }
}
