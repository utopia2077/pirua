//! Private, prewarmed storage for two borrowed echo indexes.
//!
//! The limits cover the requested backing of the two typed slots, including
//! their unused entries and padding. They do not cover the pool's `Arc`
//! allocation, allocator overhead, or construction-time heap/stack peaks.
//! `Box::new` can abort on allocation failure; this constructor does not make
//! startup OOM recoverable. Nothing in the live extension path uses this pool.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use pi_extension_protocol::wire::echo_bounded::IndexStorage;

type IndexSlot = IndexStorage<4096, 4097>;
const SLOT_COUNT: usize = 2;
const MAX_FRAME_BYTES: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IndexPoolError {
    Limit,
    Exhausted,
}

struct PoolState {
    free: [Option<Box<IndexSlot>>; SLOT_COUNT],
}

/// A non-cloneable handle. A lease retains the backing after this handle is
/// dropped, and an occupied slot cannot be issued a second time.
pub(super) struct IndexPool {
    state: Arc<Mutex<PoolState>>,
    slot_bytes: usize,
    total_slot_bytes: usize,
}

impl IndexPool {
    /// Call only during trusted startup, before accepting peer-controlled work.
    /// Both ceilings are checked before requesting either slot's backing.
    pub(super) fn prewarm(
        max_slot_backing_bytes: usize,
        max_total_slot_backing_bytes: usize,
    ) -> Result<Self, IndexPoolError> {
        Self::prewarm_with(max_slot_backing_bytes, max_total_slot_backing_bytes, || {
            Box::new(IndexSlot::new())
        })
    }

    fn prewarm_with(
        max_slot_backing_bytes: usize,
        max_total_slot_backing_bytes: usize,
        mut make_slot: impl FnMut() -> Box<IndexSlot>,
    ) -> Result<Self, IndexPoolError> {
        let slot_bytes = IndexSlot::checked_slot_bytes(MAX_FRAME_BYTES, max_slot_backing_bytes)
            .map_err(|_| IndexPoolError::Limit)?;
        let total_slot_bytes = slot_bytes
            .checked_mul(SLOT_COUNT)
            .filter(|total| *total <= max_total_slot_backing_bytes)
            .ok_or(IndexPoolError::Limit)?;

        let free = std::array::from_fn(|_| Some(make_slot()));
        Ok(Self {
            state: Arc::new(Mutex::new(PoolState { free })),
            slot_bytes,
            total_slot_bytes,
        })
    }

    pub(super) fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    pub(super) fn total_slot_bytes(&self) -> usize {
        self.total_slot_bytes
    }

    /// A fixed lookup and transfer. A rejected profile or exhausted pool
    /// creates no replacement storage.
    pub(super) fn checkout(
        &self,
        selected_frame_bytes: NonZeroUsize,
    ) -> Result<IndexLease, IndexPoolError> {
        IndexSlot::checked_slot_bytes(selected_frame_bytes, self.slot_bytes)
            .map_err(|_| IndexPoolError::Limit)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (slot_number, slot) = state
            .free
            .iter_mut()
            .enumerate()
            .find_map(|(number, entry)| entry.take().map(|slot| (number, slot)))
            .ok_or(IndexPoolError::Exhausted)?;
        Ok(IndexLease {
            state: Arc::clone(&self.state),
            slot_number,
            slot: Some(slot),
        })
    }
}

/// Exclusive ownership lasts until actual Drop, including after pool-handle
/// destruction. Borrowed views cannot outlive a mutable borrow of this lease.
pub(super) struct IndexLease {
    state: Arc<Mutex<PoolState>>,
    slot_number: usize,
    slot: Option<Box<IndexSlot>>,
}

impl IndexLease {
    pub(super) fn storage_mut(&mut self) -> &mut IndexSlot {
        self.slot.as_deref_mut().expect("lease owns its slot")
    }
}

impl Drop for IndexLease {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.free[self.slot_number].is_none() {
            state.free[self.slot_number] = Some(slot);
        } else {
            // This branch is unreachable through the safe checkout API. Keep
            // a violated invariant from dropping a Box under the mutex.
            drop(state);
            drop(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use pi_extension_protocol::wire::echo_bounded::ScanError;

    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn backing() -> (usize, usize) {
        let slot = Layout::new::<IndexSlot>().size();
        (slot, slot.checked_mul(SLOT_COUNT).unwrap())
    }

    #[test]
    fn ceilings_refuse_before_any_slot_request() {
        let (slot, total) = backing();
        let requests = Cell::new(0);
        let mut make = || {
            requests.set(requests.get() + 1);
            Box::new(IndexSlot::new())
        };
        assert!(matches!(
            IndexPool::prewarm_with(slot - 1, total, &mut make),
            Err(IndexPoolError::Limit)
        ));
        assert!(matches!(
            IndexPool::prewarm_with(slot, total - 1, &mut make),
            Err(IndexPoolError::Limit)
        ));
        assert_eq!(requests.get(), 0);

        let pool = IndexPool::prewarm_with(slot, total, &mut make).unwrap();
        assert_eq!(requests.get(), SLOT_COUNT);
        assert_eq!(pool.slot_bytes(), slot);
        assert_eq!(pool.total_slot_bytes(), total);
    }

    #[test]
    fn selected_prefixes_charge_full_backing_and_invalid_selection_keeps_slots_free() {
        let (slot, total) = backing();
        let pool = IndexPool::prewarm(slot, total).unwrap();
        for selected in [1, 137, 4096] {
            let mut lease = pool.checkout(nz(selected)).unwrap();
            let mut arena = lease.storage_mut().borrow(nz(selected), slot).unwrap();
            assert_eq!(arena.slot_bytes(), slot);
            assert_eq!(arena.selected_budget().field_capacity, selected);
            if selected == 1 {
                assert!(matches!(
                    arena.scan_envelope(&[0x00]),
                    Err(ScanError::Malformed)
                ));
            } else {
                assert_eq!(arena.scan_envelope(&[0x2a, 0x00]).unwrap().arm(), Some(5));
            }
        }
        assert!(matches!(
            pool.checkout(nz(4097)),
            Err(IndexPoolError::Limit)
        ));
        assert!(matches!(
            pool.checkout(nz(usize::MAX)),
            Err(IndexPoolError::Limit)
        ));
        let _first = pool.checkout(nz(1)).unwrap();
        let _second = pool.checkout(nz(137)).unwrap();
    }

    #[test]
    fn exclusive_leases_return_on_drop_and_scans_reuse_backing() {
        let (slot, total) = backing();
        let requests = Cell::new(0);
        let pool = IndexPool::prewarm_with(slot, total, || {
            requests.set(requests.get() + 1);
            Box::new(IndexSlot::new())
        })
        .unwrap();
        let mut first = pool.checkout(nz(137)).unwrap();
        let first_number = first.slot_number;
        let second = pool.checkout(nz(4096)).unwrap();
        assert_ne!(first_number, second.slot_number);
        assert!(matches!(
            pool.checkout(nz(1)),
            Err(IndexPoolError::Exhausted)
        ));

        {
            let mut arena = first.storage_mut().borrow(nz(137), slot).unwrap();
            assert!(matches!(
                arena.scan_envelope(&[0x00]),
                Err(ScanError::Malformed)
            ));
            assert_eq!(arena.scan_envelope(&[0x2a, 0x00]).unwrap().arm(), Some(5));
        }
        drop(first);
        let mut reused = pool.checkout(nz(1)).unwrap();
        assert_eq!(reused.slot_number, first_number);
        let mut arena = reused.storage_mut().borrow(nz(1), slot).unwrap();
        assert!(matches!(
            arena.scan_envelope(&[0x00]),
            Err(ScanError::Malformed)
        ));
        assert_eq!(requests.get(), SLOT_COUNT);
    }

    #[test]
    fn unwind_returns_a_slot_and_lease_keeps_pool_alive() {
        let (slot, total) = backing();
        let pool = IndexPool::prewarm(slot, total).unwrap();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _lease = pool.checkout(nz(137)).unwrap();
            panic!("test unwind after checkout");
        }));
        assert!(result.is_err());
        let mut lease = pool.checkout(nz(137)).unwrap();
        let weak = Arc::downgrade(&pool.state);
        drop(pool);
        assert!(weak.upgrade().is_some());
        {
            let mut arena = lease.storage_mut().borrow(nz(137), slot).unwrap();
            assert_eq!(arena.scan_envelope(&[0x2a, 0x00]).unwrap().arm(), Some(5));
        }
        drop(lease);
        assert!(weak.upgrade().is_none());
    }
}
