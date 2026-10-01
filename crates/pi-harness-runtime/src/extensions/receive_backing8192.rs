//! Bounded indexed receive backing for the staged non-test Tool route.
//!
//! The backing owns finite typed index slots and one measured body buffer per
//! lease. It does not open a channel, start a reader, or enable production
//! credit. The route must retain the returned lease until every indexed view
//! and body borrow has ended.

use std::alloc::Layout;
use std::num::NonZeroUsize;

use pi_extension_protocol::wire::echo_bounded::IndexStorage;

use super::index_pool::{IndexPoolError, ProfileIndexLease, ProfileIndexPool};

pub(super) const FRAME: usize = 8192;
pub(super) const INDEX_LIMIT: usize = 8193;

type Pool = ProfileIndexPool<FRAME, INDEX_LIMIT>;
type Index = ProfileIndexLease<FRAME, INDEX_LIMIT>;
type Storage = IndexStorage<FRAME, INDEX_LIMIT>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Backing8192Error {
    Capacity,
    Exhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackingLayout8192 {
    pub(super) slot_bytes: usize,
    pub(super) total_slot_bytes: usize,
    pub(super) body_bytes: usize,
}

impl BackingLayout8192 {
    pub(super) fn actual() -> Self {
        let slot_bytes = Layout::new::<Storage>().size();
        Self {
            slot_bytes,
            total_slot_bytes: slot_bytes.saturating_mul(2),
            body_bytes: FRAME,
        }
    }

    pub(super) fn checked() -> Result<Self, Backing8192Error> {
        let layout = Self::actual();
        if layout.slot_bytes == 0
            || layout.total_slot_bytes < layout.slot_bytes
            || layout.body_bytes != FRAME
        {
            return Err(Backing8192Error::Capacity);
        }
        Ok(layout)
    }
}

/// A route-scoped finite backing. Two leases are available; a third lease is
/// refused before any new allocation or reader admission.
pub(super) struct ReceiveBacking8192 {
    pool: Pool,
    layout: BackingLayout8192,
}

impl ReceiveBacking8192 {
    pub(super) fn construct() -> Result<Self, Backing8192Error> {
        let layout = BackingLayout8192::checked()?;
        let pool =
            Pool::prewarm(layout.slot_bytes, layout.total_slot_bytes).map_err(
                |error| match error {
                    IndexPoolError::Limit => Backing8192Error::Capacity,
                    IndexPoolError::Exhausted => Backing8192Error::Exhausted,
                },
            )?;
        Ok(Self { pool, layout })
    }

    pub(super) fn layout(&self) -> BackingLayout8192 {
        self.layout
    }

    pub(super) fn lease(
        &self,
        selected_frame_bytes: NonZeroUsize,
    ) -> Result<ReceiveLease8192, Backing8192Error> {
        if selected_frame_bytes.get() > FRAME {
            return Err(Backing8192Error::Capacity);
        }
        let index = self
            .pool
            .checkout(selected_frame_bytes)
            .map_err(|error| match error {
                IndexPoolError::Limit => Backing8192Error::Capacity,
                IndexPoolError::Exhausted => Backing8192Error::Exhausted,
            })?;
        Ok(ReceiveLease8192 {
            index: Some(index),
            body: Some(Box::new([0; FRAME])),
            selected: selected_frame_bytes,
        })
    }
}

/// Exclusive body/index ownership for one bounded receive operation.
pub(super) struct ReceiveLease8192 {
    index: Option<Index>,
    body: Option<Box<[u8; FRAME]>>,
    selected: NonZeroUsize,
}

impl ReceiveLease8192 {
    pub(super) fn selected(&self) -> NonZeroUsize {
        self.selected
    }

    pub(super) fn body_mut(&mut self) -> Result<&mut [u8; FRAME], Backing8192Error> {
        self.body.as_deref_mut().ok_or(Backing8192Error::Capacity)
    }

    pub(super) fn index_mut(&mut self) -> Result<&mut Storage, Backing8192Error> {
        self.index
            .as_mut()
            .map(ProfileIndexLease::storage_mut)
            .ok_or(Backing8192Error::Capacity)
    }

    pub(super) fn measured_body_len(&self, body: &[u8]) -> Result<usize, Backing8192Error> {
        if body.is_empty() || body.len() > self.selected.get() {
            return Err(Backing8192Error::Capacity);
        }
        Ok(body.len())
    }
}

#[cfg(test)]
pub(super) fn measured_snapshot_for_test() -> serde_json::Value {
    let backing = ReceiveBacking8192::construct().unwrap();
    let layout = backing.layout();
    let selected = NonZeroUsize::new(FRAME).unwrap();
    let mut first = backing.lease(selected).unwrap();
    let _second = backing.lease(selected).unwrap();
    let third = backing.lease(selected);
    let body = [1_u8, 2, 3, 5, 8, 13];
    let body_len = first.measured_body_len(&body).unwrap();
    let overflow = vec![0_u8; FRAME + 1];
    let overflow_result = first.measured_body_len(&overflow);
    let body_capacity = first.body_mut().unwrap().len();
    let selected_frame_bytes = first.selected().get();

    serde_json::json!({
        "layout": {
            "slot_bytes": layout.slot_bytes,
            "total_slot_bytes": layout.total_slot_bytes,
            "body_bytes": layout.body_bytes,
        },
        "pool": {
            "configured_slots": 2,
            "leases_acquired": 2,
            "third_lease": match third {
                Err(Backing8192Error::Exhausted) => "refused",
                Err(Backing8192Error::Capacity) => "capacity_refused",
                Ok(_) => "accepted",
            },
        },
        "lease": {
            "selected_frame_bytes": selected_frame_bytes,
            "body_capacity": body_capacity,
            "body_len": body_len,
            "body_sample_len": body.len(),
            "overflow_len": overflow.len(),
            "overflow": match overflow_result {
                Err(Backing8192Error::Capacity) => "refused",
                Err(Backing8192Error::Exhausted) => "exhausted",
                Ok(_) => "accepted",
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actual8192_layout_two_leases_third_refuses() {
        std::thread::Builder::new()
            .name("receive8192-backing-test".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let backing = ReceiveBacking8192::construct().unwrap();
                let selected = NonZeroUsize::new(FRAME).unwrap();
                let first = backing.lease(selected).unwrap();
                let second = backing.lease(selected).unwrap();
                assert!(matches!(
                    backing.lease(selected),
                    Err(Backing8192Error::Exhausted)
                ));
                drop(first);
                drop(second);
                assert!(backing.lease(selected).is_ok());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn measured_body_uses_transmitted_length_and_refuses_overflow() {
        std::thread::Builder::new()
            .name("receive8192-body-test".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let backing = ReceiveBacking8192::construct().unwrap();
                let mut lease = backing.lease(NonZeroUsize::new(FRAME).unwrap()).unwrap();
                assert_eq!(lease.measured_body_len(&[1, 2, 3]).unwrap(), 3);
                assert!(matches!(
                    lease.measured_body_len(&vec![0; FRAME + 1]),
                    Err(Backing8192Error::Capacity)
                ));
                lease.body_mut().unwrap()[..3].copy_from_slice(&[1, 2, 3]);
                assert_eq!(lease.selected().get(), FRAME);
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
