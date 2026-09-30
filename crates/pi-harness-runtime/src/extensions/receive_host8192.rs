//! Non-test retained receive staging for the Tool route.
//!
//! This module owns the route-scoped H capability and its finite indexed
//! backing, but keeps admission inert until native support is accepted. It
//! never creates a second reader beside `ConnectionPump`.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use pi_extension_protocol::tool_live::InvocationBinding;

use super::domain::StagedIndexedBody;
use super::receive_backing8192::{Backing8192Error, ReceiveBacking8192};
use super::transport::RetainedOrderedByteChannel;
use crate::tools::{CapturedArguments, OriginFence};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Receive8192Error {
    InvalidGeneration,
    InvalidCaller,
    Occupied,
    Capacity,
    Closed,
    NativeRefused,
}

#[derive(Clone)]
pub(super) struct CallerIdentity8192 {
    pub(super) arguments: CapturedArguments,
    pub(super) source_call_id: String,
    pub(super) epoch: u64,
    pub(super) origin: Arc<dyn OriginFence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RefusalDecision8192 {
    pub(super) body_len: usize,
}

impl CallerIdentity8192 {
    pub(super) fn matches(&self, other: &Self) -> bool {
        self.arguments.key == other.arguments.key
            && self.arguments.value == other.arguments.value
            && self.arguments.seq == other.arguments.seq
            && self.source_call_id == other.source_call_id
            && self.epoch == other.epoch
            && Arc::ptr_eq(&self.origin, &other.origin)
    }
}

/// A route-owned retained receive handoff. The ordinary pump remains the sole
/// reader; this object only reserves the finite H/backing and proves that an
/// attempted second reader is refused.
pub(super) struct StagedReceive8192 {
    channel: Arc<dyn RetainedOrderedByteChannel>,
    generation: u64,
    backing: Mutex<Option<ReceiveBacking8192>>,
    caller: Mutex<Option<(InvocationBinding, CallerIdentity8192)>>,
    reader_claimed: AtomicBool,
    close_requested: AtomicBool,
    cleaned: AtomicBool,
    #[cfg(test)]
    fail_next_bind: AtomicBool,
}

impl StagedReceive8192 {
    pub(super) fn stage(
        channel: Arc<dyn RetainedOrderedByteChannel>,
        generation: u64,
    ) -> Result<Self, Receive8192Error> {
        if generation == 0 {
            return Err(Receive8192Error::InvalidGeneration);
        }
        let backing = std::thread::Builder::new()
            .name("receive8192-staging".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(ReceiveBacking8192::construct)
            .map_err(|_| Receive8192Error::Capacity)?
            .join()
            .map_err(|_| Receive8192Error::Capacity)?
            .map_err(map_backing_error)?;
        let _ = backing.layout();
        let mut lease = backing
            .lease(std::num::NonZeroUsize::new(super::receive_backing8192::FRAME).unwrap())
            .map_err(map_backing_error)?;
        let _ = lease.selected();
        let _ = lease.body_mut().map_err(map_backing_error)?;
        let _ = lease.index_mut().map_err(map_backing_error)?;
        let _ = lease
            .measured_body_len(&[0_u8])
            .map_err(map_backing_error)?;
        let measured = StagedIndexedBody::new(&[0_u8]).map_err(|_| Receive8192Error::Capacity)?;
        let _ = measured.bytes();
        let _ = measured.body_len();
        Ok(Self {
            channel,
            generation,
            backing: Mutex::new(Some(backing)),
            caller: Mutex::new(None),
            reader_claimed: AtomicBool::new(false),
            close_requested: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_bind: AtomicBool::new(false),
        })
    }

    pub(super) fn validate_caller(
        &self,
        binding: &InvocationBinding,
        caller: &CallerIdentity8192,
    ) -> Result<(), Receive8192Error> {
        if binding.generation() != self.generation || caller.epoch == 0 {
            return Err(Receive8192Error::InvalidCaller);
        }
        let slot = self.caller.lock().unwrap_or_else(|p| p.into_inner());
        if slot.as_ref().is_some_and(|(existing_binding, existing)| {
            !existing_binding.same_live_identity(binding) || !existing.matches(caller)
        }) {
            return Err(Receive8192Error::InvalidCaller);
        }
        Ok(())
    }

    pub(super) fn bind_caller(
        &self,
        binding: InvocationBinding,
        caller: CallerIdentity8192,
    ) -> Result<(), Receive8192Error> {
        #[cfg(test)]
        if self.fail_next_bind.swap(false, Ordering::AcqRel) {
            return Err(Receive8192Error::InvalidCaller);
        }
        self.validate_caller(&binding, &caller)?;
        let mut slot = self.caller.lock().unwrap_or_else(|p| p.into_inner());
        if slot.as_ref().is_some_and(|(existing_binding, existing)| {
            !existing_binding.same_live_identity(&binding) || !existing.matches(&caller)
        }) {
            return Err(Receive8192Error::InvalidCaller);
        }
        *slot = Some((binding, caller));
        Ok(())
    }

    pub(super) fn clear_caller(&self, binding: &InvocationBinding) -> bool {
        let mut slot = self.caller.lock().unwrap_or_else(|p| p.into_inner());
        if slot
            .as_ref()
            .is_some_and(|(existing, _)| existing.same_live_identity(binding))
        {
            slot.take();
            true
        } else {
            false
        }
    }

    pub(super) fn clear_any_caller(&self) {
        self.caller.lock().unwrap_or_else(|p| p.into_inner()).take();
    }

    /// Validate one already-bound caller and retain only the measured wire
    /// length for an explicit refusal. This is the inert boundary before any
    /// indexed body handoff, credit debit, terminal admission, or settlement.
    pub(super) fn prepare_refusal(
        &self,
        binding: &InvocationBinding,
        caller: &CallerIdentity8192,
        body_len: usize,
    ) -> Result<RefusalDecision8192, Receive8192Error> {
        if body_len == 0 || body_len > super::receive_backing8192::FRAME {
            return Err(Receive8192Error::Capacity);
        }
        if self.close_requested.load(Ordering::Acquire) || self.cleaned.load(Ordering::Acquire) {
            return Err(Receive8192Error::Closed);
        }
        let slot = self.caller.lock().unwrap_or_else(|p| p.into_inner());
        let Some((existing_binding, existing_caller)) = slot.as_ref() else {
            return Err(Receive8192Error::InvalidCaller);
        };
        if !existing_binding.same_live_identity(binding) || !existing_caller.matches(caller) {
            return Err(Receive8192Error::InvalidCaller);
        }
        Ok(RefusalDecision8192 { body_len })
    }

    #[cfg(test)]
    pub(super) fn force_next_bind_failure(&self) {
        self.fail_next_bind.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn is_cleaned_for_test(&self) -> bool {
        self.cleaned.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn has_caller_for_test(&self) -> bool {
        self.caller
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }

    pub(super) fn claim_reader(&self) -> Result<ReaderClaim<'_>, Receive8192Error> {
        self.reader_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Receive8192Error::Occupied)?;
        if self.close_requested.load(Ordering::Acquire) || self.cleaned.load(Ordering::Acquire) {
            self.reader_claimed.store(false, Ordering::Release);
            self.cleanup();
            return Err(Receive8192Error::Closed);
        }
        Ok(ReaderClaim { receive: self })
    }

    fn cleanup(&self) {
        if self
            .cleaned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.backing
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            self.caller.lock().unwrap_or_else(|p| p.into_inner()).take();
        }
    }

    /// Request route-owned retained receive cleanup. An active reader keeps
    /// the finite backing and caller tuple alive until its guard is dropped.
    pub(super) fn finish_close(&self) {
        let _ = self.channel.max_read_scratch_bytes();
        self.close_requested.store(true, Ordering::Release);
        if !self.reader_claimed.load(Ordering::Acquire) {
            self.cleanup();
        }
    }

    /// Native admission remains refusal-only until S12-D supplies the current
    /// image/resource proof. Keeping this explicit prevents source staging
    /// from becoming production credit activation.
    pub(super) fn run_preopened(&self) -> Result<(), Receive8192Error> {
        let reader = self.claim_reader()?;
        drop(reader);
        Err(Receive8192Error::NativeRefused)
    }
}

pub(super) struct ReaderClaim<'a> {
    receive: &'a StagedReceive8192,
}

impl Drop for ReaderClaim<'_> {
    fn drop(&mut self) {
        self.receive.reader_claimed.store(false, Ordering::Release);
        if self.receive.close_requested.load(Ordering::Acquire) {
            self.receive.cleanup();
        }
    }
}

fn map_backing_error(error: Backing8192Error) -> Receive8192Error {
    match error {
        Backing8192Error::Capacity => Receive8192Error::Capacity,
        Backing8192Error::Exhausted => Receive8192Error::Occupied,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::transport::ChannelError;
    use pi_extension_protocol::wire::framing::EncodedFrame;
    struct Probe;

    impl crate::extensions::transport::OrderedByteChannel for Probe {
        fn max_read_scratch_bytes(&self) -> usize {
            0
        }

        fn read_chunk<'a>(
            &'a self,
            _buffer: &'a mut [u8],
        ) -> pi_ai_core::BoxFuture<'a, Result<Option<usize>, ChannelError>> {
            Box::pin(async { Ok(None) })
        }

        fn write_frame(
            &self,
            _frame: EncodedFrame,
        ) -> pi_ai_core::BoxFuture<'_, Result<(), ChannelError>> {
            Box::pin(async { Ok(()) })
        }

        fn close(&self) {}
    }

    impl RetainedOrderedByteChannel for Probe {
        fn take_notifications(&self) -> [Option<std::task::Waker>; 2] {
            [None, None]
        }

        fn close_retained(&self) -> [Option<std::task::Waker>; 2] {
            [None, None]
        }
    }

    #[test]
    fn staged_receive_keeps_one_reader_and_refuses_native_activation() {
        std::thread::Builder::new()
            .name("receive8192-host-test".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let receive = StagedReceive8192::stage(Arc::new(Probe), 7).unwrap();
                let first = receive.claim_reader().unwrap();
                assert!(matches!(
                    receive.claim_reader(),
                    Err(Receive8192Error::Occupied)
                ));
                drop(first);
                assert!(matches!(
                    receive.run_preopened(),
                    Err(Receive8192Error::NativeRefused)
                ));
                assert!(receive.claim_reader().is_ok());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn staged_receive_close_defers_cleanup_for_the_sole_reader() {
        std::thread::Builder::new()
            .name("receive8192-close-test".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let receive = StagedReceive8192::stage(Arc::new(Probe), 8).unwrap();
                let reader = receive.claim_reader().unwrap();
                receive.finish_close();
                assert!(!receive.cleaned.load(Ordering::Acquire));
                assert!(
                    receive
                        .backing
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .is_some()
                );
                drop(reader);
                assert!(receive.cleaned.load(Ordering::Acquire));
                assert!(
                    receive
                        .backing
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .is_none()
                );
                assert!(matches!(
                    receive.claim_reader(),
                    Err(Receive8192Error::Closed)
                ));
                receive.finish_close();
                assert!(receive.cleaned.load(Ordering::Acquire));
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
