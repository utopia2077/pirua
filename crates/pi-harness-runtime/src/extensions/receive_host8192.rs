//! Non-test retained receive staging for the Tool route.
//!
//! This module owns the route-scoped H capability and its finite indexed
//! backing, but keeps admission inert until native support is accepted. It
//! never creates a second reader beside `ConnectionPump`.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use pi_extension_protocol::endpoint::{ActiveFrame, SelectedEnvelope};
use pi_extension_protocol::tool_live::InvocationBinding;
#[cfg(test)]
use pi_extension_protocol::wire::echo_bounded::BorrowedIndexArena;
#[cfg(test)]
use pi_extension_protocol::wire::framing::EncodedFrame;
use pi_extension_protocol::wire::{v1_0, v1_1};

use super::domain::StagedIndexedBody;
use super::receive_backing8192::{Backing8192Error, ReceiveBacking8192, ReceiveLease8192};
use super::transport::RetainedOrderedByteChannel;
#[cfg(test)]
use super::transport::{ChannelError, OrderedByteChannel};
use crate::tools::{CapturedArguments, OriginFence};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Receive8192Error {
    InvalidGeneration,
    InvalidCaller,
    Occupied,
    Capacity,
    Closed,
    UnsupportedFrame,
    NativeRefused,
}

#[derive(Clone)]
pub(super) struct CallerIdentity8192 {
    pub(super) arguments: CapturedArguments,
    pub(super) source_call_id: String,
    pub(super) epoch: u64,
    pub(super) origin: Arc<dyn OriginFence>,
}

#[derive(Clone, Debug)]
pub(super) struct RefusalDecision8192 {
    pub(super) binding: InvocationBinding,
    pub(super) body_len: usize,
}

/// Lease before owner gives the current body/index custody a fixed lifetime.
/// This storage remains empty while the pump supplies only a measured frame.
struct StagedBacking8192 {
    lease: ReceiveLease8192,
    owner: ReceiveBacking8192,
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
    backing: Mutex<Option<StagedBacking8192>>,
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
            .spawn(move || {
                #[cfg(test)]
                {
                    return super::transport::source_observer::with_generation(
                        generation,
                        ReceiveBacking8192::construct,
                    );
                }
                #[cfg(not(test))]
                {
                    ReceiveBacking8192::construct()
                }
            })
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
        #[cfg(test)]
        {
            let layout = backing.layout();
            let mut event = super::transport::source_observer::SourceEvent::new(
                "resource",
                file!(),
                line!(),
                "StagedReceive8192::stage",
            );
            event.field = Some("Hboot");
            event.owner = Some("StagedReceive8192");
            event.generation = Some(generation);
            event.capacity = Some(layout.slot_bytes);
            event.bound_bytes = Some(layout.total_slot_bytes);
            event.body_len = Some(layout.body_bytes);
            event.operation = Some("stage");
            event.outcome = Some("staged");
            super::transport::source_observer::record(event);
        }
        Ok(Self {
            channel,
            generation,
            backing: Mutex::new(Some(StagedBacking8192 {
                lease,
                owner: backing,
            })),
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
        frame: &ActiveFrame,
    ) -> Result<RefusalDecision8192, Receive8192Error> {
        let body_len = frame.body_len;
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
        if !matches!(
            &frame.envelope,
            SelectedEnvelope::V1_0(v1_0::Envelope {
                frame: Some(
                    v1_0::envelope::Frame::StreamItem(_) | v1_0::envelope::Frame::StreamEnd(_)
                ),
            }) | SelectedEnvelope::V1_1(v1_1::Envelope {
                frame: Some(
                    v1_1::envelope::Frame::StreamItem(_) | v1_1::envelope::Frame::StreamEnd(_)
                ),
            })
        ) {
            return Err(Receive8192Error::UnsupportedFrame);
        }
        let backing = self.backing.lock().unwrap_or_else(|p| p.into_inner());
        let backing = backing.as_ref().ok_or(Receive8192Error::Closed)?;
        if body_len > backing.lease.selected().get() || body_len > backing.owner.layout().body_bytes
        {
            return Err(Receive8192Error::Capacity);
        }
        Ok(RefusalDecision8192 {
            binding: existing_binding.clone(),
            body_len,
        })
    }

    /// Test-only indexed handoff over the already retained lease. The caller
    /// owns the sole `ReaderClaim`; this helper copies the exact captured body
    /// into the prewarmed body slot, borrows the selected index prefix, and
    /// keeps both borrows alive only for the callback.
    #[cfg(test)]
    pub(super) fn with_indexed_body<R>(
        &self,
        body: &[u8],
        callback: impl FnOnce(&[u8], &mut BorrowedIndexArena<'_>) -> R,
    ) -> Result<R, Receive8192Error> {
        let mut backing = self.backing.lock().unwrap_or_else(|p| p.into_inner());
        let backing = backing.as_mut().ok_or(Receive8192Error::Closed)?;
        let max_retained_bytes = backing.owner.layout().slot_bytes;
        backing
            .lease
            .with_indexed_body(body, max_retained_bytes, callback)
            .map_err(|_| Receive8192Error::Capacity)
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
            .map_err(|_| {
                #[cfg(test)]
                {
                    let mut event = super::transport::source_observer::SourceEvent::new(
                        "resource",
                        file!(),
                        line!(),
                        "StagedReceive8192::claim_reader",
                    );
                    event.field = Some("Hboot");
                    event.owner = Some("StagedReceive8192");
                    event.operation = Some("reader_refused");
                    event.outcome = Some("Occupied");
                    event.generation = Some(self.generation);
                    super::transport::source_observer::record(event);
                }
                Receive8192Error::Occupied
            })?;
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
            #[cfg(test)]
            {
                let mut event = super::transport::source_observer::SourceEvent::new(
                    "cleanup",
                    file!(),
                    line!(),
                    "StagedReceive8192::cleanup",
                );
                event.field = Some("Hboot");
                event.owner = Some("StagedReceive8192");
                event.cleanup = Some(true);
                event.outcome = Some("complete");
                event.generation = Some(self.generation);
                super::transport::source_observer::record(event);
            }
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
        #[cfg(test)]
        {
            let mut event = super::transport::source_observer::SourceEvent::new(
                "refusal",
                file!(),
                line!(),
                "StagedReceive8192::run_preopened",
            );
            event.outcome = Some("NativeRefused");
            event.generation = Some(self.generation);
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("StagedReceive8192");
            super::transport::source_observer::record(event);
        }
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
struct ScenarioProbe;

#[cfg(test)]
impl OrderedByteChannel for ScenarioProbe {
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

#[cfg(test)]
impl RetainedOrderedByteChannel for ScenarioProbe {
    fn take_notifications(&self) -> [Option<std::task::Waker>; 2] {
        [None, None]
    }

    fn close_retained(&self) -> [Option<std::task::Waker>; 2] {
        [None, None]
    }
}

/// Exercise the existing retained backing/custody fixture on the source-root
/// collector. Every observation comes from the real lease, indexed body,
/// reader, refusal, generation, and cleanup owners.
#[cfg(test)]
pub(crate) fn scenario_snapshot_for_test(generation: u64) {
    super::transport::source_observer::with_generation(generation, || {
        let receive = StagedReceive8192::stage(Arc::new(ScenarioProbe), generation).unwrap();
        let selected = std::num::NonZeroUsize::new(super::receive_backing8192::FRAME).unwrap();
        {
            let staged = receive.backing.lock().unwrap();
            let backing = staged.as_ref().expect("staged backing");
            let extra = backing.owner.lease(selected).unwrap();
            assert!(matches!(
                backing.owner.lease(selected),
                Err(Backing8192Error::Exhausted)
            ));
            drop(extra);
        }
        receive
            .with_indexed_body(b"source-root-indexed-body", |body, _arena| {
                assert_eq!(body, b"source-root-indexed-body");
            })
            .unwrap();
        let first_reader = receive.claim_reader().unwrap();
        assert!(matches!(
            receive.claim_reader(),
            Err(Receive8192Error::Occupied)
        ));
        drop(first_reader);
        assert!(matches!(
            receive.run_preopened(),
            Err(Receive8192Error::NativeRefused)
        ));
        let final_reader = receive.claim_reader().unwrap();
        receive.finish_close();
        assert!(!receive.is_cleaned_for_test());
        drop(final_reader);
        assert!(receive.is_cleaned_for_test());
    });
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

    fn measured_trace() -> serde_json::Value {
        super::super::transport::source_observer::reset();
        let receive = StagedReceive8192::stage(Arc::new(Probe), 17).unwrap();
        let mut events = Vec::new();
        events.push(serde_json::json!({
            "event": "staged",
            "generation": 17,
            "probe_max_read_scratch_bytes": 0,
        }));

        let first_reader = receive.claim_reader().unwrap();
        events.push(serde_json::json!({
            "event": "reader_claimed",
            "reader_claimed": true,
        }));
        let occupied = receive.claim_reader();
        assert!(matches!(occupied, Err(Receive8192Error::Occupied)));
        events.push(serde_json::json!({
            "event": "second_reader_refused",
            "result": "Occupied",
            "reader_claimed": true,
        }));
        drop(first_reader);
        events.push(serde_json::json!({
            "event": "reader_released",
            "reader_claimed": false,
        }));

        let refusal = receive.run_preopened();
        assert!(matches!(refusal, Err(Receive8192Error::NativeRefused)));
        events.push(serde_json::json!({
            "event": "run_preopened",
            "result": "NativeRefused",
            "reader_claimed": false,
            "refusal_before_close": true,
        }));

        let final_reader = receive.claim_reader().unwrap();
        receive.finish_close();
        assert!(!receive.is_cleaned_for_test());
        events.push(serde_json::json!({
            "event": "close_requested",
            "result": "deferred",
            "reader_claimed": true,
            "cleaned": false,
        }));
        drop(final_reader);
        assert!(receive.is_cleaned_for_test());
        events.push(serde_json::json!({
            "event": "reader_drop_cleanup",
            "reader_claimed": false,
            "cleaned": true,
        }));

        serde_json::json!({
            "trace_kind": "refusal",
            "outcomes": {
                "normal": "not_executed",
                "partial": "not_executed",
                "error": "not_executed",
                "cancel": "not_executed",
                "refusal": "NativeRefused",
            },
            "events": events,
            "ordering": [
                "staged",
                "reader_claimed",
                "second_reader_refused",
                "reader_released",
                "run_preopened",
                "close_requested",
                "reader_drop_cleanup",
            ],
            "backing": super::super::receive_backing8192::measured_snapshot_for_test(),
            "resource_observation": super::super::transport::source_observer::resource_observation(),
        })
    }

    #[test]
    fn native_measurement_trace() {
        std::thread::Builder::new()
            .name("receive8192-native-measurement".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let trace = measured_trace();
                println!(
                    "NATIVE_MEASUREMENT {}",
                    serde_json::to_string(&trace).unwrap()
                );
            })
            .unwrap()
            .join()
            .unwrap();
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

    #[test]
    fn staged_receive_retains_one_finite_lease_until_cleanup() {
        std::thread::Builder::new()
            .name("receive8192-lease-custody-test".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let receive = StagedReceive8192::stage(Arc::new(Probe), 9).unwrap();
                let selected =
                    std::num::NonZeroUsize::new(super::super::receive_backing8192::FRAME).unwrap();
                let extra = {
                    let staged = receive.backing.lock().unwrap();
                    let backing = staged.as_ref().unwrap();
                    assert_eq!(backing.lease.selected(), selected);
                    let extra = backing.owner.lease(selected).unwrap();
                    assert!(matches!(
                        backing.owner.lease(selected),
                        Err(Backing8192Error::Exhausted)
                    ));
                    extra
                };
                drop(extra);
                let reader = receive.claim_reader().unwrap();
                receive.finish_close();
                assert!(receive.backing.lock().unwrap().is_some());
                drop(reader);
                assert!(receive.backing.lock().unwrap().is_none());
                receive.finish_close();
                assert!(receive.backing.lock().unwrap().is_none());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
