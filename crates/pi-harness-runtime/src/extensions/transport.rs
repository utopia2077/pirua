//! Host-owned ordered bytes and finite-time activation intake.
//!
//! The host authenticates the peer and supplies the channel, monotonic clock,
//! limits and catalog before constructing this pump. This module neither opens
//! an OS resource nor dispatches an active request or touches a Session.

use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::task::{Poll, Waker};

use pi_ai_core::BoxFuture;
use pi_extension_protocol::activation::{ActiveGrant, HostAuthority};
use pi_extension_protocol::endpoint::{
    ActiveFrame, Endpoint, EndpointError, EndpointEvent, EndpointPhase,
};
use pi_extension_protocol::wire::framing::EncodedFrame;

use super::critical_port::{NotificationPost, SeatLease};

#[cfg(test)]
pub(crate) mod source_observer {
    use std::sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct SourceEvent {
        pub schema_version: &'static str,
        pub trace_id: u64,
        pub seq: u64,
        pub event_id: String,
        pub kind: &'static str,
        pub source_file: &'static str,
        pub source_line: u32,
        pub function: &'static str,
        pub phase: Option<&'static str>,
        pub outcome: Option<&'static str>,
        pub body_len: Option<usize>,
        pub consumed: Option<usize>,
        pub capacity: Option<usize>,
        pub scratch_bytes: Option<usize>,
        pub bound_bytes: Option<usize>,
        pub role: Option<&'static str>,
        pub parent_present: Option<bool>,
        pub native_parent_present: Option<bool>,
        pub operation: Option<&'static str>,
        pub method: Option<String>,
        pub lane: Option<&'static str>,
        pub bytes: Option<usize>,
        pub jobs: Option<usize>,
        pub reserved_normal: Option<usize>,
        pub reserved_control: Option<usize>,
        pub reserved_cancel: Option<usize>,
        pub request_id: Option<u64>,
        pub parent_request_id: Option<u64>,
        pub generation: Option<u64>,
        pub wire_sequence_before: Option<u64>,
        pub wire_sequence_after: Option<u64>,
        pub provider: Option<String>,
        pub model: Option<String>,
        pub raw_body: Option<Vec<u8>>,
    }

    impl SourceEvent {
        pub(crate) fn new(
            kind: &'static str,
            source_file: &'static str,
            source_line: u32,
            function: &'static str,
        ) -> Self {
            Self {
                schema_version: "s12-d-resource-source-events-v1",
                trace_id: 0,
                seq: 0,
                event_id: String::new(),
                kind,
                source_file,
                source_line,
                function,
                phase: None,
                outcome: None,
                body_len: None,
                consumed: None,
                capacity: None,
                scratch_bytes: None,
                bound_bytes: None,
                role: None,
                parent_present: None,
                native_parent_present: None,
                operation: None,
                method: None,
                lane: None,
                bytes: None,
                jobs: None,
                reserved_normal: None,
                reserved_control: None,
                reserved_cancel: None,
                request_id: None,
                parent_request_id: None,
                generation: None,
                wire_sequence_before: None,
                wire_sequence_after: None,
                provider: None,
                model: None,
                raw_body: None,
            }
        }
    }

    #[derive(Default)]
    struct State {
        trace_id: u64,
        seq: u64,
        events: Vec<SourceEvent>,
    }

    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    static C3_WALKING: AtomicBool = AtomicBool::new(false);

    fn state() -> &'static Mutex<State> {
        STATE.get_or_init(|| Mutex::new(State::default()))
    }

    pub(crate) fn reset() {
        let mut state = state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.trace_id = state.trace_id.saturating_add(1);
        state.seq = 0;
        state.events.clear();
    }

    pub(crate) fn record(mut event: SourceEvent) {
        let mut state = state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.seq = state.seq.saturating_add(1);
        event.trace_id = state.trace_id;
        event.seq = state.seq;
        event.event_id = format!("source/{}/{}", event.trace_id, event.seq);
        state.events.push(event);
    }

    pub(crate) fn snapshot() -> Vec<SourceEvent> {
        state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .events
            .clone()
    }

    pub(crate) fn set_c3_walking(enabled: bool) {
        C3_WALKING.store(enabled, Ordering::Release);
    }

    pub(crate) fn c3_walking_enabled() -> bool {
        C3_WALKING.load(Ordering::Acquire)
    }

    pub(crate) fn take_active_body(
        request_id: u64,
        generation: u64,
        body_len: usize,
    ) -> Option<Vec<u8>> {
        let mut state = state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.events.iter_mut().rev().find_map(|event| {
            if event.kind == "body_capture"
                && event.outcome == Some("retained")
                && event.request_id == Some(request_id)
                && event.generation == Some(generation)
                && event.body_len == Some(body_len)
            {
                event.raw_body.take()
            } else {
                None
            }
        })
    }
}

/// Content-free host-channel failures. Implementations must not expose peer bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelError {
    Closed,
    Failed,
}

/// One host-owned ordered duplex byte channel for one connection generation.
///
/// Exactly one reader calls `read_chunk` with a pump-owned buffer. It writes a
/// nonempty prefix and returns its length, EOF, or a fixed error. It may retain
/// the borrowed buffer while pending, but must not retain it after completion or
/// cancellation. The returned length must not exceed the supplied slice.
/// `max_read_scratch_bytes` is the trusted host's stable, finite peak of
/// additional live capacity owned by one pending read, including future state
/// and copies. A host-side receive queue needs its own finite admission bound.
/// The charged metric is retained buffer capacity plus host-requested allocation
/// payload bytes; allocator rounding and metadata require separate accounting.
/// The pump exposes the checked sum of that declaration and its buffer capacity.
/// The declaration and construction/polling of the read future must return
/// promptly without blocking I/O or reentering the owner/route line; a timer
/// cannot preempt a synchronous host callback.
///
/// `write_frame` completes the entire frame in FIFO order or fails; an
/// implementation must never interleave frame bytes or buffer an unbounded
/// number of writes. Closing is idempotent and wakes pending reads and writes.
/// A host may close its own channel externally.
pub trait OrderedByteChannel: Send + Sync {
    fn max_read_scratch_bytes(&self) -> usize;

    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>>;

    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>>;

    fn close(&self);
}

/// Host-owned admission capability for the retained receive path. After an
/// in-scope read or write future is dropped, `take_notifications` detaches
/// every Waker the channel retained for that operation. `close_retained`
/// physically closes the channel and detaches the same finite two seats.
/// Neither method invokes or drops a Waker, waits, allocates, or retains a
/// borrowed read buffer. The caller destroys the returned owner-created
/// Wakers before clock client zero. An arbitrary `OrderedByteChannel` does not
/// satisfy this contract.
///
/// The ordinary Tool ingress remains available for channels that implement only
/// [`OrderedByteChannel`]. Hosts that provide this stronger capability may opt
/// into [`crate::owner::RuntimeHarness::connect_profile_tool_retained`] before
/// the channel is erased to the ordinary view.
pub trait RetainedOrderedByteChannel: OrderedByteChannel {
    fn take_notifications(&self) -> [Option<Waker>; 2];

    fn close_retained(&self) -> [Option<Waker>; 2];
}

/// Notifications detached by an opt-in physical quiet close. The channel must
/// already be closed when this value is returned. The caller transfers both
/// notifications to independently serviced, reserved posts before exposing a
/// no-denial completion. A repeat close returns empty notifications. Dropping
/// a populated value may Drop arbitrary Wakers; the trusted host must post it.
#[must_use = "post any detached notifications to independent roles"]
pub struct QuietClose {
    read: Option<Waker>,
    write: Option<Waker>,
}

impl QuietClose {
    pub fn new(read: Option<Waker>, write: Option<Waker>) -> Self {
        Self { read, write }
    }

    pub fn into_posts<'a>(
        self,
        read_seat: SeatLease<'a>,
        write_seat: SeatLease<'a>,
    ) -> [Option<NotificationPost<'a>>; 2] {
        [
            self.read.map(|wake| NotificationPost::new(wake, read_seat)),
            self.write
                .map(|wake| NotificationPost::new(wake, write_seat)),
        ]
    }
}

/// First-route host capability. `close_quiet` is bounded, idempotent and
/// physically closes before return. It neither invokes nor drops a detached
/// Waker, performs peer I/O, waits, allocates or reenters the route. Generic
/// `OrderedByteChannel::close` does not imply this contract. Before a live
/// route uses `ConnectionPump`, its host must also make the pump's repeated
/// `close` path quiet with reserved independent notification delivery, or
/// arrange that the pump can only repeat an already completed quiet close.
pub trait QuietOrderedByteChannel: OrderedByteChannel {
    fn close_quiet(&self) -> QuietClose;
}

/// Opt-in sole-writer lifecycle. Construction, poll and Drop must be bounded,
/// callback-free and nonwaiting. Only a port-owned writer Waker may be polled
/// through this future. A live first route must require this capability.
pub trait BoundedWriterChannel: QuietOrderedByteChannel {
    type Write<'a>: Future<Output = Result<(), ChannelError>> + 'a
    where
        Self: 'a;

    fn write_frame_bounded(&self, frame: EncodedFrame) -> Self::Write<'_>;
}

/// Host-supplied receiver-local monotonic milliseconds and wakeable timer.
/// `now_ms` must finish promptly in bounded time: it must not block on I/O or
/// another callback, reenter the owner/route line, or perform peer/channel I/O.
/// `sleep_until` must not complete before `deadline_ms` and must wake when due,
/// independently of a pending channel write. A host unable to meet these
/// conditions cannot activate the first tool route.
pub trait MonotonicClock: Send + Sync {
    fn now_ms(&self) -> u64;

    fn sleep_until(&self, deadline_ms: u64) -> BoxFuture<'_, ()>;
}

/// Opt-in recyclable writer timer. The future owns the reserved clock seat;
/// Drop synchronously deregisters its exact host registration, without a
/// tombstone, before the seat can be reused for the next FIFO frame.
/// Construction, poll and Drop are bounded, callback-free and nonwaiting.
pub trait ExactWriterClock: MonotonicClock {
    type WriterSleep<'a>: Future<Output = ()> + 'a
    where
        Self: 'a;

    fn writer_sleep_until<'a>(
        &'a self,
        seat: SeatLease<'a>,
        original_deadline_ms: u64,
    ) -> Self::WriterSleep<'a>;
}

#[derive(Clone, Copy, Debug)]
pub struct PumpConfig {
    pub max_read_chunk: NonZeroUsize,
    pub write_timeout_ms: NonZeroU64,
}

/// Fixed, content-free connection errors. The original peer bytes are never
/// included in an error or exposed through a failed handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PumpError {
    Closed,
    ReadFailed,
    WriteFailed,
    BadReadChunk,
    ReadBufferUnavailable,
    HandshakeTimeout,
    HandshakeEof,
    WriteTimeout,
    ClockFault,
    MalformedInput,
    TruncatedEof,
}

impl fmt::Display for PumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Closed => "extension channel closed",
            Self::ReadFailed => "extension channel read failed",
            Self::WriteFailed => "extension channel write failed",
            Self::BadReadChunk => "extension channel returned an invalid chunk",
            Self::ReadBufferUnavailable => "extension read buffer limit cannot be reserved",
            Self::HandshakeTimeout => "extension handshake timed out",
            Self::HandshakeEof => "extension handshake ended before Ready",
            Self::WriteTimeout => "extension frame write timed out",
            Self::ClockFault => "extension monotonic clock failed",
            Self::MalformedInput => "extension frame was rejected",
            Self::TruncatedEof => "extension frame ended before completion",
        };
        f.write_str(message)
    }
}

impl std::error::Error for PumpError {}

/// A bounded handshake pump that returns one selected active frame at a time.
/// The caller owns any later method admission, request demux and settlement.
pub struct ConnectionPump {
    endpoint: Endpoint,
    channel: Arc<dyn OrderedByteChannel>,
    clock: Arc<dyn MonotonicClock>,
    config: PumpConfig,
    handshake_deadline_ms: u64,
    buffered: Vec<u8>,
    filled: usize,
    offset: usize,
    read_storage_bound_bytes: usize,
    closed: bool,
    #[cfg(test)]
    raw_frame: Vec<u8>,
}

#[cfg(test)]
fn observed_phase(phase: EndpointPhase) -> &'static str {
    match phase {
        EndpointPhase::AwaitHello => "AwaitHello",
        EndpointPhase::AwaitManifest => "AwaitManifest",
        EndpointPhase::AwaitReady => "AwaitReady",
        EndpointPhase::Active => "Active",
        EndpointPhase::Closed => "Closed",
    }
}

#[cfg(test)]
fn active_request_id(envelope: &pi_extension_protocol::endpoint::SelectedEnvelope) -> Option<u64> {
    match envelope {
        pi_extension_protocol::endpoint::SelectedEnvelope::V1_0(envelope) => {
            match envelope.frame.as_ref()? {
                pi_extension_protocol::wire::v1_0::envelope::Frame::StreamItem(item) => {
                    Some(item.request_id)
                }
                pi_extension_protocol::wire::v1_0::envelope::Frame::StreamEnd(end) => {
                    Some(end.request_id)
                }
                _ => None,
            }
        }
        pi_extension_protocol::endpoint::SelectedEnvelope::V1_1(envelope) => {
            match envelope.frame.as_ref()? {
                pi_extension_protocol::wire::v1_1::envelope::Frame::StreamItem(item) => {
                    Some(item.request_id)
                }
                pi_extension_protocol::wire::v1_1::envelope::Frame::StreamEnd(end) => {
                    Some(end.request_id)
                }
                _ => None,
            }
        }
    }
}

#[cfg(test)]
fn active_sequence(envelope: &pi_extension_protocol::endpoint::SelectedEnvelope) -> Option<u64> {
    match envelope {
        pi_extension_protocol::endpoint::SelectedEnvelope::V1_0(envelope) => {
            match envelope.frame.as_ref()? {
                pi_extension_protocol::wire::v1_0::envelope::Frame::StreamItem(item) => {
                    Some(item.sequence)
                }
                _ => None,
            }
        }
        pi_extension_protocol::endpoint::SelectedEnvelope::V1_1(envelope) => {
            match envelope.frame.as_ref()? {
                pi_extension_protocol::wire::v1_1::envelope::Frame::StreamItem(item) => {
                    Some(item.sequence)
                }
                _ => None,
            }
        }
    }
}

/// Dropping a polled pump future may interrupt a partial host write or a read
/// whose cancellation behavior is unknown. Close before the pump can be reused;
/// only a completed return disarms this guard.
struct InFlight<'a> {
    pump: &'a mut ConnectionPump,
    armed: bool,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.pump.close();
        }
    }
}

impl ConnectionPump {
    pub fn new(
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
    ) -> Result<Self, PumpError> {
        let read_scratch_bytes = channel.max_read_scratch_bytes();
        config
            .max_read_chunk
            .get()
            .checked_add(read_scratch_bytes)
            .ok_or(PumpError::ReadBufferUnavailable)?;
        let mut buffered = Vec::new();
        buffered
            .try_reserve_exact(config.max_read_chunk.get())
            .map_err(|_| PumpError::ReadBufferUnavailable)?;
        buffered.resize(config.max_read_chunk.get(), 0);
        let read_storage_bound_bytes = buffered
            .capacity()
            .checked_add(read_scratch_bytes)
            .ok_or(PumpError::ReadBufferUnavailable)?;
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "resource",
                file!(),
                line!(),
                "ConnectionPump::new",
            );
            event.phase = Some("AwaitHello");
            event.capacity = Some(buffered.capacity());
            event.scratch_bytes = Some(read_scratch_bytes);
            event.bound_bytes = Some(read_storage_bound_bytes);
            source_observer::record(event);
        }
        let started_ms = clock.now_ms();
        let handshake_deadline_ms = started_ms
            .checked_add(host.limits().handshake_ms)
            .ok_or(PumpError::ClockFault)?;
        Ok(Self {
            endpoint: Endpoint::new(host, started_ms),
            channel,
            clock,
            config,
            handshake_deadline_ms,
            buffered,
            filled: 0,
            offset: 0,
            read_storage_bound_bytes,
            closed: false,
            #[cfg(test)]
            raw_frame: Vec::new(),
        })
    }

    pub fn phase(&self) -> EndpointPhase {
        if self.closed {
            EndpointPhase::Closed
        } else {
            self.endpoint.phase()
        }
    }

    /// Caller-owned read capacity plus the trusted host's declared read-call
    /// scratch. This excludes decoder, writer, and host receive-queue storage.
    pub fn read_storage_bound_bytes(&self) -> usize {
        self.read_storage_bound_bytes
    }

    /// Actual retained buffer capacity, which may exceed the slice length
    /// offered to the host on each read.
    pub fn read_buffer_capacity_bytes(&self) -> usize {
        self.buffered.capacity()
    }

    pub fn active_grant(&self) -> Option<&ActiveGrant> {
        if self.closed {
            None
        } else {
            self.endpoint.active_grant()
        }
    }

    /// Complete the handshake and return an owned selected grant at Ready,
    /// before waiting for active peer bytes. The caller can dispatch with this
    /// grant while `next_active` holds a mutable read borrow. A repeated call
    /// on an active connection does no I/O. Any coalesced active tail remains
    /// buffered for `next_active`.
    pub async fn drive_until_ready(&mut self) -> Result<ActiveGrant, PumpError> {
        if self.closed {
            return Err(PumpError::Closed);
        }
        if self.endpoint.phase() == EndpointPhase::Active {
            return Ok(self
                .endpoint
                .active_grant()
                .expect("active endpoint retains its grant")
                .clone());
        }
        let mut in_flight = InFlight {
            pump: self,
            armed: true,
        };
        let result = in_flight.pump.drive_next_active(true).await.map(|_| {
            in_flight
                .pump
                .endpoint
                .active_grant()
                .expect("Ready exposes the selected grant")
                .clone()
        });
        in_flight.armed = false;
        result
    }

    /// Drive Hello, Manifest and Ready, then return one active frame with its
    /// exact transmitted Envelope body length. Clean active EOF returns None.
    pub async fn next_active(&mut self) -> Result<Option<ActiveFrame>, PumpError> {
        if self.closed {
            return Err(PumpError::Closed);
        }
        let mut in_flight = InFlight {
            pump: self,
            armed: true,
        };
        let result = in_flight.pump.drive_next_active(false).await;
        in_flight.armed = false;
        result
    }

    async fn drive_next_active(
        &mut self,
        return_on_ready: bool,
    ) -> Result<Option<ActiveFrame>, PumpError> {
        loop {
            if self.endpoint.phase() != EndpointPhase::Active
                && self.clock.now_ms() >= self.handshake_deadline_ms
            {
                return self.fail(PumpError::HandshakeTimeout);
            }
            if self.offset == self.filled {
                self.filled = 0;
                self.offset = 0;
                let filled = match self.read_chunk().await {
                    Ok(filled) => filled,
                    Err(error) => return self.fail(error),
                };
                let Some(filled) = filled else {
                    let active = self.endpoint.phase() == EndpointPhase::Active;
                    let finished = self.endpoint.finish();
                    self.close();
                    return match finished {
                        Err(_) => Err(PumpError::TruncatedEof),
                        Ok(()) if active => Ok(None),
                        Ok(()) => Err(PumpError::HandshakeEof),
                    };
                };
                if filled == 0 || filled > self.buffered.len() {
                    return self.fail(PumpError::BadReadChunk);
                }
                self.filled = filled;
            }
            let now_ms = self.clock.now_ms();
            if self.endpoint.phase() != EndpointPhase::Active
                && now_ms >= self.handshake_deadline_ms
            {
                return self.fail(PumpError::HandshakeTimeout);
            }
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "phase",
                    file!(),
                    line!(),
                    "ConnectionPump::drive_next_active",
                );
                event.phase = Some(observed_phase(self.endpoint.phase()));
                event.outcome = Some("read");
                source_observer::record(event);
            }
            #[cfg(test)]
            let step_start = self.offset;
            let step = match self
                .endpoint
                .read_one(&self.buffered[self.offset..self.filled], now_ms)
            {
                Ok(step) if step.consumed > 0 => step,
                Ok(_) => return self.fail(PumpError::MalformedInput),
                Err(EndpointError::HandshakeTimeout) => {
                    return self.fail(PumpError::HandshakeTimeout);
                }
                Err(_) => return self.fail(PumpError::MalformedInput),
            };
            #[cfg(test)]
            self.raw_frame.extend_from_slice(
                &self.buffered[step_start..step_start.saturating_add(step.consumed)],
            );
            self.offset += step.consumed;
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "phase",
                    file!(),
                    line!(),
                    "ConnectionPump::drive_next_active",
                );
                event.phase = Some(observed_phase(self.endpoint.phase()));
                event.outcome = Some(match step.event.as_ref() {
                    Some(EndpointEvent::Send(_)) => "send",
                    Some(EndpointEvent::Ready) => "ready",
                    Some(EndpointEvent::Active(_)) => "active",
                    None => "step",
                });
                event.consumed = Some(step.consumed);
                event.body_len = match step.event.as_ref() {
                    Some(EndpointEvent::Active(frame)) => Some(frame.body_len),
                    _ => None,
                };
                source_observer::record(event);
            }
            match step.event {
                Some(EndpointEvent::Send(frame)) => {
                    #[cfg(test)]
                    self.raw_frame.clear();
                    if let Err(error) = self.write_frame(frame).await {
                        return self.fail(error);
                    }
                }
                Some(EndpointEvent::Ready) if return_on_ready => {
                    #[cfg(test)]
                    self.raw_frame.clear();
                    return Ok(None);
                }
                Some(EndpointEvent::Ready) => {
                    #[cfg(test)]
                    self.raw_frame.clear();
                }
                None => {}
                Some(EndpointEvent::Active(frame)) => {
                    #[cfg(test)]
                    {
                        let request_id = active_request_id(&frame.envelope);
                        let sequence = active_sequence(&frame.envelope);
                        let mut event = source_observer::SourceEvent::new(
                            "body_capture",
                            file!(),
                            line!(),
                            "ConnectionPump::drive_next_active",
                        );
                        event.phase = Some("Active");
                        event.body_len = Some(frame.body_len);
                        event.bound_bytes = Some(self.raw_frame.len());
                        event.request_id = request_id;
                        event.generation = self
                            .endpoint
                            .active_grant()
                            .map(|grant| grant.wire().connection_generation);
                        event.wire_sequence_before = sequence;
                        event.raw_body = self.raw_frame.len().checked_sub(frame.body_len).and_then(
                            |body_start| self.raw_frame.get(body_start..).map(ToOwned::to_owned),
                        );
                        event.bytes = event.raw_body.as_ref().map(Vec::len);
                        event.outcome = event
                            .raw_body
                            .as_ref()
                            .map_or(Some("unavailable"), |_| Some("retained"));
                        source_observer::record(event);
                        self.raw_frame.clear();
                    }
                    return Ok(Some(*frame));
                }
            }
        }
    }

    pub fn close(&mut self) {
        if !self.closed {
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "terminal",
                    file!(),
                    line!(),
                    "ConnectionPump::close",
                );
                event.phase = Some(observed_phase(self.endpoint.phase()));
                event.outcome = Some("close");
                source_observer::record(event);
            }
            self.closed = true;
            self.channel.close();
            self.filled = 0;
            self.offset = 0;
            #[cfg(test)]
            self.raw_frame.clear();
        }
    }

    fn fail<T>(&mut self, error: PumpError) -> Result<T, PumpError> {
        self.close();
        Err(error)
    }

    async fn read_chunk(&mut self) -> Result<Option<usize>, PumpError> {
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "read",
                file!(),
                line!(),
                "ConnectionPump::read_chunk",
            );
            event.phase = Some(observed_phase(self.endpoint.phase()));
            event.outcome = Some("start");
            source_observer::record(event);
        }
        let mut read = self.channel.read_chunk(&mut self.buffered);
        if self.endpoint.phase() == EndpointPhase::Active {
            let result = read.await.map_err(read_error);
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "read",
                    file!(),
                    line!(),
                    "ConnectionPump::read_chunk",
                );
                event.phase = Some(observed_phase(self.endpoint.phase()));
                event.outcome = Some("end");
                event.body_len = result.as_ref().ok().and_then(|filled| *filled);
                source_observer::record(event);
            }
            return result;
        }
        let deadline = self.handshake_deadline_ms;
        let mut timer = self.clock.sleep_until(deadline);
        let outcome = poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(PumpError::HandshakeTimeout));
            }
            read.as_mut()
                .poll(cx)
                .map(|result| result.map_err(read_error))
        })
        .await;
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "read",
                file!(),
                line!(),
                "ConnectionPump::read_chunk",
            );
            event.phase = Some(observed_phase(self.endpoint.phase()));
            event.outcome = Some("end");
            event.body_len = outcome.as_ref().ok().and_then(|filled| *filled);
            source_observer::record(event);
        }
        if self.clock.now_ms() >= deadline {
            return Err(PumpError::HandshakeTimeout);
        }
        if outcome == Err(PumpError::HandshakeTimeout) {
            return Err(PumpError::ClockFault);
        }
        outcome
    }

    async fn write_frame(&self, frame: EncodedFrame) -> Result<(), PumpError> {
        let write_deadline = self
            .clock
            .now_ms()
            .checked_add(self.config.write_timeout_ms.get())
            .ok_or(PumpError::ClockFault)?;
        let deadline = write_deadline.min(self.handshake_deadline_ms);
        let timeout = if deadline == self.handshake_deadline_ms {
            PumpError::HandshakeTimeout
        } else {
            PumpError::WriteTimeout
        };
        let mut write = self.channel.write_frame(frame);
        let mut timer = self.clock.sleep_until(deadline);
        let outcome = poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(timeout));
            }
            write
                .as_mut()
                .poll(cx)
                .map(|result| result.map_err(write_error))
        })
        .await;
        if self.clock.now_ms() >= deadline {
            return Err(timeout);
        }
        if outcome == Err(timeout) {
            return Err(PumpError::ClockFault);
        }
        outcome
    }
}

impl Drop for ConnectionPump {
    fn drop(&mut self) {
        self.close();
    }
}

fn read_error(error: ChannelError) -> PumpError {
    match error {
        ChannelError::Closed => PumpError::Closed,
        ChannelError::Failed => PumpError::ReadFailed,
    }
}

fn write_error(error: ChannelError) -> PumpError {
    match error {
        ChannelError::Closed => PumpError::Closed,
        ChannelError::Failed => PumpError::WriteFailed,
    }
}

#[cfg(test)]
mod retained_channel_tests {
    use super::*;

    struct RetainedProbe;

    impl OrderedByteChannel for RetainedProbe {
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

        fn close(&self) {}
    }

    impl RetainedOrderedByteChannel for RetainedProbe {
        fn take_notifications(&self) -> [Option<Waker>; 2] {
            [None, None]
        }

        fn close_retained(&self) -> [Option<Waker>; 2] {
            [None, None]
        }
    }

    #[test]
    fn retained_channel_capability_is_opt_in_and_detaches_two_slots() {
        let channel = RetainedProbe;
        assert!(
            channel
                .take_notifications()
                .into_iter()
                .all(|notification| notification.is_none())
        );
        assert!(
            channel
                .close_retained()
                .into_iter()
                .all(|notification| notification.is_none())
        );
        channel.close();
    }
}
