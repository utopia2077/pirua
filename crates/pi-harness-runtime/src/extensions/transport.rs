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

/// Private admission for a retained receive caller. After an in-scope read or
/// write future is dropped, `take_notifications` detaches every Waker the
/// channel retained for that operation. `close_retained` physically closes the
/// channel and detaches the same finite two seats. Neither method invokes or
/// drops a Waker, waits, allocates, or retains a borrowed read buffer. The
/// caller destroys the returned owner-created Wakers before clock client zero.
/// An arbitrary `OrderedByteChannel` does not satisfy this contract.
#[allow(dead_code)]
pub(crate) trait RetainedOrderedByteChannel: OrderedByteChannel {
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
            self.offset += step.consumed;
            match step.event {
                Some(EndpointEvent::Send(frame)) => {
                    if let Err(error) = self.write_frame(frame).await {
                        return self.fail(error);
                    }
                }
                Some(EndpointEvent::Ready) if return_on_ready => return Ok(None),
                Some(EndpointEvent::Ready) | None => {}
                Some(EndpointEvent::Active(frame)) => return Ok(Some(*frame)),
            }
        }
    }

    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.channel.close();
            self.filled = 0;
            self.offset = 0;
        }
    }

    fn fail<T>(&mut self, error: PumpError) -> Result<T, PumpError> {
        self.close();
        Err(error)
    }

    async fn read_chunk(&mut self) -> Result<Option<usize>, PumpError> {
        let mut read = self.channel.read_chunk(&mut self.buffered);
        if self.endpoint.phase() == EndpointPhase::Active {
            return read.await.map_err(read_error);
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
