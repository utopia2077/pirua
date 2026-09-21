//! Host-owned ordered bytes and finite-time activation intake.
//!
//! The host authenticates the peer and supplies the channel, monotonic clock,
//! limits and catalog before constructing this pump. This module neither opens
//! an OS resource nor dispatches an active request or touches a Session.

use std::fmt;
use std::future::poll_fn;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::task::Poll;

use pi_ai_core::BoxFuture;
use pi_extension_protocol::activation::{ActiveGrant, HostAuthority};
use pi_extension_protocol::endpoint::{
    ActiveFrame, Endpoint, EndpointError, EndpointEvent, EndpointPhase,
};
use pi_extension_protocol::wire::framing::EncodedFrame;

/// Content-free host-channel failures. Implementations must not expose peer bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelError {
    Closed,
    Failed,
}

/// One host-owned ordered duplex byte channel for one connection generation.
///
/// Exactly one reader calls `read_chunk`. It returns a nonempty chunk no larger
/// than `max_bytes`, EOF, or a fixed error. `write_frame` completes the entire
/// frame in FIFO order or fails; an implementation must never interleave frame
/// bytes or buffer an unbounded number of writes. Closing is idempotent and
/// wakes pending reads and writes. A host may close its own channel externally.
pub trait OrderedByteChannel: Send + Sync {
    fn read_chunk(
        &self,
        max_bytes: NonZeroUsize,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, ChannelError>>;

    fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>>;

    fn close(&self);
}

/// Host-supplied receiver-local monotonic milliseconds and wakeable timer.
/// `sleep_until` must not complete before `deadline_ms` and must wake when due.
pub trait MonotonicClock: Send + Sync {
    fn now_ms(&self) -> u64;

    fn sleep_until(&self, deadline_ms: u64) -> BoxFuture<'_, ()>;
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
    offset: usize,
    closed: bool,
}

/// Dropping a polled `next_active` future may interrupt a partial host write or
/// a read whose cancellation behavior is unknown. Close before the pump can be
/// reused; only a completed return disarms this guard.
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
            buffered: Vec::new(),
            offset: 0,
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

    pub fn active_grant(&self) -> Option<&ActiveGrant> {
        if self.closed {
            None
        } else {
            self.endpoint.active_grant()
        }
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
        let result = in_flight.pump.drive_next_active().await;
        in_flight.armed = false;
        result
    }

    async fn drive_next_active(&mut self) -> Result<Option<ActiveFrame>, PumpError> {
        loop {
            if self.endpoint.phase() != EndpointPhase::Active
                && self.clock.now_ms() >= self.handshake_deadline_ms
            {
                return self.fail(PumpError::HandshakeTimeout);
            }
            if self.offset == self.buffered.len() {
                self.buffered.clear();
                self.offset = 0;
                let chunk = match self.read_chunk().await {
                    Ok(chunk) => chunk,
                    Err(error) => return self.fail(error),
                };
                let Some(chunk) = chunk else {
                    let active = self.endpoint.phase() == EndpointPhase::Active;
                    let finished = self.endpoint.finish();
                    self.close();
                    return match finished {
                        Err(_) => Err(PumpError::TruncatedEof),
                        Ok(()) if active => Ok(None),
                        Ok(()) => Err(PumpError::HandshakeEof),
                    };
                };
                if chunk.is_empty() || chunk.len() > self.config.max_read_chunk.get() {
                    return self.fail(PumpError::BadReadChunk);
                }
                self.buffered = chunk;
            }
            let now_ms = self.clock.now_ms();
            if self.endpoint.phase() != EndpointPhase::Active
                && now_ms >= self.handshake_deadline_ms
            {
                return self.fail(PumpError::HandshakeTimeout);
            }
            let step = match self
                .endpoint
                .read_one(&self.buffered[self.offset..], now_ms)
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
                Some(EndpointEvent::Ready) | None => {}
                Some(EndpointEvent::Active(frame)) => return Ok(Some(*frame)),
            }
        }
    }

    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.channel.close();
            self.buffered.clear();
            self.offset = 0;
        }
    }

    fn fail<T>(&mut self, error: PumpError) -> Result<T, PumpError> {
        self.close();
        Err(error)
    }

    async fn read_chunk(&self) -> Result<Option<Vec<u8>>, PumpError> {
        let mut read = self.channel.read_chunk(self.config.max_read_chunk);
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
