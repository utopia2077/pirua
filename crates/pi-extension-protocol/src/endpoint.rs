//! Ordered frame intake for host-established activation.
//!
//! One call consumes at most one Envelope. The caller must feed any unconsumed
//! tail again after handling the returned event. The two canonical core versions
//! have identical Envelope oneof arms and tags. Among Envelope-reachable typed
//! fields, only Manifest's optional label differs; ToolProgress lives in an
//! opaque stream payload. The selected Manifest DTO is decoded by `Activation`,
//! and active frames are returned as the selected core DTO.
//!
//! This layer does not authorize or dispatch active requests or settle effects.

use std::fmt;
use std::num::NonZeroUsize;

use prost::Message;

use crate::activation::{
    Activation, ActivationError, ActivationPhase, ActiveGrant, HostAuthority, WireGrant,
};
use crate::wire::framing::{self, DecodedFrame, EncodedFrame, FrameError, FrameReader};
use crate::wire::{v1_0, v1_1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointError {
    Frame(FrameError),
    Activation(ActivationError),
    UnexpectedFrame,
    HandshakeTimeout,
    InvalidSelectedEnvelope,
    Closed,
}

impl fmt::Display for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(error) => write!(f, "{error}"),
            Self::Activation(error) => write!(f, "{error}"),
            Self::UnexpectedFrame => f.write_str("Envelope arm is invalid in this phase"),
            Self::HandshakeTimeout => f.write_str("handshake deadline exceeded"),
            Self::InvalidSelectedEnvelope => f.write_str("invalid selected Envelope"),
            Self::Closed => f.write_str("endpoint is closed"),
        }
    }
}

impl std::error::Error for EndpointError {}

impl From<FrameError> for EndpointError {
    fn from(value: FrameError) -> Self {
        Self::Frame(value)
    }
}

impl From<ActivationError> for EndpointError {
    fn from(value: ActivationError) -> Self {
        Self::Activation(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointPhase {
    AwaitHello,
    AwaitManifest,
    AwaitReady,
    Active,
    Closed,
}

/// Active outer frame decoded through the selected core DTO.
#[derive(Debug)]
pub enum SelectedEnvelope {
    V1_0(v1_0::Envelope),
    V1_1(v1_1::Envelope),
}

#[derive(Debug)]
pub struct ActiveFrame {
    pub envelope: SelectedEnvelope,
    /// Exact received Envelope body length, excluding the varint prefix.
    pub body_len: usize,
}

/// One outbound handshake frame, activation transition, or bounded active frame.
#[derive(Debug)]
pub enum EndpointEvent {
    Send(EncodedFrame),
    Ready,
    Active(Box<ActiveFrame>),
}

#[derive(Debug)]
pub struct ReadStep {
    pub consumed: usize,
    pub event: Option<EndpointEvent>,
}

/// Host-owned connection state; the peer cannot alter its principal or limits.
pub struct Endpoint {
    activation: Activation,
    reader: Option<FrameReader<v1_1::Envelope>>,
    pre_limit: NonZeroUsize,
    post_limit: NonZeroUsize,
    started_ms: u64,
    handshake_ms: u64,
}

impl Endpoint {
    pub fn new(host: HostAuthority, started_ms: u64) -> Self {
        let limits = host.limits();
        let pre_limit = NonZeroUsize::new(limits.pre_handshake_frame_bytes as usize)
            .expect("validated finite pre-handshake limit");
        let post_limit =
            NonZeroUsize::new(limits.frame_bytes as usize).expect("validated finite frame limit");
        let handshake_ms = limits.handshake_ms;
        Self {
            activation: Activation::new(host, started_ms),
            reader: Some(FrameReader::new(pre_limit)),
            pre_limit,
            post_limit,
            started_ms,
            handshake_ms,
        }
    }

    pub fn phase(&self) -> EndpointPhase {
        if self.reader.is_none() {
            return EndpointPhase::Closed;
        }
        match self.activation.phase() {
            ActivationPhase::AwaitHello => EndpointPhase::AwaitHello,
            ActivationPhase::AwaitManifest => EndpointPhase::AwaitManifest,
            ActivationPhase::AwaitReady => EndpointPhase::AwaitReady,
            ActivationPhase::Active => EndpointPhase::Active,
            ActivationPhase::Rejected => EndpointPhase::Closed,
        }
    }

    /// Executable catalog access remains unavailable until Ready was processed.
    pub fn active_grant(&self) -> Option<&ActiveGrant> {
        self.reader.as_ref()?;
        self.activation.active_grant()
    }

    /// Freeze the authenticated catalog into a separate finite shared owner.
    /// Transport loss must be forwarded to that owner so retained receipts survive.
    pub fn profile_owner(
        &self,
        reservations: crate::profile_live::Reservations,
        now_ms: u64,
        connection_remaining_ms: u64,
    ) -> Result<crate::profile_live::Owner, crate::profile_live::Error> {
        let grant = self
            .active_grant()
            .ok_or(crate::profile_live::Error::HostContract)?;
        crate::profile_live::Owner::new(
            grant.clone(),
            reservations,
            now_ms,
            connection_remaining_ms,
        )
    }

    /// Consumes at most one frame and leaves every later byte with the caller.
    pub fn read_one(&mut self, input: &[u8], now_ms: u64) -> Result<ReadStep, EndpointError> {
        if self.reader.is_none() {
            return Err(EndpointError::Closed);
        }
        if self.activation.phase() != ActivationPhase::Active
            && now_ms
                .checked_sub(self.started_ms)
                .is_none_or(|elapsed| elapsed > self.handshake_ms)
        {
            self.close();
            return Err(EndpointError::HandshakeTimeout);
        }
        let step = match self.reader.as_mut().expect("checked open").read_one(input) {
            Ok(step) => step,
            Err(error) => {
                self.close();
                return Err(error.into());
            }
        };
        let event = match step.frame {
            None => None,
            Some(frame) => match self.process(frame, now_ms) {
                Ok(event) => Some(event),
                Err(error) => {
                    self.close();
                    return Err(error);
                }
            },
        };
        Ok(ReadStep {
            consumed: step.consumed,
            event,
        })
    }

    /// Transport EOF closes this generation, including an incomplete frame.
    pub fn finish(&mut self) -> Result<(), EndpointError> {
        let mut reader = self.reader.take().ok_or(EndpointError::Closed)?;
        reader.finish().map_err(EndpointError::Frame)
    }

    fn close(&mut self) {
        self.reader = None;
    }

    fn process(
        &mut self,
        frame: DecodedFrame<v1_1::Envelope>,
        now_ms: u64,
    ) -> Result<EndpointEvent, EndpointError> {
        let DecodedFrame { envelope, body_len } = frame;
        match self.activation.phase() {
            ActivationPhase::AwaitHello => {
                let Some(v1_1::envelope::Frame::Hello(hello)) = envelope.frame else {
                    return Err(EndpointError::UnexpectedFrame);
                };
                let bytes = encode_inner(&hello, self.pre_limit)?;
                let ack = self.activation.receive_hello(&bytes, now_ms)?;
                let selected = ack
                    .selected_protocol
                    .expect("accepted Hello selects a version");
                let outbound = if selected.minor == 0 {
                    let old = v1_0::HelloAck {
                        selected_protocol: Some(v1_0::Version {
                            major: selected.major,
                            minor: selected.minor,
                        }),
                    };
                    framing::encode(
                        &v1_0::Envelope {
                            frame: Some(v1_0::envelope::Frame::HelloAck(old)),
                        },
                        self.pre_limit,
                    )?
                } else {
                    framing::encode(
                        &v1_1::Envelope {
                            frame: Some(v1_1::envelope::Frame::HelloAck(ack)),
                        },
                        self.pre_limit,
                    )?
                };
                Ok(EndpointEvent::Send(outbound))
            }
            ActivationPhase::AwaitManifest => {
                let Some(v1_1::envelope::Frame::Manifest(manifest)) = envelope.frame else {
                    return Err(EndpointError::UnexpectedFrame);
                };
                let bytes = encode_inner(&manifest, self.pre_limit)?;
                let outbound = match self.activation.receive_manifest(&bytes, now_ms)? {
                    WireGrant::V1_0(grant) => framing::encode(
                        &v1_0::Envelope {
                            frame: Some(v1_0::envelope::Frame::Grant(grant)),
                        },
                        self.pre_limit,
                    )?,
                    WireGrant::V1_1(grant) => framing::encode(
                        &v1_1::Envelope {
                            frame: Some(v1_1::envelope::Frame::Grant(grant)),
                        },
                        self.pre_limit,
                    )?,
                };
                Ok(EndpointEvent::Send(outbound))
            }
            ActivationPhase::AwaitReady => {
                let Some(v1_1::envelope::Frame::Ready(_)) = envelope.frame else {
                    return Err(EndpointError::UnexpectedFrame);
                };
                self.activation.receive_ready(&[], now_ms)?;
                self.reader
                    .as_mut()
                    .expect("open at frame boundary")
                    .use_negotiated_limit(self.post_limit)?;
                Ok(EndpointEvent::Ready)
            }
            ActivationPhase::Active => {
                if matches!(
                    envelope.frame,
                    Some(
                        v1_1::envelope::Frame::Hello(_)
                            | v1_1::envelope::Frame::HelloAck(_)
                            | v1_1::envelope::Frame::Manifest(_)
                            | v1_1::envelope::Frame::Grant(_)
                            | v1_1::envelope::Frame::Ready(_)
                    )
                ) {
                    return Err(EndpointError::UnexpectedFrame);
                }
                let selected = self.activation.selected_protocol().expect("active version");
                let envelope = if selected.minor == 0 {
                    let bytes = encode_inner(&envelope, self.post_limit)?;
                    SelectedEnvelope::V1_0(
                        v1_0::Envelope::decode(bytes.as_slice())
                            .map_err(|_| EndpointError::InvalidSelectedEnvelope)?,
                    )
                } else {
                    SelectedEnvelope::V1_1(envelope)
                };
                Ok(EndpointEvent::Active(Box::new(ActiveFrame {
                    envelope,
                    body_len,
                })))
            }
            ActivationPhase::Rejected => Err(EndpointError::Closed),
        }
    }
}

fn encode_inner<M: Message>(message: &M, limit: NonZeroUsize) -> Result<Vec<u8>, EndpointError> {
    let length = message.encoded_len();
    if length > limit.get() {
        return Err(EndpointError::Frame(FrameError::FrameTooLarge {
            length: u64::try_from(length).unwrap_or(u64::MAX),
            limit: limit.get(),
        }));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| EndpointError::Frame(FrameError::AllocationFailed))?;
    message
        .encode(&mut bytes)
        .map_err(|_| EndpointError::Frame(FrameError::InvalidEnvelope))?;
    Ok(bytes)
}
