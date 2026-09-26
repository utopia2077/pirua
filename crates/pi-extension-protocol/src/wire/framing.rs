//! Finite, incremental framing for encoded protobuf Envelope bodies.
//!
//! The host supplies both limits. A peer's frame contents never change them.
//! Semantic envelope admission, selected-minor rules, and authenticated endpoint
//! state remain the responsibility of the endpoint using this physical codec.

use std::marker::PhantomData;
use std::num::NonZeroUsize;

use prost::Message;

use super::{v1_0, v1_1};

/// The two checked-in Envelope DTOs accepted by the physical codec.
pub trait EnvelopeMessage: Message + Default {
    fn has_frame(&self) -> bool;
}

impl EnvelopeMessage for v1_0::Envelope {
    fn has_frame(&self) -> bool {
        self.frame.is_some()
    }
}

impl EnvelopeMessage for v1_1::Envelope {
    fn has_frame(&self) -> bool {
        self.frame.is_some()
    }
}

/// Errors contain lengths and limits, never peer-supplied payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    ZeroLength,
    NonCanonicalLength,
    LengthOverflow,
    FrameTooLarge { length: u64, limit: usize },
    InvalidEnvelope,
    AllocationFailed,
    TruncatedPrefix,
    TruncatedBody { remaining: usize },
    TransitionNotAtBoundary,
    AlreadyNegotiated,
    Poisoned,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLength => write!(f, "zero-length frame"),
            Self::NonCanonicalLength => write!(f, "noncanonical frame length"),
            Self::LengthOverflow => write!(f, "frame length overflow"),
            Self::FrameTooLarge { length, limit } => {
                write!(f, "frame body length {length} exceeds limit {limit}")
            }
            Self::InvalidEnvelope => write!(f, "invalid Envelope body"),
            Self::AllocationFailed => write!(f, "frame body allocation failed"),
            Self::TruncatedPrefix => write!(f, "truncated frame length"),
            Self::TruncatedBody { remaining } => {
                write!(f, "truncated frame body with {remaining} bytes remaining")
            }
            Self::TransitionNotAtBoundary => {
                write!(f, "frame limit transition requires a frame boundary")
            }
            Self::AlreadyNegotiated => write!(f, "frame limit already negotiated"),
            Self::Poisoned => write!(f, "frame reader closed after error"),
        }
    }
}

impl std::error::Error for FrameError {}

/// A decoded Envelope and the exact transmitted body length for stream credit.
#[derive(Debug)]
pub struct DecodedFrame<E> {
    pub envelope: E,
    pub body_len: usize,
}

/// One incremental read, stopping at the first complete frame boundary.
#[derive(Debug)]
pub struct ReadStep<E> {
    /// Number of input bytes consumed. The caller may retain the remaining tail.
    pub consumed: usize,
    pub frame: Option<DecodedFrame<E>>,
}

/// A framing failure or an undersized caller-supplied body buffer.
///
/// Storage capacity is a local configuration error, not a malformed peer frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawReadError {
    Frame(FrameError),
    StorageTooSmall { required: usize, capacity: usize },
}

impl std::fmt::Display for RawReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Frame(error) => error.fmt(f),
            Self::StorageTooSmall { required, capacity } => {
                write!(
                    f,
                    "raw frame storage capacity {capacity} is below limit {required}"
                )
            }
        }
    }
}

impl std::error::Error for RawReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frame(error) => Some(error),
            Self::StorageTooSmall { .. } => None,
        }
    }
}

/// A transmitted body borrowed from the reader's caller-supplied storage.
#[derive(Debug)]
pub struct RawFrame<'r> {
    /// Exact body bytes, excluding the length prefix.
    pub body: &'r [u8],
    pub body_len: usize,
}

/// One raw read, stopping at the first complete frame boundary.
#[derive(Debug)]
pub struct RawReadStep<'r> {
    /// Number of input bytes consumed, including the length prefix.
    pub consumed: usize,
    pub frame: Option<RawFrame<'r>>,
}

/// Incrementally reads raw transmitted bodies into caller-supplied storage.
///
/// The returned body borrows `self`, preventing another read, transition, or
/// storage reuse while it is retained. This reader does not decode Envelopes.
/// Any framing error permanently closes the reader.
pub struct RawFrameReader<'buf> {
    limit: NonZeroUsize,
    negotiated: bool,
    prefix: [u8; 10],
    prefix_len: usize,
    expected_body_len: Option<usize>,
    body: &'buf mut [u8],
    body_pos: usize,
    poisoned: bool,
}

impl<'buf> RawFrameReader<'buf> {
    /// Refuses a pre-handshake limit that exceeds the supplied storage.
    pub fn new(pre_limit: NonZeroUsize, backing: &'buf mut [u8]) -> Result<Self, RawReadError> {
        if pre_limit.get() > backing.len() {
            return Err(RawReadError::StorageTooSmall {
                required: pre_limit.get(),
                capacity: backing.len(),
            });
        }
        Ok(Self {
            limit: pre_limit,
            negotiated: false,
            prefix: [0; 10],
            prefix_len: 0,
            expected_body_len: None,
            body: backing,
            body_pos: 0,
            poisoned: false,
        })
    }

    /// Switches once, at a frame boundary, to a host-granted limit that fits
    /// the same storage. A rejected transition leaves the old limit active.
    pub fn use_negotiated_limit(&mut self, post_limit: NonZeroUsize) -> Result<(), RawReadError> {
        if self.poisoned {
            return Err(RawReadError::Frame(FrameError::Poisoned));
        }
        if self.negotiated {
            return Err(RawReadError::Frame(FrameError::AlreadyNegotiated));
        }
        if self.prefix_len != 0 || self.expected_body_len.is_some() {
            return Err(RawReadError::Frame(FrameError::TransitionNotAtBoundary));
        }
        if post_limit.get() > self.body.len() {
            return Err(RawReadError::StorageTooSmall {
                required: post_limit.get(),
                capacity: self.body.len(),
            });
        }
        self.limit = post_limit;
        self.negotiated = true;
        Ok(())
    }

    /// Consumes no bytes after the first complete frame, even if `input`
    /// contains a coalesced tail. Input need not outlive the returned body.
    pub fn read_one<'r>(&'r mut self, input: &[u8]) -> Result<RawReadStep<'r>, RawReadError> {
        if self.poisoned {
            return Err(RawReadError::Frame(FrameError::Poisoned));
        }

        let mut consumed = 0;
        while consumed < input.len() {
            if let Some(expected) = self.expected_body_len {
                let take = (expected - self.body_pos).min(input.len() - consumed);
                self.body[self.body_pos..self.body_pos + take]
                    .copy_from_slice(&input[consumed..consumed + take]);
                self.body_pos += take;
                consumed += take;
                if self.body_pos == expected {
                    self.expected_body_len = None;
                    self.prefix_len = 0;
                    self.body_pos = 0;
                    return Ok(RawReadStep {
                        consumed,
                        frame: Some(RawFrame {
                            body: &self.body[..expected],
                            body_len: expected,
                        }),
                    });
                }
                continue;
            }

            let byte = input[consumed];
            consumed += 1;
            if self.prefix_len == self.prefix.len() {
                return self.fail(FrameError::LengthOverflow);
            }
            self.prefix[self.prefix_len] = byte;
            self.prefix_len += 1;

            if self.prefix_len == 10 && (byte & 0xfe != 0) {
                return self.fail(FrameError::LengthOverflow);
            }
            if byte & 0x80 != 0 {
                continue;
            }
            if self.prefix_len > 1 && byte == 0 {
                return self.fail(FrameError::NonCanonicalLength);
            }

            let mut length = 0_u64;
            for (index, byte) in self.prefix[..self.prefix_len].iter().enumerate() {
                length |= u64::from(byte & 0x7f) << (index * 7);
            }
            if length == 0 {
                return self.fail(FrameError::ZeroLength);
            }
            if length > u64::try_from(self.limit.get()).unwrap_or(u64::MAX) {
                return self.fail(FrameError::FrameTooLarge {
                    length,
                    limit: self.limit.get(),
                });
            }
            let expected = match usize::try_from(length) {
                Ok(expected) => expected,
                Err(_) => return self.fail(FrameError::LengthOverflow),
            };
            self.expected_body_len = Some(expected);
        }
        Ok(RawReadStep {
            consumed,
            frame: None,
        })
    }

    /// Reports partial input at EOF and poisons the reader; a clean boundary
    /// remains reusable.
    pub fn finish(&mut self) -> Result<(), RawReadError> {
        if self.poisoned {
            return Err(RawReadError::Frame(FrameError::Poisoned));
        }
        if let Some(expected) = self.expected_body_len {
            return self.fail(FrameError::TruncatedBody {
                remaining: expected - self.body_pos,
            });
        }
        if self.prefix_len != 0 {
            return self.fail(FrameError::TruncatedPrefix);
        }
        Ok(())
    }

    fn fail<T>(&mut self, error: FrameError) -> Result<T, RawReadError> {
        self.poisoned = true;
        self.expected_body_len = None;
        self.prefix_len = 0;
        self.body_pos = 0;
        Err(RawReadError::Frame(error))
    }
}

/// A shortest-form encoded frame and its credit-relevant body length.
#[derive(Debug)]
pub struct EncodedFrame {
    pub bytes: Vec<u8>,
    pub body_len: usize,
}

/// Incrementally reads concatenated frames without retaining caller chunks.
///
/// `read_one` stops at every frame boundary. The caller can process a grant
/// before feeding any coalesced tail under the newly granted limit. Any decode
/// error permanently closes this reader; callers must close the associated
/// generation too.
pub struct FrameReader<E> {
    limit: NonZeroUsize,
    negotiated: bool,
    prefix: [u8; 10],
    prefix_len: usize,
    expected_body_len: Option<usize>,
    body: Vec<u8>,
    poisoned: bool,
    envelope: PhantomData<E>,
}

impl<E: EnvelopeMessage> FrameReader<E> {
    /// `pre_handshake_limit` is fixed by the host before peer bytes arrive.
    pub fn new(pre_handshake_limit: NonZeroUsize) -> Self {
        Self {
            limit: pre_handshake_limit,
            negotiated: false,
            prefix: [0; 10],
            prefix_len: 0,
            expected_body_len: None,
            body: Vec::new(),
            poisoned: false,
            envelope: PhantomData,
        }
    }

    /// Switches once, at a frame boundary, to the finite host-granted limit.
    pub fn use_negotiated_limit(&mut self, max_body_bytes: NonZeroUsize) -> Result<(), FrameError> {
        if self.poisoned {
            return Err(FrameError::Poisoned);
        }
        if self.negotiated {
            return Err(FrameError::AlreadyNegotiated);
        }
        if self.prefix_len != 0 || self.expected_body_len.is_some() {
            return Err(FrameError::TransitionNotAtBoundary);
        }
        self.limit = max_body_bytes;
        self.negotiated = true;
        Ok(())
    }

    /// Consumes no bytes after the first complete frame, even if more frames
    /// are present in `input`.
    pub fn read_one(&mut self, input: &[u8]) -> Result<ReadStep<E>, FrameError> {
        if self.poisoned {
            return Err(FrameError::Poisoned);
        }

        let mut consumed = 0;
        while consumed < input.len() {
            if let Some(expected) = self.expected_body_len {
                let take = (expected - self.body.len()).min(input.len() - consumed);
                self.body
                    .extend_from_slice(&input[consumed..consumed + take]);
                consumed += take;
                if self.body.len() == expected {
                    let body = std::mem::take(&mut self.body);
                    self.expected_body_len = None;
                    self.prefix_len = 0;
                    let envelope = match E::decode(body.as_slice()) {
                        Ok(envelope) if envelope.has_frame() => envelope,
                        _ => return self.fail(FrameError::InvalidEnvelope),
                    };
                    return Ok(ReadStep {
                        consumed,
                        frame: Some(DecodedFrame {
                            envelope,
                            body_len: expected,
                        }),
                    });
                }
                continue;
            }

            let byte = input[consumed];
            consumed += 1;
            if self.prefix_len == self.prefix.len() {
                return self.fail(FrameError::LengthOverflow);
            }
            self.prefix[self.prefix_len] = byte;
            self.prefix_len += 1;

            if self.prefix_len == 10 && (byte & 0xfe != 0) {
                return self.fail(FrameError::LengthOverflow);
            }
            if byte & 0x80 != 0 {
                continue;
            }
            if self.prefix_len > 1 && byte == 0 {
                return self.fail(FrameError::NonCanonicalLength);
            }

            let mut length = 0_u64;
            for (index, byte) in self.prefix[..self.prefix_len].iter().enumerate() {
                length |= u64::from(byte & 0x7f) << (index * 7);
            }
            if length == 0 {
                return self.fail(FrameError::ZeroLength);
            }
            if length > u64::try_from(self.limit.get()).unwrap_or(u64::MAX) {
                return self.fail(FrameError::FrameTooLarge {
                    length,
                    limit: self.limit.get(),
                });
            }
            let expected = match usize::try_from(length) {
                Ok(expected) => expected,
                Err(_) => return self.fail(FrameError::LengthOverflow),
            };
            if self.body.try_reserve_exact(expected).is_err() {
                return self.fail(FrameError::AllocationFailed);
            }
            self.expected_body_len = Some(expected);
        }
        Ok(ReadStep {
            consumed,
            frame: None,
        })
    }

    /// Checks for incomplete input at transport EOF. An incomplete frame
    /// permanently closes the reader; a successful check leaves it reusable.
    pub fn finish(&mut self) -> Result<(), FrameError> {
        if self.poisoned {
            return Err(FrameError::Poisoned);
        }
        if let Some(expected) = self.expected_body_len {
            return self.fail(FrameError::TruncatedBody {
                remaining: expected - self.body.len(),
            });
        }
        if self.prefix_len != 0 {
            return self.fail(FrameError::TruncatedPrefix);
        }
        Ok(())
    }

    fn fail<T>(&mut self, error: FrameError) -> Result<T, FrameError> {
        self.poisoned = true;
        self.expected_body_len = None;
        self.prefix_len = 0;
        self.body = Vec::new();
        Err(error)
    }
}

/// Encodes one valid Envelope under the selected finite body ceiling.
pub fn encode<E: EnvelopeMessage>(
    envelope: &E,
    max_body_bytes: NonZeroUsize,
) -> Result<EncodedFrame, FrameError> {
    if !envelope.has_frame() {
        return Err(FrameError::InvalidEnvelope);
    }
    let body_len = envelope.encoded_len();
    if body_len == 0 {
        return Err(FrameError::ZeroLength);
    }
    if body_len > max_body_bytes.get() {
        return Err(FrameError::FrameTooLarge {
            length: u64::try_from(body_len).unwrap_or(u64::MAX),
            limit: max_body_bytes.get(),
        });
    }
    let mut length = u64::try_from(body_len).map_err(|_| FrameError::LengthOverflow)?;
    let mut prefix = [0_u8; 10];
    let mut prefix_len = 0;
    loop {
        let mut byte = (length & 0x7f) as u8;
        length >>= 7;
        if length != 0 {
            byte |= 0x80;
        }
        prefix[prefix_len] = byte;
        prefix_len += 1;
        if length == 0 {
            break;
        }
    }

    let total_len = prefix_len
        .checked_add(body_len)
        .ok_or(FrameError::LengthOverflow)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total_len)
        .map_err(|_| FrameError::AllocationFailed)?;
    bytes.extend_from_slice(&prefix[..prefix_len]);
    envelope
        .encode(&mut bytes)
        .map_err(|_| FrameError::InvalidEnvelope)?;
    Ok(EncodedFrame { bytes, body_len })
}
