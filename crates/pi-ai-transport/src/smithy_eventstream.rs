//! Bounded Smithy event-stream messages; no provider event interpretation.
use crate::{TransportError, TransportErrorKind};
use std::fmt;
#[derive(Clone, Copy, Debug)]
pub struct SmithyEventStreamLimits {
    pub max_buffer_bytes: usize,
    pub max_message_bytes: usize,
    pub max_header_bytes: usize,
    pub max_headers: usize,
}
impl Default for SmithyEventStreamLimits {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 2 * 1024 * 1024,
            max_message_bytes: 1024 * 1024,
            max_header_bytes: 65536,
            max_headers: 1024,
        }
    }
}
#[derive(Clone, PartialEq)]
pub enum SmithyHeaderValue {
    Boolean(bool),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long([u8; 8]),
    Bytes(Vec<u8>),
    String(String),
    Timestamp([u8; 8]),
    Uuid([u8; 16]),
}
impl fmt::Debug for SmithyHeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SmithyHeaderValue([REDACTED])")
    }
}
pub struct SmithyEventStreamMessage {
    pub headers: Vec<(String, SmithyHeaderValue)>,
    pub payload: Vec<u8>,
}
impl SmithyEventStreamMessage {
    pub fn get_header(&self, name: &str) -> Option<&SmithyHeaderValue> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
}
impl fmt::Debug for SmithyEventStreamMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SmithyEventStreamMessage([REDACTED])")
    }
}
pub struct SmithyEventStreamDecoder {
    buffer: Vec<u8>,
    limits: SmithyEventStreamLimits,
    finished: bool,
    failed: bool,
}
impl fmt::Debug for SmithyEventStreamDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SmithyEventStreamDecoder([REDACTED])")
    }
}
impl SmithyEventStreamDecoder {
    pub fn new(limits: SmithyEventStreamLimits) -> Self {
        Self {
            buffer: Vec::new(),
            limits,
            finished: false,
            failed: false,
        }
    }
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }
    pub fn clear(&mut self) {
        self.buffer = Vec::new();
        self.finished = true;
    }
    fn fail<T>(&mut self, kind: TransportErrorKind) -> Result<T, TransportError> {
        self.clear();
        self.failed = true;
        Err(TransportError::new(kind))
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
        if self.finished || self.failed {
            return self.fail(TransportErrorKind::Protocol);
        }
        if bytes.len()
            > self
                .limits
                .max_buffer_bytes
                .saturating_sub(self.buffer.len())
        {
            return self.fail(TransportErrorKind::LimitExceeded);
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }
    pub fn finish(&mut self) {
        self.finished = true;
    }
    pub fn next_frame(&mut self) -> Result<Option<SmithyEventStreamMessage>, TransportError> {
        if self.failed {
            return Ok(None);
        }
        if self.buffer.len() < 4 {
            if self.finished {
                self.buffer = Vec::new();
            }
            return Ok(None);
        }
        let total =
            u32::from_be_bytes(self.buffer[..4].try_into().expect("length prefix")) as usize;
        if total < 16 {
            return self.fail(TransportErrorKind::Protocol);
        }
        if total > self.limits.max_message_bytes {
            return self.fail(TransportErrorKind::LimitExceeded);
        }
        if self.buffer.len() < total {
            if self.finished {
                return self.fail(TransportErrorKind::Protocol);
            }
            return Ok(None);
        }
        let message = match decode(&self.buffer[..total], self.limits) {
            Ok(v) => v,
            Err(e) => return self.fail(e.kind()),
        };
        self.buffer.drain(..total);
        Ok(Some(message))
    }
}
fn protocol() -> TransportError {
    TransportError::new(TransportErrorKind::Protocol)
}
fn limit() -> TransportError {
    TransportError::new(TransportErrorKind::LimitExceeded)
}
fn bytes<'a>(
    input: &'a [u8],
    position: &mut usize,
    n: usize,
    end: usize,
) -> Result<&'a [u8], TransportError> {
    let next = position
        .checked_add(n)
        .filter(|next| *next <= end)
        .ok_or_else(protocol)?;
    let out = input.get(*position..next).ok_or_else(protocol)?;
    *position = next;
    Ok(out)
}
fn decode(
    input: &[u8],
    limits: SmithyEventStreamLimits,
) -> Result<SmithyEventStreamMessage, TransportError> {
    let total = input.len();
    let header_len = u32::from_be_bytes(input[4..8].try_into().expect("prelude")) as usize;
    if crc32(&input[..8]) != u32::from_be_bytes(input[8..12].try_into().expect("prelude CRC"))
        || crc32(&input[..total - 4])
            != u32::from_be_bytes(input[total - 4..].try_into().expect("message CRC"))
    {
        return Err(protocol());
    }
    let end = 12usize
        .checked_add(header_len)
        .filter(|n| *n <= total - 4)
        .ok_or_else(protocol)?;
    if header_len > limits.max_header_bytes {
        return Err(limit());
    }
    let mut position = 12;
    let mut count = 0;
    let mut charge = 0usize;
    let mut headers: Vec<(String, SmithyHeaderValue)> = Vec::new();
    while position < end {
        count += 1;
        if count > limits.max_headers {
            return Err(limit());
        }
        let length = usize::from(bytes(input, &mut position, 1, end)?[0]);
        let name =
            String::from_utf8_lossy(bytes(input, &mut position, length, total)?).into_owned();
        let tag = bytes(input, &mut position, 1, end)?[0];
        let (value, size) = match tag {
            0 => (SmithyHeaderValue::Boolean(true), 1),
            1 => (SmithyHeaderValue::Boolean(false), 1),
            2 => (
                SmithyHeaderValue::Byte(bytes(input, &mut position, 1, end)?[0] as i8),
                1,
            ),
            3 => (
                SmithyHeaderValue::Short(i16::from_be_bytes(
                    bytes(input, &mut position, 2, end)?
                        .try_into()
                        .expect("short"),
                )),
                2,
            ),
            4 => (
                SmithyHeaderValue::Integer(i32::from_be_bytes(
                    bytes(input, &mut position, 4, end)?
                        .try_into()
                        .expect("integer"),
                )),
                4,
            ),
            5 => (
                SmithyHeaderValue::Long(
                    bytes(input, &mut position, 8, total)?
                        .try_into()
                        .expect("long"),
                ),
                8,
            ),
            6 | 7 => {
                let length = usize::from(u16::from_be_bytes(
                    bytes(input, &mut position, 2, end)?
                        .try_into()
                        .expect("value length"),
                ));
                let value = bytes(input, &mut position, length, total)?;
                if tag == 6 {
                    (SmithyHeaderValue::Bytes(value.to_vec()), length)
                } else {
                    let text = String::from_utf8_lossy(value).into_owned();
                    let size = text.len();
                    (SmithyHeaderValue::String(text), size)
                }
            }
            8 => (
                SmithyHeaderValue::Timestamp(
                    bytes(input, &mut position, 8, total)?
                        .try_into()
                        .expect("timestamp"),
                ),
                8,
            ),
            9 => (
                SmithyHeaderValue::Uuid(
                    bytes(input, &mut position, 16, total)?
                        .try_into()
                        .expect("uuid"),
                ),
                16,
            ),
            _ => return Err(protocol()),
        };
        charge = charge
            .checked_add(name.len())
            .and_then(|n| n.checked_add(size))
            .filter(|n| *n <= limits.max_header_bytes)
            .ok_or_else(limit)?;
        if name == "__proto__" {
            continue;
        }
        if let Some((_, old)) = headers.iter_mut().find(|(k, _)| *k == name) {
            *old = value;
        } else {
            headers.push((name, value));
        }
    }
    headers.sort_by_key(|(name, _)| {
        name.parse::<u32>()
            .ok()
            .filter(|n| *n != u32::MAX && n.to_string() == *name)
            .map_or((1, 0), |n| (0, n))
    });
    Ok(SmithyEventStreamMessage {
        headers,
        payload: input[end..total - 4].to_vec(),
    })
}
/// CRC32 is an error-detection checksum, not a cryptographic primitive.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
