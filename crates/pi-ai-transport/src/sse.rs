use crate::{TransportError, TransportErrorKind, is_ecma_whitespace};
use std::fmt;

/// Framing only: provider JSON validation/repair remains adapter-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SseProfile {
    PiMessages,
    Codex,
    Mistral,
    Anthropic,
}
#[derive(Clone, Copy, Debug)]
pub struct SseLimits {
    /// Intake budget: all queued bytes including the next pushed chunk.
    pub max_buffer_bytes: usize,
    /// Normalized frame bytes, excluding BOM and terminating delimiter.
    pub max_frame_bytes: usize,
    /// Maximum emitted frames between pushes. There is no allocated frame queue.
    pub max_frames_per_feed: usize,
}
impl Default for SseLimits {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 1_048_576,
            max_frame_bytes: 262_144,
            max_frames_per_feed: 1024,
        }
    }
}
#[derive(Clone, Eq, PartialEq)]
pub struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}
impl fmt::Debug for SseFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SseFrame([REDACTED])")
    }
}
pub struct SseDecoder {
    profile: SseProfile,
    limits: SseLimits,
    buffer: Vec<u8>,
    input_ended: bool,
    failed: bool,
    bom_pending: bool,
    emitted: usize,
}
impl fmt::Debug for SseDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SseDecoder([REDACTED])")
    }
}
impl SseDecoder {
    pub fn new(profile: SseProfile, limits: SseLimits) -> Self {
        Self {
            profile,
            limits,
            buffer: Vec::new(),
            input_ended: false,
            failed: false,
            bom_pending: true,
            emitted: 0,
        }
    }
    /// Bounded intake only. Drain next_frame before reading more bytes; stop
    /// after the adapter terminal without examining irrelevant trailing frames.
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), TransportError> {
        if self.failed || self.input_ended {
            return Err(TransportError::new(TransportErrorKind::Closed));
        }
        if chunk.len()
            > self
                .limits
                .max_buffer_bytes
                .saturating_sub(self.buffer.len())
        {
            return self.fail();
        }
        self.buffer.extend_from_slice(chunk);
        self.emitted = 0;
        Ok(())
    }
    /// EOF is marked without decoding; next_frame applies trailing-frame policy.
    pub fn end_input(&mut self) {
        self.input_ended = true;
    }
    pub fn next_frame(&mut self) -> Result<Option<SseFrame>, TransportError> {
        if self.failed {
            return Err(TransportError::new(TransportErrorKind::Closed));
        }
        if self.bom_pending {
            const BOM: &[u8] = b"\xef\xbb\xbf";
            if self.buffer.len() < 3 && BOM.starts_with(&self.buffer) && !self.input_ended {
                return Ok(None);
            }
            if self.buffer.starts_with(BOM) {
                self.buffer.drain(..3);
            }
            self.bom_pending = false;
        }
        if self.profile == SseProfile::PiMessages {
            let mut normalized = Vec::with_capacity(self.buffer.len());
            let mut i = 0;
            while i < self.buffer.len() {
                if self.buffer.get(i..i + 2) == Some(b"\r\n") {
                    normalized.push(b'\n');
                    i += 2;
                } else {
                    normalized.push(self.buffer[i]);
                    i += 1;
                }
            }
            self.buffer = normalized;
        }
        loop {
            let (index, length) = if let Some(next) = boundary(&self.buffer, self.profile) {
                next
            } else {
                if !self.input_ended {
                    let prefix = delimiter_prefix_len(&self.buffer, self.profile);
                    if self.buffer.len() - prefix > self.limits.max_frame_bytes {
                        return self.fail();
                    }
                    return Ok(None);
                }
                if self.profile == SseProfile::Codex {
                    self.buffer.clear();
                    return Ok(None);
                }
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                (self.buffer.len(), 0)
            };
            if index > self.limits.max_frame_bytes {
                return self.fail();
            }
            let frame = frame(&self.buffer[..index], self.profile);
            self.buffer.drain(..index + length);
            if let Some(frame) = frame {
                if self.emitted >= self.limits.max_frames_per_feed {
                    return self.fail();
                }
                self.emitted += 1;
                return Ok(Some(frame));
            }
        }
    }
    fn fail<T>(&mut self) -> Result<T, TransportError> {
        self.failed = true;
        self.buffer.clear();
        Err(TransportError::new(TransportErrorKind::LimitExceeded))
    }
}
const MIXED_ENDINGS: &[&[u8]] = &[
    b"\r\n\r\n",
    b"\r\n\r",
    b"\r\n\n",
    b"\r\r\n",
    b"\n\r\n",
    b"\r\r",
    b"\n\r",
    b"\n\n",
];
fn endings(profile: SseProfile) -> &'static [&'static [u8]] {
    match profile {
        SseProfile::Codex => &[b"\n\n"],
        SseProfile::PiMessages => &[b"\n\n", b"\r\n\r\n", b"\n\r\n", b"\r\n\n"],
        _ => MIXED_ENDINGS,
    }
}
fn delimiter_prefix_len(bytes: &[u8], profile: SseProfile) -> usize {
    endings(profile)
        .iter()
        .flat_map(|ending| (1..ending.len()).map(move |len| &ending[..len]))
        .filter(|prefix| bytes.ends_with(prefix))
        .map(<[u8]>::len)
        .max()
        .unwrap_or(0)
}
fn boundary(bytes: &[u8], profile: SseProfile) -> Option<(usize, usize)> {
    if matches!(profile, SseProfile::PiMessages | SseProfile::Codex) {
        return bytes.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    }
    for i in 0..bytes.len() {
        for ending in MIXED_ENDINGS {
            if bytes[i..].starts_with(ending) {
                return Some((i, ending.len()));
            }
        }
    }
    None
}
fn frame(bytes: &[u8], profile: SseProfile) -> Option<SseFrame> {
    let raw = String::from_utf8_lossy(bytes);
    let normalized = if matches!(profile, SseProfile::Mistral | SseProfile::Anthropic) {
        raw.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        raw.into_owned()
    };
    let mut event = None;
    let mut data = Vec::new();
    for line in normalized.split('\n') {
        if profile == SseProfile::Anthropic {
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => event = Some(value.to_owned()),
                "data" => data.push(value),
                _ => {}
            }
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.strip_prefix(' ').unwrap_or(value).to_owned());
        }
        if let Some(value) = line.strip_prefix("data:") {
            if profile == SseProfile::PiMessages && !data.is_empty() {
                continue;
            }
            data.push(if profile == SseProfile::Mistral {
                value.trim_start_matches(is_ecma_whitespace)
            } else {
                value.trim_matches(is_ecma_whitespace)
            });
        }
    }
    if profile == SseProfile::Anthropic {
        return if data.is_empty() && event.as_deref().is_none_or(str::is_empty) {
            None
        } else {
            Some(SseFrame {
                event,
                data: data.join("\n"),
            })
        };
    }
    let data = data.join("\n").trim_matches(is_ecma_whitespace).to_owned();
    if data.is_empty()
        || (data == "[DONE]" && matches!(profile, SseProfile::PiMessages | SseProfile::Codex))
    {
        None
    } else {
        Some(SseFrame { event, data })
    }
}
