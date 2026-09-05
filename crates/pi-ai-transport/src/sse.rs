use crate::{TransportError, TransportErrorKind, is_ecma_whitespace};
use std::fmt;

/// Framing only: provider JSON validation/repair remains adapter-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SseProfile {
    PiMessages,
    Codex,
    Mistral,
    Anthropic,
    /// OpenAI 6.40.0 SDK byte-chunk gate, line decoder and blank-line dispatch.
    OpenAI,
}
#[derive(Clone, Copy, Debug)]
pub struct SseLimits {
    /// All queued raw bytes plus pending normalized frame bytes, continuously.
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
    openai: OpenAiState,
}

#[derive(Default)]
struct OpenAiState {
    /// Prefix of buffer released by the SDK's raw double-newline chunk gate.
    released: usize,
    flushed: bool,
    event: Option<String>,
    data: Vec<String>,
    frame_bytes: usize,
    lines: usize,
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
            openai: OpenAiState::default(),
        }
    }
    /// Bounded intake only. Drain next_frame before reading more bytes; stop
    /// after the adapter terminal without examining irrelevant trailing frames.
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), TransportError> {
        if self.failed || self.input_ended {
            return Err(TransportError::new(TransportErrorKind::Closed));
        }
        if self
            .buffer
            .len()
            .checked_add(chunk.len())
            .and_then(|bytes| {
                bytes.checked_add(if self.profile == SseProfile::OpenAI {
                    self.openai.frame_bytes
                } else {
                    0
                })
            })
            .is_none_or(|bytes| bytes > self.limits.max_buffer_bytes)
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
        if self.profile == SseProfile::OpenAI {
            let result = self.next_openai();
            if result.is_err() {
                return self.fail();
            }
            return result;
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
        self.openai = OpenAiState::default();
        self.emitted = 0;
        Err(TransportError::new(TransportErrorKind::LimitExceeded))
    }

    fn next_openai(&mut self) -> Result<Option<SseFrame>, TransportError> {
        let state = &mut self.openai;
        loop {
            if self
                .buffer
                .len()
                .checked_add(state.frame_bytes)
                .is_none_or(|bytes| bytes > self.limits.max_buffer_bytes)
            {
                return Err(TransportError::new(TransportErrorKind::LimitExceeded));
            }
            let released = &self.buffer[..state.released];
            let line = released
                .iter()
                .position(|byte| matches!(byte, b'\r' | b'\n'))
                .and_then(|index| {
                    if released[index] == b'\n' {
                        Some((index, 1))
                    } else if released[index + 1..]
                        .iter()
                        .any(|byte| matches!(byte, b'\r' | b'\n'))
                        || state.flushed
                    {
                        Some((
                            index,
                            if released.get(index + 1) == Some(&b'\n') {
                                2
                            } else {
                                1
                            },
                        ))
                    } else {
                        None
                    }
                })
                // At EOF the SDK appends one virtual LF. Account only for
                // actual raw bytes; do not allocate that synthetic delimiter.
                .or_else(|| (state.flushed && !released.is_empty()).then_some((released.len(), 0)));
            if let Some((index, ending)) = line {
                // TextDecoder.decode is non-streaming per line and strips a BOM
                // on every line, not merely at the start of the response.
                let raw_line = self.buffer[..index]
                    .strip_prefix(b"\xef\xbb\xbf")
                    .unwrap_or(&self.buffer[..index]);
                // Each invalid UTF-8 subsequence becomes one three-byte U+FFFD.
                // Measure exactly, including truncated sequences, before any
                // replacement String or accumulated event/data allocation.
                let normalized_len = raw_line
                    .utf8_chunks()
                    .try_fold(0usize, |bytes, chunk| {
                        bytes
                            .checked_add(chunk.valid().len())?
                            .checked_add(if chunk.invalid().is_empty() { 0 } else { 3 })
                    })
                    .ok_or(TransportError::new(TransportErrorKind::LimitExceeded))?;
                let frame_bytes = if normalized_len == 0 {
                    0
                } else {
                    state
                        .frame_bytes
                        .checked_add(normalized_len)
                        .and_then(|bytes| bytes.checked_add(usize::from(state.lines != 0)))
                        .filter(|bytes| *bytes <= self.limits.max_frame_bytes)
                        .ok_or(TransportError::new(TransportErrorKind::LimitExceeded))?
                };
                if (self.buffer.len() - index - ending)
                    .checked_add(frame_bytes)
                    .is_none_or(|bytes| bytes > self.limits.max_buffer_bytes)
                {
                    return Err(TransportError::new(TransportErrorKind::LimitExceeded));
                }
                let line = String::from_utf8_lossy(raw_line).into_owned();
                self.buffer.drain(..index + ending);
                state.released -= index + ending;
                if line.is_empty() {
                    state.frame_bytes = 0;
                    state.lines = 0;
                    if state.data.is_empty() && state.event.as_deref().is_none_or(str::is_empty) {
                        continue;
                    }
                    if self.emitted >= self.limits.max_frames_per_feed {
                        return Err(TransportError::new(TransportErrorKind::LimitExceeded));
                    }
                    self.emitted += 1;
                    return Ok(Some(SseFrame {
                        event: state.event.take(),
                        data: std::mem::take(&mut state.data).join("\n"),
                    }));
                }
                state.frame_bytes = frame_bytes;
                state.lines += 1;
                if line.starts_with(':') {
                    continue;
                }
                let (field, value) = line.split_once(':').unwrap_or((&line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => state.event = Some(value.to_owned()),
                    "data" => state.data.push(value.to_owned()),
                    _ => {}
                }
                continue;
            }
            // The SDK only releases these three byte delimiters. Mixed endings
            // may form SSE blank lines but stay invisible until a gate or EOF.
            let waiting = &self.buffer[state.released..];
            let gate = (0..waiting.len()).find_map(|index| {
                [
                    b"\n\n".as_slice(),
                    b"\r\r".as_slice(),
                    b"\r\n\r\n".as_slice(),
                ]
                .into_iter()
                .find(|ending| waiting[index..].starts_with(ending))
                .map(|ending| index + ending.len())
            });
            if let Some(length) = gate {
                state.released += length;
                continue;
            }
            if !self.input_ended {
                return Ok(None);
            }
            if state.released < self.buffer.len() {
                state.released = self.buffer.len();
                continue;
            }
            if !state.flushed {
                state.flushed = true;
                if !self.buffer.is_empty() {
                    continue;
                }
            }
            // EOF never invents a blank line. An unterminated final event drops.
            self.buffer.clear();
            state.released = 0;
            state.data.clear();
            state.event = None;
            state.frame_bytes = 0;
            state.lines = 0;
            return Ok(None);
        }
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

#[cfg(test)]
mod resource_tests {
    use super::*;
    fn assert_cleared(decoder: &SseDecoder) {
        assert!(decoder.failed);
        assert!(decoder.buffer.is_empty());
        assert!(decoder.openai.event.is_none());
        assert!(decoder.openai.data.is_empty());
        assert_eq!(decoder.openai.released, 0);
        assert_eq!(decoder.openai.frame_bytes, 0);
        assert_eq!(decoder.openai.lines, 0);
        assert!(!decoder.openai.flushed);
    }
    #[test]
    fn openai_intake_failure_clears_all_owned_state() {
        let mut decoder = SseDecoder::new(
            SseProfile::OpenAI,
            SseLimits {
                max_buffer_bytes: 64,
                max_frame_bytes: 64,
                max_frames_per_feed: 1,
            },
        );
        decoder.push(b"event: synthetic\rdata: marker\r\r").unwrap();
        assert!(decoder.next_frame().unwrap().is_none());
        assert_eq!(decoder.openai.event.as_deref(), Some("synthetic"));
        assert_eq!(decoder.openai.data, ["marker"]);
        assert_eq!(
            decoder.push(&[b'x'; 64]).unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_cleared(&decoder);
        assert_eq!(
            decoder.next_frame().unwrap_err().kind(),
            TransportErrorKind::Closed
        );
        assert_cleared(&decoder);
    }
    #[test]
    fn openai_empty_push_rejects_already_exceeded_or_overflowing_charge() {
        for frame_bytes in [12, usize::MAX] {
            let mut decoder = SseDecoder::new(
                SseProfile::OpenAI,
                SseLimits {
                    max_buffer_bytes: 11,
                    max_frame_bytes: usize::MAX,
                    max_frames_per_feed: 1,
                },
            );
            // Inject invalid private state to exercise defensive empty-intake
            // arithmetic independently from the normalization growth guard.
            decoder.buffer.push(b'\r');
            decoder.openai.frame_bytes = frame_bytes;
            decoder.openai.data.push("synthetic".into());
            assert_eq!(
                decoder.push(&[]).unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
            assert_cleared(&decoder);
        }
    }
    #[test]
    fn openai_growth_failure_clears_all_owned_state() {
        let mut decoder = SseDecoder::new(
            SseProfile::OpenAI,
            SseLimits {
                max_buffer_bytes: 11,
                max_frame_bytes: 32,
                max_frames_per_feed: 1,
            },
        );
        decoder.push(b"data:\xff\xff\xff\xff\r\r").unwrap();
        assert_eq!(
            decoder.next_frame().unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_cleared(&decoder);
    }
}
