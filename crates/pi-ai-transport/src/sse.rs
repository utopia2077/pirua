use crate::{TransportError, TransportErrorKind, is_ecma_whitespace};
use std::{collections::VecDeque, fmt};

/// Framing only: provider JSON validation/repair remains adapter-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SseProfile {
    PiMessages,
    Codex,
    Mistral,
    /// Pinned pi private iterator: streaming UTF-8, immediate CR, trailing EOF.
    Anthropic,
    /// OpenAI 6.40.0 SDK byte-chunk gate, line decoder and blank-line dispatch.
    OpenAI,
}
#[derive(Clone, Copy, Debug)]
pub struct SseLimits {
    /// All queued raw/decoded/frame bytes, continuously. Anthropic also charges
    /// `size_of::<usize>()` for each queued nonempty push boundary.
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
    raw_lines: Option<Vec<String>>,
}
impl SseFrame {
    /// Bounded transient source lines for trusted provider error classification.
    /// Anthropic supplies every decoded nonempty line, including comments and
    /// ignored/repeated fields; line delimiters are excluded. Other profiles
    /// return None. This raw context must not enter public errors or telemetry.
    pub fn raw_lines_for_adapter(&self) -> Option<&[String]> {
        self.raw_lines.as_deref()
    }
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
    anthropic: AnthropicState,
}

#[derive(Default)]
struct AnthropicState {
    /// Nonempty push lengths; each live marker charges `size_of::<usize>()`.
    chunks: VecDeque<usize>,
    /// Incomplete UTF-8 prefix at the front of raw buffer, outside chunks.
    pending: usize,
    line: String,
    /// Keep each decoded line once; derive event/data only when emitting.
    raw_lines: Vec<String>,
    has_event: bool,
    has_data: bool,
    frame_bytes: usize,
}
impl AnthropicState {
    fn charge(&self, extra_chunk: bool) -> Option<usize> {
        self.chunks
            .len()
            .checked_add(usize::from(extra_chunk))?
            .checked_mul(size_of::<usize>())?
            .checked_add(self.line.len())?
            .checked_add(usize::from(
                !self.line.is_empty() && !self.raw_lines.is_empty(),
            ))?
            .checked_add(self.frame_bytes)
    }
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
            anthropic: AnthropicState::default(),
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
            .and_then(|bytes| {
                if self.profile == SseProfile::Anthropic {
                    bytes.checked_add(self.anthropic.charge(!chunk.is_empty())?)
                } else {
                    Some(bytes)
                }
            })
            .is_none_or(|bytes| bytes > self.limits.max_buffer_bytes)
        {
            return self.fail();
        }
        self.buffer.extend_from_slice(chunk);
        if self.profile == SseProfile::Anthropic && !chunk.is_empty() {
            self.anthropic.chunks.push_back(chunk.len());
        }
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
        if self.profile == SseProfile::Anthropic {
            let result = self.next_anthropic();
            return if result.is_err() { self.fail() } else { result };
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
        self.anthropic = AnthropicState::default();
        self.emitted = 0;
        Err(TransportError::new(TransportErrorKind::LimitExceeded))
    }

    fn flush_anthropic(&mut self) -> Result<Option<SseFrame>, TransportError> {
        let state = &mut self.anthropic;
        if !state.has_event && !state.has_data {
            // Pinned flushSseEvent leaves its raw lines pending in this case.
            return Ok(None);
        }
        if self.emitted >= self.limits.max_frames_per_feed {
            return Err(TransportError::new(TransportErrorKind::LimitExceeded));
        }
        self.emitted += 1;
        state.frame_bytes = 0;
        state.has_event = false;
        state.has_data = false;
        let raw_lines = std::mem::take(&mut state.raw_lines);
        let mut event = None;
        let mut data = String::new();
        let mut first_data = true;
        for line in &raw_lines {
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => event = Some(value.to_owned()),
                "data" => {
                    if !first_data {
                        data.push('\n');
                    }
                    first_data = false;
                    data.push_str(value);
                }
                _ => {}
            }
        }
        Ok(Some(SseFrame {
            event,
            data,
            raw_lines: Some(raw_lines),
        }))
    }
    fn line_anthropic(&mut self) -> Result<Option<SseFrame>, TransportError> {
        let state = &mut self.anthropic;
        let line = std::mem::take(&mut state.line);
        if line.is_empty() {
            return self.flush_anthropic();
        }
        // The prospective complete line was bounded before decoding/allocation.
        state.frame_bytes += line.len() + usize::from(!state.raw_lines.is_empty());
        if !line.starts_with(':') {
            let (field, value) = line.split_once(':').unwrap_or((&line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => state.has_event = !value.is_empty(),
                "data" => state.has_data = true,
                _ => {}
            }
        }
        state.raw_lines.push(line);
        Ok(None)
    }
    fn next_anthropic(&mut self) -> Result<Option<SseFrame>, TransportError> {
        let limit = || TransportError::new(TransportErrorKind::LimitExceeded);
        loop {
            let state = &mut self.anthropic;
            if state
                .charge(false)
                .and_then(|n| n.checked_add(self.buffer.len()))
                .is_none_or(|n| n > self.limits.max_buffer_bytes)
            {
                return Err(limit());
            }
            if state.chunks.is_empty() && (!self.input_ended || self.buffer.is_empty()) {
                if !self.input_ended {
                    return Ok(None);
                }
                if !state.line.is_empty() {
                    self.line_anthropic()?;
                }
                let result = self.flush_anthropic();
                self.anthropic = AnthropicState::default();
                return result;
            }
            let chunk = state.chunks.front().copied();
            let available = state.pending + chunk.unwrap_or(0);
            let raw = &self.buffer[..available];
            let delimiter = raw.iter().position(|b| matches!(b, b'\r' | b'\n'));
            let (end, ending) = if let Some(index) = delimiter {
                // CR consumes LF only if LF is already in THIS source read.
                (
                    index,
                    if raw[index] == b'\r' && raw.get(index + 1) == Some(&b'\n') {
                        2
                    } else {
                        1
                    },
                )
            } else {
                (
                    if chunk.is_none() {
                        available
                    } else {
                        complete_utf8_prefix(raw)
                    },
                    0,
                )
            };
            let consumed = end + ending;
            let mut bytes = &raw[..end];
            let had_output = !bytes.is_empty() || ending != 0;
            if self.bom_pending {
                bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
            }
            let decoded_len = bytes
                .utf8_chunks()
                .try_fold(0usize, |n, c| {
                    n.checked_add(c.valid().len())?
                        .checked_add(if c.invalid().is_empty() { 0 } else { 3 })
                })
                .ok_or_else(limit)?;
            let line_len = state
                .line
                .len()
                .checked_add(decoded_len)
                .ok_or_else(limit)?;
            let frame_len = state
                .frame_bytes
                .checked_add(line_len)
                .and_then(|n| {
                    n.checked_add(usize::from(line_len != 0 && !state.raw_lines.is_empty()))
                })
                .ok_or_else(limit)?;
            if frame_len > self.limits.max_frame_bytes {
                return Err(limit());
            }
            let pop = chunk.is_some() && (delimiter.is_none() || consumed == available);
            let metadata = (state.chunks.len() - usize::from(pop))
                .checked_mul(size_of::<usize>())
                .ok_or_else(limit)?;
            if (self.buffer.len() - consumed)
                .checked_add(frame_len)
                .and_then(|n| n.checked_add(metadata))
                .is_none_or(|n| n > self.limits.max_buffer_bytes)
            {
                return Err(limit());
            }
            state.line.push_str(&String::from_utf8_lossy(bytes));
            if had_output {
                self.bom_pending = false;
            }
            self.buffer.drain(..consumed);
            if pop {
                state.chunks.pop_front();
            } else if let Some(length) = state.chunks.front_mut() {
                *length -= consumed - state.pending;
            }
            state.pending = if delimiter.is_none() {
                available - consumed
            } else {
                0
            };
            if delimiter.is_some()
                && let Some(frame) = self.line_anthropic()?
            {
                return Ok(Some(frame));
            }
        }
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
                        raw_lines: None,
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
// Streaming TextDecoder retains only an incomplete trailing UTF-8 sequence.
fn complete_utf8_prefix(bytes: &[u8]) -> usize {
    let mut offset = 0;
    while offset < bytes.len() {
        match std::str::from_utf8(&bytes[offset..]) {
            Ok(_) => return bytes.len(),
            Err(error) => {
                offset += error.valid_up_to();
                if let Some(length) = error.error_len() {
                    offset += length;
                } else {
                    return offset;
                }
            }
        }
    }
    offset
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
    let normalized = if profile == SseProfile::Mistral {
        raw.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        raw.into_owned()
    };
    let mut event = None;
    let mut data = Vec::new();
    for line in normalized.split('\n') {
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
    let data = data.join("\n").trim_matches(is_ecma_whitespace).to_owned();
    if data.is_empty()
        || (data == "[DONE]" && matches!(profile, SseProfile::PiMessages | SseProfile::Codex))
    {
        None
    } else {
        Some(SseFrame {
            event,
            data,
            raw_lines: None,
        })
    }
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    fn assert_anthropic_cleared(d: &SseDecoder) {
        assert!(d.failed);
        assert!(d.buffer.is_empty());
        assert!(d.anthropic.chunks.is_empty());
        assert_eq!(d.anthropic.pending, 0);
        assert!(d.anthropic.line.is_empty());
        assert!(d.anthropic.raw_lines.is_empty());
        assert!(!d.anthropic.has_event);
        assert!(!d.anthropic.has_data);
        assert_eq!(d.anthropic.frame_bytes, 0);
    }
    #[test]
    fn anthropic_bounds_cleanup_and_empty_chunk_metadata() {
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_buffer_bytes: 128,
                ..SseLimits::default()
            },
        );
        for _ in 0..10000 {
            d.push(&[]).unwrap();
        }
        assert!(d.anthropic.chunks.is_empty());
        d.push(b"event: synthetic\rdata: marker\r").unwrap();
        assert!(d.next_frame().unwrap().is_none());
        assert!(d.anthropic.has_event);
        assert!(d.anthropic.has_data);
        assert_eq!(d.anthropic.raw_lines, ["event: synthetic", "data: marker"]);
        assert_eq!(
            d.push(&[b'x'; 128]).unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_anthropic_cleared(&d);
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_buffer_bytes: 28,
                ..SseLimits::default()
            },
        );
        d.push(b"data:\xff\xff\xff\xff\xff\xff\xff\xff").unwrap();
        assert_eq!(
            d.next_frame().unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_anthropic_cleared(&d);
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_buffer_bytes: 2 * size_of::<usize>(),
                ..SseLimits::default()
            },
        );
        d.push(b"x").unwrap();
        assert_eq!(
            d.push(b"y").unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_anthropic_cleared(&d);
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_buffer_bytes: 0,
                ..SseLimits::default()
            },
        );
        d.anthropic.line = "synthetic".into();
        assert_eq!(
            d.push(&[]).unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_anthropic_cleared(&d);
    }
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
    fn anthropic_raw_context_releases_on_eof_frame_and_feed_failure() {
        let mut decoder = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
        decoder.push(b": private-marker\n\nignored: tail").unwrap();
        assert!(decoder.next_frame().unwrap().is_none());
        assert!(!decoder.anthropic.raw_lines.is_empty());
        decoder.end_input();
        assert!(decoder.next_frame().unwrap().is_none());
        assert!(decoder.anthropic.raw_lines.is_empty());
        assert!(decoder.anthropic.line.is_empty());
        assert!(decoder.anthropic.chunks.is_empty());
        assert_eq!(decoder.anthropic.charge(false), Some(0));
        for limits in [
            SseLimits {
                max_frame_bytes: 15,
                ..SseLimits::default()
            },
            SseLimits {
                max_frames_per_feed: 0,
                ..SseLimits::default()
            },
        ] {
            let mut decoder = SseDecoder::new(SseProfile::Anthropic, limits);
            decoder.push(b": hidden\n\ndata: x\n\n").unwrap();
            assert_eq!(
                decoder.next_frame().unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
            assert_anthropic_cleared(&decoder);
        }
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
