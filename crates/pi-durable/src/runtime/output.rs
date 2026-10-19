use super::*;
use crate::{Error, Result};
#[derive(Clone, Debug, Default)]
pub(crate) struct BoundedOutput {
    pub text: String,
    pub dropped_bytes: usize,
    pub dropped_lines: usize,
}
fn lines(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        text.bytes().filter(|b| *b == b'\n').count() + usize::from(!text.ends_with('\n'))
    }
}
pub(crate) fn bound(text: &str, limits: &OutputLimits) -> BoundedOutput {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let (mut start, mut end) = (0, n);
    if limits.max_bytes == 0 || limits.max_lines == 0 {
        end = 0;
    } else if limits.retain == OutputRetention::Head {
        if let Some(i) = bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'\n')
            .nth(limits.max_lines - 1)
            .map(|(i, _)| i)
        {
            end = i + 1;
        }
        if end > limits.max_bytes {
            end = bytes[..limits.max_bytes.min(n)]
                .iter()
                .rposition(|b| *b == b'\n')
                .map_or(limits.max_bytes, |i| i + 1);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
        }
    } else {
        let mut count = 1;
        let last = if text.ends_with('\n') {
            n.saturating_sub(1)
        } else {
            n
        };
        for i in (0..last).rev() {
            if bytes[i] == b'\n' {
                if count == limits.max_lines {
                    start = i + 1;
                    break;
                }
                count += 1;
            }
        }
        if n - start > limits.max_bytes {
            let from = n - limits.max_bytes;
            start = bytes[from.saturating_sub(1)..]
                .iter()
                .position(|b| *b == b'\n')
                .map(|i| from.saturating_sub(1) + i + 1)
                .filter(|i| *i < n)
                .unwrap_or(from);
            while !text.is_char_boundary(start) {
                start += 1;
            }
        }
    }
    let kept = &text[start..end];
    BoundedOutput {
        text: kept.into(),
        dropped_bytes: n - kept.len(),
        dropped_lines: lines(text) - lines(kept),
    }
}
// Retain enough preceding text to locate a future tail's line boundary. Keeping
// only the already bounded tail loses information when the next chunk arrives.
fn tail_margin(text: &str, limits: &OutputLimits) -> String {
    let mut byte_start = text
        .len()
        .saturating_sub(limits.max_bytes.saturating_add(1));
    while !text.is_char_boundary(byte_start) {
        byte_start -= 1;
    }
    let line_start = text
        .bytes()
        .enumerate()
        .rev()
        .filter(|(_, b)| *b == b'\n')
        .nth(limits.max_lines)
        .map_or(0, |(i, _)| i);
    text[byte_start.max(line_start)..].into()
}
pub(crate) struct OutputBuffer {
    limits: OutputLimits,
    text: String,
    pending: Vec<u8>,
    started: bool,
    full: bool,
    total_bytes: usize,
    total_newlines: usize,
    ends_newline: bool,
}
impl OutputBuffer {
    pub fn new(limits: OutputLimits) -> Self {
        Self {
            limits,
            text: String::new(),
            pending: vec![],
            started: false,
            full: false,
            total_bytes: 0,
            total_newlines: 0,
            ends_newline: true,
        }
    }
    fn accept(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.total_bytes += text.len();
        self.total_newlines += text.bytes().filter(|b| *b == b'\n').count();
        self.ends_newline = text.ends_with('\n');
        if self.full {
            return;
        }
        self.text.push_str(text);
        if self.limits.retain == OutputRetention::Head {
            self.full = self.text.len() > self.limits.max_bytes
                || self.text.bytes().filter(|b| *b == b'\n').count() >= self.limits.max_lines;
        } else {
            self.text = tail_margin(&self.text, &self.limits);
        }
    }
    fn decode(&mut self, bytes: &[u8], finish: bool) -> String {
        self.pending.extend_from_slice(bytes);
        let mut decoded = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(s) => {
                    decoded.push_str(s);
                    self.pending.clear();
                    break;
                }
                Err(e) => {
                    let valid = e.valid_up_to();
                    if valid > 0 {
                        decoded.push_str(
                            std::str::from_utf8(&self.pending[..valid]).expect("validated prefix"),
                        );
                        self.pending.drain(..valid);
                    }
                    match e.error_len() {
                        Some(n) => {
                            self.pending.drain(..n);
                            decoded.push('\u{fffd}');
                        }
                        None => {
                            if finish && !self.pending.is_empty() {
                                self.pending.clear();
                                decoded.push('\u{fffd}');
                            }
                            break;
                        }
                    }
                }
            }
        }
        decoded
    }
    pub fn push(&mut self, chunk: OutputChunk, skip: Option<OutputSkip>) -> Result<bool> {
        if skip.is_some() && self.limits.retain != OutputRetention::Tail {
            return Err(Error::request("Skipped output requires tail retention"));
        }
        let bytes = matches!(chunk, OutputChunk::Bytes(_));
        let pending = if !bytes || skip.is_some() {
            self.decode(&[], true)
        } else {
            String::new()
        };
        let mut text = match chunk {
            OutputChunk::Text(s) => s,
            OutputChunk::Bytes(b) => self.decode(&b, false),
        };
        let first = !self.started && pending.is_empty() && skip.is_none();
        if !pending.is_empty() || !text.is_empty() || skip.is_some() {
            self.started = true;
        }
        if first && bytes && text.starts_with('\u{feff}') {
            text.drain(..3);
        }
        let accepted = !pending.is_empty() || !text.is_empty() || skip.is_some();
        self.accept(&pending);
        if let Some(skip) = skip {
            if skip.newlines > skip.bytes
                || (skip.bytes == 0 && skip.newlines != 0)
                || (skip.bytes > 0
                    && skip.bytes <= self.limits.max_bytes
                    && skip.newlines <= self.limits.max_lines)
            {
                return Err(Error::request(
                    "Skipped output must exceed the supplied tail window",
                ));
            }
            if skip.bytes > 0 {
                self.total_bytes += skip.bytes;
                self.total_newlines += skip.newlines;
                self.ends_newline = skip.ends_with_newline;
                self.text.clear();
            }
        }
        self.accept(&text);
        Ok(accepted)
    }
    pub fn end(&mut self) {
        let text = self.decode(&[], true);
        self.accept(&text);
    }
    pub fn snapshot(&self) -> BoundedOutput {
        let bounded = bound(&self.text, &self.limits);
        let kept_lines = lines(&bounded.text);
        BoundedOutput {
            text: bounded.text.chars().filter(|c| !matches!(*c, '\u{0}'..='\u{8}' | '\u{b}'..='\u{1f}' | '\u{fff9}'..='\u{fffb}')).collect(),
            dropped_bytes: self.total_bytes - (self.text.len() - bounded.dropped_bytes),
            dropped_lines: (self.total_newlines + usize::from(!self.ends_newline)).saturating_sub(kept_lines),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_whole_line_windows_and_scalar_boundaries() {
        let mut limits = OutputLimits {
            max_bytes: 5,
            max_lines: 10,
            retain: OutputRetention::Head,
        };
        assert_eq!(bound("aa\nbb\ncc", &limits).text, "aa\n");
        limits.retain = OutputRetention::Tail;
        assert_eq!(bound("aa\nbb\ncc", &limits).text, "bb\ncc");
        limits.max_lines = 1;
        assert_eq!(bound("aa\nbb\ncc\n", &limits).text, "cc\n");
        limits.max_lines = 10;
        assert_eq!(bound("é🙂e\u{feff}", &limits).text, "e\u{feff}");
        limits.retain = OutputRetention::Head;
        assert_eq!(bound("é🙂e\u{feff}", &limits).text, "é");
    }
    #[test]
    fn chunking_and_snapshot_frequency_preserve_tail_and_drop_totals() {
        let input = "ab\né🙂\ncd\nlast";
        for retain in [OutputRetention::Head, OutputRetention::Tail] {
            for max_bytes in 0..=input.len() + 2 {
                for max_lines in 0..=5 {
                    let limits = OutputLimits {
                        max_bytes,
                        max_lines,
                        retain,
                    };
                    let expected = bound(input, &limits);
                    let mut buffer = OutputBuffer::new(limits);
                    for character in input.chars() {
                        buffer
                            .push(OutputChunk::Text(character.to_string()), None)
                            .unwrap();
                        let _ = buffer.snapshot();
                    }
                    buffer.end();
                    let actual = buffer.snapshot();
                    assert_eq!(
                        (actual.text, actual.dropped_bytes, actual.dropped_lines),
                        (
                            expected.text,
                            expected.dropped_bytes,
                            expected.dropped_lines
                        ),
                        "{retain:?}, {max_bytes} bytes, {max_lines} lines"
                    );
                }
            }
        }
    }
    #[test]
    fn utf8_bom_incomplete_bytes_and_text_boundary_are_streamed() {
        let mut buffer = OutputBuffer::new(OutputLimits::default());
        assert!(
            !buffer
                .push(OutputChunk::Bytes(vec![0xef, 0xbb]), None)
                .unwrap()
        );
        assert!(
            buffer
                .push(OutputChunk::Bytes(vec![0xbf, b'a', 0xf0, 0x9f]), None)
                .unwrap()
        );
        buffer
            .push(OutputChunk::Text("b\u{feff}\u{0}\n".into()), None)
            .unwrap();
        buffer.end();
        assert_eq!(buffer.snapshot().text, "a\u{fffd}b\u{feff}\n");
        assert_eq!(buffer.snapshot().dropped_bytes, 0);
    }
    #[test]
    fn skipped_tail_counts_survive_and_head_rejects_skip() {
        let limits = OutputLimits {
            max_bytes: 4,
            max_lines: 1,
            retain: OutputRetention::Tail,
        };
        let mut buffer = OutputBuffer::new(limits);
        buffer
            .push(OutputChunk::Text("old\n".into()), None)
            .unwrap();
        buffer
            .push(
                OutputChunk::Text("new".into()),
                Some(OutputSkip {
                    bytes: 20,
                    newlines: 2,
                    ends_with_newline: true,
                }),
            )
            .unwrap();
        let kept = buffer.snapshot();
        assert_eq!(kept.text, "new");
        assert_eq!(kept.dropped_bytes, 24);
        assert_eq!(kept.dropped_lines, 3);
        assert!(
            OutputBuffer::new(OutputLimits::default())
                .push(
                    OutputChunk::Text("x".into()),
                    Some(OutputSkip {
                        bytes: 20,
                        newlines: 2,
                        ends_with_newline: true
                    })
                )
                .is_err()
        );
    }
}
