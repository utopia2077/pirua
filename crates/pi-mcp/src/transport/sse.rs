use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
    pub id: Option<String>,
}
/// Incremental SSE parser with limits on both partial lines and accumulated data.
/// IDs and retry hints are visible even for events without data.
pub struct SseParser {
    at_start: bool,
    bom_bytes: usize,
    buffer: Vec<u8>,
    data: Vec<String>,
    data_bytes: usize,
    event: Option<String>,
    id: Option<String>,
    pub last_event_id: Option<String>,
    pub retry_ms: Option<u64>,
    max_bytes: usize,
}
impl SseParser {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            at_start: true,
            bom_bytes: 0,
            buffer: Vec::new(),
            data: Vec::new(),
            data_bytes: 0,
            event: None,
            id: None,
            last_event_id: None,
            retry_ms: None,
            max_bytes,
        }
    }
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>> {
        let mut events = Vec::new();
        for &byte in chunk {
            if self.at_start {
                const BOM: &[u8] = b"\xef\xbb\xbf";
                if byte == BOM[self.bom_bytes] {
                    self.bom_bytes += 1;
                    if self.bom_bytes == BOM.len() {
                        self.at_start = false;
                        self.bom_bytes = 0;
                    }
                    continue;
                }
                self.at_start = false;
                // A partial prefix that is not a BOM remains ordinary input.
                for &prefix in &BOM[..self.bom_bytes] {
                    self.byte(prefix, &mut events)?;
                }
                self.bom_bytes = 0;
            }
            self.byte(byte, &mut events)?;
        }
        Ok(events)
    }
    pub fn finish(&mut self) -> Result<Vec<SseEvent>> {
        let mut events = Vec::new();
        // EOF can end an incomplete UTF-8/BOM prefix. Preserve it for the same
        // lossy text decoding used for the rest of an incomplete final line.
        self.at_start = false;
        for &prefix in &b"\xef\xbb\xbf"[..self.bom_bytes] {
            self.byte(prefix, &mut events)?;
        }
        self.bom_bytes = 0;
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.line(&line, &mut events)?;
        }
        self.dispatch(&mut events);
        Ok(events)
    }
    fn byte(&mut self, byte: u8, events: &mut Vec<SseEvent>) -> Result<()> {
        if byte == b'\n' {
            let line = std::mem::take(&mut self.buffer);
            self.line(&line, events)?;
        } else {
            self.buffer.push(byte);
            if self.buffer.len() > self.max_bytes {
                return Err(Error::invalid("SSE line exceeds byte limit"));
            }
        }
        Ok(())
    }
    fn line(&mut self, line: &[u8], events: &mut Vec<SseEvent>) -> Result<()> {
        let line = String::from_utf8_lossy(line);
        let line = line.strip_suffix('\r').unwrap_or(&line);
        if line.is_empty() {
            self.dispatch(events);
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                self.data_bytes += value.len() + usize::from(!self.data.is_empty());
                if self.data_bytes > self.max_bytes {
                    return Err(Error::invalid("SSE event exceeds byte limit"));
                }
                self.data.push(value.into());
            }
            "event" => self.event = Some(value.into()),
            "id" if !value.contains('\0') => {
                self.id = Some(value.into());
                self.last_event_id = self.id.clone();
            }
            "retry" if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                if let Ok(delay) = value.parse() {
                    self.retry_ms = Some(delay);
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn dispatch(&mut self, events: &mut Vec<SseEvent>) {
        if !self.data.is_empty() {
            events.push(SseEvent {
                event: self.event.take(),
                data: self.data.join("\n"),
                id: self.id.take(),
            });
        }
        self.data.clear();
        self.data_bytes = 0;
        self.event = None;
        self.id = None;
    }
}
