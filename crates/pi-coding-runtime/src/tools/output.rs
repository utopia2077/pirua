//! Bounded streaming shell output with a full-output spill file.
use super::{DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, EnvError, EnvResult, Truncation, truncate_tail};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Admission is short and synchronous; foreign progress never holds this lock.
pub(crate) struct OutputAcceptance {
    state: Mutex<OutputAcceptanceState>,
    drained: tokio::sync::Notify,
}
struct OutputAcceptanceState {
    accepting: bool,
    active: usize,
}
impl OutputAcceptance {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(OutputAcceptanceState {
                accepting: true,
                active: 0,
            }),
            drained: tokio::sync::Notify::new(),
        }
    }
    pub fn admit(self: &Arc<Self>) -> Option<OutputAdmission> {
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return None;
        }
        state.active += 1;
        Some(OutputAdmission {
            acceptance: self.clone(),
        })
    }
    pub fn retirement(self: &Arc<Self>) -> OutputRetirement {
        OutputRetirement(self.clone())
    }
    fn retire(&self) {
        self.state.lock().unwrap().accepting = false;
    }
    pub async fn close(&self) {
        self.retire();
        loop {
            // notify_waiters reaches this future even before its first poll.
            // Register it before checking active to avoid a lost final wakeup.
            let drained = self.drained.notified();
            if self.state.lock().unwrap().active == 0 {
                return;
            }
            drained.await;
        }
    }
}
pub(crate) struct OutputAdmission {
    acceptance: Arc<OutputAcceptance>,
}
impl Drop for OutputAdmission {
    fn drop(&mut self) {
        let drained = {
            let mut state = self.acceptance.state.lock().unwrap();
            state.active -= 1;
            !state.accepting && state.active == 0
        };
        if drained {
            self.acceptance.drained.notify_waiters();
        }
    }
}
/// Retire callbacks when an execution future is abandoned as well as when it settles.
pub(crate) struct OutputRetirement(Arc<OutputAcceptance>);
impl Drop for OutputRetirement {
    fn drop(&mut self) {
        self.0.retire();
    }
}

pub(crate) struct OutputAccumulator {
    pending: Vec<u8>,
    raw: Vec<u8>,
    tail: String,
    total_bytes: usize,
    completed_lines: usize,
    open_line_bytes: usize,
    file: Option<(PathBuf, File)>,
    error: Option<String>,
    last_update: Option<std::time::Instant>,
}
impl OutputAccumulator {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            raw: Vec::new(),
            tail: String::new(),
            total_bytes: 0,
            completed_lines: 0,
            open_line_bytes: 0,
            file: None,
            error: None,
            last_update: None,
        }
    }
    pub fn append(&mut self, bytes: &[u8]) {
        if self.error.is_some() {
            return;
        }
        if let Some((_, file)) = &mut self.file {
            if let Err(error) = file.write_all(bytes) {
                self.error = Some(error.to_string());
                return;
            }
        } else {
            self.raw.extend_from_slice(bytes);
        }
        self.pending.extend_from_slice(bytes);
        let decoded = match std::str::from_utf8(&self.pending) {
            Ok(text) => {
                let text = text.to_owned();
                self.pending.clear();
                text
            }
            Err(error) if error.error_len().is_none() => {
                let valid = error.valid_up_to();
                let text = String::from_utf8_lossy(&self.pending[..valid]).into_owned();
                self.pending.drain(..valid);
                text
            }
            Err(_) => {
                // Preserve a potentially incomplete trailing scalar across chunks.
                let mut end = self.pending.len();
                while end > 0
                    && self.pending.len() - end < 3
                    && self.pending[end - 1] & 0xc0 == 0x80
                {
                    end -= 1;
                }
                if end > 0 && self.pending[end - 1] >= 0xc0 {
                    end -= 1;
                }
                if end == self.pending.len() || self.pending.len() - end > 3 {
                    end = self.pending.len();
                }
                let text = String::from_utf8_lossy(&self.pending[..end]).into_owned();
                self.pending.drain(..end);
                text
            }
        };
        self.decode(&decoded);
        if self.file.is_none()
            && (self.total_bytes > DEFAULT_MAX_BYTES || self.lines() > DEFAULT_MAX_LINES)
            && let Err(error) = self.spill()
        {
            self.error = Some(error.to_string());
        }
    }
    fn decode(&mut self, text: &str) {
        self.total_bytes += text.len();
        for byte in text.bytes() {
            if byte == b'\n' {
                self.completed_lines += 1;
                self.open_line_bytes = 0;
            } else {
                self.open_line_bytes += 1;
            }
        }
        self.tail.push_str(text);
        if self.tail.len() > DEFAULT_MAX_BYTES * 4 {
            let mut start = self.tail.len() - DEFAULT_MAX_BYTES * 2;
            while !self.tail.is_char_boundary(start) {
                start += 1;
            }
            self.tail.drain(..start);
        }
    }
    pub(crate) fn spill(&mut self) -> EnvResult<()> {
        let mut attempts = 0;
        loop {
            let path = std::env::temp_dir().join(format!(
                "pi-output-{}-{}",
                std::process::id(),
                super::TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            match std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&path)
            {
                Ok(mut file) => {
                    file.write_all(&self.raw).map_err(super::env_error)?;
                    self.raw.clear();
                    self.file = Some((path, file));
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && attempts < 8 => {
                    attempts += 1;
                }
                Err(error) => return Err(super::env_error(error)),
            }
        }
    }
    pub fn finish(&mut self) -> EnvResult<()> {
        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            self.decode(&String::from_utf8_lossy(&pending));
        }
        if let Some(error) = &self.error {
            return Err(EnvError::new(error));
        }
        // The final replacement scalar can cross the decoded display limit even
        // when the raw bytes fit. Persist the original raw bytes in that case too.
        if self.file.is_none()
            && (self.total_bytes > DEFAULT_MAX_BYTES || self.lines() > DEFAULT_MAX_LINES)
        {
            self.spill()?;
        }
        if let Some((_, file)) = &mut self.file {
            file.flush().map_err(super::env_error)?;
        }
        Ok(())
    }
    pub fn lines(&self) -> usize {
        self.completed_lines + usize::from(self.open_line_bytes > 0)
    }
    pub fn snapshot(&self) -> (String, Truncation) {
        let (text, mut truncation) = truncate_tail(&self.tail);
        truncation.total_bytes = self.total_bytes;
        truncation.total_lines = self.lines();
        truncation.truncated =
            self.total_bytes > DEFAULT_MAX_BYTES || self.lines() > DEFAULT_MAX_LINES;
        if truncation.truncated && truncation.truncated_by.is_none() {
            truncation.truncated_by = Some(if self.total_bytes > DEFAULT_MAX_BYTES {
                "bytes"
            } else {
                "lines"
            });
        }
        (text, truncation)
    }
    pub fn path(&self) -> Option<String> {
        self.file
            .as_ref()
            .map(|(path, _)| path.to_string_lossy().into_owned())
    }
    pub fn last_line_bytes(&self) -> usize {
        self.open_line_bytes
    }
    pub fn should_update(&mut self) -> bool {
        if self
            .last_update
            .is_none_or(|at| at.elapsed() >= std::time::Duration::from_millis(100))
        {
            self.last_update = Some(std::time::Instant::now());
            true
        } else {
            false
        }
    }
    pub fn full_output(&mut self, limit: usize) -> EnvResult<(String, bool)> {
        let Some((_, file)) = &mut self.file else {
            return Ok((String::from_utf8_lossy(&self.raw).into_owned(), false));
        };
        let size = file.metadata().map_err(super::env_error)?.len() as usize;
        file.seek(SeekFrom::Start(0)).map_err(super::env_error)?;
        if size <= limit {
            let mut bytes = Vec::with_capacity(size);
            file.read_to_end(&mut bytes).map_err(super::env_error)?;
            return Ok((String::from_utf8_lossy(&bytes).into_owned(), false));
        }
        let head_len = limit / 2;
        let tail_len = limit - head_len;
        let mut head = vec![0; head_len];
        let mut tail = vec![0; tail_len];
        file.read_exact(&mut head).map_err(super::env_error)?;
        file.seek(SeekFrom::Start((size - tail_len) as u64))
            .map_err(super::env_error)?;
        file.read_exact(&mut tail).map_err(super::env_error)?;
        let head_end = std::str::from_utf8(&head)
            .err()
            .filter(|error| error.error_len().is_none())
            .map_or(head.len(), |error| error.valid_up_to());
        let tail_start = tail
            .iter()
            .position(|byte| byte & 0xc0 != 0x80)
            .unwrap_or(tail.len());
        Ok((
            format!(
                "{}\n\n[... {} bytes omitted ...]\n\n{}",
                String::from_utf8_lossy(&head[..head_end]),
                size - limit,
                String::from_utf8_lossy(&tail[tail_start..])
            ),
            true,
        ))
    }
}
