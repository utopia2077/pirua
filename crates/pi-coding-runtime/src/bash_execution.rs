//! Host-requested shell execution and canonical `bashExecution` records.
use crate::{
    CodingSession, ExecutionEnv, ProcessOptions, SessionError, SessionEvent, ShellToolOptions,
    messages,
    session::lock,
    tools::output::{OutputAcceptance, OutputAccumulator},
};
use pi_agent::{AgentError, BoxFuture, CancellationToken};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub type BashChunkSink =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<(), AgentError>> + Send + Sync>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BashResult {
    pub output: String,
    pub exit_code: Option<i32>,
    pub cancelled: bool,
    pub truncated: bool,
    pub full_output_path: Option<String>,
}
#[derive(Clone, Default)]
pub struct ExecuteBashOptions {
    pub exclude_from_context: bool,
    pub id: Option<String>,
    pub shell: ShellToolOptions,
    /// Optional invocation-local process environment; defaults to the session environment.
    pub environment: Option<Arc<dyn ExecutionEnv>>,
}
struct BashGuard {
    inner: Weak<crate::session::Inner>,
    id: u64,
    token: CancellationToken,
    complete: bool,
}
impl Drop for BashGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.token.cancel();
        }
        if let Some(inner) = self.inner.upgrade() {
            lock(&inner.state).bash.remove(&self.id);
            inner.idle.notify_waiters();
        }
    }
}
struct Capture {
    output: OutputAccumulator,
    decoder: Sanitizer,
    seen: bool,
    raw_bytes: usize,
    error: Option<String>,
}
impl CodingSession {
    /// Execute a host-requested Bash command, stream sanitized deltas, and save its result.
    /// Shell activity is independent of the Agent run; `abort_bash` cancels all shell calls.
    pub async fn execute_bash(
        &self,
        command: impl Into<String>,
        on_chunk: Option<BashChunkSink>,
        options: ExecuteBashOptions,
    ) -> Result<BashResult, SessionError> {
        self.ensure_live()?;
        let command = command.into();
        let token = CancellationToken::new();
        let id = {
            let mut state = lock(&self.inner.state);
            if state.disposed || state.disposing {
                return Err(SessionError::Disposed);
            }
            let id = state.next_bash;
            state.next_bash += 1;
            state.bash.insert(id, token.clone());
            id
        };
        let mut guard = BashGuard {
            inner: Arc::downgrade(&self.inner),
            id,
            token: token.clone(),
            complete: false,
        };
        let environment = options
            .environment
            .unwrap_or_else(|| lock(&self.inner.execution_env).clone());
        let resolved = options
            .shell
            .command_prefix
            .as_ref()
            .filter(|prefix| !prefix.is_empty())
            .map(|prefix| format!("{prefix}\n{command}"))
            .unwrap_or_else(|| command.clone());
        let capture = Arc::new(Mutex::new(Capture {
            output: OutputAccumulator::new(),
            decoder: Sanitizer::default(),
            seen: false,
            raw_bytes: 0,
            error: None,
        }));
        let acceptance = Arc::new(OutputAcceptance::new());
        let _retirement = acceptance.retirement();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<String>();
        let callback_capture = capture.clone();
        let callback_acceptance = acceptance.clone();
        let callback_sender = sender.clone();
        let mut process = Box::pin(environment.exec(
            "bash",
            &resolved,
            ProcessOptions {
                cwd: Some(self.snapshot().header.cwd),
                env: options.shell.environment,
                executable: options.shell.shell_path,
                abort_signal: Some(token.clone()),
                on_data: Some(Arc::new(move |bytes| {
                    let Some(_admission) = callback_acceptance.admit() else {
                        return;
                    };
                    let delta = {
                        let mut capture = lock(&callback_capture);
                        capture.seen |= !bytes.is_empty();
                        capture.raw_bytes += bytes.len();
                        let delta = capture.decoder.push(bytes, false);
                        capture.output.append(delta.as_bytes());
                        if !delta.is_empty()
                            && capture.raw_bytes > crate::tools::DEFAULT_MAX_BYTES
                            && capture.output.path().is_none()
                            && let Err(error) = capture.output.spill()
                        {
                            capture.error = Some(error.message);
                        }
                        delta
                    };
                    if !delta.is_empty() {
                        let _ = callback_sender.send(delta);
                    }
                })),
                ..Default::default()
            },
        ));
        let mut failure = None;
        let result = loop {
            tokio::select! {
                biased;
                Some(delta) = receiver.recv() => {
                    if failure.is_none() && let Err(error) = self.publish_bash_delta(options.id.clone(), delta, on_chunk.as_ref()).await {
                        token.cancel(); failure = Some(error);
                    }
                }
                result = &mut process => break result,
            }
        };
        acceptance.close().await;
        let (output, truncated, full_output_path, final_delta) = {
            let mut capture = lock(&capture);
            let mut final_delta = String::new();
            if !capture.seen
                && let Ok(result) = &result
            {
                let combined = result
                    .merged
                    .clone()
                    .unwrap_or_else(|| format!("{}{}", result.stdout, result.stderr));
                capture.raw_bytes += combined.len();
                final_delta.push_str(&capture.decoder.push(combined.as_bytes(), false));
            }
            final_delta.push_str(&capture.decoder.push(&[], true));
            capture.output.append(final_delta.as_bytes());
            capture
                .output
                .finish()
                .map_err(|error| SessionError::Invalid(error.message))?;
            let (output, truncation) = capture.output.snapshot();
            (
                output,
                truncation.truncated,
                capture.output.path(),
                final_delta,
            )
        };
        while let Ok(delta) = receiver.try_recv() {
            if failure.is_none()
                && let Err(error) = self
                    .publish_bash_delta(options.id.clone(), delta, on_chunk.as_ref())
                    .await
            {
                failure = Some(error);
            }
        }
        if failure.is_none() && !final_delta.is_empty() {
            failure = self
                .publish_bash_delta(options.id, final_delta, on_chunk.as_ref())
                .await
                .err();
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let cancelled = token.is_cancelled();
        let exit_code = match result {
            Ok(result) => (!cancelled && result.exit_code >= 0).then_some(result.exit_code),
            Err(_) if cancelled => None,
            Err(error) => return Err(SessionError::Invalid(error.message)),
        };
        let result = BashResult {
            output,
            exit_code,
            cancelled,
            truncated,
            full_output_path,
        };
        self.record_bash_result_inner(command, result.clone(), options.exclude_from_context, true)
            .await?;
        guard.complete = true;
        Ok(result)
    }
    async fn publish_bash_delta(
        &self,
        id: Option<String>,
        delta: String,
        sink: Option<&BashChunkSink>,
    ) -> Result<(), SessionError> {
        if let Some(sink) = sink {
            sink(delta.clone()).await?;
        }
        self.emit(SessionEvent::BashExecutionUpdate { id, delta })
            .await
    }
    /// Record a host-supplied result. During a run it waits until the Agent batch settles.
    pub async fn record_bash_result(
        &self,
        command: impl Into<String>,
        result: BashResult,
        exclude_from_context: bool,
    ) -> Result<(), SessionError> {
        self.record_bash_result_inner(command, result, exclude_from_context, false)
            .await
    }
    async fn record_bash_result_inner(
        &self,
        command: impl Into<String>,
        result: BashResult,
        exclude_from_context: bool,
        admitted_shell: bool,
    ) -> Result<(), SessionError> {
        if !admitted_shell {
            self.ensure_live()?;
        }
        let mut value = json!({"role":"bashExecution","command":command.into(),"output":result.output,"cancelled":result.cancelled,"truncated":result.truncated,"timestamp":messages::now()});
        if let Some(code) = result.exit_code {
            value["exitCode"] = json!(code);
        }
        if let Some(path) = result.full_output_path {
            value["fullOutputPath"] = json!(path);
        }
        if exclude_from_context {
            value["excludeFromContext"] = json!(true);
        }
        let message = messages::decode(value)?;
        // Check busy state and publish/queue under the canonical manager lock, so a
        // concurrent finalized message cannot split a shell record into a tool batch.
        let mut manager = lock(&self.inner.manager);
        if self.is_streaming() {
            lock(&self.inner.state).pending_bash.push(message);
            return Ok(());
        }
        manager.append(pi_coding_session::EntryContent::Message {
            message: serde_json::to_value(&message)?,
        })?;
        drop(manager);
        self.refresh()
    }
    pub fn abort_bash(&self) {
        let tokens = lock(&self.inner.state)
            .bash
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for token in tokens {
            token.cancel();
        }
    }
    pub fn is_bash_running(&self) -> bool {
        !lock(&self.inner.state).bash.is_empty()
    }
    pub fn has_pending_bash_messages(&self) -> bool {
        !lock(&self.inner.state).pending_bash.is_empty()
    }
    /// Wait specifically for shell calls; Agent `wait_for_idle` retains release semantics.
    pub async fn wait_for_bash(&self) {
        loop {
            let notified = self.inner.idle.notified();
            if !self.is_bash_running() {
                return;
            }
            notified.await;
        }
    }
    pub(crate) async fn flush_bash(&self) -> Result<(), SessionError> {
        loop {
            let message = lock(&self.inner.state).pending_bash.first().cloned();
            let Some(message) = message else {
                return Ok(());
            };
            self.persist_message(&message)?;
            lock(&self.inner.state).pending_bash.remove(0);
            self.refresh()?;
        }
    }
}

// The ANSI patterns below follow the pinned utils/ansi.ts, derived from ansi-regex
// and strip-ansi, copyright Sindre Sorhus <sindresorhus@gmail.com>.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#[derive(Default)]
struct Sanitizer {
    bytes: Vec<u8>,
    ansi: String,
}
impl Sanitizer {
    fn push(&mut self, bytes: &[u8], finish: bool) -> String {
        self.bytes.extend_from_slice(bytes);
        let mut decoded = std::mem::take(&mut self.ansi);
        loop {
            match std::str::from_utf8(&self.bytes) {
                Ok(text) => {
                    decoded.push_str(text);
                    self.bytes.clear();
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    decoded
                        .push_str(std::str::from_utf8(&self.bytes[..valid]).expect("valid prefix"));
                    match error.error_len() {
                        Some(size) => {
                            decoded.push('\u{fffd}');
                            self.bytes.drain(..valid + size);
                        }
                        None if finish => {
                            decoded.push_str(&String::from_utf8_lossy(&self.bytes[valid..]));
                            self.bytes.clear();
                            break;
                        }
                        None => {
                            self.bytes.drain(..valid);
                            break;
                        }
                    }
                }
            }
        }
        if !finish {
            let split = incomplete_ansi_suffix(&decoded);
            self.ansi = decoded.split_off(split);
        }
        static ANSI: OnceLock<Regex> = OnceLock::new();
        let regex = ANSI.get_or_init(|| Regex::new(r"(?s)(?:\x1b\].*?(?:\x07|\x1b\\|\u009c))|[\x1b\u009b][\[\]()#;?]*(?:[0-9]{1,4}(?:[;:][0-9]{0,4})*)?[0-9A-PR-TZcf-nq-uy=><~]").expect("release ANSI pattern"));
        regex
            .replace_all(&decoded, "")
            .chars()
            .filter(|c| {
                !matches!(*c, '\0'..='\x08' | '\x0b'..='\x1f' | '\u{fff9}'..='\u{fffb}')
                    && *c != '\r'
            })
            .collect()
    }
}
fn incomplete_ansi_suffix(value: &str) -> usize {
    let mut units = 0;
    let start = value
        .char_indices()
        .rev()
        .find_map(|(index, c)| {
            units += c.len_utf16();
            (units > 256).then_some(index + c.len_utf8())
        })
        .unwrap_or(0);
    let window = &value[start..];
    let osc = window.match_indices("\x1b]").find_map(|(index, _)| {
        let suffix = &window[index + 2..];
        (!suffix.contains(['\x07', '\u{009c}']) && !suffix.contains("\x1b\\"))
            .then_some(start + index)
    });
    static INCOMPLETE: OnceLock<Regex> = OnceLock::new();
    let csi = INCOMPLETE
        .get_or_init(|| {
            Regex::new(r"[\x1b\u009b][\[\]()#;?]*(?:[0-9]{1,4}(?:[;:][0-9]{0,4})*)?$")
                .expect("release incomplete ANSI pattern")
        })
        .find(window)
        .map(|m| start + m.start());
    osc.into_iter().chain(csi).min().unwrap_or(value.len())
}
