use crate::{protocol::*, types::*};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::BufReader,
    process::{Command, Stdio},
    sync::{Arc, Condvar, Mutex, Weak, mpsc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn boundary(message: impl Into<String>) -> BoundaryError {
    BoundaryError(message.into())
}
fn failure(kind: ErrorKind, message: impl Into<String>) -> ExecutionError {
    ExecutionError {
        kind,
        name: None,
        message: message.into(),
        stack: None,
    }
}
#[derive(Default)]
struct Admission {
    closed: bool,
    active: usize,
    runs: Vec<Weak<Shared>>,
}
struct Owner {
    options: SandboxOptions,
    admission: Mutex<Admission>,
    changed: Condvar,
}
struct Permit(Arc<Owner>);
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.0.admission.lock().unwrap();
        state.active -= 1;
        self.0.changed.notify_all();
    }
}
struct PendingCall {
    call: HostCall,
    started: Instant,
    status: CallStatus,
    duration: u64,
}
struct State {
    finished: bool,
    forced_error: Option<ExecutionError>,
    result: Option<ExecutionResult>,
    events: VecDeque<Event>,
    calls: BTreeMap<u64, PendingCall>,
    waiting: BTreeSet<u64>,
    sequences: BTreeSet<u64>,
    delivery_bytes: usize,
    call_bytes: usize,
    output_chars: usize,
    output: Vec<OutputItem>,
    current_seq: u64,
    issued: u64,
}
impl State {
    fn new() -> Self {
        Self {
            finished: false,
            forced_error: None,
            result: None,
            events: VecDeque::new(),
            calls: BTreeMap::new(),
            waiting: BTreeSet::new(),
            sequences: BTreeSet::new(),
            delivery_bytes: 0,
            call_bytes: 0,
            output_chars: 0,
            output: Vec::new(),
            current_seq: 0,
            issued: 0,
        }
    }
    fn call_results(&mut self) -> Vec<Call> {
        self.calls
            .values_mut()
            .filter(|c| c.call.target == CallTarget::Tool)
            .map(|c| {
                if c.status == CallStatus::Cancelled {
                    c.duration = c.started.elapsed().as_millis() as u64;
                }
                Call {
                    name: c.call.name.clone(),
                    status: c.status.clone(),
                    duration_ms: c.duration,
                }
            })
            .collect()
    }
}
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    cancellation: Cancellation,
}

/// One local backend: a fresh Rust QuickJS helper process for each script.
/// The executable is an explicit trusted host capability, never resolved via PATH.
#[derive(Clone)]
pub struct Sandbox {
    owner: Arc<Owner>,
}
impl Sandbox {
    pub fn new(mut options: SandboxOptions) -> Result<Self, BoundaryError> {
        if options.max_workers == 0
            || options.max_concurrent_tools == 0
            || options.memory_limit_bytes < 8 * 1024 * 1024
            || options.max_stack_bytes < 64 * 1024
            || options.max_stack_bytes > 1024 * 1024
        {
            return Err(boundary(
                "CodeMode requires nonzero admission/tool capacity, heap >= 8 MiB and stack 64 KiB..1 MiB",
            ));
        }
        options.worker_executable = options
            .worker_executable
            .canonicalize()
            .map_err(|e| boundary(format!("CodeMode worker executable: {e}")))?;
        Ok(Self {
            owner: Arc::new(Owner {
                options,
                admission: Mutex::new(Admission::default()),
                changed: Condvar::new(),
            }),
        })
    }
    pub fn active_workers(&self) -> usize {
        self.owner.admission.lock().unwrap().active
    }
    /// Stop every run and wait until every admitted child has actually exited.
    pub fn close(&self) {
        let mut admission = self.owner.admission.lock().unwrap();
        admission.closed = true;
        for shared in admission.runs.iter().filter_map(Weak::upgrade) {
            shared.cancellation.cancel();
        }
        while admission.active > 0 {
            admission = self.owner.changed.wait(admission).unwrap();
        }
    }
    pub fn start(
        &self,
        code: impl Into<String>,
        options: RunOptions,
    ) -> Result<Run, BoundaryError> {
        self.launch(code.into(), options, true)
    }
    fn launch(
        &self,
        code: String,
        mut options: RunOptions,
        controlled: bool,
    ) -> Result<Run, BoundaryError> {
        validate(&code, &mut options)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State::new()),
            changed: Condvar::new(),
            cancellation: Cancellation::default(),
        });
        let permit = {
            let mut admission = self.owner.admission.lock().unwrap();
            if admission.closed {
                return Err(boundary("CodeMode sandbox is closed"));
            }
            if admission.active >= self.owner.options.max_workers {
                return Err(boundary("CodeMode worker admission capacity exhausted"));
            }
            admission.active += 1;
            admission.runs.retain(|run| run.strong_count() > 0);
            admission.runs.push(Arc::downgrade(&shared));
            Permit(self.owner.clone())
        };
        let start = Start {
            code,
            tools: options.tools,
            globals: options.globals,
            store: options
                .store
                .into_iter()
                .map(|(k, v)| (k, serde_json::to_string(&v).unwrap()))
                .collect(),
            started_at: options.started_at,
            seed: options.seed,
            controlled,
            memory: self.owner.options.memory_limit_bytes,
            stack: self.owner.options.max_stack_bytes,
        };
        // The native VM has no OS bindings. Clearing the environment also ensures
        // that credentials are absent even if the helper fails before VM creation.
        let mut child = Command::new(&self.owner.options.worker_executable)
            .env_clear()
            .env("TZ", "UTC")
            .env("LC_ALL", "C")
            .env("LANG", "C")
            .current_dir(std::env::temp_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| boundary(format!("Failed to launch CodeMode worker: {e}")))?;
        let pid = child.id();
        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (writer, deliveries) = mpsc::channel::<Delivery>();
        let (messages, received) = mpsc::sync_channel(64);
        let reader = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let message = read_json::<Message>(&mut reader);
                let ended = !matches!(message, Ok(Some(_)));
                if messages.send(message).is_err() || ended {
                    break;
                }
            }
        });
        let writer_cancel = shared.cancellation.clone();
        let write_thread = thread::spawn(move || {
            if write_json(&mut stdin, &start).is_err() {
                return;
            }
            while !writer_cancel.is_cancelled() {
                match deliveries.recv_timeout(Duration::from_millis(5)) {
                    Ok(delivery) => {
                        if write_json(&mut stdin, &delivery).is_err() {
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });
        let monitor_shared = shared.clone();
        let external = options.cancellation;
        let began = Instant::now();
        thread::spawn(move || {
            let mut terminal: Option<
                Result<(Option<serde_json::Value>, StoreWrites), ExecutionError>,
            > = None;
            let mut during = None;
            loop {
                if let Some(error) = monitor_shared.state.lock().unwrap().forced_error.take() {
                    terminal = Some(Err(error));
                    break;
                }
                if monitor_shared.cancellation.is_cancelled() || external.is_cancelled() {
                    terminal = Some(Err(failure(
                        ErrorKind::Aborted,
                        "CodeMode execution aborted",
                    )));
                    break;
                }
                if options
                    .timeout
                    .is_some_and(|timeout| began.elapsed() >= timeout)
                {
                    terminal = Some(Err(failure(
                        ErrorKind::Timeout,
                        "CodeMode execution deadline exceeded",
                    )));
                    break;
                }
                let message = received.recv_timeout(Duration::from_millis(5));
                // Closing the sandbox also closes the worker input. Its EOF or
                // terminal error may wake this receive before the next loop's
                // cancellation check; admitted host cancellation owns that wait.
                if monitor_shared.cancellation.is_cancelled() || external.is_cancelled() {
                    terminal = Some(Err(failure(
                        ErrorKind::Aborted,
                        "CodeMode execution aborted",
                    )));
                    break;
                }
                match message {
                    Ok(Ok(Some(message))) => {
                        let mut state = monitor_shared.state.lock().unwrap();
                        match process_message(&mut state, message, controlled) {
                            Ok(Some(done)) => {
                                terminal = Some(done);
                                during = controlled.then_some(During {
                                    seq: state.current_seq,
                                    issued: state.issued,
                                });
                            }
                            Ok(None) => {}
                            Err(error) => terminal = Some(Err(error)),
                        }
                        monitor_shared.changed.notify_all();
                        if terminal.is_some() {
                            break;
                        }
                    }
                    Ok(Ok(None)) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                        terminal = Some(Err(failure(
                            ErrorKind::Sandbox,
                            "CodeMode worker exited before reporting an outcome",
                        )));
                        break;
                    }
                    Ok(Err(error)) => {
                        terminal = Some(Err(failure(
                            ErrorKind::Sandbox,
                            format!("CodeMode worker protocol: {error}"),
                        )));
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            {
                let mut state = monitor_shared.state.lock().unwrap();
                state.finished = true;
                monitor_shared.cancellation.cancel();
                monitor_shared.changed.notify_all();
            }
            // Kill and reap even after a successful done: exit() can have been
            // caught by a script that then spins. Admission belongs to process life.
            let _ = child.kill();
            let reaped = child.wait();
            drop(received);
            let _ = reader.join();
            let _ = write_thread.join();
            drop(permit);
            let mut state = monitor_shared.state.lock().unwrap();
            let calls = state.call_results();
            let output = std::mem::take(&mut state.output);
            let done = if let Err(error) = reaped {
                Err(failure(
                    ErrorKind::Sandbox,
                    format!("Failed to reap CodeMode worker: {error}"),
                ))
            } else {
                terminal.unwrap_or_else(|| {
                    Err(failure(
                        ErrorKind::Sandbox,
                        "CodeMode worker ended without outcome",
                    ))
                })
            };
            state.result = Some(match done {
                Ok((value, store_writes)) => ExecutionResult::Success {
                    value,
                    store_writes,
                    output,
                    calls,
                    during,
                },
                Err(error) => ExecutionResult::Error {
                    error,
                    output,
                    calls,
                    during,
                },
            });
            monitor_shared.changed.notify_all();
        });
        Ok(Run {
            shared,
            writer,
            pid,
        })
    }
    /// Execute convenience driver over the controlled bridge. Host callbacks are
    /// concurrent and receive a cooperative cancellation signal. Their code is
    /// host-owned; callbacks must honor cancellation to release their resources.
    pub fn execute<F>(
        &self,
        code: impl Into<String>,
        options: ExecuteOptions,
        handler: F,
    ) -> Result<ExecutionResult, BoundaryError>
    where
        F: Fn(HostCall, Cancellation) -> Outcome + Send + Sync + 'static,
    {
        let started_at = now();
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let run = self.launch(
            code.into(),
            RunOptions {
                started_at,
                seed,
                tools: options.tools,
                globals: options.globals,
                store: options.store,
                timeout: Some(options.timeout.unwrap_or(self.owner.options.timeout)),
                cancellation: options.cancellation,
            },
            false,
        )?;
        let handler = Arc::new(handler);
        let (answers, received) = mpsc::channel();
        let mut queued = VecDeque::new();
        let mut running = 0;
        let mut seq = 0;
        loop {
            if run.is_finished() {
                return Ok(run.wait());
            }
            if let Some(Event::Call(call)) = run.next_event(Duration::from_millis(2)) {
                queued.push_back(call);
            }
            while !run.is_finished() && running < self.owner.options.max_concurrent_tools {
                let Some(call) = queued.pop_front() else {
                    break;
                };
                let handler = handler.clone();
                let answers = answers.clone();
                let cancellation = run.cancellation();
                running += 1;
                thread::spawn(move || {
                    let id = call.id;
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handler(call, cancellation)
                    }))
                    .unwrap_or_else(|_| Outcome::error("Host tool panicked"));
                    let _ = answers.send((id, outcome));
                });
            }
            while let Ok((id, outcome)) = received.try_recv() {
                running -= 1;
                seq += 1;
                if let Err(error) = run.deliver(seq, now(), id, outcome) {
                    // A rejected callback value has not consumed this call/seq.
                    // Keep the admitted run and let the script catch a bounded
                    // tool error, just as for a callback serialization failure.
                    let rejection =
                        Outcome::error(format!("CodeMode host outcome rejected: {error}"));
                    if let Err(error) = run.deliver(seq, now(), id, rejection) {
                        return Ok(run.fail(format!(
                            "CodeMode host outcome could not be delivered: {error}"
                        )));
                    }
                }
            }
        }
    }
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1000.0
}

type Terminal = Result<(Option<serde_json::Value>, StoreWrites), ExecutionError>;
fn process_message(
    state: &mut State,
    message: Message,
    controlled: bool,
) -> Result<Option<Terminal>, ExecutionError> {
    match message {
        Message::Started { fingerprint } => state.events.push_back(Event::Started { fingerprint }),
        Message::Call { call } => {
            let bytes = serde_json::to_vec(&call.args).unwrap().len();
            if call.id != state.issued + 1
                || call.id > MAX_CALLS as u64
                || bytes > MAX_JSON_BYTES
                || state.call_bytes + bytes > MAX_BRIDGE_BYTES
            {
                return Err(failure(
                    ErrorKind::Sandbox,
                    "CodeMode call bridge limit or call order violated",
                ));
            }
            state.call_bytes += bytes;
            state.issued = call.id;
            state.waiting.insert(call.id);
            state.calls.insert(
                call.id,
                PendingCall {
                    call: call.clone(),
                    started: Instant::now(),
                    status: CallStatus::Cancelled,
                    duration: 0,
                },
            );
            state.events.push_back(Event::Call(call));
        }
        Message::Output { item } => {
            let chars = match &item {
                OutputItem::Text { text, .. } => text.encode_utf16().count(),
                OutputItem::Image { data, .. } => data.len(),
            };
            if state.output_chars + chars > MAX_OUTPUT_CHARS
                || state.output.len() >= MAX_OUTPUT_ITEMS
            {
                return Err(failure(
                    ErrorKind::Sandbox,
                    "CodeMode output limit violated",
                ));
            }
            state.output_chars += chars;
            state.output.push(item);
        }
        Message::Done {
            value,
            writes,
            during,
        } => {
            if let Some(during) = during {
                state.current_seq = during.seq;
            }
            return Ok(Some(Ok((value, writes))));
        }
        Message::Failed { error, during } => {
            if let Some(during) = during {
                state.current_seq = during.seq;
            }
            return Ok(Some(Err(error)));
        }
        Message::Idle { seq, issued } => {
            state.current_seq = seq;
            if controlled {
                state.events.push_back(Event::Idle { seq, issued });
            }
        }
    }
    Ok(None)
}

/// A single process-backed run. Dropping it requests cancellation. `wait()` only
/// returns after the helper has been reaped; `is_finished()` can become true earlier.
pub struct Run {
    shared: Arc<Shared>,
    writer: mpsc::Sender<Delivery>,
    pid: u32,
}
impl Drop for Run {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl Run {
    fn fail(&self, message: String) -> ExecutionResult {
        {
            let mut state = self.shared.state.lock().unwrap();
            if !state.finished {
                state.forced_error = Some(failure(ErrorKind::Sandbox, message));
            }
        }
        self.shared.changed.notify_all();
        self.wait()
    }
    pub fn process_id(&self) -> u32 {
        self.pid
    }
    pub fn cancellation(&self) -> Cancellation {
        self.shared.cancellation.clone()
    }
    pub fn cancel(&self) {
        self.shared.cancellation.cancel();
    }
    pub fn is_finished(&self) -> bool {
        self.shared.state.lock().unwrap().finished
    }
    pub fn try_result(&self) -> Option<ExecutionResult> {
        self.shared.state.lock().unwrap().result.clone()
    }
    pub fn wait(&self) -> ExecutionResult {
        let mut state = self.shared.state.lock().unwrap();
        while state.result.is_none() {
            state = self.shared.changed.wait(state).unwrap();
        }
        state.result.clone().unwrap()
    }
    /// Consume the next Started/Call/Idle event in VM order. None means either
    /// timeout or no remaining event after completion; inspect `is_finished()`.
    pub fn next_event(&self, timeout: Duration) -> Option<Event> {
        let mut state = self.shared.state.lock().unwrap();
        if state.events.is_empty() && !state.finished {
            state = self
                .shared
                .changed
                .wait_timeout_while(state, timeout, |s| s.events.is_empty() && !s.finished)
                .unwrap()
                .0;
        }
        state.events.pop_front()
    }
    /// Accept exactly one answer per waiting call and sequence number. Sequence
    /// gaps are buffered in the helper and clocks never move backwards. Validation
    /// errors leave the call and number available for a corrected delivery.
    pub fn deliver(
        &self,
        seq: u64,
        now: f64,
        id: u64,
        outcome: Outcome,
    ) -> Result<(), BoundaryError> {
        let mut state = self.shared.state.lock().unwrap();
        if state.finished {
            return Ok(());
        }
        if !now.is_finite() {
            return Err(boundary("Delivery clock is not a finite number"));
        }
        if seq == 0 || state.sequences.contains(&seq) {
            return Err(boundary(
                "Result sequence numbers start at 1 and may only be used once",
            ));
        }
        if !state.waiting.contains(&id) {
            return Err(boundary(format!("Call {id} is not waiting for an answer")));
        }
        crate::json::check_outcome(&outcome)?;
        let bytes = serde_json::to_vec(&outcome)
            .map_err(|e| boundary(e.to_string()))?
            .len();
        if bytes > MAX_JSON_BYTES || state.delivery_bytes + bytes > MAX_BRIDGE_BYTES {
            return Err(boundary("CodeMode result bridge limit exceeded"));
        }
        let status = if matches!(&outcome, Outcome::Success { .. }) {
            CallStatus::Ok
        } else {
            CallStatus::Error
        };
        self.writer
            .send(Delivery {
                seq,
                now,
                id,
                outcome,
            })
            .map_err(|_| boundary("CodeMode worker input closed"))?;
        state.waiting.remove(&id);
        state.sequences.insert(seq);
        state.delivery_bytes += bytes;
        // Done may follow the last settlement without an idle. The worker reports
        // its exact processing sequence with terminal messages (see protocol).
        if let Some(call) = state.calls.get_mut(&id) {
            call.status = status;
            call.duration = call.started.elapsed().as_millis() as u64;
        }
        Ok(())
    }
}
fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && to_identifier(value) == value
}
fn validate(code: &str, options: &mut RunOptions) -> Result<(), BoundaryError> {
    if code.len() > MAX_JSON_BYTES || (!options.started_at.is_finite() || options.started_at < 0.0)
    {
        return Err(boundary(
            "CodeMode requires finite started_at and source <= 1 MiB",
        ));
    }
    let mut names = BTreeSet::new();
    let mut aliases = BTreeMap::new();
    for tool in &mut options.tools {
        let alias = tool
            .js_name
            .get_or_insert_with(|| to_identifier(&tool.name));
        if !valid_identifier(alias) || !names.insert(tool.name.clone()) {
            return Err(boundary(
                "CodeMode tool names must be unique and script names must be identifiers",
            ));
        }
        if aliases.insert(alias.clone(), tool.name.clone()).is_some() {
            return Err(boundary("CodeMode script names must be unique"));
        }
    }
    for tool in &options.tools {
        if aliases
            .get(&tool.name)
            .is_some_and(|name| name != &tool.name)
        {
            return Err(boundary(
                "CodeMode tool names may not shadow another tool's script name",
            ));
        }
    }
    let reserved = [
        "tools",
        "ALL_TOOLS",
        "console",
        "text",
        "image",
        "exit",
        "store",
        "load",
        "globalThis",
        "Date",
        "Math",
        "JSON",
        "Object",
        "Promise",
        "performance",
        "eval",
        "Function",
        "undefined",
        "NaN",
        "Infinity",
    ];
    let mut globals = BTreeSet::new();
    let mut roots = BTreeSet::new();
    for global in &options.globals {
        let parts: Vec<_> = global.name.split('.').collect();
        if parts.len() > 2
            || parts.iter().any(|part| !valid_identifier(part))
            || reserved.contains(&parts[0])
            || !globals.insert(global.name.clone())
        {
            return Err(boundary(
                "CodeMode globals must be unique identifiers or namespace.member and may not shadow built-ins",
            ));
        }
        roots.insert(parts[0]);
    }
    for root in roots {
        if globals.contains(root)
            && globals
                .iter()
                .any(|name| name.starts_with(&format!("{root}.")))
        {
            return Err(boundary(
                "CodeMode global namespace conflicts with a global function",
            ));
        }
    }
    let mut total = 0;
    for (key, value) in &options.store {
        crate::json::check_value(value)?;
        let json = serde_json::to_string(value).unwrap();
        let chars = json.encode_utf16().count();
        total += key.encode_utf16().count() + chars;
        if chars > MAX_STORE_VALUE_CHARS || total > MAX_STORE_TOTAL_CHARS {
            return Err(boundary("CodeMode initial store exceeds limits"));
        }
    }
    if options.tools.len() + options.globals.len() > MAX_CALLS
        || (serde_json::to_vec(&options.tools).unwrap().len()
            + serde_json::to_vec(&options.globals).unwrap().len())
            > MAX_JSON_BYTES
    {
        return Err(boundary("CodeMode declarations exceed limits"));
    }
    Ok(())
}
