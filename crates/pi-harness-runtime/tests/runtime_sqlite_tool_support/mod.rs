//! Test-only gate around the real SQLite writer. It never supplies a CAS result.
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::*;
use pi_agent_session::storage::*;
use pi_agent_sqlite::SqliteStorage;
use pi_ai_core::CancellationToken;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub struct File(pub std::path::PathBuf);
impl File {
    pub fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-runtime-sqlite-tool-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path.join("session.db"))
    }
}
impl Drop for File {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
    }
}

pub struct CheckedSqlite {
    pub inner: Arc<SqliteStorage>,
    pub args_read_target: AtomicUsize,
    pub args_reads: AtomicUsize,
    pub args_read_entered: CancellationToken,
    pub args_read_release: CancellationToken,
    pub attempts: AtomicUsize,
    pub refused: AtomicUsize,
    pub applied: AtomicUsize,
    pub first_entered: CancellationToken,
    pub first_release: CancellationToken,
    pub second_entered: CancellationToken,
    pub second_release: CancellationToken,
    pub second_timestamp: Mutex<Option<SafeUnsigned>>,
    pub application_clock_calls: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub clock_calls_during_commit: AtomicUsize,
    pub preparing_result: AtomicBool,
    after_applied: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl CheckedSqlite {
    pub fn new(
        inner: Arc<SqliteStorage>,
        application_clock_calls: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            inner,
            args_read_target: AtomicUsize::new(0),
            args_reads: AtomicUsize::new(0),
            args_read_entered: CancellationToken::new(),
            args_read_release: CancellationToken::new(),
            attempts: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            applied: AtomicUsize::new(0),
            first_entered: CancellationToken::new(),
            first_release: CancellationToken::new(),
            second_entered: CancellationToken::new(),
            second_release: CancellationToken::new(),
            second_timestamp: Mutex::new(None),
            application_clock_calls,
            clock_calls_during_commit: AtomicUsize::new(0),
            preparing_result: AtomicBool::new(false),
            after_applied: Mutex::new(None),
        }
    }
    pub fn after_applied(&self, callback: impl FnOnce() + Send + 'static) {
        *self.after_applied.lock().unwrap() = Some(Box::new(callback));
    }
}
impl Storage for CheckedSqlite {
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.inner.commit(transaction)
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.inner.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        Box::pin(async move {
            let value = self.inner.get_register(namespace, key).await?;
            if namespace == RegisterNamespace::OperationToolArguments {
                let ordinal = self.args_reads.fetch_add(1, Ordering::SeqCst) + 1;
                if self.args_read_target.load(Ordering::SeqCst) == ordinal {
                    self.args_read_entered.cancel();
                    self.args_read_release.cancelled().await;
                }
            }
            Ok(value)
        })
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        self.inner.list_registers(namespace, prefix)
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_branch(query)
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.inner.scan_branch_structure(query)
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_entries(query)
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.inner.scan_usage(query)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        self.inner.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.close()
    }
}
impl AtomicStorage for CheckedSqlite {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(request)
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.inner.snapshot()
    }
}
impl CheckedStorage for CheckedSqlite {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        self.inner.commit_checked(request, validator)
    }
}
impl RuntimeStorage for CheckedSqlite {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.inner.read_stamp()
    }
    fn prepare_entries(
        &self,
        stamp: ReadStamp,
        slots: Vec<PreparedEntrySlot>,
    ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
        Box::pin(async move {
            let result = slots.iter().any(|slot| matches!(&slot.entry, NewEntry::Message(message)
                if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))));
            if result {
                self.preparing_result.store(true, Ordering::SeqCst);
            }
            let prepared = self.inner.prepare_entries(stamp, slots).await;
            if result {
                self.preparing_result.store(false, Ordering::SeqCst);
            }
            prepared
        })
    }
    fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        prepared: PreparedEntries,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            assert!(prepared.entries().iter().any(|entry| matches!(entry, Entry::Message(message) if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
            match attempt {
                0 => {
                    self.first_entered.cancel();
                    self.first_release.cancelled().await;
                }
                1 => {
                    let Entry::Message(message) = prepared.entries().iter().find(|entry| matches!(entry, Entry::Message(message) if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))).unwrap() else { unreachable!() };
                    *self.second_timestamp.lock().unwrap() = Some(message.timestamp);
                    self.second_entered.cancel();
                    self.second_release.cancelled().await;
                }
                _ => panic!("unexpected extra ToolResult CAS attempt"),
            }
            let calls_before = (self.application_clock_calls)();
            let outcome = self
                .inner
                .commit_prepared_checked(prepared, request, validator)
                .await?;
            let calls_after = (self.application_clock_calls)();
            self.clock_calls_during_commit
                .fetch_add((calls_after - calls_before) as usize, Ordering::SeqCst);
            match &outcome {
                ConditionalOutcome::NotApplied => {
                    self.refused.fetch_add(1, Ordering::SeqCst);
                }
                ConditionalOutcome::Applied(_) => {
                    self.applied.fetch_add(1, Ordering::SeqCst);
                    if let Some(callback) = self.after_applied.lock().unwrap().take() {
                        callback();
                    }
                }
            }
            Ok(outcome)
        })
    }
}

/// The parent owns termination even if an assertion or acknowledgment times out.
pub struct CrashChild {
    process: Option<std::process::Child>,
    lines: std::sync::mpsc::Receiver<String>,
    reader: Option<std::thread::JoinHandle<()>>,
}
impl CrashChild {
    const CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

    pub fn start(path: &std::path::Path) -> Self {
        Self::start_test(path, "sqlite_post_apply_crash_child", None)
    }
    pub fn start_pending(path: &std::path::Path, replay: &str) -> Self {
        Self::start_test(path, "sqlite_pending_crash_child", Some(replay))
    }
    fn start_test(path: &std::path::Path, test: &str, replay: Option<&str>) -> Self {
        use std::process::{Command, Stdio};
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", test, "--ignored", "--nocapture"])
            .env("PI_RUNTIME_SQLITE_CRASH_PATH", path)
            .stdout(Stdio::piped());
        if let Some(replay) = replay {
            command.env("PI_RUNTIME_SQLITE_PENDING_REPLAY", replay);
        }
        let mut process = command.spawn().unwrap();
        let output = process.stdout.take().unwrap();
        let (sender, lines) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(output).lines() {
                if sender.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        Self {
            process: Some(process),
            lines,
            reader: Some(reader),
        }
    }
    pub fn await_applied(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let line = self
                .lines
                .recv_timeout(remaining)
                .expect("child did not acknowledge the applied SQLite commit in time");
            if line.contains("PI_RUNTIME_SQLITE_CRASH:APPLIED:v1") {
                return;
            }
        }
    }
    pub fn await_pending(&self, replay: &str) -> u64 {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let prefix = format!("PI_RUNTIME_SQLITE_PENDING:v1:{replay}:");
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let line = self
                .lines
                .recv_timeout(remaining)
                .expect("child did not acknowledge persisted pending Request in time");
            if let Some((_, mark)) = line.split_once(&prefix) {
                return mark.trim().parse().expect("child ID high-water mark");
            }
        }
    }
    pub fn kill_and_reap(&mut self) {
        let status = self
            .cleanup(std::time::Instant::now() + Self::CLEANUP_TIMEOUT)
            .expect("kill and reap parked SQLite child within deadline")
            .expect("parked SQLite child was already reaped");
        assert!(!status.success(), "parked child exited cleanly");
    }
    fn cleanup(
        &mut self,
        deadline: std::time::Instant,
    ) -> Result<Option<std::process::ExitStatus>, &'static str> {
        let mut kill_failed = false;
        let status = if let Some(process) = self.process.as_mut() {
            kill_failed = process.kill().is_err();
            let status = loop {
                match process.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) => {
                        let remaining =
                            deadline.saturating_duration_since(std::time::Instant::now());
                        if remaining.is_zero() {
                            return Err("SQLite child did not exit within cleanup deadline");
                        }
                        std::thread::sleep(remaining.min(std::time::Duration::from_millis(10)));
                    }
                    Err(_) => return Err("could not poll SQLite child exit"),
                }
            };
            self.process.take();
            Some(status)
        } else {
            None
        };
        if let Some(reader) = self.reader.as_ref() {
            while !reader.is_finished() {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(
                        "SQLite child output reader did not finish within cleanup deadline",
                    );
                }
                std::thread::sleep(remaining.min(std::time::Duration::from_millis(10)));
            }
            self.reader
                .take()
                .unwrap()
                .join()
                .map_err(|_| "SQLite child output reader panicked")?;
        }
        if kill_failed {
            return Err("could not kill parked SQLite child");
        }
        Ok(status)
    }
}
impl Drop for CrashChild {
    fn drop(&mut self) {
        if (self.process.is_some() || self.reader.is_some())
            && let Err(error) = self.cleanup(std::time::Instant::now() + Self::CLEANUP_TIMEOUT)
        {
            eprintln!("SQLite crash child cleanup: {error}");
        }
    }
}

#[test]
fn crash_child_reader_cleanup_respects_deadline() {
    let (_sender, lines) = std::sync::mpsc::channel::<String>();
    let (release, held) = std::sync::mpsc::channel::<()>();
    let reader = std::thread::spawn(move || held.recv().unwrap());
    let mut child = CrashChild {
        process: None,
        lines,
        reader: Some(reader),
    };
    assert!(matches!(
        child.cleanup(std::time::Instant::now() + std::time::Duration::from_millis(30)),
        Err("SQLite child output reader did not finish within cleanup deadline")
    ));
    release.send(()).unwrap();
    assert!(matches!(
        child.cleanup(std::time::Instant::now() + std::time::Duration::from_secs(1)),
        Ok(None)
    ));
}
