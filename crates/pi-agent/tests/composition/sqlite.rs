use super::support::*;
use pi_agent_sqlite::{SqliteOptions, SqliteStorage};
#[test]
fn real_sqlite_rewrite_resets_search_and_erases_old_text() {
    let d = Dir::new();
    let clock = Arc::new(Clock::default());
    let r = sqlite(&d.sub("source"), clock.clone());
    let root = create(r.as_ref(), "rewrite");
    for (i, id, parent, text) in [
        (0, "r0", None, "eraseme"),
        (1, "r1", Some("r0"), "keepneedle"),
        (2, "r2", Some("r1"), "copied eraseme"),
    ] {
        clock.set(3000 + i);
        assert_eq!(
            append(root.as_ref(), id, parent, text).first_seq,
            n(2 + 2 * i)
        );
    }
    let reader = Observed::new(r.clone());
    let search = service(&d.0.join("index.db"), reader.clone());
    ready(search.sync()).unwrap();
    let mut initial = hits(&search, "eraseme", None)
        .unwrap()
        .into_iter()
        .map(|h| h.entry_id.to_string())
        .collect::<Vec<_>>();
    initial.sort();
    assert_eq!(initial, ["r0", "r2"]);
    let cursor = reader.take().last().unwrap().result.next_cursor.clone();
    let metadata = root.metadata();
    let before =
        SqliteStorage::read_snapshot(&metadata.path, &codec(), SqliteOptions::default()).unwrap();
    ready(root.close()).unwrap();
    let new = ready(r.rewrite(metadata, |observation| {
        assert_eq!(observation.snapshot, before.snapshot);
        let mut next = observation.snapshot.clone();
        next.entries
            .retain(|e| entry_structure(e).id.as_str() != "r0");
        for entry in &mut next.entries {
            if let Entry::Message(e) = entry {
                if e.id.as_str() == "r1" {
                    e.parent_id = None;
                }
                if e.id.as_str() == "r2" {
                    e.message = serde_json::from_value(
                        json!({"role":"user","content":"redactedcopy","timestamp":9000}),
                    )
                    .unwrap();
                }
            }
        }
        Ok(next)
    }))
    .unwrap();
    let after =
        SqliteStorage::read_snapshot(&new.path, &codec(), SqliteOptions::default()).unwrap();
    assert_eq!(after.metadata.store_generation, n(1));
    assert_ne!(after.store_token, before.store_token);
    assert_eq!(after.snapshot.next_seq, Some(n(8)));
    let reset = r
        .read_batch(
            &new,
            ReadBatchRequest {
                cursor: Some(cursor),
                limit: n(2),
            },
        )
        .unwrap();
    assert!(reset.reset);
    assert_eq!(
        reset
            .entries
            .iter()
            .map(|e| entry_structure(e).id.to_string())
            .collect::<Vec<_>>(),
        ["r1", "r2"]
    );
    reader.take();
    ready(search.sync()).unwrap();
    assert!(
        reader
            .take()
            .iter()
            .any(|r| r.result.reset && r.result.metadata.store_generation == n(1))
    );
    assert!(
        hits(&search, "eraseme", None).unwrap().is_empty(),
        "actual reset must remove old r0 and copied prose"
    );
    assert_eq!(
        normalized(hits(&search, "keepneedle", None).unwrap()),
        vec![json!({"sessionId":"rewrite","entryId":"r1","timestamp":3001,"snippet":"keepneedle"})]
    );
    assert_eq!(
        normalized(hits(&search, "redactedcopy", None).unwrap()),
        vec![
            json!({"sessionId":"rewrite","entryId":"r2","timestamp":3002,"snippet":"redactedcopy"})
        ]
    );
    assert_eq!(
        SqliteStorage::read_snapshot(&new.path, &codec(), SqliteOptions::default())
            .unwrap()
            .snapshot,
        after.snapshot
    );
    ready(search.close()).unwrap();
    let open = ready(r.open(new)).unwrap();
    assert_eq!(ready(open.get_leaf_id()).unwrap(), Some(eid("r2")));
    ready(open.close()).unwrap();
}
#[test]
fn sqlite_identical_generation_zero_recreation_changes_search_cursor() {
    let d = Dir::new();
    let clock = Arc::new(Clock::default());
    let r = sqlite(&d.sub("source"), clock.clone());
    clock.set(4100);
    let root = create(r.as_ref(), "same-prefix");
    append(root.as_ref(), "same", None, "repeatneedle");
    let metadata = root.metadata();
    let before =
        SqliteStorage::read_snapshot(&metadata.path, &codec(), SqliteOptions::default()).unwrap();
    let reader = Observed::new(r.clone());
    let search = service(&d.0.join("index.db"), reader.clone());
    ready(search.sync()).unwrap();
    let cursor = reader.take().last().unwrap().result.next_cursor.clone();
    ready(root.close()).unwrap();
    ready(r.delete(metadata)).unwrap();
    let new = create(r.as_ref(), "same-prefix");
    append(new.as_ref(), "same", None, "repeatneedle");
    let after =
        SqliteStorage::read_snapshot(&new.metadata().path, &codec(), SqliteOptions::default())
            .unwrap();
    assert_eq!(before.snapshot, after.snapshot);
    assert_ne!(before.store_token, after.store_token);
    assert_eq!(after.metadata.store_generation, n(0));
    let batch = r
        .read_batch(
            &new.metadata(),
            ReadBatchRequest {
                cursor: Some(cursor.clone()),
                limit: n(2),
            },
        )
        .unwrap();
    assert!(batch.reset);
    assert_ne!(batch.next_cursor.witness, cursor.witness);
    ready(search.sync()).unwrap();
    assert!(reader.take().iter().any(|r| r.result.reset));
    assert_eq!(
        normalized(hits(&search, "repeatneedle", None).unwrap()),
        vec![
            json!({"sessionId":"same-prefix","entryId":"same","timestamp":4100,"snippet":"repeatneedle"})
        ]
    );
    ready(search.close()).unwrap();
    ready(new.close()).unwrap();
}
#[test]
#[ignore = "passive child driver; exercised by foreign-writer composition"]
fn source_writer_driver() {
    use std::io::{BufRead, Write};
    let path = PathBuf::from(std::env::var_os("PI_COMPOSITION_SOURCE").unwrap());
    let clock = Arc::new(Clock::default());
    let r = sqlite(&path, clock.clone());
    let root = create(r.as_ref(), "foreign");
    clock.set(5000);
    assert_eq!(
        append(root.as_ref(), "foreign-a", None, "foreignneedle").first_seq,
        n(2)
    );
    println!("COMPOSITION_READY");
    std::io::stdout().flush().unwrap();
    let line = std::io::stdin().lock().lines().next().unwrap().unwrap();
    assert_eq!(line, "continue");
    clock.set(5001);
    assert_eq!(
        append(
            root.as_ref(),
            "foreign-b",
            Some("foreign-a"),
            "foreignneedle"
        )
        .first_seq,
        n(4)
    );
    println!("COMPOSITION_WRITTEN");
    std::io::stdout().flush().unwrap();
    let line = std::io::stdin().lock().lines().next().unwrap().unwrap();
    assert_eq!(line, "close");
    ready(root.close()).unwrap();
}
struct Child {
    process: std::process::Child,
    out: Option<std::thread::JoinHandle<()>>,
    err: Option<std::thread::JoinHandle<String>>,
    events: std::sync::mpsc::Receiver<String>,
}
impl Child {
    fn wait(&self, event: &str) {
        assert_eq!(
            self.events.recv_timeout(Duration::from_secs(15)).unwrap(),
            event
        );
    }
    fn send(&mut self, text: &str) {
        use std::io::Write;
        writeln!(self.process.stdin.as_mut().unwrap(), "{text}").unwrap();
    }
    fn finish(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(s) = self.process.try_wait().unwrap() {
                break s;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(2));
        };
        self.out.take().unwrap().join().unwrap();
        let err = self.err.take().unwrap().join().unwrap();
        assert!(status.success(), "{status}: {err}");
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let Some(h) = self.out.take() {
            let _ = h.join();
        }
        if let Some(h) = self.err.take() {
            let _ = h.join();
        }
    }
}
#[test]
fn same_search_service_reads_foreign_sqlite_writer_that_continues() {
    let d = Dir::new();
    let source = d.sub("source");
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "sqlite::source_writer_driver",
            "--ignored",
            "--nocapture",
        ])
        .env("PI_COMPOSITION_SOURCE", &source)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = process.stdout.take().unwrap();
    let stderr = process.stderr.take().unwrap();
    let (tx, events) = std::sync::mpsc::channel();
    let out = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if line.starts_with("COMPOSITION_") {
                tx.send(line).unwrap();
            }
        }
    });
    let err = std::thread::spawn(move || {
        use std::io::Read;
        let mut all = String::new();
        std::io::BufReader::new(stderr)
            .read_to_string(&mut all)
            .unwrap();
        all
    });
    let mut child = Child {
        process,
        out: Some(out),
        err: Some(err),
        events,
    };
    child.wait("COMPOSITION_READY");
    let r = sqlite(&source, Arc::new(Clock::default()));
    let metadata = r.read_list(()).unwrap().remove(0);
    assert!(matches!(
        ready(r.open(metadata.clone())),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    let reader = Observed::new(r.clone());
    let search = service(&d.0.join("index.db"), reader);
    ready(search.sync()).unwrap();
    assert_eq!(
        normalized(hits(&search, "foreignneedle", None).unwrap()),
        vec![
            json!({"sessionId":"foreign","entryId":"foreign-a","timestamp":5000,"snippet":"foreignneedle"})
        ]
    );
    child.send("continue");
    child.wait("COMPOSITION_WRITTEN");
    ready(search.sync()).unwrap();
    assert_eq!(
        normalized(hits(&search, "foreignneedle", None).unwrap()),
        vec![
            json!({"sessionId":"foreign","entryId":"foreign-a","timestamp":5000,"snippet":"foreignneedle"}),
            json!({"sessionId":"foreign","entryId":"foreign-b","timestamp":5001,"snippet":"foreignneedle"})
        ]
    );
    ready(search.close()).unwrap();
    child.send("close");
    child.finish();
    let open = ready(r.open(metadata)).unwrap();
    assert_eq!(ready(open.get_stats()).unwrap().message_count, n(2));
    ready(open.close()).unwrap();
}
