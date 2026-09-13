use super::support::*;
#[test]
fn process_driver() {
    let Ok(root) = std::env::var("PI_SEARCH_TEST_ROOT") else {
        return;
    };
    let db = std::env::var("PI_SEARCH_TEST_DB").unwrap();
    let phase = std::env::var("PI_SEARCH_TEST_PHASE").unwrap();
    let s = SqliteSearchService::with_options(
        db,
        jsonl(std::path::Path::new(&root)),
        (),
        SearchOptions {
            batch_size: n(1),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    if phase != "none" {
        hook(&s, move |at| {
            if at == phase {
                use std::io::Write;
                println!("SEARCH_PHASE_ACK");
                std::io::stdout().flush().unwrap();
                let mut line = String::new();
                std::io::stdin().read_line(&mut line).unwrap();
            }
        });
    }
    ready(s.sync()).unwrap();
    ready(s.close()).unwrap();
}
struct Child {
    process: std::process::Child,
    ack: std::sync::mpsc::Receiver<()>,
    stdout: Option<std::thread::JoinHandle<String>>,
    stderr: Option<std::thread::JoinHandle<String>>,
}
impl Child {
    fn acknowledged(&self) {
        self.ack
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("actual child phase");
    }
    fn finish(&mut self, killed: bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let status = loop {
            if let Some(status) = self.process.try_wait().unwrap() {
                break status;
            }
            assert!(std::time::Instant::now() < deadline, "child timeout");
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        let stdout = self.stdout.take().unwrap().join().unwrap();
        let stderr = self.stderr.take().unwrap().join().unwrap();
        assert_eq!(
            status.success(),
            !killed,
            "child status {status}; stdout={stdout}; stderr={stderr}"
        );
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let Some(h) = self.stdout.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stderr.take() {
            let _ = h.join();
        }
    }
}
fn child(root: &std::path::Path, db: &std::path::Path, phase: &str) -> Child {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "sqlite::tests::processes::process_driver",
            "--nocapture",
        ])
        .env("PI_SEARCH_TEST_ROOT", root)
        .env("PI_SEARCH_TEST_DB", db)
        .env("PI_SEARCH_TEST_PHASE", phase)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let out = process.stdout.take().unwrap();
    let err = process.stderr.take().unwrap();
    let (tx, ack) = std::sync::mpsc::channel();
    let stdout = std::thread::spawn(move || {
        use std::io::BufRead;
        let mut all = String::new();
        for line in std::io::BufReader::new(out).lines() {
            let line = line.unwrap();
            if line.contains("SEARCH_PHASE_ACK") {
                let _ = tx.send(());
            }
            all.push_str(&line);
            all.push('\n');
        }
        all
    });
    let stderr = std::thread::spawn(move || {
        use std::io::Read;
        let mut all = String::new();
        std::io::BufReader::new(err)
            .read_to_string(&mut all)
            .unwrap();
        all
    });
    Child {
        process,
        ack,
        stdout: Some(stdout),
        stderr: Some(stderr),
    }
}
#[test]
fn actual_process_crash_before_and_after_atomic_batch_commit() {
    let d = Dir::new();
    let path = d.0.join("source.jsonl");
    file(
        &path,
        "s",
        0,
        vec![entry("a", 0, "crashneedle"), entry("b", 2, "crashneedle")],
    );
    let before = std::fs::read(path.clone()).unwrap();
    let db = d.0.join("index.db");
    let r = jsonl(&d.0);
    let s = SqliteSearchService::new(&db, r.clone(), ()).unwrap();
    ready(s.close()).unwrap();
    for phase in ["before_commit", "after_commit"] {
        let mut c = child(&d.0, &db, phase);
        c.acknowledged();
        c.process.kill().unwrap();
        c.finish(true);
        let s = SqliteSearchService::new(&db, r.clone(), ()).unwrap();
        let expected = if phase == "before_commit" { 0 } else { 1 };
        assert_eq!(hits(&s, "crashneedle").len(), expected);
        assert_eq!(
            s.inner.progress(Some(&sid("s"))).unwrap().cursor.is_some(),
            expected == 1
        );
        ready(s.close()).unwrap();
    }
    let s = SqliteSearchService::new(&db, r, ()).unwrap();
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "crashneedle"), ["a", "b"]);
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "crashneedle"), ["a", "b"]);
    ready(s.close()).unwrap();
    assert_eq!(std::fs::read(path).unwrap(), before);
}
#[test]
fn actual_two_process_overlap_retries_stale_batch_idempotently() {
    let d = Dir::new();
    file(
        &d.0.join("source.jsonl"),
        "s",
        0,
        vec![entry("a", 0, "sharedneedle"), entry("b", 2, "sharedneedle")],
    );
    let db = d.0.join("index.db");
    let s = SqliteSearchService::new(&db, jsonl(&d.0), ()).unwrap();
    ready(s.close()).unwrap();
    let mut a = child(&d.0, &db, "after_batch");
    a.acknowledged();
    let mut b = child(&d.0, &db, "none");
    b.finish(false);
    // EOF releases each later phase, so no additional process protocol can strand it.
    drop(a.process.stdin.take());
    a.finish(false);
    let s = SqliteSearchService::new(&db, jsonl(&d.0), ()).unwrap();
    assert_eq!(hits(&s, "sharedneedle"), ["a", "b"]);
    ready(s.close()).unwrap();
}
