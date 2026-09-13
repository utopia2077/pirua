use super::support::*;
fn acknowledge() {
    use std::io::Write;
    println!("SQLITE_REPO_PHASE_ACK");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
}
#[test]
fn process_driver() {
    let Ok(root) = std::env::var("PI_SQLITE_REPO_TEST_ROOT") else {
        return;
    };
    let mode = std::env::var("PI_SQLITE_REPO_TEST_MODE").unwrap();
    let phase = std::env::var("PI_SQLITE_REPO_TEST_PHASE").unwrap();
    let r = repo(Path::new(&root));
    match mode.as_str() {
        "create" => {
            acknowledge();
            match ready(r.create(options("competing"))) {
                Ok(s) => {
                    println!("CREATED");
                    ready(s.close()).unwrap();
                }
                Err(SessionError::Storage(StorageError::Busy(_) | StorageError::Conflict(_))) => {
                    println!("REFUSED")
                }
                Err(e) => panic!("unexpected create failure {e:?}"),
            }
        }
        "writer" => {
            let s = ready(r.create(options("writer"))).unwrap();
            append(&s, "before", "acked");
            acknowledge();
            append(&s, "after", "still writable");
            ready(s.close()).unwrap();
        }
        "rewrite" => {
            let m = r.read_list(()).unwrap().remove(0);
            hook(&r, move |at, _| {
                if at == phase {
                    acknowledge();
                }
                Ok(())
            });
            ready(r.rewrite(m, |o| Ok(changed(o)))).unwrap();
        }
        "fork" => {
            let m = r.read_list(()).unwrap().remove(0);
            hook(&r, move |at, _| {
                if at == phase {
                    acknowledge();
                }
                Ok(())
            });
            let fork = ready(r.fork(
                m,
                SessionForkRequest {
                    fork: serde_json::from_value(json!({"scope":"tree"})).unwrap(),
                    create: options("forked"),
                },
            ))
            .unwrap();
            ready(fork.close()).unwrap();
        }
        _ => panic!("unknown child mode"),
    }
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
    fn finish(&mut self, killed: bool) -> String {
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
        eprintln!("actual child terminal {status}; intentional_kill={killed}");
        stdout
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
fn child(root: &std::path::Path, db: &str, phase: &str) -> Child {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "repo::tests::processes::process_driver",
            "--nocapture",
        ])
        .env("PI_SQLITE_REPO_TEST_ROOT", root)
        .env("PI_SQLITE_REPO_TEST_MODE", db)
        .env("PI_SQLITE_REPO_TEST_PHASE", phase)
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
            if line.contains("SQLITE_REPO_PHASE_ACK") {
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
fn two_actual_creators_publish_exactly_one_complete_session() {
    let d = Dir::new();
    let mut a = child(&d.0, "create", "none");
    let mut b = child(&d.0, "create", "none");
    a.acknowledged();
    b.acknowledged();
    drop(a.process.stdin.take());
    drop(b.process.stdin.take());
    let a = a.finish(false);
    let b = b.finish(false);
    assert_eq!(
        usize::from(a.contains("CREATED")) + usize::from(b.contains("CREATED")),
        1
    );
    assert_eq!(
        usize::from(a.contains("REFUSED")) + usize::from(b.contains("REFUSED")),
        1
    );
    let r = repo(&d.0);
    let listed = r.read_list(()).unwrap();
    assert_eq!(listed.len(), 1);
    let actual = observe(&listed[0]);
    assert_eq!(actual.snapshot.registers.len(), 2);
    assert_eq!(actual.snapshot.next_seq, Some(n(2)));
}
#[test]
fn actual_foreign_writer_readonly_repo_and_rejected_admin_leave_writer_usable() {
    let d = Dir::new();
    let mut child = child(&d.0, "writer", "none");
    child.acknowledged();
    let r = repo(&d.0);
    let m = r.read_list(()).unwrap().remove(0);
    let batch = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: None,
                limit: n(100),
            },
        )
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(entry_structure(&batch.entries[0]).id.as_str(), "before");
    assert!(matches!(
        ready(r.delete(m.clone())),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    assert!(matches!(
        ready(r.rewrite(m.clone(), |o| Ok(o.snapshot.clone()))),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    drop(child.process.stdin.take());
    child.finish(false);
    let after = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: Some(batch.next_cursor),
                limit: n(100),
            },
        )
        .unwrap();
    assert_eq!(entry_structure(&after.entries[0]).id.as_str(), "after");
}
#[test]
fn actual_process_rewrite_crash_before_and_after_rename_preserves_complete_file() {
    for phase in ["before_rewrite_finalize", "after_rewrite_publish"] {
        let d = Dir::new();
        let m = seeded(&d, true);
        let before = observe(&m);
        let mut c = child(&d.0, "rewrite", phase);
        c.acknowledged();
        c.process.kill().unwrap();
        c.finish(true);
        let after = observe(&m);
        if phase == "before_rewrite_finalize" {
            assert_eq!(after.snapshot, before.snapshot);
            assert_eq!(after.store_token, before.store_token);
        } else {
            assert_eq!(ids(&after.snapshot), ["A", "C", "D"]);
            assert_eq!(after.metadata.store_generation, n(1));
            assert_ne!(after.store_token, before.store_token);
            let r = repo(&d.0);
            let root = ready(r.open(m)).unwrap();
            ready(root.close()).unwrap();
        }
        assert!(
            filenames(&d)
                .iter()
                .all(|n| !n.starts_with(".pi-sqlite-temp"))
        );
    }
}
#[test]
fn actual_process_fork_first_publication_is_complete_and_source_stays_live() {
    let d = Dir::new();
    let m = seeded(&d, true);
    let r = repo(&d.0);
    let source = ready(r.open(m.clone())).unwrap();
    let original = observe(&m);
    let mut c = child(&d.0, "fork", "before_expose");
    c.acknowledged();
    let fork = repo(&d.0)
        .read_list(())
        .unwrap()
        .into_iter()
        .find(|m| m.base.id.as_str() == "forked")
        .unwrap();
    let copied = observe(&fork);
    assert_eq!(copied.snapshot.entries, original.snapshot.entries);
    assert!(copied.snapshot.usage.is_empty());
    assert_eq!(copied.snapshot.next_seq, Some(n(28)));
    c.process.kill().unwrap();
    c.finish(true);
    assert_eq!(observe(&fork).snapshot, copied.snapshot);
    append(&source, "after-child-fork", "source survives");
    ready(source.close()).unwrap();
}
