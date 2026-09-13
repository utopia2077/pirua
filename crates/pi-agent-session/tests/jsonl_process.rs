//! Host-process durability only: ACK after an actual unbuffered file write, kill
//! without close, and reopen in a different process. No fsync/power-loss claim.
#[path = "jsonl_support/mod.rs"]
mod support;
use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    jsonl::{JsonlOptions, JsonlStorage},
    storage::AtomicStorage,
};
use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::Arc,
};
use support::*;
fn codec() -> Arc<SessionCodec> {
    Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    )
}
#[test]
fn jsonl_child_driver() {
    let Ok(mode) = std::env::var("PI_JSONL_CHILD_MODE") else {
        return;
    };
    let path = std::env::var_os("PI_JSONL_CHILD_PATH").unwrap();
    if mode == "writer" {
        let metadata=serde_json::from_value(serde_json::json!({"id":"process","createdAt":1,"storageVersion":1,"storeGeneration":0})).unwrap();
        let storage = JsonlStorage::create(
            &path,
            metadata,
            codec(),
            Arc::new(Clock),
            JsonlOptions::default(),
        )
        .unwrap();
        let result = ready(storage.commit(tx(vec![
            custom("entry", None),
            set("ack", serde_json::from_str("42").unwrap()),
        ])))
        .unwrap();
        assert_eq!(result.seqs, vec![n(0), n(1)]);
        println!("JSONL_DURABLE_ACK");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        panic!("writer must be killed before returning or close");
    } else {
        assert_eq!(mode, "reader");
        let storage =
            JsonlStorage::open(&path, codec(), Arc::new(Clock), JsonlOptions::default()).unwrap();
        let state = ready(storage.snapshot()).unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.registers.len(), 1);
        assert_eq!(state.next_seq, Some(n(2)));
        assert_eq!(
            serde_json::to_value(&state.registers[0]).unwrap()["value"],
            42
        );
        ready(storage.close()).unwrap();
        println!("JSONL_SEPARATE_REOPEN_OK");
    }
}
#[test]
fn acknowledged_commit_survives_writer_kill_and_separate_process_reopen() {
    let path = std::env::temp_dir().join(format!("pi-jsonl-process-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    let file = path.join("session.jsonl");
    let executable = std::env::current_exe().unwrap();
    let mut writer = Command::new(&executable)
        .args(["--exact", "jsonl_child_driver", "--nocapture"])
        .env("PI_JSONL_CHILD_MODE", "writer")
        .env("PI_JSONL_CHILD_PATH", &file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(writer.stdout.take().unwrap());
    let mut ack = false;
    let mut transcript = String::new();
    loop {
        let mut line = String::new();
        if output.read_line(&mut line).unwrap() == 0 {
            break;
        }
        transcript.push_str(&line);
        if line.trim() == "JSONL_DURABLE_ACK" {
            ack = true;
            break;
        }
    }
    let _ = writer.kill();
    let status = writer.wait().unwrap();
    drop(output);
    drop(writer);
    assert!(ack, "writer exited {status}: {transcript}");
    assert!(!status.success());
    let reader = Command::new(executable)
        .args(["--exact", "jsonl_child_driver", "--nocapture"])
        .env("PI_JSONL_CHILD_MODE", "reader")
        .env("PI_JSONL_CHILD_PATH", &file)
        .output()
        .unwrap();
    std::fs::remove_dir_all(path).unwrap();
    assert!(
        reader.status.success(),
        "reader: {} {}",
        String::from_utf8_lossy(&reader.stdout),
        String::from_utf8_lossy(&reader.stderr)
    );
    assert!(String::from_utf8_lossy(&reader.stdout).contains("JSONL_SEPARATE_REOPEN_OK"));
}
