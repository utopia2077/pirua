//! Actual FileRepo conversion acknowledgment, process exit, and distinct reopen.
mod file_repository_support;
use file_repository_support::*;
use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
};
#[test]
fn file_repository_child_driver() {
    let Ok(mode) = std::env::var("PI_FILE_REPO_CHILD") else {
        return;
    };
    let path = std::path::PathBuf::from(std::env::var_os("PI_FILE_REPO_PATH").unwrap());
    let repo = repository(path.parent().unwrap());
    let session = ready(repo.open_path(&path)).unwrap();
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/session/jsonl/repository-process-v3.expected.json"
    ))
    .unwrap();
    if mode == "writer" {
        assert_eq!(
            ready(session.get_leaf_id()).unwrap().unwrap().as_str(),
            expected["leafId"].as_str().unwrap()
        );
        ready(session.commit(Transaction { writes: vec![] })).unwrap();
        println!("FILE_REPO_CONVERSION_ACK");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        panic!("writer must exit by parent termination");
    } else {
        assert_eq!(mode, "reader");
        assert_eq!(
            ready(session.get_leaf_id()).unwrap().unwrap().as_str(),
            expected["leafId"].as_str().unwrap()
        );
        assert_eq!(ready(session.get_stats()).unwrap().usage, Usage::zero());
        let bytes = std::fs::read(&path).unwrap();
        let rows: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|v| !v.is_empty())
            .map(|v| serde_json::from_slice(v).unwrap())
            .collect();
        assert_eq!(rows[0]["v"], expected["formatAfterAcknowledgment"]);
        assert_eq!(rows.iter().filter(|v| v["kind"] == "usage").count(), 1);
        assert_eq!(rows.last().unwrap(), &serde_json::json!([]));
        ready(session.close()).unwrap();
        println!("FILE_REPO_REOPEN_OK");
    }
}
#[test]
fn actual_legacy_conversion_ack_survives_process_exit_and_repository_reopen() {
    let root = std::env::temp_dir().join(format!("pi-file-repo-process-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("legacy.jsonl");
    std::fs::write(
        &path,
        include_bytes!("../../../conformance/session/jsonl/repository-process-v3.jsonl"),
    )
    .unwrap();
    let executable = std::env::current_exe().unwrap();
    let mut writer = Command::new(&executable)
        .args(["--exact", "file_repository_child_driver", "--nocapture"])
        .env("PI_FILE_REPO_CHILD", "writer")
        .env("PI_FILE_REPO_PATH", &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(writer.stdout.take().unwrap());
    let mut ack = false;
    let mut transcript = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        transcript.push_str(&line);
        if line.trim() == "FILE_REPO_CONVERSION_ACK" {
            ack = true;
            break;
        }
    }
    let _ = writer.kill();
    let status = writer.wait().unwrap();
    drop(reader);
    drop(writer);
    assert!(ack, "writer {status}: {transcript}");
    assert!(!status.success());
    let reader = Command::new(executable)
        .args(["--exact", "file_repository_child_driver", "--nocapture"])
        .env("PI_FILE_REPO_CHILD", "reader")
        .env("PI_FILE_REPO_PATH", &path)
        .output()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        reader.status.success(),
        "{} {}",
        String::from_utf8_lossy(&reader.stdout),
        String::from_utf8_lossy(&reader.stderr)
    );
    assert!(String::from_utf8_lossy(&reader.stdout).contains("FILE_REPO_REOPEN_OK"));
}
