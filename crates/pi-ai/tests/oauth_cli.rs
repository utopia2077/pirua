#![cfg(feature = "dev-oauth-cli")]
#[allow(dead_code)]
#[path = "../src/bin/pi_ai_cli/mod.rs"]
mod cli;
use cli::{Error, Files, Io, Login, Selected};
use pi_ai_core::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    future::Future,
    path::PathBuf,
    process::{Command, Stdio},
    sync::Mutex,
    time::{Duration, Instant},
};
struct Fake {
    case: Value,
    trace: Mutex<Vec<Value>>,
    answers: Mutex<VecDeque<String>>,
    serial: Mutex<u64>,
}
impl Fake {
    fn record(&self, v: Value) {
        self.trace.lock().unwrap().push(v);
    }
    fn kind(&self, k: &str) -> bool {
        self.case["kind"] == k
    }
}
impl Io for Fake {
    fn open(&self) -> Result<(), Error> {
        let mut n = self.serial.lock().unwrap();
        *n += 1;
        self.record(json!(["open", *n]));
        Ok(())
    }
    fn close(&self) {
        self.record(json!(["close", *self.serial.lock().unwrap()]));
    }
    fn output(&self, s: &str) -> Result<(), Error> {
        self.record(json!(["out", s]));
        Ok(())
    }
    fn line<'a>(
        &'a self,
        q: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async move {
            self.record(json!(["question", *self.serial.lock().unwrap(), q]));
            if self.kind("eof-pending") {
                std::future::pending::<()>().await;
            }
            if self.kind("question-error") {
                return Err(Error::Interaction);
            }
            Ok(self.answers.lock().unwrap().pop_front().unwrap_or_default())
        })
    }
}
impl Files for Fake {
    fn exists(&self) -> Result<bool, Error> {
        self.record(json!(["exists", "auth.json"]));
        if self.kind("exists-error") {
            return Err(Error::File);
        }
        Ok(!self.kind("missing"))
    }
    fn read(&self) -> Result<String, Error> {
        self.record(json!(["read", "auth.json", "utf-8"]));
        if self.kind("read-error") {
            return Err(Error::File);
        }
        Ok(match self.case["kind"].as_str().unwrap_or("") {
            "malformed" => "{",
            "object" => r#"{"unrelated":{"keep":true},"anthropic":"old"}"#,
            "array" => "[1]",
            "null" => "null",
            "number" => "5",
            "string" => "\"abc\"",
            "boolean" => "true",
            _ => "{}",
        }
        .into())
    }
    fn write(&self, s: &str) -> Result<(), Error> {
        self.record(json!([
            "write",
            "auth.json",
            serde_json::from_str::<Value>(s).unwrap(),
            "utf-8"
        ]));
        if self.kind("write-error") {
            return Err(Error::File);
        }
        Ok(())
    }
}
fn credential() -> OAuthCredential {
    serde_json::from_value(json!({"type":"oauth","access":"synthetic-access","refresh":"synthetic-refresh","expires":123,"accountId":"synthetic-account"})).unwrap()
}
fn select() -> AuthPrompt {
    AuthPrompt::Select {
        message: "Method".into(),
        options: vec![
            AuthSelectOption {
                id: "browser".into(),
                label: "Browser".into(),
                description: None,
            },
            AuthSelectOption {
                id: "device".into(),
                label: "Device".into(),
                description: None,
            },
        ],
    }
}
impl Login for Fake {
    fn login<'a>(
        &'a self,
        p: Selected,
        host: &'a dyn AuthInteraction,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, Error>> {
        Box::pin(async move {
            self.record(json!(["login", p.id()]));
            if self.kind("login-error") {
                return Err(Error::Login);
            }
            if self.kind("select-error") {
                host.prompt(select(), c)
                    .await
                    .map_err(|_| Error::InvalidSelection)?;
            }
            if ["empty-secret", "eof-pending", "question-error"]
                .iter()
                .any(|k| self.kind(k))
            {
                let answer = host
                    .prompt(
                        AuthPrompt::Secret {
                            message: "Paste".into(),
                            placeholder: Some("code".into()),
                        },
                        c,
                    )
                    .await
                    .map_err(|_| Error::Interaction)?;
                self.record(json!(["answer", answer]));
            }
            if self.kind("events") {
                for event in [
                    AuthEvent::AuthUrl {
                        url: "https://synthetic.invalid".into(),
                        instructions: Some("Instructions".into()),
                    },
                    AuthEvent::DeviceCode {
                        verification_uri: "https://synthetic.invalid/device".into(),
                        user_code: "SYNTHETIC".into(),
                        interval_seconds: None,
                        expires_in_seconds: None,
                    },
                    AuthEvent::Info {
                        message: "Info".into(),
                        links: None,
                    },
                    AuthEvent::Progress {
                        message: "Progress".into(),
                    },
                ] {
                    host.notify(event).unwrap();
                }
                self.answers.lock().unwrap().push_back("2junk".into());
                let answer = host.prompt(select(), c).await.unwrap();
                self.record(json!(["answer", answer]));
            }
            self.record(json!(["login-success"]));
            Ok(credential())
        })
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn observations() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/oauth-cli/observations.json"
    ))
    .unwrap()
}
#[test]
fn oauth_cli_complete_pinned_source_reexecution() {
    let output = Command::new("node")
        .arg(root().join("conformance/provider-traces/oauth-cli/source.mjs"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(output.status.success(), "source runner failed");
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(actual == observations(), "source observations changed");
    assert_eq!(actual["cases"].as_array().unwrap().len(), 32);
}
#[test]
fn oauth_cli_32_source_command_observations_and_explicit_normalizations() {
    runtime().block_on(async {
        for case in observations()["cases"].as_array().unwrap() {
            let fake = Fake {
                case: case.clone(),
                trace: Mutex::new(vec![]),
                answers: Mutex::new(
                    case["answers"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|v| v.as_str().unwrap().into())
                        .collect(),
                ),
                serial: Mutex::new(0),
            };
            let args = case["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().into())
                .collect::<Vec<_>>();
            let mut expected = case["trace"].as_array().unwrap().clone();
            if fake.kind("eof-pending") {
                let mut future = Box::pin(cli::run(cli::plan(&args).unwrap(), &fake, &fake, &fake));
                assert!(
                    std::future::poll_fn(|cx| std::task::Poll::Ready(
                        future.as_mut().poll(cx).is_pending()
                    ))
                    .await
                );
                assert!(
                    *fake.trace.lock().unwrap() == expected,
                    "pending source trace changed"
                );
                drop(future);
                continue;
            }
            let result = match cli::plan(&args) {
                Ok(plan) => cli::run(plan, &fake, &fake, &fake).await,
                Err(e) => Err(e),
            };
            if ["array", "null", "number", "string", "boolean"]
                .iter()
                .any(|k| fake.kind(k))
            {
                assert_eq!(result, Err(Error::InvalidAuthFile));
                expected
                    .retain(|e| !matches!(e[0].as_str(), Some("write" | "out" | "error" | "exit")));
            } else {
                let source_failed = expected.iter().any(|e| e[0] == "exit");
                assert_eq!(
                    result.is_err(),
                    source_failed,
                    "source result class changed"
                );
                expected.retain(|e| !matches!(e[0].as_str(), Some("error" | "exit")));
            }
            assert!(
                *fake.trace.lock().unwrap() == expected,
                "CLI trace mismatch for {}",
                case["id"]
            );
        }
    });
}
#[test]
fn oauth_cli_auth_file_admission_and_preservation_controls() {
    let c = credential();
    for s in [
        "[]",
        "null",
        "true",
        "1",
        "\"secret sentinel\"",
        r#"{"same":1,"same":2}"#,
        r#"{"nested":{"same":1,"same":2}}"#,
        r#"{"n":9007199254740992}"#,
        r#"{"n":1e999}"#,
        r#"{"s":"\ud800"}"#,
    ] {
        assert!(cli::file::syntax(s));
        assert!(matches!(
            cli::file::updated(Some(s), Selected::Codex, &c),
            Err(Error::InvalidAuthFile)
        ));
    }
    let deep = format!("{{\"v\":{}0{}}}", "[".repeat(200), "]".repeat(200));
    assert!(cli::file::syntax(&deep));
    assert!(matches!(
        cli::file::updated(Some(&deep), Selected::Codex, &c),
        Err(Error::InvalidAuthFile)
    ));
    for malformed in [
        "",
        "{",
        "[1,]",
        "{\"n\":01}",
        "{\"n\":1e}",
        "{\"s\":\"\\x\"}",
        "{}false",
        "{\"v\":true,}",
    ] {
        assert!(!cli::file::syntax(malformed));
        assert!(cli::file::updated(Some(malformed), Selected::Codex, &c).is_ok());
    }
    let previous = r#"{"other":{"nested":[null,true,1.25,"text"]},"openai-codex":"old"}"#;
    let text = cli::file::updated(Some(previous), Selected::Codex, &c).unwrap();
    assert!(!text.ends_with('\n'));
    assert!(text.contains("\n  \""));
    let saved: Value = serde_json::from_str(&text).unwrap();
    let old: Value = serde_json::from_str(previous).unwrap();
    assert!(saved["other"] == old["other"]);
    assert!(
        saved["openai-codex"] == serde_json::to_value(c).unwrap(),
        "credential values or extras changed"
    );
    let mut broken = saved.clone();
    broken["openai-codex"]
        .as_object_mut()
        .unwrap()
        .remove("accountId");
    assert!(broken != saved, "extra-loss control must discriminate");
    broken = saved.clone();
    broken.as_object_mut().unwrap().remove("other");
    assert!(broken != saved, "unrelated-loss control must discriminate");
}
#[test]
fn oauth_cli_decimal_prefix_and_prompt_variance() {
    for (s, want) in [
        ("1x", Some(0)),
        ("\u{feff}+0002.9", Some(1)),
        ("3e2", Some(2)),
        ("0x2", None),
        ("-1", None),
        ("", None),
        ("999999999999999999999", None),
        ("\u{0085}1", None),
    ] {
        assert_eq!(cli::selection(s, 3), want);
    }
    assert!(
        "1x".parse::<usize>().is_err(),
        "whole-string parser counterexample"
    );
    runtime().block_on(async {
        for prompt in [
            AuthPrompt::Text {
                message: "M".into(),
                placeholder: None,
            },
            AuthPrompt::Secret {
                message: "M".into(),
                placeholder: Some("".into()),
            },
            AuthPrompt::ManualCode {
                message: "M".into(),
                placeholder: Some("H".into()),
            },
        ] {
            let fake = Fake {
                case: json!({}),
                trace: Mutex::new(vec![]),
                answers: Mutex::new(VecDeque::from(["  bytes  ".into()])),
                serial: Mutex::new(0),
            };
            let answer = cli::Interaction { io: &fake }
                .answer(prompt, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(answer, "  bytes  ");
        }
    });
}
struct Temp(PathBuf);
impl Temp {
    fn new(label: &str) -> Self {
        let p = std::env::temp_dir().join(format!("oauth-cli-{}-{label}", std::process::id()));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn oauth_cli_real_binary_no_network_help_errors_pipes_and_eof() {
    let temp = Temp::new("binary");
    for (args, input, success) in [
        (vec![], "", true),
        (vec!["help"], "", true),
        (vec!["--help"], "", true),
        (vec!["-h"], "", true),
        (vec!["list", "ignored"], "", true),
        (vec!["unknown"], "", false),
        (vec!["login", "google"], "", false),
        (vec!["login"], "4\n", false),
        (vec!["login"], "2junk\n0\n", false),
        (vec!["login", "openai-codex"], "", false),
    ] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_pi_ai_cli"))
            .args(&args)
            .current_dir(&temp.0)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        {
            use std::io::Write;
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
        }
        let start = Instant::now();
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if start.elapsed() > Duration::from_secs(5) {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("CLI child did not exit")
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.success(), success);
        if success {
            let name = if args.first() == Some(&"list") {
                "list"
            } else {
                "help"
            };
            let source = observations();
            let case = source["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["id"] == name)
                .unwrap();
            let expected = case["trace"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| format!("{}\n", v[1].as_str().unwrap()))
                .collect::<String>();
            assert_eq!(String::from_utf8(output.stdout.clone()).unwrap(), expected);
            assert!(output.stderr.is_empty());
        } else {
            assert!(String::from_utf8_lossy(&output.stderr).starts_with("Error: "));
        }

        assert!(!temp.0.join("auth.json").exists());
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains("https://"),
            "no-network path reached flow authorization stage"
        );
    }
}

struct ControlledInput {
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    entered: std::sync::mpsc::Sender<()>,
    buffer: Vec<u8>,
    offset: usize,
}
impl std::io::Read for ControlledInput {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        use std::io::BufRead;
        let data = self.fill_buf()?;
        let n = data.len().min(bytes.len());
        bytes[..n].copy_from_slice(&data[..n]);
        self.consume(n);
        Ok(n)
    }
}
impl std::io::BufRead for ControlledInput {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.offset == self.buffer.len() {
            let _ = self.entered.send(());
            self.buffer = self.rx.recv().unwrap_or_default();
            self.offset = 0;
        }
        Ok(&self.buffer[self.offset..])
    }
    fn consume(&mut self, n: usize) {
        self.offset += n;
    }
}
#[test]
fn oauth_cli_reader_cancellation_identity_close_empty_and_eof() {
    runtime().block_on(async {
        let (tx, rx) = std::sync::mpsc::channel();
        let (entered, notice) = std::sync::mpsc::channel();
        let reader = cli::console::Reader::new(ControlledInput {
            rx,
            entered,
            buffer: vec![],
            offset: 0,
        })
        .unwrap();
        let original = CancellationToken::new();
        let interface = CancellationToken::new();
        let mut first = Box::pin(reader.line(&original, &interface));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(first.as_mut().poll(cx).is_pending()))
                .await
        );
        notice.recv_timeout(Duration::from_secs(1)).unwrap();
        // A second live question must not consume a line.
        assert_eq!(
            reader.line(&CancellationToken::new(), &interface).await,
            Err(Error::Interaction)
        );
        original.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .expect("canceled reader did not settle"),
            Err(Error::Interaction)
        );
        let next = CancellationToken::new();
        let mut second = Box::pin(reader.line(&next, &interface));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                second.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        tx.send(b"retired synthetic input\n".to_vec()).unwrap();
        notice.recv_timeout(Duration::from_secs(1)).unwrap();
        tx.send(b"fresh input\r\n".to_vec()).unwrap();
        assert!(
            second.await.unwrap() == "fresh input",
            "retired reply reached later question"
        );
        tx.send(b"\n".to_vec()).unwrap();
        assert_eq!(reader.line(&next, &interface).await.unwrap(), "");
        let mut pending = Box::pin(reader.line(&next, &interface));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                pending.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        interface.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pending)
                .await
                .expect("closed interface did not settle"),
            Err(Error::Interaction)
        );
        drop(tx);
        let eof = cli::console::Reader::new(std::io::Cursor::new(Vec::<u8>::new())).unwrap();
        assert_eq!(
            eof.line(&CancellationToken::new(), &CancellationToken::new())
                .await,
            Err(Error::Interaction)
        );
    });
}
