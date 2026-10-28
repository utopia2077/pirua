use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
struct Client {
    contracts: pi_acp_server::protocol::Contracts,
    child: Child,
    input: Option<ChildStdin>,
    lines: mpsc::Receiver<Value>,
    events: Vec<Value>,
    root: PathBuf,
    next: u64,
}
impl Client {
    fn start() -> Self {
        Self::start_mode(true)
    }
    fn start_mode(fixture: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "pirua-acp-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Self::at_mode(root, fixture)
    }
    fn at(root: PathBuf) -> Self {
        Self::at_mode(root, true)
    }
    fn at_mode(root: PathBuf, fixture: bool) -> Self {
        std::fs::create_dir_all(&root).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_pi-acp-server"));
        if fixture {
            command.args(["--fixture-provider", "--fixture-delay-ms", "20"]);
        }
        let mut child = command
            .env_remove("OPENAI_API_KEY")
            .env_remove("GEMINI_API_KEY")
            .env_remove("ANTHROPIC_API_KEY")
            .env_remove("DEEPSEEK_API_KEY")
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("--config-dir")
            .arg(root.join("config"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let line = line.unwrap();
                let value: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("invalid protocol line {line}: {e}"));
                if tx.send(value).is_err() {
                    break;
                }
            }
        });
        Self {
            contracts: pi_acp_server::protocol::Contracts::new(),
            child,
            input,
            lines,
            events: vec![],
            root,
            next: 0,
        }
    }
    fn send(&mut self, value: Value) {
        writeln!(self.input.as_mut().unwrap(), "{value}").unwrap();
        self.input.as_mut().unwrap().flush().unwrap();
    }
    fn send_raw(&mut self, bytes: &[u8]) {
        self.input.as_mut().unwrap().write_all(bytes).unwrap();
        self.input.as_mut().unwrap().flush().unwrap();
    }
    fn read(&mut self) -> Value {
        let v = self
            .lines
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("stdio timeout {e}; events {:?}", self.events));
        if v["method"] == "session/update" {
            self.contracts
                .validate("UpdateSessionNotification", &v["params"])
                .unwrap_or_else(|e| panic!("invalid outgoing update {v}: {e:?}"));
        }
        self.events.push(v.clone());
        v
    }
    fn begin(&mut self, method: &str, params: Value) -> u64 {
        self.next += 1;
        let id = self.next;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        id
    }
    fn result(&mut self, id: u64) -> Value {
        loop {
            let v = self.read();
            if v["id"] == id {
                return v;
            }
        }
    }
    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.begin(method, params);
        let v = self.result(id);
        assert!(v.get("error").is_none(), "{v}");
        v["result"].clone()
    }
    fn init(&mut self, pi: bool) {
        self.call("initialize",json!({"protocolVersion":2,"info":{"name":"rust-child-test","version":"1"},"capabilities":if pi{json!({"_meta":{"pirua.dev/tui":{"version":1}}})}else{json!({})}}));
    }
    fn new_session(&mut self) -> String {
        self.call("session/new", json!({"cwd":self.root.to_string_lossy()}))["sessionId"]
            .as_str()
            .unwrap()
            .into()
    }
    fn prompt(&mut self, id: &str, text: &str) -> Value {
        self.call(
            "session/prompt",
            json!({"sessionId":id,"prompt":[{"type":"text","text":text}]}),
        )
    }
    fn idle(&mut self, id: &str, start: usize) -> Value {
        if let Some(v) = self.events[start..].iter().find(|v| {
            v["params"]["sessionId"] == id
                && v["params"]["update"]["sessionUpdate"] == "state_update"
                && v["params"]["update"]["state"] == "idle"
        }) {
            return v.clone();
        }
        loop {
            let v = self.read();
            if v["params"]["sessionId"] == id
                && v["params"]["update"]["sessionUpdate"] == "state_update"
                && v["params"]["update"]["state"] == "idle"
            {
                return v;
            }
        }
    }
    fn snapshot(&mut self, id: &str) -> Value {
        self.call("_pirua/session/snapshot", json!({"sessionId":id}))
    }
    /// The next form that asks for input; informational notifications have no fields.
    fn form(&mut self) -> Value {
        loop {
            let v = self.read();
            if v["method"] == "elicitation/create"
                && v["params"]["requestedSchema"]["required"].is_array()
            {
                return v;
            }
        }
    }
    fn authenticate_connection(&mut self, pi: bool) -> Value {
        let mut capabilities = json!({"elicitation":{"form":{}}});
        if pi {
            capabilities["_meta"] = json!({"pirua.dev/tui":{"version":1}});
        }
        self.call("initialize", json!({"protocolVersion":2,"info":{"name":"auth-test","version":"1"},"capabilities":capabilities}))
    }
    fn shutdown(&mut self) {
        self.input.take();
        for _ in 0..200 {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("server did not exit after EOF");
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[test]
fn cancelling_production_authentication_returns_protocol_cancellation() {
    let mut c = Client::start_mode(false);
    c.call("initialize", json!({"protocolVersion":2,"info":{"name":"auth-cancel","version":"1"},"capabilities":{"elicitation":{"form":{}}}}));
    let login = c.begin("auth/login", json!({"methodId":"deepseek:apiKey"}));
    loop {
        let frame = c.read();
        if frame["method"] == "elicitation/create" {
            assert_eq!(
                frame["params"]["requestedSchema"]["required"],
                json!(["value"])
            );
            break;
        }
    }
    c.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":login}}));
    let response = c.result(login);
    assert_eq!(response["error"]["code"], -32800, "{response}");
    c.shutdown();
}
#[test]
fn cancelling_a_login_withdraws_its_open_form_at_the_client() {
    let mut c = Client::start_mode(false);
    c.authenticate_connection(false);
    let login = c.begin("auth/login", json!({"methodId":"deepseek:apiKey"}));
    let form = c.form();
    c.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":login}}));
    let response = c.result(login);
    assert_eq!(response["error"]["code"], -32800, "{response}");
    let withdrawn = c
        .events
        .iter()
        .find(|v| v["method"] == "$/cancel_request")
        .unwrap_or_else(|| panic!("the open form was never withdrawn: {:?}", c.events));
    assert_eq!(withdrawn["params"]["requestId"], form["id"]);
    assert!(withdrawn.get("id").is_none(), "a notification has no id");
    c.contracts
        .validate("CancelRequestNotification", &withdrawn["params"])
        .unwrap();
    c.shutdown();
}
#[test]
fn insertion_cancel_queue_clear_and_shutdown_are_real_process_operations() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let receipt = c.prompt(&s, "fixture:block");
    assert!(receipt["messageId"].is_string());
    let snapshot = c.snapshot(&s);
    assert_eq!(snapshot["state"], "running");
    assert!(
        snapshot["history"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["id"] == receipt["messageId"])
    );
    let a = c.call(
        "_pirua/session/queue",
        json!({"sessionId":s,"delivery":"steer","prompt":[{"type":"text","text":"duplicate"}]}),
    );
    let b = c.call(
        "_pirua/session/queue",
        json!({"sessionId":s,"delivery":"steer","prompt":[{"type":"text","text":"duplicate"}]}),
    );
    assert_ne!(a["submissionId"], b["submissionId"]);
    assert!(a.get("messageId").is_none());
    let removed = c.call("_pirua/session/clear_queue", json!({"sessionId":s}));
    assert_eq!(removed["removed"].as_array().unwrap().len(), 2);
    let marker = c.events.len();
    c.call("session/cancel", json!({"sessionId":s}));
    let idle = c.idle(&s, marker);
    assert_eq!(idle["params"]["update"]["stopReason"], "cancelled");
    let snapshot = c.snapshot(&s);
    assert!(snapshot["queue"]["steering"].as_array().unwrap().is_empty());
    assert_eq!(snapshot["stats"]["userMessages"], 1);
    c.prompt(&s, "fixture:block");
    c.shutdown();
}
#[test]
fn request_cancel_before_insertion_and_close_after_insertion() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let request = c.begin(
        "session/prompt",
        json!({"sessionId":s,"prompt":[{"type":"text","text":"fixture:before-insert"}]}),
    );
    loop {
        let v = c.read();
        if v["params"]["update"]["state"] == "running" {
            break;
        }
    }
    let marker = c.events.len();
    c.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":request}}));
    let response = c.result(request);
    assert_eq!(response["error"]["code"], -32800);
    c.call("session/cancel", json!({"sessionId":s}));
    assert_eq!(
        c.idle(&s, marker)["params"]["update"]["stopReason"],
        "cancelled"
    );
    let snapshot = c.snapshot(&s);
    assert_eq!(snapshot["stats"]["userMessages"], 0);
    let r = c.prompt(&s, "fixture:block");
    let marker = c.events.len();
    c.call("session/close", json!({"sessionId":s}));
    assert!(r["messageId"].is_string());
    assert!(
        c.events[marker..]
            .iter()
            .any(|v| v["params"]["update"]["stopReason"] == "cancelled")
    );
}
#[test]
fn stream_snapshot_retry_and_detailed_stop_reasons() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let marker = c.events.len();
    c.prompt(&s, "hello");
    loop {
        let v = c.read();
        if v["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
            break;
        }
    }
    let snapshot = c.snapshot(&s);
    let sequence = snapshot["sequence"].as_u64().unwrap();
    assert_eq!(snapshot["state"], "running");
    let chunks = c
        .events
        .iter()
        .filter(|v| {
            v["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
                && v["params"]["_meta"]["pirua.dev/projection"]["sequence"]
                    .as_u64()
                    .is_some_and(|n| n <= sequence)
        })
        .map(|v| v["params"]["update"]["content"]["text"].as_str().unwrap())
        .collect::<String>();
    let entity = snapshot["liveEntities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["sessionUpdate"] == "agent_message")
        .unwrap();
    let text = entity["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|b| b["text"].as_str())
        .collect::<String>();
    assert_eq!(text, chunks);
    c.idle(&s, marker);
    let marker = c.events.len();
    c.prompt(&s, "fixture:retry");
    let idle = c.idle(&s, marker);
    assert_eq!(idle["params"]["update"]["stopReason"], "end_turn");
    assert!(
        c.events[marker..]
            .iter()
            .any(|v| v["params"]["changes"]["activity"]["retrying"] == true)
    );
    let marker = c.events.len();
    c.prompt(&s, "fixture:length");
    assert_eq!(
        c.idle(&s, marker)["params"]["update"]["stopReason"],
        "max_tokens"
    );
    let marker = c.events.len();
    let receipt = c.prompt(&s, "fixture:error");
    assert!(receipt["messageId"].is_string());
    assert_eq!(
        c.idle(&s, marker)["params"]["update"]["stopReason"],
        "error"
    );
}
#[test]
fn replay_tree_fork_configuration_shell_and_compaction() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let marker = c.events.len();
    let receipt = c.prompt(&s, "fixture:tool");
    c.idle(&s, marker);
    assert!(
        c.events[marker..]
            .iter()
            .any(
                |v| v["params"]["update"]["sessionUpdate"] == "tool_call_update"
                    && v["params"]["update"]["status"] == "completed"
            )
    );
    let snapshot = c.snapshot(&s);
    let revision = snapshot["history"]["revision"].clone();
    let entry = receipt["messageId"].clone();
    c.call(
        "_pirua/session/label",
        json!({"sessionId":s,"entryId":entry,"label":"first","expectedRevision":revision}),
    );
    c.call(
        "_pirua/session/rename",
        json!({"sessionId":s,"name":"Native transcript"}),
    );
    c.call(
        "session/set_config_option",
        json!({"sessionId":s,"configId":"thinkingLevel","type":"id","value":"low"}),
    );
    let fork = c.call(
        "_pirua/session/fork",
        json!({"sessionId":s,"entryId":entry,"position":"before"}),
    );
    assert_ne!(fork["sessionId"], s);
    assert_eq!(fork["editorText"], "fixture:tool");
    assert_eq!(c.snapshot(&s)["sessionId"], s);
    let shell = c.call(
        "_pirua/session/bash",
        json!({"sessionId":s,"command":"printf native-shell"}),
    );
    assert_eq!(shell["output"], "native-shell");
    assert_eq!(shell["exitCode"], 0);
    let marker = c.events.len();
    c.prompt(&s, "/compact");
    c.idle(&s, marker);
    assert!(
        c.events[marker..]
            .iter()
            .any(
                |v| v["params"]["update"]["sessionUpdate"] == "compaction_update"
                    && v["params"]["update"]["status"] == "completed"
            )
    );
    let completed = c.events[marker..]
        .iter()
        .find(|v| {
            v["params"]["update"]["sessionUpdate"] == "compaction_update"
                && v["params"]["update"]["status"] == "completed"
        })
        .unwrap()["params"]["update"]
        .clone();
    let snapshot = c.snapshot(&s);
    let entry_id = &completed["_meta"]["pirua.dev/render"]["entryId"];
    let compact = snapshot["history"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| &e["id"] == entry_id)
        .expect("live completion identifies this committed compaction");
    assert_eq!(compact["summary"], "Fixture compaction summary");
    assert_eq!(compact["usage"]["input"], 37);
    assert_eq!(compact["usage"]["output"], 11);
    assert_eq!(compact["usage"]["cost"]["total"], 0.0048);
    c.call("_pirua/session/reload", json!({"sessionId":s}));
    c.call("session/close", json!({"sessionId":s}));
    let marker = c.events.len();
    c.call(
        "session/resume",
        json!({"sessionId":s,"cwd":c.root.to_string_lossy()}),
    );
    assert!(
        !c.events[marker..]
            .iter()
            .any(|v| v["params"]["update"]["sessionUpdate"] == "user_message")
    );
    assert!(
        c.events[marker..]
            .iter()
            .any(|v| v["params"]["update"]["sessionUpdate"] == "state_update")
    );
    let marker = c.events.len();
    c.call(
        "session/resume",
        json!({"sessionId":s,"cwd":c.root.to_string_lossy(),"replayFrom":{"type":"start"}}),
    );
    assert!(
        c.events[marker..]
            .iter()
            .any(|v| v["params"]["update"]["messageId"] == receipt["messageId"])
    );
    assert!(
        c.events[marker..]
            .iter()
            .any(|v| v["params"]["update"]["sessionUpdate"] == "compaction_update")
    );
    assert!(
        c.events[marker..]
            .iter()
            .any(
                |v| v["params"]["update"]["sessionUpdate"] == "terminal_update"
                    && v["params"]["update"]["command"] == "printf native-shell"
            )
    );
}
#[test]
fn malformed_unknown_batches_and_notification_silence() {
    let mut c = Client::start();
    let v = c.begin("session/list", json!({}));
    assert!(c.result(v)["error"].is_object());
    let v = c.begin(
        "initialize",
        json!({"protocolVersion":1,"info":{"name":"old","version":"1"}}),
    );
    assert_eq!(c.result(v)["error"]["code"], -32022);
    c.init(false);
    let v = c.begin("unknown/method", json!({}));
    assert_eq!(c.result(v)["error"]["code"], -32601);
    let v = c.begin(
        "session/prompt",
        json!({"sessionId":"s","prompt":[{"type":"text","text":false}]}),
    );
    assert_eq!(c.result(v)["error"]["code"], -32602);
    c.send(json!([{"jsonrpc":"2.0","method":"unknown/notify"},{"jsonrpc":"2.0","id":200,"method":"session/list","params":{}},23]));
    let batch = c.read();
    assert_eq!(batch.as_array().unwrap().len(), 2);
    assert!(batch.as_array().unwrap().iter().any(|v| v["id"] == 200));
    assert!(
        batch
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["error"]["code"] == -32600)
    );
    c.send(json!([{"jsonrpc":"2.0","method":"unknown/notify"}]));
    assert!(c.lines.recv_timeout(Duration::from_millis(100)).is_err());
}
#[test]
fn invalid_utf8_lines_are_parse_errors_and_the_connection_keeps_serving() {
    let mut c = Client::start();
    let parse = json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}});
    c.send_raw(b"\xff\xfe\n");
    c.send_raw(b"{\"jsonrpc\":\"2.0\",\"id\":100,\"method\":\"session/list\",\"params\":{\"cwd\":\"\xff\"}}\n");
    c.send_raw(b"\r\n\n");
    for _ in 0..4 {
        assert_eq!(c.read(), parse);
    }
    c.init(false);
    c.send_raw(b"{\"jsonrpc\":\"2.0\",\"id\":101,\"method\":\"session/list\",\"params\":{}}\r\n");
    assert!(c.result(101)["result"]["sessions"].is_array());
    assert!(c.child.try_wait().unwrap().is_none());
    c.shutdown();
    assert!(c.child.wait().unwrap().success());
}
#[test]
fn snapshot_preserves_native_slots_when_text_thought_and_text_share_identity() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    c.prompt(&s, "fixture:multi-slot");
    loop {
        let v = c.read();
        if v["params"]["update"]["_meta"]["pirua.dev/render"]["contentIndex"] == 2 {
            break;
        }
    }
    let snapshot = c.snapshot(&s);
    let entities = snapshot["liveEntities"].as_array().unwrap();
    let text = entities
        .iter()
        .find(|e| e["sessionUpdate"] == "agent_message")
        .unwrap();
    let thought = entities
        .iter()
        .find(|e| e["sessionUpdate"] == "agent_thought")
        .unwrap();
    assert_eq!(text["messageId"], thought["messageId"]);
    let native = &text["_meta"]["pirua.dev/render"]["message"];
    assert_eq!(
        native["content"],
        json!([{"type":"text","text":"first"},{"type":"thinking","thinking":"middle"},{"type":"text","text":"last"}])
    );
    assert_eq!(
        text["content"],
        json!([{"type":"text","text":"first"},{"type":"text","text":"last"}])
    );
    c.call("session/cancel", json!({"sessionId":s}));
}
#[test]
fn queued_idle_inputs_drive_to_insertion_and_stale_or_busy_changes_fail() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let revision = c.snapshot(&s)["history"]["revision"].clone();
    let a = c.call(
        "_pirua/session/queue",
        json!({"sessionId":s,"delivery":"followUp","prompt":[{"type":"text","text":"same"}]}),
    );
    let b = c.call(
        "_pirua/session/queue",
        json!({"sessionId":s,"delivery":"followUp","prompt":[{"type":"text","text":"same"}]}),
    );
    assert_ne!(a["submissionId"], b["submissionId"]);
    let ids = [a["submissionId"].clone(), b["submissionId"].clone()];
    let mut inserted = std::collections::BTreeMap::new();
    loop {
        for event in &c.events {
            let r = &event["params"]["inputResolution"];
            if r["outcome"] == "inserted" && ids.contains(&r["submissionId"]) {
                inserted.insert(
                    r["submissionId"].as_str().unwrap().to_owned(),
                    r["messageId"].as_str().unwrap().to_owned(),
                );
            }
        }
        if inserted.len() == 2 {
            break;
        }
        c.read();
    }
    assert_ne!(inserted.values().next(), inserted.values().next_back());
    c.send(json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":s}}));
    loop {
        let v = c.read();
        if v["params"]["update"]["state"] == "idle" {
            break;
        }
    }
    let stale=c.begin("_pirua/session/label",json!({"sessionId":s,"entryId":inserted.values().next().unwrap(),"label":"old","expectedRevision":revision}));
    assert_eq!(c.result(stale)["error"]["data"]["kind"], "stale_revision");
    c.prompt(&s, "fixture:block");
    let busy = c.begin(
        "_pirua/session/navigate",
        json!({"sessionId":s,"targetId":inserted.values().next().unwrap()}),
    );
    assert_eq!(c.result(busy)["error"]["data"]["kind"], "busy");
    let duplicate = c.begin(
        "session/prompt",
        json!({"sessionId":s,"prompt":[{"type":"text","text":"another"}]}),
    );
    assert_eq!(c.result(duplicate)["error"]["data"]["kind"], "busy");
    let queued=c.call("_pirua/session/queue",json!({"sessionId":s,"delivery":"steer","prompt":[{"type":"text","text":"fixture:transform same"}]}));
    let removed = c.call("_pirua/session/clear_queue", json!({"sessionId":s}));
    assert_eq!(
        removed["removed"][0]["submissionId"],
        queued["submissionId"]
    );
    assert_eq!(removed["removed"][0]["text"], "Transformed: same");
    c.call("session/cancel", json!({"sessionId":s}));
}
#[test]
fn persisted_replay_after_restart_uses_full_retained_branch_and_stable_user_ids() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let marker = c.events.len();
    let first = c.prompt(&s, "original user input");
    c.idle(&s, marker);
    let marker = c.events.len();
    c.prompt(&s, "recent user input");
    c.idle(&s, marker);
    let marker = c.events.len();
    c.prompt(&s, "/compact");
    c.idle(&s, marker);
    c.shutdown();
    let root = std::mem::take(&mut c.root);
    drop(c);
    let mut c = Client::at(root);
    c.init(false);
    let marker = c.events.len();
    c.call(
        "session/resume",
        json!({"sessionId":s,"cwd":c.root.to_string_lossy(),"replayFrom":{"type":"start"}}),
    );
    assert!(
        c.events[marker..]
            .iter()
            .any(|v| v["params"]["update"]["sessionUpdate"] == "user_message"
                && v["params"]["update"]["messageId"] == first["messageId"])
    );
    let users = c.events[marker..]
        .iter()
        .filter(|v| v["params"]["update"]["sessionUpdate"] == "user_message")
        .count();
    assert_eq!(users, 2);
    assert!(
        !c.events
            .iter()
            .any(|v| v["method"] == "_pirua/session/changed")
    );
}
#[test]
fn auth_reverse_request_uses_native_login_logout_without_credentials_on_wire() {
    let temporary = Client::start();
    let root = temporary.root.join("production");
    let mut c = Client::at_mode(root, false);
    let initialization=c.call("initialize",json!({"protocolVersion":2,"info":{"name":"auth-test","version":"1"},"capabilities":{"elicitation":{"form":{}}}}));
    assert!(
        initialization["authMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["methodId"] == "deepseek:apiKey")
    );
    let login = c.begin("auth/login", json!({"methodId":"deepseek:apiKey"}));
    let reverse = loop {
        let v = c.read();
        if v["method"] == "elicitation/create" {
            break v;
        }
    };
    c.contracts
        .validate("CreateElicitationRequest", &reverse["params"])
        .unwrap();
    assert_eq!(reverse["params"]["requestId"], login);
    assert_eq!(reverse["params"]["_meta"]["pirua.dev/auth"]["secret"], true);
    c.send(json!({"jsonrpc":"2.0","id":reverse["id"],"result":{"action":"accept","content":{"value":"synthetic-secret-for-auth-test"}}}));
    assert_eq!(c.result(login)["result"], json!({}));
    assert!(
        !c.events
            .iter()
            .any(|v| v.to_string().contains("synthetic-secret-for-auth-test"))
    );
    c.call("auth/logout", json!({}));
    c.shutdown();
    drop(c);
    drop(temporary);
}
#[test]
fn shell_request_cancellation_stops_the_native_process() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let request = c.begin(
        "_pirua/session/bash",
        json!({"sessionId":s,"command":"printf started; sleep 30; printf should-not-run"}),
    );
    loop {
        let v = c.read();
        if v["params"]["update"]["sessionUpdate"] == "terminal_output_chunk" {
            break;
        }
    }
    c.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":request}}));
    let result = c.result(request);
    assert_eq!(result["result"]["cancelled"], true);
    assert!(
        !result["result"]["output"]
            .as_str()
            .unwrap()
            .contains("should-not-run")
    );
}
#[test]
fn cancellation_and_clear_remain_responsive_during_unadmitted_queue_preprocessing() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    c.prompt(&s, "fixture:block");
    let queue=c.begin("_pirua/session/queue",json!({"sessionId":s,"delivery":"followUp","prompt":[{"type":"text","text":"fixture:before-insert"}]}));
    let removed = c.call("_pirua/session/clear_queue", json!({"sessionId":s}));
    assert_eq!(removed["removed"], json!([]));
    c.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":queue}}));
    assert_eq!(c.result(queue)["error"]["code"], -32800);
    c.call("session/cancel", json!({"sessionId":s}));
}

#[test]
fn unsupported_config_value_variants_and_mismatched_kinds_never_mutate() {
    let mut c = Client::start();
    c.init(true);
    let s = c.new_session();
    let before = c.snapshot(&s);
    for value in [
        json!({"configId":"autoRetry","type":"future_value_shape","value":false}),
        json!({"configId":"autoCompaction","type":"_custom_boolean","value":true}),
        json!({"configId":"thinkingLevel","type":"future_value_shape","value":"low"}),
        json!({"configId":"autoRetry","type":"id","value":"false"}),
        json!({"configId":"thinkingLevel","type":"boolean","value":false}),
        json!({"configId":"thinkingLevel","type":"id","value":"not-advertised"}),
        json!({"configId":"autoRetry","type":"boolean","value":"false"}),
    ] {
        let mut params = value.clone();
        params["sessionId"] = json!(s);
        let start = c.events.len();
        let request = c.begin("session/set_config_option", params);
        let response = c.result(request);
        let after = c.snapshot(&s);
        assert_eq!(
            after, before,
            "Rejected config must leave the complete projection unchanged: {value}; response {response}"
        );
        assert!(
            response["error"]["code"].is_i64(),
            "Expected a defined JSON-RPC error: {response}"
        );
        if value["type"] == "future_value_shape" || value["type"] == "_custom_boolean" {
            assert_eq!(
                response["error"]["data"]["kind"], "unsupported",
                "{response}"
            );
        } else {
            assert_eq!(response["error"]["code"], -32602, "{response}");
        }
        assert!(
            !c.events[start..]
                .iter()
                .any(|v| v["method"] == "session/update"),
            "Rejected config must not publish state/config changes"
        );
    }
    let changed = c.call(
        "session/set_config_option",
        json!({"sessionId":s,"configId":"autoRetry","type":"boolean","value":false}),
    );
    assert!(
        changed["configOptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["configId"] == "autoRetry" && v["currentValue"] == false)
    );
    assert_eq!(c.snapshot(&s)["settings"]["retry"]["enabled"], false);
    c.shutdown();
}

#[test]
fn loaded_image_policy_remains_effective_after_resource_reload() {
    // A valid 2001x1 PNG exceeds the native default width by one pixel.
    const IMAGE: &str = "iVBORw0KGgoAAAANSUhEUgAAB9EAAAABCAIAAADmXckUAAAAHUlEQVR4nO3BMQEAAADCoPVPbQlPoAAAAAAAgIcBF3QAAfL8WSkAAAAASUVORK5CYII=";
    for (auto_resize, block_images) in [(false, true), (false, false), (true, false)] {
        let root = std::env::temp_dir().join(format!(
            "pirua-acp-images-{}-{}-{auto_resize}-{block_images}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("config")).unwrap();
        std::fs::write(
            root.join("config/settings.json"),
            json!({"images":{"autoResize":auto_resize,"blockImages":block_images}}).to_string(),
        )
        .unwrap();
        let mut c = Client::at(root);
        c.init(true);
        let s = c.new_session();
        for reload in [false, true] {
            if reload {
                c.call("_pirua/session/reload", json!({"sessionId":s}));
            }
            let start = c.events.len();
            c.call("session/prompt", json!({"sessionId":s,"prompt":[{"type":"text","text":"inspect policy"},{"type":"image","data":IMAGE,"mimeType":"image/png"}]}));
            c.idle(&s, start);
            let snapshot = c.snapshot(&s);
            assert_eq!(
                snapshot["settings"]["images"],
                json!({"autoResize":auto_resize,"blockImages":block_images})
            );
            let messages = snapshot["messages"].as_array().unwrap();
            let user = messages.iter().rev().find(|m| m["role"] == "user").unwrap();
            let image = user["content"]
                .as_array()
                .unwrap()
                .iter()
                .find(|b| b["type"] == "image")
                .expect("outbound blocking must retain the canonical image");
            assert_eq!(
                image["data"] != IMAGE,
                auto_resize,
                "Actual native image normalization must honor autoResize (reload={reload})"
            );
            let assistant = messages
                .iter()
                .rev()
                .find(|m| m["role"] == "assistant")
                .unwrap();
            let text = assistant["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n");
            assert_eq!(
                text.contains("Image reading is disabled."),
                block_images,
                "The real fixture response reflects its provider-bound text (reload={reload}): {text}"
            );
        }
        c.shutdown();
    }
}
