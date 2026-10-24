use pi_acp_server::{Application, Options, serve};
use pi_coding_session::{
    EntryContent, Error, SessionCommit, SessionInfo, SessionSnapshot, SessionStore,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf};
struct FailCommit {
    inner: Arc<dyn SessionStore>,
    role: &'static str,
    once: AtomicBool,
}
impl SessionStore for FailCommit {
    fn create(&self, s: &SessionSnapshot) -> Result<(), Error> {
        self.inner.create(s)
    }
    fn load(&self, id: &str) -> Result<SessionSnapshot, Error> {
        self.inner.load(id)
    }
    fn list(&self, cwd: Option<&str>) -> Result<Vec<SessionInfo>, Error> {
        self.inner.list(cwd)
    }
    fn commit(&self, id: &str, c: &SessionCommit) -> Result<u64, Error> {
        if c.append.iter().any(
            |e| matches!(&e.content,EntryContent::Message{message} if message["role"]==self.role),
        ) && self.once.swap(false, Ordering::SeqCst)
        {
            return Err(Error::Invalid(format!(
                "synthetic {} commit failure",
                self.role
            )));
        }
        self.inner.commit(id, c)
    }
}
struct Wire {
    input: WriteHalf<DuplexStream>,
    output: tokio::io::Lines<BufReader<ReadHalf<DuplexStream>>>,
    events: Vec<Value>,
    next: u64,
}
impl Wire {
    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(std::time::Duration::from_secs(5), self.output.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("protocol output");
        let value = serde_json::from_str(&line).unwrap();
        self.events.push(value);
        self.events.last().unwrap().clone()
    }
    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.next += 1;
        let id = self.next;
        let frame = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        self.input
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        self.input.flush().await.unwrap();
        loop {
            let value = self.read().await;
            if value["id"] == id {
                return value;
            }
        }
    }
    async fn idle(&mut self, start: usize) {
        if self.events[start..]
            .iter()
            .any(|v| v["params"]["update"]["state"] == "idle")
        {
            return;
        }
        loop {
            if self.read().await["params"]["update"]["state"] == "idle" {
                return;
            }
        }
    }
}
#[test]
fn failed_user_commit_never_inserts_and_failed_assistant_commit_cannot_revoke_receipt() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async{
        for role in ["user","assistant"]{
            let root=std::env::temp_dir().join(format!("pirua-acp-storage-{}-{}-{role}",std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
            let options=Options::parse(["--fixture-provider".into(),"--data-dir".into(),root.join("data").to_string_lossy().into_owned(),"--config-dir".into(),root.join("config").to_string_lossy().into_owned()]).unwrap();let mut app=Application::new(options).await.unwrap();app.store=Arc::new(FailCommit{inner:app.store.clone(),role,once:AtomicBool::new(true)});
            let(client,server)=tokio::io::duplex(65536);let(server_input,server_output)=tokio::io::split(server);let task=tokio::spawn(serve(app,server_input,server_output));let(output,input)=tokio::io::split(client);let mut wire=Wire{input,output:BufReader::new(output).lines(),events:vec![],next:0};
            wire.call("initialize",json!({"protocolVersion":2,"info":{"name":"storage-test","version":"1"},"capabilities":{"_meta":{"pirua.dev/tui":{"version":1}}}})).await;let created=wire.call("session/new",json!({"cwd":root.to_string_lossy()})).await;let id=created["result"]["sessionId"].clone();let start=wire.events.len();let response=wire.call("session/prompt",json!({"sessionId":id,"prompt":[{"type":"text","text":"failed first"}]})).await;wire.idle(start).await;
            if role=="user"{assert!(response["error"].is_object(),"{response}");assert!(!wire.events[start..].iter().any(|v|v["params"]["update"]["sessionUpdate"]=="user_message"));}else{assert!(response["result"]["messageId"].is_string(),"{response}");assert!(wire.events[start..].iter().any(|v|v["params"]["update"]["stopReason"]=="error"));}
            let next=wire.call("session/prompt",json!({"sessionId":id,"prompt":[{"type":"text","text":"successful next"}]})).await;assert!(next["result"]["messageId"].is_string(),"{next}");let snapshot=wire.call("_pirua/session/snapshot",json!({"sessionId":id})).await;if role=="user"{assert!(!snapshot["result"]["messages"].as_array().unwrap().iter().any(|m|m.to_string().contains("failed first")));}
            wire.input.shutdown().await.unwrap();drop(wire.input);tokio::time::timeout(std::time::Duration::from_secs(5),task).await.unwrap().unwrap().unwrap();std::fs::remove_dir_all(root).unwrap();
        }
    });
}
