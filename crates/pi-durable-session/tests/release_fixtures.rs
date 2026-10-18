//! Consumes independently generated, immutable release evidence. Native comparison
//! follows the adapter boundary documented in the oracle README; this file owns no expectations.
mod support;
use pi_durable_session::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use support::*;
fn value<T: Serialize>(v: T) -> Value {
    serde_json::to_value(v).unwrap()
}
fn parse<T: DeserializeOwned>(v: &Value) -> Result<T> {
    serde_json::from_value(v.clone()).map_err(Error::from)
}
fn undefined() -> Value {
    json!({"$undefined":true})
}
fn option<T: Serialize>(v: Option<T>) -> Value {
    v.map(value).unwrap_or_else(undefined)
}
fn result<T: Serialize>(r: Result<T>) -> Value {
    match r {
        Ok(v) => value(v),
        Err(e) => json!({"error":{"kind":format!("{:?}",e.kind),"message":e.message}}),
    }
}
fn resolve(v: &Value, aliases: &BTreeMap<String, Value>) -> Value {
    match v {
        Value::String(s) if s.starts_with('$') => {
            let mut path = s[1..].split('.');
            let name = path.next().unwrap();
            let mut result = aliases
                .get(name)
                .unwrap_or_else(|| panic!("unknown ref {s}"))
                .clone();
            for key in path {
                result = result[key].clone();
            }
            result
        }
        Value::Array(a) => Value::Array(a.iter().map(|v| resolve(v, aliases)).collect()),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), resolve(v, aliases)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
fn normalize(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut m = m.clone();
            if m.contains_key("error") {
                m.insert("error".into(), json!(true));
            }
            if m.get("type") == Some(&json!("document.copy")) {
                m.remove("value");
                m.remove("version");
            }
            if let Some(cursor) = m.get_mut("next").and_then(Value::as_object_mut) {
                cursor.retain(|k, _| k == "after" || k == "order");
            }
            Value::Object(m.into_iter().map(|(k, v)| (k, normalize(&v))).collect())
        }
        Value::Array(a) => Value::Array(a.iter().map(normalize).collect()),
        Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && f.fract() == 0.0
                && f >= 0.0
                && f <= MAX_SAFE_INTEGER as f64
            {
                return json!(f as u64);
            }
            v.clone()
        }
        _ => v.clone(),
    }
}
fn read(s: &dyn Storage, method: &str, a: &[Value]) -> Result<Value> {
    let c = |i: usize| ConversationId::new(a[i].as_u64().unwrap());
    let e = |i: usize| EntryId::new(a[i].as_u64().unwrap());
    let n = |i: usize| a[i].as_u64().unwrap() as usize;
    let cursor = |i: usize| {
        a.get(i)
            .filter(|v| **v != undefined())
            .map(parse::<Cursor>)
            .transpose()
    };
    Ok(match method {
        "conversation" => option(s.conversation(c(0)?)?),
        "entry" => {
            if a.len() == 1 {
                option(s.entry(e(0)?)?)
            } else {
                option(s.visible_entry(c(0)?, e(1)?)?)
            }
        }
        "findLatestHeadMarker" => option(
            s.find_latest_head_marker(
                c(0)?,
                a.get(1)
                    .filter(|v| **v != undefined())
                    .map(parse)
                    .transpose()?,
            )?,
        ),
        "scanConversations" => {
            value(s.scan_conversations(&parse(&a[0])?, n(1), cursor(2)?.as_ref())?)
        }
        "scanEntries" => value(s.scan_entries(&parse(&a[0])?, n(1), cursor(2)?.as_ref())?),
        "task" => option(s.task(parse(&a[0])?)?),
        "scanTasks" => value(s.scan_tasks(&parse(&a[0])?, n(1), cursor(2)?.as_ref())?),
        "submission" => option(s.submission(parse(&a[0])?)?),
        "scanSubmissions" => {
            value(s.scan_submissions(&parse(&a[0])?, n(1), cursor(2)?.as_ref())?)
        }
        "submissionByRequest" => option(s.submission_by_request(c(0)?, a[1].as_str().unwrap())?),
        "document" => option(s.document(parse(&a[0])?, parse(&a[1])?)?),
        "findDocument" => option(s.find_document(&parse(&a[0])?, parse(&a[1])?)?),
        "scanDocuments" => value(s.scan_documents(&parse(&a[0])?, n(1), cursor(2)?.as_ref())?),
        _ => panic!("unimplemented fixture storage read {method}"),
    })
}
struct Definition {
    definition: DocDefinition,
    descriptor: Value,
}
fn definitions(case: &Value, calls: Arc<Mutex<Vec<Value>>>) -> BTreeMap<String, Definition> {
    let mut result = BTreeMap::new();
    for d in case["input"]["definitions"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let d = d.clone();
        let name = d["name"].as_str().unwrap().to_owned();
        let kind = d["kind"].as_str().unwrap();
        let semantics = match d["scope"].as_str().unwrap() {
            "session" => DocumentSemantics::Session,
            "conversation" => DocumentSemantics::Conversation {
                history: parse(&d["history"]).unwrap(),
                fork: parse(&d["fork"]).unwrap(),
            },
            "task" => DocumentSemantics::Task,
            _ => unreachable!(),
        };
        let initial = d["initial"].clone();
        let seed_field = d["seedField"].as_str().map(str::to_owned);
        let policy = d["checkpoint"].clone();
        let callback_name = name.clone();
        let callback_calls = calls.clone();
        let definition=DocDefinition::new(kind,d["version"].as_u64().unwrap() as u32,semantics,d["family"].as_bool().unwrap_or(false),move|seed|{let mut value=initial.clone();if let Some(field)=&seed_field{value[field]=seed.clone();}Ok(value)},move|v,ops,info|{callback_calls.lock().unwrap().push(json!({"doc":callback_name,"value":v,"ops":ops,"info":{"deltasSinceBase":info.deltas_since_base}}));if policy==json!("throw"){return Err(Error::validation("oracle checkpoint policy failure"))}Ok(policy==json!("always")||policy.as_u64().is_some_and(|n|info.deltas_since_base>=n as usize))}).unwrap();
        result.insert(
            name,
            Definition {
                definition,
                descriptor: d,
            },
        );
    }
    result
}
fn doc_args(
    op: &Value,
    defs: &BTreeMap<String, Definition>,
) -> (DocDefinition, DocumentAddress, Value) {
    let d = &defs[op["doc"].as_str().unwrap()];
    let scope = match d.descriptor["scope"].as_str().unwrap() {
        "session" => Scope::Session,
        "conversation" => Scope::Conversation {
            conversation_id: parse(&op["owner"]).unwrap(),
        },
        "task" => Scope::Task {
            task_id: parse(&op["owner"]).unwrap(),
        },
        _ => unreachable!(),
    };
    let key = op["key"].as_str().map(str::to_owned);
    (
        d.definition.clone(),
        d.definition.address(scope, key).unwrap(),
        op.get("seed").cloned().unwrap_or(Value::Null),
    )
}
fn get_path<'a>(v: &'a Value, p: &[Segment]) -> &'a Value {
    let mut v = v;
    for s in p {
        v = match s {
            Segment::Key(k) => &v[k],
            Segment::Index(i) => &v[*i],
        };
    }
    v
}
fn edits(tx: &mut Transaction<'_>, id: DocumentId, edits: &Value) -> Result<()> {
    for edit in edits.as_array().into_iter().flatten() {
        let a = edit.as_array().unwrap();
        let path: Path = parse(&a[1])?;
        let op = match a[0].as_str().unwrap() {
            "set" => Op::Set(path, a[2].clone()),
            "delete" => Op::Delete(path),
            "append" => Op::Append(path, a[2].as_str().unwrap().to_owned()),
            "trim" => Op::Trim(path, a[2].as_u64().unwrap() as usize),
            "splice" => Op::Splice(
                path,
                a[2].as_u64().unwrap() as usize,
                a[3].as_u64().unwrap() as usize,
                a[4].as_array().unwrap().clone(),
            ),
            "reverse" => {
                let n = get_path(tx.document_value(id)?, &path)
                    .as_array()
                    .unwrap()
                    .len();
                Op::Permute(path, (0..n).rev().collect())
            }
            _ => panic!("unknown edit"),
        };
        tx.mutate(id, op)?;
    }
    Ok(())
}
fn tx_read(tx: &mut Transaction<'_>, method: &str, a: &[Value]) -> Result<Value> {
    Ok(match method {
        "conversation" => option(tx.conversation(parse(&a[0])?)?),
        "entry" => option(tx.entry(parse(&a[0])?)?.map(|s| s.entry)),
        "task" => option(tx.task(parse(&a[0])?)?),
        _ => panic!("unimplemented fixture tx read {method}"),
    })
}
fn tx_step(
    tx: &mut Transaction<'_>,
    raw: &Value,
    defs: &BTreeMap<String, Definition>,
    aliases: &mut BTreeMap<String, Value>,
) -> Result<Value> {
    let step = resolve(raw, aliases);
    let r = match step["op"].as_str().unwrap() {
        "createConversation" => value(tx.create_conversation(ConversationOwnership::Ownerless)?),
        "forkConversation" => value(tx.fork_conversation(
            parse(&step["conversationId"])?,
            parse(&step["at"])?,
            ConversationOwnership::Ownerless,
        )?),
        "appendEntry" => {
            let v = &step["value"];
            let draft = EntryDraft {
                kind: v["kind"].as_str().unwrap().into(),
                model: v.get("model").map(parse).transpose()?,
                data: v.get("data").cloned(),
                head: v
                    .get("head")
                    .map(|h| {
                        if h == "self" {
                            Ok(EntryHead::Own)
                        } else {
                            parse(h).map(EntryHead::Entry)
                        }
                    })
                    .transpose()?,
                edits: v.get("edits").map(parse).transpose()?,
            };
            value(tx.append_entry(parse(&step["conversationId"])?, draft)?)
        }
        "createSubmission" => {
            let mut v = step["value"].clone();
            v["id"] = json!(2);
            let r: SubmissionRecord = parse(&v)?;
            value(tx.create_submission(SubmissionCreate {
                conversation_id: r.conversation_id,
                submission_type: r.submission_type,
                request_id: r.request_id,
                trace: r.trace,
                state: r.state,
            })?)
        }
        "placeSubmission" => {
            tx.place_submission(parse(&step["id"])?, parse(&step["entry"])?)?;
            undefined()
        }
        "settleSubmission" => {
            let s = &step["settlement"];
            let settlement = if s["status"] == "done" {
                SubmissionSettlement::Done {
                    answer: parse(&s["answer"])?,
                }
            } else {
                SubmissionSettlement::Unanswered {
                    reason: s["reason"].as_str().unwrap().into(),
                    detail: s.get("detail").cloned(),
                }
            };
            tx.settle_submission(parse(&step["id"])?, settlement)?;
            undefined()
        }
        "doc" => {
            let (d, a, seed) = doc_args(&step, defs);
            let id = tx.doc(&d, a, seed)?;
            edits(tx, id, &step["edits"])?;
            tx.document_value(id)?.clone()
        }
        "docSame" => {
            let (d, a, seed) = doc_args(&step, defs);
            let first = tx.doc(&d, a.clone(), seed.clone())?;
            let second = tx.doc(&d, a, seed)?;
            json!(first == second)
        }
        "retireDoc" => {
            let (d, a, _) = doc_args(&step, defs);
            tx.retire_doc(&d, a)?;
            undefined()
        }
        "read" => tx_read(
            tx,
            step["method"].as_str().unwrap(),
            step["args"].as_array().unwrap(),
        )?,
        "throw" => return Err(Error::validation(step["message"].as_str().unwrap())),
        op => panic!("unimplemented tx step {op}"),
    };
    if let Some(name) = step["as"].as_str() {
        aliases.insert(name.into(), r.clone());
    }
    Ok(r)
}
fn publication(p: &CommitPublication) -> Value {
    let changes=p.changes.iter().map(|change| match change {
        CommitChange::Table(write)=>value(write),
        CommitChange::Document {record,conversation_id,version,value:v,ops,..}=>{
            let mut change=json!({"type":"document","record":record,"value":v.as_ref(),"ops":ops});
            if let Some(id)=conversation_id {change["conversationId"]=value(id);}
            if let Some(version)=version {change["version"]=json!(version);}
            change
        }
        CommitChange::DocumentCopy {record,conversation_id,source,..}=>json!({"type":"document.copy","record":record,"conversationId":conversation_id,"source":source}),
    }).collect::<Vec<_>>();
    json!({"seq":p.seq,"changes":changes})
}
fn sorted_changes(v: &Value) -> Value {
    let mut v = v.clone();
    if let Some(changes) = v["changes"].as_array_mut() {
        changes.sort_by_key(|c| {
            format!(
                "{}:{:020}",
                c["type"].as_str().unwrap(),
                c.get("record").or(c.get("value")).unwrap()["id"]
                    .as_u64()
                    .unwrap()
            )
        });
    }
    v
}
fn verify_publications(actual: &[Arc<CommitPublication>], expected: &Value) {
    let expected = expected.as_array().unwrap();
    assert_eq!(actual.len(), expected.len(), "publication count");
    let mut previous: BTreeMap<u64, Value> = BTreeMap::new();
    for (a, e) in actual.iter().zip(expected) {
        let mut a = publication(a);
        let mut e = e.clone();
        for c in a["changes"].as_array_mut().unwrap() {
            if c["type"] == "document" {
                let id = c["record"]["id"].as_u64().unwrap();
                let ops: Vec<Op> = parse(&c["ops"]).unwrap();
                if !c["value"].is_null() {
                    if let Some(before) = previous.get(&id) {
                        assert_eq!(
                            apply(before, &ops).unwrap(),
                            c["value"],
                            "native publication replay"
                        );
                    }
                    previous.insert(id, c["value"].clone());
                }
                c.as_object_mut().unwrap().remove("ops");
            }
        }
        for c in e["changes"].as_array_mut().unwrap() {
            if c["type"] == "document" {
                c.as_object_mut().unwrap().remove("ops");
            }
        }
        assert_eq!(
            normalize(&sorted_changes(&a)),
            normalize(&sorted_changes(&e)),
            "publication metadata/value/sequence"
        );
    }
}
fn verify_writes(actual: &[Vec<StorageWrite>], expected: &Value, publications: &Value) {
    let expected = expected.as_array().unwrap();
    let publications = publications.as_array().unwrap();
    assert_eq!(actual.len(), expected.len(), "admitted batch count");
    let mut previous: BTreeMap<u64, Value> = BTreeMap::new();
    let mut revisions: BTreeMap<u64, Vec<(u64, Value)>> = BTreeMap::new();
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        let mut a = value(actual);
        let mut e = expected.clone();
        let order = |batch: &mut Value| {
            batch.as_array_mut().unwrap().sort_by_key(|w| {
                format!(
                    "{}:{:020}",
                    w["type"].as_str().unwrap(),
                    w.get("record")
                        .or(w.get("value"))
                        .and_then(|v| v["id"].as_u64())
                        .or(w["id"].as_u64())
                        .unwrap()
                )
            });
        };
        order(&mut a);
        order(&mut e);
        assert_eq!(
            a.as_array().unwrap().len(),
            e.as_array().unwrap().len(),
            "write cardinality"
        );
        for (actual, expected) in a
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .zip(e.as_array_mut().unwrap())
        {
            if actual["type"] == "document.change"
                && actual["content"]["kind"] == "delta"
                && expected["content"]["kind"] == "delta"
            {
                let id = actual["id"].as_u64().unwrap();
                let ops: Vec<Op> = parse(&actual["content"]["ops"]).unwrap();
                let expected_ops: Vec<Op> = parse(&expected["content"]["ops"]).unwrap();
                assert_eq!(
                    ops.is_empty(),
                    expected_ops.is_empty(),
                    "empty versus structural batch"
                );
                if !expected_ops.iter().any(|op| matches!(op, Op::Replace(_))) {
                    assert!(
                        !ops.iter().any(|op| matches!(op, Op::Replace(_))),
                        "root replacement cannot stand in for positional operations"
                    );
                }
                if expected_ops
                    .iter()
                    .any(|op| matches!(op,Op::Splice(_,_,n,_) if *n>0))
                {
                    assert!(
                        ops.iter().any(|op| matches!(op,Op::Splice(_,_,n,_) if *n>0)
                            || matches!(op, Op::Delete(_))),
                        "positional removals must survive preparation"
                    );
                }
                if let Some(before) = previous.get(&id) {
                    assert_eq!(
                        apply(before, &ops).unwrap(),
                        apply(before, &expected_ops).unwrap(),
                        "independent emitted-batch replay"
                    );
                }
                actual["content"].as_object_mut().unwrap().remove("ops");
                expected["content"].as_object_mut().unwrap().remove("ops");
            }
        }
        assert_eq!(
            normalize(&a),
            normalize(&e),
            "storage command metadata/base/checkpoint selection"
        );
        let publication = &publications[index];
        let seq = publication["seq"].as_u64().unwrap();
        for change in publication["changes"].as_array().unwrap() {
            let Some(record) = change.get("record") else {
                continue;
            };
            let id = record["id"].as_u64().unwrap();
            if change["type"] == "document" && !change["value"].is_null() {
                previous.insert(id, change["value"].clone());
                revisions
                    .entry(id)
                    .or_default()
                    .push((seq, change["value"].clone()));
            } else if change["type"] == "document.copy" {
                let source = &change["source"];
                let source_id = source["id"].as_u64().unwrap();
                let selected = if source["at"] == "current" {
                    previous.get(&source_id).cloned()
                } else {
                    revisions
                        .get(&source_id)
                        .and_then(|rs| {
                            rs.iter()
                                .rev()
                                .find(|(seq, _)| *seq <= source["at"].as_u64().unwrap())
                        })
                        .map(|(_, v)| v.clone())
                };
                if let Some(selected) = selected {
                    previous.insert(id, selected.clone());
                    revisions.entry(id).or_default().push((seq, selected));
                }
            }
        }
    }
}
struct WatchTrace {
    watch: DocumentWatch,
    frames: Arc<Mutex<Vec<Value>>>,
    notify: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Notify>,
}
async fn watch_frames(w: &WatchTrace, expected_len: usize) -> Value {
    loop {
        let notified = w.notify.notified();
        let frames = w.frames.lock().unwrap().clone();
        if frames.len() >= expected_len {
            return value(frames);
        }
        tokio::time::timeout(std::time::Duration::from_secs(3), notified)
            .await
            .expect("watch failed to deliver expected frame");
    }
}
fn watch_end(e: WatchEnd) -> Value {
    json!({"reason":match e{WatchEnd::Stopped=>"stopped",WatchEnd::Cancelled=>"cancelled",WatchEnd::SessionClosed=>"session_closed",WatchEnd::Retired=>"retired",WatchEnd::ListenerError(_)=>"listener_error",WatchEnd::SessionFailed(_)=>"session_failed"}})
}
async fn run(case: &Value, backend: &str) {
    if case["family"] == "delta" {
        let input = &case["input"];
        let r = parse::<Vec<Op>>(&input["ops"]).and_then(|ops| apply(&input["value"], &ops));
        let expected = &case["expected"]["chord"]["observations"];
        assert_eq!(
            normalize(&result(r)),
            normalize(&expected[0]["value"]),
            "{}",
            case["id"]
        );
        assert_eq!(
            &input["value"],
            &expected.as_array().unwrap().last().unwrap()["value"]
        );
        return;
    }
    let temp = Temp::new();
    let make = || -> Arc<dyn Storage> {
        if backend == "memory" {
            Arc::new(MemoryStorage::new())
        } else {
            Arc::new(SqliteStorage::open(&temp.path).unwrap())
        }
    };
    let mut probe = Probe::new(make());
    let is_session = case["family"] != "storage";
    let mut session =
        is_session.then(|| Session::with_clock(probe.clone(), || 1234567890000.0).unwrap());
    let mut subscription = if let Some(s) = &session {
        Some(s.subscribe_commits().await.unwrap())
    } else {
        None
    };
    let mut publications = Vec::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let defs = Arc::new(definitions(case, calls.clone()));
    let aliases = Arc::new(Mutex::new(BTreeMap::new()));
    let mut retained_writes = Vec::new();
    let mut watches: BTreeMap<String, WatchTrace> = BTreeMap::new();
    let mut states: BTreeMap<String, (DocumentState, StateSubscription, Vec<Value>)> =
        BTreeMap::new();
    let expected = &case["expected"][backend];
    let operations = case["input"]["operations"].as_array().unwrap();
    for (index, raw) in operations.iter().enumerate() {
        let op = if raw["op"] == "transaction" {
            raw.clone()
        } else {
            resolve(raw, &aliases.lock().unwrap())
        };
        let expected_value = &expected["observations"][index]["value"];
        let r: Result<Value> = match op["op"].as_str().unwrap() {
            "commit" => parse::<Vec<StorageWrite>>(&op["writes"])
                .and_then(|w| probe.inner.commit(&w))
                .map(value),
            "mint" => (0..op["count"].as_u64().unwrap_or(1))
                .map(|_| probe.inner.mint_id())
                .collect::<Result<Vec<_>>>()
                .map(value),
            "read" => read(
                probe.inner.as_ref(),
                op["method"].as_str().unwrap(),
                op["args"].as_array().unwrap(),
            ),
            "transaction" => {
                let steps = raw["steps"].as_array().unwrap().clone();
                let out = Arc::new(Mutex::new(Vec::new()));
                let values = out.clone();
                let defs = defs.clone();
                let aliases = aliases.clone();
                let result = session
                    .as_ref()
                    .unwrap()
                    .commit(move |tx| {
                        for step in steps {
                            let r = tx_step(tx, &step, &defs, &mut aliases.lock().unwrap())?;
                            values.lock().unwrap().push(r);
                        }
                        Ok(())
                    })
                    .await;
                let mut result_value = json!({"steps":out.lock().unwrap().clone()});
                if let Err(error) = result {
                    result_value["error"] = json!({"message":error.message});
                }
                Ok(result_value)
            }
            "snapshot" | "snapshotAsOf" => {
                let (d, a, _) = doc_args(&op, &defs);
                let s = session.as_ref().unwrap();
                if op["op"] == "snapshot" {
                    s.snapshot(d, a).await.map(option)
                } else {
                    s.snapshot_as_of(d, a, parse(&op["at"]).unwrap())
                        .await
                        .map(option)
                }
            }
            "watch" => {
                let (d, a, _) = doc_args(&op, &defs);
                session.as_ref().unwrap().watch_doc(d, a).await.map(|w| {
                    if let Some(watch) = w {
                        let v = watch.value().as_ref().clone();
                        watches.insert(
                            op["name"].as_str().unwrap().into(),
                            WatchTrace {
                                watch,
                                frames: Arc::new(Mutex::new(Vec::new())),
                                notify: Arc::new(tokio::sync::Notify::new()),
                                gate: Arc::new(tokio::sync::Notify::new()),
                            },
                        );
                        v
                    } else {
                        undefined()
                    }
                })
            }
            "watchStart" => {
                let w = &watches[op["name"].as_str().unwrap()];
                let frames = w.frames.clone();
                let notify = w.notify.clone();
                let gate = w.gate.clone();
                let hold = op["holdFirst"].as_bool().unwrap_or(false);
                w.watch
                    .start(move |f| {
                        let frames = frames.clone();
                        let notify = notify.clone();
                        let gate = gate.clone();
                        async move {
                            let first = {
                                let mut frames = frames.lock().unwrap();
                                frames.push(json!({"value":f.value.as_ref(),"ops":f.ops}));
                                frames.len() == 1
                            };
                            notify.notify_one();
                            if hold && first {
                                gate.notified().await;
                            }
                            Ok(())
                        }
                    })
                    .unwrap();
                Ok(watch_frames(w, expected_value.as_array().unwrap().len()).await)
            }
            "watchFrames" => {
                let w = &watches[op["name"].as_str().unwrap()];
                Ok(watch_frames(w, expected_value.as_array().unwrap().len()).await)
            }
            "watchRelease" => {
                let w = &watches[op["name"].as_str().unwrap()];
                w.gate.notify_one();
                Ok(watch_frames(w, expected_value.as_array().unwrap().len()).await)
            }
            "watchClosed" => Ok(watch_end(
                watches[op["name"].as_str().unwrap()].watch.closed().await,
            )),
            "watchStop" => Ok(watch_end(
                watches[op["name"].as_str().unwrap()].watch.stop(),
            )),
            "watchValue" => Ok(watches[op["name"].as_str().unwrap()]
                .watch
                .value()
                .as_ref()
                .clone()),
            "state" => {
                let (d, a, _) = doc_args(&op, &defs);
                session.as_ref().unwrap().document_state(d,a).await.map(|s|if let Some(state)=s{let sub=state.subscribe();let v=state.value().as_ref().clone();let baseline=json!({"value":sub.baseline.value.as_ref(),"sequence":sub.baseline.sequence});states.insert(op["name"].as_str().unwrap().into(),(state,sub,vec![baseline]));v}else{undefined()})
            }
            "stateFrames" => {
                let (_, sub, frames) = states.get_mut(op["name"].as_str().unwrap()).unwrap();
                while let Some(f) = sub.try_next() {
                    frames.push(json!({"value":f.value.as_ref(),"sequence":f.sequence}));
                }
                Ok(value(frames))
            }
            "stateValue" => Ok(states[op["name"].as_str().unwrap()]
                .0
                .value()
                .as_ref()
                .clone()),
            "repeatTransactions" => {
                let (d, a, seed) = doc_args(&op, &defs);
                for n in 0..op["count"].as_u64().unwrap() {
                    let (d, a, seed) = (d.clone(), a.clone(), seed.clone());
                    let field = op["field"].as_str().unwrap().to_owned();
                    let count = op["from"].as_u64().unwrap() + n;
                    session
                        .as_ref()
                        .unwrap()
                        .commit(move |tx| {
                            let id = tx.doc(&d, a, seed)?;
                            tx.mutate(id, Op::Set(vec![Segment::Key(field)], json!(count)))
                        })
                        .await
                        .unwrap();
                }
                Ok(op["count"].clone())
            }
            "retained" => Ok(aliases.lock().unwrap()[op["name"].as_str().unwrap()].clone()),
            "close" => {
                if let Some(s) = &session {
                    s.close().await.unwrap();
                } else {
                    probe.inner.close().unwrap();
                }
                Ok(json!("closed"))
            }
            "reopen" => {
                if let Some(s) = &session {
                    s.close().await.unwrap();
                } else {
                    probe.inner.close().unwrap();
                }
                if let Some(sub) = &mut subscription {
                    while let Some(p) = sub.try_next() {
                        publications.push(p);
                    }
                }
                retained_writes.extend(probe.writes.lock().unwrap().clone());
                probe = Probe::new(make());
                if is_session {
                    session = Some(Session::with_clock(probe.clone(), || 1234567890000.0).unwrap());
                    subscription =
                        Some(session.as_ref().unwrap().subscribe_commits().await.unwrap());
                }
                Ok(json!("reopened"))
            }
            unknown => panic!("unimplemented fixture operation {unknown}"),
        };
        let r = result(r);
        if let Some(name) = op["as"].as_str() {
            aliases.lock().unwrap().insert(name.into(), r.clone());
        }
        // Watch op spelling may differ for native scoped edits; exact frame value/order remains normative.
        let mut actual = normalize(&r);
        let mut want = normalize(expected_value);
        if matches!(
            op["op"].as_str(),
            Some("watchFrames" | "watchRelease" | "watchStart")
        ) {
            for v in [&mut actual, &mut want] {
                for frame in v.as_array_mut().unwrap() {
                    let ops: Vec<Op> = parse(&frame["ops"]).unwrap();
                    assert!(!ops.is_empty(), "real watch frame must have operations");
                    frame.as_object_mut().unwrap().remove("ops");
                }
            }
        }
        assert_eq!(
            actual, want,
            "case {} backend {backend} operation {index} {}",
            case["id"], op["op"]
        );
    }
    if let Some(sub) = &mut subscription {
        while let Some(p) = sub.try_next() {
            publications.push(p);
        }
    }
    if is_session {
        verify_publications(&publications, &expected["publications"]);
        retained_writes.extend(probe.writes.lock().unwrap().clone());
        let expected_writes = expected["writes"].as_array().unwrap();
        assert_eq!(
            retained_writes.len(),
            expected_writes.len(),
            "admitted batch count {} ",
            case["id"]
        );
        verify_writes(
            &retained_writes,
            &expected["writes"],
            &expected["publications"],
        );
        // Checkpoint input values/count/timing are independent expectations; op batches replay above.
        let actual = calls
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .map(|mut v| {
                v.as_object_mut().unwrap().remove("ops");
                v
            })
            .collect::<Vec<_>>();
        let want = expected["checkpointCalls"]
            .as_array()
            .unwrap()
            .iter()
            .cloned()
            .map(|mut v| {
                v.as_object_mut().unwrap().remove("ops");
                v
            })
            .collect::<Vec<_>>();
        assert_eq!(
            normalize(&value(actual)),
            normalize(&value(want)),
            "checkpoint calls {} ",
            case["id"]
        );
    }
    for w in watches.values() {
        w.gate.notify_one();
        w.watch.stop();
    }
    if let Some(s) = session {
        s.close().await.unwrap();
    } else {
        probe.inner.close().unwrap();
    }
}
#[test]
fn independent_release_cases() {
    let path = std::env::var_os("DURABLE_SESSION_FIXTURES")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../conformance/durable-v1.4.0/fixtures/session.json")
        });
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(path).expect("independent release fixtures must be installed"),
    )
    .unwrap();
    assert_eq!(
        fixture["upstreamCommit"],
        "4a080252f06caf63a525d7873c5d94d3c23887e0"
    );
    runtime().block_on(async {
        let mut count = 0;
        for case in fixture["cases"].as_array().unwrap() {
            if case.get("nativeComparison").is_some() {
                continue;
            }
            for backend in ["chord", "memory", "sqlite"] {
                if case["expected"].get(backend).is_some() {
                    run(case, backend).await;
                    count += 1;
                }
            }
        }
        assert_eq!(count, 57, "frozen common-domain fixture execution count");
    });
}

#[test]
fn independently_authored_native_contracts() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/durable-v1.4.0/fixtures/native-contract.json");
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    runtime().block_on(async {
        for case in fixture["cases"].as_array().unwrap() {
            match case["id"].as_str().unwrap() {
                "native-allocation-only-reopen-no-reuse" => {
                    let temp = Temp::new();
                    let storage = SqliteStorage::open(&temp.path).unwrap();
                    let before = (0..2)
                        .map(|_| storage.mint_id().unwrap())
                        .collect::<Vec<_>>();
                    storage.close().unwrap();
                    let storage = SqliteStorage::open(&temp.path).unwrap();
                    let after = (0..2)
                        .map(|_| storage.mint_id().unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(value(before), case["expected"]["mintedBefore"]);
                    assert_eq!(value(after), case["expected"]["mintedAfter"]);
                    storage.close().unwrap();
                }
                "native-surrogate-splitting-trim-rejects" => {
                    for (_, storage, _) in backends() {
                        let probe = Probe::new(storage);
                        let session = Session::open(probe.clone()).unwrap();
                        let initial = case["input"]["value"].clone();
                        let original = initial.clone();
                        let definition = DocDefinition::new(
                            "unicode",
                            1,
                            DocumentSemantics::Session,
                            false,
                            move |_| Ok(initial.clone()),
                            |_, _, _| Ok(false),
                        )
                        .unwrap();
                        let address = definition.address(Scope::Session, None).unwrap();
                        let (d, a) = (definition.clone(), address.clone());
                        session
                            .commit(move |tx| tx.doc(&d, a, Value::Null))
                            .await
                            .unwrap();
                        probe.writes.lock().unwrap().clear();
                        let ops: Vec<Op> = parse(&case["input"]["ops"]).unwrap();
                        let (d, a) = (definition.clone(), address.clone());
                        let rejected = session
                            .commit(move |tx| {
                                let id = tx.doc(&d, a, Value::Null)?;
                                tx.apply_all(id, &ops)
                            })
                            .await
                            .is_err();
                        assert_eq!(json!(rejected), case["expected"]["rejected"]);
                        assert_eq!(
                            session
                                .snapshot(definition, address)
                                .await
                                .unwrap()
                                .unwrap(),
                            case["expected"]["valueAfter"]
                        );
                        assert_eq!(original, case["expected"]["valueAfter"]);
                        assert_eq!(
                            json!(probe.writes.lock().unwrap().len()),
                            case["expected"]["commits"]
                        );
                        assert!(session.failure().is_none());
                        session.close().await.unwrap();
                    }
                }
                "native-session-invalid-delta-rolls-back" => {
                    for (_, storage, _) in backends() {
                        let probe = Probe::new(storage);
                        let session = Session::open(probe.clone()).unwrap();
                        let initial = case["input"]["initialDocument"]["value"].clone();
                        let definition = DocDefinition::new(
                            "invalid",
                            1,
                            DocumentSemantics::Session,
                            false,
                            move |_| Ok(initial.clone()),
                            |_, _, _| Ok(false),
                        )
                        .unwrap();
                        let address = definition.address(Scope::Session, None).unwrap();
                        let (d, a) = (definition.clone(), address.clone());
                        session
                            .commit(move |tx| tx.doc(&d, a, Value::Null))
                            .await
                            .unwrap();
                        probe.writes.lock().unwrap().clear();
                        let ops: Vec<Op> =
                            parse(&case["input"]["transaction"]["documentOps"]).unwrap();
                        let created = Arc::new(Mutex::new(None));
                        let shared = created.clone();
                        let (d, a) = (definition.clone(), address.clone());
                        let rejected = session
                            .commit(move |tx| {
                                *shared.lock().unwrap() = Some(
                                    tx.create_conversation(ConversationOwnership::Ownerless)?.id,
                                );
                                let id = tx.doc(&d, a, Value::Null)?;
                                tx.apply_all(id, &ops)
                            })
                            .await
                            .is_err();
                        assert_eq!(json!(rejected), case["expected"]["rejected"]);
                        let created_id = created.lock().unwrap().unwrap();
                        assert_eq!(
                            option(session.conversation(created_id).await.unwrap()),
                            case["expected"]["createdConversation"]
                        );
                        assert_eq!(
                            session
                                .snapshot(definition, address)
                                .await
                                .unwrap()
                                .unwrap(),
                            case["expected"]["documentAfter"]
                        );
                        assert_eq!(
                            json!(probe.writes.lock().unwrap().len()),
                            case["expected"]["backendCommitAttempts"]
                        );
                        session.close().await.unwrap();
                    }
                }
                unknown => panic!("unhandled independent native contract {unknown}"),
            }
        }
    });
}
