mod durable_views_support;
use durable_views_support::*;
use pi_durable::context::read_context;
use pi_durable_session::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn resolve(value: &Value, names: &BTreeMap<String, Value>) -> Value {
    match value {
        Value::String(text) if text.starts_with('$') => {
            let (name, field) = text[1..].split_once('.').unwrap();
            names[name][field].clone()
        }
        Value::Array(values) => {
            Value::Array(values.iter().map(|value| resolve(value, names)).collect())
        }
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), resolve(value, names)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

#[test]
fn frozen_runtime_context_cases_through_public_session() {
    runtime().block_on(async {
        let fixture = fixture();
        for case_id in [
            "runtime-context-edits-normalization",
            "runtime-context-baseline-identity-head-edits",
        ] {
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["id"] == case_id)
                .unwrap();
            for (backend, store, _temp) in backends() {
                // The runtime's five initial built-ins consume IDs2..6. This read-only
                // module creates no runtime docs; reserve their namespace explicitly.
                for id in 2..=6 {
                    assert_eq!(store.mint_id().unwrap(), id);
                }
                let session = Session::open(store).unwrap();
                root(&session).await;
                let observations = case["expected"][backend]["observations"]
                    .as_array()
                    .unwrap();
                let mut names = BTreeMap::new();
                let mut compared = 0;
                for (index, operation) in case["input"]["operations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    let op = operation["op"].as_str().unwrap();
                    // Generation/submission execution belongs to the runtime owner.
                    if op == "submit" || op == "close" {
                        break;
                    }
                    let label = operation
                        .get("label")
                        .or_else(|| operation.get("as"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("{index}:{op}"));
                    let expected = &observations
                        .iter()
                        .find(|entry| entry["label"] == label)
                        .unwrap()["value"];
                    if op == "append" {
                        let value = resolve(&operation["value"], &names);
                        let draft = EntryDraft {
                            kind: value["kind"].as_str().unwrap().into(),
                            model: value.get("model").map(|v| v.as_array().unwrap().clone()),
                            data: value.get("data").cloned(),
                            head: value.get("head").map(|v| {
                                EntryHead::Entry(serde_json::from_value(v.clone()).unwrap())
                            }),
                            edits: value
                                .get("edits")
                                .map(|v| serde_json::from_value(v.clone()).unwrap()),
                        };
                        let entry = session
                            .commit(move |tx| tx.append_entry(ROOT_CONVERSATION_ID, draft))
                            .await
                            .unwrap();
                        let actual = serde_json::to_value(entry).unwrap();
                        assert_eq!(&actual, expected, "{case_id}/{backend}/{label}");
                        if let Some(name) = operation["as"].as_str() {
                            names.insert(name.to_owned(), actual);
                        }
                    } else if op == "context" {
                        let at = operation
                            .get("at")
                            .map(|v| serde_json::from_value(resolve(v, &names)).unwrap());
                        let actual = read_context(&session, ROOT_CONVERSATION_ID, at)
                            .await
                            .unwrap();
                        let expected: pi_durable::context::ContextView =
                            serde_json::from_value(expected.clone()).unwrap();
                        assert_eq!(actual, expected, "{case_id}/{backend}/{label}");
                        compared += 1;
                    } else if op == "entries" {
                        assert_eq!(
                            serde_json::to_value(
                                session.history(ROOT_CONVERSATION_ID, None).await.unwrap()
                            )
                            .unwrap(),
                            expected["items"]
                        );
                    } else {
                        panic!("unsupported context-only operation {op}");
                    }
                }
                assert_eq!(
                    compared,
                    if case_id.contains("normalization") {
                        2
                    } else {
                        3
                    }
                );
                session.close().await.unwrap();
            }
        }
    });
}

#[test]
fn empty_cutoff_fork_and_detached_snapshots() {
    runtime().block_on(async {
        for (backend, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            assert!(
                read_context(&session, ROOT_CONVERSATION_ID, None)
                    .await
                    .unwrap()
                    .entries
                    .is_empty()
            );
            assert!(
                read_context(
                    &session,
                    ROOT_CONVERSATION_ID,
                    Some(EntryId::new(999).unwrap())
                )
                .await
                .is_err()
            );
            let first = append(&session, vec![user("first")], "raw", None, None).await;
            let before = read_context(&session, ROOT_CONVERSATION_ID, None)
                .await
                .unwrap();
            append(
                &session,
                vec![],
                "edit",
                None,
                Some(vec![ContextEdit::Replace {
                    target: first.id,
                    messages: vec![user("edited")],
                }]),
            )
            .await;
            assert_eq!(
                serde_json::to_value(&before.messages).unwrap(),
                json!([user("first")]),
                "{backend}"
            );
            let child = session
                .commit(move |tx| {
                    tx.fork_conversation(
                        ROOT_CONVERSATION_ID,
                        first.id,
                        ConversationOwnership::Ownerless,
                    )
                })
                .await
                .unwrap();
            assert_eq!(
                read_context(&session, child.id, None).await.unwrap(),
                before
            );
            let unrelated = session
                .commit(|tx| tx.create_conversation(ConversationOwnership::Ownerless))
                .await
                .unwrap();
            assert!(
                read_context(&session, unrelated.id, Some(first.id))
                    .await
                    .is_err()
            );
            let mut detached = before.clone();
            detached.entries[0].model = Some(vec![user("host mutation")]);
            assert_eq!(
                read_context(&session, ROOT_CONVERSATION_ID, Some(first.id))
                    .await
                    .unwrap(),
                before
            );
            session.close().await.unwrap();
        }
    });
}

#[test]
fn positional_system_rules_keep_original_baseline_provenance() {
    runtime().block_on(async {
        for (_, store, _temp) in backends() {
            let session = Session::open(store).unwrap();
            root(&session).await;
            append(&session, vec![user("input")], "raw", None, None).await;
            let baseline =
                append(&session, vec![system("baseline")], "pi.system", None, None).await;
            assert_eq!(
                serde_json::to_value(
                    read_context(&session, ROOT_CONVERSATION_ID, None)
                        .await
                        .unwrap()
                        .messages
                )
                .unwrap(),
                json!([system("baseline"), user("input")])
            );
            append(&session, vec![assistant("answer")], "raw", None, None).await;
            append(&session, vec![system("later")], "pi.system", None, None).await;
            append(
                &session,
                vec![],
                "edit",
                None,
                Some(vec![ContextEdit::Omit {
                    target: baseline.id,
                }]),
            )
            .await;
            let messages = serde_json::to_value(
                read_context(&session, ROOT_CONVERSATION_ID, None)
                    .await
                    .unwrap()
                    .messages,
            )
            .unwrap();
            assert_eq!(
                messages,
                json!([user("input"), assistant("answer"), system("later")])
            );
            // A reset makes its own range independent of the old baseline.
            append(&session, vec![], "pi.reset", Some(EntryHead::Own), None).await;
            append(&session, vec![assistant("kept")], "raw", None, None).await;
            append(
                &session,
                vec![system("new baseline")],
                "pi.system",
                None,
                None,
            )
            .await;
            assert_eq!(
                serde_json::to_value(
                    read_context(&session, ROOT_CONVERSATION_ID, None)
                        .await
                        .unwrap()
                        .messages
                )
                .unwrap(),
                json!([system("new baseline"), assistant("kept")])
            );
            session.close().await.unwrap();
        }
    });
}
