use pi_coding_runtime::*;
use pi_coding_session::{SessionHeader, SessionSnapshot};
use serde_json::{Value, json};
fn estimate_json(e: ContextUsageEstimate) -> Value {
    json!({"tokens":e.tokens,"usageTokens":e.usage_tokens,"trailingTokens":e.trailing_tokens,"lastUsageIndex":e.last_usage_index})
}
#[test]
fn pinned_release_context_compaction_conversion_and_cache_cases() {
    let cases: Value = serde_json::from_str(include_str!(
        "../../../conformance/coding-session-runtime-v1.4.0/cases.json"
    ))
    .unwrap();
    let trace: Value = serde_json::from_str(include_str!(
        "../../../conformance/coding-session-runtime-v1.4.0/trace.json"
    ))
    .unwrap();
    for case in cases.as_array().unwrap() {
        if case["kind"] == "bash-sanitize" {
            continue;
        } // Async shell fixtures run in bash_execution.rs.
        let id = case["id"].as_str().unwrap();
        let expected = &trace["observations"][id];
        let actual = match case["kind"].as_str().unwrap() {
            "branch-selection" => {
                let entries: Vec<pi_coding_session::SessionEntry> =
                    serde_json::from_value(case["entries"].clone()).unwrap();
                let runtime = FixtureModels {
                    virtual_registered: case["virtualRegistered"].as_bool().unwrap(),
                };
                branch_model_selection(&entries, &runtime)
                    .map(|(provider, id)| json!({"provider":provider,"modelId":id}))
                    .unwrap_or(Value::Null)
            }
            "virtual-state" => json!(branch_virtual_model_state(
                &serde_json::from_value::<Vec<_>>(case["entries"].clone()).unwrap(),
                case["provider"].as_str().unwrap(),
                case["modelId"].as_str().unwrap()
            )),
            "tokens" => json!(estimate_tokens(&case["message"])),
            "convert" => {
                let messages = serde_json::from_value(case["messages"].clone()).unwrap();
                serde_json::to_value(convert_to_llm(messages).unwrap()).unwrap()
            }
            "estimate" => estimate_json(estimate_context_tokens(
                case["messages"].as_array().unwrap(),
            )),
            "prepare" => {
                let entries = serde_json::from_value::<Vec<pi_coding_session::SessionEntry>>(
                    case["entries"].clone(),
                )
                .unwrap();
                let snapshot = SessionSnapshot {
                    header: SessionHeader {
                        id: "fixture".into(),
                        cwd: "/tmp".into(),
                        timestamp: 0,
                        parent_session: None,
                    },
                    revision: 0,
                    leaf_id: entries.last().map(|e| e.id.clone()),
                    entries: entries.clone(),
                };
                let projection = pi_coding_session::project(&snapshot).unwrap();
                let settings = serde_json::from_value(case["settings"].clone()).unwrap();
                let preparation=prepare_compaction(&projection,&entries,&settings).unwrap().map(|p|json!({"firstKeptEntryId":p.first_kept_entry_id,"messagesToSummarize":p.messages_to_summarize,"turnPrefixMessages":p.turn_prefix_messages,"isSplitTurn":p.is_split_turn,"tokensBefore":p.tokens_before,"previousSummary":p.previous_summary,"fileOps":{"read":p.file_ops.read,"written":p.file_ops.written,"edited":p.file_ops.edited},"settings":p.settings}));
                let mut value = json!({"estimate":estimate_json(estimate_projected_context_tokens(&projection,&entries)),"preparation":preparation});
                if value["preparation"]["previousSummary"].is_null() {
                    value["preparation"]
                        .as_object_mut()
                        .map(|o| o.remove("previousSummary"));
                }
                value
            }
            "branch-prepare" => {
                let entries: Vec<pi_coding_session::SessionEntry> =
                    serde_json::from_value(case["entries"].clone()).unwrap();
                let p = prepare_branch_entries(&entries, case["tokenBudget"].as_u64().unwrap())
                    .unwrap();
                json!({"messages":p.messages,"totalTokens":p.total_tokens,"fileOps":{"read":p.file_ops.read,"written":p.file_ops.written,"edited":p.file_ops.edited}})
            }
            "cache-delay" => json!(cache_warming_delay_ms(case["ttl"].as_u64().unwrap())),
            "cache-ttl" | "replayable" => {
                let mut model = serde_json::to_value(session_model()).unwrap();
                for (k, v) in case["model"].as_object().unwrap() {
                    model[k] = v.clone();
                }
                let model = serde_json::from_value(model).unwrap();
                let options = serde_json::from_value(case["options"].clone()).unwrap();
                if case["kind"] == "cache-ttl" {
                    json!(prompt_cache_ttl_ms(&model, &options))
                } else {
                    json!(is_cache_replayable(&model, &options))
                }
            }
            "serialize" => {
                let messages = case["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|m| complete_message(m.clone()))
                    .collect::<Vec<_>>();
                json!(serialize_conversation(&messages))
            }
            "failure" => {
                let message = complete_message(case["message"].clone());
                if let pi_ai::Message::Assistant(m) = message {
                    json!(summarization_failure(&m, case["label"].as_str().unwrap()))
                } else {
                    panic!("expected assistant")
                }
            }
            kind => panic!("unhandled fixture {kind}"),
        };
        assert_eq!(
            normalize(actual),
            normalize(expected.clone()),
            "source fixture {id}"
        );
    }
}
fn session_model() -> pi_ai::Model {
    serde_json::from_value(json!({"id":"fixture","name":"Fixture","api":"openai-responses","provider":"openai","baseUrl":"https://fixture.invalid/v1","reasoning":true,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":1000})).unwrap()
}
fn complete_message(mut value: Value) -> pi_ai::Message {
    if value["role"] == "assistant" {
        for(k,v)in json!({"api":"openai-responses","provider":"openai","model":"fixture","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1}).as_object().unwrap(){if value.get(k).is_none(){value[k]=v.clone();}}
    }
    if value["role"] == "toolResult" {
        value["toolCallId"] = json!("x");
        value["toolName"] = json!("read");
        value["isError"] = json!(false);
        value["timestamp"] = json!(1);
    }
    if let Some(content) = value["content"].as_array_mut() {
        for block in content {
            if block["type"] == "toolCall" && block.get("id").is_none() {
                block["id"] = json!("x");
            }
        }
    }
    serde_json::from_value(value).unwrap()
}

fn normalize(value: Value) -> Value {
    match value {
        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().unwrap();
            if f.fract() == 0.0 && f >= 0.0 && f <= pi_ai::JS_SAFE_INTEGER_MAX as f64 {
                json!(f as u64)
            } else {
                Value::Number(n)
            }
        }
        Value::Array(a) => Value::Array(a.into_iter().map(normalize).collect()),
        Value::Object(o) => Value::Object(o.into_iter().map(|(k, v)| (k, normalize(v))).collect()),
        v => v,
    }
}

struct FixtureModels {
    virtual_registered: bool,
}
impl ModelRuntime for FixtureModels {
    fn get_model(&self, _: &str, id: &str) -> Option<pi_ai::Model> {
        if self.virtual_registered && id == "router" {
            let mut model = session_model();
            model.id = "router".to_owned().try_into().unwrap();
            model.api = "pi-virtual".to_owned().try_into().unwrap();
            Some(model)
        } else {
            None
        }
    }
    fn models(&self) -> Vec<pi_ai::Model> {
        Vec::new()
    }
    fn is_virtual(&self, model: &pi_ai::Model) -> bool {
        model.api.as_str() == "pi-virtual"
    }
}
