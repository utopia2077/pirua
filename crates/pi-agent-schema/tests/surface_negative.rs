use pi_agent_schema::*;

fn usage() -> serde_json::Value {
    serde_json::json!({
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 0,
        "cost": {"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}
    })
}

fn user_message() -> serde_json::Value {
    serde_json::json!({"role":"user","content":"hello","timestamp":0})
}

fn assistant_message(reason: &str) -> serde_json::Value {
    let mut value = serde_json::json!({
        "role":"assistant",
        "content":[],
        "api":"future-api",
        "provider":"future-provider",
        "model":"future-model",
        "usage":usage(),
        "stopReason":reason,
        "timestamp":0
    });
    if matches!(reason, "error" | "aborted") {
        value["errorMessage"] = serde_json::Value::String("bounded failure".to_owned());
    }
    value
}

fn branch_summary_entry() -> serde_json::Value {
    serde_json::json!({
        "id":"summary-entry","parentId":"target","seq":1,"timestamp":1,
        "type":"branch_summary","fromId":"old","summary":"summary","fromHook":false
    })
}

fn lane_snapshot() -> serde_json::Value {
    serde_json::json!({
        "lane":"main","transcript":[],"leafId":null,"operation":null,
        "queues":{"steer":[],"followUp":[],"nextRun":[]},
        "pendingWrites":[],"faulted":false
    })
}

#[test]
fn scans_metadata_and_forks_enforce_wire_boundaries() {
    let scan: BranchScan = serde_json::from_value(serde_json::json!({
        "order":"newestFirst","limit":9007199254740991u64,"cursor":{"seq":0}
    }))
    .expect("safe scan");
    assert_eq!(
        scan.limit.map(pi_ai_core::SafeUnsigned::get),
        Some(9_007_199_254_740_991)
    );
    assert!(
        serde_json::from_value::<BranchScan>(serde_json::json!({"limit":9007199254740992u64}))
            .is_err()
    );
    assert!(serde_json::from_value::<BranchScan>(serde_json::json!({"limit":null})).is_err());
    assert!(serde_json::from_value::<BranchScan>(serde_json::json!({"order":"sideways"})).is_err());

    let metadata =
        serde_json::json!({"id":"s","createdAt":0,"storageVersion":1,"storeGeneration":0});
    assert!(serde_json::from_value::<SessionMetadata>(metadata.clone()).is_ok());
    let mut missing_generation = metadata.clone();
    missing_generation
        .as_object_mut()
        .expect("metadata object")
        .remove("storeGeneration");
    assert!(serde_json::from_value::<SessionMetadata>(missing_generation).is_err());
    let mut null_cwd = metadata;
    null_cwd["cwd"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<SessionMetadata>(null_cwd).is_err());

    assert!(serde_json::from_value::<ForkOptions>(serde_json::json!({})).is_ok());
    assert!(serde_json::from_value::<ForkOptions>(serde_json::json!({"entryId":"e"})).is_ok());
    assert!(serde_json::from_value::<ForkOptions>(serde_json::json!({"scope":"branch"})).is_ok());
    assert!(serde_json::from_value::<ForkOptions>(serde_json::json!({"scope":"tree"})).is_ok());
    assert!(serde_json::from_value::<ForkOptions>(serde_json::json!({"scope":null})).is_err());
    assert!(serde_json::from_value::<ForkOptions>(serde_json::json!({"scope":"future"})).is_err());
    assert!(
        serde_json::from_value::<ForkOptions>(serde_json::json!({"scope":"tree","entryId":"e"}))
            .is_err()
    );
}

#[test]
fn rust_errors_are_bounded_and_debug_redacted() {
    assert!(ErrorContext::new("").is_err());
    assert!(ErrorContext::new("x".repeat(513)).is_err());
    let marker = "private-content-marker";
    let context = ErrorContext::new(marker).expect("bounded context");
    assert!(!format!("{context:?}").contains(marker));
    let error = StorageError::Io(context.clone());
    assert_eq!(error.category(), "io");
    assert!(!format!("{error:?}").contains(marker));
    let session = SessionError::Schema(context);
    assert_eq!(session.category(), "schema");
    assert!(!format!("{session:?}").contains(marker));
}

#[test]
fn settled_assistant_boundaries_reject_pending_and_unexplained_failures() {
    assert!(serde_json::from_value::<SettledAssistantMessage>(assistant_message("stop")).is_ok());
    assert!(
        serde_json::from_value::<SettledAssistantMessage>(assistant_message("pending")).is_err()
    );
    let mut error = assistant_message("error");
    error
        .as_object_mut()
        .expect("assistant object")
        .remove("errorMessage");
    assert!(serde_json::from_value::<SettledAssistantMessage>(error).is_err());
}

#[test]
fn tool_dtos_preserve_required_details_and_replay_presence() {
    let base = serde_json::json!({
        "name":"tool","label":"Tool","description":"desc","parameters":{}
    });
    let omitted: AgentTool = serde_json::from_value(base.clone()).expect("tool without replay");
    assert!(omitted.replay.is_none());
    let mut explicit = base;
    explicit["replay"] = serde_json::Value::String("never".to_owned());
    let explicit: AgentTool = serde_json::from_value(explicit).expect("explicit never");
    assert_eq!(explicit.replay, Some(ToolReplay::Never));

    assert!(serde_json::from_value::<AgentToolResult>(serde_json::json!({"content":[]})).is_err());
    assert!(
        serde_json::from_value::<AgentToolResult>(serde_json::json!({"content":[],"details":null}))
            .is_ok()
    );
}

#[test]
fn stream_option_header_patch_decode_is_default_deny_and_policy_aware() {
    let custom = serde_json::json!({"headers":{"X-Public":"value","X-Delete":null}});
    assert!(serde_json::from_value::<AgentHarnessStreamOptionsPatch>(custom.clone()).is_err());
    let raw: RawAgentHarnessStreamOptionsPatch = serde_json::from_value(custom).expect("raw patch");
    let policy =
        ProviderHeaderPolicy::from_allowlist(["x-public".to_owned(), "x-delete".to_owned()]);
    let classified = raw.into_durable(&policy).expect("public patch");
    let encoded = serde_json::to_value(classified).expect("classified patch serializes");
    assert_eq!(encoded["headers"]["X-Public"], "value");
    assert!(encoded["headers"]["X-Delete"].is_null());

    let hard: RawAgentHarnessStreamOptionsPatch = serde_json::from_value(serde_json::json!({
        "headers":{"Authorization":"secret-sentinel"}
    }))
    .expect("raw hard-denied patch");
    let allow = ProviderHeaderPolicy::from_allowlist(["authorization".to_owned()]);
    assert!(matches!(
        hard.into_durable(&allow),
        Err(ProviderHeaderClassificationError::HardDenied(_))
    ));
    assert!(
        serde_json::from_value::<RawAgentHarnessStreamOptionsPatch>(serde_json::json!({
            "headers":{"X":"one","x":"two"}
        }))
        .is_err()
    );
}

#[test]
fn response_headers_are_string_only_and_keys_only_debug() {
    let headers: ResponseHeaders = serde_json::from_value(serde_json::json!({
        "X-Public":"secret-looking-marker"
    }))
    .expect("string headers");
    assert!(!format!("{headers:?}").contains("secret-looking-marker"));
    assert!(serde_json::from_value::<ResponseHeaders>(serde_json::json!({"X":null})).is_err());
}

#[test]
fn outcomes_encode_never_constraints_and_flat_resume_shape() {
    assert!(
        serde_json::from_value::<RunOutcome>(serde_json::json!({
            "kind":"completed","leafId":"leaf"
        }))
        .is_ok()
    );
    assert!(
        serde_json::from_value::<RunOutcome>(serde_json::json!({
            "kind":"completed","leafId":"leaf","finalEntryId":"entry"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<RunOutcome>(serde_json::json!({
            "kind":"failed","leafId":"leaf"
        }))
        .is_err()
    );

    let completed = serde_json::json!({"kind":"completed","oldLeafId":null,"newLeafId":null});
    assert!(serde_json::from_value::<NavigationOutcome>(completed.clone()).is_ok());
    let mut summarized = completed;
    summarized["summaryEntry"] = branch_summary_entry();
    assert!(serde_json::from_value::<NavigationOutcome>(summarized).is_ok());
    assert!(
        serde_json::from_value::<NavigationOutcome>(serde_json::json!({
            "kind":"declined","leafId":null,"summaryEntry":branch_summary_entry()
        }))
        .is_err()
    );

    let resume = serde_json::json!({
        "operation":"run","runId":"op","kind":"completed","leafId":"leaf"
    });
    let decoded: ResumeOutcome = serde_json::from_value(resume.clone()).expect("flat resume");
    assert_eq!(
        serde_json::to_value(decoded).expect("resume encodes"),
        resume
    );
    let mut unknown = resume;
    unknown["extra"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<ResumeOutcome>(unknown).is_err());
    assert!(
        serde_json::from_value::<RunSuccess>(serde_json::json!({
            "runId":"op","kind":"completed","leafId":"leaf","extra":true
        }))
        .is_err()
    );
}

#[test]
fn expected_error_unions_are_method_specific() {
    let no_active = serde_json::json!({"_tag":"NoActiveRun","message":"idle","lane":"main"});
    assert!(serde_json::from_value::<QueueExpectedError>(no_active.clone()).is_ok());
    assert!(serde_json::from_value::<RunExpectedError>(no_active).is_err());
    let unknown_skill =
        serde_json::json!({"_tag":"UnknownSkill","message":"missing","name":"skill"});
    assert!(serde_json::from_value::<RunExpectedError>(unknown_skill.clone()).is_ok());
    assert!(serde_json::from_value::<CompactionExpectedError>(unknown_skill).is_err());
    assert!(
        serde_json::from_value::<RunExpectedError>(serde_json::json!({
            "_tag":"Closed","message":"closed","extra":true
        }))
        .is_err()
    );

    let ok = serde_json::json!({"ok":true,"value":{"entryId":"e"}});
    assert!(serde_json::from_value::<WireResult<QueueAdmission, QueueExpectedError>>(ok).is_ok());
    assert!(
        serde_json::from_value::<WireResult<QueueAdmission, QueueExpectedError>>(
            serde_json::json!({
                "ok":true,"value":{"entryId":"e"},"error":{"_tag":"Closed","message":"closed"}
            })
        )
        .is_err()
    );
}

#[test]
fn snapshots_enforce_required_null_and_pending_write_exclusivity() {
    let snapshot = lane_snapshot();
    assert!(serde_json::from_value::<LaneSnapshot>(snapshot.clone()).is_ok());
    let mut omitted_leaf = snapshot.clone();
    omitted_leaf
        .as_object_mut()
        .expect("snapshot object")
        .remove("leafId");
    assert!(serde_json::from_value::<LaneSnapshot>(omitted_leaf).is_err());
    let mut omitted_operation = snapshot;
    omitted_operation
        .as_object_mut()
        .expect("snapshot object")
        .remove("operation");
    assert!(serde_json::from_value::<LaneSnapshot>(omitted_operation).is_err());

    let message = serde_json::json!({"entryId":"e","type":"message","message":user_message()});
    assert!(serde_json::from_value::<PendingWrite>(message.clone()).is_ok());
    let mut message_with_data = message;
    message_with_data["data"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<PendingWrite>(message_with_data).is_err());
    assert!(
        serde_json::from_value::<PendingWrite>(serde_json::json!({
            "entryId":"e","type":"custom","customType":"state","message":user_message()
        }))
        .is_err()
    );
}

#[test]
fn event_union_enforces_scope_recovery_and_never_rules() {
    let start = serde_json::json!({"type":"run_start","lane":"main","runId":"op"});
    let event: HarnessEvent = serde_json::from_value(start.clone()).expect("run start");
    assert_eq!(serde_json::to_value(event).expect("event encodes"), start);
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"run_start","runId":"op"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"run_start","lane":"main","runId":"op","recovery":false
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"run_start","lane":"main","runId":"op","recovery":null
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"fact_update","fact":"name","lane":"main"
        }))
        .is_err()
    );

    let usage_event = serde_json::json!({
        "type":"usage","lane":"main",
        "row":{"id":"u","seq":1,"usage":usage(),"adjustment":false},
        "totals":usage(),"recovery":true
    });
    assert!(serde_json::from_value::<HarnessEvent>(usage_event).is_err());
    assert!(serde_json::from_value::<HarnessEvent>(serde_json::json!({"type":"future"})).is_err());

    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"run_end","lane":"main","runId":"op","leafId":null,
            "outcome":"completed","finalEntryId":"entry"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"message_end","lane":"main","runId":"op",
            "message":assistant_message("pending")
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"navigation_end","lane":"main","runId":"op","oldLeafId":null,
            "newLeafId":null,"outcome":"declined","summaryEntry":branch_summary_entry()
        }))
        .is_err()
    );
}

#[test]
fn hook_name_event_result_pairing_is_closed() {
    let before_tool = serde_json::json!({
        "name":"before_tool",
        "event":{"lane":"main","runId":"op","toolCallId":"call","toolName":"tool","args":{}}
    });
    assert!(serde_json::from_value::<HookEvent>(before_tool.clone()).is_ok());
    let mut mismatched = before_tool;
    mismatched["name"] = serde_json::Value::String("before_navigation".to_owned());
    assert!(serde_json::from_value::<HookEvent>(mismatched).is_err());

    assert!(
        serde_json::from_value::<HookResult>(serde_json::json!({
            "name":"before_compaction","result":{"decline":true}
        }))
        .is_ok()
    );
    assert!(
        serde_json::from_value::<HookResult>(serde_json::json!({
            "name":"before_compaction","result":{
                "decline":true,
                "compaction":{"summary":"s","tokensBefore":0,"retainedTail":[]}
            }
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HookResult>(serde_json::json!({"name":"before_resume"})).is_ok()
    );
    assert!(
        serde_json::from_value::<HookResult>(serde_json::json!({
            "name":"before_resume","result":{}
        }))
        .is_err()
    );
}

#[test]
fn deterministic_maps_and_open_ids_remain_distinct() {
    let raw = RawProviderHeaderPatch::default();
    assert_eq!(raw, RawProviderHeaderPatch::default());
    assert!(SessionId::new("future-session").is_ok());
    assert!(SessionId::new("").is_err());
}
