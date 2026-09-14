use pi_agent_schema::*;
use pi_harness_core::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
mod current_validation_support;
use CurrentValidationError as E;
use current_validation_support::*;

#[test]
fn all_27_labels_and_idle_have_exact_inventory_and_cas_projection() {
    let fixtures = cases();
    let expected: BTreeSet<_> = OPERATION_STATE_V1_INVENTORY
        .iter()
        .copied()
        .chain(["idle"])
        .collect();
    let actual: BTreeSet<_> = fixtures
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(actual, expected);
    for case in &fixtures {
        let loaded = Loaded::new(case);
        let refs = collect_current_references(&loaded.roots).unwrap();
        let ids = |field: &str| -> BTreeSet<String> {
            case[field]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect()
        };
        assert_eq!(
            refs.entries
                .iter()
                .filter(|(_, r)| r.required)
                .map(|(id, _)| id.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
            ids("required"),
            "{}",
            case["name"]
        );
        assert_eq!(
            refs.entries
                .iter()
                .filter(|(_, r)| !r.required)
                .map(|(id, _)| id.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
            ids("reserved"),
            "{}",
            case["name"]
        );
        assert_eq!(
            refs.registers.keys().collect::<Vec<_>>(),
            loaded.registers.keys().collect::<Vec<_>>(),
            "{}",
            case["name"]
        );
        let validated = loaded
            .validate()
            .unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        if case["name"] == "idle" {
            assert!(validated.is_none());
        } else {
            let projected = validated.unwrap();
            assert_eq!(projected.operation_state_seq.get(), 9_007_199_254_740_984);
            assert_eq!(projected.lane_state_seq.get(), 9_007_199_254_740_981);
            assert_eq!(projected.configuration_seq.get(), 9_007_199_254_740_980);
            let Register::OperationState { value, .. } = loaded.roots.operation_state.unwrap()
            else {
                unreachable!()
            };
            assert_eq!(projected.state, *value);
            let Register::OperationMeta { value, .. } = loaded.roots.operation_meta.unwrap() else {
                unreachable!()
            };
            assert_eq!(projected.operation, value);
            assert_eq!(projected.leaf_id.unwrap().as_str(), "leaf");
        }
    }
}

#[test]
fn every_required_role_missing_or_misindexed_rejects_and_reserved_absence_preserves_state() {
    for case in cases() {
        let base = Loaded::new(&case).validate().unwrap();
        for id in case["required"].as_array().unwrap() {
            let id: EntryId = serde_json::from_value(id.clone()).unwrap();
            let mut loaded = Loaded::new(&case);
            let entry = loaded.entries.remove(&id).unwrap();
            assert_eq!(
                loaded.validate().unwrap_err(),
                E::MissingEntry,
                "{} {id:?}",
                case["name"]
            );
            loaded
                .entries
                .insert(EntryId::new("nearby").unwrap(), entry.clone());
            assert_eq!(loaded.validate().unwrap_err(), E::MissingEntry);
            let mut value = serde_json::to_value(entry).unwrap();
            value["id"] = json!("different-contained-id");
            loaded
                .entries
                .insert(id, serde_json::from_value(value).unwrap());
            assert_eq!(loaded.validate().unwrap_err(), E::LoadedIdentity);
        }
        for id in case["reserved"].as_array().unwrap() {
            let id: EntryId = serde_json::from_value(id.clone()).unwrap();
            let mut loaded = Loaded::new(&case);
            loaded.entries.remove(&id);
            assert_eq!(loaded.validate().unwrap(), base, "{}", case["name"]);
            let wrong: Entry = serde_json::from_value(json!({"id":id,"type":"custom","customType":"wrong","parentId":null,"seq":50,"timestamp":0})).unwrap();
            loaded.entries.insert(id.clone(), wrong);
            assert_eq!(loaded.validate().unwrap_err(), E::EntryRole);
            let mut value = entry_mut(&mut case.clone(), id.as_str()).clone();
            value["id"] = json!("wrong");
            loaded
                .entries
                .insert(id, serde_json::from_value(value).unwrap());
            assert_eq!(loaded.validate().unwrap_err(), E::LoadedIdentity);
        }
    }
}

#[test]
fn every_required_register_requires_exact_namespace_and_unparsed_key() {
    for case in cases() {
        let keys = Loaded::new(&case)
            .registers
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let mut loaded = Loaded::new(&case);
            let value = loaded.registers.remove(&key).unwrap();
            assert_eq!(loaded.validate().unwrap_err(), E::MissingRegister);
            loaded
                .registers
                .insert((key.0, format!("{}:old", key.1)), value.clone());
            assert_eq!(loaded.validate().unwrap_err(), E::MissingRegister);
            let mut wire = serde_json::to_value(value).unwrap();
            wire["key"] = json!("other");
            loaded
                .registers
                .insert(key.clone(), serde_json::from_value(wire).unwrap());
            assert_eq!(loaded.validate().unwrap_err(), E::LoadedIdentity);
            loaded.registers.insert(
                key,
                serde_json::from_value(
                    json!({"namespace":"fact.custom","key":"wrong","value":null,"seq":0}),
                )
                .unwrap(),
            );
            assert_eq!(loaded.validate().unwrap_err(), E::LoadedIdentity);
        }
    }
}

#[test]
fn root_presence_kinds_keys_lane_id_and_intent_are_joined() {
    for name in [
        "idle",
        "run.checkpoint",
        "compaction.deciding",
        "navigation.summarized.deciding",
    ] {
        for field in ["configuration", "lane_state", "leaf"] {
            let mut c = case(name);
            c["roots"][field]["key"] = json!("other");
            bad(&c, E::RootIdentity);
            let mut l = Loaded::new(&case(name));
            match field {
                "configuration" => l.roots.configuration = l.roots.leaf.clone(),
                "lane_state" => l.roots.lane_state = l.roots.configuration.clone(),
                _ => l.roots.leaf = l.roots.lane_state.clone(),
            }
            assert_eq!(l.validate().unwrap_err(), E::RootIdentity);
        }
        let mut c = case(name);
        c["roots"]["lane"] = json!("other");
        bad(&c, E::RootIdentity);
    }
    for field in ["operation_meta", "operation_state"] {
        let mut c = case("run.checkpoint");
        c["roots"][field] = Value::Null;
        bad(&c, E::RootIdentity);
        let mut c = case("run.checkpoint");
        c["roots"][field]["key"] = json!("other");
        bad(&c, E::RootIdentity);
        let mut c = case("idle");
        c["roots"][field] = case("run.checkpoint")["roots"][field].clone();
        bad(&c, E::RootIdentity);
    }
    for field in ["operationId", "lane"] {
        let mut c = case("run.checkpoint");
        c["roots"]["operation_meta"]["value"][field] = json!("other");
        bad(&c, E::RootIdentity);
    }
    let mut c = case("run.checkpoint");
    c["roots"]["operation_meta"]["value"]["intent"] = json!({"kind":"compaction"});
    bad(&c, E::RootIdentity);
    let mut c = case("run.checkpoint");
    c["roots"]["lane_state"]["value"]["currentOperationId"] = json!("other");
    bad(&c, E::RootIdentity);
}

#[test]
fn assistant_roles_accumulate_without_restricting_post_hook_metadata_or_historical_aborts() {
    for (name, id) in [
        ("run.assistant.effect_pending", "entry-response"),
        ("run.deferred.effect_pending", "entry-poll"),
        ("run.tools.completed", "entry-response"),
        ("run.failure_drain.response", "entry-response"),
    ] {
        let c = case(name);
        good(&c); // Fixture's provider/model/API all differ from captured values (original601).
        let mut wrong = c.clone();
        entry_mut(&mut wrong, id)["message"] = json!({"role":"user","content":"","timestamp":0});
        bad(&wrong, E::EntryRole);
        let mut aborted = c.clone();
        let e = entry_mut(&mut aborted, id);
        e["message"]["stopReason"] = json!("aborted");
        e["message"]["errorMessage"] = json!("private sentinel");
        bad(&aborted, E::Control);
        aborted["roots"]["operation_state"]["value"]["control"] = json!({"status":"cancel_requested","requestedAt":1,"drainedSteer":[],"drainedFollowUp":[]});
        good(&aborted);
    }
    let mut c = case("run.checkpoint");
    let mut historic =
        entry_mut(&mut case("run.assistant.effect_pending"), "entry-response").clone();
    historic["id"] = json!("prompt");
    historic["message"]["stopReason"] = json!("aborted");
    historic["message"]["errorMessage"] = json!("past");
    *entry_mut(&mut c, "prompt") = historic;
    good(&c);
    c["roots"]["operation_state"]["value"]["latestAssistantEntryId"] = json!("prompt");
    bad(&c, E::Control);
    let mut c = case("run.checkpoint");
    c["roots"]["operation_state"]["value"]["latestAssistantEntryId"] = json!("leaf");
    let refs = collect_current_references(&Loaded::new(&c).roots).unwrap();
    assert_eq!(
        refs.entries[&EntryId::new("leaf").unwrap()]
            .constraints
            .len(),
        2
    );
    bad(&c, E::EntryRole);
}

#[test]
fn deferred_source_requires_deferred_reason_and_handle() {
    for name in ["run.deferred.suspended", "run.deferred.effect_pending"] {
        let mut c = case(name);
        entry_mut(&mut c, "entry-response")["message"]
            .as_object_mut()
            .unwrap()
            .remove("deferred");
        bad(&c, E::EntryRole);
        let mut c = case(name);
        entry_mut(&mut c, "entry-response")["message"]["stopReason"] = json!("stop");
        bad(&c, E::EntryRole);
    }
}

#[test]
fn original_indices_complete_plan_pairing_prefix_and_result_sequences_are_enforced() {
    let name = "run.tools.completed";
    for indices in [
        vec![0_u64, 1, 2],
        vec![1, 1, 5],
        vec![3, 1, 5],
        vec![1, 3, 9007199254740991],
    ] {
        let mut c = case(name);
        for (call, index) in c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .zip(indices)
        {
            call["sourceIndex"] = json!(index);
        }
        // Keep all exact args present so source position is the distinguishing check.
        let index =
            c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"][2]["sourceIndex"]
                .clone();
        c["registers"][1]["key"] = json!(format!("op:step-1:{}", index.as_u64().unwrap()));
        bad(&c, E::ToolBatch);
    }
    let mut c = case(name);
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"]
        .as_array_mut()
        .unwrap()
        .pop();
    bad(&c, E::ToolBatch);
    let mut c = case("run.tools.planned");
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"][1]["resultEntryId"] =
        json!("result-0");
    bad(&c, E::ReservationCollision);
    for field in ["toolCallId", "toolName"] {
        for id in ["result-0", "result-2"] {
            let mut c = case(name);
            entry_mut(&mut c, id)["message"][field] = json!("wrong");
            bad(&c, E::ToolBatch);
        }
    }
    let mut c = case(name);
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"][0] =
        json!({"status":"planned","sourceIndex":1,"resultEntryId":"result-0"});
    bad(&c, E::ToolBatch);
    let mut c = case(name);
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"][2] =
        json!({"status":"completed","sourceIndex":5,"resultEntryId":"result-2","terminate":false});
    bad(&c, E::ToolBatch);
    for (id, seq) in [("result-0", 10), ("result-1", 20), ("result-1", 19)] {
        let mut c = case(name);
        entry_mut(&mut c, id)["seq"] = json!(seq);
        bad(&c, E::ToolBatch);
    }
    let mut c = case(name);
    entry_mut(&mut c, "result-0")["terminate"] = json!(true);
    bad(&c, E::ToolBatch);
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"][0]["terminate"] = json!(true);
    good(&c);
}

#[test]
fn original601_cancelled_interleaved_writes_preserve_source_order_without_parent_adjacency() {
    let mut c = case("run.tools.completed");
    c["roots"]["operation_state"]["value"]["control"] =
        json!({"status":"cancel_requested","requestedAt":1,"drainedSteer":[],"drainedFollowUp":[]});
    entry_mut(&mut c, "result-0")["parentId"] = json!("entry-response");
    entry_mut(&mut c, "result-1")["parentId"] = json!("interleaved-write");
    c["entries"].as_array_mut().unwrap().push(json!({"id":"interleaved-write","parentId":"result-0","seq":25,"timestamp":0,"type":"custom","customType":"fixture"}));
    c["roots"]["leaf"]["value"] = json!("result-1");
    good(&c);
    let refs = collect_current_references(&Loaded::new(&c).roots).unwrap();
    assert!(
        !refs
            .entries
            .contains_key(&EntryId::new("interleaved-write").unwrap())
    );
    entry_mut(&mut c, "result-1")["seq"] = json!(19);
    bad(&c, E::ToolBatch);
}

#[test]
fn original601_empty_tool_use_allows_only_an_exact_zero_call_plan_without_continuation_claim() {
    let mut c = case("run.tools.planned");
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["calls"] = json!([]);
    bad(&c, E::ToolBatch);
    entry_mut(&mut c, "entry-response")["message"]["content"] =
        json!([{"type":"text","text":"no calls"}]);
    good(&c);
    let state = Loaded::new(&c).validate().unwrap().unwrap().state;
    assert!(
        matches!(state,OperationState::Run(r) if matches!(&r.phase,RunPhase::Tools(p) if p.batch.calls.is_empty()))
    );
}

#[test]
fn original601_navigation_scalar_cases_include_both_root_directions() {
    let name = "navigation.unsummarized.ready_to_commit";
    let mut root_to_entry = case(name);
    root_to_entry["roots"]["operation_meta"]["value"]["sourceLeafId"] = Value::Null;
    good(&root_to_entry);
    let mut entry_to_root = case(name);
    entry_to_root["roots"]["operation_meta"]["value"]["intent"]["targetId"] = Value::Null;
    entry_to_root["roots"]["operation_state"]["value"]["targetId"] = Value::Null;
    entry_to_root["roots"]["operation_meta"]["value"]["intent"]
        .as_object_mut()
        .unwrap()
        .remove("label");
    entry_to_root["roots"]["operation_state"]["value"]
        .as_object_mut()
        .unwrap()
        .remove("label");
    good(&entry_to_root);
    let mut c = entry_to_root.clone();
    c["roots"]["operation_meta"]["value"]["sourceLeafId"] = Value::Null;
    bad(&c, E::Navigation);
    let mut c = entry_to_root;
    c["roots"]["operation_state"]["value"]["label"] = json!("");
    c["roots"]["operation_meta"]["value"]["intent"]["label"] = json!("");
    bad(&c, E::Navigation);
    let mut c = case(name);
    c["roots"]["operation_meta"]["value"]["sourceLeafId"] = json!("target");
    bad(&c, E::Navigation);
    let mut c = case("navigation.summarized.deciding");
    c["roots"]["operation_meta"]["value"]["sourceLeafId"] = Value::Null;
    bad(&c, E::Navigation);
    let mut c = case("navigation.summarized.deciding");
    c["roots"]["operation_meta"]["value"]["intent"]["targetId"] = Value::Null;
    bad(&c, E::Navigation);
    for name in [name, "navigation.summarized.deciding"] {
        for (field, value) in [
            ("targetId", json!("source")),
            ("label", json!("different")),
            ("summarize", json!(name.contains("unsummarized"))),
        ] {
            let mut c = case(name);
            c["roots"]["operation_meta"]["value"]["intent"][field] = value;
            bad(&c, E::Navigation);
        }
    }
    let mut c = case("navigation.summarized.deciding");
    c["roots"]["operation_meta"]["value"]["intent"]["customInstructions"] = json!("different");
    bad(&c, E::Navigation);
}

#[test]
fn all_structural_owners_join_preparation_task_kind_reason_and_branch_source() {
    for name in OPERATION_STATE_V1_INVENTORY
        .iter()
        .filter(|n| n.contains("compaction") || n.contains("navigation.summarized"))
    {
        let original = case(name);
        let index = original["registers"]
            .as_array()
            .unwrap()
            .iter()
            .position(|r| r["namespace"] == "op.preparation")
            .unwrap();
        let mut c = original.clone();
        c["registers"][index]["value"] = if name.starts_with("navigation") {
            case("compaction.deciding")["registers"][1]["value"].clone()
        } else {
            case("navigation.summarized.deciding")["registers"][1]["value"].clone()
        };
        bad(&c, E::RegisterRole);
        if !name.contains("generating") {
            continue;
        }
        let path = if name.starts_with("compaction") {
            "/roots/operation_state/value/structural/generation/context"
        } else {
            "/roots/operation_state/value/phase/structural/generation/context"
        };
        for (field, value) in [
            ("taskId", json!("different")),
            (
                "kind",
                json!(if name.starts_with("navigation") {
                    "compaction"
                } else {
                    "branch_summary"
                }),
            ),
            (
                "reason",
                json!(if name.starts_with("run") {
                    "manual"
                } else {
                    "threshold"
                }),
            ),
        ] {
            let mut c = original.clone();
            c.pointer_mut(path).unwrap()[field] = value;
            bad(&c, E::Structural);
        }
        if name.starts_with("navigation") {
            let mut c = original.clone();
            entry_mut(&mut c, "entry-summary")["fromId"] = json!("leaf");
            bad(&c, E::EntryRole);
        }
    }
    let mut c = case("compaction.deciding");
    c["roots"]["operation_meta"]["value"]["intent"]["customInstructions"] = json!("different");
    bad(&c, E::Structural);
    // Failure provenance is not a structural decision and must not demand preparation.
    let l = Loaded::new(&case("run.failure_drain.structural"));
    assert!(
        collect_current_references(&l.roots)
            .unwrap()
            .registers
            .keys()
            .all(|(ns, _)| *ns != RegisterNamespace::OperationPreparation)
    );
}

#[test]
fn attempt_caps_summary_request_scalars_and_reserved_namespace_collisions() {
    for name in OPERATION_STATE_V1_INVENTORY
        .iter()
        .filter(|n| n.contains("assistant.") || n.contains("generating."))
    {
        let mut c = case(name);
        let path = if name.starts_with("run.assistant") {
            "/roots/operation_state/value/phase/generation"
        } else if name.starts_with("compaction") {
            "/roots/operation_state/value/structural/generation"
        } else {
            "/roots/operation_state/value/phase/structural/generation"
        };
        let g = c.pointer_mut(path).unwrap();
        let field = if g["status"] == "effect_pending" {
            "attempt"
        } else {
            "nextAttempt"
        };
        let cap = g["context"]["retryPolicy"]["maxAttempts"].as_u64().unwrap();
        g[field] = json!(cap + 1);
        bad(&c, E::Attempt);
        let g = c.pointer_mut(path).unwrap();
        g[field] = json!(9007199254740991_u64);
        g["context"]["retryPolicy"]["maxAttempts"] = json!(9007199254740991_u64);
        good(&c);
    }
    for name in [
        "run.compaction.generating.effect_pending.request_present",
        "compaction.generating.effect_pending.request_present",
        "navigation.summarized.generating.effect_pending.request_present",
    ] {
        let path = if name.starts_with("compaction") {
            "/roots/operation_state/value/structural/generation"
        } else {
            "/roots/operation_state/value/phase/structural/generation"
        };
        for index in [2_u64, 4294967296, 9007199254740991] {
            let mut c = case(name);
            c.pointer_mut(path).unwrap()["request"]["index"] = json!(index);
            bad(&c, E::Structural);
        }
        let mut c = case(name);
        c.pointer_mut(path).unwrap()["usageIds"] = json!(["a", "b", "c"]);
        bad(&c, E::Structural);
        let mut c = case(name);
        let g = c.pointer_mut(path).unwrap();
        g["usageIds"] = json!(["same", "same"]);
        g.as_object_mut().unwrap().remove("request");
        bad(&c, E::ReservationCollision);
        let mut c = case(name);
        let g = c.pointer_mut(path).unwrap();
        g["usageIds"] = json!(["same"]);
        g["request"] = json!({"index":1,"usageId":"same"});
        bad(&c, E::ReservationCollision);
        let mut c = case(name);
        let g = c.pointer_mut(path).unwrap();
        g["request"]["usageId"] = g["context"]["resultEntryId"].clone();
        bad(&c, E::ReservationCollision);
        let mut c = case(name);
        let g = c.pointer_mut(path).unwrap();
        g["usageIds"] = json!(["a", "b"]);
        g.as_object_mut().unwrap().remove("request");
        good(&c);
    }
    for (name, field) in [
        ("run.assistant.effect_pending", "generation"),
        ("run.deferred.effect_pending", "deferred"),
    ] {
        let mut c = case(name);
        let g = &mut c["roots"]["operation_state"]["value"]["phase"][field];
        g["usageId"] = g["responseEntryId"].clone();
        bad(&c, E::ReservationCollision);
    }
}

#[test]
fn marked_compaction_resume_scalars_are_checked_without_recomputing_settings() {
    let mut c = case("run.compaction.deciding");
    c["roots"]["operation_state"]["value"]["phase"]["resumeAfter"]
        .as_object_mut()
        .unwrap()
        .remove("thresholdCheckedTriggerEntryId");
    bad(&c, E::Structural);
    let mut c = case("run.compaction.deciding");
    c["roots"]["operation_state"]["value"]["phase"]["resumeAfter"]["thresholdCheckedTriggerEntryId"] =
        json!("source");
    bad(&c, E::Structural);
    for continuation in [
        json!({"kind":"may_finish","includeFinalAssistant":false}),
        json!({"kind":"need_assistant","overflowRecoveryUsed":false}),
    ] {
        let mut c = case("run.compaction.generating.ready");
        c["roots"]["operation_state"]["value"]["phase"]["resumeAfter"]["continuation"] =
            continuation;
        bad(&c, E::Structural);
    }
}

#[test]
fn pending_ownership_and_payload_roles_apply_to_idle_inboxes_writes_and_cancelled_drains() {
    let queues = ["steer", "followUp", "writes"];
    for queue in queues {
        let mut c = case("run.checkpoint");
        c["roots"]["operation_state"]["value"]["inbox"][queue] = json!(["next"]);
        bad(&c, E::PendingOwnership);
    }
    let mut c = case("idle");
    c["roots"]["lane_state"]["value"]["pendingNextRun"] = json!(["next", "next"]);
    bad(&c, E::PendingOwnership);
    let mut c = case("idle");
    c["registers"][0]["value"] = json!({"type":"custom","customType":"fixture","payload":null});
    bad(&c, E::RegisterRole);
    for cancelled in [false, true] {
        for queue in if cancelled {
            vec!["drainedSteer", "drainedFollowUp"]
        } else {
            vec!["steer", "followUp", "writes"]
        } {
            let mut c = case("run.checkpoint");
            let state = &mut c["roots"]["operation_state"]["value"];
            if cancelled {
                state["control"] = json!({"status":"cancel_requested","requestedAt":0,"drainedSteer":[],"drainedFollowUp":[]});
                state["control"][queue] = json!(["queued"]);
            } else {
                state["inbox"][queue] = json!(["queued"]);
            }
            c["registers"].as_array_mut().unwrap().push(json!({"namespace":"pending.entry","key":"queued","value":{"type":"message","payload":{"role":"user","content":"","timestamp":0}},"seq":1}));
            good(&c);
            c["registers"][1]["value"] =
                json!({"type":"custom","customType":"fixture","payload":null});
            if queue == "writes" {
                good(&c);
            } else {
                bad(&c, E::RegisterRole);
            }
            let mut l = Loaded::new(&c);
            l.registers
                .remove(&(RegisterNamespace::PendingEntry, "queued".into()));
            assert_eq!(l.validate().unwrap_err(), E::MissingRegister);
        }
    }
    let mut c = case("run.checkpoint");
    let s = &mut c["roots"]["operation_state"]["value"];
    s["control"] = json!({"status":"cancel_requested","requestedAt":0,"drainedSteer":["next"],"drainedFollowUp":[]});
    bad(&c, E::PendingOwnership);
    for queue in ["steer", "followUp"] {
        let mut c = case("run.checkpoint");
        let s = &mut c["roots"]["operation_state"]["value"];
        s["control"] = json!({"status":"cancel_requested","requestedAt":0,"drainedSteer":[],"drainedFollowUp":[]});
        s["inbox"][queue] = json!(["live"]);
        bad(&c, E::Control);
    }
}

#[test]
fn delimiter_ids_preserve_exact_keys_and_unrelated_history_cannot_change_inventory_or_result() {
    let mut c = case("run.tools.completed");
    c["roots"]["lane"] = json!("lane:α");
    for field in ["configuration", "lane_state", "leaf"] {
        c["roots"][field]["key"] = json!("lane:α");
    }
    c["roots"]["operation_meta"]["value"]["lane"] = json!("lane:α");
    for field in ["operation_meta", "operation_state"] {
        c["roots"][field]["key"] = json!("op:α:β");
    }
    c["roots"]["operation_meta"]["value"]["operationId"] = json!("op:α:β");
    c["roots"]["lane_state"]["value"]["currentOperationId"] = json!("op:α:β");
    c["roots"]["operation_state"]["value"]["phase"]["batch"]["turnId"] = json!("turn::γ");
    c["registers"][1]["key"] = json!("op:α:β:turn::γ:5");
    good(&c);
    let mut l = Loaded::new(&c);
    let expected = l.validate().unwrap();
    let inventory = collect_current_references(&l.roots).unwrap();
    l.entries = l.entries.into_iter().rev().collect::<BTreeMap<_, _>>();
    l.registers = l.registers.into_iter().rev().collect();
    l.entries.insert(
        EntryId::new("unrequested-map-id").unwrap(),
        l.entries.values().next().unwrap().clone(),
    );
    l.registers.insert(
        (RegisterNamespace::FactCustom, "unrelated".into()),
        l.roots.configuration.clone(),
    );
    assert_eq!(l.validate().unwrap(), expected);
    assert_eq!(collect_current_references(&l.roots).unwrap(), inventory);
    let mut c = case("compaction.deciding");
    c["roots"]["operation_state"]["value"]["structural"]["taskId"] = json!("task::α");
    c["registers"][1]["key"] = json!("op:task::α");
    good(&c);
}

#[test]
fn errors_have_fixed_redacted_diagnostics() {
    let mut c = case("run.checkpoint");
    c["roots"]["operation_meta"]["value"]["lane"] = json!("private-sentinel");
    let error = Loaded::new(&c).validate().unwrap_err();
    assert_eq!(error, E::RootIdentity);
    assert_eq!(
        error.to_string(),
        "current entry/register validation failed: RootIdentity"
    );
    assert_eq!(format!("{error:?}"), "RootIdentity");
}

#[test]
fn every_active_label_hydrates_cancelled_drains_and_rejects_duplicate_owners() {
    for name in OPERATION_STATE_V1_INVENTORY {
        let mut c = case(name);
        c["roots"]["operation_state"]["value"]["control"] = json!({"status":"cancel_requested","requestedAt":9007199254740991_u64,"drainedSteer":["drained-s"],"drainedFollowUp":["drained-f"]});
        for id in ["drained-s", "drained-f"] {
            c["registers"].as_array_mut().unwrap().push(json!({"namespace":"pending.entry","key":id,"value":{"type":"message","payload":{"role":"user","content":"","timestamp":0}},"seq":99}));
        }
        good(&c);
        let mut l = Loaded::new(&c);
        l.registers
            .remove(&(RegisterNamespace::PendingEntry, "drained-s".into()));
        assert_eq!(l.validate().unwrap_err(), E::MissingRegister, "{name}");
        c["roots"]["operation_state"]["value"]["control"]["drainedFollowUp"] = json!(["drained-s"]);
        bad(&c, E::PendingOwnership);
    }
}

#[test]
fn prompt_and_current_assistant_roles_reject_standalone_valid_custom_or_user_entries() {
    let mut c = case("run.checkpoint");
    let mut custom = entry_mut(&mut c, "leaf").clone();
    custom["id"] = json!("prompt");
    *entry_mut(&mut c, "prompt") = custom;
    bad(&c, E::EntryRole);
    for name in [
        "run.tools.completed",
        "run.deferred.suspended",
        "run.failure_drain.response",
    ] {
        let mut c = case(name);
        let mut custom = entry_mut(&mut c, "leaf").clone();
        custom["id"] = json!("entry-response");
        *entry_mut(&mut c, "entry-response") = custom;
        bad(&c, E::EntryRole);
    }
    // Threshold trigger and ordinary generation trigger intentionally accept custom entries.
    good(&case("run.checkpoint"));
    good(&case("run.assistant.ready"));
}
