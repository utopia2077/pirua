mod session_support;
use session_support::*;
use std::sync::{Arc, atomic::Ordering};
#[test]
fn generic_consumer_reaches_all_tree_and_session_methods() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let probe = Arc::new(Probe::new(raw.clone()));
    let s = session(probe, codec());
    assert_eq!(s.metadata(), metadata());
    assert!(pi_agent_session::id::uuid_timestamp(&s.id_generator().next(None)).is_ok());
    assert_eq!(ready(s.get_leaf_id()).unwrap(), None);
    let a = ready(s.append_message(user("a"))).unwrap();
    let b = ready(s.append_custom_entry(ct("state"), Presence::Null)).unwrap();
    assert_eq!(ready(s.get_stats()).unwrap().message_count, n(1));
    assert!(ready(s.get_entry(a.clone())).unwrap().is_some());
    assert_eq!(
        ready(s.get_entries(&[a.clone(), b.clone(), eid("absent")]))
            .unwrap()
            .len(),
        2
    );
    ready(s.set_name(Some("name".into()))).unwrap();
    assert_eq!(ready(s.get_name()).unwrap().as_deref(), Some("name"));
    ready(s.set_label(a.clone(), Some("label".into()))).unwrap();
    assert_eq!(
        ready(s.get_label(a.clone())).unwrap().as_deref(),
        Some("label")
    );
    ready(s.set_custom_fact("x".into(), Some(JsonValue::Null))).unwrap();
    assert_eq!(
        ready(s.get_custom_fact("x")).unwrap(),
        Some(JsonValue::Null)
    );
    ready(s.set_custom_fact("x".into(), None)).unwrap();
    assert_eq!(ready(s.get_custom_fact("x")).unwrap(), None);
    assert_eq!(ready(s.get_leaf_id()).unwrap(), Some(b.clone()));
    assert_eq!(
        ready(s.find_entries(EntryQuery::default())).unwrap().len(),
        2
    );
    assert!(matches!(
        ready(s.find_entry(EntryQuery::default())).unwrap(),
        Some(Entry::Custom(_))
    ));
    assert_eq!(
        ready(s.find_entries_on_branch(BranchScan::default()))
            .unwrap()
            .len(),
        2
    );
    assert!(
        ready(s.find_entry_on_branch(BranchScan::default()))
            .unwrap()
            .is_some()
    );
    assert!(
        ready(s.get_register(RegisterNamespace::FactName, ""))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        ready(s.list_registers(RegisterNamespace::FactLabel, None))
            .unwrap()
            .len(),
        1
    );
    ready(s.commit(tx(vec![]))).unwrap();
    let unknown = s.view(lane("unknown"));
    let before = bytes(raw.as_ref());
    assert!(ready(unknown.get_name()).is_err());
    assert!(ready(unknown.append_message(user("no"))).is_err());
    assert_eq!(before, bytes(raw.as_ref()));
    ready(s.set_name(None)).unwrap();
    ready(s.set_label(a, None)).unwrap();
    ready(s.set_label(eid("absent"), None)).unwrap();
    assert!(ready(s.set_label(eid("absent"), Some("bad".into()))).is_err());
    let view = s.view(lane("main"));
    ready(s.close()).unwrap();
    ready(s.close()).unwrap();
    assert_eq!(ready(view.get_leaf_id()).unwrap_err().category(), "closed");
}
#[test]
fn same_lane_cas_retries_and_state_only_contender_blocks_append() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let probe = Arc::new(Probe::new(raw.clone()));
    let s = session(probe.clone(), codec());
    let r = raw.clone();
    *probe.before.lock().unwrap() = Some(Box::new(move || {
        ready(r.commit(tx(vec![
            custom_entry("other", None),
            set(RegisterSet::LaneLeaf {
                key: "main".into(),
                value: Some(eid("other")),
            }),
        ])))
        .unwrap();
    }));
    let added = ready(s.append_message(user("tail"))).unwrap();
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 2);
    let Entry::Message(e) = ready(s.get_entry(added)).unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(e.parent_id, Some(eid("other")));
    let r = raw.clone();
    *probe.before.lock().unwrap() = Some(Box::new(move || {
        ready(r.commit(tx(vec![set(RegisterSet::LaneState {
            key: "main".into(),
            value: LaneState {
                current_operation_id: Some(OperationId::new("op").unwrap()),
                pending_next_run: vec![],
            },
        })])))
        .unwrap();
    }));
    let prior = ready(raw.stats()).unwrap();
    assert_eq!(
        ready(s.append_message(user("blocked")))
            .unwrap_err()
            .category(),
        "invalid_operation"
    );
    assert_eq!(ready(raw.stats()).unwrap(), prior);
}
#[test]
fn concurrent_appends_chain_and_separate_lanes_diverge() {
    let (_, raw, s) = fresh();
    let s = Arc::new(s);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let s = s.clone();
            scope.spawn(move || {
                ready(s.append_message(user("thread"))).unwrap();
            });
        }
    });
    assert_eq!(
        ready(s.find_entries_on_branch(BranchScan::default()))
            .unwrap()
            .len(),
        8
    );
    let anchor = ready(s.get_leaf_id()).unwrap();
    seed(raw.as_ref(), "side", anchor.clone());
    let side = s.view(lane("side"));
    let a = ready(s.append_message(user("main"))).unwrap();
    let b = ready(side.append_custom_entry(ct("state"), Presence::Missing)).unwrap();
    for id in [a, b] {
        let entry = ready(s.get_entry(id)).unwrap().unwrap();
        let value = serde_json::to_value(entry).unwrap();
        assert_eq!(value["parentId"], serde_json::to_value(&anchor).unwrap());
    }
}
#[test]
fn global_cursor_zero_max_limits_and_detached_reads() {
    let (_, raw, s) = fresh();
    let a = ready(s.append_message(user("first"))).unwrap();
    let b = ready(s.append_message(user("second"))).unwrap();
    let seq = match ready(s.get_entry(b.clone())).unwrap().unwrap() {
        Entry::Message(e) => e.seq,
        _ => panic!(),
    };
    let query = EntryQuery {
        cursor: Some(EntryCursor { seq }),
        limit: Some(n(1)),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(ready(s.find_entry(query)).unwrap().unwrap()).unwrap()["id"],
        a.as_str()
    );
    for (order, cursor) in [
        (ScanOrder::Descending, 0),
        (ScanOrder::Ascending, 9_007_199_254_740_991),
    ] {
        assert!(
            ready(s.find_entries(EntryQuery {
                order: Some(order),
                cursor: Some(EntryCursor { seq: n(cursor) }),
                ..Default::default()
            }))
            .unwrap()
            .is_empty()
        );
    }
    assert!(
        ready(s.find_entry(EntryQuery {
            limit: Some(n(0)),
            ..Default::default()
        }))
        .unwrap()
        .is_none()
    );
    let before = bytes(raw.as_ref());
    let mut entry = ready(s.get_entry(b)).unwrap().unwrap();
    if let Entry::Message(e) = &mut entry {
        e.message = user("changed");
    }
    assert_eq!(before, bytes(raw.as_ref()));
}
#[test]
fn late_pending_guard_allows_placement_but_rejects_coexistence() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let probe = Arc::new(Probe::new(raw.clone()));
    let s = session(probe.clone(), codec());
    let r = raw.clone();
    *probe.before.lock().unwrap() = Some(Box::new(move || {
        ready(r.commit(tx(vec![set(RegisterSet::PendingEntry {
            key: "reserved".into(),
            value: PendingEntry {
                entry_type: PendingEntryType::Custom,
                custom_type: Some(ct("state")),
                payload: Presence::Missing,
            },
        })])))
        .unwrap();
    }));
    assert_eq!(
        ready(s.commit(tx(vec![custom_entry("reserved", None)])))
            .unwrap_err()
            .category(),
        "schema"
    );
    assert!(
        ready(raw.get_entries(&[eid("reserved")]))
            .unwrap()
            .is_empty()
    );
    ready(s.commit(tx(vec![
        custom_entry("reserved", None),
        del(RegisterNamespace::PendingEntry, "reserved"),
    ])))
    .unwrap();
    assert!(ready(s.get_entry(eid("reserved"))).unwrap().is_some());
}
#[test]
fn literal_session_query_contract() {
    let (_, _, s) = fresh();
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/session/session-contract.json"
    ))
    .unwrap();
    let writes = serde_json::from_value(contract["writes"].clone()).unwrap();
    ready(s.commit(tx(writes))).unwrap();
    for (group, branch) in [("globalCases", false), ("branchCases", true)] {
        for case in contract[group].as_array().unwrap() {
            let entries =
                if branch {
                    ready(s.find_entries_on_branch(
                        serde_json::from_value(case["query"].clone()).unwrap(),
                    ))
                    .unwrap()
                } else {
                    ready(s.find_entries(serde_json::from_value(case["query"].clone()).unwrap()))
                        .unwrap()
                };
            let ids: Vec<_> = entries
                .iter()
                .map(|e| serde_json::to_value(e).unwrap()["id"].clone())
                .collect();
            assert_eq!(serde_json::to_value(ids).unwrap(), case["ids"], "{group}");
        }
    }
}

#[test]
fn direct_summary_sources_precede_new_summary_and_imports_remain_opaque() {
    struct Count(std::sync::atomic::AtomicUsize);
    impl pi_telemetry::TelemetryContext for Count {
        fn start_span(&self, options: pi_telemetry::SpanOptions) -> pi_telemetry::SpanGuard {
            self.0.fetch_add(1, Ordering::SeqCst);
            pi_telemetry::TelemetryContext::start_span(&pi_telemetry::NoopTelemetryContext, options)
        }
    }
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/session/session-contract.json"
    ))
    .unwrap();
    let mut observations = Vec::new();
    for case in contract["provenanceCases"].as_array().unwrap() {
        let (_, raw) = memory();
        seed(raw.as_ref(), "main", None);
        let count = Arc::new(Count(std::sync::atomic::AtomicUsize::new(0)));
        let session = ready(RuntimeSession::new(
            raw.clone(),
            metadata(),
            codec(),
            Arc::new(Ids::default()),
            count.clone(),
        ))
        .unwrap();
        let before = bytes(raw.as_ref());
        let writes = serde_json::from_value(case["writes"].clone()).unwrap();
        let result = ready(session.commit_conditional(ConditionalCommit {
            conditions: vec![],
            transaction: tx(writes),
        }));
        let accepted = matches!(&result, Ok(ConditionalOutcome::Applied(_)));
        observations.push((case["name"].as_str().unwrap().to_owned(), accepted));
        if !case["accepted"].as_bool().unwrap() && !accepted {
            assert!(
                matches!(result, Err(CheckedCommitError::Rejected(error)) if error.category() == "schema")
            );
            assert_eq!(
                bytes(raw.as_ref()),
                before,
                "rejection must preserve allocator and complete snapshot"
            );
            assert_eq!(
                count.0.load(Ordering::SeqCst),
                0,
                "rejection must emit no write span"
            );
        } else if accepted {
            assert_eq!(count.0.load(Ordering::SeqCst), 1);
        }
    }
    // An already committed source is also valid; no operation-state interpreter is needed.
    let (_, raw, session) = fresh();
    ready(session.commit(tx(vec![custom_entry("source", None)]))).unwrap();
    let summary: Write = serde_json::from_value(json!({"kind":"entry","entry":{"id":"summary","parentId":null,"type":"branch_summary","fromId":"source","summary":"preexisting","fromHook":false}})).unwrap();
    ready(session.commit(tx(vec![summary]))).unwrap();
    let snapshot = ready(raw.snapshot()).unwrap();
    assert!(matches!(&snapshot.entries[1],Entry::BranchSummary(summary) if summary.seq == n(3)));
    // A fork/import preserves source provenance even when it is outside the selected path.
    let (_, imported_source) = memory();
    seed(imported_source.as_ref(), "main", None);
    let imported: Write = serde_json::from_value(json!({"kind":"entry","entry":{"id":"imported","parentId":null,"type":"branch_summary","fromId":"off-path-source","summary":"preserve exact source text","fromHook":true}})).unwrap();
    ready(imported_source.commit(tx(vec![imported]))).unwrap();
    let snapshot = ready(imported_source.snapshot()).unwrap();
    let backing = Arc::new(MemoryBacking::from_snapshot(snapshot.clone()).unwrap());
    let opened = backing.open(Arc::new(Clock)).unwrap();
    let reopened = session_support::session(opened.clone(), codec());
    assert!(
        matches!(ready(reopened.get_entry(eid("imported"))).unwrap(),Some(Entry::BranchSummary(summary)) if summary.from_id == eid("off-path-source"))
    );
    assert_eq!(ready(opened.snapshot()).unwrap(), snapshot);
    let expected: Vec<_> = contract["provenanceCases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| {
            (
                case["name"].as_str().unwrap().to_owned(),
                case["accepted"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        observations, expected,
        "direct summary source must precede its newly authored row"
    );
}
