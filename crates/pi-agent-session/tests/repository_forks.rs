mod repository_support;
use repository_support::*;
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../conformance/session/repository-contract.json"
    ))
    .unwrap()
}
fn config() -> LaneConfiguration {
    serde_json::from_value(serde_json::json!({"model":{"provider":"future","modelId":"model"},"thinkingLevel":"off","activeToolNames":[]})).unwrap()
}
fn source(repo: &MemorySessionRepo) -> Arc<MemorySession> {
    let mut opts = options("source");
    opts.cwd = Some("source-cwd".into());
    let source = ready(repo.create(opts)).unwrap();
    let writes: Vec<Write> = serde_json::from_value(fixture()["entry_writes"].clone()).unwrap();
    commit(&source, writes);
    commit(
        &source,
        vec![
            leaf("main", Some("S")),
            leaf("side", Some("E")),
            state("side"),
            Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                key: "main".into(),
                value: config(),
            })),
            Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                key: "side".into(),
                value: config(),
            })),
            Write::SetRegister(Box::new(RegisterSet::PendingEntry {
                key: "pending".into(),
                value: PendingEntry {
                    entry_type: PendingEntryType::Custom,
                    custom_type: Some(CustomType::new("note").unwrap()),
                    payload: Presence::Null,
                },
            })),
        ],
    );
    ready(source.set_name(Some("name".into()))).unwrap();
    ready(source.set_custom_fact("custom".into(), Some(JsonValue::Null))).unwrap();
    ready(source.set_label(eid("A"), Some("kept".into()))).unwrap();
    ready(source.set_label(eid("E"), Some("side".into()))).unwrap();
    let mut usage = Usage::zero();
    usage.input = n(9);
    usage.total_tokens = n(9);
    usage.cost.total = FiniteF64::new(1.5).unwrap();
    commit(
        &source,
        vec![Write::Usage(NewUsageRow {
            id: UsageId::new("usage").unwrap(),
            usage,
            entry_id: Some(eid("B")),
            adjustment: true,
            details: Presence::Missing,
        })],
    );
    let durable: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .unwrap();
    for value in durable["registers"].as_array().unwrap() {
        let ns = value["namespace"].as_str().unwrap();
        if ns.starts_with("op.") || ns == "lane.lastResult" {
            let mut value = value.clone();
            value.as_object_mut().unwrap().remove("seq");
            commit(
                &source,
                vec![Write::SetRegister(Box::new(
                    serde_json::from_value(value).unwrap(),
                ))],
            );
        }
    }
    source
}
fn assert_idle(session: &MemorySession, lanes: &[&str]) {
    let leafs = ready(session.list_registers(RegisterNamespace::LaneLeaf, None)).unwrap();
    assert_eq!(leafs.len(), lanes.len());
    for lane in lanes {
        assert!(
            matches!(ready(session.get_register(RegisterNamespace::LaneState,lane)).unwrap(),Some(Register::LaneState {value:LaneState {current_operation_id:None,pending_next_run},..}) if pending_next_run.is_empty())
        );
    }
    for ns in [
        RegisterNamespace::OperationMeta,
        RegisterNamespace::OperationState,
        RegisterNamespace::OperationToolArguments,
        RegisterNamespace::OperationPreparation,
        RegisterNamespace::PendingEntry,
        RegisterNamespace::LaneLastResult,
    ] {
        assert!(ready(session.list_registers(ns, None)).unwrap().is_empty());
    }
    assert_eq!(ready(session.get_stats()).unwrap().usage, Usage::zero());
}
#[test]
fn branch_at_before_and_side_forks_preserve_literal_rows_facts_and_provenance() {
    let repo = repo();
    let source = source(&repo);
    let before = entries(&source);
    let f = fixture();
    for (dest, options, expected) in [
        ("at", BranchForkOptions::default(), "branch_ids"),
        (
            "before",
            BranchForkOptions {
                position: Some(ForkPosition::Before),
                ..Default::default()
            },
            "before_ids",
        ),
        (
            "side",
            BranchForkOptions {
                entry_id: Some(eid("E")),
                ..Default::default()
            },
            "side_ids",
        ),
    ] {
        let fork = ready(repo.fork(
            source.metadata(),
            request(dest, ForkOptions::Branch(options)),
        ))
        .unwrap();
        let copied = entries(&fork);
        assert_eq!(
            ids(&copied),
            serde_json::from_value::<Vec<String>>(f["expected"][expected].clone()).unwrap()
        );
        for entry in &copied {
            let entryid = serde_json::to_value(entry).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_eq!(
                serde_json::to_vec(entry).unwrap(),
                serde_json::to_vec(
                    before
                        .iter()
                        .find(|row| serde_json::to_value(row).unwrap()["id"] == entryid)
                        .unwrap()
                )
                .unwrap()
            );
        }
        if dest == "at" {
            assert_eq!(
                copied
                    .iter()
                    .map(|row| serde_json::to_value(row).unwrap()["seq"].as_u64().unwrap())
                    .collect::<Vec<_>>(),
                vec![2, 3, 5, 6, 7]
            );
        }
        assert_idle(&fork, &["main"]);
        assert_eq!(ready(fork.get_stats()).unwrap().message_count, n(1));
        if dest == "at" {
            let mut tokens = Vec::new();
            for namespace in [
                RegisterNamespace::LaneConfig,
                RegisterNamespace::LaneLeaf,
                RegisterNamespace::LaneState,
                RegisterNamespace::FactName,
                RegisterNamespace::FactLabel,
                RegisterNamespace::FactCustom,
            ] {
                for register in ready(fork.list_registers(namespace, None)).unwrap() {
                    tokens.push(
                        serde_json::to_value(register).unwrap()["seq"]
                            .as_u64()
                            .unwrap(),
                    );
                }
            }
            tokens.sort();
            assert_eq!(tokens, vec![8, 9, 10, 11, 12, 13]);
        }
        assert_eq!(ready(fork.get_name()).unwrap(), Some("name".into()));
        assert_eq!(
            ready(fork.get_custom_fact("custom")).unwrap(),
            Some(JsonValue::Null)
        );
        assert_eq!(
            ready(fork.get_label(eid("A"))).unwrap(),
            Some("kept".into())
        );
        assert_eq!(
            ready(fork.get_label(eid("E"))).unwrap().is_some(),
            dest == "side"
        );
        assert!(
            ready(fork.get_register(RegisterNamespace::LaneConfig, "main"))
                .unwrap()
                .is_some()
        );
        assert_eq!(fork.metadata().cwd, None);
        assert_eq!(fork.metadata().parent_session_id, Some(sid("source")));
        assert_eq!(fork.metadata().store_generation, n(0));
        let max = copied
            .iter()
            .map(|row| serde_json::to_value(row).unwrap()["seq"].as_u64().unwrap())
            .max()
            .unwrap();
        for reg in ready(fork.list_registers(RegisterNamespace::LaneLeaf, None)).unwrap() {
            assert!(serde_json::to_value(reg).unwrap()["seq"].as_u64().unwrap() > max);
        }
    }
    assert_eq!(entries(&source), before);
    assert_eq!(ready(source.get_stats()).unwrap().usage.input, n(9));
}
#[test]
fn tree_fork_copies_all_lanes_and_labels_and_both_root_scopes_stay_unconfigured() {
    let repo = repo();
    let source = source(&repo);
    let mut request = request("tree", tree());
    request.create.cwd = Some("explicit".into());
    let fork = ready(repo.fork(source.metadata(), request)).unwrap();
    assert_eq!(ids(&entries(&fork)), vec!["A", "B", "E", "C", "D", "S"]);
    assert_idle(&fork, &["main", "side"]);
    assert_eq!(
        ready(fork.view(LaneId::new("side").unwrap()).get_leaf_id()).unwrap(),
        Some(eid("E"))
    );
    assert_eq!(
        ready(fork.get_label(eid("E"))).unwrap(),
        Some("side".into())
    );
    assert_eq!(fork.metadata().cwd.as_deref(), Some("explicit"));
    let empty = ready(repo.create(options("empty"))).unwrap();
    for (id, scope) in [("empty-branch", branch()), ("empty-tree", tree())] {
        let copied =
            ready(repo.fork(empty.metadata(), repository_support::request(id, scope))).unwrap();
        assert!(entries(&copied).is_empty());
        assert_eq!(ready(copied.get_leaf_id()).unwrap(), None);
        assert!(
            ready(copied.get_register(RegisterNamespace::LaneConfig, "main"))
                .unwrap()
                .is_none()
        );
    }
    let before_root = ready(repo.fork(
        source.metadata(),
        repository_support::request(
            "before-root",
            ForkOptions::Branch(BranchForkOptions {
                entry_id: Some(eid("A")),
                position: Some(ForkPosition::Before),
                ..Default::default()
            }),
        ),
    ))
    .unwrap();
    assert!(entries(&before_root).is_empty());
    assert_eq!(ready(before_root.get_leaf_id()).unwrap(), None);
}
#[test]
fn failed_fork_does_not_publish_destination_or_overwrite_existing() {
    let repo = repo();
    let source = source(&repo);
    let original = entries(&source);
    let bad = ForkOptions::Branch(BranchForkOptions {
        entry_id: Some(eid("missing")),
        ..Default::default()
    });
    assert!(ready(repo.fork(source.metadata(), request("failed", bad))).is_err());
    assert!(
        !ready(repo.list(()))
            .unwrap()
            .iter()
            .any(|m| m.id == sid("failed"))
    );
    let mut contradictory = request("failed", branch());
    contradictory.create.parent_session_id = Some(sid("other"));
    assert!(ready(repo.fork(source.metadata(), contradictory)).is_err());
    ready(repo.create(options("failed"))).unwrap();
    assert_eq!(entries(&source), original);
}
