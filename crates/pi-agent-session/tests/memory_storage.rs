mod support;
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::memory::*;
use pi_agent_session::storage::*;
use std::sync::Arc;
use support::*;

#[test]
fn literal_branch_and_inventory_contract() {
    let observer = Observations::default();
    queries(&Factory, &observer);
    assert_eq!(*observer.0.lock().unwrap(), vec!["queries"]);
}
#[test]
fn atomic_rollback_identity_and_parent_contract() {
    atomicity(&Factory, &Observations::default());
}
#[test]
fn registers_and_exact_prestate_cas_contract() {
    registers_and_cas(&Factory, &Observations::default());
}
#[test]
fn maintained_ledger_range_and_overflow_contract() {
    ledger(&Factory, &Observations::default());
}

#[test]
fn all_register_namespaces_are_mechanical_and_classified_headers_survive() {
    let (_, storage) = memory();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .unwrap();
    let mut namespaces = std::collections::BTreeSet::new();
    for value in fixture["registers"].as_array().unwrap() {
        let mut value = value.clone();
        value.as_object_mut().unwrap().remove("seq");
        if value["namespace"] == "lane.leaf" {
            value["value"] = serde_json::Value::Null;
        }
        let set: RegisterSet = serde_json::from_value(value.clone()).unwrap();
        ready(storage.commit(tx(vec![Write::SetRegister(Box::new(set))]))).unwrap();
        namespaces.insert(value["namespace"].as_str().unwrap().to_owned());
    }
    assert_eq!(namespaces.len(), 12);
    let before = ready(storage.snapshot()).unwrap();
    for register in before.registers {
        let mut value = serde_json::to_value(register).unwrap();
        value.as_object_mut().unwrap().remove("seq");
        let set: RegisterSet = serde_json::from_value(value).unwrap();
        ready(storage.commit(tx(vec![Write::SetRegister(Box::new(set))]))).unwrap();
    }
    let states: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .unwrap();
    let mut state = states
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "run.assistant.effect_pending")
        .unwrap()["value"]
        .clone();
    state["phase"]["generation"]["context"]["streamOptions"]["headers"] =
        serde_json::json!({"X-Public":"retained"});
    let raw: RawRegisterSet = serde_json::from_value(
        serde_json::json!({"namespace":"op.state","key":"classified","value":state}),
    )
    .unwrap();
    let classified = raw
        .into_durable(&ProviderHeaderPolicy::from_allowlist([
            "x-public".to_owned()
        ]))
        .unwrap();
    ready(storage.commit(tx(vec![Write::SetRegister(Box::new(classified))]))).unwrap();
    let register = ready(storage.get_register(RegisterNamespace::OperationState, "classified"))
        .unwrap()
        .unwrap();
    let bytes = serde_json::to_vec(&register).unwrap();
    assert!(serde_json::from_slice::<Register>(&bytes).is_err());
    let imported =
        Arc::new(MemoryBacking::from_snapshot(ready(storage.snapshot()).unwrap()).unwrap())
            .open(Arc::new(Clock))
            .unwrap();
    assert_eq!(
        serde_json::to_vec(
            &ready(imported.get_register(RegisterNamespace::OperationState, "classified"))
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        bytes
    );
}

#[test]
fn detached_input_reads_and_original_rows_survive_import() {
    let (_, storage) = memory();
    let write:Write=serde_json::from_value(serde_json::json!({"kind":"entry","entry":{"id":"e","parentId":null,"type":"custom","customType":"x","data":{"items":[1]}}})).unwrap();
    let original = write.clone();
    ready(storage.commit(tx(vec![write]))).unwrap();
    let mut read = ready(storage.get_entries(&[id("e")])).unwrap();
    assert_eq!(
        serde_json::to_value(&read[&id("e")]).unwrap()["data"],
        serde_json::json!({"items":[1]})
    );
    if let Entry::Custom(value) = read.get_mut(&id("e")).unwrap() {
        value.data = Presence::Missing;
    }
    let before = bytes(&*storage);
    let mut snapshot = ready(storage.snapshot()).unwrap();
    if let Entry::Custom(value) = &mut snapshot.entries[0] {
        value.timestamp = n(99);
        value.seq = n(7);
    }
    snapshot.next_seq = Some(n(8));
    let imported = Arc::new(MemoryBacking::from_snapshot(snapshot.clone()).unwrap())
        .open(Arc::new(Clock))
        .unwrap();
    assert_eq!(ready(imported.snapshot()).unwrap(), snapshot);
    assert_eq!(bytes(&*storage), before);
    assert!(serde_json::to_value(original).unwrap()["entry"]["data"].is_object());
    ready(imported.commit(tx(vec![set("new", JsonValue::Null)]))).unwrap();
    assert_eq!(bytes(&*storage), before);
}

#[test]
fn allocator_max_is_assigned_once_and_exhaustion_is_atomic() {
    let (_, storage) = memory();
    let mut snapshot = ready(storage.snapshot()).unwrap();
    snapshot.next_seq = Some(SafeUnsigned::MAX);
    let store = Arc::new(MemoryBacking::from_snapshot(snapshot).unwrap())
        .open(Arc::new(Clock))
        .unwrap();
    assert!(
        ready(store.commit(tx(vec![
            set("a", JsonValue::Null),
            set("b", JsonValue::Null)
        ])))
        .is_err()
    );
    assert_eq!(
        ready(store.snapshot()).unwrap().next_seq,
        Some(SafeUnsigned::MAX)
    );
    assert_eq!(
        ready(store.commit(tx(vec![set("a", JsonValue::Null)])))
            .unwrap()
            .seqs,
        vec![SafeUnsigned::MAX]
    );
    let before = bytes(&*store);
    assert!(ready(store.commit(tx(vec![]))).is_err());
    assert!(ready(store.commit(tx(vec![delete("a")]))).is_err());
    assert_eq!(bytes(&*store), before);
}

#[test]
fn finite_cost_overflow_and_optional_field_sum_are_atomic() {
    let (_, storage) = memory();
    let mut amount = Usage::zero();
    amount.cost.input = FiniteF64::new(f64::MAX).unwrap();
    ready(storage.commit(tx(vec![usage("one", amount.clone(), None)]))).unwrap();
    let before = bytes(&*storage);
    assert!(ready(storage.commit(tx(vec![usage("two", amount, None)]))).is_err());
    assert_eq!(bytes(&*storage), before);
    assert_eq!(ready(storage.stats()).unwrap().usage.reasoning, None);
}

#[test]
fn snapshot_import_rejects_incoherent_authority() {
    let (_, storage) = memory();
    ready(storage.commit(tx(vec![
        custom("a", None),
        custom("b", Some("a")),
        usage("u", Usage::zero(), Some("b")),
        set("k", JsonValue::Null),
    ])))
    .unwrap();
    let snapshot = ready(storage.snapshot()).unwrap();
    let mut invalid = snapshot.clone();
    invalid.entries.remove(0);
    assert!(MemoryBacking::from_snapshot(invalid).is_err());
    let mut invalid = snapshot.clone();
    invalid.next_seq = Some(n(1));
    assert!(MemoryBacking::from_snapshot(invalid).is_err());
    let mut invalid = snapshot.clone();
    invalid.stats.message_count = n(1);
    assert!(MemoryBacking::from_snapshot(invalid).is_err());
    let mut invalid = snapshot.clone();
    invalid.usage[0].seq = n(0);
    assert!(MemoryBacking::from_snapshot(invalid).is_err());
    let mut invalid = snapshot.clone();
    invalid.registers.push(invalid.registers[0].clone());
    assert!(MemoryBacking::from_snapshot(invalid).is_err());
    assert!(MemoryBacking::from_snapshot(snapshot).is_ok());
}

#[test]
fn snapshot_lane_leaf_token_must_follow_its_target_entry() {
    let (_, storage) = memory();
    ready(storage.commit(tx(vec![
        delete("gap"),
        custom("entry", None),
        Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
            key: "main".to_owned(),
            value: Some(id("entry")),
        })),
    ])))
    .unwrap();
    let valid = ready(storage.snapshot()).unwrap();
    assert!(MemoryBacking::from_snapshot(valid.clone()).is_ok());
    let mut invalid = valid;
    if let Register::LaneLeaf { seq, .. } = &mut invalid.registers[0] {
        *seq = n(0);
    }
    assert!(matches!(
        MemoryBacking::from_snapshot(invalid),
        Err(StorageError::Corruption(_))
    ));
}

#[test]
fn every_register_namespace_deletes_without_history() {
    let (_, storage) = memory();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .unwrap();
    let mut writes = Vec::new();
    let mut deletions = Vec::new();
    for item in fixture["registers"].as_array().unwrap() {
        let mut item = item.clone();
        item.as_object_mut().unwrap().remove("seq");
        if item["namespace"] == "lane.leaf" {
            item["value"] = serde_json::Value::Null;
        }
        deletions.push(Write::DeleteRegister(RegisterDelete {
            namespace: serde_json::from_value(item["namespace"].clone()).unwrap(),
            key: item["key"].as_str().unwrap().to_owned(),
        }));
        writes.push(Write::SetRegister(Box::new(
            serde_json::from_value(item).unwrap(),
        )));
    }
    ready(storage.commit(tx(writes))).unwrap();
    assert_eq!(ready(storage.snapshot()).unwrap().registers.len(), 12);
    ready(storage.commit(tx(deletions.clone()))).unwrap();
    assert!(ready(storage.snapshot()).unwrap().registers.is_empty());
    ready(storage.commit(tx(deletions))).unwrap();
    assert!(ready(storage.snapshot()).unwrap().registers.is_empty());
}
