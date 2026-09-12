//! Shared logical runner. Expected rows come from literal pinned-clause fixtures.
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::storage::*;
use serde::Deserialize;
use std::sync::Arc;

pub trait BackendFactory {
    fn create(&self) -> Arc<dyn AtomicStorage>;
    fn execute<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        ready(future)
    }
}
pub trait Observer {
    fn observed(&self, stage: &'static str, snapshot: &StorageSnapshot);
}
pub fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match std::pin::pin!(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("Memory admitted operation suspended"),
    }
}
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn id(v: &str) -> EntryId {
    EntryId::new(v).unwrap()
}
pub fn tx(writes: Vec<Write>) -> Transaction {
    Transaction { writes }
}
pub fn set(key: &str, value: JsonValue) -> Write {
    Write::SetRegister(Box::new(RegisterSet::FactCustom {
        key: key.to_owned(),
        value,
    }))
}
pub fn delete(key: &str) -> Write {
    Write::DeleteRegister(RegisterDelete {
        namespace: RegisterNamespace::FactCustom,
        key: key.to_owned(),
    })
}
pub fn condition(key: &str, expected: ExpectedRegister) -> RegisterCondition {
    RegisterCondition {
        register: RegisterKey {
            namespace: RegisterNamespace::FactCustom,
            key: key.to_owned(),
        },
        expected,
    }
}
pub fn custom(name: &str, parent: Option<&str>) -> Write {
    Write::Entry(NewEntry::Custom(NewCustomEntry {
        id: id(name),
        parent_id: parent.map(id),
        entry_type: CustomEntryType::Custom,
        custom_type: CustomType::new("x").unwrap(),
        data: Presence::Missing,
    }))
}
pub fn usage(name: &str, value: Usage, entry: Option<&str>) -> Write {
    Write::Usage(NewUsageRow {
        id: UsageId::new(name).unwrap(),
        usage: value,
        entry_id: entry.map(id),
        adjustment: true,
        details: Presence::Missing,
    })
}
pub fn bytes(storage: &dyn AtomicStorage) -> Vec<u8> {
    serde_json::to_vec(&ready(storage.snapshot()).unwrap()).unwrap()
}
pub fn ids(entries: Vec<Entry>) -> Vec<String> {
    entries
        .into_iter()
        .map(|entry| {
            serde_json::to_value(entry).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}
#[derive(Deserialize)]
struct QueryCase<Q> {
    query: Q,
    ids: Vec<String>,
}
#[derive(Deserialize)]
struct Fixture {
    authority: String,
    clauses: Vec<String>,
    writes: Vec<Write>,
    branches: Vec<QueryCase<BranchScan>>,
    inventories: Vec<QueryCase<EntryScan>>,
}

pub fn queries(factory: &impl BackendFactory, observer: &impl Observer) {
    let fixture: Fixture = serde_json::from_str(include_str!("storage-contract.json")).unwrap();
    assert!(fixture.authority.starts_with("853a80d"));
    assert!(fixture.clauses.contains(&"737-751".to_owned()));
    let storage = factory.create();
    let committed = factory.execute(storage.commit(tx(fixture.writes))).unwrap();
    assert_eq!(committed.seqs, vec![n(0), n(1), n(2), n(3), n(4), n(5)]);
    for case in fixture.branches {
        assert_eq!(
            ids(factory
                .execute(storage.scan_branch(case.query.clone()))
                .unwrap()),
            case.ids
        );
        let structure = factory
            .execute(storage.scan_branch_structure(case.query))
            .unwrap();
        assert_eq!(
            structure
                .iter()
                .map(|row| row.id.to_string())
                .collect::<Vec<_>>(),
            case.ids
        );
        for row in structure {
            let json = serde_json::to_value(row).unwrap();
            assert!(json.get("data").is_none());
            assert!(json.get("message").is_none());
            assert!(json.get("summary").is_none());
        }
    }
    for case in fixture.inventories {
        assert_eq!(
            ids(factory.execute(storage.scan_entries(case.query)).unwrap()),
            case.ids
        );
    }
    assert_eq!(
        factory
            .execute(storage.get_entries(&[id("E"), id("missing"), id("E")]))
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        factory.execute(storage.scan_branch(BranchScan::default())),
        Err(StorageError::InvalidData(_))
    ));
    assert!(matches!(
        factory.execute(storage.scan_branch(BranchScan {
            start: Some(id("missing")),
            ..Default::default()
        })),
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(
        factory.execute(storage.stats()).unwrap().message_count,
        n(1)
    );
    observer.observed("queries", &factory.execute(storage.snapshot()).unwrap());
}
pub fn atomicity(factory: &impl BackendFactory, observer: &impl Observer) {
    let storage = factory.create();
    assert_eq!(
        factory.execute(storage.commit(tx(vec![]))).unwrap().seqs,
        vec![]
    );
    assert_eq!(
        factory.execute(storage.snapshot()).unwrap().next_seq,
        Some(n(0))
    );
    let result = factory
        .execute(storage.commit(tx(vec![
            custom("root", None),
            custom("child", Some("root")),
            usage("cost", Usage::zero(), Some("child")),
            set("v", JsonValue::Null),
            delete("absent"),
        ])))
        .unwrap();
    assert_eq!(result.seqs, vec![n(0), n(1), n(2), n(3), n(4)]);
    let before = snapshot_bytes(factory, &*storage);
    for bad in [
        custom("root", None),
        custom("cost", None),
        usage("root", Usage::zero(), None),
        usage("cost", Usage::zero(), None),
        custom("bad", Some("later")),
        custom("self", Some("self")),
        usage("dangling", Usage::zero(), Some("missing")),
    ] {
        let writes = vec![
            custom("prefix", None),
            set("v", JsonValue::Bool(true)),
            usage("new-cost", Usage::zero(), None),
            bad,
        ];
        assert!(matches!(
            factory.execute(storage.commit(tx(writes))),
            Err(StorageError::Corruption(_))
        ));
        assert_eq!(
            snapshot_bytes(factory, &*storage),
            before,
            "late failure leaked canonical authority"
        );
    }
    assert!(
        factory
            .execute(storage.commit(tx(vec![
                custom("new", None),
                usage("new", Usage::zero(), None)
            ])))
            .is_err()
    );
    assert_eq!(snapshot_bytes(factory, &*storage), before);
    let other = factory.create();
    factory
        .execute(other.commit(tx(vec![custom("root", None)])))
        .unwrap();
    assert_eq!(
        snapshot_bytes(factory, &*storage),
        before,
        "session isolation"
    );
    observer.observed("atomicity", &factory.execute(storage.snapshot()).unwrap());
}
pub fn registers_and_cas(factory: &impl BackendFactory, observer: &impl Observer) {
    let storage = factory.create();
    let absent = condition("k", ExpectedRegister::Absent);
    let request = |conditions, writes| ConditionalCommit {
        conditions,
        transaction: tx(writes),
    };
    assert!(matches!(
        factory
            .execute(storage.commit_conditional(request(
                vec![absent.clone()],
                vec![set("k", JsonValue::Null)]
            )))
            .unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    let before = snapshot_bytes(factory, &*storage);
    assert_eq!(
        factory
            .execute(storage.commit_conditional(request(vec![absent], vec![custom("never", None)])))
            .unwrap(),
        ConditionalOutcome::NotApplied
    );
    assert_eq!(snapshot_bytes(factory, &*storage), before);
    let zero = condition("k", ExpectedRegister::Seq(n(0)));
    assert!(matches!(
        factory
            .execute(storage.commit_conditional(request(
                vec![zero.clone(), condition("other", ExpectedRegister::Absent)],
                vec![set("k", JsonValue::Null)]
            )))
            .unwrap(),
        ConditionalOutcome::Applied(_)
    ));
    let before = snapshot_bytes(factory, &*storage);
    assert_eq!(
        factory
            .execute(
                storage
                    .commit_conditional(request(vec![zero.clone()], vec![custom("never", None)]))
            )
            .unwrap(),
        ConditionalOutcome::NotApplied,
        "equal value must not defeat CAS"
    );
    assert_eq!(snapshot_bytes(factory, &*storage), before);
    assert!(matches!(
        factory.execute(storage.commit_conditional(request(vec![zero.clone(), zero], vec![]))),
        Err(StorageError::InvalidData(_))
    ));
    assert_eq!(snapshot_bytes(factory, &*storage), before);
    factory
        .execute(storage.commit(tx(vec![delete("k"), set("k", JsonValue::Null)])))
        .unwrap();
    assert_eq!(
        factory
            .execute(storage.commit_conditional(request(
                vec![condition("k", ExpectedRegister::Seq(n(1)))],
                vec![]
            )))
            .unwrap(),
        ConditionalOutcome::NotApplied,
        "recreation is a new token"
    );
    factory
        .execute(storage.commit(tx(vec![
            set("a", JsonValue::Null),
            set("a\0z", JsonValue::Null),
            set("ab", JsonValue::Null),
            set("é", JsonValue::Null),
            set("", JsonValue::Null),
            Write::SetRegister(Box::new(RegisterSet::FactName {
                key: "a".to_owned(),
                value: "name".to_owned(),
            })),
        ])))
        .unwrap();
    let listed = factory
        .execute(storage.list_registers(RegisterNamespace::FactCustom, Some("a")))
        .unwrap();
    let keys: Vec<_> = listed
        .into_iter()
        .map(|row| {
            serde_json::to_value(row).unwrap()["key"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(keys, vec!["a", "a\0z", "ab"]);
    assert!(
        factory
            .execute(storage.get_register(RegisterNamespace::FactCustom, "missing"))
            .unwrap()
            .is_none()
    );
    observer.observed("cas", &factory.execute(storage.snapshot()).unwrap());
}
pub fn ledger(factory: &impl BackendFactory, observer: &impl Observer) {
    let storage = factory.create();
    let mut a = Usage::zero();
    a.input = n(2);
    a.total_tokens = n(9);
    a.cost.total = FiniteF64::new(-3.0).unwrap();
    a.reasoning = Some(n(1));
    let mut b = Usage::zero();
    b.output = n(3);
    b.total_tokens = n(8);
    b.cache_write_1h = Some(n(0));
    b.cost.total = FiniteF64::new(2.0).unwrap();
    factory
        .execute(storage.commit(tx(vec![
            usage("a", a, None),
            set("gap", JsonValue::Null),
            usage("b", b, None),
        ])))
        .unwrap();
    let stats = factory.execute(storage.stats()).unwrap();
    assert_eq!(
        (
            stats.usage.input,
            stats.usage.output,
            stats.usage.total_tokens
        ),
        (n(2), n(3), n(17))
    );
    assert_eq!(stats.usage.reasoning, Some(n(1)));
    assert_eq!(stats.usage.cache_write_1h, Some(n(0)));
    assert_eq!(stats.usage.cost.total.get(), -1.0);
    for (query, expected) in [
        (UsageScan::default(), vec!["a", "b"]),
        (
            UsageScan {
                from_seq: Some(n(2)),
                to_seq: Some(n(2)),
                ..Default::default()
            },
            vec!["b"],
        ),
        (
            UsageScan {
                order: Some(ScanOrder::Descending),
                limit: Some(n(1)),
                ..Default::default()
            },
            vec!["b"],
        ),
        (
            UsageScan {
                from_seq: Some(n(3)),
                to_seq: Some(n(0)),
                ..Default::default()
            },
            vec![],
        ),
        (
            UsageScan {
                limit: Some(n(0)),
                ..Default::default()
            },
            vec![],
        ),
    ] {
        assert_eq!(
            factory
                .execute(storage.scan_usage(query))
                .unwrap()
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            expected
        );
    }
    let before = snapshot_bytes(factory, &*storage);
    let mut huge = Usage::zero();
    huge.input = SafeUnsigned::MAX;
    assert!(matches!(
        factory.execute(storage.commit(tx(vec![
            set("prefix", JsonValue::Null),
            usage("overflow", huge, None)
        ]))),
        Err(StorageError::InvalidData(_))
    ));
    assert_eq!(snapshot_bytes(factory, &*storage), before);
    observer.observed("ledger", &factory.execute(storage.snapshot()).unwrap());
}

pub fn snapshot_bytes(factory: &impl BackendFactory, storage: &dyn AtomicStorage) -> Vec<u8> {
    serde_json::to_vec(&factory.execute(storage.snapshot()).unwrap()).unwrap()
}
