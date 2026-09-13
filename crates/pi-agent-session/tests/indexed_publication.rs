//! Test-only indexed backend over real Memory; this is interface proof, not SQLite.
mod read_repository_support;
use pi_agent_session::session::RuntimeSession;
use read_repository_support::*;
use std::sync::Mutex;
struct Backend {
    raw: Arc<MemoryStorage>,
    indexed: AtomicU64,
    fallback: AtomicU64,
    reads: Mutex<Vec<IndexedReadSet>>,
}
impl Storage for Backend {
    fn commit(&self, t: Transaction) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.raw.commit(t)
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.raw.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        ns: RegisterNamespace,
        k: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        self.raw.get_register(ns, k)
    }
    fn list_registers<'a>(
        &'a self,
        ns: RegisterNamespace,
        k: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        self.raw.list_registers(ns, k)
    }
    fn scan_branch(&self, q: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.raw.scan_branch(q)
    }
    fn scan_branch_structure(
        &self,
        q: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.raw.scan_branch_structure(q)
    }
    fn scan_entries(&self, q: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.raw.scan_entries(q)
    }
    fn scan_usage(&self, q: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.raw.scan_usage(q)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        self.raw.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.raw.close()
    }
}
impl AtomicStorage for Backend {
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.raw.snapshot()
    }
    fn commit_conditional(
        &self,
        r: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.raw.commit_conditional(r)
    }
}
struct Guard<'a, V: PublicationValidator + ?Sized> {
    validator: &'a V,
    backend: &'a Backend,
}
impl<V: PublicationValidator + ?Sized> PublicationValidator for Guard<'_, V> {
    type Error = V::Error;
    fn validate(&self, s: &StorageSnapshot) -> Result<(), Self::Error> {
        let indexed = self
            .validator
            .indexed()
            .expect("real Session must supply indexed capability");
        let read = indexed.read_set();
        assert!(
            read.all_pending_keys,
            "entire pending namespace must be declared"
        );
        let projection = read.project(s);
        let full = self.validator.validate(s);
        let projected = indexed.validate_projection(&projection);
        assert_eq!(
            full.is_ok(),
            projected.is_ok(),
            "complete and projected validation disagree"
        );
        self.backend.reads.lock().unwrap().push(read);
        self.backend.indexed.fetch_add(1, Ordering::SeqCst);
        projected
    }
}
impl CheckedStorage for Backend {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        r: ConditionalCommit,
        v: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            if v.indexed().is_none() {
                self.fallback.fetch_add(1, Ordering::SeqCst);
                self.raw.commit_checked(r, v).await
            } else {
                self.raw
                    .commit_checked(
                        r,
                        &Guard {
                            validator: v,
                            backend: self,
                        },
                    )
                    .await
            }
        })
    }
}
fn session() -> (Arc<Backend>, RuntimeSession<Backend>) {
    let raw = Arc::new(MemoryBacking::new())
        .open(Arc::new(Clock))
        .unwrap();
    let backend = Arc::new(Backend {
        raw,
        indexed: AtomicU64::new(0),
        fallback: AtomicU64::new(0),
        reads: Mutex::new(vec![]),
    });
    let metadata = serde_json::from_value(
        serde_json::json!({"id":"indexed","createdAt":0,"storageVersion":1,"storeGeneration":0}),
    )
    .unwrap();
    let session = ready(RuntimeSession::new(
        backend.clone(),
        metadata,
        Arc::new(
            SessionCodec::new(
                SessionCodecOptions::default(),
                ProviderHeaderPolicy::default(),
            )
            .unwrap(),
        ),
        Arc::new(Ids::default()),
        Arc::new(NoopTelemetryContext),
    ))
    .unwrap();
    (backend, session)
}
fn pending(key: &str) -> Write {
    Write::SetRegister(Box::new(RegisterSet::PendingEntry {
        key: key.into(),
        value: PendingEntry {
            entry_type: PendingEntryType::Custom,
            custom_type: Some(CustomType::new("note").unwrap()),
            payload: Presence::Missing,
        },
    }))
}
fn summary(id: &str, from: &str) -> Write {
    serde_json::from_value(serde_json::json!({"kind":"entry","entry":{"id":id,"parentId":null,"type":"branch_summary","fromId":from,"summary":"text","fromHook":false}})).unwrap()
}
#[test]
fn runtime_offers_equivalent_indexed_summary_and_label_guards() {
    for (writes, ok) in [
        (vec![summary("self", "self")], false),
        (vec![summary("s", "later"), custom("later", "data")], false),
        (
            vec![custom("earlier", "data"), summary("s", "earlier")],
            true,
        ),
    ] {
        let (backend, session) = session();
        assert_eq!(ready(session.commit(tx(writes))).is_ok(), ok);
        assert_eq!(backend.indexed.load(Ordering::SeqCst), 1);
        assert_eq!(backend.fallback.load(Ordering::SeqCst), 0);
    }
    let (backend, session) = session();
    let old = Write::SetRegister(Box::new(RegisterSet::FactLabel {
        key: "unresolved".into(),
        value: "old imported".into(),
    }));
    ready(backend.raw.commit(tx(vec![old.clone()]))).unwrap();
    ready(session.commit(tx(vec![]))).unwrap();
    assert!(ready(session.commit(tx(vec![old]))).is_err());
    ready(session.commit(tx(vec![
        custom("target", "data"),
        Write::SetRegister(Box::new(RegisterSet::FactLabel {
            key: "target".into(),
            value: "new".into(),
        })),
    ])))
    .unwrap();
}
#[test]
fn unrelated_pending_coexistence_is_not_hidden_by_a_touched_only_projection() {
    let (backend, session) = session();
    ready(
        backend
            .raw
            .commit(tx(vec![custom("unrelated", "data"), pending("unrelated")])),
    )
    .unwrap();
    let before = ready(backend.snapshot()).unwrap();
    assert!(ready(session.commit(tx(vec![]))).is_err());
    assert_eq!(ready(backend.snapshot()).unwrap(), before);
    assert!(
        backend
            .reads
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .all_pending_keys
    );
}
#[test]
fn projected_final_state_allows_late_placement_and_rejects_new_coexistence() {
    let (backend, session) = session();
    ready(backend.raw.commit(tx(vec![pending("queued")]))).unwrap();
    ready(session.commit(tx(vec![
        custom("queued", "placed"),
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::PendingEntry,
            key: "queued".into(),
        }),
    ])))
    .unwrap();
    assert!(ready(session.commit(tx(vec![custom("bad", "data"), pending("bad")]))).is_err());
    assert!(ready(session.commit(tx(vec![pending("queued")]))).is_err());
}
#[test]
fn explicit_absence_and_complete_pending_namespace_are_required() {
    let (backend, _) = session();
    let snapshot = ready(backend.snapshot()).unwrap();
    let read = IndexedReadSet {
        entry_ids: ["missing".to_owned(), "".to_owned()].into(),
        all_pending_keys: true,
    };
    let projection = read.project(&snapshot);
    assert!(projection.entries["missing"].is_none());
    assert!(projection.entries[""].is_none());
    read.check(&projection).unwrap();
    let mut incomplete = projection.clone();
    incomplete.entries.remove("missing");
    assert!(read.check(&incomplete).is_err());
    let mut incomplete = projection;
    incomplete.pending_keys = None;
    assert!(read.check(&incomplete).is_err());
}
#[test]
fn custom_complete_validators_keep_the_default_fallback() {
    struct Full;
    impl PublicationValidator for Full {
        type Error = ();
        fn validate(&self, s: &StorageSnapshot) -> Result<(), ()> {
            assert!(s.entries.is_empty());
            Ok(())
        }
    }
    let (backend, _) = session();
    ready(backend.commit_checked(
        ConditionalCommit {
            conditions: vec![],
            transaction: tx(vec![]),
        },
        &Full,
    ))
    .unwrap();
    assert_eq!(backend.fallback.load(Ordering::SeqCst), 1);
    assert_eq!(backend.indexed.load(Ordering::SeqCst), 0);
}
#[test]
fn external_backend_helpers_and_telemetry_construct_real_session_without_extra_edges() {
    let entry = materialize_entry(
        match custom("e", "body") {
            Write::Entry(v) => v,
            _ => unreachable!(),
        },
        n(4),
        n(7),
    );
    assert_eq!(entry_structure(&entry).seq, n(4));
    let register = materialize_register(
        RegisterSet::FactCustom {
            key: "key".into(),
            value: JsonValue::Null,
        },
        n(9),
    );
    assert_eq!(register_seq(&register), n(9));
    assert_eq!(register_key(&register).key, "key");
    let raw = Arc::new(MemoryBacking::new())
        .open(Arc::new(Clock))
        .unwrap();
    ready(raw.commit(tx(lane_seed_writes("main", None, None)))).unwrap();
    let source = ready(raw.snapshot()).unwrap();
    let (selected, writes) = prepare_fork(
        source,
        ForkOptions::Branch(BranchForkOptions::default()),
        n(1),
    )
    .unwrap();
    assert!(selected.entries.is_empty());
    assert_eq!(writes.len(), 2);
    let _: Arc<dyn TelemetryContext> = Arc::new(NoopTelemetryContext);
}

#[test]
fn actual_memory_and_jsonl_keep_the_complete_validation_route() {
    struct Full(AtomicU64);
    impl PublicationValidator for Full {
        type Error = ();
        fn indexed(&self) -> Option<&dyn IndexedPublicationValidator<Error = ()>> {
            panic!("accepted Memory/JSONL must keep full validation")
        }
        fn validate(&self, s: &StorageSnapshot) -> Result<(), ()> {
            assert!(s.entries.is_empty());
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn check<S: CheckedStorage>(storage: &S) {
        let validator = Full(AtomicU64::new(0));
        ready(storage.commit_checked(
            ConditionalCommit {
                conditions: vec![],
                transaction: tx(vec![]),
            },
            &validator,
        ))
        .unwrap();
        assert_eq!(validator.0.load(Ordering::SeqCst), 1);
    }
    let memory = Arc::new(MemoryBacking::new())
        .open(Arc::new(Clock))
        .unwrap();
    check(&*memory);
    let dir = Dir::new();
    let metadata = serde_json::from_value(
        serde_json::json!({"id":"full","createdAt":0,"storageVersion":1,"storeGeneration":0}),
    )
    .unwrap();
    let storage = JsonlStorage::create(
        dir.0.join("full.jsonl"),
        metadata,
        Arc::new(
            SessionCodec::new(
                SessionCodecOptions::default(),
                ProviderHeaderPolicy::default(),
            )
            .unwrap(),
        ),
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    check(&*storage);
}
