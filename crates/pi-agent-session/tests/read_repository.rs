mod read_repository_support;
use read_repository_support::*;
#[test]
fn actual_memory_domains_are_lazy_owner_scoped_and_do_not_consume_runtime_ids() {
    let ids = Arc::new(Ids::default());
    let repo = memory(ids.clone());
    let clone = repo.clone();
    let other = memory(ids.clone());
    assert_eq!(
        repo.source_domain().unwrap(),
        clone.source_domain().unwrap()
    );
    assert_ne!(
        repo.source_domain().unwrap(),
        other.source_domain().unwrap()
    );
    assert_eq!(ids.0.load(Ordering::SeqCst), 0);
    assert!(repo.managed_files().unwrap().is_none());
    assert!(repo.read_list(()).unwrap().is_empty());
}
#[test]
fn memory_pages_include_nonmessages_and_keep_surviving_views_active() {
    let repo = memory(Arc::new(Ids::default()));
    let root = ready(repo.create(options("m"))).unwrap();
    let view = root.view(LaneId::new("main").unwrap());
    ready(root.commit(tx(vec![custom("a", "one"), custom("b", "two")]))).unwrap();
    let metadata = root.metadata();
    let first = repo.read_batch(&metadata, request(None, 1)).unwrap();
    assert_eq!(first.entries.len(), 1);
    assert!(!first.complete);
    assert!(!first.reset);
    assert_eq!(first.next_cursor.last_entry_seq, Some(n(2)));
    let wire = serde_json::to_vec(&first.next_cursor).unwrap();
    let cursor = serde_json::from_slice(&wire).unwrap();
    let second = repo
        .read_batch(&metadata, request(Some(cursor), 1))
        .unwrap();
    assert_eq!(entry_structure(&second.entries[0]).id.as_str(), "b");
    assert!(second.complete);
    ready(root.set_name(Some("only a register".into()))).unwrap();
    let gap = repo
        .read_batch(&metadata, request(Some(second.next_cursor.clone()), 1))
        .unwrap();
    assert_eq!(gap.next_cursor.last_entry_seq, Some(n(3)));
    assert!(gap.complete && gap.entries.is_empty());
    assert!(ready(view.get_leaf_id()).is_ok());
    assert!(Arc::ptr_eq(
        &root,
        &ready(repo.open(metadata.clone())).unwrap()
    ));
    ready(root.close()).unwrap();
    let last = repo
        .read_batch(&metadata, request(Some(second.next_cursor), 2))
        .unwrap();
    assert!(last.entries.is_empty());
    assert!(last.complete);
    assert!(ready(view.get_leaf_id()).is_err());
    ready(repo.open(metadata)).unwrap();
}
#[test]
fn semantic_recreation_changes_or_preserves_prefix_as_declared() {
    let repo = memory(Arc::new(Ids::default()));
    let root = ready(repo.create(options("same"))).unwrap();
    ready(root.commit(tx(vec![custom("a", "same")]))).unwrap();
    let metadata = root.metadata();
    let cursor = repo
        .read_batch(&metadata, request(None, 1))
        .unwrap()
        .next_cursor;
    ready(repo.delete(metadata.clone())).unwrap();
    let same = ready(repo.create(options("same"))).unwrap();
    ready(same.commit(tx(vec![custom("a", "same"), custom("b", "extra")]))).unwrap();
    let continued = repo
        .read_batch(&metadata, request(Some(cursor.clone()), 2))
        .unwrap();
    assert!(!continued.reset);
    assert_eq!(continued.entries.len(), 1);
    ready(repo.delete(metadata.clone())).unwrap();
    let changed = ready(repo.create(options("same"))).unwrap();
    ready(changed.commit(tx(vec![custom("a", "changed")]))).unwrap();
    let reset = repo
        .read_batch(&metadata, request(Some(cursor.clone()), 2))
        .unwrap();
    assert!(reset.reset);
    assert_eq!(reset.entries.len(), 1);
    ready(repo.delete(metadata.clone())).unwrap();
    ready(repo.create(options("same"))).unwrap();
    let truncated = repo
        .read_batch(&metadata, request(Some(cursor), 2))
        .unwrap();
    assert!(truncated.reset && truncated.entries.is_empty() && truncated.complete);
}
#[test]
fn jsonl_inactive_reads_never_open_a_writer_or_compact() {
    let dir = Dir::new();
    let repo = jsonl(&dir);
    let root = ready(repo.create(options("disk"))).unwrap();
    ready(root.commit(tx(vec![custom("a", "before")]))).unwrap();
    for _ in 0..30 {
        ready(root.set_name(Some(
            "long dead name repeated so native open would compact".into(),
        )))
        .unwrap();
    }
    let metadata = root.metadata();
    ready(root.close()).unwrap();
    let before = std::fs::read(&metadata.path).unwrap();
    let page = repo.read_batch(&metadata, request(None, 1)).unwrap();
    assert_eq!(
        std::fs::read(&metadata.path).unwrap(),
        before,
        "read must not compact"
    );
    let other = jsonl(&dir);
    let writer = ready(other.open(metadata.clone())).unwrap();
    assert!(matches!(
        repo.read_batch(&metadata, request(Some(page.next_cursor), 1)),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    assert!(matches!(
        repo.read_list(()),
        Err(SessionError::Storage(StorageError::Busy(_)))
    ));
    ready(writer.close()).unwrap();
}
#[test]
fn jsonl_active_compaction_and_legacy_conversion_preserve_semantic_cursor() {
    let dir = Dir::new();
    let repo = jsonl(&dir);
    let path = dir.0.join("legacy.jsonl");
    let source = legacy();
    std::fs::write(&path, &source).unwrap();
    let metadata = repo.read_list(()).unwrap().remove(0);
    let first = repo.read_batch(&metadata, request(None, 1)).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), source);
    let writer = ready(repo.open(metadata.clone())).unwrap();
    let view = writer.view(LaneId::new("main").unwrap());
    let active = repo
        .read_batch(&metadata, request(Some(first.next_cursor.clone()), 1))
        .unwrap();
    assert!(active.complete && !active.reset && active.entries.is_empty());
    assert!(
        ready(view.get_leaf_id()).is_ok(),
        "readonly batch must not revoke borrowed view"
    );
    ready(writer.commit(tx(vec![]))).unwrap();
    assert!(ready(view.get_leaf_id()).is_ok());
    let converted = repo
        .read_batch(&metadata, request(Some(first.next_cursor.clone()), 1))
        .unwrap();
    assert!(!converted.reset && converted.entries.is_empty());
    ready(writer.close()).unwrap();
    let raw = JsonlStorage::open(
        &path,
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
    ready(raw.compact()).unwrap();
    ready(raw.close()).unwrap();
    assert!(
        !repo
            .read_batch(&metadata, request(Some(first.next_cursor.clone()), 1))
            .unwrap()
            .reset
    );
    let bytes = String::from_utf8(std::fs::read(&path).unwrap())
        .unwrap()
        .replace("before", "change");
    std::fs::write(&path, bytes).unwrap();
    assert!(
        repo.read_batch(&metadata, request(Some(first.next_cursor), 1))
            .unwrap()
            .reset
    );
}
#[test]
fn file_domains_and_managed_paths_use_actual_canonical_root() {
    let dir = Dir::new();
    let other = Dir::new();
    let repo = jsonl(&dir);
    assert_eq!(
        repo.source_domain().unwrap(),
        jsonl(&dir).source_domain().unwrap()
    );
    assert_ne!(
        repo.source_domain().unwrap(),
        jsonl(&other).source_domain().unwrap()
    );
    let domain = repo.managed_files().unwrap().unwrap();
    assert!(
        domain
            .contains_path(&dir.0.join("arbitrary.jsonl"))
            .unwrap()
    );
    assert!(!domain.contains_path(&dir.0.join("index.sqlite")).unwrap());
    assert!(
        !domain
            .contains_path(&other.0.join("arbitrary.jsonl"))
            .unwrap()
    );
    std::fs::create_dir(dir.0.join("nested")).unwrap();
    assert!(!domain.contains_path(&dir.0.join("nested/a.jsonl")).unwrap());
    #[cfg(unix)]
    {
        use std::os::unix::{ffi::OsStringExt, fs::symlink};
        let invalid = std::ffi::OsString::from_vec(b"\xff.jsonl".to_vec());
        assert!(domain.contains_path(&dir.0.join(invalid)).unwrap());
        let alias = other.0.join("alias");
        symlink(&dir.0, &alias).unwrap();
        assert_eq!(
            repo.source_domain().unwrap(),
            SourceDomain::for_files("jsonl", &alias).unwrap()
        );
        assert!(domain.contains_path(&alias.join("a.jsonl")).unwrap());
    }
}
#[test]
fn zero_limit_bad_cursor_and_unsupported_source_are_explicit_errors() {
    let dir = Dir::new();
    let repo = jsonl(&dir);
    let root = ready(repo.create(options("x"))).unwrap();
    let metadata = root.metadata();
    assert!(repo.read_batch(&metadata, request(None, 0)).is_err());
    let empty = repo.read_batch(&metadata, request(None, 1)).unwrap();
    assert!(empty.complete && empty.entries.is_empty());
    assert_eq!(empty.next_cursor.last_entry_seq, None);
    let mut bad = empty.next_cursor;
    bad.witness[0] = 255;
    assert!(repo.read_batch(&metadata, request(Some(bad), 1)).is_err());
    ready(root.close()).unwrap();
    let bytes = std::fs::read(&metadata.path).unwrap();
    let mut text = String::from_utf8(bytes).unwrap();
    text = text.replace("\"storageVersion\":1", "\"storageVersion\":2");
    std::fs::write(&metadata.path, text).unwrap();
    assert!(matches!(
        repo.read_batch(&metadata, request(None, 1)),
        Err(SessionError::NewerStorageVersion(_))
    ));
    assert_eq!(repo.read_list(()).unwrap().len(), 1);
}

#[test]
fn actual_jsonl_cursor_zero_max_and_generation_change_are_exact() {
    let dir = Dir::new();
    let codec = Arc::new(
        SessionCodec::new(
            SessionCodecOptions::default(),
            ProviderHeaderPolicy::default(),
        )
        .unwrap(),
    );
    let make = |id: &str, seq| {
        materialize_entry(
            match custom(id, "payload") {
                Write::Entry(v) => v,
                _ => unreachable!(),
            },
            seq,
            n(1),
        )
    };
    let metadata: SessionMetadata = serde_json::from_value(
        serde_json::json!({"id":"max","createdAt":0,"storageVersion":1,"storeGeneration":7}),
    )
    .unwrap();
    let snapshot = StorageSnapshot {
        entries: vec![make("zero", n(0)), make("max", SafeUnsigned::MAX)],
        registers: vec![],
        usage: vec![],
        next_seq: None,
        storage_version: n(1),
        store_generation: n(7),
        stats: SessionStats {
            message_count: n(0),
            usage: Usage::zero(),
        },
    };
    let path = dir.0.join("max.jsonl");
    let raw = JsonlStorage::create_from_snapshot(
        &path,
        metadata,
        snapshot,
        codec,
        Arc::new(Clock),
        JsonlOptions::default(),
    )
    .unwrap();
    ready(raw.close()).unwrap();
    let repo = jsonl(&dir);
    let metadata = repo.read_list(()).unwrap().remove(0);
    let first = repo.read_batch(&metadata, request(None, 1)).unwrap();
    assert_eq!(first.next_cursor.last_entry_seq, Some(n(0)));
    let last = repo
        .read_batch(&metadata, request(Some(first.next_cursor), 1))
        .unwrap();
    assert_eq!(last.next_cursor.last_entry_seq, Some(SafeUnsigned::MAX));
    assert!(last.complete);
    let empty = repo
        .read_batch(&metadata, request(Some(last.next_cursor.clone()), 1))
        .unwrap();
    assert!(empty.entries.is_empty() && empty.complete && !empty.reset);
    let source = String::from_utf8(std::fs::read(&path).unwrap())
        .unwrap()
        .replace("\"storeGeneration\":7", "\"storeGeneration\":8");
    std::fs::write(path, source).unwrap();
    let reset = repo
        .read_batch(&metadata, request(Some(last.next_cursor), 1))
        .unwrap();
    assert!(reset.reset);
    assert_eq!(reset.metadata.store_generation, n(8));
    assert_eq!(entry_structure(&reset.entries[0]).seq, n(0));
}
#[test]
fn readonly_legacy_codec_refusal_changes_no_bytes() {
    let dir = Dir::new();
    let repo = jsonl(&dir);
    let path = dir.0.join("legacy.jsonl");
    let bytes = include_bytes!("../../../conformance/session/legacy-v3/all-v3.jsonl");
    std::fs::write(&path, bytes).unwrap();
    let metadata = repo.read_list(()).unwrap().remove(0);
    assert!(repo.read_batch(&metadata, request(None, 1)).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn empty_prefix_witness_matches_independent_framing_vector() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/session/read/prefix-witness.json"
    ))
    .unwrap();
    let repo = memory(Arc::new(Ids::default()));
    let root = ready(repo.create(options(fixture["id"].as_str().unwrap()))).unwrap();
    let batch = repo.read_batch(&root.metadata(), request(None, 1)).unwrap();
    let hex: String = batch
        .next_cursor
        .witness
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, fixture["witnessHex"].as_str().unwrap());
}
