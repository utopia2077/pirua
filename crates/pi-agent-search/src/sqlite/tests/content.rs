use super::support::*;
#[test]
fn actual_memory_six_methods_first_poll_and_borrowed_writer_survive() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "m");
    let view = root.view(LaneId::new("main").unwrap());
    append(root.as_ref(), "e", "memoryneedle");
    let before = r
        .read_batch(
            &root.metadata(),
            ReadBatchRequest {
                cursor: None,
                limit: n(100),
            },
        )
        .unwrap();
    let s = SqliteSearchService::with_options(
        d.0.join("index.db"),
        r.clone(),
        (),
        SearchOptions {
            debounce: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    drop(s.sync());
    assert!(hits(&s, "memoryneedle").is_empty());
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "memoryneedle"), ["e"]);
    let result = ready(s.search_sessions(query("memoryneedle", None))).unwrap();
    assert_eq!(result[0].session_id, sid("m"));
    assert_eq!(result[0].top.as_ref().unwrap().timestamp, n(1234));
    ready(s.sync()).unwrap();
    assert_eq!(s.inner.progress(Some(&sid("m"))).unwrap().epoch, 1);
    s.notify(&sid("unknown"));
    ready(s.remove(sid("m"))).unwrap();
    assert!(hits(&s, "memoryneedle").is_empty());
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "memoryneedle"), ["e"]);
    ready(s.close()).unwrap();
    ready(s.close()).unwrap();
    assert!(matches!(ready(s.sync()), Err(SearchError::Closed)));
    assert!(matches!(
        ready(s.search_entries(query("", Some(0))).unwrap()),
        Err(SearchError::Closed)
    ));
    assert_eq!(
        before,
        r.read_batch(
            &root.metadata(),
            ReadBatchRequest {
                cursor: None,
                limit: n(100)
            }
        )
        .unwrap()
    );
    assert!(ready(view.get_leaf_id()).is_ok());
    append(root.as_ref(), "after", "still writable");
}
#[test]
fn frozen_actual_jsonl_text_status_branches_and_default_exclusions() {
    let d = Dir::new();
    let path = d.0.join("source.jsonl");
    let bytes = include_bytes!("../../../../../conformance/search/text-corpus.jsonl");
    std::fs::write(&path, bytes).unwrap();
    let r = jsonl(&d.0);
    let s = SqliteSearchService::with_options(
        d.0.join("index.db"),
        r.clone(),
        (),
        SearchOptions {
            batch_size: n(2),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    ready(s.sync()).unwrap();
    let data = corpus();
    let text = &data["textCorpus"];
    for (q, expected) in text["defaultPositiveQueries"].as_object().unwrap() {
        let values: Vec<_> = expected
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(hits(&s, q), values, "{q}");
    }
    for q in text["defaultNegativeQueries"].as_array().unwrap() {
        assert!(hits(&s, q.as_str().unwrap()).is_empty(), "{q}");
    }
    let cursor = s
        .inner
        .progress(Some(&sid("text-corpus")))
        .unwrap()
        .cursor
        .unwrap();
    assert_eq!(cursor.last_entry_seq, Some(n(36)));
    let results = ready(s.search_entries(query("statuserror", None)).unwrap()).unwrap();
    assert_eq!(results[0].timestamp, n(1008));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    ready(s.close()).unwrap();
    let writer = ready(r.open(r.read_list(()).unwrap().remove(0))).unwrap();
    assert!(ready(writer.get_leaf_id()).is_ok());
    ready(writer.close()).unwrap();
}
struct Output;
impl TextProjector for Output {
    fn identity(&self) -> &str {
        "review-output-v1"
    }
    fn project(&self, entry: &Entry) -> Result<Option<String>, SearchError> {
        if let Entry::Message(e) = entry {
            let v = serde_json::to_value(&e.message).unwrap();
            if v["role"] == "bashExecution" {
                return Ok(v["output"].as_str().map(str::to_owned));
            }
        }
        DefaultTextProjector.project(entry)
    }
}
#[test]
fn explicit_custom_projection_is_bound_and_refuses_mismatch_before_mutation() {
    let d = Dir::new();
    std::fs::write(
        d.0.join("source.jsonl"),
        include_bytes!("../../../../../conformance/search/text-corpus.jsonl"),
    )
    .unwrap();
    let r = jsonl(&d.0);
    let path = d.0.join("index.db");
    let s = SqliteSearchService::with_options(
        &path,
        r.clone(),
        (),
        SearchOptions::default(),
        Arc::new(Output),
    )
    .unwrap();
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "customoutputneedle"), ["bash-output"]);
    ready(s.close()).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        SqliteSearchService::new(&path, r, ()),
        Err(SearchError::ScopeMismatch)
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
#[test]
fn frozen_literal_and_unicode_queries_do_not_enable_raw_operators() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "grammar");
    let data = corpus();
    let grammar = &data["grammarCorpus"];
    for doc in grammar["documents"].as_array().unwrap() {
        append(
            root.as_ref(),
            doc["entryId"].as_str().unwrap(),
            doc["text"].as_str().unwrap(),
        );
    }
    let s = SqliteSearchService::new(d.0.join("index.db"), r, ()).unwrap();
    ready(s.sync()).unwrap();
    for case in grammar["cases"].as_array().unwrap() {
        if let Some(expected) = case["matchingEntryIds"].as_array() {
            let mut got: Vec<_> = ready(
                s.search_entries(query(
                    case["text"].as_str().unwrap(),
                    case["limit"].as_u64(),
                ))
                .unwrap(),
            )
            .unwrap()
            .into_iter()
            .map(|h| h.entry_id.as_str().to_owned())
            .collect();
            got.sort();
            let mut want: Vec<_> = expected
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            want.sort();
            assert_eq!(got, want, "{}", case["text"]);
        }
    }
    assert!(hits(&s, "\"\"\" ; DROP TABLE search_documents").is_empty());
    ready(s.close()).unwrap();
}
#[test]
fn frozen_rank_top_per_session_precedes_limit_and_default_is_twenty() {
    let d = Dir::new();
    let data = corpus();
    let docs = data["rankingCorpus"]["documents"].as_array().unwrap();
    for id in ["sa", "sb"] {
        let entries = docs
            .iter()
            .filter(|v| v["sessionId"] == id)
            .map(|v| {
                let mut e = entry(
                    v["entryId"].as_str().unwrap(),
                    v["seq"].as_u64().unwrap(),
                    v["text"].as_str().unwrap(),
                );
                e["timestamp"] = v["timestamp"].clone();
                e
            })
            .collect();
        file(&d.0.join(format!("{id}.jsonl")), id, 0, entries);
    }
    for i in 0..26 {
        let id = format!("s{i:02}");
        file(
            &d.0.join(format!("{id}.jsonl")),
            &id,
            0,
            vec![entry("only", 2, "defaultneedle")],
        );
    }
    let s = SqliteSearchService::new(d.0.join("index.db"), jsonl(&d.0), ()).unwrap();
    ready(s.sync()).unwrap();
    let e = ready(s.search_entries(query("quasar", Some(2))).unwrap()).unwrap();
    assert_eq!(
        e.iter().map(|e| e.entry_id.as_str()).collect::<Vec<_>>(),
        ["early", "late"]
    );
    let sessions = ready(s.search_sessions(query("quasar", Some(2)))).unwrap();
    assert_eq!(
        sessions
            .iter()
            .map(|h| h.session_id.as_str())
            .collect::<Vec<_>>(),
        ["sa", "sb"]
    );
    assert_eq!(sessions[0].top.as_ref().unwrap().entry_id.as_str(), "early");
    assert_eq!(
        ready(s.search_sessions(query("quasar", Some(1))))
            .unwrap()
            .len(),
        1
    );
    let defaults = ready(s.search_sessions(query("defaultneedle", None))).unwrap();
    assert_eq!(defaults.len(), 20);
    assert_eq!(defaults[19].session_id.as_str(), "s19");
    assert_eq!(
        ready(s.search_sessions(query("defaultneedle", Some(26))))
            .unwrap()
            .len(),
        26
    );
    ready(s.close()).unwrap();
}
#[test]
fn durable_actual_nonmessage_zero_max_and_empty_cursors() {
    let d = Dir::new();
    let path = d.0.join("progress.jsonl");
    let bytes = include_bytes!("../../../../../conformance/search/progress-corpus.jsonl");
    std::fs::write(&path, bytes).unwrap();
    file(&d.0.join("empty.jsonl"), "empty", 0, vec![]);
    let r = jsonl(&d.0);
    let db = d.0.join("index.db");
    let s = SqliteSearchService::with_options(
        &db,
        r.clone(),
        (),
        SearchOptions {
            batch_size: n(1),
            ..Default::default()
        },
        Arc::new(DefaultTextProjector),
    )
    .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let db_copy = db.clone();
    hook(&s, move |phase| {
        if phase == "after_commit" {
            let c = rusqlite::Connection::open(&db_copy).unwrap();
            let bytes: Vec<u8> = c
                .query_row(
                    "SELECT cursor FROM search_cursors WHERE session='progress'",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or_default();
            if !bytes.is_empty() {
                let c: ReadCursor = serde_json::from_slice(&bytes).unwrap();
                observed
                    .lock()
                    .unwrap()
                    .push(c.last_entry_seq.unwrap().get());
            }
        }
    });
    ready(s.sync()).unwrap();
    assert_eq!(*seen.lock().unwrap(), [0, 7, 9007199254740991]);
    assert_eq!(hits(&s, "progressneedle"), ["m7"]);
    assert_eq!(
        s.inner
            .progress(Some(&sid("empty")))
            .unwrap()
            .cursor
            .unwrap()
            .last_entry_seq,
        None
    );
    let cursor = s
        .inner
        .progress(Some(&sid("progress")))
        .unwrap()
        .cursor
        .unwrap();
    let actual = r
        .read_batch(
            &r.read_list(())
                .unwrap()
                .into_iter()
                .find(|m| m.base.id == sid("progress"))
                .unwrap(),
            ReadBatchRequest {
                cursor: Some(cursor.clone()),
                limit: n(1),
            },
        )
        .unwrap();
    assert_eq!(actual.next_cursor, cursor);
    clear_hook(&s);
    ready(s.close()).unwrap();
    let s = SqliteSearchService::new(&db, r, ()).unwrap();
    ready(s.sync()).unwrap();
    assert_eq!(
        s.inner.progress(Some(&sid("progress"))).unwrap().cursor,
        Some(cursor)
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    ready(s.close()).unwrap();
}
#[test]
fn changed_identical_truncated_prefix_and_generation_reset_actual_jsonl() {
    let d = Dir::new();
    let path = d.0.join("source.jsonl");
    file(&path, "same", 0, vec![entry("old", 0, "oldneedle")]);
    let r = jsonl(&d.0);
    let s = SqliteSearchService::new(d.0.join("index.db"), r, ()).unwrap();
    ready(s.sync()).unwrap();
    let first = s.inner.progress(Some(&sid("same"))).unwrap().cursor;
    file(
        &path,
        "same",
        0,
        vec![entry("old", 0, "oldneedle"), entry("later", 5, "newneedle")],
    );
    ready(s.sync()).unwrap();
    assert_eq!(hits(&s, "oldneedle"), ["old"]);
    assert_eq!(hits(&s, "newneedle"), ["later"]);
    file(
        &path,
        "same",
        0,
        vec![entry("recreated", 0, "recreatedneedle")],
    );
    ready(s.sync()).unwrap();
    assert!(hits(&s, "oldneedle").is_empty());
    assert!(hits(&s, "newneedle").is_empty());
    assert_eq!(hits(&s, "recreatedneedle"), ["recreated"]);
    assert_ne!(s.inner.progress(Some(&sid("same"))).unwrap().cursor, first);
    file(
        &path,
        "same",
        1,
        vec![entry("rewrite", 0, "rewrittenneedle")],
    );
    ready(s.sync()).unwrap();
    assert_eq!(
        s.inner
            .progress(Some(&sid("same")))
            .unwrap()
            .cursor
            .unwrap()
            .generation,
        n(1)
    );
    assert!(hits(&s, "recreatedneedle").is_empty());
    file(&path, "same", 1, vec![]);
    ready(s.sync()).unwrap();
    assert!(hits(&s, "rewrittenneedle").is_empty());
    ready(s.close()).unwrap();
}
#[test]
fn derived_delete_rebuild_preserves_actual_source_and_normalized_hits() {
    let d = Dir::new();
    let p = d.0.join("source.jsonl");
    file(
        &p,
        "s",
        0,
        vec![entry("a", 0, "sharedneedle"), entry("b", 4, "sharedneedle")],
    );
    let before = std::fs::read(&p).unwrap();
    let r = jsonl(&d.0);
    let db = d.0.join("derived.db");
    let s = SqliteSearchService::new(&db, r.clone(), ()).unwrap();
    ready(s.sync()).unwrap();
    let expected = ready(s.search_entries(query("sharedneedle", None)).unwrap()).unwrap();
    ready(s.close()).unwrap();
    drop(s);
    std::fs::remove_file(&db).unwrap();
    let rebuilt = SqliteSearchService::new(db, r, ()).unwrap();
    ready(rebuilt.sync()).unwrap();
    assert_eq!(
        ready(rebuilt.search_entries(query("sharedneedle", None)).unwrap()).unwrap(),
        expected
    );
    assert_eq!(std::fs::read(p).unwrap(), before);
    ready(rebuilt.close()).unwrap();
}
#[test]
fn scope_canonical_alias_and_nonindex_refuse_without_mutation() {
    let d = Dir::new();
    let other = Dir::new();
    let r = jsonl(&d.0);
    let path = d.0.join("reserved.jsonl");
    assert!(SqliteSearchService::new(&path, r.clone(), ()).is_err());
    assert!(!path.exists());
    file(&path, "canonical", 0, vec![]);
    let before = std::fs::read(&path).unwrap();
    #[cfg(unix)]
    {
        let alias = other.0.join("alias.db");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(SqliteSearchService::new(&alias, r.clone(), ()).is_err());
    }
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let db = d.0.join("derived.db");
    let s = SqliteSearchService::new(&db, r, ()).unwrap();
    ready(s.close()).unwrap();
    let before = std::fs::read(&db).unwrap();
    assert!(matches!(
        SqliteSearchService::new(&db, jsonl(&other.0), ()),
        Err(SearchError::ScopeMismatch)
    ));
    assert_eq!(std::fs::read(&db).unwrap(), before);
    let foreign = d.0.join("foreign.db");
    let c = rusqlite::Connection::open(&foreign).unwrap();
    c.execute("CREATE TABLE canonical(value)", []).unwrap();
    drop(c);
    let before = std::fs::read(&foreign).unwrap();
    assert!(SqliteSearchService::new(&foreign, memory(), ()).is_err());
    assert_eq!(std::fs::read(foreign).unwrap(), before);
    assert!(SqliteSearchService::new(d.0.join("missing/derived.db"), memory(), ()).is_err());
}
#[test]
fn empty_existing_sqlite_database_can_initialize_and_memory_domain_cannot_change() {
    let d = Dir::new();
    let db = d.0.join("empty.db");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch("PRAGMA user_version=0").unwrap();
    drop(c);
    assert!(std::fs::metadata(&db).unwrap().len() > 0);
    let r = memory();
    let s = SqliteSearchService::new(&db, r.clone(), ()).unwrap();
    ready(s.close()).unwrap();
    let before = std::fs::read(&db).unwrap();
    assert!(matches!(
        SqliteSearchService::new(&db, memory(), ()),
        Err(SearchError::ScopeMismatch)
    ));
    assert_eq!(std::fs::read(&db).unwrap(), before);
    let s = SqliteSearchService::new(&db, r, ()).unwrap();
    ready(s.close()).unwrap();
}
#[test]
fn actual_memory_same_id_recreation_replaces_changed_prefix_without_closing_views() {
    let d = Dir::new();
    let r = memory();
    let root = create(r.as_ref(), "same");
    append(root.as_ref(), "old", "oldneedle");
    let s = SqliteSearchService::new(d.0.join("index.db"), r.clone(), ()).unwrap();
    ready(s.sync()).unwrap();
    ready(r.delete(root.metadata())).unwrap();
    let fresh = create(r.as_ref(), "same");
    append(fresh.as_ref(), "new", "newneedle");
    let view = fresh.view(LaneId::new("main").unwrap());
    ready(s.sync()).unwrap();
    assert!(hits(&s, "oldneedle").is_empty());
    assert_eq!(hits(&s, "newneedle"), ["new"]);
    assert!(ready(view.get_leaf_id()).is_ok());
    ready(s.close()).unwrap();
    append(fresh.as_ref(), "after", "still writable");
}
#[test]
fn real_legacy_read_and_first_write_conversion_leave_search_cursor_valid() {
    let d = Dir::new();
    let path = d.0.join("legacy.jsonl");
    let bytes=b"{\"type\":\"session\",\"version\":3,\"id\":\"legacy\",\"timestamp\":\"1970-01-01T00:00:00Z\",\"cwd\":\"source\"}\n{\"type\":\"message\",\"id\":\"old\",\"parentId\":null,\"timestamp\":\"1970-01-01T00:00:00.001Z\",\"message\":{\"role\":\"user\",\"content\":\"legacyneedle\",\"timestamp\":1}}\n";
    std::fs::write(&path, bytes).unwrap();
    let r = jsonl(&d.0);
    let s = SqliteSearchService::new(d.0.join("index.db"), r.clone(), ()).unwrap();
    ready(s.sync()).unwrap();
    let ids = hits(&s, "legacyneedle");
    assert_eq!(ids.len(), 1);
    let cursor = s.inner.progress(Some(&sid("legacy"))).unwrap().cursor;
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let root = ready(r.open(r.read_list(()).unwrap().remove(0))).unwrap();
    ready(root.commit(Transaction { writes: vec![] })).unwrap();
    ready(s.sync()).unwrap();
    assert_eq!(
        s.inner.progress(Some(&sid("legacy"))).unwrap().cursor,
        cursor
    );
    assert_eq!(hits(&s, "legacyneedle"), ids);
    assert!(ready(root.get_leaf_id()).is_ok());
    ready(s.close()).unwrap();
    ready(root.close()).unwrap();
}
