use super::support::*;
fn text_lifecycle<R: Source>(r: Arc<R>, clock: Arc<Clock>, index: &Path) {
    clock.set(42);
    let root = create(r.as_ref(), "text-corpus");
    let view = root.view(LaneId::new("main").unwrap());
    let f = fixture();
    let text = &f["textHistory"];
    for step in text["steps"].as_array().unwrap() {
        clock.set(step["clockMs"].as_u64().unwrap());
        let result =
            ready(root.commit(serde_json::from_value(step["transaction"].clone()).unwrap()))
                .unwrap();
        assert_eq!(
            result.first_seq.get(),
            step["expectedFirstSeq"].as_u64().unwrap()
        );
        assert_eq!(result.timestamp.get(), step["clockMs"].as_u64().unwrap());
    }
    let before = canonical(r.as_ref(), root.as_ref());
    assert_eq!(
        before.entries,
        serde_json::from_value::<Vec<Entry>>(text["expectedEntries"].clone()).unwrap()
    );
    assert_eq!(before.stats.message_count, n(16));
    assert_eq!(
        ready(view.get_leaf_id()).unwrap(),
        Some(eid("branch-summary"))
    );
    let reader = Observed::new(r.clone());
    let search = service(index, reader.clone());
    drop(search.sync());
    assert!(hits(&search, "userneedle", None).unwrap().is_empty());
    assert!(reader.take().is_empty());
    ready(search.sync()).unwrap();
    let reads = reader.take();
    assert!(!reads.is_empty());
    assert!(reads.first().unwrap().request.is_none());
    let cursor = reads.last().unwrap().result.next_cursor.clone();
    assert_eq!(cursor.last_entry_seq, Some(n(38)));
    assert!(reads.last().unwrap().result.complete);
    let check = |search: &SqliteSearchService<Observed<R>>| {
        let mut all = vec![];
        for case in text["queryCases"].as_array().unwrap() {
            let observed =
                normalized(hits(search, case["query"]["text"].as_str().unwrap(), None).unwrap());
            assert_eq!(json!(observed), case["expectedHits"], "{}", case["query"]);
            all.push(observed);
        }
        all
    };
    let expected = check(&search);
    ready(search.sync()).unwrap();
    assert_eq!(canonical(r.as_ref(), root.as_ref()), before);
    ready(search.remove(sid("text-corpus"))).unwrap();
    assert!(hits(&search, "userneedle", None).unwrap().is_empty());
    ready(search.sync()).unwrap();
    assert_eq!(check(&search), expected);
    ready(search.close()).unwrap();
    drop(search);
    reader.take();
    let reopened = service(index, reader.clone());
    ready(reopened.sync()).unwrap();
    let resumed = reader.take();
    assert_eq!(
        resumed.first().unwrap().request.as_ref(),
        Some(&cursor),
        "actual Search must reload its durable cursor"
    );
    assert_eq!(check(&reopened), expected);
    ready(reopened.close()).unwrap();
    drop(reopened);
    remove_index(index);
    let rebuilt = service(index, reader.clone());
    ready(rebuilt.sync()).unwrap();
    assert_eq!(check(&rebuilt), expected);
    assert_eq!(canonical(r.as_ref(), root.as_ref()), before);
    assert!(ready(view.get_leaf_id()).is_ok());
    reader.take();
    clock.set(2100);
    let result = commit(
        root.as_ref(),
        vec![serde_json::from_value(text["allocatorCanary"]["write"].clone()).unwrap()],
    );
    assert_eq!(result.first_seq, n(44));
    let after_write = canonical(r.as_ref(), root.as_ref());
    rebuilt.notify(&sid("text-corpus"));
    let notified = normalized(notify_hits(&rebuilt, "canaryneedle", 1));
    assert_eq!(
        json!(notified),
        json!([text["allocatorCanary"]["expectedSearchHit"]])
    );
    assert!(rebuilt.take_background_errors().unwrap().is_empty());
    ready(rebuilt.sync()).unwrap();
    assert_eq!(
        normalized(hits(&rebuilt, "canaryneedle", None).unwrap()),
        notified
    );
    assert_eq!(canonical(r.as_ref(), root.as_ref()), after_write);
    assert!(reader.take().iter().all(|r| r.id == sid("text-corpus")));
    ready(rebuilt.close()).unwrap();
    ready(root.close()).unwrap();
}
fn rank_grammar<R: Source>(r: Arc<R>, clock: Arc<Clock>, index: &Path) {
    let a = create(r.as_ref(), "sa");
    let b = create(r.as_ref(), "sb");
    clock.set(8000);
    assert_eq!(
        commit(a.as_ref(), vec![message("early", None, "quasar")]).first_seq,
        n(2)
    );
    commit(
        a.as_ref(),
        (0..4)
            .map(|_| register(RegisterNamespace::FactCustom, "gap", Value::Null))
            .collect(),
    );
    clock.set(1000);
    assert_eq!(
        commit(a.as_ref(), vec![message("late", None, "quasar")]).first_seq,
        n(7)
    );
    clock.set(500);
    commit(b.as_ref(), vec![message("only", None, "quasar")]);
    let reader = Observed::new(r.clone());
    let search = service(index, reader.clone());
    ready(search.sync()).unwrap();
    let entry = normalized(hits(&search, "quasar", Some(2)).unwrap());
    assert_eq!(entry,json!([{"sessionId":"sa","entryId":"early","timestamp":8000,"snippet":"quasar"},{"sessionId":"sa","entryId":"late","timestamp":1000,"snippet":"quasar"}]).as_array().unwrap().clone());
    let sessions = ready(search.search_sessions(query("quasar", Some(2)))).unwrap();
    assert_eq!(
        sessions
            .iter()
            .map(|h| (
                h.session_id.as_str(),
                h.top.as_ref().unwrap().entry_id.as_str(),
                h.top.as_ref().unwrap().timestamp.get()
            ))
            .collect::<Vec<_>>(),
        [("sa", "early", 8000), ("sb", "only", 500)]
    );
    let one = ready(search.search_sessions(query("quasar", Some(1)))).unwrap();
    assert_eq!(
        one.iter()
            .map(|h| (
                h.session_id.as_str(),
                h.top.as_ref().unwrap().entry_id.as_str()
            ))
            .collect::<Vec<_>>(),
        [("sa", "early")]
    );
    assert_eq!(one[0].top.as_ref().unwrap().timestamp, n(8000));
    assert_eq!(
        one[0].top.as_ref().unwrap().snippet.as_deref(),
        Some("quasar")
    );
    let grammar = create(r.as_ref(), "grammar");
    let f = fixture();
    for doc in f["grammarHistory"]["documents"].as_array().unwrap() {
        clock.set(doc["expectedOuterTimestamp"].as_u64().unwrap());
        assert_eq!(
            commit(
                grammar.as_ref(),
                vec![
                    message(
                        doc["entryId"].as_str().unwrap(),
                        None,
                        doc["text"].as_str().unwrap()
                    ),
                    register(RegisterNamespace::FactCustom, "gap", Value::Null)
                ]
            )
            .first_seq
            .get(),
            doc["actualEntrySeq"].as_u64().unwrap()
        );
    }
    let mut defaults = vec![];
    for i in 0..26 {
        clock.set(7000 + i);
        let s = create(r.as_ref(), &format!("s{i:02}"));
        commit(s.as_ref(), vec![message("only", None, "defaultneedle")]);
        defaults.push(s);
    }
    ready(search.sync()).unwrap();
    for case in f["grammarHistory"]["cases"].as_array().unwrap() {
        reader.take();
        let mut actual = hits(
            &search,
            case["text"].as_str().unwrap(),
            case["limit"].as_u64(),
        )
        .unwrap()
        .into_iter()
        .map(|h| h.entry_id.to_string())
        .collect::<Vec<_>>();
        let mut expected: Vec<String> =
            serde_json::from_value(case["matchingEntryIds"].clone()).unwrap();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "{case}");
        assert!(
            reader.take().is_empty(),
            "queries never pull canonical source"
        );
    }
    for (limit, count) in [(None, 20), (Some(26), 26)] {
        let entries = hits(&search, "defaultneedle", limit).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|h| h.session_id.to_string())
                .collect::<Vec<_>>(),
            (0..count).map(|i| format!("s{i:02}")).collect::<Vec<_>>()
        );
        assert!(
            entries
                .iter()
                .enumerate()
                .all(|(i, h)| h.timestamp == n(7000 + i as u64))
        );
        let sessions = ready(search.search_sessions(query("defaultneedle", limit))).unwrap();
        assert_eq!(
            sessions
                .iter()
                .map(|h| h.session_id.to_string())
                .collect::<Vec<_>>(),
            (0..count).map(|i| format!("s{i:02}")).collect::<Vec<_>>()
        );
        for (i, hit) in sessions.iter().enumerate() {
            let top = hit.top.as_ref().unwrap();
            assert_eq!(top.entry_id, eid("only"));
            assert_eq!(top.timestamp, n(7000 + i as u64));
            assert_eq!(top.snippet.as_deref(), Some("defaultneedle"));
            assert!(hit.score.as_ref().is_some_and(|s| s.get().is_finite()));
        }
    }
    ready(search.close()).unwrap();
    ready(a.close()).unwrap();
    ready(b.close()).unwrap();
    ready(grammar.close()).unwrap();
    for s in defaults {
        ready(s.close()).unwrap();
    }
}
fn forks_recreation_scope<R: Source>(r: Arc<R>, other: Arc<R>, clock: Arc<Clock>, index: &Path) {
    let source = create(r.as_ref(), "fork-source");
    let view = source.view(LaneId::new("main").unwrap());
    clock.set(2000);
    append(source.as_ref(), "shared", None, "forkneedle");
    clock.set(2001);
    append(source.as_ref(), "main", Some("shared"), "mainneedle");
    clock.set(2002);
    commit(
        source.as_ref(),
        vec![message("off", Some("shared"), "offneedle")],
    );
    let before = canonical(r.as_ref(), source.as_ref());
    let f = fixture();
    let mut forks = vec![];
    for request in f["forkHistory"]["forkRequests"].as_array().unwrap() {
        let s = ready(r.fork(
            source.metadata(),
            SessionForkRequest {
                fork: serde_json::from_value(request["fork"].clone()).unwrap(),
                create: serde_json::from_value(request["create"].clone()).unwrap(),
            },
        ))
        .unwrap();
        let id = s.metadata().base_metadata().id.to_string();
        let got = canonical(r.as_ref(), s.as_ref());
        let expected: &Vec<Value> = f["forkHistory"]["expectedForkEntries"][&id]
            .as_array()
            .unwrap();
        assert_eq!(
            got.entries,
            before
                .entries
                .iter()
                .filter(|e| expected
                    .iter()
                    .any(|id| id.as_str() == Some(entry_structure(e).id.as_str())))
                .cloned()
                .collect::<Vec<_>>()
        );
        forks.push(s);
    }
    let reader = Observed::new(r.clone());
    let search = service(index, reader.clone());
    ready(search.sync()).unwrap();
    for (q, key) in [
        ("forkneedle", "expectedForkneedleHitOrder"),
        ("offneedle", "expectedOffneedleHitOrder"),
    ] {
        let actual = hits(&search, q, None)
            .unwrap()
            .iter()
            .map(|h| json!([h.session_id, h.entry_id, h.timestamp]))
            .collect::<Vec<_>>();
        assert_eq!(json!(actual), f["forkHistory"][key]);
    }
    assert_eq!(canonical(r.as_ref(), source.as_ref()), before);
    assert_eq!(ready(view.get_leaf_id()).unwrap(), Some(eid("main")));
    clock.set(2200);
    assert_eq!(
        commit(
            source.as_ref(),
            vec![message("survives", Some("main"), "survivalneedle")]
        )
        .first_seq,
        n(7)
    );
    let old = create(r.as_ref(), "recreate");
    clock.set(4000);
    append(old.as_ref(), "same", None, "oldneedle");
    ready(search.sync()).unwrap();
    let old_cursor = reader
        .take()
        .into_iter()
        .rev()
        .find(|v| v.id == sid("recreate"))
        .unwrap()
        .result
        .next_cursor;
    let metadata = old.metadata();
    ready(old.close()).unwrap();
    ready(r.delete(metadata)).unwrap();
    let new = create(r.as_ref(), "recreate");
    append(new.as_ref(), "same", None, "recreatedneedle");
    let reset = r
        .read_batch(
            &new.metadata(),
            ReadBatchRequest {
                cursor: Some(old_cursor),
                limit: n(2),
            },
        )
        .unwrap();
    assert!(reset.reset);
    assert_eq!(reset.metadata.store_generation, n(0));
    ready(search.sync()).unwrap();
    assert!(hits(&search, "oldneedle", None).unwrap().is_empty());
    assert_eq!(
        normalized(hits(&search, "recreatedneedle", None).unwrap()),
        vec![
            json!({"sessionId":"recreate","entryId":"same","timestamp":4000,"snippet":"recreatedneedle"})
        ]
    );
    let outsider = create(other.as_ref(), "fork-source");
    commit(
        outsider.as_ref(),
        vec![message("other", None, "outsiderneedle")],
    );
    assert_ne!(r.source_domain().unwrap(), other.source_domain().unwrap());
    assert_eq!(r.source_domain().unwrap(), reader.source_domain().unwrap());
    let scoped_hits = normalized(hits(&search, "forkneedle", None).unwrap());
    let scoped_state = canonical(r.as_ref(), source.as_ref());
    assert!(matches!(
        SqliteSearchService::new(index, other.clone(), ()),
        Err(SearchError::ScopeMismatch)
    ));
    assert_eq!(
        normalized(hits(&search, "forkneedle", None).unwrap()),
        scoped_hits
    );
    assert_eq!(canonical(r.as_ref(), source.as_ref()), scoped_state);
    assert!(hits(&search, "outsiderneedle", None).unwrap().is_empty());
    if let Some(path) = R::canonical_path(&source.metadata()) {
        let path = path.to_owned();
        let before = canonical(r.as_ref(), source.as_ref());
        assert!(matches!(
            SqliteSearchService::new(&path, r.clone(), ()),
            Err(SearchError::ScopeMismatch)
        ));
        let suffix = path.extension().unwrap().to_str().unwrap();
        let fresh = path
            .parent()
            .unwrap()
            .join(format!("unused-index.{suffix}"));
        assert!(!fresh.exists());
        assert!(matches!(
            SqliteSearchService::new(&fresh, r.clone(), ()),
            Err(SearchError::ScopeMismatch)
        ));
        assert!(!fresh.exists());
        assert_eq!(canonical(r.as_ref(), source.as_ref()), before);
    }
    ready(search.close()).unwrap();
    ready(source.close()).unwrap();
    ready(new.close()).unwrap();
    ready(outsider.close()).unwrap();
    for fork in forks {
        ready(fork.close()).unwrap();
    }
}
macro_rules! suite {
    ($text:ident,$rank:ident,$fork:ident,$constructor:ident) => {
        #[test]
        fn $text() {
            let d = Dir::new();
            let clock = Arc::new(Clock::default());
            let r = $constructor(&d.sub("source"), clock.clone());
            text_lifecycle(r, clock, &d.0.join("index.db"));
        }
        #[test]
        fn $rank() {
            let d = Dir::new();
            let clock = Arc::new(Clock::default());
            let r = $constructor(&d.sub("source"), clock.clone());
            rank_grammar(r, clock, &d.0.join("index.db"));
        }
        #[test]
        fn $fork() {
            let d = Dir::new();
            let clock = Arc::new(Clock::default());
            let r = $constructor(&d.sub("source"), clock.clone());
            let other = $constructor(&d.sub("other"), clock.clone());
            forks_recreation_scope(r, other, clock, &d.0.join("index.db"));
        }
    };
}
fn memory_at(_: &Path, clock: Arc<Clock>) -> Arc<MemorySessionRepo> {
    memory(clock)
}
suite!(
    memory_text_lifecycle,
    memory_rank_grammar,
    memory_fork_recreation_scope,
    memory_at
);
suite!(
    jsonl_text_lifecycle,
    jsonl_rank_grammar,
    jsonl_fork_recreation_scope,
    jsonl
);
#[cfg(feature = "sqlite")]
suite!(
    sqlite_text_lifecycle,
    sqlite_rank_grammar,
    sqlite_fork_recreation_scope,
    sqlite
);
