use super::support::*;
#[test]
fn actual_jsonl_legacy_conversion_preserves_search_progress() {
    let d = Dir::new();
    let directory = d.sub("source");
    let path = directory.join("legacy.jsonl");
    let f = fixture();
    let legacy = &f["legacyHistory"];
    let bytes = legacy["sourceLines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| format!("{}\n", serde_json::to_string(row).unwrap()))
        .collect::<String>()
        .into_bytes();
    std::fs::write(&path, &bytes).unwrap();
    let clock = Arc::new(Clock::default());
    let r = jsonl(&directory, clock.clone());
    let m = r.read_list(()).unwrap().remove(0);
    let direct = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: None,
                limit: n(2),
            },
        )
        .unwrap();
    assert_eq!(
        entry_structure(&direct.entries[0]).id.as_str(),
        legacy["expectedEntryId"].as_str().unwrap()
    );
    assert_eq!(direct.next_cursor.last_entry_seq, Some(n(0)));
    let reader = Observed::new(r.clone());
    let search = service(&d.0.join("index.db"), reader.clone());
    ready(search.sync()).unwrap();
    let initial = normalized(hits(&search, "legacyneedle", None).unwrap());
    assert_eq!(
        initial,
        vec![
            json!({"sessionId":"composition-legacy","entryId":legacy["expectedEntryId"],"timestamp":1000,"snippet":"legacyneedle"})
        ]
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let cursor = reader.take().last().unwrap().result.next_cursor.clone();
    let root = ready(r.open(m.clone())).unwrap();
    let view = root.view(LaneId::new("main").unwrap());
    commit(root.as_ref(), vec![]);
    let converted = std::fs::read(&path).unwrap();
    assert_ne!(converted, bytes);
    let header: Value =
        serde_json::from_slice(converted.split(|b| *b == b'\n').next().unwrap()).unwrap();
    assert_eq!(header["v"], 4);
    let resumed = r
        .read_batch(
            &m,
            ReadBatchRequest {
                cursor: Some(cursor.clone()),
                limit: n(2),
            },
        )
        .unwrap();
    assert!(!resumed.reset);
    assert!(resumed.entries.is_empty() && resumed.complete);
    ready(search.sync()).unwrap();
    let reads = reader.take();
    assert_eq!(reads.first().unwrap().request.as_ref(), Some(&cursor));
    assert!(reads.iter().all(|r| !r.result.reset));
    assert_eq!(
        normalized(hits(&search, "legacyneedle", None).unwrap()),
        initial
    );
    assert!(ready(view.get_leaf_id()).is_ok());
    clock.set(5000);
    commit(
        root.as_ref(),
        vec![message(
            "legacy-new",
            Some(legacy["expectedEntryId"].as_str().unwrap()),
            "legacynewneedle",
        )],
    );
    search.notify(&m.base.id);
    assert_eq!(
        normalized(notify_hits(&search, "legacynewneedle", 1)),
        vec![
            json!({"sessionId":"composition-legacy","entryId":"legacy-new","timestamp":5000,"snippet":"legacynewneedle"})
        ]
    );
    assert!(search.take_background_errors().unwrap().is_empty());
    ready(search.close()).unwrap();
    ready(root.close()).unwrap();
}
