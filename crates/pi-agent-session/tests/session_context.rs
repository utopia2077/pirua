mod session_support;
use pi_agent_session::context::*;
use session_support::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
fn assistant(reason: &str) -> AgentMessage {
    let mut value = json!({"role":"assistant","content":[],"api":"a","provider":"p","model":"m","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":reason,"timestamp":0});
    if ["error", "aborted"].contains(&reason) {
        value["errorMessage"] = json!("settled failure");
    }
    serde_json::from_value(value).unwrap()
}
struct Project {
    events: Arc<Mutex<Vec<&'static str>>>,
    empty: bool,
}
impl EntryProjector for Project {
    fn project(
        &self,
        _: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.events.lock().unwrap().push("project"); // exercise a genuinely pending callback
            let mut first = true;
            std::future::poll_fn(move |cx| {
                if first {
                    first = false;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                } else {
                    std::task::Poll::Ready(())
                }
            })
            .await;
            Ok(if self.empty {
                None
            } else {
                Some(vec![user("projected")])
            })
        })
    }
}
struct Stages(Arc<Mutex<Vec<&'static str>>>);
impl SummaryRenderer for Stages {
    fn render(
        &self,
        s: ContextSummary,
    ) -> BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push("render");
            Ok(user(&s.text))
        })
    }
}
impl AgentContextTransform for Stages {
    fn transform<'a>(
        &'a self,
        mut m: Vec<AgentMessage>,
        _: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Vec<AgentMessage>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push("transform");
            m.push(user("transformed"));
            Ok(m)
        })
    }
}
impl ProviderMessageConverter for Stages {
    fn convert(
        &self,
        m: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<Message>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push("convert");
            Ok(m.into_iter()
                .map(|m| match m {
                    AgentMessage::BuiltIn(m) => m,
                    _ => panic!(),
                })
                .collect())
        })
    }
}
struct Cancel;
impl Cancellation for Cancel {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}
#[test]
fn bounded_compaction_tail_filters_and_explicit_callback_order() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let p = Arc::new(Probe::new(raw.clone()));
    let s = session(p.clone(), codec());
    // Raw earlier payload is a role unavailable to this codec. A scan past the newest
    // compaction would fail decoding, making the no-earlier-hydration check causal.
    let old:Write=serde_json::from_value(json!({"kind":"entry","entry":{"id":"old","parentId":"prefix","type":"message","message":{"role":"unknown","text":"earlier"}}})).unwrap();
    ready(raw.commit(tx(vec![custom_entry("prefix", None), old]))).unwrap();
    let tail = vec![
        user("tail"),
        assistant("error"),
        assistant("aborted"),
        assistant("deferred"),
        assistant("length"),
    ];
    let comp:NewEntry=serde_json::from_value(json!({"id":"cp","parentId":"old","type":"compaction","summary":"summary","retainedTail":tail,"tokensBefore":99,"fromHook":false})).unwrap();
    let branch:NewEntry=serde_json::from_value(json!({"id":"bs","parentId":"cp","type":"branch_summary","fromId":"cp","summary":"branch","fromHook":false})).unwrap();
    ready(s.commit(tx(vec![
        Write::Entry(comp),
        Write::Entry(branch),
        custom_entry("custom", Some("bs")),
        set(RegisterSet::LaneLeaf {
            key: "main".into(),
            value: Some(eid("custom")),
        }),
    ])))
    .unwrap();
    let events = Arc::new(Mutex::new(vec![]));
    let projector = Project {
        events: events.clone(),
        empty: false,
    };
    let projectors: BTreeMap<_, &dyn EntryProjector> =
        [(ct("state"), &projector as &dyn EntryProjector)].into();
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/session/context-contract.json"
    ))
    .unwrap();
    let items = ready(s.context_items(&projectors)).unwrap();
    assert_eq!(
        items.len(),
        contract["structuralItems"].as_u64().unwrap() as usize
    );
    assert!(
        matches!(&items[0],ContextItem::Summary(s)if s.kind==SummaryKind::Compaction&&s.source_id==eid("cp")&&s.text=="summary")
    );
    assert!(matches!(&items[3],ContextItem::Summary(s)if s.kind==SummaryKind::BranchSummary));
    let queries = p.branches.lock().unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].start, Some(eid("custom")));
    assert_eq!(queries[0].stop_at_type, Some(EntryType::Compaction));
    drop(queries);
    let stages = Stages(events.clone());
    let out = ready(materialize(items, &stages, Some(&stages), &stages, &Cancel)).unwrap();
    assert_eq!(
        out.len(),
        contract["providerMessages"].as_u64().unwrap() as usize
    );
    assert_eq!(
        serde_json::to_value(&*events.lock().unwrap()).unwrap(),
        contract["callbackOrder"]
    );
    assert_eq!(ready(s.context_items(&BTreeMap::new())).unwrap().len(), 4);
    let empty = Project {
        events,
        empty: true,
    };
    assert_eq!(
        ready(s.context_items(&[(ct("state"), &empty as &dyn EntryProjector)].into()))
            .unwrap()
            .len(),
        4
    );
    // Custom state queries explicitly do not stop at compaction.
    let all_custom = ready(s.find_entries_on_branch(BranchScan {
        entry_type: Some(EntryType::Custom),
        ..Default::default()
    }))
    .unwrap();
    assert_eq!(all_custom.len(), 2);
    assert_eq!(
        serde_json::to_value(&all_custom[1]).unwrap()["id"],
        "prefix"
    );
}
#[test]
fn root_context_and_typed_application_failure() {
    let (_, _, s) = fresh();
    assert!(ready(s.context_items(&BTreeMap::new())).unwrap().is_empty());
    struct Failure;
    impl SummaryRenderer for Failure {
        fn render(
            &self,
            _: ContextSummary,
        ) -> BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>> {
            Box::pin(async {
                Err(HarnessRuntimeError::Fault(HarnessFault::new(
                    ErrorContext::new("application failure").unwrap(),
                )))
            })
        }
    }
    let items = vec![ContextItem::Summary(ContextSummary {
        kind: SummaryKind::Compaction,
        source_id: eid("cp"),
        timestamp: n(0),
        text: "secret".into(),
    })];
    let stages = Stages(Arc::new(Mutex::new(vec![])));
    let error = ready(materialize(items, &Failure, None, &stages, &Cancel)).unwrap_err();
    assert!(matches!(error, ContextError::Application(_)));
    assert!(!format!("{error:?} {error}").contains("secret"));
    assert!(stages.0.lock().unwrap().is_empty());
}
