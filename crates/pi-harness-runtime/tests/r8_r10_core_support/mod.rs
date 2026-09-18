use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::{NoopTelemetryContext, register_seq},
    codec::SessionCodec,
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    session::RuntimeSession,
    storage::*,
};
use pi_ai_core::SafeUnsigned;
use pi_harness_core::{
    CurrentRoots, collect_current_references, planner::*, structural,
    validate_current_entries_and_registers,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
pub fn n(x: u64) -> SafeUnsigned {
    SafeUnsigned::new(x).unwrap()
}
pub fn id(s: &str) -> EntryId {
    EntryId::new(s).unwrap()
}
pub fn ready<T>(f: impl std::future::Future<Output = T>) -> T {
    match std::pin::pin!(f)
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(v) => v,
        _ => panic!("actual Memory must finish"),
    }
}
pub struct Time(pub AtomicU64);
impl StorageClock for Time {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(self.0.load(Ordering::SeqCst)))
    }
}
struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        panic!("explicit fixture identities")
    }
}
pub struct Store {
    pub session: RuntimeSession<MemoryStorage>,
    pub storage: Arc<MemoryStorage>,
    pub time: Arc<Time>,
}
impl Store {
    pub fn new() -> Self {
        let time = Arc::new(Time(AtomicU64::new(100)));
        let storage = Arc::new(MemoryBacking::new()).open(time.clone()).unwrap();
        let metadata = serde_json::from_value(
            json!({"id":"session","createdAt":1,"storageVersion":1,"storeGeneration":0}),
        )
        .unwrap();
        let codec = Arc::new(SessionCodec::new(Default::default(), Default::default()).unwrap());
        let session = ready(RuntimeSession::new(
            storage.clone(),
            metadata,
            codec,
            Arc::new(Ids),
            Arc::new(NoopTelemetryContext),
        ))
        .unwrap();
        ready(session.commit(Transaction{writes:vec![Write::Entry(NewEntry::Message(NewMessageEntry{id:id("leaf"),parent_id:None,entry_type:Default::default(),message:serde_json::from_value(json!({"role":"user","content":"input","timestamp":1})).unwrap(),terminate:None})),set(RegisterSet::LaneConfig{key:"main".into(),value:serde_json::from_value(json!({"model":{"provider":"p","modelId":"m"},"thinkingLevel":"off","activeToolNames":[]})).unwrap()}),set(RegisterSet::LaneLeaf{key:"main".into(),value:Some(id("leaf"))}),set(RegisterSet::LaneState{key:"main".into(),value:LaneState{current_operation_id:None,pending_next_run:vec![]}})]})).unwrap();
        Self {
            session,
            storage,
            time,
        }
    }
    pub fn register(&self, ns: RegisterNamespace, key: &str) -> Register {
        ready(self.session.get_register(ns, key)).unwrap().unwrap()
    }
    pub fn accept(&self) {
        self.accept_kind(false)
    }
    pub fn accept_navigation(&self) {
        self.accept_kind(true)
    }
    fn accept_kind(&self, navigation: bool) {
        if navigation {
            ready(self.session.commit(Transaction {
                writes: vec![Write::Entry(NewEntry::Message(NewMessageEntry {
                        id: id("target"),
                        parent_id: None,
                        entry_type: Default::default(),
                        message: serde_json::from_value(
                            json!({"role":"user","content":"target","timestamp":1}),
                        )
                        .unwrap(),
                        terminate: None,
                    }))],
            }))
            .unwrap();
        }

        let Register::LaneState {
            value: lane_state,
            seq: lane_seq,
            ..
        } = self.register(RegisterNamespace::LaneState, "main")
        else {
            panic!()
        };
        let prep=serde_json::from_value(json!({"kind":"compaction","messagesToSummarize":[],"turnPrefixMessages":[],"retainedTail":[],"isSplitTurn":false,"tokensBefore":5,"fileOps":{"read":[],"written":[],"edited":[]},"settings":{"enabled":true,"reserveTokens":10,"keepRecentTokens":10}})).unwrap();
        let prep = if navigation {
            serde_json::from_value(json!({"kind":"branch_summary","messages":[],"fileOps":{"read":[],"written":[],"edited":[]},"totalTokens":0})).unwrap()
        } else {
            prep
        };
        let a = structural::Acceptance {
            operation: Operation {
                operation_id: OperationId::new("op").unwrap(),
                lane: LaneId::new("main").unwrap(),
                source_leaf_id: Some(id("leaf")),
                started_at: n(10),
                intent: if navigation {
                    OperationIntent::Navigation(NavigationIntent {
                        kind: Default::default(),
                        target_id: Some(id("target")),
                        summarize: true,
                        label: Some("selected".into()),
                        custom_instructions: None,
                    })
                } else {
                    OperationIntent::Compaction(CompactionIntent {
                        kind: Default::default(),
                        custom_instructions: None,
                    })
                },
            },
            lane_state,
            lane_seq,
            leaf_seq: Some(register_seq(
                &self.register(RegisterNamespace::LaneLeaf, "main"),
            )),
            source_leaf: Some(id("leaf")),
            configuration_seq: register_seq(&self.register(RegisterNamespace::LaneConfig, "main")),
            settings_revision: n(0),
            identities_present: true,
            task_id: TaskId::new("task").unwrap(),
            preparation: Some(prep),
            target: if navigation {
                ready(self.session.get_entries(&[id("target")]))
                    .unwrap()
                    .remove(&id("target"))
            } else {
                None
            },
            telemetry_context: (),
        };
        assert!(matches!(
            self.publish(structural::acceptance_recipe(&a).unwrap().unwrap()),
            ConditionalOutcome::Applied(_)
        ));
    }
    pub fn current(&self) -> (CurrentOperation, PlannerInputs<()>, CommitFacts) {
        let roots = CurrentRoots {
            lane: LaneId::new("main").unwrap(),
            configuration: self.register(RegisterNamespace::LaneConfig, "main"),
            lane_state: self.register(RegisterNamespace::LaneState, "main"),
            leaf: self.register(RegisterNamespace::LaneLeaf, "main"),
            operation_meta: Some(self.register(RegisterNamespace::OperationMeta, "op")),
            operation_state: Some(self.register(RegisterNamespace::OperationState, "op")),
        };
        let refs = collect_current_references(&roots).unwrap();
        let entries = ready(
            self.session
                .get_entries(&refs.entries.keys().cloned().collect::<Vec<_>>()),
        )
        .unwrap();
        let mut registers = BTreeMap::new();
        for (ns, key) in refs.registers.keys() {
            registers.insert((*ns, key.clone()), self.register(*ns, key));
        }
        let c = validate_current_entries_and_registers(&roots, &entries, &registers)
            .unwrap()
            .unwrap();
        let loaded = entries
            .into_iter()
            .map(|(id, e)| (id.as_str().to_owned(), PlannerLoadedValue::Entry(e)))
            .chain(
                registers
                    .into_iter()
                    .map(|((_, k), r)| (k, PlannerLoadedValue::Register(r))),
            )
            .collect();
        let i = PlannerInputs {
            running: BTreeMap::new(),
            deferred_poll: DeferredPollPermit::None,
            deferred_cancellations: BTreeSet::new(),
            loaded,
            runtime: RuntimeSnapshot {
                settings_revision: n(0),
                stream_options: Default::default(),
                retry_policy: NormalizedRetryPolicy {
                    max_attempts: OneBasedSafeUnsigned::new(3).unwrap(),
                    base_delay_ms: n(1),
                },
            },
            context: None,
            now: n(100),
        };
        let f = CommitFacts {
            session_id: SessionId::new("session").unwrap(),
            leaf_seq: Some(register_seq(&roots.leaf)),
            meta_seq: register_seq(roots.operation_meta.as_ref().unwrap()),
        };
        (c, i, f)
    }
    pub fn publish(&self, r: CommitRecipe<()>) -> ConditionalOutcome {
        ready(self.session.commit_conditional(request(r))).unwrap()
    }
    pub fn pending(&self) -> SummaryEffectPlan<()> {
        let (c, i, f) = self.current();
        let pf = PlanningFacts::Structural(structural::Planning {
            session_id: f.session_id.clone(),
            telemetry_context: (),
            result_entry_id: Some(id("summary")),
        });
        let ProcedureAction::Existing(Action::Dispatch(p)) = next_action(&c, &i, &pf).unwrap()
        else {
            panic!()
        };
        let EffectPlan::Hook(h) = p.effect else {
            panic!()
        };
        let result = if matches!(h.event, HookEvent::BeforeNavigation(_)) {
            HookResult::BeforeNavigation(None)
        } else {
            HookResult::BeforeCompaction(None)
        };
        let out = HookEffectOutput::new(&h, result).unwrap();
        let commit = SettlementCommit {
            current: c.clone(),
            plan: EffectPlan::Hook(h),
            output: SettlementOutput::Hook(out),
            telemetry_context: (),
        };
        let r = apply(
            settlement_recipe(
                &c,
                &commit,
                &i,
                &f,
                &SettlementFacts::Structural(structural::Settlement {
                    result_entry_id: Some(id("summary")),
                    ..Default::default()
                }),
            )
            .unwrap(),
        );
        assert!(matches!(self.publish(r), ConditionalOutcome::Applied(_)));
        let (c, i, f) = self.current();
        let ProcedureAction::Existing(Action::Transition(t)) = next_action(&c, &i, &pf).unwrap()
        else {
            panic!()
        };
        let r = apply(
            pi_harness_core::transition::transition_recipe(
                &c,
                &TransitionCommit {
                    current: c.clone(),
                    next: t.next,
                    telemetry_context: (),
                    expected_configuration_seq: t.expected_configuration_seq,
                    expected_settings_revision: t.expected_settings_revision,
                },
                &i,
                &f,
                &pf,
            )
            .unwrap(),
        );
        let Some(EffectPlan::Summary(p)) = r.dispatch.clone() else {
            panic!()
        };
        assert!(matches!(self.publish(r), ConditionalOutcome::Applied(_)));
        p
    }
}
pub fn set(r: RegisterSet) -> Write {
    Write::SetRegister(Box::new(r))
}
pub fn apply(d: RecipeDecision<()>) -> CommitRecipe<()> {
    match d {
        RecipeDecision::Apply(r) => *r,
        _ => panic!("expected actual recipe"),
    }
}
pub fn request(r: CommitRecipe<()>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: r
            .expected
            .into_iter()
            .map(|c| pi_agent_session::storage::RegisterCondition {
                register: RegisterKey {
                    namespace: c.namespace,
                    key: c.key,
                },
                expected: c
                    .seq
                    .map_or(ExpectedRegister::Absent, ExpectedRegister::Seq),
            })
            .collect(),
        transaction: r.transaction,
    }
}
pub fn response() -> SettledAssistantMessage {
    serde_json::from_value(json!({"role":"assistant","content":[],"api":"a","provider":"p","model":"m","usage":{"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":3,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1})).unwrap()
}
