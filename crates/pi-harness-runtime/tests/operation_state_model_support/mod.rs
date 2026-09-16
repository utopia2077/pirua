//! Test-only model: real Session publication, detached reads, and a small ledger oracle.
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::NoopTelemetryContext,
    codec::SessionCodec,
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    session::RuntimeSession,
    storage::{
        AtomicStorage, ConditionalCommit, ConditionalOutcome, ExpectedRegister, RegisterKey,
        StorageSnapshot,
    },
};
use pi_harness_core::{
    CurrentRoots, collect_current_references, planner::*, terminal::*, transition::*,
    validate_current_entries_and_registers,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
pub fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    match std::pin::pin!(future)
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(value) => value,
        _ => panic!("Memory operation unexpectedly parked"),
    }
}
struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(100))
    }
}
struct Ids;
impl IdGenerator for Ids {
    fn next(&self, _: Option<SafeUnsigned>) -> String {
        panic!("model supplies every fresh ID explicitly")
    }
}
pub fn configuration() -> LaneConfiguration {
    serde_json::from_value(json!({"model":{"provider":"model-provider","modelId":"model"},"thinkingLevel":"off","activeToolNames":["tool"]})).unwrap()
}
pub fn settings() -> RunSettings {
    serde_json::from_value(json!({"compaction":{"enabled":false,"reserveTokens":0,"keepRecentTokens":0},"steeringMode":"all","followUpMode":"all","toolExecution":"parallel"})).unwrap()
}
pub fn user() -> AgentMessage {
    serde_json::from_value(json!({"role":"user","content":"model input","timestamp":1})).unwrap()
}
pub fn assistant(reason: &str, calls: bool) -> SettledAssistantMessage {
    let mut content = vec![];
    if calls {
        for i in 0..3 {
            content.push(json!({"type":"text","text":"separator"}));
            content.push(
                json!({"type":"toolCall","id":format!("call-{i}"),"name":"tool","arguments":{}}),
            );
        }
    }
    let mut value = json!({"role":"assistant","content":content,"api":"explicit-fixture-api","provider":"model-provider","model":"model","usage":{"input":0,"output":10,"cacheRead":0,"cacheWrite":0,"totalTokens":10,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":reason,"timestamp":100});
    if reason == "error" {
        value["errorMessage"] = json!("retry fixture")
    };
    serde_json::from_value(value).unwrap()
}
pub fn tool_result(usage: bool) -> AgentToolResult {
    AgentToolResult {
        content: vec![],
        details: JsonValue::Null,
        usage: usage.then(Usage::zero),
        added_tool_names: None,
        terminate: None,
    }
}
pub fn apply(decision: RecipeDecision<()>) -> CommitRecipe<()> {
    match decision {
        RecipeDecision::Apply(recipe) => *recipe,
        other => panic!("expected recipe: {other:?}"),
    }
}
pub fn request(recipe: CommitRecipe<()>) -> ConditionalCommit {
    ConditionalCommit {
        conditions: recipe
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
        transaction: recipe.transaction,
    }
}

/// Deliberately not a second transition implementation: only committed identities,
/// ordered source completions, reserved-pair budget, and terminal ownership.
#[derive(Default)]
pub struct Reference {
    pub entries: Vec<String>,
    pub usage: BTreeMap<String, String>,
    pub args: BTreeMap<String, JsonObject>,
    pub attempts: Vec<u64>,
    pub tool_order: Vec<usize>,
    pub terminal: bool,
}
#[derive(Clone, Copy, Debug)]
pub enum End {
    Assistant,
    Tools,
    Failed,
}
pub struct Model {
    pub session: RuntimeSession<MemoryStorage>,
    pub storage: Arc<MemoryStorage>,
    pub trace: Vec<String>,
    pub reference: Reference,
    pub now: SafeUnsigned,
    pub running: BTreeMap<EffectKey, EffectPlan<()>>,
}
impl Model {
    pub fn new() -> Self {
        let storage = Arc::new(MemoryBacking::new())
            .open(Arc::new(Clock))
            .unwrap();
        let codec = Arc::new(SessionCodec::new(Default::default(), Default::default()).unwrap());
        let metadata = serde_json::from_value(
            json!({"id":"model-session","createdAt":1,"storageVersion":1,"storeGeneration":0}),
        )
        .unwrap();
        let session = ready(RuntimeSession::new(
            storage.clone(),
            metadata,
            codec,
            Arc::new(Ids),
            Arc::new(NoopTelemetryContext),
        ))
        .unwrap();
        ready(session.commit(Transaction {
            writes: vec![
                set(RegisterSet::LaneConfig {
                    key: "main".into(),
                    value: configuration(),
                }),
                set(RegisterSet::LaneLeaf {
                    key: "main".into(),
                    value: None,
                }),
                set(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                }),
            ],
        }))
        .unwrap();
        let mut model = Self {
            session,
            storage,
            trace: vec!["configured empty Session".into()],
            reference: Default::default(),
            now: n(100),
            running: BTreeMap::new(),
        };
        model.accept();
        model
    }
    pub fn snapshot(&self) -> StorageSnapshot {
        ready(self.storage.snapshot()).unwrap()
    }
    pub fn register(&self, namespace: RegisterNamespace, key: &str) -> Option<Register> {
        ready(self.session.get_register(namespace, key)).unwrap()
    }
    pub fn roots(&self) -> CurrentRoots {
        let state = self.register(RegisterNamespace::LaneState, "main").unwrap();
        let Register::LaneState { value, .. } = &state else {
            panic!()
        };
        let id = value.current_operation_id.as_ref();
        CurrentRoots {
            lane: LaneId::new("main").unwrap(),
            configuration: self
                .register(RegisterNamespace::LaneConfig, "main")
                .unwrap(),
            lane_state: state.clone(),
            leaf: self.register(RegisterNamespace::LaneLeaf, "main").unwrap(),
            operation_meta: id
                .and_then(|id| self.register(RegisterNamespace::OperationMeta, id.as_str())),
            operation_state: id
                .and_then(|id| self.register(RegisterNamespace::OperationState, id.as_str())),
        }
    }
    pub fn load(&self) -> (Option<CurrentOperation>, PlannerInputs<()>, CommitFacts) {
        let roots = self.roots();
        let refs = collect_current_references(&roots).unwrap();
        let ids: Vec<_> = refs.entries.keys().cloned().collect();
        let entries = ready(self.session.get_entries(&ids)).unwrap();
        let mut registers = BTreeMap::new();
        for (namespace, key) in refs.registers.keys() {
            if let Some(value) = self.register(*namespace, key) {
                registers.insert((*namespace, key.clone()), value);
            }
        }
        let current = validate_current_entries_and_registers(&roots, &entries, &registers).unwrap();
        let mut loaded: BTreeMap<_, _> = entries
            .into_iter()
            .map(|(id, e)| (id.as_str().to_owned(), PlannerLoadedValue::Entry(e)))
            .chain(
                registers
                    .into_iter()
                    .map(|((_, key), r)| (key, PlannerLoadedValue::Register(r))),
            )
            .collect();
        if let Some(CurrentOperation {
            state: OperationState::Run(run),
            operation,
            ..
        }) = &current
            && let RunPhase::Tools(p) = &run.phase
        {
            for c in &p.batch.calls {
                let key = format!(
                    "{}:{}:{}",
                    operation.operation_id.as_str(),
                    p.batch.turn_id.as_str(),
                    pi_harness_core::tool_batch::source(c).0.get()
                );
                if let Some(value) = self.register(RegisterNamespace::OperationToolArguments, &key)
                {
                    loaded.insert(key, PlannerLoadedValue::Register(value));
                }
            }
        }
        let leaf_seq = match roots.leaf {
            Register::LaneLeaf { seq, .. } => Some(seq),
            _ => panic!(),
        };
        let meta_seq = match roots.operation_meta {
            Some(Register::OperationMeta { seq, .. }) => seq,
            None => n(0),
            _ => panic!(),
        };
        (
            current,
            PlannerInputs {
                running: self.running.clone(),
                deferred_poll: DeferredPollPermit::None,
                deferred_cancellations: BTreeSet::new(),
                loaded,
                runtime: RuntimeSnapshot {
                    settings_revision: n(0),
                    stream_options: Default::default(),
                    retry_policy: NormalizedRetryPolicy {
                        max_attempts: OneBasedSafeUnsigned::new(3).unwrap(),
                        base_delay_ms: n(5),
                    },
                },
                context: None,
                now: self.now,
            },
            CommitFacts {
                session_id: SessionId::new("model-session").unwrap(),
                leaf_seq,
                meta_seq,
            },
        )
    }
    pub fn current(&self) -> (CurrentOperation, PlannerInputs<()>, CommitFacts) {
        let (c, i, f) = self.load();
        (c.unwrap(), i, f)
    }
    pub fn check(&self) {
        let snapshot = self.snapshot();
        self.session.codec().validate_snapshot(&snapshot).unwrap();
        let (current, _, _) = self.load();
        let entries: Vec<_> = snapshot
            .entries
            .iter()
            .map(|e| {
                serde_json::to_value(e).unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        assert_eq!(
            entries, self.reference.entries,
            "committed identity/order oracle"
        );
        for (position, entry) in snapshot.entries.iter().enumerate() {
            let value = serde_json::to_value(entry).unwrap();
            let expected_parent = position
                .checked_sub(1)
                .map(|index| self.reference.entries[index].as_str());
            assert_eq!(
                value["parentId"],
                json!(expected_parent),
                "each modeled append extends the previously committed leaf"
            );
        }
        if let Some(CurrentOperation {
            state: OperationState::Run(run),
            ..
        }) = &current
            && let RunPhase::Assistant(AssistantPhase {
                generation:
                    Generation::EffectPending {
                        response_entry_id,
                        usage_id,
                        attempt,
                        ..
                    },
                ..
            }) = &run.phase
        {
            assert!(
                !entries.iter().any(|id| id == response_entry_id.as_str()),
                "intent cannot materialize a response early"
            );
            assert!(
                !snapshot.usage.iter().any(|row| &row.id == usage_id),
                "unsettled intent cannot bill usage"
            );
            assert_eq!(Some(attempt.get()), self.reference.attempts.last().copied());
        }

        let usage: BTreeMap<_, _> = snapshot
            .usage
            .iter()
            .map(|u| {
                (
                    u.id.as_str().into(),
                    u.entry_id.as_ref().unwrap().as_str().into(),
                )
            })
            .collect();
        assert_eq!(
            usage, self.reference.usage,
            "response/usage pairing and no orphan/duplicate billing"
        );
        assert_eq!(snapshot.usage.len(), usage.len());
        let actual_args: BTreeMap<_, _> = snapshot
            .registers
            .iter()
            .filter_map(|r| {
                if let Register::OperationToolArguments { key, value, .. } = r {
                    Some((key.clone(), value.clone()))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            actual_args, self.reference.args,
            "saved args / cleanup oracle"
        );
        assert_eq!(
            self.reference.tool_order,
            (0..self.reference.tool_order.len()).collect::<Vec<_>>(),
            "tool settlement source prefix"
        );
        assert!(self.reference.attempts.iter().all(|a| *a <= 3));
        let Register::LaneLeaf { value, .. } =
            self.register(RegisterNamespace::LaneLeaf, "main").unwrap()
        else {
            panic!()
        };
        assert_eq!(
            value.as_ref().map(EntryId::as_str),
            self.reference.entries.last().map(String::as_str)
        );
        if self.reference.terminal {
            assert!(current.is_none());
            for ns in [
                RegisterNamespace::OperationMeta,
                RegisterNamespace::OperationState,
                RegisterNamespace::OperationToolArguments,
                RegisterNamespace::OperationPreparation,
                RegisterNamespace::PendingEntry,
            ] {
                assert!(
                    ready(self.session.list_registers(ns, None))
                        .unwrap()
                        .is_empty()
                );
            }
            assert!(
                self.register(RegisterNamespace::LaneLastResult, "main")
                    .is_some()
            );
        } else {
            assert!(current.is_some());
        }
    }
    pub fn publish(&mut self, recipe: CommitRecipe<()>, label: &str) {
        self.trace.push(label.into());
        assert!(
            matches!(
                ready(self.session.commit_conditional(request(recipe))).unwrap(),
                ConditionalOutcome::Applied(_)
            ),
            "valid modeled candidate must publish"
        );
        self.check();
    }
    pub fn reject_recipe(&mut self, recipe: CommitRecipe<()>, label: &str) {
        self.trace.push(label.into());
        let before = self.snapshot();
        assert!(matches!(
            ready(self.session.commit_conditional(request(recipe))).unwrap(),
            ConditionalOutcome::NotApplied
        ));
        assert_eq!(
            self.snapshot(),
            before,
            "CAS rejection changed durable state"
        );
        self.check();
    }
    pub fn reject_output(
        &mut self,
        commit: &SettlementCommit<()>,
        extra: &SettlementFacts,
        label: &str,
    ) {
        self.trace.push(label.into());
        let before = self.snapshot();
        let (c, i, f) = self.current();
        assert_eq!(
            settlement_recipe(&c, commit, &i, &f, extra).unwrap(),
            RecipeDecision::NoLongerExpected
        );
        assert_eq!(
            self.snapshot(),
            before,
            "wrong/stale output changed durable state"
        );
        self.check();
    }
    pub fn pulse_lane(&mut self) {
        self.trace.push("external valid lane-token pulse".into());
        let Register::LaneState { value, .. } =
            self.register(RegisterNamespace::LaneState, "main").unwrap()
        else {
            panic!()
        };
        ready(self.session.commit(Transaction {
            writes: vec![set(RegisterSet::LaneState {
                key: "main".into(),
                value,
            })],
        }))
        .unwrap();
        self.check();
    }
    fn accept(&mut self) {
        let roots = self.roots();
        let Register::LaneState {
            value: lane_state,
            seq: lane_seq,
            ..
        } = roots.lane_state
        else {
            panic!()
        };
        let Register::LaneLeaf { seq: leaf_seq, .. } = roots.leaf else {
            panic!()
        };
        let Register::LaneConfig {
            seq: configuration_seq,
            ..
        } = roots.configuration
        else {
            panic!()
        };
        let facts = AcceptanceFacts {
            operation: Operation {
                operation_id: OperationId::new("operation").unwrap(),
                lane: LaneId::new("main").unwrap(),
                source_leaf_id: None,
                started_at: n(1),
                intent: OperationIntent::Run(RunIntent {
                    kind: Default::default(),
                    prompt_entry_ids: vec![EntryId::new("prompt").unwrap()],
                    system_prompt_override: None,
                    resume_data: None,
                }),
            },
            lane_state,
            lane_seq,
            leaf_seq: Some(leaf_seq),
            configuration_seq,
            settings_revision: n(0),
            settings: settings(),
            captured: vec![],
            messages: vec![NewMessageEntry {
                id: EntryId::new("prompt").unwrap(),
                parent_id: None,
                entry_type: Default::default(),
                message: user(),
                terminate: None,
            }],
            injections: vec![],
            skip_inbox_once: None,
            identities_present: true,
            telemetry_context: (),
        };
        let recipe = acceptance_recipe(&facts).unwrap();
        self.reference.entries.push("prompt".into());
        self.publish(recipe, "accept prompt");
    }
    pub fn begin_step(&mut self, step: &str, race: bool) {
        let (c, i, f) = self.current();
        let planning = PlanningFacts::Checkpoint {
            session_id: f.session_id.clone(),
            telemetry_context: (),
            step_id: StepId::new(step).unwrap(),
        };
        let ProcedureAction::Existing(Action::Transition(t)) =
            next_action(&c, &i, &planning).unwrap()
        else {
            panic!()
        };
        let commit = TransitionCommit {
            current: c.clone(),
            next: t.next,
            telemetry_context: (),
            expected_configuration_seq: t.expected_configuration_seq,
            expected_settings_revision: t.expected_settings_revision,
        };
        let recipe = apply(transition_recipe(&c, &commit, &i, &f, &planning).unwrap());
        if race {
            self.pulse_lane();
            self.reject_recipe(recipe, "reject stale step capture");
            self.begin_step(step, false)
        } else {
            self.publish(recipe, "capture configured step")
        }
    }
    pub fn intent(&mut self, label: &str) -> AssistantEffectPlan<()> {
        let (c, i, f) = self.current();
        let OperationState::Run(r) = &c.state else {
            panic!()
        };
        let RunPhase::Assistant(AssistantPhase {
            generation:
                Generation::Ready {
                    context,
                    next_attempt,
                },
            ..
        }) = &r.phase
        else {
            panic!()
        };
        let attempt = next_attempt.get();
        let event:BeforeRequestEvent=serde_json::from_value(json!({"lane":"main","runId":"operation","step":"assistant","attempt":attempt,"streamOptions":context.stream_options,"model":{"id":"model","name":"model","api":"explicit-fixture-api","provider":"model-provider","baseUrl":"https://example.invalid","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":1000,"maxTokens":10}})).unwrap();
        let planning = PlanningFacts::BeforeRequest {
            session_id: f.session_id.clone(),
            telemetry_context: (),
            event: Box::new(event),
        };
        let ProcedureAction::Existing(Action::Dispatch(d)) =
            next_action(&c, &i, &planning).unwrap()
        else {
            panic!()
        };
        let EffectPlan::Hook(h) = d.effect else {
            panic!()
        };
        let output = RequestOptionsOutput::new(&h, Default::default()).unwrap();
        let commit = SettlementCommit {
            current: c.clone(),
            plan: EffectPlan::Hook(h),
            output: SettlementOutput::RequestOptions(output),
            telemetry_context: (),
        };
        let recipe = apply(
            settlement_recipe(
                &c,
                &commit,
                &i,
                &f,
                &SettlementFacts::Request {
                    response_entry_id: EntryId::new(format!("response-{label}-{attempt}")).unwrap(),
                    usage_id: UsageId::new(format!("usage-{label}-{attempt}")).unwrap(),
                },
            )
            .unwrap(),
        );
        let EffectPlan::Assistant(plan) = recipe.dispatch.clone().unwrap() else {
            panic!()
        };
        assert_eq!(
            plan.generation.response_entry_id.as_str(),
            format!("response-{label}-{attempt}"),
            "intent must retain caller-supplied response reservation"
        );
        assert_eq!(
            plan.generation.usage_id.as_str(),
            format!("usage-{label}-{attempt}"),
            "intent must retain caller-supplied usage reservation"
        );
        self.reference.attempts.push(attempt);
        self.publish(recipe, &format!("intent {label} attempt {attempt}"));
        plan
    }
    pub fn assistant_commit(
        &self,
        plan: &AssistantEffectPlan<()>,
        message: SettledAssistantMessage,
    ) -> SettlementCommit<()> {
        let (c, _, _) = self.current();
        SettlementCommit {
            current: c,
            plan: EffectPlan::Assistant(plan.clone()),
            output: SettlementOutput::Assistant {
                key: plan.key.clone(),
                message,
            },
            telemetry_context: (),
        }
    }
    pub fn record_response(&mut self, plan: &AssistantEffectPlan<()>) {
        let id = plan.generation.response_entry_id.as_str().to_owned();
        self.reference.entries.push(id.clone());
        self.reference
            .usage
            .insert(plan.generation.usage_id.as_str().into(), id);
    }
    pub fn finish(&mut self, expected: End) {
        let (c, i, f) = self.current();
        let result = operation_result(&c, &i).unwrap();
        let OperationResult::Run(outcome) = &result else {
            panic!("modeled run returned a structural result");
        };
        assert!(
            matches!(
                (expected, outcome.as_ref()),
                (End::Assistant, RunOutcome::CompletedWithAssistant(_))
                    | (End::Tools, RunOutcome::CompletedWithoutAssistant(_))
                    | (End::Failed, RunOutcome::FailedWithAssistant(_))
            ),
            "terminal result disagrees with the generated history: {expected:?} {outcome:?}"
        );
        let commit = TerminalCommit {
            current: c.clone(),
            result,
            telemetry_context: (),
        };
        let recipe = apply(
            terminal_recipe(&c, &commit, &i, &f, &TerminalFacts { registers: vec![] }).unwrap(),
        );
        self.reference.terminal = true;
        let duplicate = recipe.clone();
        self.publish(recipe, "terminal cleanup");
        let Some(Register::LaneLastResult { value, .. }) =
            self.register(RegisterNamespace::LaneLastResult, "main")
        else {
            panic!()
        };
        assert!(
            matches!(
                (expected, value),
                (End::Assistant, LaneLastResult::RunCompletedAssistant(_))
                    | (End::Tools, LaneLastResult::RunCompletedTerminatedTools(_))
                    | (End::Failed, LaneLastResult::RunFailed(_))
            ),
            "durable terminal kind disagrees with generated history"
        );
        self.reject_recipe(duplicate, "stale terminal cannot recreate operation");
        let before = self.snapshot();
        assert!(self.load().0.is_none());
        assert_eq!(self.snapshot(), before);
    }
}
pub fn set(value: RegisterSet) -> Write {
    Write::SetRegister(Box::new(value))
}
pub fn wrong_key(key: &mut EffectKey, axis: usize) {
    match axis {
        0 => key.session_id = SessionId::new("wrong").unwrap(),
        1 => key.lane = LaneId::new("wrong").unwrap(),
        2 => key.operation_id = OperationId::new("wrong").unwrap(),
        3 => {
            key.kind = if key.kind == EffectKind::Assistant {
                EffectKind::Tool
            } else {
                EffectKind::Assistant
            }
        }
        4 => key.step_or_task_id = EffectStepId::new("wrong").unwrap(),
        5 => key.logical_attempt = n(99),
        _ => key.item_index = Some(n(99)),
    }
}
