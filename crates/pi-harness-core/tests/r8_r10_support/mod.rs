use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_harness_core::{planner::*, structural};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn id(v: &str) -> EntryId {
    EntryId::new(v).unwrap()
}
pub fn base() -> (CurrentOperation, PlannerInputs<()>, CommitFacts) {
    let c: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/harness/r1-r4/core/states.json"
    ))
    .unwrap();
    let v = c
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "run.checkpoint")
        .unwrap();
    let r = &v["roots"];
    let current = CurrentOperation {
        operation: serde_json::from_value(r["operation_meta"]["value"].clone()).unwrap(),
        state: serde_json::from_value(r["operation_state"]["value"].clone()).unwrap(),
        operation_state_seq: n(10),
        lane_state: serde_json::from_value(r["lane_state"]["value"].clone()).unwrap(),
        lane_state_seq: n(11),
        leaf_id: Some(id("leaf")),
        configuration: serde_json::from_value(r["configuration"]["value"].clone()).unwrap(),
        configuration_seq: n(12),
    };
    let inputs = PlannerInputs {
        running: BTreeMap::new(),
        deferred_poll: DeferredPollPermit::None,
        deferred_cancellations: BTreeSet::new(),
        loaded: BTreeMap::new(),
        runtime: RuntimeSnapshot {
            settings_revision: n(7),
            stream_options: Default::default(),
            retry_policy: NormalizedRetryPolicy {
                max_attempts: OneBasedSafeUnsigned::new(3).unwrap(),
                base_delay_ms: n(10),
            },
        },
        context: None,
        now: n(50),
    };
    (
        current,
        inputs,
        CommitFacts {
            session_id: SessionId::new("session").unwrap(),
            leaf_seq: Some(n(8)),
            meta_seq: n(9),
        },
    )
}
pub fn prep() -> DurableStructuralPreparation {
    serde_json::from_value(json!({"kind":"compaction","messagesToSummarize":[],"turnPrefixMessages":[],"retainedTail":[],"isSplitTurn":false,"tokensBefore":90,"fileOps":{"read":[],"written":[],"edited":[]},"settings":{"enabled":true,"reserveTokens":10,"keepRecentTokens":10}})).unwrap()
}
pub fn fixture(
    kind: &str,
    status: &str,
    request: bool,
) -> (CurrentOperation, PlannerInputs<()>, CommitFacts) {
    let (mut c, mut i, f) = base();
    c.operation.source_leaf_id = Some(id("leaf"));
    let task = TaskId::new("task").unwrap();
    let (summary_kind, reason) = if kind == "navigation" {
        (SummaryKind::BranchSummary, None)
    } else {
        (
            SummaryKind::Compaction,
            Some(if kind == "run" {
                SummaryReason::Threshold
            } else {
                SummaryReason::Manual
            }),
        )
    };
    let context = SummaryContext {
        task_id: task.clone(),
        result_entry_id: id("summary"),
        kind: summary_kind,
        configuration: c.configuration.clone(),
        stream_options: Default::default(),
        retry_policy: i.runtime.retry_policy.clone(),
        reason,
    };
    let generation = match status {
        "ready" => SummaryGeneration::Ready {
            context,
            next_attempt: OneBasedSafeUnsigned::new(1).unwrap(),
        },
        "retry" => SummaryGeneration::RetryWait {
            context,
            next_attempt: OneBasedSafeUnsigned::new(2).unwrap(),
            not_before: n(60),
            error_message: "prior".into(),
        },
        _ => SummaryGeneration::EffectPending {
            context,
            attempt: OneBasedSafeUnsigned::new(1).unwrap(),
            request: request.then(|| SummaryRequestIntent {
                index: n(0),
                usage_id: UsageId::new("usage0").unwrap(),
            }),
            usage_ids: vec![],
        },
    };
    let d = if status == "deciding" {
        StructuralDecision::Deciding {
            task_id: task.clone(),
        }
    } else {
        StructuralDecision::Generating {
            task_id: task.clone(),
            generation: Box::new(generation),
        }
    };
    if kind == "run" {
        let OperationState::Run(r) = &mut c.state else {
            panic!()
        };
        r.inbox = Default::default();
        r.settings.compaction = serde_json::from_value(
            json!({"enabled":true,"reserveTokens":10,"keepRecentTokens":10}),
        )
        .unwrap();
        let p = CheckpointPhase {
            kind: Default::default(),
            continuation: Continuation::NeedAssistant {
                overflow_recovery_used: false,
            },
            trigger_entry_id: id("leaf"),
            threshold_checked_trigger_entry_id: Some(id("leaf")),
            skip_inbox_once: None,
        };
        r.phase = RunPhase::Compaction(RunCompactionPhase {
            kind: Default::default(),
            reason: RunCompactionReason::Threshold,
            structural: d,
            resume_after: p,
        });
    } else if kind == "navigation" {
        c.operation.intent = OperationIntent::Navigation(NavigationIntent {
            kind: Default::default(),
            target_id: Some(id("target")),
            summarize: true,
            label: Some("label".into()),
            custom_instructions: None,
        });
        c.state=serde_json::from_value(json!({"kind":"navigation","control":{"status":"running"},"targetId":"target","summarize":true,"label":"label","phase":{"kind":"summary","structural":d}})).unwrap();
    } else {
        c.operation.intent = OperationIntent::Compaction(CompactionIntent {
            kind: Default::default(),
            custom_instructions: None,
        });
        c.state = OperationState::Compaction(CompactionState {
            kind: Default::default(),
            control: Control::Running(Default::default()),
            custom_instructions: None,
            structural: d,
        });
    }
    let p = if kind == "navigation" {
        serde_json::from_value(json!({"kind":"branch_summary","messages":[],"fileOps":{"read":[],"written":[],"edited":[]},"totalTokens":0})).unwrap()
    } else {
        prep()
    };
    let key = structural::preparation_key(&c).unwrap();
    i.loaded.insert(
        key.clone(),
        PlannerLoadedValue::Register(Register::OperationPreparation {
            key,
            value: p,
            seq: n(6),
        }),
    );
    (c, i, f)
}
pub fn response() -> SettledAssistantMessage {
    serde_json::from_value(json!({"role":"assistant","content":[{"type":"text","text":"summary"}],"api":"a","provider":"captured-provider","model":"captured-model","usage":{"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":3,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":10})).unwrap()
}
pub fn request(p: &SummaryEffectPlan<()>) -> SummaryRequestPlan<()> {
    SummaryRequestPlan {
        task_id: p.generation.context.task_id.clone(),
        attempt: p.generation.attempt,
        request_index: n(0),
        usage_id: UsageId::new("usage0").unwrap(),
        configuration: p.generation.context.configuration.clone(),
        messages: vec![],
        telemetry_context: (),
    }
}
pub fn applied(d: RecipeDecision<()>) -> CommitRecipe<()> {
    match d {
        RecipeDecision::Apply(r) => *r,
        x => panic!("{x:?}"),
    }
}
pub fn cancelled(c: &mut CurrentOperation) {
    let control = Control::CancelRequested(CancelRequestedControl {
        status: Default::default(),
        requested_at: n(55),
        drained_steer: vec![],
        drained_follow_up: vec![],
    });
    match &mut c.state {
        OperationState::Run(r) => r.control = control,
        OperationState::Compaction(r) => r.control = control,
        OperationState::Navigation(NavigationState::Summarized(r)) => r.control = control,
        OperationState::Navigation(NavigationState::Unsummarized(r)) => r.control = control,
    }
}
pub fn planning(f: &CommitFacts) -> PlanningFacts<()> {
    PlanningFacts::Structural(structural::Planning {
        session_id: f.session_id.clone(),
        telemetry_context: (),
        result_entry_id: Some(id("summary")),
    })
}
