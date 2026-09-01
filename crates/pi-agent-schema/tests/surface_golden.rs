use std::collections::BTreeSet;

use pi_agent_schema::*;

fn inventory() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-surface/inventory.json"
    ))
    .expect("C2 inventory")
}

fn usage() -> serde_json::Value {
    serde_json::json!({
        "input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,
        "cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}
    })
}

fn user_message() -> serde_json::Value {
    serde_json::json!({"role":"user","content":"hello","timestamp":0})
}

fn assistant(reason: &str) -> serde_json::Value {
    let mut value = serde_json::json!({
        "role":"assistant","content":[],"api":"api","provider":"provider","model":"model",
        "usage":usage(),"stopReason":reason,"timestamp":0
    });
    if matches!(reason, "error" | "aborted") {
        value["errorMessage"] = serde_json::Value::String("failure".to_owned());
    }
    value
}

fn deferred() -> serde_json::Value {
    serde_json::json!({"provider":"provider","modelId":"model","api":"api","id":"deferred"})
}

fn operation_error() -> serde_json::Value {
    serde_json::json!({"code":"failure","message":"failure"})
}

fn tool_result() -> serde_json::Value {
    serde_json::json!({
        "role":"toolResult","toolCallId":"call","toolName":"tool","content":[],
        "isError":false,"timestamp":0
    })
}

fn agent_tool_result() -> serde_json::Value {
    serde_json::json!({"content":[],"details":null})
}

fn message_entry() -> serde_json::Value {
    serde_json::json!({
        "id":"entry","parentId":null,"seq":1,"timestamp":1,"type":"message",
        "message":user_message()
    })
}

fn compaction_entry() -> serde_json::Value {
    serde_json::json!({
        "id":"compaction","parentId":"leaf","seq":2,"timestamp":2,"type":"compaction",
        "summary":"summary","retainedTail":[],"tokensBefore":0,"fromHook":false
    })
}

fn branch_summary_entry() -> serde_json::Value {
    serde_json::json!({
        "id":"summary","parentId":"target","seq":2,"timestamp":2,"type":"branch_summary",
        "fromId":"old","summary":"summary","fromHook":false
    })
}

fn missing() -> serde_json::Value {
    serde_json::json!({"tools":["tool"],"models":["provider/model"]})
}

fn queued() -> serde_json::Value {
    serde_json::json!({"entryId":"queued","message":user_message()})
}

fn model() -> serde_json::Value {
    serde_json::json!({
        "id":"model","name":"Model","api":"api","provider":"provider",
        "baseUrl":"https://example.invalid/v1","reasoning":false,"input":["text"],
        "cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0},
        "contextWindow":1000,"maxTokens":100
    })
}

fn compaction_preparation() -> serde_json::Value {
    serde_json::json!({
        "messagesToSummarize":[],"turnPrefixMessages":[],"retainedTail":[],
        "isSplitTurn":false,"tokensBefore":0,
        "fileOps":{"read":[],"written":[],"edited":[]},
        "settings":{"enabled":true,"reserveTokens":10,"keepRecentTokens":20}
    })
}

fn branch_preparation() -> serde_json::Value {
    serde_json::json!({
        "messages":[],"fileOps":{"read":[],"written":[],"edited":[]},"totalTokens":0
    })
}

fn lane_event(event_type: &str, payload: serde_json::Value) -> serde_json::Value {
    let mut object = payload.as_object().expect("payload object").clone();
    object.insert("type".to_owned(), event_type.into());
    object.insert("lane".to_owned(), "main".into());
    serde_json::Value::Object(object)
}

fn global_event(event_type: &str, payload: serde_json::Value) -> serde_json::Value {
    let mut object = payload.as_object().expect("payload object").clone();
    object.insert("type".to_owned(), event_type.into());
    serde_json::Value::Object(object)
}

fn event_fixture(name: &str) -> serde_json::Value {
    match name {
        "lane.run_start" => lane_event("run_start", serde_json::json!({"runId":"op"})),
        "lane.run_resume" => lane_event(
            "run_resume",
            serde_json::json!({"runId":"op","recovery":true}),
        ),
        "lane.run_suspend.deferred" => lane_event(
            "run_suspend",
            serde_json::json!({"runId":"op","reason":"deferred","deferred":deferred()}),
        ),
        "lane.run_suspend.missing_identities" => lane_event(
            "run_suspend",
            serde_json::json!({"runId":"op","reason":"missing_identities","missing":missing()}),
        ),
        "lane.run_abort" => lane_event(
            "run_abort",
            serde_json::json!({"runId":"op","steer":[],"followUp":[]}),
        ),
        name if name.starts_with("lane.run_end.") => {
            let outcome = if name.contains(".completed.") {
                "completed"
            } else if name.contains(".aborted.") {
                "aborted"
            } else {
                "failed"
            };
            let mut payload = serde_json::json!({"runId":"op","leafId":null,"outcome":outcome});
            if name.contains("failed") {
                payload["error"] = operation_error();
            }
            if name.ends_with("with_final") {
                payload["finalEntryId"] = "final".into();
                payload["finalMessage"] = assistant(if outcome == "aborted" {
                    "aborted"
                } else if outcome == "failed" {
                    "error"
                } else {
                    "stop"
                });
            }
            lane_event("run_end", payload)
        }
        "global.fault" => global_event(
            "fault",
            serde_json::json!({"code":"fault","message":"fault"}),
        ),
        name if name.ends_with("handler_error.hook") => {
            let payload = serde_json::json!({"error":"handler","kind":"hook","hook":"before_run"});
            if name.starts_with("lane.") {
                lane_event("handler_error", payload)
            } else {
                global_event("handler_error", payload)
            }
        }
        name if name.ends_with("handler_error.event") => {
            let payload = serde_json::json!({"error":"handler","kind":"event","event":"run_start"});
            if name.starts_with("lane.") {
                lane_event("handler_error", payload)
            } else {
                global_event("handler_error", payload)
            }
        }
        "lane.turn_start" => lane_event(
            "turn_start",
            serde_json::json!({"runId":"op","turnId":"turn"}),
        ),
        "lane.turn_end" => lane_event(
            "turn_end",
            serde_json::json!({"runId":"op","turnId":"turn","message":assistant("stop"),"toolResults":[tool_result()]}),
        ),
        "lane.retry_scheduled" => lane_event(
            "retry_scheduled",
            serde_json::json!({"runId":"op","step":"step","attempt":1,"maxAttempts":2,"delayMs":1,"errorMessage":"failure"}),
        ),
        "lane.retry_start" => lane_event(
            "retry_start",
            serde_json::json!({"runId":"op","step":"step","attempt":2}),
        ),
        "lane.retry_end" => lane_event(
            "retry_end",
            serde_json::json!({"runId":"op","step":"step","attempt":2,"success":false,"finalError":"failure"}),
        ),
        "lane.message_start" => lane_event(
            "message_start",
            serde_json::json!({"runId":"op","message":assistant("pending")}),
        ),
        "lane.message_update" => lane_event(
            "message_update",
            serde_json::json!({
                "runId":"op","message":assistant("pending"),
                "event":{"type":"start","partial":assistant("pending")}
            }),
        ),
        "lane.message_end" => lane_event(
            "message_end",
            serde_json::json!({"runId":"op","message":assistant("stop"),"entryId":"entry"}),
        ),
        "lane.tool_start" => lane_event(
            "tool_start",
            serde_json::json!({"runId":"op","turnId":"turn","toolCallId":"call","toolName":"tool","args":{}}),
        ),
        "lane.tool_update" => lane_event(
            "tool_update",
            serde_json::json!({"runId":"op","turnId":"turn","toolCallId":"call","toolName":"tool","partialResult":agent_tool_result()}),
        ),
        "lane.tool_end" => lane_event(
            "tool_end",
            serde_json::json!({"runId":"op","turnId":"turn","toolCallId":"call","toolName":"tool","result":agent_tool_result(),"isError":false,"terminate":false}),
        ),
        "lane.entry_added" => {
            lane_event("entry_added", serde_json::json!({"entry":message_entry()}))
        }
        "lane.write_pending" => lane_event(
            "write_pending",
            serde_json::json!({"runId":"op","entryId":"entry","entryType":"message"}),
        ),
        "lane.queue_update" => lane_event(
            "queue_update",
            serde_json::json!({"steer":[queued()],"followUp":[],"nextRun":[]}),
        ),
        "global.fact_update.name" => global_event(
            "fact_update",
            serde_json::json!({"fact":"name","name":"session"}),
        ),
        "global.fact_update.label" => global_event(
            "fact_update",
            serde_json::json!({"fact":"label","targetId":"entry","label":"label"}),
        ),
        "global.fact_update.custom" => global_event(
            "fact_update",
            serde_json::json!({"fact":"custom","key":"key","value":null}),
        ),
        "lane.config_update.model" => lane_event(
            "config_update",
            serde_json::json!({"property":"model","value":{"provider":"provider","modelId":"model"},"previous":null}),
        ),
        "lane.config_update.thinkingLevel" => lane_event(
            "config_update",
            serde_json::json!({"property":"thinkingLevel","value":"off","previous":"low"}),
        ),
        "lane.config_update.activeTools" => lane_event(
            "config_update",
            serde_json::json!({"property":"activeTools","value":["tool"],"previous":[]}),
        ),
        name if name.starts_with("global.config_update.") => global_event(
            "config_update",
            serde_json::json!({"property":name.rsplit('.').next().expect("property")}),
        ),
        "lane.compaction_start" => lane_event(
            "compaction_start",
            serde_json::json!({"runId":"op","reason":"manual"}),
        ),
        name if name.starts_with("lane.compaction_end.") => {
            let outcome = name.rsplit('.').next().expect("outcome");
            let mut payload = serde_json::json!({"runId":"op","reason":"manual","outcome":outcome});
            if outcome == "completed" {
                payload["entry"] = compaction_entry();
                payload["fromHook"] = false.into();
            } else if outcome == "failed" {
                payload["error"] = operation_error();
            }
            lane_event("compaction_end", payload)
        }
        "lane.navigation_start" => lane_event(
            "navigation_start",
            serde_json::json!({"runId":"op","targetId":null}),
        ),
        name if name.starts_with("lane.navigation_end.") => {
            let outcome = name.split('.').nth(2).expect("outcome");
            let mut payload = serde_json::json!({"runId":"op","oldLeafId":null,"newLeafId":null,"outcome":outcome});
            if name.ends_with("summary_present") {
                payload["summaryEntry"] = branch_summary_entry();
            } else if outcome == "failed" {
                payload["error"] = operation_error();
            }
            lane_event("navigation_end", payload)
        }
        "lane.lane_created" => lane_event("lane_created", serde_json::json!({"at":null})),
        "global.usage" => global_event(
            "usage",
            serde_json::json!({
                "lane":"main","row":{"id":"usage","seq":1,"usage":usage(),"adjustment":false},
                "totals":usage()
            }),
        ),
        other => panic!("unhandled event fixture {other}"),
    }
}

fn outcome_fixture(name: &str) -> serde_json::Value {
    match name {
        name if name.ends_with("completed.with_final") => serde_json::json!({
            "kind":"completed","leafId":"leaf","finalEntryId":"final","finalMessage":assistant("stop")
        }),
        name if name.ends_with("completed.without_final") => {
            serde_json::json!({"kind":"completed","leafId":"leaf"})
        }
        name if name.ends_with("aborted.with_final") => serde_json::json!({
            "kind":"aborted","leafId":"leaf","finalEntryId":"final","finalMessage":assistant("aborted")
        }),
        name if name.ends_with("aborted.without_final") => {
            serde_json::json!({"kind":"aborted","leafId":"leaf"})
        }
        name if name.ends_with("failed.with_final") => serde_json::json!({
            "kind":"failed","leafId":"leaf","error":operation_error(),
            "finalEntryId":"final","finalMessage":assistant("error")
        }),
        name if name.ends_with("failed.without_final") => {
            serde_json::json!({"kind":"failed","leafId":"leaf","error":operation_error()})
        }
        "run.suspended.deferred" => serde_json::json!({
            "kind":"suspended","reason":"deferred","leafId":"leaf","finalEntryId":"final","deferred":deferred()
        }),
        name if name.ends_with("suspended.missing_identities") => serde_json::json!({
            "kind":"suspended","reason":"missing_identities","leafId":if name.starts_with("navigation") { serde_json::Value::Null } else { "leaf".into() },"missing":missing()
        }),
        "compaction.completed" => {
            serde_json::json!({"kind":"completed","leafId":"leaf","entry":compaction_entry()})
        }
        "compaction.declined" => serde_json::json!({"kind":"declined","leafId":"leaf"}),
        "compaction.aborted" => serde_json::json!({"kind":"aborted","leafId":"leaf"}),
        "compaction.failed" => {
            serde_json::json!({"kind":"failed","leafId":"leaf","error":operation_error()})
        }
        "navigation.completed" => {
            serde_json::json!({"kind":"completed","oldLeafId":null,"newLeafId":null})
        }
        "navigation.declined" => serde_json::json!({"kind":"declined","leafId":null}),
        "navigation.aborted" => serde_json::json!({"kind":"aborted","leafId":null}),
        "navigation.failed" => {
            serde_json::json!({"kind":"failed","leafId":null,"error":operation_error()})
        }
        other => panic!("unhandled outcome fixture {other}"),
    }
}

macro_rules! exhaust_selector {
    ($value:expr, $($pattern:pat_param)|+ $(,)?) => {
        match $value {
            $($pattern)|+ => {}
        }
    };
}

fn harness_event_envelope_shape(event: &HarnessEvent) -> &'static str {
    match event {
        HarnessEvent::RunStart(event) => {
            exhaust_selector!(event.event_type, RunStartEventType::RunStart);
            "lane.run_start"
        }
        HarnessEvent::RunResume(event) => {
            exhaust_selector!(event.event_type, RunResumeEventType::RunResume);
            "lane.run_resume"
        }
        HarnessEvent::RunSuspendDeferred(event) => {
            exhaust_selector!(event.event_type, RunSuspendEventType::RunSuspend);
            exhaust_selector!(event.reason, DeferredEventReason::Deferred);
            "lane.run_suspend.deferred"
        }
        HarnessEvent::RunSuspendMissing(event) => {
            exhaust_selector!(event.event_type, RunSuspendEventType::RunSuspend);
            exhaust_selector!(
                event.reason,
                MissingIdentitiesEventReason::MissingIdentities
            );
            "lane.run_suspend.missing_identities"
        }
        HarnessEvent::RunAbort(event) => {
            exhaust_selector!(event.event_type, RunAbortEventType::RunAbort);
            "lane.run_abort"
        }
        HarnessEvent::RunEndCompletedWithFinal(event) => {
            exhaust_selector!(event.event_type, RunEndEventType::RunEnd);
            exhaust_selector!(event.outcome, CompletedEventOutcome::Completed);
            "lane.run_end.completed.with_final"
        }
        HarnessEvent::RunEndCompletedWithoutFinal(event) => {
            exhaust_selector!(event.event_type, RunEndEventType::RunEnd);
            exhaust_selector!(event.outcome, CompletedEventOutcome::Completed);
            "lane.run_end.completed.without_final"
        }
        HarnessEvent::RunEndAbortedWithFinal(event) => {
            exhaust_selector!(event.event_type, RunEndEventType::RunEnd);
            exhaust_selector!(event.outcome, AbortedEventOutcome::Aborted);
            "lane.run_end.aborted.with_final"
        }
        HarnessEvent::RunEndAbortedWithoutFinal(event) => {
            exhaust_selector!(event.event_type, RunEndEventType::RunEnd);
            exhaust_selector!(event.outcome, AbortedEventOutcome::Aborted);
            "lane.run_end.aborted.without_final"
        }
        HarnessEvent::RunEndFailedWithFinal(event) => {
            exhaust_selector!(event.event_type, RunEndEventType::RunEnd);
            exhaust_selector!(event.outcome, FailedEventOutcome::Failed);
            "lane.run_end.failed.with_final"
        }
        HarnessEvent::RunEndFailedWithoutFinal(event) => {
            exhaust_selector!(event.event_type, RunEndEventType::RunEnd);
            exhaust_selector!(event.outcome, FailedEventOutcome::Failed);
            "lane.run_end.failed.without_final"
        }
        HarnessEvent::TurnStart(event) => {
            exhaust_selector!(event.event_type, TurnStartEventType::TurnStart);
            "lane.turn_start"
        }
        HarnessEvent::TurnEnd(event) => {
            exhaust_selector!(event.event_type, TurnEndEventType::TurnEnd);
            "lane.turn_end"
        }
        HarnessEvent::RetryScheduled(event) => {
            exhaust_selector!(event.event_type, RetryScheduledEventType::RetryScheduled);
            "lane.retry_scheduled"
        }
        HarnessEvent::RetryStart(event) => {
            exhaust_selector!(event.event_type, RetryStartEventType::RetryStart);
            "lane.retry_start"
        }
        HarnessEvent::RetryEnd(event) => {
            exhaust_selector!(event.event_type, RetryEndEventType::RetryEnd);
            "lane.retry_end"
        }
        HarnessEvent::MessageStart(event) => {
            exhaust_selector!(event.event_type, MessageStartEventType::MessageStart);
            "lane.message_start"
        }
        HarnessEvent::MessageUpdate(event) => {
            exhaust_selector!(event.event_type, MessageUpdateEventType::MessageUpdate);
            "lane.message_update"
        }
        HarnessEvent::MessageEnd(event) => {
            exhaust_selector!(event.event_type, MessageEndEventType::MessageEnd);
            "lane.message_end"
        }
        HarnessEvent::ToolStart(event) => {
            exhaust_selector!(event.event_type, ToolStartEventType::ToolStart);
            "lane.tool_start"
        }
        HarnessEvent::ToolUpdate(event) => {
            exhaust_selector!(event.event_type, ToolUpdateEventType::ToolUpdate);
            "lane.tool_update"
        }
        HarnessEvent::ToolEnd(event) => {
            exhaust_selector!(event.event_type, ToolEndEventType::ToolEnd);
            "lane.tool_end"
        }
        HarnessEvent::EntryAdded(event) => {
            exhaust_selector!(event.event_type, EntryAddedEventType::EntryAdded);
            "lane.entry_added"
        }
        HarnessEvent::WritePending(event) => {
            exhaust_selector!(event.event_type, WritePendingEventType::WritePending);
            "lane.write_pending"
        }
        HarnessEvent::QueueUpdate(event) => {
            exhaust_selector!(event.event_type, QueueUpdateEventType::QueueUpdate);
            "lane.queue_update"
        }
        HarnessEvent::ModelConfigUpdate(event) => {
            exhaust_selector!(event.event_type, ConfigUpdateEventType::ConfigUpdate);
            exhaust_selector!(event.property, ModelConfigProperty::Model);
            "lane.config_update.model"
        }
        HarnessEvent::ThinkingConfigUpdate(event) => {
            exhaust_selector!(event.event_type, ConfigUpdateEventType::ConfigUpdate);
            exhaust_selector!(event.property, ThinkingConfigProperty::ThinkingLevel);
            "lane.config_update.thinkingLevel"
        }
        HarnessEvent::ActiveToolsConfigUpdate(event) => {
            exhaust_selector!(event.event_type, ConfigUpdateEventType::ConfigUpdate);
            exhaust_selector!(event.property, ActiveToolsConfigProperty::ActiveTools);
            "lane.config_update.activeTools"
        }
        HarnessEvent::CompactionStart(event) => {
            exhaust_selector!(event.event_type, CompactionStartEventType::CompactionStart);
            exhaust_selector!(
                event.reason,
                CompactionEventReason::Manual
                    | CompactionEventReason::Threshold
                    | CompactionEventReason::Overflow
            );
            "lane.compaction_start"
        }
        HarnessEvent::CompactionEndCompleted(event) => {
            exhaust_selector!(event.event_type, CompactionEndEventType::CompactionEnd);
            exhaust_selector!(event.outcome, CompletedEventOutcome::Completed);
            exhaust_selector!(
                event.reason,
                CompactionEventReason::Manual
                    | CompactionEventReason::Threshold
                    | CompactionEventReason::Overflow
            );
            "lane.compaction_end.completed"
        }
        HarnessEvent::CompactionEndDeclined(event) => {
            exhaust_selector!(event.event_type, CompactionEndEventType::CompactionEnd);
            exhaust_selector!(event.outcome, DeclinedEventOutcome::Declined);
            exhaust_selector!(
                event.reason,
                CompactionEventReason::Manual
                    | CompactionEventReason::Threshold
                    | CompactionEventReason::Overflow
            );
            "lane.compaction_end.declined"
        }
        HarnessEvent::CompactionEndAborted(event) => {
            exhaust_selector!(event.event_type, CompactionEndEventType::CompactionEnd);
            exhaust_selector!(event.outcome, AbortedEventOutcome::Aborted);
            exhaust_selector!(
                event.reason,
                CompactionEventReason::Manual
                    | CompactionEventReason::Threshold
                    | CompactionEventReason::Overflow
            );
            "lane.compaction_end.aborted"
        }
        HarnessEvent::CompactionEndFailed(event) => {
            exhaust_selector!(event.event_type, CompactionEndEventType::CompactionEnd);
            exhaust_selector!(event.outcome, FailedEventOutcome::Failed);
            exhaust_selector!(
                event.reason,
                CompactionEventReason::Manual
                    | CompactionEventReason::Threshold
                    | CompactionEventReason::Overflow
            );
            "lane.compaction_end.failed"
        }
        HarnessEvent::NavigationStart(event) => {
            exhaust_selector!(event.event_type, NavigationStartEventType::NavigationStart);
            "lane.navigation_start"
        }
        HarnessEvent::NavigationEndCompleted(event) => {
            exhaust_selector!(event.event_type, NavigationEndEventType::NavigationEnd);
            exhaust_selector!(event.outcome, CompletedEventOutcome::Completed);
            "lane.navigation_end.completed"
        }
        HarnessEvent::NavigationEndDeclined(event) => {
            exhaust_selector!(event.event_type, NavigationEndEventType::NavigationEnd);
            exhaust_selector!(event.outcome, DeclinedEventOutcome::Declined);
            "lane.navigation_end.declined"
        }
        HarnessEvent::NavigationEndAborted(event) => {
            exhaust_selector!(event.event_type, NavigationEndEventType::NavigationEnd);
            exhaust_selector!(event.outcome, AbortedEventOutcome::Aborted);
            "lane.navigation_end.aborted"
        }
        HarnessEvent::NavigationEndFailed(event) => {
            exhaust_selector!(event.event_type, NavigationEndEventType::NavigationEnd);
            exhaust_selector!(event.outcome, FailedEventOutcome::Failed);
            "lane.navigation_end.failed"
        }
        HarnessEvent::LaneCreated(event) => {
            exhaust_selector!(event.event_type, LaneCreatedEventType::LaneCreated);
            "lane.lane_created"
        }
        HarnessEvent::LaneHookHandlerError(event) => {
            exhaust_selector!(event.event_type, HandlerErrorEventType::HandlerError);
            exhaust_selector!(event.kind, HookHandlerKind::Hook);
            "lane.handler_error.hook"
        }
        HarnessEvent::LaneEventHandlerError(event) => {
            exhaust_selector!(event.event_type, HandlerErrorEventType::HandlerError);
            exhaust_selector!(event.kind, EventHandlerKind::Event);
            "lane.handler_error.event"
        }
        HarnessEvent::Fault(event) => {
            exhaust_selector!(event.event_type, FaultEventType::Fault);
            "global.fault"
        }
        HarnessEvent::NameFactUpdate(event) => {
            exhaust_selector!(event.event_type, FactUpdateEventType::FactUpdate);
            exhaust_selector!(event.fact, NameFact::Name);
            "global.fact_update.name"
        }
        HarnessEvent::LabelFactUpdate(event) => {
            exhaust_selector!(event.event_type, FactUpdateEventType::FactUpdate);
            exhaust_selector!(event.fact, LabelFact::Label);
            "global.fact_update.label"
        }
        HarnessEvent::CustomFactUpdate(event) => {
            exhaust_selector!(event.event_type, FactUpdateEventType::FactUpdate);
            exhaust_selector!(event.fact, CustomFact::Custom);
            "global.fact_update.custom"
        }
        HarnessEvent::GlobalConfigUpdate(event) => {
            exhaust_selector!(event.event_type, ConfigUpdateEventType::ConfigUpdate);
            match event.property {
                GlobalConfigProperty::Tools => "global.config_update.tools",
                GlobalConfigProperty::Resources => "global.config_update.resources",
                GlobalConfigProperty::StreamOptions => "global.config_update.streamOptions",
                GlobalConfigProperty::RetryPolicy => "global.config_update.retryPolicy",
                GlobalConfigProperty::CompactionSettings => {
                    "global.config_update.compactionSettings"
                }
                GlobalConfigProperty::SteeringMode => "global.config_update.steeringMode",
                GlobalConfigProperty::FollowUpMode => "global.config_update.followUpMode",
            }
        }
        HarnessEvent::GlobalHookHandlerError(event) => {
            exhaust_selector!(event.event_type, HandlerErrorEventType::HandlerError);
            exhaust_selector!(event.kind, HookHandlerKind::Hook);
            "global.handler_error.hook"
        }
        HarnessEvent::GlobalEventHandlerError(event) => {
            exhaust_selector!(event.event_type, HandlerErrorEventType::HandlerError);
            exhaust_selector!(event.kind, EventHandlerKind::Event);
            "global.handler_error.event"
        }
        HarnessEvent::Usage(event) => {
            exhaust_selector!(event.event_type, UsageEventType::Usage);
            "global.usage"
        }
    }
}

#[test]
fn all_56_event_envelope_goldens_round_trip_exactly() {
    let names = inventory()["eventEnvelopeFixtures"]
        .as_array()
        .expect("event fixtures")
        .clone();
    assert_eq!(names.len(), 56);
    let mut selected_shapes = BTreeSet::new();
    for name in names {
        let name = name.as_str().expect("fixture name");
        let value = event_fixture(name);
        let decoded: HarnessEvent = serde_json::from_value(value.clone())
            .unwrap_or_else(|error| panic!("{name} must decode: {error}\n{value}"));
        let expected_shape = name
            .strip_suffix(".summary_absent")
            .or_else(|| name.strip_suffix(".summary_present"))
            .unwrap_or(name);
        assert_eq!(
            harness_event_envelope_shape(&decoded),
            expected_shape,
            "{name} must select its exact source-linked shape"
        );
        selected_shapes.insert(expected_shape.to_owned());
        let encoded = serde_json::to_value(decoded)
            .unwrap_or_else(|error| panic!("{name} must encode: {error}"));
        assert_eq!(encoded, value, "{name} must round-trip exactly");
    }
    assert_eq!(selected_shapes.len(), 55);
}

#[test]
fn all_operation_and_resume_outcome_goldens_round_trip_exactly() {
    let inventory = inventory();
    for name in inventory["runOutcomes"].as_array().expect("run outcomes") {
        let name = format!("run.{}", name.as_str().expect("name"));
        let value = outcome_fixture(&name);
        let decoded: RunOutcome =
            serde_json::from_value(value.clone()).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(serde_json::to_value(decoded).expect("run encodes"), value);
    }
    for name in inventory["compactionOutcomes"]
        .as_array()
        .expect("compaction outcomes")
    {
        let name = format!("compaction.{}", name.as_str().expect("name"));
        let value = outcome_fixture(&name);
        let decoded: CompactionOutcome =
            serde_json::from_value(value.clone()).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            serde_json::to_value(decoded).expect("compaction encodes"),
            value
        );
    }
    for name in inventory["navigationOutcomes"]
        .as_array()
        .expect("navigation outcomes")
    {
        let name = format!("navigation.{}", name.as_str().expect("name"));
        let value = outcome_fixture(&name);
        let decoded: NavigationOutcome =
            serde_json::from_value(value.clone()).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            serde_json::to_value(decoded).expect("navigation encodes"),
            value
        );
    }

    let resume_names = inventory["resumeOutcomes"]
        .as_array()
        .expect("resume outcomes");
    assert_eq!(resume_names.len(), 18);
    for name in resume_names {
        let name = name.as_str().expect("name");
        let mut value = outcome_fixture(name);
        let object = value.as_object_mut().expect("outcome object");
        object.insert(
            "operation".to_owned(),
            name.split('.').next().expect("operation").into(),
        );
        object.insert("runId".to_owned(), "op".into());
        let decoded: ResumeOutcome = serde_json::from_value(value.clone())
            .unwrap_or_else(|error| panic!("{name}: {error}\n{value}"));
        assert_eq!(
            serde_json::to_value(decoded).expect("resume encodes"),
            value
        );
    }

    let mut navigation_summary = outcome_fixture("navigation.completed");
    navigation_summary["summaryEntry"] = branch_summary_entry();
    let object = navigation_summary
        .as_object_mut()
        .expect("navigation object");
    object.insert("operation".to_owned(), "navigation".into());
    object.insert("runId".to_owned(), "op".into());
    let decoded: ResumeOutcome =
        serde_json::from_value(navigation_summary.clone()).expect("nineteenth resume witness");
    assert_eq!(
        serde_json::to_value(decoded).expect("resume summary encodes"),
        navigation_summary
    );
}

fn hook_event_fixture(name: &str) -> serde_json::Value {
    let event = match name {
        "before_run" => serde_json::json!({
            "lane":"main","runId":"op","prompt":[user_message()],"systemPrompt":"system",
            "resources":{"promptTemplates":[],"skills":[]}
        }),
        "before_resume" => serde_json::json!({
            "lane":"main","runId":"op","kind":"run","prompt":[user_message()],"resumeData":null
        }),
        "before_run_end" => serde_json::json!({
            "lane":"main","runId":"op","messages":[user_message()]
        }),
        "transform_context" => serde_json::json!({
            "lane":"main","runId":"op","messages":[user_message()]
        }),
        "before_request" => serde_json::json!({
            "lane":"main","runId":"op","model":model(),"step":"assistant","attempt":1,
            "streamOptions":{}
        }),
        "before_payload" => serde_json::json!({
            "lane":"main","runId":"op","model":model(),"payload":null
        }),
        "after_response" => serde_json::json!({
            "lane":"main","runId":"op","status":200,"headers":{"X-Public":"value"},
            "message":assistant("stop")
        }),
        "before_tool" => serde_json::json!({
            "lane":"main","runId":"op","toolCallId":"call","toolName":"tool","args":{}
        }),
        "after_tool" => serde_json::json!({
            "lane":"main","runId":"op","toolCallId":"call","toolName":"tool","args":{},
            "content":[],"details":null,"isError":false,"usage":usage()
        }),
        "before_compaction" => serde_json::json!({
            "lane":"main","runId":"op","reason":"manual",
            "preparation":compaction_preparation(),"customInstructions":"focus"
        }),
        "before_navigation" => serde_json::json!({
            "lane":"main","runId":"op","targetId":"target",
            "preparation":branch_preparation(),"customInstructions":"focus"
        }),
        other => panic!("unhandled hook event {other}"),
    };
    serde_json::json!({"name":name,"event":event})
}

fn hook_result_fixture(name: &str) -> serde_json::Value {
    let result = match name {
        "before_run" => serde_json::json!({
            "messages":[user_message()],"systemPrompt":"replacement","resumeData":null
        }),
        "before_run_end" => serde_json::json!({"followUp":"continue"}),
        "transform_context" => serde_json::json!({"messages":[user_message()]}),
        "before_request" => serde_json::json!({"streamOptions":{"deferred":false}}),
        "before_payload" => serde_json::json!({"payload":null}),
        "after_response" => serde_json::json!({"message":assistant("stop")}),
        "before_tool" => {
            serde_json::json!({"args":{},"block":{"reason":"blocked","terminate":true}})
        }
        "after_tool" => serde_json::json!({
            "content":[],"details":null,"isError":false,"usage":usage(),"terminate":false
        }),
        "before_compaction" => serde_json::json!({"decline":true}),
        "before_navigation" => serde_json::json!({
            "summary":{"summary":"summary","readFiles":[],"modifiedFiles":[]}
        }),
        other => panic!("unhandled hook result {other}"),
    };
    serde_json::json!({"name":name,"result":result})
}

#[test]
fn every_hook_event_and_ten_result_dtos_round_trip_exactly() {
    let inventory = inventory();
    for name in inventory["hooks"].as_array().expect("hook names") {
        let name = name.as_str().expect("hook name");
        let value = hook_event_fixture(name);
        let decoded: HookEvent = serde_json::from_value(value.clone())
            .unwrap_or_else(|error| panic!("{name} event: {error}\n{value}"));
        assert_eq!(
            serde_json::to_value(decoded).expect("hook event encodes"),
            value
        );
    }
    for name in inventory["hookResultDtos"]
        .as_array()
        .expect("hook results")
    {
        let name = name.as_str().expect("hook name");
        let value = hook_result_fixture(name);
        let decoded: HookResult = serde_json::from_value(value.clone())
            .unwrap_or_else(|error| panic!("{name} result: {error}\n{value}"));
        assert_eq!(
            serde_json::to_value(decoded).expect("hook result encodes"),
            value
        );
    }
    let before_resume = serde_json::json!({"name":"before_resume"});
    let decoded: HookResult = serde_json::from_value(before_resume.clone()).expect("void result");
    assert_eq!(
        serde_json::to_value(decoded).expect("void result encodes"),
        before_resume
    );
}

fn assert_round_trip<T>(name: &str, value: serde_json::Value)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let decoded: T = serde_json::from_value(value.clone())
        .unwrap_or_else(|error| panic!("{name}: {error}\n{value}"));
    assert_eq!(serde_json::to_value(decoded).expect("value encodes"), value);
}

#[test]
fn every_snapshot_shape_round_trips_exactly() {
    let queued_value = queued();
    assert_round_trip::<QueuedItem>("queued", queued_value.clone());
    assert_round_trip::<PendingWrite>(
        "pending message",
        serde_json::json!({"entryId":"entry","type":"message","message":user_message()}),
    );
    assert_round_trip::<PendingWrite>(
        "pending custom",
        serde_json::json!({"entryId":"entry","type":"custom","customType":"state","data":null}),
    );
    assert_round_trip::<RunningToolSnapshot>(
        "running tool",
        serde_json::json!({
            "toolCallId":"call","toolName":"tool","args":{},"partialResult":agent_tool_result()
        }),
    );
    let aborting = serde_json::json!({"steer":[user_message()],"followUp":[]});
    assert_round_trip::<AbortingSnapshot>("aborting", aborting.clone());
    let suspended = serde_json::json!({
        "lane":"main","operationId":"op","kind":"run","reason":"crash","startedAt":0,
        "prompt":[user_message()],"aborting":aborting,"missing":{"tools":[],"models":[]}
    });
    assert_round_trip::<SuspendedOperation>("suspended", suspended.clone());
    assert_round_trip::<OperationSnapshot>(
        "operation",
        serde_json::json!({
            "id":"op","kind":"run","status":"suspended","startedAt":0,"suspended":suspended,
            "streamingMessage":assistant("pending"),
            "runningTools":[{"toolCallId":"call","toolName":"tool","args":{}}],
            "retry":{"attempt":1,"maxAttempts":2,"nextAttemptAt":10}
        }),
    );
    let queues = serde_json::json!({"steer":[queued_value],"followUp":[],"nextRun":[]});
    assert_round_trip::<LaneQueuesSnapshot>("queues", queues.clone());
    assert_round_trip::<LaneSnapshot>(
        "lane snapshot",
        serde_json::json!({
            "lane":"main","transcript":[message_entry()],"leafId":"entry","operation":null,
            "queues":queues,"pendingWrites":[],"faulted":false
        }),
    );
    let lane_info = serde_json::json!({
        "name":"main","leafId":null,
        "operation":{"id":"op","kind":"run","status":"aborting"}
    });
    assert_round_trip::<LaneInfo>("lane info", lane_info.clone());
    assert_round_trip::<SessionSnapshot>(
        "session snapshot",
        serde_json::json!({"lanes":[lane_info],"faulted":false}),
    );
}

#[test]
fn all_fourteen_expected_error_tags_round_trip_in_narrow_unions() {
    assert_round_trip::<RunExpectedError>(
        "LaneBusy",
        serde_json::json!({
            "_tag":"LaneBusy","message":"busy","lane":"main",
            "operationId":"op","operationKind":"run"
        }),
    );
    assert_round_trip::<RunExpectedError>(
        "MissingIdentities",
        serde_json::json!({
            "_tag":"MissingIdentities","message":"missing","lane":"main",
            "tools":["tool"],"models":["provider/model"]
        }),
    );
    assert_round_trip::<QueueExpectedError>(
        "NoActiveRun",
        serde_json::json!({"_tag":"NoActiveRun","message":"idle","lane":"main"}),
    );
    assert_round_trip::<AbortExpectedError>(
        "NoActiveOperation",
        serde_json::json!({"_tag":"NoActiveOperation","message":"idle","lane":"main"}),
    );
    assert_round_trip::<ResumeExpectedError>(
        "NothingToResume",
        serde_json::json!({"_tag":"NothingToResume","message":"idle","lane":"main"}),
    );
    assert_round_trip::<CompactionExpectedError>(
        "NothingToCompact",
        serde_json::json!({"_tag":"NothingToCompact","message":"empty","lane":"main"}),
    );
    assert_round_trip::<RunExpectedError>(
        "InvalidMessage",
        serde_json::json!({
            "_tag":"InvalidMessage","message":"invalid","lane":"main","reason":"empty"
        }),
    );
    assert_round_trip::<NavigationExpectedError>(
        "InvalidNavigation",
        serde_json::json!({
            "_tag":"InvalidNavigation","message":"invalid","lane":"main","reason":"root"
        }),
    );
    assert_round_trip::<RunExpectedError>(
        "UnknownSkill",
        serde_json::json!({"_tag":"UnknownSkill","message":"missing","name":"skill"}),
    );
    assert_round_trip::<RunExpectedError>(
        "UnknownTemplate",
        serde_json::json!({"_tag":"UnknownTemplate","message":"missing","name":"template"}),
    );
    assert_round_trip::<NavigationExpectedError>(
        "UnknownTarget",
        serde_json::json!({"_tag":"UnknownTarget","message":"missing","targetId":"target"}),
    );
    assert_round_trip::<CreateLaneExpectedError>(
        "LaneExists",
        serde_json::json!({"_tag":"LaneExists","message":"exists","lane":"other"}),
    );
    assert_round_trip::<CreateLaneExpectedError>(
        "InvalidLane",
        serde_json::json!({
            "_tag":"InvalidLane","message":"invalid","lane":"other","reason":"reserved"
        }),
    );
    assert_round_trip::<RunExpectedError>(
        "Closed",
        serde_json::json!({"_tag":"Closed","message":"closed"}),
    );
}

#[test]
fn public_acceptance_queue_cancel_abort_and_usage_values_round_trip() {
    assert_round_trip::<RunSuccess>(
        "run success",
        serde_json::json!({"runId":"op","kind":"completed","leafId":"leaf"}),
    );
    assert_round_trip::<CompactionSuccess>(
        "compaction success",
        serde_json::json!({
            "runId":"op","kind":"completed","leafId":"leaf","entry":compaction_entry()
        }),
    );
    assert_round_trip::<NavigationSuccess>(
        "navigation success",
        serde_json::json!({
            "runId":"op","kind":"completed","oldLeafId":null,"newLeafId":null
        }),
    );
    assert_round_trip::<QueueAdmission>("queue", serde_json::json!({"entryId":"entry"}));
    for kind in ["cancelled", "already_consumed", "not_found"] {
        assert_round_trip::<CancelQueuedOutcome>(kind, serde_json::json!({"kind":kind}));
    }
    assert_round_trip::<AbortOutcome>(
        "abort",
        serde_json::json!({"runId":"op","steer":[user_message()],"followUp":[]}),
    );
    assert_round_trip::<RecordUsageOutcome>("record usage", serde_json::json!({"usageId":"usage"}));
}

#[test]
fn resources_structural_results_scans_and_metadata_round_trip() {
    assert_round_trip::<Resources>(
        "resources",
        serde_json::json!({
            "promptTemplates":[{"name":"template","content":"content","description":"desc"}],
            "skills":[{
                "name":"skill","description":"desc","content":"content","filePath":"/skill/SKILL.md",
                "disableModelInvocation":true
            }]
        }),
    );
    assert_round_trip::<CompactResult>(
        "compact result",
        serde_json::json!({
            "summary":"summary","tokensBefore":10,"retainedTail":[user_message()],
            "usage":usage(),"details":null
        }),
    );
    assert_round_trip::<BranchSummaryResult>(
        "branch result",
        serde_json::json!({
            "summary":"summary","usage":usage(),"readFiles":["a"],"modifiedFiles":["b"]
        }),
    );
    assert_round_trip::<BranchScan>(
        "branch scan",
        serde_json::json!({
            "start":"entry","stopAtType":"compaction","stopAtId":"root","type":"message",
            "customType":"custom","order":"oldestFirst","limit":10,"cursor":{"seq":4}
        }),
    );
    assert_round_trip::<EntryScan>(
        "entry scan",
        serde_json::json!({
            "type":"custom","customType":"state","fromSeq":1,"toSeq":10,"order":"asc","limit":4
        }),
    );
    assert_round_trip::<UsageScan>(
        "usage scan",
        serde_json::json!({"fromSeq":1,"toSeq":10,"order":"desc","limit":4}),
    );
    assert_round_trip::<EntryQuery>(
        "entry query",
        serde_json::json!({"type":"message","order":"desc","limit":4,"cursor":{"seq":8}}),
    );
    assert_round_trip::<SessionMetadata>(
        "session metadata",
        serde_json::json!({
            "id":"session","createdAt":0,"storageVersion":1,"storeGeneration":2,
            "cwd":"/repo","parentSessionId":"parent","legacyParentSessionPath":"/legacy"
        }),
    );
}
