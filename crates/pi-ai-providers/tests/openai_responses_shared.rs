use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::{Future, pending};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use pi_ai_core::{
    AssistantMessage, AssistantMessageEvent, AssistantMessageEventStream, AssistantStreamInit,
    AssistantStreamProducer, BoxFuture, CancellationToken, Cancelled, Clock, Context, DoneReason,
    Message, Model, Presence, StopReason, Tool, Usage, create_assistant_message_event_stream,
};
use pi_ai_providers::api::openai_responses_shared::*;
use pi_ai_providers::api::wire_json::{
    WireJsonError, WireNumberError, project_wire_safe_unsigned, stringify_wire_json,
};
use serde_json::{Value, json};

struct FixedClock;
impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _token: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(pending())
    }
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
fn model() -> Model {
    serde_json::from_value(json!({"id":"model","name":"Model","api":"openai-responses","provider":"openai","baseUrl":"https://example.invalid","reasoning":true,"input":["text","image"],"cost":{"input":1000000,"output":2000000,"cacheRead":3000000,"cacheWrite":4000000},"contextWindow":10000,"maxTokens":1000})).unwrap()
}
fn output() -> AssistantMessage {
    serde_json::from_value(json!({"role":"assistant","api":"openai-responses","provider":"openai","model":"model","responseModel":"retained","endTurn":false,"content":[],"stopReason":"pending","timestamp":7,"usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}}})).unwrap()
}
fn stream(token: &CancellationToken) -> (AssistantMessageEventStream, AssistantStreamProducer) {
    let output = output();
    create_assistant_message_event_stream(AssistantStreamInit {
        api: output.api,
        provider: output.provider,
        model: output.model,
        clock: Arc::new(FixedClock),
        cancellation: token.clone(),
    })
}
struct Events {
    events: VecDeque<Value>,
    pending_after: bool,
    dropped: Arc<AtomicUsize>,
}
impl Events {
    fn new(events: Vec<Value>) -> Self {
        Self {
            events: events.into(),
            pending_after: false,
            dropped: Arc::new(AtomicUsize::new(0)),
        }
    }
}
struct DropWitness(Arc<AtomicUsize>);
impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ResponsesEventSource for Events {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Value>, ResponsesError>> {
        let event = self.events.pop_front();
        let wait = self.pending_after && event.is_none();
        let witness = DropWitness(self.dropped.clone());
        Box::pin(async move {
            let _witness = witness;
            if wait {
                pending::<()>().await;
            }
            Ok(event)
        })
    }
}
fn terminal(status: &str) -> Value {
    json!({"type":"response.completed","response":{"status":status}})
}
fn progress(stream: &mut AssistantMessageEventStream) -> Vec<Value> {
    let mut values = Vec::new();
    loop {
        let mut next = pin!(stream.next());
        match next
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
        {
            Poll::Ready(Some(event)) => values.push(serde_json::to_value(event).unwrap()),
            _ => break,
        }
    }
    values
}
fn normalize_signatures(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if matches!(key.as_str(), "textSignature" | "thinkingSignature")
                    && let Some(encoded) = value.as_str()
                {
                    *value = serde_json::from_str(encoded).unwrap();
                } else {
                    normalize_signatures(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_signatures(value);
            }
        }
        _ => {}
    }
}
fn context(messages: Vec<Message>) -> Context {
    Context {
        system_prompt: None,
        messages,
        tools: None,
    }
}
fn tool(name: &str) -> Tool {
    serde_json::from_value(json!({"name":name,"description":"synthetic","parameters":{"type":"object","properties":{"p":{"type":"string"}},"required":["p"]}})).unwrap()
}

#[test]
fn interleaved_slots_preserve_complete_progress_snapshots_and_terminal_backfill() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-responses-shared/interleaved.json"
    ))
    .unwrap();
    assert_eq!(fixture["evidenceClass"], "source-confirmed");
    let mut source = Events::new(fixture["events"].as_array().unwrap().clone());
    let token = CancellationToken::new();
    let (mut consumer, producer) = stream(&token);
    let mut result = output();
    ready(process_responses_stream(
        &mut source,
        &mut result,
        &producer,
        &model(),
        &OpenAIResponsesStreamOptions::default(),
        &token,
    ))
    .unwrap();
    let observed = progress(&mut consumer);
    let expected = fixture["progress"].as_array().unwrap();
    assert_eq!(observed.len(), expected.len());
    for (mut observed, expected) in observed.into_iter().zip(expected) {
        assert_eq!(observed["type"], expected["type"]);
        assert_eq!(observed["contentIndex"], expected["index"]);
        if let Some(delta) = expected.get("delta") {
            assert_eq!(&observed["delta"], delta);
        }
        normalize_signatures(&mut observed);
        let mut partial = serde_json::to_value(output()).unwrap();
        partial["content"] = expected["content"].clone();
        partial["stopReason"] = expected["stop"].clone();
        partial["responseId"] = json!("resp");
        assert_eq!(observed["partial"], partial);
        if observed["type"] == "toolcall_end" {
            assert_eq!(observed["toolCall"], partial["content"][2]);
        }
    }
    let mut wire = serde_json::to_value(&result).unwrap();
    normalize_signatures(&mut wire);
    assert_eq!(wire["content"], fixture["finalContent"]);
    assert_eq!(wire["usage"], fixture["finalUsage"]);
    assert_eq!(wire["responseId"], "resp-final");
    assert_eq!(wire["rawStopReason"], "completed");
    assert_eq!(wire["stopReason"], "toolUse");
    assert_eq!(wire["responseModel"], "retained");
    assert_eq!(wire["endTurn"], false);
    // Shared processor does not settle: only the wrapper may publish the terminal.
    assert!(
        pin!(consumer.result())
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    producer
        .push(AssistantMessageEvent::Done {
            reason: DoneReason::ToolUse,
            message: Box::new(result.clone()),
        })
        .unwrap();
    assert_eq!(ready(consumer.result()), result);
}

#[test]
fn terminal_required_eof_statuses_and_redacted_provider_classification() {
    for (event, reason, raw, error) in [
        (terminal("completed"), StopReason::Stop, "completed", false),
        (
            json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}),
            StopReason::Length,
            "incomplete.max_output_tokens",
            false,
        ),
        (
            json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"future_reason_v2"}}}),
            StopReason::Error,
            "incomplete.future_reason_v2",
            true,
        ),
        (
            json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"content_filter"}}}),
            StopReason::Error,
            "incomplete.content_filter",
            true,
        ),
        (terminal("queued"), StopReason::Stop, "queued", false),
        (terminal("cancelled"), StopReason::Error, "cancelled", true),
    ] {
        let token = CancellationToken::new();
        let (_, producer) = stream(&token);
        let mut out = output();
        let mut source = Events::new(vec![event]);
        ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token,
        ))
        .unwrap();
        assert_eq!(out.stop_reason, reason);
        assert_eq!(out.raw_stop_reason.as_ref().unwrap().as_str(), raw);
        assert_eq!(out.error_message.is_some(), error);
    }
    let token = CancellationToken::new();
    let (_, producer) = stream(&token);
    let mut out = output();
    let mut source = Events::new(vec![]);
    assert_eq!(
        ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token
        ))
        .unwrap_err()
        .kind,
        ResponsesErrorKind::MissingTerminal
    );
    for (message, retry, overflow) in [
        ("server error synthetic-private", true, false),
        ("context length exceeded synthetic-private", false, true),
    ] {
        let mut source = Events::new(vec![
            json!({"type":"response.failed","response":{"status":"failed","error":{"code":"unknown","message":message}}}),
        ]);
        let error = ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token,
        ))
        .unwrap_err();
        assert_eq!(error.kind, ResponsesErrorKind::Provider);
        assert_eq!(error.retryable, retry);
        assert_eq!(error.context_overflow, overflow);
        assert!(!format!("{error} {error:?} {out:?}").contains("synthetic-private"));
        assert_eq!(out.raw_stop_reason.as_ref().unwrap().as_str(), "failed");
    }
}

#[test]
fn pending_reads_cancel_with_usage_and_backfilled_signatures_and_drop_the_loser() {
    let token = CancellationToken::new();
    let (mut consumer, producer) = stream(&token);
    let mut out = output();
    let model = model();
    let mut source = Events::new(vec![
        json!({"type":"response.output_item.done","output_index":3,"item":{"type":"reasoning","id":"rs","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"msg","phase":"final_answer","content":[{"type":"output_text","text":"answer"}]}}),
        json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":9,"output_tokens":3,"total_tokens":12},"output":[{"type":"reasoning","id":"rs","encrypted_content":"opaque"}]}}),
    ]);
    source.pending_after = true;
    let drops = source.dropped.clone();
    let options = OpenAIResponsesStreamOptions::default();
    let mut future = Box::pin(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model,
        &options,
        &token,
    ));
    assert!(
        future
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(progress(&mut consumer).len(), 4);
    let before = drops.load(Ordering::SeqCst);
    token.cancel();
    assert_eq!(
        ready(future.as_mut()).unwrap_err().kind,
        ResponsesErrorKind::Cancelled
    );
    drop(future);
    assert!(drops.load(Ordering::SeqCst) > before);
    let result = ready(consumer.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.usage.input.get(), 9);
    assert_eq!(result.usage.output.get(), 3);
    let wire = serde_json::to_value(result).unwrap();
    let signature: Value =
        serde_json::from_str(wire["content"][0]["thinkingSignature"].as_str().unwrap()).unwrap();
    assert_eq!(signature["encrypted_content"], "opaque");
    assert_eq!(wire["content"][1]["text"], "answer");
}

#[test]
fn custom_grammar_and_function_done_preserve_replacement_and_cleanup() {
    let grammar = BTreeMap::from([("query".to_owned(), "payload".to_owned())]);
    let options = OpenAIResponsesStreamOptions {
        grammar_tool_input_properties: Some(&grammar),
        ..Default::default()
    };
    let mut source = Events::new(vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","id":"ctc","call_id":"call","name":"query","input":""}}),
        json!({"type":"response.custom_tool_call_input.delta","output_index":0,"delta":"a\""}),
        json!({"type":"response.custom_tool_call_input.done","output_index":0,"input":"a\"\nb"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"custom_tool_call","id":"ctc","call_id":"call","name":"query","input":"a\"\nb","namespace":"dynamic"}}),
        // Missing output_index remains distinct from zero; pinned cleanup fixture uses it.
        json!({"type":"response.output_item.added","item":{"type":"function_call","id":"fc","call_id":"call2","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","delta":"{\"old\":true}"}),
        json!({"type":"response.function_call_arguments.done","arguments":"{\"new\":false}"}),
        json!({"type":"response.output_item.done","item":{"type":"function_call","id":"fc","call_id":"call2","name":"lookup","arguments":""}}),
        terminal("completed"),
    ]);
    let token = CancellationToken::new();
    let (mut consumer, producer) = stream(&token);
    let mut out = output();
    ready(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model(),
        &options,
        &token,
    ))
    .unwrap();
    let events = progress(&mut consumer);
    let deltas = events
        .iter()
        .filter(|event| event["type"] == "toolcall_delta" && event["contentIndex"] == 0)
        .map(|event| event["delta"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(deltas, "{\"payload\":\"a\\\"\\nb\"}");
    let content = serde_json::to_value(out.content).unwrap();
    assert_eq!(
        content,
        json!([{"type":"toolCall","id":"call|ctc","name":"query","arguments":{"payload":"a\"\nb"},"namespace":"dynamic"},{"type":"toolCall","id":"call2|fc","name":"lookup","arguments":{"new":false}}])
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "toolcall_delta" && event["contentIndex"] == 1)
            .count(),
        1
    );
}

#[test]
fn tools_preserve_default_false_explicit_null_unsupported_omission_and_grammar() {
    let plain = tool("lookup");
    for strict in [
        Presence::Missing,
        Presence::Null,
        Presence::Value(false),
        Presence::Value(true),
    ] {
        let result = convert_responses_tools(
            std::slice::from_ref(&plain),
            &ConvertResponsesToolsOptions {
                strict: strict.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            result[0]["strict"],
            match strict {
                Presence::Null => Value::Null,
                Presence::Value(value) => json!(value),
                Presence::Missing => json!(false),
            }
        );
    }
    let result = convert_responses_tools(
        std::slice::from_ref(&plain),
        &ConvertResponsesToolsOptions {
            supports_strict_mode: Some(false),
            defer_loading: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(result[0].get("strict").is_none());
    assert!(result[0].get("defer_loading").is_none());
    let grammar:Tool=serde_json::from_value(json!({"name":"query","description":"synthetic","parameters":{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]},"constrainedSampling":{"type":"grammar","variants":{"openai_lark":"start: /x/"}}})).unwrap();
    assert_eq!(
        convert_responses_tools(
            &[grammar],
            &ConvertResponsesToolsOptions {
                supports_openai_grammar_tools: Some(true),
                defer_loading: Some(true),
                ..Default::default()
            }
        )
        .unwrap(),
        vec![
            json!({"type":"custom","name":"query","description":"synthetic","format":{"type":"grammar","syntax":"lark","definition":"start: /x/"},"defer_loading":true})
        ]
    );
}

#[test]
fn replay_signature_fallback_ids_multimodal_outputs_and_namespace_rules() {
    let model = model();
    let mut assistant = output();
    assistant.stop_reason = StopReason::Stop;
    assistant.content=serde_json::from_value(json!([
        {"type":"text","text":"first","textSignature":"{\"v\":1,\"id\":\"msg_known\",\"phase\":\"commentary\"}"},
        {"type":"text","text":"second"},
        {"type":"toolCall","id":"call|fc_item","name":"lookup","arguments":{"x":1},"namespace":"dynamic"}
    ])).unwrap();
    let result:Message=serde_json::from_value(json!({"role":"toolResult","toolCallId":"call|fc_item","toolName":"lookup","content":[{"type":"text","text":"one"},{"type":"text","text":"two"},{"type":"image","data":"AA==","mimeType":"image/png"}],"isError":false,"timestamp":1})).unwrap();
    let mut context = context(vec![Message::Assistant(Box::new(assistant)), result]);
    context.system_prompt = Some("system".to_owned());
    let allowed = BTreeSet::from(["openai".to_owned()]);
    let wire = convert_responses_messages(
        &model,
        &context,
        &allowed,
        &ConvertResponsesMessagesOptions::default(),
    )
    .unwrap();
    assert_eq!(
        wire,
        vec![
            json!({"role":"developer","content":"system"}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"first","annotations":[]}],"status":"completed","id":"msg_known","phase":"commentary"}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"second","annotations":[]}],"status":"completed","id":"msg_pi_0_1"}),
            json!({"type":"function_call","id":"fc_item","call_id":"call","name":"lookup","arguments":"{\"x\":1}","namespace":"dynamic"}),
            json!({"type":"function_call_output","call_id":"call","output":[{"type":"input_text","text":"one\ntwo"},{"type":"input_image","detail":"auto","image_url":"data:image/png;base64,AA=="}]})
        ]
    );
    let mut changed = model.clone();
    changed.id = pi_ai_core::ModelId::new("different").unwrap();
    let wire = convert_responses_messages(
        &changed,
        &context,
        &allowed,
        &ConvertResponsesMessagesOptions {
            include_system_prompt: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    let call = wire
        .iter()
        .find(|item| item["type"] == "function_call")
        .unwrap();
    assert!(call.get("id").is_none());
    assert!(call.get("namespace").is_none());
    assert_eq!(wire[0]["id"], "msg_pi_0");
    assert!(wire[0].get("phase").is_none());
}

#[test]
fn deferred_definitions_load_once_after_markers_and_tools_search_is_paired() {
    let result:Message=serde_json::from_value(json!({"role":"toolResult","toolCallId":"call","toolName":"base","content":[],"addedToolNames":["late","missing","late"],"isError":false,"timestamp":1})).unwrap();
    let context = context(vec![result.clone(), result]);
    let deferred = BTreeMap::from([("late".to_owned(), tool("late"))]);
    for mode in [
        ResponsesDeferredToolsMode::AdditionalTools,
        ResponsesDeferredToolsMode::ToolSearch,
    ] {
        let wire = convert_responses_messages(
            &model(),
            &context,
            &BTreeSet::new(),
            &ConvertResponsesMessagesOptions {
                deferred_tools: Some(&deferred),
                deferred_tools_mode: Some(mode),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(wire[0]["type"], "function_call_output");
        assert_eq!(wire[0]["output"], "(no tool output)");
        if mode == ResponsesDeferredToolsMode::AdditionalTools {
            assert_eq!(wire.len(), 3);
            assert_eq!(wire[1]["type"], "additional_tools");
            assert_eq!(wire[1]["role"], "developer");
            assert!(wire[1]["tools"][0].get("defer_loading").is_none());
        } else {
            assert_eq!(wire.len(), 4);
            assert_eq!(wire[1]["type"], "tool_search_call");
            assert_eq!(wire[1]["call_id"], wire[2]["call_id"]);
            assert_eq!(wire[1]["arguments"], json!({"query":"late","limit":1}));
            assert_eq!(wire[2]["tools"][0]["defer_loading"], true);
        }
    }
}

struct Pricing {
    resolved: AtomicUsize,
    applied: AtomicUsize,
    enabled: bool,
}
impl ResponsesServiceTierPricing for Pricing {
    fn apply(
        &self,
        usage: &mut Usage,
        tier: Presence<ResponsesServiceTier>,
    ) -> Result<(), ResponsesError> {
        assert_eq!(tier, Presence::Value(ResponsesServiceTier::Priority));
        self.applied.fetch_add(1, Ordering::SeqCst);
        usage.cost.total = pi_ai_core::FiniteF64::new(123.0).unwrap();
        Ok(())
    }
}
impl ResponsesServiceTierPolicy for Pricing {
    fn pricing(&self) -> Option<&dyn ResponsesServiceTierPricing> {
        self.enabled.then_some(self)
    }
    fn resolve(
        &self,
        response: Presence<ResponsesServiceTier>,
        request: Presence<ResponsesServiceTier>,
    ) -> Presence<ResponsesServiceTier> {
        assert_eq!(response, Presence::Null);
        assert_eq!(request, Presence::Value(ResponsesServiceTier::Flex));
        self.resolved.fetch_add(1, Ordering::SeqCst);
        Presence::Value(ResponsesServiceTier::Priority)
    }
}
#[test]
fn tier_resolution_only_runs_with_pricing_and_preserves_null_request_precedence() {
    for enabled in [false, true] {
        let policy = Pricing {
            resolved: AtomicUsize::new(0),
            applied: AtomicUsize::new(0),
            enabled,
        };
        let token = CancellationToken::new();
        let (_, producer) = stream(&token);
        let mut output = output();
        let mut source = Events::new(vec![
            json!({"type":"response.completed","response":{"status":"completed","service_tier":null}}),
        ]);
        ready(process_responses_stream(
            &mut source,
            &mut output,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions {
                service_tier: Presence::Value(ResponsesServiceTier::Flex),
                service_tier_policy: Some(&policy),
                ..Default::default()
            },
            &token,
        ))
        .unwrap();
        assert_eq!(policy.resolved.load(Ordering::SeqCst), usize::from(enabled));
        assert_eq!(policy.applied.load(Ordering::SeqCst), usize::from(enabled));
        assert_eq!(
            output.usage.cost.total.get(),
            if enabled { 123.0 } else { 0.0 }
        );
    }
}

#[test]
fn foreign_ids_use_pinned_hash_and_collisions_fail_without_changing_input() {
    let model = model();
    let allowed = BTreeSet::from(["openai".to_owned()]);
    let mut assistant = output();
    assistant.provider = pi_ai_core::ProviderId::new("github-copilot").unwrap();
    assistant.stop_reason = StopReason::ToolUse;
    assistant.content = serde_json::from_value(
        json!([{"type":"toolCall","id":"call|hello","name":"lookup","arguments":{}}]),
    )
    .unwrap();
    let mut transcript = context(vec![Message::Assistant(Box::new(assistant.clone()))]);
    let wire = convert_responses_messages(
        &model,
        &transcript,
        &allowed,
        &ConvertResponsesMessagesOptions::default(),
    )
    .unwrap();
    // "hello" is the pinned shortHash vector, independently accepted in S03.
    assert_eq!(wire[0]["id"], "fc_1h6qa0qrowduu");
    assert_eq!(wire[0]["call_id"], "call");
    assert_eq!(
        wire[1],
        json!({"type":"function_call_output","call_id":"call","output":"No result provided"})
    );
    let alternative = convert_responses_messages(
        &model,
        &transcript,
        &allowed,
        &ConvertResponsesMessagesOptions {
            synthetic_result_timestamp: Some(pi_ai_core::SafeUnsigned::new(999).unwrap()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(wire, alternative);
    assistant.content=serde_json::from_value(json!([{"type":"toolCall","id":"a?","name":"lookup","arguments":{}},{"type":"toolCall","id":"a!","name":"lookup","arguments":{}}])).unwrap();
    transcript.messages = vec![Message::Assistant(Box::new(assistant))];
    let before = transcript.clone();
    assert_eq!(
        convert_responses_messages(
            &model,
            &transcript,
            &allowed,
            &ConvertResponsesMessagesOptions::default()
        )
        .unwrap_err()
        .kind,
        ResponsesErrorKind::Conversion
    );
    assert_eq!(transcript, before);
}

#[test]
fn omitted_item_id_summary_separator_refusal_and_existing_encryption_follow_source() {
    let mut source = Events::new(vec![
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"reasoning","id":"rs","summary":[]}}),
        json!({"type":"response.reasoning_summary_text.delta","output_index":1,"delta":"one"}),
        json!({"type":"response.reasoning_summary_part.done","output_index":1}),
        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"reasoning","id":"rs","summary":[],"encrypted_content":"original"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg"}}),
        json!({"type":"response.refusal.delta","output_index":0,"delta":"refused"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg","content":[{"type":"refusal","refusal":"refused"}]}}),
        json!({"type":"response.output_item.done","output_index":2,"item":{"type":"function_call","call_id":"call","name":"lookup","arguments":"{}"}}),
        json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning","id":"rs","encrypted_content":"replacement"}]}}),
    ]);
    let token = CancellationToken::new();
    let (mut consumer, producer) = stream(&token);
    let mut out = output();
    ready(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model(),
        &OpenAIResponsesStreamOptions::default(),
        &token,
    ))
    .unwrap();
    let mut wire = serde_json::to_value(out).unwrap();
    normalize_signatures(&mut wire);
    assert_eq!(wire["content"][0]["thinking"], "one\n\n");
    assert_eq!(
        wire["content"][0]["thinkingSignature"]["encrypted_content"],
        "original"
    );
    assert_eq!(wire["content"][1]["text"], "refused");
    assert_eq!(wire["content"][2]["id"], "call|undefined");
    let events = progress(&mut consumer);
    assert!(
        events
            .iter()
            .any(|event| event["type"] == "thinking_delta" && event["delta"] == "\n\n")
    );
}

#[test]
fn malformed_protocol_values_fail_without_copying_free_form_diagnostics() {
    for event in [
        json!({"type":"response.output_item.added","output_index":-1,"item":{"type":"message"}}),
        json!({"type":"response.output_item.added","output_index":9007199254740992_u64,"item":{"type":"message"}}),
        json!({"type":"response.completed","response":{"status":"credential://synthetic-private"}}),
        json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"synthetic-private\nfree form"}}}),
        json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":-1}}}),
    ] {
        let token = CancellationToken::new();
        let (_, producer) = stream(&token);
        let mut out = output();
        let mut source = Events::new(vec![event]);
        let error = ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token,
        ))
        .unwrap_err();
        assert_eq!(error.kind, ResponsesErrorKind::InvalidWire);
        assert!(!format!("{error} {error:?} {out:?}").contains("synthetic-private"));
    }
    let token = CancellationToken::new();
    let (_, producer) = stream(&token);
    let mut out = output();
    let mut source = Events::new(vec![json!({"type":"unknown","data":"x".repeat(1024*1024)})]);
    assert_eq!(
        ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token
        ))
        .unwrap_err()
        .kind,
        ResponsesErrorKind::ResourceLimit
    );
}

struct DefaultPricing;
impl ResponsesServiceTierPolicy for DefaultPricing {
    fn pricing(&self) -> Option<&dyn ResponsesServiceTierPricing> {
        Some(self)
    }
}
impl ResponsesServiceTierPricing for DefaultPricing {
    fn apply(
        &self,
        usage: &mut Usage,
        tier: Presence<ResponsesServiceTier>,
    ) -> Result<(), ResponsesError> {
        assert_eq!(tier, Presence::Value(ResponsesServiceTier::Flex));
        usage.cost.total = pi_ai_core::FiniteF64::new(42.0).unwrap();
        Ok(())
    }
}
#[test]
fn pricing_without_custom_resolver_uses_nullish_response_request_precedence() {
    let token = CancellationToken::new();
    let (_, producer) = stream(&token);
    let mut out = output();
    let mut source = Events::new(vec![terminal("completed")]);
    ready(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model(),
        &OpenAIResponsesStreamOptions {
            service_tier: Presence::Value(ResponsesServiceTier::Flex),
            service_tier_policy: Some(&DefaultPricing),
            ..Default::default()
        },
        &token,
    ))
    .unwrap();
    assert_eq!(out.usage.cost.total.get(), 42.0);
}

struct FailingPricing<'a>(Option<&'a CancellationToken>);
impl ResponsesServiceTierPolicy for FailingPricing<'_> {
    fn pricing(&self) -> Option<&dyn ResponsesServiceTierPricing> {
        Some(self)
    }
}
impl ResponsesServiceTierPricing for FailingPricing<'_> {
    fn apply(
        &self,
        usage: &mut Usage,
        _tier: Presence<ResponsesServiceTier>,
    ) -> Result<(), ResponsesError> {
        usage.cost.total = pi_ai_core::FiniteF64::new(77.0).unwrap();
        if let Some(token) = self.0 {
            token.cancel();
        }
        Err(ResponsesError::new(ResponsesErrorKind::Policy))
    }
}
#[test]
fn policy_failure_preserves_loaded_usage_and_cancellation_sees_the_prior_publication() {
    for cancel_in_policy in [false, true] {
        let token = CancellationToken::new();
        let (consumer, producer) = stream(&token);
        let mut out = output();
        let policy = FailingPricing(cancel_in_policy.then_some(&token));
        let mut source = Events::new(vec![
            json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":9,"output_tokens":3,"total_tokens":12}}}),
        ]);
        let error = ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions {
                service_tier_policy: Some(&policy),
                ..Default::default()
            },
            &token,
        ))
        .unwrap_err();
        assert_eq!(out.usage.input.get(), 9);
        assert_eq!(out.usage.cost.total.get(), 77.0);
        assert_eq!(
            error.kind,
            if cancel_in_policy {
                ResponsesErrorKind::Cancelled
            } else {
                ResponsesErrorKind::Policy
            }
        );
        token.cancel();
        let settled = ready(consumer.result());
        assert_eq!(settled.usage.input.get(), 9);
        assert_eq!(
            settled.usage.cost.total.get(),
            if cancel_in_policy { 15.0 } else { 77.0 }
        );
    }
}

#[test]
fn opaque_signature_bytes_match_executed_pinned_processor_with_nested_wire_order() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-responses-shared/signature-bytes.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["evidenceClass"],
        "executed-pinned-processor-signature-oracle"
    );
    let events = serde_json::from_str(fixture["wireEvents"].as_str().unwrap()).unwrap();
    let mut source = Events::new(events);
    let token = CancellationToken::new();
    let (mut consumer, producer) = stream(&token);
    let mut out = output();
    ready(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model(),
        &OpenAIResponsesStreamOptions::default(),
        &token,
    ))
    .unwrap();
    let events = progress(&mut consumer);
    let thinking = events
        .iter()
        .find(|event| event["type"] == "thinking_end")
        .unwrap();
    let text = events
        .iter()
        .find(|event| event["type"] == "text_end")
        .unwrap();
    assert_eq!(
        thinking["partial"]["content"][0]["thinkingSignature"],
        fixture["thinkingEnd"]
    );
    assert_eq!(
        text["partial"]["content"][1]["textSignature"],
        fixture["textEnd"]
    );
    assert_eq!(
        serde_json::to_value(out).unwrap()["content"][0]["thinkingSignature"],
        fixture["finalThinking"]
    );
}

#[test]
fn checked_wire_stringify_matches_executed_number_key_and_escape_oracles() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-responses-shared/wire-json.json"
    ))
    .unwrap();
    assert_eq!(fixture["evidenceClass"], "executed-js-stringify");
    for case in fixture["cases"].as_array().unwrap() {
        let value: Value = serde_json::from_str(case["raw"].as_str().unwrap()).unwrap();
        let before = value.clone();
        assert_eq!(
            stringify_wire_json(&value).unwrap(),
            case["expected"].as_str().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(value, before);
    }
    let object: pi_ai_core::JsonObject = serde_json::from_str(r#"{"z":1,"a":2}"#).unwrap();
    assert_eq!(serde_json::to_string(&object).unwrap(), r#"{"a":2,"z":1}"#);
}

#[test]
fn reasoning_signature_and_encryption_backfill_apply_js_number_and_key_rules() {
    // Reproduces the independently executed pinned processor's numeric metadata probe.
    let event:Value=serde_json::from_str(r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs","summary":[],"metadata":{"10":1.0,"2":1e20,"z":-0.0}}}"#).unwrap();
    let mut source = Events::new(vec![
        event,
        json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning","id":"rs","encrypted_content":"opaque"}]}}),
    ]);
    let token = CancellationToken::new();
    let (mut consumer, producer) = stream(&token);
    let mut out = output();
    ready(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model(),
        &OpenAIResponsesStreamOptions::default(),
        &token,
    ))
    .unwrap();
    let events = progress(&mut consumer);
    let end = events
        .iter()
        .find(|event| event["type"] == "thinking_end")
        .unwrap();
    assert_eq!(
        end["partial"]["content"][0]["thinkingSignature"],
        r#"{"type":"reasoning","id":"rs","summary":[],"metadata":{"2":100000000000000000000,"10":1,"z":0}}"#
    );
    assert_eq!(
        serde_json::to_value(out).unwrap()["content"][0]["thinkingSignature"],
        r#"{"type":"reasoning","id":"rs","summary":[],"metadata":{"2":100000000000000000000,"10":1,"z":0},"encrypted_content":"opaque"}"#
    );
}

#[test]
fn unknown_status_and_rejected_reason_preserve_prior_accounting_and_pricing() {
    for response in [
        json!({"status":"future_status","id":"resp","usage":{"input_tokens":9,"output_tokens":3,"total_tokens":12}}),
        json!({"status":"incomplete","id":"resp","usage":{"input_tokens":9,"output_tokens":3,"total_tokens":12},"incomplete_details":{"reason":"synthetic-private free form"}}),
    ] {
        let token = CancellationToken::new();
        let (consumer, producer) = stream(&token);
        let mut out = output();
        let mut source = Events::new(vec![
            json!({"type":"response.completed","response":response}),
        ]);
        let error = ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions {
                service_tier: Presence::Value(ResponsesServiceTier::Flex),
                service_tier_policy: Some(&DefaultPricing),
                ..Default::default()
            },
            &token,
        ))
        .unwrap_err();
        assert_eq!(error.kind, ResponsesErrorKind::InvalidWire);
        assert_eq!(out.usage.input.get(), 9);
        assert_eq!(out.usage.output.get(), 3);
        assert_eq!(out.usage.cost.total.get(), 42.0);
        assert_eq!(out.response_id.as_ref().unwrap().as_str(), "resp");
        assert!(out.raw_stop_reason.is_none());
        assert!(!format!("{out:?} {error:?}").contains("synthetic-private"));
        token.cancel();
        let settled = ready(consumer.result());
        assert_eq!(settled.usage.input.get(), 9);
        assert_eq!(settled.usage.cost.total.get(), 42.0);
        assert_eq!(settled.response_id.as_ref().unwrap().as_str(), "resp");
    }
}

#[test]
fn checked_wire_stringify_bounds_are_exact_and_errors_are_redacted() {
    assert_eq!(
        stringify_wire_json(&Value::String("x".repeat(1024 * 1024 - 2)))
            .unwrap()
            .len(),
        1024 * 1024
    );
    let input = Value::String("synthetic-private".to_owned() + &"x".repeat(1024 * 1024));
    let error = stringify_wire_json(&input).unwrap_err();
    assert_eq!(error, WireJsonError::ByteLimit);
    assert!(!format!("{error} {error:?}").contains("synthetic-private"));
    for (depth, okay) in [(128, true), (129, false)] {
        let mut value = Value::Null;
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        assert_eq!(stringify_wire_json(&value).is_ok(), okay);
    }
}

#[test]
fn replayed_function_arguments_use_the_same_js_stringify_projection() {
    let mut assistant = output();
    assistant.stop_reason = StopReason::ToolUse;
    assistant.content=serde_json::from_value(json!([{"type":"toolCall","id":"call|fc","name":"lookup","arguments":{"10":1.0,"2":1e20,"z":-0.0}}])).unwrap();
    let wire = convert_responses_messages(
        &model(),
        &context(vec![Message::Assistant(Box::new(assistant))]),
        &BTreeSet::from(["openai".to_owned()]),
        &ConvertResponsesMessagesOptions::default(),
    )
    .unwrap();
    assert_eq!(
        wire[0]["arguments"],
        r#"{"2":100000000000000000000,"10":1,"z":0}"#
    );
}

#[test]
fn wire_unsigned_checks_mathematical_values_without_changing_durable_decoders() {
    for (raw, expected) in [
        ("0", 0),
        ("-0", 0),
        ("-0.0", 0),
        ("0e2", 0),
        ("1.0", 1),
        ("1e2", 100),
        ("1.03e2", 103),
        ("9007199254740991", 9007199254740991),
        ("9007199254740991.0", 9007199254740991),
        ("9.007199254740991e15", 9007199254740991),
    ] {
        let value: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(
            project_wire_safe_unsigned(&value).unwrap().get(),
            expected,
            "{raw}"
        );
    }
    for raw in [
        "-1",
        "-1.0",
        "1.5",
        "0.5",
        "1e-2",
        "9007199254740992",
        "9007199254740992.0",
        "9.007199254740992e15",
        "18446744073709551615",
        "true",
        "null",
        "\"1\"",
        "{}",
        "[]",
    ] {
        assert_eq!(
            project_wire_safe_unsigned(&serde_json::from_str::<Value>(raw).unwrap()),
            Err(WireNumberError),
            "{raw}"
        );
    }
    // Nonfinite JSON is rejected before constructing the finite Value boundary.
    for raw in ["NaN", "Infinity", "1e400", "-1e400"] {
        assert!(serde_json::from_str::<Value>(raw).is_err());
    }
    assert!(serde_json::Number::from_f64(f64::INFINITY).is_none());
    assert!(serde_json::Number::from_f64(f64::NAN).is_none());
    assert!(serde_json::from_str::<pi_ai_core::SafeUnsigned>("1.0").is_err());
    let error = project_wire_safe_unsigned(&json!("synthetic-private")).unwrap_err();
    assert!(!format!("{error} {error:?}").contains("synthetic-private"));
}

#[test]
fn numeric_slot_and_usage_spellings_match_executed_pinned_processor() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-responses-shared/numeric-wire.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["evidenceClass"],
        "executed-pinned-processor-numeric-oracle"
    );
    for case in fixture["cases"].as_array().unwrap() {
        let mut source = Events::new(serde_json::from_str(case["raw"].as_str().unwrap()).unwrap());
        let token = CancellationToken::new();
        let (_, producer) = stream(&token);
        let mut out = output();
        ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token,
        ))
        .unwrap();
        let wire = serde_json::to_value(out).unwrap();
        let mut usage = wire["usage"].clone();
        usage.as_object_mut().unwrap().remove("cost");
        assert_eq!(wire["content"], case["expectedContent"]);
        assert_eq!(usage, case["expectedUsage"]);
        assert_eq!(wire["stopReason"], case["expectedStop"]);
    }
}

#[test]
fn invalid_wire_numbers_fail_at_both_slot_and_usage_boundaries() {
    for raw in [
        "-1.0",
        "1.5",
        "1e-2",
        "9007199254740992.0",
        "9007199254740992",
        "true",
        "\"1\"",
    ] {
        for event in [
            format!(
                r#"{{"type":"response.output_item.added","output_index":{raw},"item":{{"type":"message","id":"m"}}}}"#
            ),
            format!(
                r#"{{"type":"response.completed","response":{{"status":"completed","usage":{{"input_tokens":{raw}}}}}}}"#
            ),
        ] {
            let token = CancellationToken::new();
            let (_, producer) = stream(&token);
            let mut out = output();
            let mut source = Events::new(vec![serde_json::from_str(&event).unwrap()]);
            assert_eq!(
                ready(process_responses_stream(
                    &mut source,
                    &mut out,
                    &producer,
                    &model(),
                    &OpenAIResponsesStreamOptions::default(),
                    &token
                ))
                .unwrap_err()
                .kind,
                ResponsesErrorKind::InvalidWire,
                "{raw}"
            );
        }
    }
}

#[test]
fn missing_null_usage_and_negative_zero_index_keep_their_distinct_semantics() {
    for (response, expected_input) in [
        (r#"{"status":"completed"}"#, 8),
        (r#"{"status":"completed","usage":null}"#, 8),
        (r#"{"status":"completed","usage":{}}"#, 0),
        (
            r#"{"status":"completed","usage":{"input_tokens":null,"output_tokens":null,"total_tokens":null}}"#,
            0,
        ),
    ] {
        let token = CancellationToken::new();
        let (_, producer) = stream(&token);
        let mut out = output();
        out.usage.input = pi_ai_core::SafeUnsigned::new(8).unwrap();
        let mut source = Events::new(vec![
            serde_json::from_str(&format!(
                r#"{{"type":"response.completed","response":{response}}}"#
            ))
            .unwrap(),
        ]);
        ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token,
        ))
        .unwrap();
        assert_eq!(out.usage.input.get(), expected_input);
    }
    let raw = r#"[{"type":"response.output_item.added","item":{"type":"reasoning","id":"rs","summary":[]}},{"type":"response.output_item.added","output_index":-0.0,"item":{"type":"message","id":"m"}},{"type":"response.output_text.delta","output_index":0e2,"delta":"zero-slot"},{"type":"response.reasoning_text.delta","delta":"missing-slot"},{"type":"response.completed","response":{"status":"completed"}}]"#;
    let token = CancellationToken::new();
    let (_, producer) = stream(&token);
    let mut out = output();
    let mut source = Events::new(serde_json::from_str(raw).unwrap());
    ready(process_responses_stream(
        &mut source,
        &mut out,
        &producer,
        &model(),
        &OpenAIResponsesStreamOptions::default(),
        &token,
    ))
    .unwrap();
    let content = serde_json::to_value(out.content).unwrap();
    assert_eq!(
        content,
        json!([{"type":"thinking","thinking":"missing-slot"},{"type":"text","text":"zero-slot"}])
    );
    let mut out = output();
    let mut source = Events::new(vec![
        json!({"type":"response.output_item.added","output_index":null,"item":{"type":"message","id":"m"}}),
    ]);
    assert_eq!(
        ready(process_responses_stream(
            &mut source,
            &mut out,
            &producer,
            &model(),
            &OpenAIResponsesStreamOptions::default(),
            &token
        ))
        .unwrap_err()
        .kind,
        ResponsesErrorKind::InvalidWire
    );
}
