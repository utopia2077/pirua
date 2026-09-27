use super::registry::{Callback, CapturedHookPipeline, Entry};
use super::{apply_stream_options_patch, failure, fault};
use pi_agent_schema::*;
use pi_agent_session::codec::SessionCodec;
use pi_ai_core::{JsonObject, JsonValue, Presence, StopReason, Tool, ToolCall, validate_tool_call};
use pi_telemetry::{
    AttributeValue, SpanAttributes, SpanGuard, SpanOptions, SpanStatus, TelemetryContext,
};
use std::{
    future::poll_fn,
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};

/// Passive recording port, separate from event listener dispatch.
/// Implementations must absorb observer failures and never change control flow.
pub trait HookErrorSink: Send + Sync {
    /// Records one failed invocation in pipeline order; no listener is awaited.
    fn report(&self, event: LaneHookHandlerErrorEvent);
}

/// Explicit host/session bindings for one already-authorized invocation.
pub struct StartedHookContext<'a> {
    /// Actual Session codec, including registered custom-role schemas.
    pub codec: &'a SessionCodec,
    /// Current host header policy, associated by the caller with this codec.
    pub header_policy: &'a ProviderHeaderPolicy,
    /// Exact selected tool declaration; required for before_tool only.
    pub tool: Option<&'a Tool>,
    /// Observation only; an already-started pipeline is not interrupted.
    pub cancellation: &'a dyn Cancellation,
    /// Explicit parent context for each handler span.
    pub telemetry: &'a dyn TelemetryContext,
    /// Whether the supplied operation id is already durable/accepted.
    pub operation_accepted: bool,
    /// Whether handler errors belong to recovery event publication.
    pub recovery: bool,
    /// Saved map projected independently by stable id for before_resume.
    pub resume_data: &'a JsonObject,
    /// Passive handler-error recording port.
    pub errors: &'a dyn HookErrorSink,
}

#[derive(Clone, Debug, PartialEq)]
enum Output {
    Request(AgentHarnessStreamOptions),
    Result(HookResult),
}
/// Complete process-local output. A request pipeline returns effective options,
/// never a fabricated single patch. Other families retain HookResult aggregation.
#[derive(Clone, Debug, PartialEq)]
pub struct HookPipelineOutput(Output);
impl HookPipelineOutput {
    /// Construct a matching non-request result, rejecting request-patch transport.
    pub fn from_result(name: HookName, result: HookResult) -> Result<Self, HarnessRuntimeError> {
        if result.name() != name || name == HookName::BeforeRequest {
            return Err(fault());
        }
        Ok(Self(Output::Result(result)))
    }
    /// Construct the distinct complete before_request output.
    pub fn before_request(options: AgentHarnessStreamOptions) -> Self {
        Self(Output::Request(options))
    }
    /// Family of this complete output.
    pub fn name(&self) -> HookName {
        match &self.0 {
            Output::Request(_) => HookName::BeforeRequest,
            Output::Result(r) => r.name(),
        }
    }
    /// Complete effective request options, if this is before_request.
    pub fn request_options(&self) -> Option<&AgentHarnessStreamOptions> {
        match &self.0 {
            Output::Request(o) => Some(o),
            _ => None,
        }
    }
    /// Aggregate for the other ten families. Before-run resume data is id-keyed.
    pub fn result(&self) -> Option<&HookResult> {
        match &self.0 {
            Output::Result(r) => Some(r),
            _ => None,
        }
    }
}

impl CapturedHookPipeline {
    /// Invoke a matching captured pipeline exactly once in order. This method
    /// assumes the caller has authorized start; its output grants no settlement.
    pub async fn run_started(
        &self,
        event: HookEvent,
        context: &StartedHookContext<'_>,
    ) -> Result<HookPipelineOutput, HarnessRuntimeError> {
        self.run_started_inner(
            event,
            context,
            #[cfg(feature = "extensions")]
            None,
        )
        .await
    }
    #[cfg(feature = "extensions")]
    pub(crate) async fn run_started_scoped(
        &self,
        event: HookEvent,
        context: &StartedHookContext<'_>,
        scope: &dyn crate::extensions::profiles::hooks::ScopeAuthority,
    ) -> Result<HookPipelineOutput, HarnessRuntimeError> {
        self.run_started_inner(event, context, Some(scope)).await
    }
    async fn run_started_inner(
        &self,
        mut event: HookEvent,
        context: &StartedHookContext<'_>,
        #[cfg(feature = "extensions")] scope: Option<
            &dyn crate::extensions::profiles::hooks::ScopeAuthority,
        >,
    ) -> Result<HookPipelineOutput, HarnessRuntimeError> {
        if event.name() != self.name {
            return Err(fault());
        }
        validate_initial(&event, context).map_err(|_| fault())?;
        let mut aggregate = initial_result(self.name);
        for entry in &self.entries {
            let span = start_span(entry, &event, context);
            let invocation =
                resume_invocation(event.clone(), entry.id.as_ref(), context.resume_data);
            #[cfg(feature = "extensions")]
            let result = if let Callback::Remote(remote) = &entry.callback {
                match scope {
                    Some(scope) => remote.call(invocation, context, scope).await?,
                    None => Err(failure("remote hook has no runtime owner")),
                }
            } else {
                invoke(&entry.callback, invocation).await
            };
            #[cfg(not(feature = "extensions"))]
            let result = invoke(&entry.callback, invocation).await;
            let accepted = result.and_then(|result| {
                let mut candidate = event.clone();
                let mut candidate_aggregate = aggregate.clone();
                let outcome = apply(
                    &mut candidate,
                    &mut candidate_aggregate,
                    result,
                    entry.id.as_ref(),
                    context,
                )?;
                event = candidate;
                aggregate = candidate_aggregate;
                Ok(outcome)
            });
            match accepted {
                Ok((outcome, stop)) => {
                    finish_span(span, outcome, false);
                    if stop {
                        break;
                    }
                }
                Err(error) => {
                    context.errors.report(LaneHookHandlerErrorEvent {
                        event_type: Default::default(),
                        lane: correlation(&event).0.clone(),
                        recovery: context.recovery.then_some(Default::default()),
                        error: error.to_string(),
                        stack: None,
                        kind: Default::default(),
                        hook: hook_text(self.name).into(),
                    });
                    finish_span(span, "failed", true);
                    if self.name == HookName::BeforeTool {
                        let HookResult::BeforeTool(previous) = &mut aggregate else {
                            unreachable!()
                        };
                        previous.get_or_insert_with(Default::default).block =
                            Some(BeforeToolBlock {
                                reason: "before_tool handler failed".into(),
                                terminate: None,
                            });
                        break;
                    }
                }
            }
        }
        if let HookEvent::BeforeRequest(event) = event {
            Ok(HookPipelineOutput::before_request(event.stream_options))
        } else {
            HookPipelineOutput::from_result(self.name, aggregate)
        }
    }
}
#[cfg(feature = "extensions")]
pub(crate) fn validate_remote_result(
    event: &HookEvent,
    result: &HookResult,
    id: Option<&HandlerId>,
    context: &StartedHookContext<'_>,
) -> Result<(), HandlerError> {
    let mut candidate = event.clone();
    let mut aggregate = initial_result(event.name());
    apply(&mut candidate, &mut aggregate, result.clone(), id, context).map(|_| ())
}

async fn invoke(callback: &Callback, event: HookEvent) -> Result<HookResult, HandlerError> {
    // Construct and poll the user's future within separate unwind boundaries.
    macro_rules! call {
        ($handler:expr, $event:expr, $map:expr) => {{
            let mut future = catch_unwind(AssertUnwindSafe(|| $handler.call($event)))
                .map_err(|_| failure("hook handler panicked"))?;
            let result =
                poll_fn(
                    |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                        Ok(value) => value,
                        Err(_) => Poll::Ready(Err(failure("hook handler panicked"))),
                    },
                )
                .await?;
            Ok(($map)(result))
        }};
    }
    match (callback, event) {
        #[cfg(feature = "extensions")]
        (Callback::Remote(_), _) => Err(failure("remote hook has no runtime owner")),
        (Callback::BeforeRun(h), HookEvent::BeforeRun(e)) => call!(h, e, HookResult::BeforeRun),
        (Callback::BeforeResume(h), HookEvent::BeforeResume(e)) => {
            call!(h, e, |()| HookResult::BeforeResume)
        }
        (Callback::BeforeRunEnd(h), HookEvent::BeforeRunEnd(e)) => {
            call!(h, e, HookResult::BeforeRunEnd)
        }
        (Callback::TransformContext(h), HookEvent::TransformContext(e)) => {
            call!(h, e, HookResult::TransformContext)
        }
        (Callback::BeforeRequest(h), HookEvent::BeforeRequest(e)) => {
            call!(h, *e, HookResult::BeforeRequest)
        }
        (Callback::BeforePayload(h), HookEvent::BeforePayload(e)) => {
            call!(h, *e, HookResult::BeforePayload)
        }
        (Callback::AfterResponse(h), HookEvent::AfterResponse(e)) => {
            call!(h, *e, |r: Option<AfterResponseResult>| {
                HookResult::AfterResponse(r.map(Box::new))
            })
        }
        (Callback::BeforeTool(h), HookEvent::BeforeTool(e)) => call!(h, e, HookResult::BeforeTool),
        (Callback::AfterTool(h), HookEvent::AfterTool(e)) => call!(h, e, HookResult::AfterTool),
        (Callback::BeforeCompaction(h), HookEvent::BeforeCompaction(e)) => {
            call!(h, *e, |r: Option<BeforeCompactionResult>| {
                HookResult::BeforeCompaction(r.map(Box::new))
            })
        }
        (Callback::BeforeNavigation(h), HookEvent::BeforeNavigation(e)) => {
            call!(h, *e, HookResult::BeforeNavigation)
        }
        _ => Err(failure("hook family mismatch")),
    }
}
fn initial_result(name: HookName) -> HookResult {
    match name {
        HookName::BeforeRun => HookResult::BeforeRun(None),
        HookName::BeforeResume => HookResult::BeforeResume,
        HookName::BeforeRunEnd => HookResult::BeforeRunEnd(None),
        HookName::TransformContext => HookResult::TransformContext(None),
        HookName::BeforeRequest => HookResult::BeforeRequest(None),
        HookName::BeforePayload => HookResult::BeforePayload(None),
        HookName::AfterResponse => HookResult::AfterResponse(None),
        HookName::BeforeTool => HookResult::BeforeTool(None),
        HookName::AfterTool => HookResult::AfterTool(None),
        HookName::BeforeCompaction => HookResult::BeforeCompaction(None),
        HookName::BeforeNavigation => HookResult::BeforeNavigation(None),
    }
}
fn hook_text(name: HookName) -> &'static str {
    match name {
        HookName::BeforeRun => "before_run",
        HookName::BeforeResume => "before_resume",
        HookName::BeforeRunEnd => "before_run_end",
        HookName::TransformContext => "transform_context",
        HookName::BeforeRequest => "before_request",
        HookName::BeforePayload => "before_payload",
        HookName::AfterResponse => "after_response",
        HookName::BeforeTool => "before_tool",
        HookName::AfterTool => "after_tool",
        HookName::BeforeCompaction => "before_compaction",
        HookName::BeforeNavigation => "before_navigation",
    }
}
fn correlation(event: &HookEvent) -> (&LaneId, &OperationId) {
    match event {
        HookEvent::BeforeRun(e) => (&e.lane, &e.run_id),
        HookEvent::BeforeResume(e) => match e {
            BeforeResumeEvent::Run(e) => (&e.lane, &e.run_id),
            BeforeResumeEvent::Compaction(e) => (&e.lane, &e.run_id),
            BeforeResumeEvent::Navigation(e) => (&e.lane, &e.run_id),
        },
        HookEvent::BeforeRunEnd(e) => (&e.lane, &e.run_id),
        HookEvent::TransformContext(e) => (&e.lane, &e.run_id),
        HookEvent::BeforeRequest(e) => (&e.lane, &e.run_id),
        HookEvent::BeforePayload(e) => (&e.lane, &e.run_id),
        HookEvent::AfterResponse(e) => (&e.lane, &e.run_id),
        HookEvent::BeforeTool(e) => (&e.lane, &e.run_id),
        HookEvent::AfterTool(e) => (&e.lane, &e.run_id),
        HookEvent::BeforeCompaction(e) => (&e.lane, &e.run_id),
        HookEvent::BeforeNavigation(e) => (&e.lane, &e.run_id),
    }
}
fn resume_invocation(
    mut event: HookEvent,
    id: Option<&HandlerId>,
    saved: &JsonObject,
) -> HookEvent {
    if let HookEvent::BeforeResume(e) = &mut event {
        let data = match id.and_then(|id| saved.as_map().get(id.as_str())) {
            None => Presence::Missing,
            Some(JsonValue::Null) => Presence::Null,
            Some(value) => Presence::Value(value.clone()),
        };
        match e {
            BeforeResumeEvent::Run(e) => e.resume_data = data,
            BeforeResumeEvent::Compaction(e) => e.resume_data = data,
            BeforeResumeEvent::Navigation(e) => e.resume_data = data,
        }
    }
    event
}
fn messages(values: &[AgentMessage], context: &StartedHookContext<'_>) -> Result<(), HandlerError> {
    for value in values {
        context
            .codec
            .validate_message(value)
            .map_err(|_| failure("hook message validation failed"))?;
    }
    Ok(())
}
fn tool_args(
    event: &BeforeToolEvent,
    args: &JsonObject,
    context: &StartedHookContext<'_>,
) -> Result<JsonObject, HandlerError> {
    let tool = context
        .tool
        .filter(|t| t.name == event.tool_name)
        .ok_or_else(|| failure("hook tool binding mismatch"))?;
    validate_tool_call(
        std::slice::from_ref(tool),
        &ToolCall {
            content_type: Default::default(),
            id: event.tool_call_id.clone(),
            name: event.tool_name.clone(),
            arguments: args.clone(),
            thought_signature: None,
            namespace: None,
        },
    )
    .map_err(|_| failure("hook tool argument validation failed"))
}
fn validate_initial(
    event: &HookEvent,
    context: &StartedHookContext<'_>,
) -> Result<(), HandlerError> {
    match event {
        HookEvent::BeforeRun(e) => messages(&e.prompt, context),
        HookEvent::BeforeResume(BeforeResumeEvent::Run(e)) => messages(&e.prompt, context),
        HookEvent::BeforeRunEnd(e) => messages(&e.messages, context),
        HookEvent::TransformContext(e) => messages(&e.messages, context),
        HookEvent::BeforeRequest(e) => {
            apply_stream_options_patch(
                &e.stream_options,
                &Default::default(),
                context.header_policy,
            )
            .map_err(|_| failure("hook request option validation failed"))?;
            Ok(())
        }
        HookEvent::BeforeTool(e) => {
            // Intake validation does not manufacture a handler replacement.
            tool_args(e, &e.args, context)?;
            Ok(())
        }
        HookEvent::BeforeCompaction(e) => {
            messages(&e.preparation.messages_to_summarize, context)?;
            messages(&e.preparation.turn_prefix_messages, context)?;
            messages(&e.preparation.retained_tail, context)
        }
        HookEvent::BeforeNavigation(e) => messages(&e.preparation.messages, context),
        _ => Ok(()),
    }
}
fn start_span(entry: &Entry, event: &HookEvent, context: &StartedHookContext<'_>) -> SpanGuard {
    let (lane, run) = correlation(event);
    let mut attributes = SpanAttributes::from([
        (
            "pi.lane.name".into(),
            AttributeValue::String(lane.as_str().into()),
        ),
        (
            "pi.hook.name".into(),
            AttributeValue::String(hook_text(event.name()).into()),
        ),
    ]);
    if context.operation_accepted {
        attributes.insert(
            "pi.operation.id".into(),
            AttributeValue::String(run.as_str().into()),
        );
    }
    if let Some(id) = &entry.id {
        attributes.insert(
            "pi.hook.registration_id".into(),
            AttributeValue::String(id.as_str().into()),
        );
    }
    context.telemetry.start_span(SpanOptions {
        name: "pi.harness.hook".into(),
        attributes,
    })
}
fn finish_span(span: SpanGuard, outcome: &str, failed: bool) {
    span.set_attributes(&SpanAttributes::from([(
        "pi.hook.outcome".into(),
        AttributeValue::String(outcome.into()),
    )]));
    span.set_status(if failed {
        SpanStatus::Error { error: None }
    } else {
        SpanStatus::Ok
    });
    span.finish();
}

fn apply(
    event: &mut HookEvent,
    aggregate: &mut HookResult,
    result: HookResult,
    id: Option<&HandlerId>,
    context: &StartedHookContext<'_>,
) -> Result<(&'static str, bool), HandlerError> {
    // All mutations here target detached state; the caller publishes only on Ok.
    match (event, aggregate, result) {
        (HookEvent::BeforeRun(e), HookResult::BeforeRun(a), HookResult::BeforeRun(Some(r))) => {
            if let Some(m) = &r.messages {
                messages(m, context)?;
            }
            let a = a.get_or_insert_with(Default::default);
            if let Some(m) = r.messages {
                e.prompt.extend(m.clone());
                a.messages.get_or_insert_with(Vec::new).extend(m);
            }
            if let Some(p) = r.system_prompt {
                e.system_prompt = p.clone();
                a.system_prompt = Some(p);
            }
            if !r.resume_data.is_missing() {
                let id = id.ok_or_else(|| failure("missing stable hook id"))?;
                let mut saved = match &a.resume_data {
                    Presence::Value(JsonValue::Object(o)) => o.as_map().clone(),
                    _ => Default::default(),
                };
                saved.insert(
                    id.as_str().into(),
                    match r.resume_data {
                        Presence::Value(v) => v,
                        _ => JsonValue::Null,
                    },
                );
                a.resume_data = Presence::Value(JsonValue::Object(JsonObject::new(saved)));
            }
        }
        (_, HookResult::BeforeResume, HookResult::BeforeResume) => (),
        (_, HookResult::BeforeRunEnd(a), HookResult::BeforeRunEnd(Some(r))) => {
            let a = a.get_or_insert_with(Default::default);
            if r.follow_up.is_some() {
                a.follow_up = r.follow_up;
            }
        }
        (
            HookEvent::TransformContext(e),
            HookResult::TransformContext(a),
            HookResult::TransformContext(Some(r)),
        ) => {
            messages(&r.messages, context)?;
            e.messages = r.messages.clone();
            *a = Some(r);
        }
        (HookEvent::BeforeRequest(e), _, HookResult::BeforeRequest(Some(r))) => {
            if let Some(patch) = r.stream_options {
                e.stream_options =
                    apply_stream_options_patch(&e.stream_options, &patch, context.header_policy)
                        .map_err(|_| failure("hook request option validation failed"))?;
            }
        }
        (
            HookEvent::BeforePayload(e),
            HookResult::BeforePayload(a),
            HookResult::BeforePayload(Some(r)),
        ) => {
            e.payload = r.payload.clone();
            *a = Some(r);
        }
        (
            HookEvent::AfterResponse(e),
            HookResult::AfterResponse(a),
            HookResult::AfterResponse(Some(r)),
        ) => {
            if let Some(message) = &r.message {
                if message.as_message().stop_reason == StopReason::Aborted
                    && !context.cancellation.is_cancelled()
                {
                    return Err(failure("aborted hook replacement requires cancellation"));
                }
                e.message = message.clone();
                a.get_or_insert_with(Default::default).message = Some(message.clone());
            } else {
                a.get_or_insert_with(Default::default);
            }
        }
        (
            HookEvent::BeforeTool(e),
            HookResult::BeforeTool(a),
            HookResult::BeforeTool(Some(mut r)),
        ) => {
            if let Some(args) = &r.args {
                r.args = Some(tool_args(e, args, context)?);
            }
            let a = a.get_or_insert_with(Default::default);
            if let Some(args) = r.args {
                e.args = args.clone();
                a.args = Some(args);
            }
            if let Some(block) = r.block {
                a.block = Some(block);
                return Ok(("blocked", true));
            }
        }
        (HookEvent::AfterTool(e), HookResult::AfterTool(a), HookResult::AfterTool(Some(r))) => {
            let a = a.get_or_insert_with(Default::default);
            if let Some(content) = r.content {
                e.content = content.clone();
                a.content = Some(content);
            }
            if !r.details.is_missing() {
                e.details = r.details.clone();
                a.details = r.details;
            }
            if let Some(error) = r.is_error {
                e.is_error = error;
                a.is_error = Some(error);
            }
            if let Some(usage) = r.usage {
                e.usage = Some(usage.clone());
                a.usage = Some(usage);
            }
            if let Some(terminate) = r.terminate {
                a.terminate = Some(terminate);
            }
        }
        (_, HookResult::BeforeCompaction(a), HookResult::BeforeCompaction(Some(r))) => {
            if let BeforeCompactionResult::Compaction(s) = r.as_ref() {
                messages(&s.compaction.retained_tail, context)?;
            }
            let stop = matches!(
                r.as_ref(),
                BeforeCompactionResult::DeclineTrue(_) | BeforeCompactionResult::Compaction(_)
            );
            *a = Some(r);
            return Ok(("completed", stop));
        }
        (_, HookResult::BeforeNavigation(a), HookResult::BeforeNavigation(Some(r))) => {
            let stop = matches!(
                r,
                BeforeNavigationResult::DeclineTrue(_) | BeforeNavigationResult::Summary(_)
            );
            *a = Some(r);
            return Ok(("completed", stop));
        }
        (
            _,
            _,
            HookResult::BeforeRun(None)
            | HookResult::BeforeRunEnd(None)
            | HookResult::TransformContext(None)
            | HookResult::BeforeRequest(None)
            | HookResult::BeforePayload(None)
            | HookResult::AfterResponse(None)
            | HookResult::BeforeTool(None)
            | HookResult::AfterTool(None)
            | HookResult::BeforeCompaction(None)
            | HookResult::BeforeNavigation(None),
        ) => return Ok(("skipped", false)),
        _ => return Err(failure("hook family mismatch")),
    }
    Ok(("completed", false))
}
