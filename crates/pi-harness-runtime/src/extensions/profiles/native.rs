//! Construct native DTOs under the captured session and header policies before
//! the protocol owner can publish progress or retain a first terminal.
#[cfg(test)]
use crate::extensions::transport::source_observer;
use pi_agent_schema::*;
use pi_agent_session::codec::SessionCodec;
use pi_ai_core::{
    AssistantMessageEvent, Context, Model, ModelHeaderPolicy, RawModel, SimpleStreamOptionsWire,
    Tool, Usage,
};
use pi_extension_protocol::profiles::{NativeError, NativeValidator, Stage, ValidatedPayload};

pub struct NativeDomain<'a> {
    pub codec: &'a SessionCodec,
    pub model_policy: &'a ModelHeaderPolicy,
    pub expected_model: Option<&'a Model>,
    pub expected_deferred_source: Option<&'a pi_ai_core::AssistantMessage>,
    pub hook_event: Option<HookEvent>,
    pub hook_result: Option<HookResult>,
    pub assistant_event: Option<AssistantMessageEvent>,
    pub assistant_message: Option<SettledAssistantMessage>,
    #[cfg(test)]
    pub(crate) observation_generation: Option<u64>,
    #[cfg(test)]
    pub(crate) observation_identity: Option<String>,
    #[cfg(test)]
    pub(crate) provider_observation:
        Option<std::sync::Arc<super::provider::ProviderRequestObservation>>,
}
impl<'a> NativeDomain<'a> {
    pub fn new(codec: &'a SessionCodec, model_policy: &'a ModelHeaderPolicy) -> Self {
        Self {
            codec,
            model_policy,
            expected_model: None,
            expected_deferred_source: None,
            hook_event: None,
            hook_result: None,
            assistant_event: None,
            assistant_message: None,
            #[cfg(test)]
            observation_generation: None,
            #[cfg(test)]
            observation_identity: None,
            #[cfg(test)]
            provider_observation: None,
        }
    }
    fn messages(&self, messages: &[AgentMessage]) -> Result<(), NativeError> {
        for message in messages {
            self.codec
                .validate_message(message)
                .map_err(|_| NativeError::Rejected)?;
        }
        Ok(())
    }
    fn event(&self, event: &HookEvent) -> Result<(), NativeError> {
        match event {
            HookEvent::BeforeRun(e) => self.messages(&e.prompt)?,
            HookEvent::BeforeResume(BeforeResumeEvent::Run(e)) => self.messages(&e.prompt)?,
            HookEvent::TransformContext(e) => self.messages(&e.messages)?,
            HookEvent::BeforeCompaction(e) => {
                self.messages(&e.preparation.messages_to_summarize)?;
                self.messages(&e.preparation.turn_prefix_messages)?;
                self.messages(&e.preparation.retained_tail)?;
            }
            HookEvent::BeforeNavigation(e) => self.messages(&e.preparation.messages)?,
            _ => {}
        }
        Ok(())
    }
    fn result(&self, result: &HookResult) -> Result<(), NativeError> {
        match result {
            HookResult::BeforeRun(Some(r)) => {
                if let Some(messages) = &r.messages {
                    self.messages(messages)?;
                }
            }
            HookResult::TransformContext(Some(r)) => self.messages(&r.messages)?,
            HookResult::BeforeCompaction(Some(r)) => {
                if let BeforeCompactionResult::Compaction(supply) = r.as_ref() {
                    self.messages(&supply.compaction.retained_tail)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn assistant_identity(
        &self,
        message: &pi_ai_core::AssistantMessage,
    ) -> Result<(), NativeError> {
        if let Some(model) = self.expected_model
            && (message.api != model.api
                || message.provider != model.provider
                || message.model != model.id)
        {
            return Err(NativeError::Rejected);
        }
        Ok(())
    }
    fn snapshot(&self, snapshot: &LaneSnapshot) -> Result<(), NativeError> {
        for item in snapshot
            .queues
            .steer
            .iter()
            .chain(&snapshot.queues.follow_up)
            .chain(&snapshot.queues.next_run)
        {
            self.codec
                .validate_message(&item.message)
                .map_err(|_| NativeError::Rejected)?;
        }
        for write in &snapshot.pending_writes {
            if let PendingWrite::Message(write) = write {
                self.codec
                    .validate_message(&write.message)
                    .map_err(|_| NativeError::Rejected)?;
            }
        }
        Ok(())
    }
}
impl NativeValidator for NativeDomain<'_> {
    fn validate(&mut self, candidate: &ValidatedPayload) -> Result<(), NativeError> {
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "native_validation",
                file!(),
                line!(),
                "NativeDomain::validate",
            );
            event.field = Some("D_req");
            event.owner = Some("NativeDomain");
            event.operation = Some("native_validation");
            event.outcome = Some("start");
            event.jobs = Some(candidate.projections.len());
            event.generation = self.observation_generation;
            event.target = self.observation_identity.clone();
            if candidate.method == "provider.invoke" && candidate.stage == Stage::Terminal {
                if let Some(observation) = &self.provider_observation {
                    observation.annotate_bound(&mut event);
                }
            }
            source_observer::record(event);
            let mut phase = source_observer::SourceEvent::new(
                "native_validation",
                file!(),
                line!(),
                "NativeDomain::validate",
            );
            phase.field = Some("Hpre");
            phase.owner = Some("NativeDomain");
            phase.operation = Some("validation");
            phase.outcome = Some("start");
            phase.jobs = Some(candidate.projections.len());
            phase.generation = self.observation_generation;
            phase.target = self.observation_identity.clone();
            source_observer::record(phase);
        }
        macro_rules! decode {
            ($value:expr, $ty:ty) => {
                serde_json::from_value::<$ty>($value.clone()).map_err(|_| NativeError::Rejected)?
            };
        }
        for (root, value) in &candidate.projections {
            if root.starts_with("hook.input:") {
                let event = decode!(value, RawHookEvent)
                    .into_durable(self.model_policy, self.codec.header_policy())
                    .map_err(|_| NativeError::Rejected)?;
                self.event(&event)?;
                self.hook_event = Some(event);
            } else if root.starts_with("hook.output:") {
                let result = decode!(value, RawHookResult)
                    .into_durable(self.codec.header_policy())
                    .map_err(|_| NativeError::Rejected)?;
                self.result(&result)?;
                self.hook_result = Some(result);
            } else {
                match root.as_str() {
                    "provider.model.v1" => {
                        let model = decode!(value, RawModel)
                            .into_model(self.model_policy)
                            .map_err(|_| NativeError::Rejected)?;
                        if model.headers.is_some()
                            || self
                                .expected_model
                                .is_some_and(|expected| expected != &model)
                        {
                            return Err(NativeError::Rejected);
                        }
                    }
                    "provider.context.v1" => {
                        let context = decode!(value, Context);
                        if context.tools.is_some() {
                            return Err(NativeError::Rejected);
                        }
                        if let Some(source) = self.expected_deferred_source
                            && (context.system_prompt.is_some()
                                || context.messages.as_slice()
                                    != [pi_ai_core::Message::Assistant(Box::new(source.clone()))])
                        {
                            return Err(NativeError::Rejected);
                        }
                        for message in context.messages {
                            self.codec
                                .validate_message(&AgentMessage::BuiltIn(message))
                                .map_err(|_| NativeError::Rejected)?;
                        }
                    }
                    "provider.tools.v1" => {
                        let _ = decode!(value, Vec<Tool>);
                    }
                    "provider.options.v1" => {
                        let _ = decode!(value, SimpleStreamOptionsWire);
                    }
                    "provider.assistant_event.v1" => {
                        let event = decode!(value, AssistantMessageEvent);
                        let partial = match &event {
                            AssistantMessageEvent::Start { partial }
                            | AssistantMessageEvent::TextStart { partial, .. }
                            | AssistantMessageEvent::TextDelta { partial, .. }
                            | AssistantMessageEvent::TextEnd { partial, .. }
                            | AssistantMessageEvent::ThinkingStart { partial, .. }
                            | AssistantMessageEvent::ThinkingDelta { partial, .. }
                            | AssistantMessageEvent::ThinkingEnd { partial, .. }
                            | AssistantMessageEvent::ToolCallStart { partial, .. }
                            | AssistantMessageEvent::ToolCallDelta { partial, .. }
                            | AssistantMessageEvent::ToolCallEnd { partial, .. } => partial,
                            AssistantMessageEvent::Done { .. }
                            | AssistantMessageEvent::Error { .. } => {
                                return Err(NativeError::Rejected);
                            }
                        };
                        self.assistant_identity(partial)?;
                        self.assistant_event = Some(event);
                    }
                    "provider.assistant_message.v1" => {
                        let message = decode!(value, pi_ai_core::AssistantMessage);
                        self.assistant_identity(&message)?;
                        self.assistant_message = Some(
                            SettledAssistantMessage::new(message)
                                .map_err(|_| NativeError::Rejected)?,
                        );
                    }
                    "provider.usage.v1" => {
                        let _ = decode!(value, Usage);
                    }
                    "host.queue_item.v1" => {
                        let message = decode!(value, AgentMessage);
                        self.codec
                            .validate_message(&message)
                            .map_err(|_| NativeError::Rejected)?;
                    }
                    "host.filtered_snapshot.v1" | "event.filtered_snapshot.v1" => {
                        self.snapshot(&decode!(value, LaneSnapshot))?;
                    }
                    "event.filtered_event.v1" => {
                        let _ = decode!(value, HarnessEvent);
                    }
                    "resource.metadata:skill" | "resource.metadata:prompt_template" => {
                        super::resources::metadata(root, value)
                            .map_err(|_| NativeError::Rejected)?;
                    }
                    "host.resource_snapshot.v1" => {
                        super::resources::ResourceValue::from_json(value)
                            .map_err(|_| NativeError::Rejected)?;
                    }
                    "telemetry.record.v1" => {
                        super::telemetry::Record::from_json(value)
                            .map_err(|_| NativeError::Rejected)?;
                    }
                    _ => return Err(NativeError::LocalFault),
                }
            }
        }
        if candidate.method == "resource.list" && candidate.stage == Stage::Terminal {
            for manifest in candidate.fields["resources"]
                .as_array()
                .ok_or(NativeError::Rejected)?
            {
                let kind = manifest["kind"].as_str().ok_or(NativeError::Rejected)?;
                super::resources::metadata(
                    &format!("resource.metadata:{kind}"),
                    &manifest["metadata"],
                )
                .map_err(|_| NativeError::Rejected)?;
            }
        }
        if candidate.method == "provider.invoke"
            && candidate.stage == Stage::Terminal
            && let Some(message) = &self.assistant_message
        {
            let usage: Usage = decode!(&candidate.projections["provider.usage.v1"], Usage);
            if message.as_message().usage != usage {
                return Err(NativeError::Rejected);
            }
        }
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "native_validation",
                file!(),
                line!(),
                "NativeDomain::validate",
            );
            event.field = Some("D_req");
            event.owner = Some("NativeDomain");
            event.operation = Some("native_validation");
            event.outcome = Some("complete");
            event.jobs = Some(candidate.projections.len());
            event.generation = self.observation_generation;
            event.target = self.observation_identity.clone();
            if candidate.method == "provider.invoke" && candidate.stage == Stage::Terminal {
                if let Some(observation) = &self.provider_observation {
                    observation.annotate_bound(&mut event);
                }
            }
            source_observer::record(event);
            let mut phase = source_observer::SourceEvent::new(
                "native_validation",
                file!(),
                line!(),
                "NativeDomain::validate",
            );
            phase.field = Some("Hpre");
            phase.owner = Some("NativeDomain");
            phase.operation = Some("validation");
            phase.outcome = Some("complete");
            phase.jobs = Some(candidate.projections.len());
            phase.generation = self.observation_generation;
            phase.target = self.observation_identity.clone();
            source_observer::record(phase);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceError;
