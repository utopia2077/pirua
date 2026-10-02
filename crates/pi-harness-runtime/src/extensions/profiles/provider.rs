//! Auth-none catalog availability and the Harness-owned keyed request source.
use super::{
    connection::{CallScope, ProfileConnection},
    native::NativeDomain,
};
use crate::{effects::RuntimeEffects, owner::RuntimeHarness, request::KeyedRequestSource};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::*;
use pi_extension_protocol::{
    profile_live::{Role, encode_payload},
    profiles::wire_v1_1 as dto,
};
use std::sync::Arc;

#[cfg(test)]
use crate::extensions::transport::source_observer;

#[derive(Clone)]
pub(crate) struct RegisteredProvider {
    pub connection: Arc<ProfileConnection>,
    pub capability: dto::CapabilityRef,
    pub model_policy: ModelHeaderPolicy,
    pub models: Vec<Model>,
    pub registration: ProviderRegistration,
    pub payload_schema: Option<pi_extension_protocol::jcs::PortableSchema>,
}
struct CatalogProvider {
    models: Arc<[Model]>,
    auth: ProviderAuth,
}
impl Provider for CatalogProvider {
    fn id(&self) -> &ProviderId {
        &self.models[0].provider
    }
    fn name(&self) -> &str {
        "protocol provider"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(self.models.clone())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        None
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeHarness<C, S, M>
{
    /// Catalog registration makes an explicit no-auth provider available. Only
    /// native keyed Assistant/Summary owners can dispatch this registration.
    pub async fn register_profile_provider(
        &self,
        connection: Arc<ProfileConnection>,
        capability: dto::CapabilityRef,
        models: Vec<Model>,
        model_policy: ModelHeaderPolicy,
        payload_schema: Option<pi_extension_protocol::jcs::PortableSchema>,
    ) -> Result<(), HarnessRuntimeError> {
        #[cfg(test)]
        let observed_provider_name = capability.name.clone();
        #[cfg(test)]
        let observed_model_name = models.first().map(|model| model.id.as_str().to_owned());
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "provider_register",
                file!(),
                line!(),
                "RuntimeHarness::register_profile_provider",
            );
            event.outcome = Some("start");
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("ProviderRegistry");
            event.provider = Some(observed_provider_name.clone());
            event.model = observed_model_name.clone();
            source_observer::record(event);
        }
        if models.is_empty()
            || capability.kind != "provider"
            || !connection
                .grant()
                .profiles()
                .iter()
                .any(|p| p.reference() == &capability)
            || models.iter().any(|model| {
                model.headers.is_some()
                    || model.provider != models[0].provider
                    || model.provider.as_str() != capability.name
            })
        {
            return Err(super::fault());
        }
        let provider = models[0].provider.clone();
        self.main
            .owner
            .models
            .set_provider(Arc::new(CatalogProvider {
                models: models.clone().into(),
                auth: ProviderAuth::no_auth(),
            }))
            .await
            .map_err(|_| super::fault())?;
        let registration = self
            .main
            .owner
            .models
            .provider_registration(&provider)
            .ok_or_else(super::fault)?;
        let _settings = self.main.owner.settings_line.acquire().await?;
        self.main
            .owner
            .profile_providers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                provider,
                RegisteredProvider {
                    connection,
                    capability,
                    model_policy,
                    models,
                    registration,
                    payload_schema,
                },
            );
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "provider_register",
                file!(),
                line!(),
                "RuntimeHarness::register_profile_provider",
            );
            event.outcome = Some("ok");
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("ProviderRegistry");
            event.provider = Some(observed_provider_name);
            event.model = observed_model_name;
            source_observer::record(event);
        }
        Ok(())
    }
}
pub(crate) struct Source<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub fx: Arc<RuntimeEffects<C, S, M>>,
    pub key: EffectKey,
    pub provider: RegisteredProvider,
    pub deferred_source: Option<(EntryId, AssistantMessage)>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    KeyedRequestSource for Source<C, S, M>
{
    fn stream(
        &self,
        model: Model,
        context: Context,
        overrides: ModelsRequestOverrides,
        options: SimpleStreamOptions,
        lifecycle: RequestLifecycleOptions,
    ) -> pi_agent_schema::BoxFuture<'_, Result<ObservedAssistantStream, HarnessRuntimeError>> {
        Box::pin(async move {
            let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
                api: model.api.clone(),
                provider: model.provider.clone(),
                model: model.id.clone(),
                clock: options.runtime.clock.clone(),
                cancellation: options.runtime.cancellation.clone(),
            });
            let metadata = RequestLifecycleInvocation::new(Default::default()).1;
            if model.headers.is_some()
                || !overrides.headers.as_map().is_empty()
                || overrides.auth.api_key.is_some()
                || overrides.auth.env.is_some()
                || overrides.auth.min_oauth_validity_ms.is_some()
                || overrides.runtime_endpoint.is_some()
            {
                producer.fail_setup(StreamSetupFailure::Authentication).ok();
                return Ok(ObservedAssistantStream { stream, metadata });
            }
            let fx = self.fx.clone();
            let key = self.key.clone();
            let provider = self.provider.clone();
            let deferred_source = self.deferred_source.clone();
            let host = options.runtime.spawner.clone();
            let task = Box::pin(async move {
                let result = invoke(
                    &fx,
                    &key,
                    &provider,
                    &model,
                    context,
                    options,
                    lifecycle,
                    deferred_source.as_ref(),
                )
                .await;
                match result {
                    Ok(message) => {
                        producer.push(terminal(message.into_message())).ok();
                    }
                    Err(error) => {
                        let _ = fx.lane.owner.observe::<()>(Err(error));
                        producer.fail_setup(StreamSetupFailure::Adapter).ok();
                    }
                }
            });
            host.spawn(task).map_err(|_| super::fault())?;
            Ok(ObservedAssistantStream { stream, metadata })
        })
    }
}
struct ProviderDispatch<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: std::sync::Weak<RuntimeEffects<C, S, M>>,
    identity: Arc<()>,
    key: EffectKey,
    source: Option<(EntryId, AssistantMessage)>,
    provider: ProviderId,
    registration: ProviderRegistration,
    cancel: CancellationToken,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    super::connection::DispatchScope for ProviderDispatch<C, S, M>
{
    fn prepare(
        &self,
    ) -> pi_agent_schema::BoxFuture<'_, Result<crate::scheduler::MutationPermit, HarnessRuntimeError>>
    {
        Box::pin(async move {
            let fx = self.fx.upgrade().ok_or_else(super::fault)?;
            let line = fx.lane.lane.line.acquire().await?;
            let h = fx.lane.load().await?;
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "source_current",
                    file!(),
                    line!(),
                    "ProviderDispatch::prepare",
                );
                event.outcome = Some("check");
                event.field = Some("D_req");
                event.owner = Some("ProviderDispatch");
                event.provider = Some(self.provider.as_str().to_owned());
                source_observer::record(event);
            }
            if !Arc::ptr_eq(&fx.identity, &self.identity)
                || self.cancel.is_cancelled()
                || fx.retired.is_cancelled()
                || !source_current(&fx, &h, &self.key, self.source.as_ref())
                || fx
                    .lane
                    .owner
                    .models
                    .provider_registration(&self.provider)
                    .is_none_or(|registration| !registration.same_registration(&self.registration))
            {
                return Err(super::fault());
            }
            Ok(line)
        })
    }
}
// Keep the captured native authority and independent finite route inputs explicit.
#[allow(clippy::too_many_arguments)]
async fn invoke<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>(
    fx: &Arc<RuntimeEffects<C, S, M>>,
    key: &EffectKey,
    provider: &RegisteredProvider,
    model: &Model,
    mut context: Context,
    options: SimpleStreamOptions,
    lifecycle: RequestLifecycleOptions,
    deferred_source: Option<&(EntryId, AssistantMessage)>,
) -> Result<SettledAssistantMessage, HarnessRuntimeError> {
    #[cfg(test)]
    {
        let mut event =
            source_observer::SourceEvent::new("provider", file!(), line!(), "provider::invoke");
        event.outcome = Some("start");
        event.field = Some("D_req");
        event.owner = Some("ProviderDispatch");
        event.provider = Some(model.provider.as_str().to_owned());
        event.model = Some(model.id.as_str().to_owned());
        source_observer::record(event);
    }
    let tools = context.tools.take().unwrap_or_default();
    let mut raw =
        serde_json::json!({"model":model,"context":context,"tools":tools,"options":options.wire});
    if let Some(transform) = lifecycle.payload {
        let schema = provider.payload_schema.as_ref().ok_or_else(super::fault)?;
        schema
            .validate_instance(
                &serde_json::to_vec(&raw).map_err(|_| super::fault())?,
                pi_extension_protocol::jcs::Limits::default(),
            )
            .map_err(|_| super::fault())?;
        if let Some(replacement) = transform
            .transform(model, &mut raw)
            .await
            .map_err(|_| super::fault())?
        {
            raw = replacement;
        }
        schema
            .validate_instance(
                &serde_json::to_vec(&raw).map_err(|_| super::fault())?,
                pi_extension_protocol::jcs::Limits::default(),
            )
            .map_err(|_| super::fault())?;
    }
    let contract = provider
        .connection
        .grant()
        .profiles()
        .iter()
        .find(|p| p.reference() == &provider.capability)
        .ok_or_else(super::fault)?;
    let registry = contract.approved().registry();
    let payload = encode_payload(
        &dto::InvokeProviderRequest {
            model_id: model.id.as_str().into(),
            auth_mode: "none".into(),
            model: Some(
                super::bound(registry, "provider.model.v1", &raw["model"])
                    .map_err(|_| super::fault())?,
            ),
            context: Some(
                super::bound(registry, "provider.context.v1", &raw["context"])
                    .map_err(|_| super::fault())?,
            ),
            tools: Some(
                super::bound(registry, "provider.tools.v1", &raw["tools"])
                    .map_err(|_| super::fault())?,
            ),
            options: Some(
                super::bound(registry, "provider.options.v1", &raw["options"])
                    .map_err(|_| super::fault())?,
            ),
        },
        provider.connection.max_payload_bytes(),
    )
    .map_err(|_| super::fault())?;
    #[cfg(test)]
    {
        let mut event =
            source_observer::SourceEvent::new("provider", file!(), line!(), "provider::invoke");
        event.outcome = Some("payload_encoded");
        event.field = Some("D_req");
        event.owner = Some("ProviderDispatch");
        event.provider = Some(model.provider.as_str().to_owned());
        event.model = Some(model.id.as_str().to_owned());
        event.body_len = Some(payload.len());
        source_observer::record(event);
    }
    let cancellation = options.runtime.cancellation.clone();
    let slot = provider.connection.acquire_slot(&cancellation).await?;
    let line = fx.lane.lane.line.acquire().await?;
    let h = fx.lane.load().await?;
    if !source_current(fx, &h, key, deferred_source)
        || cancellation.is_cancelled()
        || fx
            .lane
            .owner
            .models
            .provider_registration(&model.provider)
            .is_none_or(|registration| !registration.same_registration(&provider.registration))
    {
        return Err(super::fault());
    }
    #[cfg(test)]
    {
        let mut event = source_observer::SourceEvent::new(
            "source_current",
            file!(),
            line!(),
            "provider::invoke",
        );
        event.outcome = Some("check");
        event.field = Some("D_req");
        event.owner = Some("ProviderDispatch");
        event.provider = Some(model.provider.as_str().to_owned());
        event.model = Some(model.id.as_str().to_owned());
        source_observer::record(event);
    }
    let mut native = NativeDomain::new(fx.lane.owner.session.codec(), &provider.model_policy);
    native.expected_model = Some(model);
    native.expected_deferred_source = deferred_source.map(|(_, message)| message);
    let actions = super::actions::LaneActions {
        lane: fx.lane.clone(),
        resources: fx.profile_resources.clone(),
        registry: registry.clone(),
        max_bytes: provider.connection.max_payload_bytes(),
        scope: super::actions::scope(fx, key.operation_id.clone(), Some(key.clone()), true),
    };
    let reply = provider
        .connection
        .call(
            provider.capability.clone(),
            "provider.invoke",
            payload,
            CallScope {
                key: Some(key.clone()),
                role: Role::Planned,
                parent: None,
                native_parent: None,
                permit: Some(line),
                slot: Some(slot),
                dispatch: Some(Arc::new(ProviderDispatch {
                    fx: Arc::downgrade(fx),
                    identity: fx.identity.clone(),
                    key: key.clone(),
                    source: deferred_source.cloned(),
                    provider: model.provider.clone(),
                    registration: provider.registration.clone(),
                    cancel: cancellation.clone(),
                })),
            },
            &mut native,
            Some(Arc::new(actions)),
            &cancellation,
        )
        .await?;
    let _line = fx.lane.lane.line.acquire().await?;
    let h = fx.lane.load().await?;
    if !source_current(fx, &h, key, deferred_source) {
        provider.connection.retire(
            &reply.binding,
            pi_extension_protocol::profile_live::RetireReason::NoLongerExpected,
        )?;
        return Err(super::fault());
    }
    #[cfg(test)]
    {
        let mut event =
            source_observer::SourceEvent::new("terminal", file!(), line!(), "provider::invoke");
        event.outcome = Some("ok");
        event.field = Some("EH_inline_target_provider_allocator");
        event.owner = Some("ProviderDispatch");
        event.operation = Some("provider.invoke");
        event.provider = Some(model.provider.as_str().to_owned());
        event.model = Some(model.id.as_str().to_owned());
        event.request_id = Some(reply.binding.id());
        source_observer::record(event);
    }
    fx.profile_receipts
        .retain_provider(key, provider.connection.clone(), &reply);
    if reply.error.is_some() {
        return crate::provider::synthetic_response(
            model.api.clone(),
            model.provider.clone(),
            model.id.clone(),
            SafeUnsigned::new(fx.lane.owner.clock.now_unix_ms()).map_err(|_| super::fault())?,
            false,
            "request failed".into(),
        );
    }

    native.assistant_message.take().ok_or_else(super::fault)
}
fn terminal(message: AssistantMessage) -> AssistantMessageEvent {
    let message = Box::new(message);
    match message.stop_reason {
        StopReason::Error => AssistantMessageEvent::Error {
            reason: ErrorReason::Error,
            error: message,
        },
        StopReason::Aborted => AssistantMessageEvent::Error {
            reason: ErrorReason::Aborted,
            error: message,
        },
        reason => AssistantMessageEvent::Done {
            reason: match reason {
                StopReason::Stop => DoneReason::Stop,
                StopReason::Length => DoneReason::Length,
                StopReason::ToolUse => DoneReason::ToolUse,
                StopReason::Deferred => DoneReason::Deferred,
                _ => unreachable!("native settled terminal"),
            },
            message,
        },
    }
}

fn source_current<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    fx: &RuntimeEffects<C, S, M>,
    h: &crate::restore::HydratedLane,
    key: &EffectKey,
    source: Option<&(EntryId, AssistantMessage)>,
) -> bool {
    if key.kind == EffectKind::CancelDeferred {
        if key.session_id != fx.lane.owner.session.metadata().base_metadata().id
            || key.lane != fx.lane.lane.name
            || h.current.as_ref().is_none_or(|current| {
                current.operation.operation_id != key.operation_id
                    || !pi_harness_core::structural::is_cancelled(current)
            })
        {
            return false;
        }
        let Some((id, message)) = source else {
            return false;
        };
        if fx
            .cancel_targets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .is_none_or(|target| {
                target.plan.key != *key || message.deferred.as_ref() != Some(&target.plan.handle)
            })
        {
            return false;
        }
    } else if !super::custody::current_key(fx, h, key) {
        return false;
    }
    source.is_none_or(|(id,message)|matches!(h.entries.get(id),Some(Entry::Message(entry)) if matches!(&entry.message,AgentMessage::BuiltIn(Message::Assistant(existing)) if existing.as_ref()==message)))
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    Source<C, S, M>
{
    pub(crate) async fn cancel(
        &self,
        model: Model,
        options: SimpleStreamOptions,
    ) -> Result<Result<(), AuthResolutionError>, HarnessRuntimeError> {
        if self.key.kind != EffectKind::CancelDeferred {
            return Err(super::fault());
        }
        let source = self.deferred_source.as_ref().ok_or_else(super::fault)?;
        let context = Context {
            system_prompt: None,
            messages: vec![Message::Assistant(Box::new(source.1.clone()))],
            tools: Some(vec![]),
        };
        let result = invoke(
            &self.fx,
            &self.key,
            &self.provider,
            &model,
            context,
            options,
            Default::default(),
            Some(source),
        )
        .await;
        let result = match result {
            Ok(message)
                if !matches!(
                    message.as_message().stop_reason,
                    StopReason::Error | StopReason::Aborted
                ) =>
            {
                Ok(())
            }
            Ok(_) | Err(_) => Err(AuthResolutionError::Models(ModelsError::new(
                ModelsErrorCode::Provider,
            ))),
        };
        let line = self.fx.lane.lane.line.acquire().await?;
        let h = self.fx.lane.load().await?;
        if source_current(&self.fx, &h, &self.key, Some(source)) {
            self.fx.profile_receipts.check(&self.key)?;
            drop(line);
            self.fx.profile_receipts.settled(&self.key);
        } else {
            drop(line);
            self.fx.profile_receipts.retire(&self.key);
        }
        Ok(result)
    }
}
