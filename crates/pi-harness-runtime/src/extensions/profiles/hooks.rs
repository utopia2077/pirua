//! Remote callbacks join the same source-ordered native captured pipeline.
use super::{
    connection::{CallScope, ProfileConnection, Reply},
    native::NativeDomain,
};
use crate::hooks::StartedHookContext;
use pi_agent_schema::*;
use pi_extension_protocol::{
    profile_live,
    profiles::{NativeError, NativeValidator, Stage, ValidatedPayload, wire_v1_1 as dto},
};
use std::sync::Arc;

pub(crate) trait ScopeAuthority: Send + Sync {
    fn prepare<'a>(
        &'a self,
        event: &'a HookEvent,
        connection: &'a Arc<ProfileConnection>,
    ) -> BoxFuture<'a, Result<CallScope, HarnessRuntimeError>>;
    fn retain(
        &self,
        event: &HookEvent,
        scope_key: Option<EffectKey>,
        connection: Arc<ProfileConnection>,
        reply: &Reply,
    );
    fn actions(
        &self,
        connection: &ProfileConnection,
        event: &HookEvent,
        key: Option<EffectKey>,
    ) -> Box<dyn super::connection::HostActions>;
    fn cancellation(&self) -> pi_ai_core::CancellationToken;
}
#[derive(Clone)]
pub struct RemoteHook {
    pub(crate) name: HookName,
    pub(crate) connection: Arc<ProfileConnection>,
    pub(crate) capability: dto::CapabilityRef,
    pub(crate) model_policy: pi_ai_core::ModelHeaderPolicy,
}
impl RemoteHook {
    pub fn new(
        connection: Arc<ProfileConnection>,
        capability: dto::CapabilityRef,
        model_policy: pi_ai_core::ModelHeaderPolicy,
    ) -> Result<Self, HarnessRuntimeError> {
        let name: HookName =
            serde_json::from_value(serde_json::Value::String(capability.name.clone()))
                .map_err(|_| super::fault())?;
        if capability.kind != "hook"
            || capability.handler_id.as_ref().is_none_or(String::is_empty)
            || !connection
                .grant()
                .profiles()
                .iter()
                .any(|p| p.reference() == &capability)
        {
            return Err(super::fault());
        }
        Ok(Self {
            name,
            connection,
            capability,
            model_policy,
        })
    }
    pub fn stable_id(&self) -> Result<HandlerId, HookRegistrationError> {
        let principal = &self.capability.authenticated_extension_id;
        HandlerId::new(format!(
            "extension:{}:{}{}",
            principal.len(),
            principal,
            self.capability
                .handler_id
                .as_ref()
                .expect("registered handler")
        ))
        .map_err(|_| {
            HookRegistrationError::new(
                ErrorContext::new("remote handler identity exceeds native limit").expect("static"),
            )
        })
    }
    pub(crate) async fn call(
        &self,
        event: HookEvent,
        context: &StartedHookContext<'_>,
        scope: &dyn ScopeAuthority,
    ) -> Result<Result<HookResult, HandlerError>, HarnessRuntimeError> {
        let cancellation = scope.cancellation();
        let resume_slot = pi_ai_core::CancellationToken::new();
        let slot = self
            .connection
            .acquire_slot(if self.name == HookName::BeforeResume {
                &resume_slot
            } else {
                &cancellation
            })
            .await?;
        let mut call_scope = scope.prepare(&event, &self.connection).await?;
        call_scope.slot = Some(slot);
        let key = call_scope.key.clone();
        let contract = self
            .connection
            .grant()
            .profiles()
            .iter()
            .find(|p| p.reference() == &self.capability)
            .ok_or_else(super::fault)?;
        let input = serde_json::to_value(&event).map_err(|_| super::fault())?;
        let payload = profile_live::encode_payload(
            &dto::InvokeHookRequest {
                handler_id: self
                    .capability
                    .handler_id
                    .clone()
                    .expect("registered handler"),
                input: Some(
                    super::bound(
                        contract.approved().registry(),
                        &format!("hook.input:{}", self.capability.name),
                        &input,
                    )
                    .map_err(|_| super::fault())?,
                ),
            },
            self.connection.max_payload_bytes(),
        )
        .map_err(|_| super::fault())?;
        let id = self.stable_id().map_err(|_| super::fault())?;
        let mut native = HookNative {
            domain: NativeDomain::new(context.codec, &self.model_policy),
            event: &event,
            context,
            result: None,
            id,
        };
        let actions = scope.actions(&self.connection, &event, key.clone());
        let reply = self
            .connection
            .call(
                self.capability.clone(),
                &self.capability.name,
                payload,
                call_scope,
                &mut native,
                Some(Arc::from(actions)),
                &cancellation,
            )
            .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(_) => return Ok(Err(handler_error())),
        };
        scope.retain(&event, key, self.connection.clone(), &reply);
        if reply.error.is_some() {
            return Ok(Err(handler_error()));
        }
        Ok(Ok(native.result.ok_or_else(super::fault)?))
    }
}
fn handler_error() -> HandlerError {
    HandlerError::new(ErrorContext::new("handler_error").expect("fixed"))
}
impl<
    C: Send + Sync + 'static,
    S: pi_agent_session::storage::RuntimeStorage + 'static,
    M: SessionMetadataRecord,
> crate::owner::RuntimeHarness<C, S, M>
{
    pub fn register_profile_hook(
        &self,
        hook: RemoteHook,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.main.owner.hooks.register_remote(hook)
    }
}
struct HookNative<'a, 'b> {
    domain: NativeDomain<'a>,
    event: &'a HookEvent,
    context: &'a StartedHookContext<'b>,
    result: Option<HookResult>,
    id: HandlerId,
}
impl NativeValidator for HookNative<'_, '_> {
    fn validate(&mut self, candidate: &ValidatedPayload) -> Result<(), NativeError> {
        self.domain.validate(candidate)?;
        match candidate.stage {
            Stage::Request => {
                if self
                    .domain
                    .hook_event
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| NativeError::Rejected)?
                    != Some(serde_json::to_value(self.event).map_err(|_| NativeError::Rejected)?)
                {
                    return Err(NativeError::Rejected);
                }
            }
            Stage::Terminal => {
                let result = if let Some(result) = self.domain.hook_result.take() {
                    result
                } else {
                    serde_json::from_value::<RawHookResult>(
                        serde_json::json!({"name":candidate.method}),
                    )
                    .map_err(|_| NativeError::Rejected)?
                    .into_durable(self.context.header_policy)
                    .map_err(|_| NativeError::Rejected)?
                };
                crate::hooks::validate_remote_result(
                    self.event,
                    &result,
                    Some(&self.id),
                    self.context,
                )
                .map_err(|_| NativeError::Rejected)?;
                self.result = Some(result);
            }
            Stage::Progress => return Err(NativeError::Rejected),
        }
        Ok(())
    }
}
