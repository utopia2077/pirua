//! The six granted reentrant actions dispatch through existing native lanes.
use super::{
    connection::{ActionInvocation, HostActions},
    resources::ResourceRevision,
};
use crate::owner::RuntimeLane;
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_extension_protocol::{
    profile_live::encode_payload,
    profiles::{ValidatedPayload, wire_v1_1 as dto},
    wire::v1_1,
};
use std::sync::{Arc, Mutex, Weak};

pub(crate) trait NativeScope: Send + Sync {
    fn check(&self, h: &crate::restore::HydratedLane) -> Result<(), v1_1::ErrorCode>;
}
struct EffectActionScope<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Weak<crate::effects::RuntimeEffects<C, S, M>>,
    identity: Arc<()>,
    operation: OperationId,
    key: Option<EffectKey>,
    accepted: bool,
}
pub(crate) fn scope<
    C: Send + Sync + 'static,
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord,
>(
    fx: &Arc<crate::effects::RuntimeEffects<C, S, M>>,
    operation: OperationId,
    key: Option<EffectKey>,
    accepted: bool,
) -> Arc<dyn NativeScope> {
    Arc::new(EffectActionScope {
        fx: Arc::downgrade(fx),
        identity: fx.identity.clone(),
        operation,
        key,
        accepted,
    })
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> NativeScope
    for EffectActionScope<C, S, M>
{
    fn check(&self, h: &crate::restore::HydratedLane) -> Result<(), v1_1::ErrorCode> {
        let fx = self.fx.upgrade().ok_or(v1_1::ErrorCode::InvalidParent)?;
        if !Arc::ptr_eq(&fx.identity, &self.identity)
            || fx.cancellation.is_cancelled()
            || fx.retired.is_cancelled()
        {
            return Err(v1_1::ErrorCode::InvalidParent);
        }
        let current = if self.accepted {
            h.current.as_ref().is_some_and(|current| {
                current.operation.operation_id == self.operation
                    && !pi_harness_core::structural::is_cancelled(current)
                    && self
                        .key
                        .as_ref()
                        .is_none_or(|key| super::custody::current_key(&fx, h, key))
            })
        } else {
            h.current.is_none()
                && fx
                    .lane
                    .lane
                    .reservation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_ref()
                    == Some(&self.operation)
        };
        if !current {
            return Err(v1_1::ErrorCode::InvalidParent);
        }
        Ok(())
    }
}
pub(crate) struct ActionGuard {
    scope: Arc<dyn NativeScope>,
    invocation: ActionInvocation,
    refused: Mutex<Option<v1_1::ErrorCode>>,
}
impl ActionGuard {
    fn new(scope: Arc<dyn NativeScope>, invocation: ActionInvocation) -> Arc<Self> {
        Arc::new(Self {
            scope,
            invocation,
            refused: Mutex::new(None),
        })
    }
    pub(crate) fn check(&self, h: &crate::restore::HydratedLane) -> bool {
        let checked = self
            .scope
            .check(h)
            .and_then(|()| self.invocation.dispatch());
        if let Err(error) = checked {
            *self.refused.lock().unwrap_or_else(|p| p.into_inner()) = Some(error);
            false
        } else {
            true
        }
    }
    fn refusal(&self) -> Option<v1_1::ErrorCode> {
        *self.refused.lock().unwrap_or_else(|p| p.into_inner())
    }
}
pub(crate) struct LaneActions<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub lane: RuntimeLane<C, S, M>,
    pub resources: Option<Arc<ResourceRevision>>,
    pub registry: pi_extension_protocol::profiles::Registry,
    pub max_bytes: usize,
    pub scope: Arc<dyn NativeScope>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> HostActions
    for LaneActions<C, S, M>
{
    fn execute<'a>(
        &'a self,
        invocation: ActionInvocation,
        payload: ValidatedPayload,
    ) -> BoxFuture<'a, Result<Vec<u8>, v1_1::ErrorCode>> {
        Box::pin(async move {
            use v1_1::ErrorCode as E;
            if payload.method != "resource.get"
                && payload.fields["scope"].as_str() != Some(self.lane.lane.name.as_str())
            {
                return Err(E::Forbidden);
            }
            let guard = ActionGuard::new(self.scope.clone(), invocation);
            match payload.method.as_str() {
                "snapshot.get" => {
                    if payload.fields["visibility"].as_str() != Some("captured_policy") {
                        return Err(E::Forbidden);
                    }
                    let line = self
                        .lane
                        .lane
                        .line
                        .acquire()
                        .await
                        .map_err(|_| E::HandlerError)?;
                    let h = self.lane.load().await.map_err(|_| E::HandlerError)?;
                    if !guard.check(&h) {
                        return Err(guard.refusal().unwrap_or(E::InvalidParent));
                    }
                    let snapshot = self
                        .lane
                        .capture_lane_snapshot()
                        .await
                        .map_err(|_| E::HandlerError)?;
                    drop(line);
                    let mut value = serde_json::to_value(snapshot).map_err(|_| E::HandlerError)?;
                    super::events::sanitize_snapshot(&mut value);
                    encode_payload(
                        &dto::SnapshotGetResult {
                            filtered_snapshot: Some(
                                super::bound(&self.registry, "host.filtered_snapshot.v1", &value)
                                    .map_err(|_| E::HandlerError)?,
                            ),
                        },
                        self.max_bytes,
                    )
                    .map_err(|_| E::HandlerError)
                }
                "resource.get" => {
                    let line = self
                        .lane
                        .lane
                        .line
                        .acquire()
                        .await
                        .map_err(|_| E::HandlerError)?;
                    let h = self.lane.load().await.map_err(|_| E::HandlerError)?;
                    if !guard.check(&h) {
                        return Err(guard.refusal().unwrap_or(E::InvalidParent));
                    }
                    let resources = self.resources.as_ref().ok_or(E::NotFound)?;
                    if payload.fields["scope"].as_str() != Some(resources.scope()) {
                        return Err(E::Forbidden);
                    }
                    let id = payload.fields["id"].as_str().ok_or(E::HandlerError)?;
                    let version = payload.fields["version"].as_str();
                    let value = resources.get(id, version).ok_or_else(|| {
                        if resources.get(id, None).is_some() {
                            E::VersionMismatch
                        } else {
                            E::NotFound
                        }
                    })?;
                    drop(line);
                    encode_payload(
                        &dto::ResourceGetResult {
                            snapshot: Some(
                                super::bound(
                                    &self.registry,
                                    "host.resource_snapshot.v1",
                                    &value.json(),
                                )
                                .map_err(|_| E::HandlerError)?,
                            ),
                        },
                        self.max_bytes,
                    )
                    .map_err(|_| E::HandlerError)
                }
                "queue.steer" | "queue.follow_up" | "queue.next_run" => {
                    let value = payload
                        .projections
                        .get("host.queue_item.v1")
                        .ok_or(E::HandlerError)?;
                    let message: AgentMessage =
                        serde_json::from_value(value.clone()).map_err(|_| E::HandlerError)?;
                    self.lane
                        .owner
                        .session
                        .codec()
                        .validate_message(&message)
                        .map_err(|_| E::Forbidden)?;
                    let kind = match payload.method.as_str() {
                        "queue.next_run" => crate::inbox::QueueKind::Next,
                        "queue.steer" => crate::inbox::QueueKind::Steer,
                        _ => crate::inbox::QueueKind::FollowUp,
                    };
                    let result = self
                        .lane
                        .queue_input_guarded(message, kind, Some(guard.clone()))
                        .await
                        .map_err(|_| E::HandlerError)?;
                    if let Some(error) = guard.refusal() {
                        return Err(error);
                    }
                    let id = result.map_err(|_| E::Forbidden)?.entry_id;
                    encode_payload(
                        &dto::QueueResult {
                            item_id: id.as_str().into(),
                            accepted: true,
                        },
                        self.max_bytes,
                    )
                    .map_err(|_| E::HandlerError)
                }
                "operation.abort" => {
                    let operation = OperationId::new(
                        payload.fields["operation_id"]
                            .as_str()
                            .ok_or(E::HandlerError)?,
                    )
                    .map_err(|_| E::HandlerError)?;
                    let aborted = self
                        .lane
                        .abort_profile_operation(&operation, guard.clone())
                        .await
                        .map_err(|_| E::HandlerError)?;
                    if let Some(error) = guard.refusal() {
                        return Err(error);
                    }
                    encode_payload(&dto::OperationAbortResult { aborted }, self.max_bytes)
                        .map_err(|_| E::HandlerError)
                }
                "secret.resolve" => Err(E::BrokerUnavailable),
                _ => Err(E::UnsupportedMethod),
            }
        })
    }
}
