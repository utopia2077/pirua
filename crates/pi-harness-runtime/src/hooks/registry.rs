use super::context;
use pi_agent_schema::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

/// Opaque revision identity; equal exactly when a registry has not changed.
#[derive(Clone, Debug)]
pub struct CaptureRevision(Arc<()>);
impl PartialEq for CaptureRevision {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for CaptureRevision {}

#[derive(Default)]
struct State {
    revision: Arc<()>,
    entries: BTreeMap<HookName, Vec<Arc<Entry>>>,
}
/// An instance-owned registry implementing all eleven frozen registration methods.
#[derive(Clone, Default)]
pub struct HookRegistry {
    state: Arc<Mutex<State>>,
}

pub(super) struct Entry {
    pub id: Option<HandlerId>,
    identity: Arc<()>,
    pub callback: Callback,
}
pub(super) enum Callback {
    #[cfg(feature = "extensions")]
    Remote(Arc<crate::extensions::profiles::hooks::RemoteHook>),
    BeforeRun(Arc<dyn HookHandler<BeforeRun>>),
    BeforeResume(Arc<dyn HookHandler<BeforeResume>>),
    BeforeRunEnd(Arc<dyn HookHandler<BeforeRunEnd>>),
    TransformContext(Arc<dyn HookHandler<TransformContext>>),
    BeforeRequest(Arc<dyn HookHandler<BeforeRequest>>),
    BeforePayload(Arc<dyn HookHandler<BeforePayload>>),
    AfterResponse(Arc<dyn HookHandler<AfterResponse>>),
    BeforeTool(Arc<dyn HookHandler<BeforeTool>>),
    AfterTool(Arc<dyn HookHandler<AfterTool>>),
    BeforeCompaction(Arc<dyn HookHandler<BeforeCompaction>>),
    BeforeNavigation(Arc<dyn HookHandler<BeforeNavigation>>),
}
impl Callback {
    fn name(&self) -> HookName {
        match self {
            #[cfg(feature = "extensions")]
            Self::Remote(hook) => hook.name,
            Self::BeforeRun(_) => HookName::BeforeRun,
            Self::BeforeResume(_) => HookName::BeforeResume,
            Self::BeforeRunEnd(_) => HookName::BeforeRunEnd,
            Self::TransformContext(_) => HookName::TransformContext,
            Self::BeforeRequest(_) => HookName::BeforeRequest,
            Self::BeforePayload(_) => HookName::BeforePayload,
            Self::AfterResponse(_) => HookName::AfterResponse,
            Self::BeforeTool(_) => HookName::BeforeTool,
            Self::AfterTool(_) => HookName::AfterTool,
            Self::BeforeCompaction(_) => HookName::BeforeCompaction,
            Self::BeforeNavigation(_) => HookName::BeforeNavigation,
        }
    }
}

/// An immutable ordered capture, retaining callbacks independently of registration.
#[derive(Clone)]
pub struct CapturedHookPipeline {
    pub(super) name: HookName,
    revision: CaptureRevision,
    pub(super) entries: Vec<Arc<Entry>>,
}
impl CapturedHookPipeline {
    /// Hook family captured by this value.
    pub fn name(&self) -> HookName {
        self.name
    }
    /// Registry revision at the capture linearization point.
    pub fn revision(&self) -> &CaptureRevision {
        &self.revision
    }
    /// Number of captured callbacks.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Whether this capture contains no callbacks.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
impl HookRegistry {
    /// Register an authenticated remote callback in the same ordered registry.
    #[cfg(feature = "extensions")]
    pub fn register_remote(
        &self,
        hook: crate::extensions::profiles::hooks::RemoteHook,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(Some(hook.stable_id()?), Callback::Remote(Arc::new(hook)))
    }
    /// Create an empty independent registry.
    pub fn new() -> Self {
        Self::default()
    }
    /// Capture membership under a short lock; invoking it never reopens the registry.
    pub fn capture(&self, name: HookName) -> CapturedHookPipeline {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        CapturedHookPipeline {
            name,
            revision: CaptureRevision(state.revision.clone()),
            entries: state.entries.get(&name).cloned().unwrap_or_default(),
        }
    }
    fn register(
        &self,
        id: Option<HandlerId>,
        callback: Callback,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        let name = callback.name();
        let identity = Arc::new(());
        let entry = Arc::new(Entry {
            id,
            identity: identity.clone(),
            callback,
        });
        let revision = Arc::new(());
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let entries = state.entries.entry(name).or_default();
            if matches!(name, HookName::BeforeRun | HookName::BeforeResume)
                && entries.iter().any(|e| e.id == entry.id)
            {
                return Err(HookRegistrationError::new(context(
                    "duplicate stable hook id",
                )));
            }
            entries.push(entry.clone());
            state.revision = revision;
        }
        Ok(Arc::new(Subscription {
            state: Arc::downgrade(&self.state),
            name,
            identity,
        }))
    }
}
struct Subscription {
    state: Weak<Mutex<State>>,
    name: HookName,
    identity: Arc<()>,
}
impl HookSubscription for Subscription {
    fn unsubscribe(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let revision = Arc::new(());
        let retired = {
            let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
            let found = state.entries.get_mut(&self.name).and_then(|entries| {
                entries
                    .iter()
                    .position(|e| Arc::ptr_eq(&e.identity, &self.identity))
                    .map(|i| entries.remove(i))
            });
            if found.is_some() {
                state.revision = revision;
            }
            found
        };
        drop(retired); // User-owned callback destructors may reenter the registry.
    }
}
impl Hooks for HookRegistry {
    fn before_run(
        &self,
        id: HandlerId,
        handler: Arc<dyn HookHandler<BeforeRun>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(Some(id), Callback::BeforeRun(handler))
    }
    fn before_resume(
        &self,
        id: HandlerId,
        handler: Arc<dyn HookHandler<BeforeResume>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(Some(id), Callback::BeforeResume(handler))
    }
    fn before_run_end(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<BeforeRunEnd>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::BeforeRunEnd(handler))
    }
    fn transform_context(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<TransformContext>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::TransformContext(handler))
    }
    fn before_request(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<BeforeRequest>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::BeforeRequest(handler))
    }
    fn before_payload(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<BeforePayload>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::BeforePayload(handler))
    }
    fn after_response(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<AfterResponse>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::AfterResponse(handler))
    }
    fn before_tool(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<BeforeTool>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::BeforeTool(handler))
    }
    fn after_tool(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<AfterTool>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::AfterTool(handler))
    }
    fn before_compaction(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<BeforeCompaction>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::BeforeCompaction(handler))
    }
    fn before_navigation(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<BeforeNavigation>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError> {
        self.register(id, Callback::BeforeNavigation(handler))
    }
}
