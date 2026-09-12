//! Ordered instance registration; binding identity is independent of adapter identity.
use super::CompatApiModules;
use pi_ai_core::{
    ApiAdapter, ApiId, AssistantStream, ModelsError, ModelsErrorCode, SimpleStreamRequest,
    TextStreamRequest,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
};
pub struct CompatApiBinding {
    api: ApiId,
    adapter: Arc<dyn ApiAdapter>,
    source_id: Option<String>,
}
impl CompatApiBinding {
    pub fn new(adapter: Arc<dyn ApiAdapter>, source_id: Option<String>) -> Self {
        Self {
            api: adapter.api_id().clone(),
            adapter,
            source_id,
        }
    }
    pub fn api_id(&self) -> &ApiId {
        &self.api
    }
    pub fn source_id(&self) -> Option<&str> {
        self.source_id.as_deref()
    }
    pub fn stream(&self, request: TextStreamRequest) -> Result<AssistantStream, ModelsError> {
        self.check(&request.resolved.model.api)?;
        Ok(self.adapter.stream(request))
    }
    pub fn stream_simple(
        &self,
        request: SimpleStreamRequest,
    ) -> Result<AssistantStream, ModelsError> {
        self.check(&request.resolved.model.api)?;
        Ok(self.adapter.stream_simple(request))
    }
    pub(super) fn check(&self, api: &ApiId) -> Result<(), ModelsError> {
        if api == &self.api {
            Ok(())
        } else {
            Err(ModelsError::new(ModelsErrorCode::Provider))
        }
    }
}
impl fmt::Debug for CompatApiBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompatApiBinding([REDACTED])")
    }
}
#[derive(Default)]
struct State {
    entries: Vec<Arc<CompatApiBinding>>,
    remembered: Vec<Arc<CompatApiBinding>>,
}
#[derive(Default)]
pub struct CompatApiRegistry {
    state: Mutex<State>,
}
impl CompatApiRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_api_provider(&self, adapter: Arc<dyn ApiAdapter>, source_id: Option<String>) {
        let binding = Arc::new(CompatApiBinding::new(adapter, source_id));
        let displaced = {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(existing) = s.entries.iter_mut().find(|v| v.api == binding.api) {
                Some(std::mem::replace(existing, binding))
            } else {
                s.entries.push(binding);
                None
            }
        };
        drop(displaced);
    }
    pub fn get_api_provider(&self, api: &ApiId) -> Option<Arc<CompatApiBinding>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .iter()
            .find(|v| &v.api == api)
            .cloned()
    }
    pub fn get_api_providers(&self) -> Vec<Arc<CompatApiBinding>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .clone()
    }
    pub fn unregister_api_providers(&self, source_id: &str) {
        let removed = {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let (removed, kept) = std::mem::take(&mut s.entries)
                .into_iter()
                .partition(|v| v.source_id.as_deref() == Some(source_id));
            s.entries = kept;
            removed
        };
        drop(removed);
    }
    pub fn register_builtin_api_providers(&self, modules: &CompatApiModules) {
        // Adapter identity is obtained before locking, including for unused candidates.
        let candidates = modules.bindings();
        let mut unused = Vec::new();
        let old = {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let mut remembered = Vec::new();
            for candidate in candidates {
                if let Some(existing) = s.entries.iter().find(|v| v.api == candidate.api) {
                    remembered.push(existing.clone());
                    unused.push(candidate);
                } else {
                    s.entries.push(candidate.clone());
                    remembered.push(candidate);
                }
            }
            std::mem::replace(&mut s.remembered, remembered)
        };
        drop(old);
        drop(unused);
    }
    pub fn reset_api_providers(&self, modules: &CompatApiModules) {
        let entries = modules.bindings();
        let replacement = State {
            remembered: entries.clone(),
            entries,
        };
        let old = std::mem::replace(
            &mut *self.state.lock().unwrap_or_else(|p| p.into_inner()),
            replacement,
        );
        drop(old);
    }
    pub(super) fn capture(&self, api: &ApiId) -> Option<(Arc<CompatApiBinding>, bool)> {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let binding = s.entries.iter().find(|v| &v.api == api)?.clone();
        let remembered = s.remembered.iter().any(|v| Arc::ptr_eq(v, &binding));
        Some((binding, remembered))
    }
}
impl fmt::Debug for CompatApiRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompatApiRegistry([REDACTED])")
    }
}
