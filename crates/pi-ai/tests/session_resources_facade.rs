use std::sync::Arc;

use pi_ai::{
    SessionResource, SessionResourceError, SessionResourceHandle, SessionResourceRegistry,
    SessionResourceRegistryError,
};

struct FacadeResource;

impl SessionResource for FacadeResource {
    fn cleanup(&self, session_id: Option<&str>) -> Result<(), SessionResourceError> {
        assert_eq!(session_id, Some("facade"));
        Ok(())
    }
}

#[test]
fn session_resources_facade_reexports_exact_surface() {
    let registry = SessionResourceRegistry::new();
    let resource: Arc<dyn SessionResource> = Arc::new(FacadeResource);
    let handle: Result<SessionResourceHandle, SessionResourceRegistryError> =
        registry.register(resource);
    let _handle = handle.expect("facade registration succeeds");
    assert!(registry.cleanup(Some("facade")).is_ok());
}
