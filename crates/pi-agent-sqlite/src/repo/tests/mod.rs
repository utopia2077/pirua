use super::*;
pub(super) type Hook = Arc<dyn Fn(&str, &Path) -> Result<(), SessionError> + Send + Sync>;
mod admin;
mod logical;
mod processes;
mod support;

static PHASES: std::sync::OnceLock<Mutex<BTreeMap<PathBuf, Hook>>> = std::sync::OnceLock::new();
pub(crate) fn phase(name: &str, path: &Path) -> Result<(), SessionError> {
    let hook = {
        PHASES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .get(path)
            .cloned()
    };
    if let Some(hook) = hook {
        hook(name, path)?;
    }
    Ok(())
}
struct PhaseGuard(PathBuf);
impl Drop for PhaseGuard {
    fn drop(&mut self) {
        PHASES.get().unwrap().lock().unwrap().remove(&self.0);
    }
}
fn instrument(path: &Path, hook: Hook) -> PhaseGuard {
    PHASES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(path.to_owned(), hook);
    PhaseGuard(path.to_owned())
}
