use super::*;
use crate::{Error, Registry, Result};
use std::sync::{Arc, Mutex};
#[derive(Clone, Default)]
pub struct RuntimeRegistrySnapshot {
    pub(crate) extensions: Arc<Vec<Extension>>,
}
impl RuntimeRegistrySnapshot {
    pub fn installed(&self) -> &[Extension] {
        &self.extensions
    }
    pub fn extension(&self, name: &str) -> Option<Extension> {
        self.extensions.iter().find(|e| e.name == name).cloned()
    }
}
#[derive(Clone, Default)]
pub struct RuntimeRegistry {
    pub(crate) inner: Arc<Mutex<RegistryState>>,
}
#[derive(Default)]
pub(crate) struct RegistryState {
    pub snapshot: RuntimeRegistrySnapshot,
    pub subscribers: Vec<Arc<dyn Fn(RuntimeRegistrySnapshot) -> Result<()> + Send + Sync>>,
}
impl RuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn snapshot(&self) -> RuntimeRegistrySnapshot {
        self.inner.lock().unwrap().snapshot.clone()
    }
    pub fn install(&self, extension: Extension) -> Result<()> {
        let mut guard = self.inner.lock().unwrap();
        let mut next = (*guard.snapshot.extensions).clone();
        if let Some(index) = next.iter().position(|e| e.name == extension.name) {
            next[index] = extension
        } else {
            next.push(extension)
        }
        publish(&mut guard, next)
    }
    pub fn uninstall(&self, name: &str) -> Result<()> {
        let mut guard = self.inner.lock().unwrap();
        let next = guard
            .snapshot
            .extensions
            .iter()
            .filter(|e| e.name != name)
            .cloned()
            .collect();
        publish(&mut guard, next)
    }
    pub(crate) fn bind(
        &self,
        generic: Registry,
        owner: std::sync::Weak<RuntimeInner>,
    ) -> Result<()> {
        let mut guard = self.inner.lock().unwrap();
        let publish: Arc<dyn Fn(RuntimeRegistrySnapshot) -> Result<()> + Send + Sync> =
            Arc::new(move |snapshot| {
                let mut tasks = vec![
                    super::generation::definition(owner.clone(), snapshot.clone())?,
                    super::tool::definition(owner.clone(), snapshot.clone())?,
                    super::compaction::definition(owner.clone(), snapshot.clone())?,
                ];
                tasks.extend(snapshot.extensions.iter().flat_map(|e| e.tasks.clone()));
                generic.install("pi.runtime", tasks)
            });
        publish(guard.snapshot.clone())?;
        guard.subscribers.push(publish);
        Ok(())
    }
}
fn publish(state: &mut RegistryState, extensions: Vec<Extension>) -> Result<()> {
    let mut kinds = std::collections::BTreeSet::from([
        "pi.generation".to_string(),
        "pi.tool".into(),
        "pi.compaction".into(),
    ]);
    for e in &extensions {
        let mut tools = std::collections::BTreeSet::new();
        for t in &e.tools {
            if !tools.insert(t.name()) {
                return Err(Error::request("duplicate extension tool"));
            }
        }
        let mut sections = std::collections::BTreeSet::new();
        for s in &e.sections {
            let valid = s.key.bytes().enumerate().all(|(i, c)| {
                if i == 0 {
                    c.is_ascii_lowercase()
                } else {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-'
                }
            });
            if s.key.is_empty() || !valid || s.key == "instructions" || !sections.insert(&s.key) {
                return Err(Error::request("invalid, reserved or duplicate section key"));
            }
        }
        for task in &e.tasks {
            if !kinds.insert(task.kind().into()) {
                return Err(Error::request("duplicate task kind"));
            }
        }
    }
    let snapshot = RuntimeRegistrySnapshot {
        extensions: Arc::new(extensions),
    };
    for apply in &state.subscribers {
        apply(snapshot.clone())?;
    }
    state.snapshot = snapshot;
    Ok(())
}
pub(crate) fn resolve(
    state: AgentState,
    snapshot: &RuntimeRegistrySnapshot,
    settings: &DurableSettings,
    report: impl Fn(Error),
) -> Agent {
    let base = settings
        .extensions
        .clone()
        .unwrap_or_else(|| snapshot.extensions.iter().map(|e| e.name.clone()).collect());
    let selected = match &state.extensions {
        Some(NameSelection::Exact(v)) => v.clone(),
        Some(NameSelection::Edit { add, remove }) => base
            .into_iter()
            .chain(add.clone().unwrap_or_default())
            .filter(|n| !remove.as_deref().unwrap_or_default().contains(n))
            .collect(),
        None => base,
    };
    let mut seen = std::collections::BTreeSet::new();
    let extensions = selected
        .iter()
        .filter(|n| seen.insert((*n).clone()))
        .filter_map(|n| snapshot.extension(n))
        .collect::<Vec<_>>();
    let mut tools: Vec<ToolRegistration> = Vec::new();
    let mut sections: Vec<PromptSection> = Vec::new();
    for e in &extensions {
        for t in &e.tools {
            if let Some(i) = tools.iter().position(|x| x.name() == t.name()) {
                tools[i] = t.clone()
            } else {
                tools.push(t.clone())
            }
        }
        for s in &e.sections {
            if let Some(i) = sections.iter().position(|x| x.key == s.key) {
                sections[i] = s.clone()
            } else {
                sections.push(s.clone())
            }
        }
    }
    for e in &extensions {
        for wrap in &e.wraps {
            match wrap {
                Wrap::Tool { name, wrap } => {
                    if let Some(i) = tools.iter().position(|t| t.name() == name) {
                        match wrap(tools[i].clone()) {
                            Ok(t) if t.name() == name => tools[i] = t,
                            Ok(_) => {
                                tools.remove(i);
                                report(Error::request("wrapper renamed tool"));
                            }
                            Err(e) => {
                                tools.remove(i);
                                report(e)
                            }
                        }
                    }
                }
                Wrap::Section { key, wrap } => {
                    if let Some(i) = sections.iter().position(|s| &s.key == key) {
                        match wrap(sections[i].clone()) {
                            Ok(s) if &s.key == key => sections[i] = s,
                            Ok(_) => {
                                sections.remove(i);
                                report(Error::request("wrapper renamed section"));
                            }
                            Err(e) => {
                                sections.remove(i);
                                report(e)
                            }
                        }
                    }
                }
            }
        }
    }
    let enabled = match &state.tools {
        None => tools,
        Some(ToolSelection::Exact(names)) => select_tools(&tools, names),
        Some(ToolSelection::Remove { remove }) => tools
            .into_iter()
            .filter(|t| !remove.contains(&t.name().to_string()))
            .collect(),
    };
    let callable = enabled
        .iter()
        .filter(|t| {
            t.callers
                .as_ref()
                .is_none_or(|c| c.contains(&ToolCaller::Tools))
        })
        .cloned()
        .collect();
    let model = enabled
        .iter()
        .filter(|t| {
            t.callers
                .as_ref()
                .is_none_or(|c| c.contains(&ToolCaller::Model))
        })
        .cloned()
        .collect::<Vec<_>>();
    let tools = match &state.model_tools {
        Some(NameSelection::Exact(names)) => select_tools(&model, names),
        other => {
            let (add, remove) = match other {
                Some(NameSelection::Edit { add, remove }) => (
                    add.as_deref().unwrap_or_default(),
                    remove.as_deref().unwrap_or_default(),
                ),
                _ => (&[][..], &[][..]),
            };
            let mut v = model
                .iter()
                .filter(|t| !t.deferred && !remove.iter().any(|n| n == t.name()))
                .cloned()
                .collect::<Vec<_>>();
            for t in select_tools(&model, add) {
                if t.deferred && !remove.iter().any(|n| n == t.name()) {
                    v.push(t)
                }
            }
            v
        }
    };
    let deferred = model
        .into_iter()
        .filter(|t| t.deferred && !tools.iter().any(|x| x.name() == t.name()))
        .collect();
    if let Some(text) = &state.instructions {
        let text = text.clone();
        sections.push(PromptSection {
            key: "instructions".into(),
            tag: true,
            render: Arc::new(move |_| {
                let t = text.clone();
                Box::pin(async move { Ok(Some(t)) })
            }),
        })
    }
    Agent {
        thinking_level: state
            .thinking_level
            .unwrap_or(pi_ai::ModelThinkingLevel::Off),
        state,
        extensions,
        tools,
        callable,
        deferred,
        sections,
    }
}
fn select_tools(tools: &[ToolRegistration], names: &[String]) -> Vec<ToolRegistration> {
    let mut seen = std::collections::BTreeSet::new();
    names
        .iter()
        .filter(|n| seen.insert((*n).clone()))
        .filter_map(|n| tools.iter().find(|t| t.name() == n).cloned())
        .collect()
}
pub(crate) fn configure(
    tx: &mut crate::Transaction<'_>,
    c: crate::ConversationId,
    change: AgentChange,
) -> Result<()> {
    let (id, mut state) = doc(tx, &agent_doc(), c)?;
    let patch = ai_value(&change)?;
    for (k, v) in patch.as_object().unwrap() {
        if v.is_null() {
            state.as_object_mut().unwrap().remove(k);
        } else {
            state[k] = v.clone();
        }
    }
    save(tx, id, state)
}
