use super::*;
use crate::{Result, session::*};
use pi_ai::{Message, SystemContent, SystemMessage, SystemSections, ToolReference};
use serde_json::json;
pub(crate) fn sections(messages: &[Message]) -> Vec<(String, String)> {
    let mut values = Vec::new();
    for m in messages {
        if let Message::System(s) = m
            && let Some(sections) = &s.sections
        {
            for (k, v) in sections.iter() {
                if let Some(i) = values.iter().position(|(name, _)| name == k) {
                    if let Some(v) = v {
                        values[i] = (k.clone(), v.clone())
                    } else {
                        values.remove(i);
                    }
                } else if let Some(v) = v {
                    values.push((k.clone(), v.clone()))
                }
            }
        }
    }
    values
}
pub(crate) async fn plan(
    phase: &Phase,
    view: &crate::context::ContextView,
    agent: Agent,
) -> Result<Vec<EntryDraft>> {
    let shown = sections(&view.messages);
    let mut desired = Vec::new();
    for section in &agent.sections {
        match (section.render)(PromptInput {
            conversation_id: phase.conversation,
            agent: agent.clone(),
            shown: shown.clone(),
            api: phase.api(),
        })
        .await
        {
            Ok(Some(text)) => desired.push((
                section.key.clone(),
                if section.tag {
                    format!("<{}>\n{}\n</{}>", section.key, text, section.key)
                } else {
                    text
                },
            )),
            Ok(None) => (),
            Err(e) => {
                phase.check()?;
                phase.report(e);
                if let Some(item) = shown.iter().find(|(k, _)| k == &section.key) {
                    desired.push(item.clone())
                }
            }
        }
    }
    let declarations = agent
        .tools
        .iter()
        .map(|t| t.declaration.clone())
        .collect::<Vec<_>>();
    let offered = pi_ai::get_current_tools(&view.messages);
    let baseline = view.head.as_ref().is_some_and(|h| {
        !view
            .entries
            .iter()
            .any(|e| e.kind == "pi.system" && e.id > h.id)
    });
    let mut patches = Vec::new();
    if baseline {
        patches.push(
            desired
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect::<Vec<_>>(),
        )
    } else {
        let order = shown
            .iter()
            .filter(|(k, _)| desired.iter().any(|(x, _)| x == k))
            .map(|(k, _)| k)
            .chain(
                desired
                    .iter()
                    .filter(|(k, _)| !shown.iter().any(|(x, _)| x == k))
                    .map(|(k, _)| k),
            )
            .collect::<Vec<_>>();
        if order != desired.iter().map(|(k, _)| k).collect::<Vec<_>>() {
            patches.push(shown.iter().map(|(k, _)| (k.clone(), None)).collect());
            patches.push(
                desired
                    .iter()
                    .map(|(k, v)| (k.clone(), Some(v.clone())))
                    .collect(),
            )
        } else {
            let mut patch = shown
                .iter()
                .filter(|(k, v)| desired.iter().find(|(x, _)| x == k).map(|(_, x)| x) != Some(v))
                .map(|(k, _)| {
                    (
                        k.clone(),
                        desired.iter().find(|(x, _)| x == k).map(|(_, v)| v.clone()),
                    )
                })
                .collect::<Vec<_>>();
            patch.extend(
                desired
                    .iter()
                    .filter(|(k, _)| !shown.iter().any(|(x, _)| x == k))
                    .map(|(k, v)| (k.clone(), Some(v.clone()))),
            );
            if !patch.is_empty() {
                patches.push(patch)
            }
        }
    }
    let kept = offered
        .iter()
        .filter(|t| declarations.iter().any(|d| d == *t))
        .cloned()
        .collect::<Vec<_>>();
    let mut added = declarations
        .iter()
        .filter(|t| !kept.iter().any(|k| k.name == t.name))
        .cloned()
        .collect::<Vec<_>>();
    let mut removed = offered
        .iter()
        .filter(|t| !kept.iter().any(|k| k.name == t.name))
        .map(|t| ToolReference {
            name: t.name.clone(),
        })
        .collect::<Vec<_>>();
    if baseline
        || kept
            .iter()
            .chain(added.iter())
            .map(|t| &t.name)
            .collect::<Vec<_>>()
            != declarations.iter().map(|t| &t.name).collect::<Vec<_>>()
    {
        removed = if baseline {
            vec![]
        } else {
            offered
                .iter()
                .map(|t| ToolReference {
                    name: t.name.clone(),
                })
                .collect()
        };
        added = declarations;
    }
    if patches.is_empty() && (!added.is_empty() || !removed.is_empty()) {
        patches.push(vec![])
    }
    let count = patches.len();
    let mut result = Vec::new();
    for (i, patch) in patches.into_iter().enumerate() {
        let message = SystemMessage {
            role: Default::default(),
            content: SystemContent::Text(String::new()),
            sections: (!patch.is_empty()).then(|| SystemSections::new(patch)),
            tools_added: (i + 1 == count && !added.is_empty()).then(|| added.clone()),
            tools_removed: (i + 1 == count && !removed.is_empty()).then(|| removed.clone()),
            timestamp: timestamp(phase.now()?)?,
        };
        let mut draft = EntryDraft::new("pi.system");
        draft.model = Some(vec![ai_value(&message)?]);
        if baseline {
            let edits = view
                .entries
                .iter()
                .filter(|e| e.kind == "pi.system")
                .map(|e| ContextEdit::Omit { target: e.id })
                .collect::<Vec<_>>();
            if !edits.is_empty() {
                draft.edits = Some(edits)
            }
        }
        result.push(draft)
    }
    let _ = json!({});
    Ok(result)
}
