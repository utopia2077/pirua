//! Pure navigation validation and unsummarized result selection.
use crate::{planner::*, structural};
use pi_agent_schema::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Expected pure navigation-validation failure.
pub enum AdmissionError {
    /// Root, current-leaf, label, or summarized-source rule was violated.
    InvalidNavigation,
    /// The non-null target did not exist in the accepting Session.
    UnknownTarget,
}
/// `target` is a detached point read from the same Session that will accept.
pub fn validate(
    source: &Option<EntryId>,
    intent: &NavigationIntent,
    target: Option<&Entry>,
) -> Result<(), AdmissionError> {
    if &intent.target_id == source
        || (intent.target_id.is_none() && (intent.summarize || intent.label.is_some()))
        || (intent.summarize && source.is_none())
    {
        return Err(AdmissionError::InvalidNavigation);
    }
    if let Some(id) = &intent.target_id {
        let actual = target.map(|e| match e {
            Entry::Message(e) => &e.id,
            Entry::Compaction(e) => &e.id,
            Entry::BranchSummary(e) => &e.id,
            Entry::Custom(e) => &e.id,
        });
        if actual != Some(id) {
            return Err(AdmissionError::UnknownTarget);
        }
    } else if target.is_some() {
        return Err(AdmissionError::InvalidNavigation);
    }
    Ok(())
}
/// Compute a root-or-entry move or cancelled outcome without summary work.
pub fn unsummarized_result(current: &CurrentOperation) -> Result<OperationResult, PlanError> {
    let OperationState::Navigation(NavigationState::Unsummarized(n)) = &current.state else {
        return Err(PlanError::InvalidCurrent);
    };
    if structural::is_cancelled(current) {
        return structural::failed_result(current, None, true);
    }
    Ok(OperationResult::Navigation(NavigationOutcome::Completed(
        Box::new(NavigationCompleted {
            kind: Default::default(),
            old_leaf_id: current.operation.source_leaf_id.clone(),
            new_leaf_id: n.target_id.clone(),
            summary_entry: None,
        }),
    )))
}
