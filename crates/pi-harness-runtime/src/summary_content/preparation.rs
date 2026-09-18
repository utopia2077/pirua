use super::*;
use pi_ai_core::{Message, StopReason};
use std::collections::BTreeMap;

/// A logical position inside a real row; not a fabricated committed entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionOrigin {
    Stored(EntryId),
    /// Reserved future response identity, with no committed metadata.
    Prospective(EntryId),
    Retained {
        entry_id: EntryId,
        index: SafeUnsigned,
    },
    Projected {
        entry_id: EntryId,
        index: SafeUnsigned,
    },
}
/// Source entry distinction retained for cuts and branch-budget exceptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionKind {
    Message,
    BranchSummary,
    CompactionSummary,
    Boundary,
}
/// Detached content and its actual source lineage.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedPosition {
    pub origin: ProjectionOrigin,
    pub kind: PositionKind,
    pub message: Option<AgentMessage>,
    pub file_ops: DurableFileOperations,
}
/// Source facts not recoverable from the projected message vector alone.
#[derive(Clone, Debug, PartialEq)]
pub struct DetachedBranch {
    pub positions: Vec<ProjectedPosition>,
    pub previous_summary: Option<String>,
    pub previous_file_ops: DurableFileOperations,
    /// True only if the effective branch leaf itself is a compaction row.
    pub ends_at_compaction: bool,
}
fn retain(m: &AgentMessage) -> bool {
    !matches!(m,AgentMessage::BuiltIn(Message::Assistant(a)) if matches!(a.stop_reason,StopReason::Error|StopReason::Aborted|StopReason::Deferred))
}
/// Project already-detached chronological rows. Custom outputs and rendered summaries
/// must be computed by the owner from the exact committed rows, outside locks.
/// `prospective_tail` represents a filtered future response without faking row metadata.
pub fn project_branch(
    entries: &[Entry],
    custom: &BTreeMap<EntryId, Vec<AgentMessage>>,
    summaries: &BTreeMap<EntryId, AgentMessage>,
    prospective_tail: Option<EntryId>,
) -> Result<DetachedBranch, ContentError> {
    let start = entries
        .iter()
        .rposition(|e| matches!(e, Entry::Compaction(_)))
        .unwrap_or(0);
    let mut branch = DetachedBranch {
        positions: vec![],
        previous_summary: None,
        previous_file_ops: DurableFileOperations::default(),
        ends_at_compaction: prospective_tail.is_none()
            && matches!(entries.last(), Some(Entry::Compaction(_))),
    };
    for entry in &entries[start..] {
        match entry {
            Entry::Message(e) => {
                branch.positions.push(ProjectedPosition {
                    origin: ProjectionOrigin::Stored(e.id.clone()),
                    kind: if retain(&e.message) {
                        PositionKind::Message
                    } else {
                        PositionKind::Boundary
                    },
                    message: retain(&e.message).then(|| e.message.clone()),
                    file_ops: Default::default(),
                });
            }
            Entry::Custom(e) => {
                let outputs = custom.get(&e.id).map(Vec::as_slice).unwrap_or(&[]);
                if outputs.is_empty() {
                    branch.positions.push(ProjectedPosition {
                        origin: ProjectionOrigin::Stored(e.id.clone()),
                        kind: PositionKind::Boundary,
                        message: None,
                        file_ops: Default::default(),
                    })
                }
                for (index, message) in outputs.iter().enumerate() {
                    branch.positions.push(ProjectedPosition {
                        origin: ProjectionOrigin::Projected {
                            entry_id: e.id.clone(),
                            index: number(index as u64)?,
                        },
                        kind: PositionKind::Message,
                        message: Some(message.clone()),
                        file_ops: Default::default(),
                    })
                }
            }
            Entry::Compaction(e) => {
                branch.previous_summary = Some(e.summary.clone());
                let mut files = Files::default();
                files.details(&e.details);
                branch.previous_file_ops = files.finish()?;
                branch.positions.push(ProjectedPosition {
                    origin: ProjectionOrigin::Stored(e.id.clone()),
                    kind: PositionKind::CompactionSummary,
                    message: Some(
                        summaries
                            .get(&e.id)
                            .ok_or(ContentError::InvalidInput)?
                            .clone(),
                    ),
                    file_ops: branch.previous_file_ops.clone(),
                });
                for (index, m) in e.retained_tail.iter().enumerate() {
                    if retain(m) {
                        branch.positions.push(ProjectedPosition {
                            origin: ProjectionOrigin::Retained {
                                entry_id: e.id.clone(),
                                index: number(index as u64)?,
                            },
                            kind: PositionKind::Message,
                            message: Some(m.clone()),
                            file_ops: Default::default(),
                        })
                    }
                }
            }
            Entry::BranchSummary(e) => {
                let mut files = Files::default();
                files.details(&e.details);
                branch.positions.push(ProjectedPosition {
                    origin: ProjectionOrigin::Stored(e.id.clone()),
                    kind: PositionKind::BranchSummary,
                    message: Some(
                        summaries
                            .get(&e.id)
                            .ok_or(ContentError::InvalidInput)?
                            .clone(),
                    ),
                    file_ops: files.finish()?,
                });
            }
        }
    }
    if let Some(id) = prospective_tail {
        branch.positions.push(ProjectedPosition {
            origin: ProjectionOrigin::Prospective(id),
            kind: PositionKind::Boundary,
            message: None,
            file_ops: Default::default(),
        });
    }
    Ok(branch)
}
fn role(p: &ProjectedPosition) -> Option<String> {
    p.message
        .as_ref()
        .and_then(|m| value(m).ok())
        .and_then(|v| v["role"].as_str().map(str::to_owned))
}
/// Selected position indices, not committed sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CutPoint {
    pub first_kept: SafeUnsigned,
    pub turn_start: Option<SafeUnsigned>,
    pub split: bool,
}
/// Pinned cut algorithm on logical source positions.
pub fn find_cut_point(
    positions: &[ProjectedPosition],
    keep: SafeUnsigned,
) -> Result<CutPoint, ContentError> {
    let cuts: Vec<_> = positions
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.kind == PositionKind::BranchSummary
                || p.kind == PositionKind::Message
                    && matches!(
                        role(p).as_deref(),
                        Some(
                            "user"
                                | "assistant"
                                | "bashExecution"
                                | "custom"
                                | "branchSummary"
                                | "compactionSummary"
                        )
                    )
        })
        .map(|(i, _)| i)
        .collect();
    let Some(&first) = cuts.first() else {
        return Ok(CutPoint {
            first_kept: number(0)?,
            turn_start: None,
            split: false,
        });
    };
    let mut cut = first;
    let mut tokens = 0;
    for (i, p) in positions.iter().enumerate().rev() {
        if p.kind != PositionKind::Message {
            continue;
        }
        if let Some(m) = &p.message {
            tokens = add(tokens, estimate_tokens(m)?.get())?
        }
        if tokens >= keep.get() {
            if let Some(&c) = cuts.iter().find(|&&c| c >= i) {
                cut = c
            }
            break;
        }
    }
    while cut > 0
        && !matches!(
            positions[cut - 1].kind,
            PositionKind::CompactionSummary | PositionKind::Message
        )
    {
        cut -= 1
    }
    let user = positions[cut].kind == PositionKind::Message
        && role(&positions[cut]).as_deref() == Some("user");
    let turn = if user {
        None
    } else {
        (0..=cut).rev().find(|&i| {
            positions[i].kind == PositionKind::BranchSummary
                || positions[i].kind == PositionKind::Message
                    && matches!(
                        role(&positions[i]).as_deref(),
                        Some("user" | "bashExecution")
                    )
        })
    };
    Ok(CutPoint {
        first_kept: number(cut as u64)?,
        turn_start: turn.map(|n| number(n as u64)).transpose()?,
        split: !user && turn.is_some(),
    })
}
/// Prepare compaction from v3 projection while retaining physical empty/latest facts.
pub fn prepare_compaction(
    branch: &DetachedBranch,
    settings: CompactionSettings,
) -> Result<Option<CompactionPreparation>, ContentError> {
    if branch.positions.is_empty() || branch.ends_at_compaction {
        return Ok(None);
    }
    let context: Vec<_> = branch
        .positions
        .iter()
        .filter_map(|p| p.message.clone())
        .collect();
    let positions: Vec<_> = branch
        .positions
        .iter()
        .filter(|p| p.kind != PositionKind::CompactionSummary)
        .cloned()
        .collect();
    let cut = find_cut_point(&positions, settings.keep_recent_tokens)?;
    let first_kept =
        usize::try_from(cut.first_kept.get()).map_err(|_| ContentError::NumberRange)?;
    let history_end = if cut.split {
        usize::try_from(cut.turn_start.ok_or(ContentError::InvalidInput)?.get())
            .map_err(|_| ContentError::NumberRange)?
    } else {
        first_kept
    };
    let messages = |range: std::ops::Range<usize>| {
        positions[range]
            .iter()
            .filter_map(|p| p.message.clone())
            .collect::<Vec<_>>()
    };
    let history = messages(0..history_end);
    let prefix = if cut.split {
        messages(history_end..first_kept)
    } else {
        vec![]
    };
    let tail = messages(first_kept..positions.len());
    let mut files = Files::default();
    files.extend(&branch.previous_file_ops);
    for m in history.iter().chain(&prefix) {
        files.message(m)?
    }
    Ok(Some(CompactionPreparation {
        messages_to_summarize: history,
        turn_prefix_messages: prefix,
        retained_tail: tail,
        is_split_turn: cut.split,
        tokens_before: estimate_context(&context)?.tokens,
        previous_summary: branch.previous_summary.clone(),
        file_ops: files.finish()?,
        settings,
    }))
}
/// Prepare the already-selected abandoned branch with source budget/role rules.
pub fn prepare_branch(
    positions: &[ProjectedPosition],
    budget: i128,
) -> Result<BranchPreparation, ContentError> {
    let mut files = Files::default();
    for p in positions
        .iter()
        .filter(|p| p.kind == PositionKind::BranchSummary)
    {
        files.extend(&p.file_ops)
    }
    let mut messages = vec![];
    let mut total = 0;
    for p in positions.iter().rev() {
        let Some(m) = &p.message else { continue };
        if matches!(m, AgentMessage::BuiltIn(Message::ToolResult(_))) {
            continue;
        }
        files.message(m)?;
        let tokens = estimate_tokens(m)?.get();
        let next = add(total, tokens)?;
        if budget > 0 && i128::from(next) > budget {
            if matches!(
                p.kind,
                PositionKind::CompactionSummary | PositionKind::BranchSummary
            ) && (total as f64) < (budget as f64) * 0.9
            {
                messages.push(m.clone());
                total = next
            }
            break;
        }
        messages.push(m.clone());
        total = next;
    }
    messages.reverse();
    Ok(BranchPreparation {
        messages,
        file_ops: files.finish()?,
        total_tokens: number(total)?,
    })
}
/// Source branch budget; a zero model window selects the pinned 128000 fallback.
pub fn branch_budget(window: SafeUnsigned, reserve: SafeUnsigned) -> i128 {
    i128::from(if window.get() == 0 {
        128000
    } else {
        window.get()
    }) - i128::from(reserve.get())
}
