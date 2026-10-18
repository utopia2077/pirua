use crate::*;
use session::{ConversationQuery, ConversationRecord, TaskQuery};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct Graph {
    pub tasks: BTreeMap<TaskId, TaskRecord>,
    pub conversations: BTreeMap<ConversationId, ConversationRecord>,
}
impl Graph {
    pub fn read(tx: &mut Transaction<'_>) -> Result<Self> {
        let mut graph = Self::default();
        let mut cursor = None;
        loop {
            let page = tx.scan_tasks(&TaskQuery::default(), 256, cursor.as_ref())?;
            graph
                .tasks
                .extend(page.items.into_iter().map(|task| (task.id, task)));
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        loop {
            let page =
                tx.scan_conversations(&ConversationQuery::default(), 256, cursor.as_ref())?;
            graph
                .conversations
                .extend(page.items.into_iter().map(|c| (c.id, c)));
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        Ok(graph)
    }
    pub fn overlay(&mut self, tx: &Transaction<'_>) {
        self.tasks
            .extend(tx.staged_tasks().into_iter().map(|t| (t.id, t)));
        self.conversations
            .extend(tx.staged_conversations().into_iter().map(|c| (c.id, c)));
    }
    fn above(&self, task: &TaskRecord) -> Option<TaskId> {
        task.owner.or_else(|| {
            self.conversations
                .get(&task.conversation_id)?
                .owner
                .as_ref()
                .map(|owner| owner.task_id)
        })
    }
    pub fn owned_live(&self, owner: TaskId) -> Vec<TaskId> {
        self.tasks
            .values()
            .filter(|task| {
                !task.state.is_terminal() && task.id != owner && self.below(task, owner, false)
            })
            .map(|task| task.id)
            .collect()
    }
    pub fn below(&self, task: &TaskRecord, owner: TaskId, cross_background: bool) -> bool {
        let mut current = task;
        let mut seen = BTreeSet::new();
        loop {
            if !cross_background && current.background {
                return false;
            }
            let Some(id) = self.above(current) else {
                return false;
            };
            if id == owner {
                return true;
            }
            if !seen.insert(id) {
                return false;
            }
            let Some(above) = self.tasks.get(&id) else {
                return false;
            };
            current = above;
        }
    }
    pub fn cancelled_above(&self, task: &TaskRecord) -> Option<Option<AbortReason>> {
        let mut current = task;
        let mut seen = BTreeSet::new();
        loop {
            if current.background {
                return None;
            }
            let id = self.above(current)?;
            if !seen.insert(id) {
                return None;
            }
            current = self.tasks.get(&id)?;
            if !current.state.is_terminal() && (current.abort_requested || failed(current)) {
                return Some(if failed(current) {
                    None
                } else {
                    current.abort_reason
                });
            }
        }
    }
    pub fn in_scope(
        &self,
        task: &TaskRecord,
        conversation: Option<ConversationId>,
        cross_background: bool,
    ) -> bool {
        let mut current = task;
        let mut seen = BTreeSet::new();
        loop {
            if !cross_background && current.background {
                return false;
            }
            if let Some(owner) = current.owner {
                if !seen.insert(owner) {
                    return false;
                }
                let Some(above) = self.tasks.get(&owner) else {
                    return false;
                };
                current = above;
                continue;
            }
            if conversation == Some(current.conversation_id) {
                return true;
            }
            let Some(owner) = self
                .conversations
                .get(&current.conversation_id)
                .and_then(|c| c.owner.as_ref())
            else {
                return conversation.is_none();
            };
            if !seen.insert(owner.task_id) {
                return false;
            }
            let Some(above) = self.tasks.get(&owner.task_id) else {
                return false;
            };
            current = above;
        }
    }
    pub fn conversation_in_scope(
        &self,
        id: ConversationId,
        scope: ConversationId,
        cross_background: bool,
    ) -> bool {
        if id == scope {
            return true;
        }
        let Some(owner) = self
            .conversations
            .get(&id)
            .and_then(|c| c.owner.as_ref())
            .and_then(|owner| self.tasks.get(&owner.task_id))
        else {
            return false;
        };
        self.in_scope(owner, Some(scope), cross_background)
    }
    pub fn conversation_cancelled(&self, id: ConversationId) -> bool {
        let Some(owner) = self
            .conversations
            .get(&id)
            .and_then(|c| c.owner.as_ref())
            .and_then(|owner| self.tasks.get(&owner.task_id))
        else {
            return false;
        };
        (!owner.state.is_terminal() && (owner.abort_requested || failed(owner)))
            || self.cancelled_above(owner).is_some()
    }
    pub fn waiting_on(&self, record: &TaskRecord) -> Vec<TaskId> {
        if record.abort_requested {
            return self.owned_live(record.id);
        }
        if let TaskState::Waiting { on, .. } = &record.state {
            on.iter()
                .copied()
                .filter(|id| self.tasks.get(id).is_some_and(|t| !t.state.is_terminal()))
                .collect()
        } else {
            vec![]
        }
    }
    pub fn validate_wait(
        &self,
        current: &TaskRecord,
        on: &[TaskId],
        policy: JoinPolicy,
        mode: InvocationMode,
    ) -> Result<()> {
        if mode == InvocationMode::Abort {
            return Err(Error::request(format!(
                "Abort handler of task {} cannot wait",
                current.id
            )));
        }
        let mut pending = Vec::new();
        for id in on {
            let target = self
                .tasks
                .get(id)
                .ok_or_else(|| Error::request(format!("Task {id} does not exist")))?;
            if policy == JoinPolicy::FailFast && target.owner != Some(current.id) {
                return Err(Error::request(format!(
                    "Task {} can wait failFast only on tasks it owns; {id} is not one",
                    current.id
                )));
            }
            if !target.state.is_terminal() && target.owner != Some(current.id) {
                pending.push(*id);
            }
        }
        let mut from: BTreeMap<TaskId, Option<TaskId>> =
            pending.iter().map(|id| (*id, None)).collect();
        let mut visited = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if id == current.id {
                let mut path = vec![id];
                let mut at = id;
                while let Some(Some(above)) = from.get(&at) {
                    path.push(*above);
                    at = *above;
                }
                path.reverse();
                let target = path[0];
                path.insert(0, current.id);
                return Err(Error::request(format!(
                    "Task {} cannot wait on {target}: {} is a wait cycle",
                    current.id,
                    path.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(" -> ")
                )));
            }
            if !visited.insert(id) {
                continue;
            }
            let Some(task) = self.tasks.get(&id) else {
                continue;
            };
            if task.state.is_terminal() {
                continue;
            }
            let mut reach = self.owned_live(id);
            if !task.abort_requested
                && let TaskState::Waiting { on, .. } = &task.state
            {
                reach.extend(on);
            }
            for target in reach {
                if let std::collections::btree_map::Entry::Vacant(entry) = from.entry(target) {
                    entry.insert(Some(id));
                    pending.push(target);
                }
            }
        }
        Ok(())
    }
}
