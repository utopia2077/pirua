use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use ring::rand::{SecureRandom, SystemRandom};
use serde_json::Value;

use crate::{
    ContentReplacement, EntryContent, Error, SessionCommit, SessionEntry, SessionHeader,
    SessionProjection, SessionSnapshot, SessionStore, TreeNode,
    projection::{branch, project_validated},
    store::validate_snapshot,
};

/// Compaction's system checkpoint is supplied by the AI transcript owner.
/// This avoids teaching the storage layer how to replay provider tool declarations.
#[derive(Clone, Debug, Default)]
pub struct CompactionOptions {
    pub summary: String,
    pub first_kept_entry_id: Option<String>,
    pub tokens_before: u64,
    pub details: Option<Value>,
    pub from_hook: Option<bool>,
    pub usage: Option<Value>,
    pub system_message: Option<Value>,
}

/// Process-local handle with a versioned snapshot of one canonical stored session.
/// Writes adopt state only after the store commits. On conflict, call reload first.
pub struct SessionManager {
    store: Arc<dyn SessionStore>,
    snapshot: SessionSnapshot,
}

impl SessionManager {
    pub fn create(store: Arc<dyn SessionStore>, cwd: impl Into<String>) -> Result<Self, Error> {
        Self::create_with_header(
            store,
            SessionHeader {
                id: random_id(16)?,
                cwd: cwd.into(),
                timestamp: now(),
                parent_session: None,
            },
        )
    }

    pub fn create_with_header(
        store: Arc<dyn SessionStore>,
        header: SessionHeader,
    ) -> Result<Self, Error> {
        let snapshot = SessionSnapshot {
            header,
            revision: 0,
            leaf_id: None,
            entries: Vec::new(),
        };
        validate_snapshot(&snapshot)?;
        store.create(&snapshot)?;
        Ok(Self { store, snapshot })
    }

    pub fn open(store: Arc<dyn SessionStore>, id: &str) -> Result<Self, Error> {
        let snapshot = store.load(id)?;
        validate_snapshot(&snapshot)?;
        if snapshot.header.id != id {
            return Err(Error::Invalid(
                "store returned a different session".to_owned(),
            ));
        }
        Ok(Self { store, snapshot })
    }

    pub fn reload(&mut self) -> Result<(), Error> {
        let snapshot = self.store.load(&self.snapshot.header.id)?;
        validate_snapshot(&snapshot)?;
        if snapshot.header.id != self.snapshot.header.id {
            return Err(Error::Invalid(
                "store returned a different session".to_owned(),
            ));
        }
        self.snapshot = snapshot;
        Ok(())
    }

    pub fn snapshot(&self) -> &SessionSnapshot {
        &self.snapshot
    }
    pub fn header(&self) -> &SessionHeader {
        &self.snapshot.header
    }
    pub fn entries(&self) -> &[SessionEntry] {
        &self.snapshot.entries
    }
    pub fn leaf_id(&self) -> Option<&str> {
        self.snapshot.leaf_id.as_deref()
    }
    pub fn get_entry(&self, id: &str) -> Option<&SessionEntry> {
        self.snapshot.entries.iter().find(|entry| entry.id == id)
    }
    pub fn get_branch(&self) -> Vec<SessionEntry> {
        branch(&self.snapshot, self.leaf_id())
    }
    pub fn projection(&self) -> SessionProjection {
        project_validated(&self.snapshot)
    }
    pub fn children(&self, id: &str) -> Vec<&SessionEntry> {
        self.entries()
            .iter()
            .filter(|entry| entry.parent_id.as_deref() == Some(id))
            .collect()
    }

    /// Atomically appends entries and moves the leaf. No local state changes on failure.
    pub fn commit(
        &mut self,
        append: Vec<SessionEntry>,
        leaf_id: Option<String>,
    ) -> Result<(), Error> {
        let commit = SessionCommit {
            expected_revision: self.snapshot.revision,
            append,
            leaf_id,
        };
        let revision = self.store.commit(&self.snapshot.header.id, &commit)?;
        self.snapshot.entries.extend(commit.append);
        self.snapshot.leaf_id = commit.leaf_id;
        self.snapshot.revision = revision;
        Ok(())
    }

    pub fn append(&mut self, content: EntryContent) -> Result<String, Error> {
        self.append_at(content, now())
    }

    pub fn append_at(&mut self, content: EntryContent, timestamp: u64) -> Result<String, Error> {
        let id = self.new_entry_id()?;
        let entry = SessionEntry {
            id: id.clone(),
            parent_id: self.snapshot.leaf_id.clone(),
            timestamp,
            content,
        };
        self.commit(vec![entry], Some(id.clone()))?;
        Ok(id)
    }

    pub fn append_message(&mut self, message: Value) -> Result<String, Error> {
        if matches!(
            message["role"].as_str(),
            Some("compactionSummary" | "branchSummary")
        ) {
            return Err(Error::Invalid(
                "summaries must be appended as session entries".to_owned(),
            ));
        }
        self.append(EntryContent::Message { message })
    }

    pub fn set_model(
        &mut self,
        provider: impl Into<String>,
        model_id: impl Into<String>,
    ) -> Result<String, Error> {
        self.append(EntryContent::ModelChange {
            provider: provider.into(),
            model_id: model_id.into(),
        })
    }

    pub fn set_thinking_level(
        &mut self,
        thinking_level: impl Into<String>,
    ) -> Result<String, Error> {
        self.append(EntryContent::ThinkingLevelChange {
            thinking_level: thinking_level.into(),
        })
    }

    pub fn set_name(&mut self, name: &str) -> Result<String, Error> {
        let mut sanitized = String::new();
        let mut newline = false;
        for character in name.chars() {
            if matches!(character, '\n' | '\r') {
                if !newline {
                    sanitized.push(' ');
                }
                newline = true;
            } else {
                sanitized.push(character);
                newline = false;
            }
        }
        self.append(EntryContent::SessionInfo {
            name: sanitized.trim().to_owned(),
        })
    }

    pub fn set_label(&mut self, target_id: &str, label: Option<String>) -> Result<String, Error> {
        self.require_entry(target_id)?;
        self.append(EntryContent::Label {
            target_id: target_id.to_owned(),
            label: label.filter(|label| !label.is_empty()),
        })
    }

    pub fn label(&self, target_id: &str) -> Option<&str> {
        self.entries()
            .iter()
            .rev()
            .find_map(|entry| match &entry.content {
                EntryContent::Label {
                    target_id: target,
                    label,
                } if target == target_id => Some(label.as_deref()),
                _ => None,
            })
            .flatten()
    }

    pub fn branch(&mut self, leaf_id: Option<&str>) -> Result<(), Error> {
        if let Some(id) = leaf_id {
            self.require_entry(id)?;
        }
        self.commit(Vec::new(), leaf_id.map(str::to_owned))
    }

    pub fn branch_with_summary(
        &mut self,
        destination: Option<&str>,
        summary: String,
        details: Option<Value>,
        usage: Option<Value>,
    ) -> Result<String, Error> {
        if let Some(id) = destination {
            self.require_entry(id)?;
        }
        let id = self.new_entry_id()?;
        let entry = SessionEntry {
            id: id.clone(),
            parent_id: destination.map(str::to_owned),
            timestamp: now(),
            content: EntryContent::BranchSummary {
                from_id: self.leaf_id().unwrap_or("root").to_owned(),
                summary,
                details,
                from_hook: None,
                usage,
            },
        };
        self.commit(vec![entry], Some(id.clone()))?;
        Ok(id)
    }

    pub fn append_compaction(&mut self, options: CompactionOptions) -> Result<String, Error> {
        if let Some(kept) = &options.first_kept_entry_id
            && !self.get_branch().iter().any(|entry| entry.id == *kept)
        {
            return Err(Error::Invalid(
                "compaction tail must begin on the active branch".to_owned(),
            ));
        }
        if let Some(system) = &options.system_message
            && system["role"] != "system"
        {
            return Err(Error::Invalid(
                "compaction checkpoint must be a system message".to_owned(),
            ));
        }
        let id = self.new_entry_id()?;
        let entry = SessionEntry {
            id: id.clone(),
            parent_id: self.snapshot.leaf_id.clone(),
            timestamp: now(),
            content: EntryContent::Compaction {
                summary: options.summary,
                first_kept_entry_id: options.first_kept_entry_id.unwrap_or_else(|| id.clone()),
                tokens_before: options.tokens_before,
                details: options.details,
                from_hook: options.from_hook,
                usage: options.usage,
                system_message: options.system_message,
            },
        };
        self.commit(vec![entry], Some(id.clone()))?;
        Ok(id)
    }

    pub fn edit_context(
        &mut self,
        target_id: &str,
        replacement: Option<ContentReplacement>,
    ) -> Result<String, Error> {
        let target = self.require_entry(target_id)?;
        let editable = match &target.content {
            EntryContent::CustomMessage { .. } => true,
            EntryContent::Message { message } => matches!(
                message["role"].as_str(),
                Some("user" | "assistant" | "toolResult")
            ),
            _ => false,
        };
        if !editable || !self.get_branch().iter().any(|entry| entry.id == target_id) {
            return Err(Error::Invalid(
                "context edit requires editable content on the active branch".to_owned(),
            ));
        }
        if let Some(replacement) = &replacement
            && !replacement.content.is_string()
            && !replacement.content.is_array()
        {
            return Err(Error::Invalid(
                "context replacement must contain string or array content".to_owned(),
            ));
        }
        self.append(EntryContent::ContextEdit {
            target_id: target_id.to_owned(),
            replacement,
        })
    }

    /// Copies one path into a new independent session. Labels are resolved globally,
    /// then reattached while preserving the compacted tail through removed label nodes.
    pub fn fork(&self, leaf_id: &str) -> Result<Self, Error> {
        self.require_entry(leaf_id)?;
        let path = branch(&self.snapshot, Some(leaf_id));
        let mut entries = Vec::new();
        let mut parent_id = None;
        let mut labels = Vec::new();
        let mut replacements = BTreeMap::new();
        for mut entry in path {
            if matches!(entry.content, EntryContent::Label { .. }) {
                labels.push(entry.id);
                continue;
            }
            for label in labels.drain(..) {
                replacements.insert(label, entry.id.clone());
            }
            entry.parent_id = parent_id;
            if let EntryContent::Compaction {
                first_kept_entry_id,
                ..
            } = &mut entry.content
                && let Some(replacement) = replacements.get(first_kept_entry_id)
            {
                *first_kept_entry_id = replacement.clone();
            }
            parent_id = Some(entry.id.clone());
            entries.push(entry);
        }
        // JS Map preserves first insertion order: replacement keeps its position,
        // while clear/re-add moves the label to the end. Keep the source timestamp.
        let mut inherited_labels: Vec<(String, String, u64)> = Vec::new();
        for entry in self.entries() {
            if let EntryContent::Label { target_id, label } = &entry.content {
                let position = inherited_labels
                    .iter()
                    .position(|(id, _, _)| id == target_id);
                match label.as_ref().filter(|label| !label.is_empty()) {
                    Some(label) => {
                        let resolved = (target_id.clone(), label.clone(), entry.timestamp);
                        if let Some(position) = position {
                            inherited_labels[position] = resolved;
                        } else {
                            inherited_labels.push(resolved);
                        }
                    }
                    None => {
                        if let Some(position) = position {
                            inherited_labels.remove(position);
                        }
                    }
                }
            }
        }
        inherited_labels.retain(|(id, _, _)| entries.iter().any(|entry| entry.id == *id));
        for (target_id, label, timestamp) in inherited_labels {
            let id = random_id(8)?;
            entries.push(SessionEntry {
                id: id.clone(),
                parent_id,
                timestamp,
                content: EntryContent::Label {
                    target_id,
                    label: Some(label),
                },
            });
            parent_id = Some(id);
        }
        let snapshot = SessionSnapshot {
            header: SessionHeader {
                id: random_id(16)?,
                cwd: self.header().cwd.clone(),
                timestamp: now(),
                parent_session: Some(self.header().id.clone()),
            },
            revision: 0,
            leaf_id: parent_id,
            entries,
        };
        self.store.create(&snapshot)?;
        Ok(Self {
            store: self.store.clone(),
            snapshot,
        })
    }

    pub fn tree(&self) -> Vec<TreeNode> {
        let mut nodes: BTreeMap<String, TreeNode> = BTreeMap::new();
        let mut roots = Vec::new();
        let entries = self
            .entries()
            .iter()
            .map(|entry| (entry.id.as_str(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut labels = BTreeMap::new();
        for entry in self.entries() {
            if let EntryContent::Label { target_id, label } = &entry.content {
                match label.as_deref().filter(|label| !label.is_empty()) {
                    Some(label) => {
                        labels.insert(target_id.as_str(), (label, entry.timestamp));
                    }
                    None => {
                        labels.remove(target_id.as_str());
                    }
                }
            }
        }
        // Every parent precedes its children in validated append order.
        for entry in self.entries().iter().rev() {
            let mut node = nodes.remove(&entry.id).unwrap_or_else(|| TreeNode {
                entry: entry.clone(),
                children: Vec::new(),
                label: None,
                label_timestamp: None,
            });
            if let Some((label, timestamp)) = labels.get(entry.id.as_str()) {
                node.label = Some((*label).to_owned());
                node.label_timestamp = Some(*timestamp);
            }
            node.children.reverse();
            node.children.sort_by_key(|child| child.entry.timestamp);
            if let Some(parent_id) = &entry.parent_id {
                let parent = entries[parent_id.as_str()];
                nodes
                    .entry(parent_id.clone())
                    .or_insert_with(|| TreeNode {
                        entry: parent.clone(),
                        children: Vec::new(),
                        label: None,
                        label_timestamp: None,
                    })
                    .children
                    .push(node);
            } else {
                roots.push(node);
            }
        }
        roots.reverse();
        roots
    }

    fn require_entry(&self, id: &str) -> Result<&SessionEntry, Error> {
        self.get_entry(id)
            .ok_or_else(|| Error::NotFound(id.to_owned()))
    }

    fn new_entry_id(&self) -> Result<String, Error> {
        loop {
            let id = random_id(8)?;
            if self.get_entry(&id).is_none() {
                return Ok(id);
            }
        }
    }
}

fn now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn random_id(bytes: usize) -> Result<String, Error> {
    let mut random = vec![0_u8; bytes];
    SystemRandom::new()
        .fill(&mut random)
        .map_err(|_| Error::Entropy)?;
    Ok(random.iter().map(|byte| format!("{byte:02x}")).collect())
}
