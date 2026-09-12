//! Runtime validation of known durable message slots and policy-aware roots.
use crate::storage::{PublicationValidator, StorageSnapshot};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn context(message: &'static str) -> ErrorContext {
    ErrorContext::new(message).expect("static bounded context")
}
pub(crate) fn schema() -> SessionError {
    SessionError::Schema(context("session schema validation failed"))
}
pub(crate) fn invalid() -> SessionError {
    SessionError::InvalidOperation(context("operation is invalid for this session view"))
}
pub(crate) fn storage(error: StorageError) -> SessionError {
    if matches!(error, StorageError::Closed(_)) {
        SessionError::Closed(context("session handle is closed"))
    } else {
        SessionError::Storage(error)
    }
}

/// Compiled repository codec. No schema retrieval occurs during reads or publication.
pub struct SessionCodec {
    schemas: BTreeMap<CustomRole, jsonschema::Validator>,
    policy: ProviderHeaderPolicy,
}
impl SessionCodec {
    /// Compile registered custom schemas offline, with explicit durable-header policy.
    pub fn new(
        options: SessionCodecOptions,
        policy: ProviderHeaderPolicy,
    ) -> Result<Self, SessionError> {
        let mut schemas = BTreeMap::new();
        for (role, source) in options.custom_message_schemas() {
            let value = serde_json::to_value(source).map_err(|_| schema())?;
            let options = jsonschema::options().offline();
            let validator = if value.get("$schema").is_none() {
                options.with_draft(jsonschema::Draft::Draft7).build(&value)
            } else {
                options.build(&value)
            }
            .map_err(|_| schema())?;
            schemas.insert(role.clone(), validator);
        }
        Ok(Self { schemas, policy })
    }
    /// Validate a durable Agent message, including its registered custom role.
    pub fn validate_message(&self, message: &AgentMessage) -> Result<(), SessionError> {
        let value = serde_json::to_value(message).map_err(|_| schema())?;
        if let AgentMessage::Custom(message) = message {
            let validator = self.schemas.get(message.role()).ok_or_else(|| {
                SessionError::UnknownCustomRole(context("custom message role is not registered"))
            })?;
            if !validator.is_valid(&value) {
                return Err(schema());
            }
        }
        Ok(())
    }
    fn messages(&self, values: &[AgentMessage]) -> Result<(), SessionError> {
        for value in values {
            self.validate_message(value)?;
        }
        Ok(())
    }
    fn pending(&self, value: &PendingEntry) -> Result<(), SessionError> {
        match value.entry_type {
            PendingEntryType::Message => {
                if value.custom_type.is_some() {
                    return Err(schema());
                }
                let Presence::Value(payload) = &value.payload else {
                    return Err(schema());
                };
                let message: AgentMessage =
                    serde_json::from_value(serde_json::to_value(payload).map_err(|_| schema())?)
                        .map_err(|_| schema())?;
                self.validate_message(&message)
            }
            PendingEntryType::Custom => {
                if value.custom_type.is_none() {
                    Err(schema())
                } else {
                    Ok(())
                }
            }
        }
    }
    fn preparation(&self, value: &DurableStructuralPreparation) -> Result<(), SessionError> {
        match value {
            DurableStructuralPreparation::Compaction {
                messages_to_summarize,
                turn_prefix_messages,
                retained_tail,
                ..
            } => {
                self.messages(messages_to_summarize)?;
                self.messages(turn_prefix_messages)?;
                self.messages(retained_tail)
            }
            DurableStructuralPreparation::BranchSummary { messages, .. } => self.messages(messages),
        }
    }
    /// Validate a complete committed entry without interpreting opaque custom JSON.
    pub fn validate_entry(&self, entry: &Entry) -> Result<(), SessionError> {
        serde_json::to_writer(std::io::sink(), entry).map_err(|_| schema())?;
        match entry {
            Entry::Message(e) => self.validate_message(&e.message),
            Entry::Compaction(e) => self.messages(&e.retained_tail),
            _ => Ok(()),
        }
    }
    /// Validate a complete register using its supplied header policy and typed slots.
    pub fn validate_register(&self, register: &Register) -> Result<(), SessionError> {
        let bytes = serde_json::to_vec(register).map_err(|_| schema())?;
        let raw: RawRegister = serde_json::from_slice(&bytes).map_err(|_| schema())?;
        raw.into_durable(&self.policy).map_err(|_| schema())?;
        match register {
            Register::PendingEntry { value, .. } => self.pending(value),
            Register::OperationPreparation { value, .. } => self.preparation(value),
            _ => Ok(()),
        }
    }
    /// Validate serialization, header policy and every newly written typed message slot.
    pub fn validate_transaction(&self, transaction: &Transaction) -> Result<(), SessionError> {
        let bytes = serde_json::to_vec(transaction).map_err(|_| schema())?;
        let raw: RawTransaction = serde_json::from_slice(&bytes).map_err(|_| schema())?;
        raw.into_durable(&self.policy).map_err(|_| schema())?;
        for write in &transaction.writes {
            match write {
                Write::Entry(NewEntry::Message(e)) => self.validate_message(&e.message)?,
                Write::Entry(NewEntry::Compaction(e)) => self.messages(&e.retained_tail)?,
                Write::SetRegister(value) => match value.as_ref() {
                    RegisterSet::PendingEntry { value, .. } => self.pending(value)?,
                    RegisterSet::OperationPreparation { value, .. } => self.preparation(value)?,
                    _ => (),
                },
                _ => (),
            }
        }
        Ok(())
    }
    /// Decode a raw transaction with the repository policy, then validate custom schemas.
    pub fn decode_transaction(&self, bytes: &[u8]) -> Result<Transaction, SessionError> {
        let raw: RawTransaction = serde_json::from_slice(bytes).map_err(|_| schema())?;
        let transaction = raw.into_durable(&self.policy).map_err(|_| schema())?;
        self.validate_transaction(&transaction)?;
        Ok(transaction)
    }
    /// Decode a latest register with the repository policy and registered custom schemas.
    pub fn decode_register(&self, bytes: &[u8]) -> Result<Register, SessionError> {
        let raw: RawRegister = serde_json::from_slice(bytes).map_err(|_| schema())?;
        let value = raw.into_durable(&self.policy).map_err(|_| schema())?;
        self.validate_register(&value)?;
        Ok(value)
    }
    /// Decode a committed entry and validate its actual message slots.
    pub fn decode_entry(&self, bytes: &[u8]) -> Result<Entry, SessionError> {
        let value = serde_json::from_slice(bytes).map_err(|_| schema())?;
        self.validate_entry(&value)?;
        Ok(value)
    }
    /// Validate detached repository data before exposing an opened session.
    pub fn validate_snapshot(&self, snapshot: &StorageSnapshot) -> Result<(), SessionError> {
        for entry in &snapshot.entries {
            self.validate_entry(entry)?;
        }
        for register in &snapshot.registers {
            self.validate_register(register)?;
        }
        PendingPublication.validate(snapshot)
    }
}

/// Pure final-state guard, shared by every Session publication.
pub struct PendingPublication;
impl PublicationValidator for PendingPublication {
    type Error = SessionError;
    fn validate(&self, snapshot: &StorageSnapshot) -> Result<(), SessionError> {
        let ids: BTreeSet<_> = snapshot
            .entries
            .iter()
            .map(|e| crate::storage::values::structure(e).id)
            .collect();
        for register in &snapshot.registers {
            if let Register::PendingEntry { key, .. } = register
                && ids.iter().any(|id| id.as_str() == key)
            {
                return Err(schema());
            }
        }
        Ok(())
    }
}
