use super::*;
use pi_agent_schema::session_values::FiniteF64;

#[derive(Clone)]
pub(super) struct State {
    pub entries: BTreeMap<EntryId, Entry>,
    pub structures: BTreeMap<EntryId, EntryStructure>,
    pub entry_seq: BTreeMap<SafeUnsigned, EntryId>,
    pub registers: BTreeMap<RegisterKey, Register>,
    pub usage: BTreeMap<SafeUnsigned, UsageRow>,
    ids: BTreeSet<String>,
    next_seq: Option<SafeUnsigned>,
    storage_version: SafeUnsigned,
    store_generation: SafeUnsigned,
    pub stats: SessionStats,
}
fn safe(value: u64) -> Result<SafeUnsigned, StorageError> {
    SafeUnsigned::new(value).map_err(|_| invalid("usage or count exceeds safe integer range"))
}
fn add(a: SafeUnsigned, b: SafeUnsigned) -> Result<SafeUnsigned, StorageError> {
    safe(a.get() + b.get())
}
fn optional(
    a: Option<SafeUnsigned>,
    b: Option<SafeUnsigned>,
) -> Result<Option<SafeUnsigned>, StorageError> {
    match (a, b) {
        (None, None) => Ok(None),
        (Some(v), None) | (None, Some(v)) => Ok(Some(v)),
        (Some(a), Some(b)) => add(a, b).map(Some),
    }
}
fn cost(a: FiniteF64, b: FiniteF64) -> Result<FiniteF64, StorageError> {
    FiniteF64::new(a.get() + b.get()).map_err(|_| invalid("aggregate usage cost is not finite"))
}
fn add_usage(a: &mut Usage, b: &Usage) -> Result<(), StorageError> {
    a.input = add(a.input, b.input)?;
    a.output = add(a.output, b.output)?;
    a.cache_read = add(a.cache_read, b.cache_read)?;
    a.cache_write = add(a.cache_write, b.cache_write)?;
    a.cache_write_1h = optional(a.cache_write_1h, b.cache_write_1h)?;
    a.reasoning = optional(a.reasoning, b.reasoning)?;
    a.total_tokens = add(a.total_tokens, b.total_tokens)?;
    a.cost.input = cost(a.cost.input, b.cost.input)?;
    a.cost.output = cost(a.cost.output, b.cost.output)?;
    a.cost.cache_read = cost(a.cost.cache_read, b.cost.cache_read)?;
    a.cost.cache_write = cost(a.cost.cache_write, b.cost.cache_write)?;
    a.cost.total = cost(a.cost.total, b.cost.total)?;
    Ok(())
}
impl State {
    pub fn empty() -> Self {
        Self {
            entries: BTreeMap::new(),
            structures: BTreeMap::new(),
            entry_seq: BTreeMap::new(),
            registers: BTreeMap::new(),
            usage: BTreeMap::new(),
            ids: BTreeSet::new(),
            next_seq: Some(SafeUnsigned::new(0).expect("zero")),
            storage_version: SafeUnsigned::new(1).expect("one"),
            store_generation: SafeUnsigned::new(0).expect("zero"),
            stats: SessionStats {
                message_count: SafeUnsigned::new(0).expect("zero"),
                usage: Usage::zero(),
            },
        }
    }
    pub fn snapshot(&self) -> StorageSnapshot {
        StorageSnapshot {
            entries: self
                .entry_seq
                .values()
                .map(|id| self.entries[id].clone())
                .collect(),
            registers: self.registers.values().cloned().collect(),
            usage: self.usage.values().cloned().collect(),
            next_seq: self.next_seq,
            storage_version: self.storage_version,
            store_generation: self.store_generation,
            stats: self.stats.clone(),
        }
    }
    pub fn from_snapshot(snapshot: StorageSnapshot) -> Result<Self, StorageError> {
        serde_json::to_writer(std::io::sink(), &snapshot)
            .map_err(|_| invalid("snapshot is not serializable"))?;
        let mut state = Self::empty();
        state.next_seq = snapshot.next_seq;
        state.storage_version = snapshot.storage_version;
        state.store_generation = snapshot.store_generation;
        let mut tokens = BTreeSet::new();
        let mut entries = snapshot.entries;
        entries.sort_by_key(|entry| structure(entry).seq);
        for entry in entries {
            let row = structure(&entry);
            if !tokens.insert(row.seq) {
                return Err(corrupt("duplicate snapshot sequence"));
            }
            state.insert_entry(entry)?;
        }
        let mut usage = snapshot.usage;
        usage.sort_by_key(|row| row.seq);
        for row in usage {
            if !tokens.insert(row.seq) {
                return Err(corrupt("duplicate snapshot sequence"));
            }
            if let Some(entry_id) = &row.entry_id
                && state
                    .structures
                    .get(entry_id)
                    .is_none_or(|entry| entry.seq >= row.seq)
            {
                return Err(corrupt("usage precedes its entry"));
            }
            state.insert_usage(row)?;
        }
        for register in snapshot.registers {
            if !tokens.insert(register_seq(&register)) {
                return Err(corrupt("duplicate snapshot sequence"));
            }
            state.check_register(&register)?;
            if state
                .registers
                .insert(register_key(&register), register)
                .is_some()
            {
                return Err(corrupt("duplicate snapshot register"));
            }
        }
        if let Some(next) = state.next_seq
            && tokens.last().is_some_and(|last| *last >= next)
        {
            return Err(corrupt(
                "snapshot allocation does not follow retained sequences",
            ));
        }
        // Exhaustion may follow an unretained delete token; no historical log exists.
        if state.stats != snapshot.stats {
            return Err(corrupt("snapshot statistics disagree with ledger"));
        }
        Ok(state)
    }
    fn insert_entry(&mut self, entry: Entry) -> Result<(), StorageError> {
        let row = structure(&entry);
        if !self.ids.insert(row.id.as_str().to_owned()) {
            return Err(corrupt("duplicate entry or usage identity"));
        }
        if let Some(parent) = &row.parent_id
            && self
                .structures
                .get(parent)
                .is_none_or(|parent| parent.seq >= row.seq)
        {
            return Err(corrupt("entry parent is missing or not earlier"));
        }
        if row.entry_type == EntryType::Message {
            self.stats.message_count =
                add(self.stats.message_count, SafeUnsigned::new(1).expect("one"))?;
        }
        self.entry_seq.insert(row.seq, row.id.clone());
        self.entries.insert(row.id.clone(), entry);
        self.structures.insert(row.id.clone(), row);
        Ok(())
    }
    fn insert_usage(&mut self, row: UsageRow) -> Result<(), StorageError> {
        if !self.ids.insert(row.id.as_str().to_owned()) {
            return Err(corrupt("duplicate entry or usage identity"));
        }
        if row
            .entry_id
            .as_ref()
            .is_some_and(|id| !self.entries.contains_key(id))
        {
            return Err(corrupt("usage entry is missing"));
        }
        add_usage(&mut self.stats.usage, &row.usage)?;
        self.usage.insert(row.seq, row);
        Ok(())
    }
    fn check_register(&self, register: &Register) -> Result<(), StorageError> {
        if let Register::LaneLeaf {
            value: Some(id), ..
        } = register
            && self
                .structures
                .get(id)
                .is_none_or(|entry| entry.seq >= register_seq(register))
        {
            return Err(corrupt("register entry is missing or not earlier"));
        }
        Ok(())
    }
    pub fn matches(&self, conditions: &[RegisterCondition]) -> bool {
        conditions.iter().all(|condition| {
            match (self.registers.get(&condition.register), condition.expected) {
                (None, ExpectedRegister::Absent) => true,
                (Some(value), ExpectedRegister::Seq(seq)) => register_seq(value) == seq,
                _ => false,
            }
        })
    }
    pub(super) fn next_seq(&self) -> Option<SafeUnsigned> {
        self.next_seq
    }
    pub fn apply(
        &mut self,
        tx: Transaction,
        timestamp: SafeUnsigned,
    ) -> Result<CommitResult, StorageError> {
        let first_seq = self.next_seq.ok_or_else(exhausted)?;
        let mut seqs = Vec::with_capacity(tx.writes.len());
        for write in tx.writes {
            let seq = self.next_seq.ok_or_else(exhausted)?;
            self.next_seq = SafeUnsigned::new(seq.get() + 1).ok();
            match write {
                Write::Entry(entry) => self.insert_entry(materialize(entry, seq, timestamp))?,
                Write::Usage(row) => self.insert_usage(UsageRow {
                    id: row.id,
                    seq,
                    usage: row.usage,
                    entry_id: row.entry_id,
                    adjustment: row.adjustment,
                    details: row.details,
                })?,
                Write::SetRegister(value) => {
                    let register = set_register(*value, seq);
                    self.check_register(&register)?;
                    self.registers.insert(register_key(&register), register);
                }
                Write::DeleteRegister(key) => {
                    self.registers.remove(&RegisterKey {
                        namespace: key.namespace,
                        key: key.key,
                    });
                }
            }
            seqs.push(seq);
        }
        Ok(CommitResult {
            first_seq,
            seqs,
            timestamp,
        })
    }
}

impl State {
    pub fn hydrate(&self, id: &EntryId) -> Entry {
        #[cfg(test)]
        super::tests::HYDRATED.with(|seen| seen.borrow_mut().push(id.clone()));
        self.entries[id].clone()
    }
}
