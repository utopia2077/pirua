use super::*;

fn limit(value: Option<SafeUnsigned>) -> usize {
    value.map_or(usize::MAX, |value| {
        usize::try_from(value.get()).unwrap_or(usize::MAX)
    })
}
fn selected(row: &EntryStructure, kind: Option<EntryType>, custom: Option<&CustomType>) -> bool {
    kind.is_none_or(|kind| row.entry_type == kind)
        && custom.is_none_or(|custom| row.custom_type.as_ref() == Some(custom))
}
fn in_range(seq: SafeUnsigned, from: Option<SafeUnsigned>, to: Option<SafeUnsigned>) -> bool {
    from.is_none_or(|bound| seq >= bound) && to.is_none_or(|bound| seq <= bound)
}
impl State {
    pub fn branch(&self, q: &BranchScan) -> Result<Vec<EntryStructure>, StorageError> {
        let start = q
            .start
            .as_ref()
            .ok_or_else(|| invalid("branch start is required"))?;
        if !self.structures.contains_key(start) {
            return Err(corrupt("branch start is missing"));
        }
        let newest = q.order.unwrap_or(BranchOrder::NewestFirst) == BranchOrder::NewestFirst;
        let stops = |row: &EntryStructure| {
            q.stop_at_id.as_ref() == Some(&row.id) || q.stop_at_type == Some(row.entry_type)
        };
        let mut path = Vec::new();
        let mut current = Some(start);
        // Structure-only traversal. Newest-first can stop before reading older structure;
        // payload hydration is performed only after the complete selection below.
        while let Some(id) = current {
            let row = self
                .structures
                .get(id)
                .ok_or_else(|| corrupt("branch parent is missing"))?;
            path.push(row.clone());
            if newest && stops(row) {
                break;
            }
            current = row.parent_id.as_ref();
        }
        if !newest {
            path.reverse();
        }
        if let Some(stop) = path.iter().position(stops) {
            path.truncate(stop + 1);
        }
        Ok(path
            .into_iter()
            .filter(|row| selected(row, q.entry_type, q.custom_type.as_ref()))
            .filter(|row| {
                q.cursor.is_none_or(|cursor| {
                    if newest {
                        row.seq < cursor.seq
                    } else {
                        row.seq > cursor.seq
                    }
                })
            })
            .take(limit(q.limit))
            .collect())
    }
    pub fn scan_entries(&self, q: &EntryScan) -> Vec<Entry> {
        let rows = self
            .entry_seq
            .iter()
            .filter(|(seq, _)| in_range(**seq, q.from_seq, q.to_seq))
            .filter(|(_, id)| {
                selected(&self.structures[*id], q.entry_type, q.custom_type.as_ref())
            });
        if q.order == Some(ScanOrder::Descending) {
            rows.rev()
                .take(limit(q.limit))
                .map(|(_, id)| self.entries[id].clone())
                .collect()
        } else {
            rows.take(limit(q.limit))
                .map(|(_, id)| self.entries[id].clone())
                .collect()
        }
    }
    pub fn scan_usage(&self, q: &UsageScan) -> Vec<UsageRow> {
        let rows = self
            .usage
            .iter()
            .filter(|(seq, _)| in_range(**seq, q.from_seq, q.to_seq));
        if q.order == Some(ScanOrder::Descending) {
            rows.rev()
                .take(limit(q.limit))
                .map(|(_, row)| row.clone())
                .collect()
        } else {
            rows.take(limit(q.limit))
                .map(|(_, row)| row.clone())
                .collect()
        }
    }
}
