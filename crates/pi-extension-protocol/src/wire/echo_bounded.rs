//! Bounded, borrowed wire index for the active echo route.
//!
//! This is a zero-credit source seam. It validates a whole v1.1 Envelope in
//! wire order and preserves Prost's merge rules without materializing a DTO.
//! It does not perform value/JCS/schema/typed admission or authorize Credit.

use std::alloc::Layout;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

const NONE: usize = usize::MAX;
const PROST_RECURSION_LIMIT: usize = 100;
// ProtocolLimits has the largest known field tag in the active schema (17).
// Slot zero is unused because protobuf field tags start at one.
const LAST_RECORD_SLOTS: usize = 18;
// Process-unique successful-scan identity, independent of arena address or
// storage lease. Exhaustion fails closed instead of reusing a prior identity.
static NEXT_SCAN_TOKEN: AtomicU64 = AtomicU64::new(0);

fn next_scan_token() -> Result<u64, ScanError> {
    NEXT_SCAN_TOKEN
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
            previous.checked_add(1)
        })
        .map_err(|_| ScanError::Limit)?
        .checked_add(1)
        .ok_or(ScanError::Limit)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanError {
    Malformed,
    Limit,
    Allocation,
    UnsupportedVariant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectedMinor {
    V1_0,
    V1_1,
}

/// Minimum requested payload for the selected frame allowance. Legacy
/// `IndexArena::budget` reports actual retained Vec capacity instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexBudget {
    pub field_capacity: usize,
    pub state_capacity: usize,
    pub capacity_bytes: usize,
}

impl IndexBudget {
    pub fn checked(frame_bytes: NonZeroUsize) -> Result<Self, ScanError> {
        let field_capacity = frame_bytes.get();
        let state_capacity = field_capacity.checked_add(1).ok_or(ScanError::Limit)?;
        let fields = Layout::array::<FieldRecord>(field_capacity).map_err(|_| ScanError::Limit)?;
        let states = Layout::array::<MessageState>(state_capacity).map_err(|_| ScanError::Limit)?;
        let capacity_bytes = fields
            .size()
            .checked_add(states.size())
            .ok_or(ScanError::Limit)?;
        Ok(Self {
            field_capacity,
            state_capacity,
            capacity_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FieldRecord {
    parent: usize,
    tag: u32,
    start: usize,
    end: usize,
    child: usize,
    // One-based index of the next direct field of the same message.
    next_sibling: usize,
}

impl FieldRecord {
    const EMPTY: Self = Self {
        parent: 0,
        tag: 0,
        start: 0,
        end: 0,
        child: NONE,
        next_sibling: 0,
    };
}

#[derive(Clone, Copy, Debug, Default)]
struct MessageState {
    kind: Kind,
    oneof_tag: u32,
    oneof_child: usize,
    // A zero slot means absent; otherwise it is the field index plus one.
    // Keeping these slots in the existing state array charges their capacity
    // to IndexBudget and makes singular-field merge and view lookup constant.
    last_record_by_tag: [usize; LAST_RECORD_SLOTS],
    first_record: usize,
    last_record: usize,
    selected: bool,
    encoded_size: usize,
}

impl MessageState {
    const EMPTY: Self = Self {
        kind: Kind::Envelope,
        oneof_tag: 0,
        oneof_child: NONE,
        last_record_by_tag: [0; LAST_RECORD_SLOTS],
        first_record: 0,
        last_record: 0,
        selected: false,
        encoded_size: 0,
    };
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
enum Kind {
    #[default]
    Envelope,
    Version,
    VersionRange,
    ProfileRange,
    ProfileVersion,
    PermissionRequest,
    PermissionGrant,
    DynamicProjectionDigest,
    ToolSchemaSet,
    CapabilityDeclaration,
    SupportedProfileDigest,
    CapabilityRef,
    ProtocolLimits,
    Hello,
    HelloAck,
    EventSubscriptionRequest,
    Manifest,
    CapabilityGrant,
    Ready,
    EffectKey,
    NullValue,
    WireList,
    WireMapEntry,
    WireMap,
    WireValue,
    SchemaBoundValue,
    TypedError,
    Request,
    Response,
    StreamItem,
    StreamEnd,
    Cancel,
    Credit,
    GoAway,
    ToolProgress,
    InvokeToolResult,
}

#[derive(Clone, Copy)]
enum FieldType {
    Varint,
    Fixed64,
    String,
    Bytes,
    Message(Kind),
}
#[derive(Clone, Copy)]
struct Spec {
    ty: FieldType,
    repeated: bool,
    oneof: bool,
}

macro_rules! specs {
    ($tag:expr; $($number:literal => $ty:expr $(, $mode:ident)?;)+) => {
        match $tag {
            $($number => Some(Spec { ty: $ty, repeated: specs!(@repeated $($mode)?), oneof: specs!(@oneof $($mode)?) }),)+
            _ => None,
        }
    };
    (@repeated repeated) => { true };
    (@repeated oneof) => { false };
    (@repeated) => { false };
    (@oneof oneof) => { true };
    (@oneof repeated) => { false };
    (@oneof) => { false };
}

fn spec(kind: Kind, tag: u32) -> Option<Spec> {
    use FieldType::{Bytes as B, Fixed64 as F64, Message as M, String as S, Varint as V};
    use Kind as K;
    match kind {
        K::Envelope => {
            specs!(tag; 1=>M(K::Hello),oneof; 2=>M(K::HelloAck),oneof; 3=>M(K::Manifest),oneof; 4=>M(K::CapabilityGrant),oneof; 5=>M(K::Ready),oneof; 6=>M(K::Request),oneof; 7=>M(K::Response),oneof; 8=>M(K::StreamItem),oneof; 9=>M(K::StreamEnd),oneof; 10=>M(K::Cancel),oneof; 11=>M(K::Credit),oneof; 12=>M(K::GoAway),oneof;)
        }
        K::Version => specs!(tag; 1=>V; 2=>V;),
        K::VersionRange => specs!(tag; 1=>V; 2=>V; 3=>V;),
        K::ProfileRange => specs!(tag; 1=>S; 2=>V; 3=>V; 4=>V;),
        K::ProfileVersion => specs!(tag; 1=>S; 2=>V; 3=>V;),
        K::PermissionRequest => specs!(tag; 1=>S; 2=>S; 3=>V;),
        K::PermissionGrant => specs!(tag; 1=>S; 2=>S;),
        K::DynamicProjectionDigest => specs!(tag; 1=>S; 2=>B;),
        K::ToolSchemaSet => {
            specs!(tag; 1=>S; 2=>V; 3=>M(K::DynamicProjectionDigest); 4=>M(K::DynamicProjectionDigest); 5=>M(K::DynamicProjectionDigest);)
        }
        K::CapabilityDeclaration => {
            specs!(tag; 1=>S; 2=>S; 3=>M(K::ProfileRange); 4=>S; 6=>V; 7=>V; 8=>V; 9=>M(K::SupportedProfileDigest),repeated; 10=>S; 11=>M(K::ToolSchemaSet),repeated;)
        }
        K::SupportedProfileDigest => specs!(tag; 1=>V; 2=>B;),
        K::CapabilityRef => specs!(tag; 1=>S; 2=>S; 3=>S; 4=>M(K::ProfileVersion); 5=>B; 6=>S;),
        K::ProtocolLimits => {
            specs!(tag; 1=>V; 2=>V; 3=>V; 4=>V; 5=>V; 6=>V; 7=>V; 8=>V; 9=>V; 10=>V; 11=>V; 12=>V; 13=>V; 14=>V; 15=>V; 16=>V; 17=>V;)
        }
        K::Hello => specs!(tag; 1=>M(K::VersionRange); 2=>M(K::ProfileRange),repeated;),
        K::HelloAck => specs!(tag; 1=>M(K::Version);),
        K::EventSubscriptionRequest => specs!(tag; 1=>S;),
        K::Manifest => {
            specs!(tag; 1=>S; 2=>S; 3=>M(K::VersionRange); 4=>M(K::CapabilityDeclaration),repeated; 5=>M(K::PermissionRequest),repeated; 6=>M(K::EventSubscriptionRequest),repeated;)
        }
        K::CapabilityGrant => {
            specs!(tag; 1=>S; 2=>M(K::Version); 3=>M(K::CapabilityRef),repeated; 4=>M(K::PermissionGrant),repeated; 5=>M(K::ProtocolLimits); 6=>V;)
        }
        K::Ready | K::NullValue => None,
        K::EffectKey => specs!(tag; 1=>S; 2=>S; 3=>S; 4=>S; 5=>S; 6=>V; 7=>V;),
        K::WireList => specs!(tag; 1=>M(K::WireValue),repeated;),
        K::WireMapEntry => specs!(tag; 1=>S; 2=>M(K::WireValue);),
        K::WireMap => specs!(tag; 1=>M(K::WireMapEntry),repeated;),
        K::WireValue => {
            specs!(tag; 1=>M(K::NullValue),oneof; 2=>V,oneof; 3=>V,oneof; 4=>F64,oneof; 5=>S,oneof; 6=>B,oneof; 7=>M(K::WireList),oneof; 8=>M(K::WireMap),oneof;)
        }
        K::SchemaBoundValue => specs!(tag; 1=>S; 2=>B; 3=>M(K::WireValue);),
        K::TypedError => specs!(tag; 1=>V; 2=>S;),
        K::Request => {
            specs!(tag; 1=>V; 2=>V; 3=>M(K::CapabilityRef); 4=>M(K::EffectKey); 5=>S; 6=>V; 7=>V; 8=>B;)
        }
        K::Response => specs!(tag; 1=>V; 2=>B,oneof; 3=>M(K::TypedError),oneof;),
        K::StreamItem => specs!(tag; 1=>V; 2=>V; 3=>B;),
        K::StreamEnd => specs!(tag; 1=>V; 2=>B,oneof; 3=>M(K::TypedError),oneof;),
        K::Cancel => specs!(tag; 1=>V; 2=>V;),
        K::Credit => specs!(tag; 1=>V; 2=>V;),
        K::GoAway => specs!(tag; 1=>V; 2=>V;),
        K::ToolProgress => specs!(tag; 1=>M(K::SchemaBoundValue); 2=>V;),
        K::InvokeToolResult => specs!(tag; 1=>M(K::SchemaBoundValue);),
    }
}

#[derive(Clone, Copy, Default)]
struct ParseFrame {
    end: usize,
    state: usize,
    group_tag: u32,
    is_group: bool,
}

/// Opaque, initialized fixed storage for a future owner-accounted index lease.
/// The arrays are disjoint and aligned by Rust's type system. Construct this
/// during trusted pool initialization, before accepting peer-controlled work.
pub struct IndexStorage<const NF: usize, const NS: usize> {
    fields: [FieldRecord; NF],
    states: [MessageState; NS],
}

impl<const NF: usize, const NS: usize> IndexStorage<NF, NS> {
    pub const fn new() -> Self {
        Self {
            fields: [FieldRecord::EMPTY; NF],
            states: [MessageState::EMPTY; NS],
        }
    }

    /// Full backing charge, including unused entries and struct padding. A
    /// future owner must admit this charge before acquiring a storage lease.
    pub fn checked_slot_bytes(
        frame_bytes: NonZeroUsize,
        max_retained_bytes: usize,
    ) -> Result<usize, ScanError> {
        let selected = IndexBudget::checked(frame_bytes)?;
        if NF < selected.field_capacity || NS < selected.state_capacity {
            return Err(ScanError::Limit);
        }
        Layout::array::<FieldRecord>(NF).map_err(|_| ScanError::Limit)?;
        Layout::array::<MessageState>(NS).map_err(|_| ScanError::Limit)?;
        let bytes = Layout::new::<Self>().size();
        if bytes > isize::MAX as usize || bytes > max_retained_bytes {
            return Err(ScanError::Limit);
        }
        Ok(bytes)
    }

    /// Bind a checked prefix. The exclusive borrow lasts through every view
    /// made by the arena; the caller retains responsibility for its pool lease.
    pub fn borrow(
        &mut self,
        frame_bytes: NonZeroUsize,
        max_retained_bytes: usize,
    ) -> Result<BorrowedIndexArena<'_>, ScanError> {
        let slot_bytes = Self::checked_slot_bytes(frame_bytes, max_retained_bytes)?;
        let selected = IndexBudget::checked(frame_bytes)?;
        Ok(BorrowedIndexArena {
            fields: &mut self.fields[..selected.field_capacity],
            states: &mut self.states[..selected.state_capacity],
            selected,
            slot_bytes,
        })
    }
}

impl<const NF: usize, const NS: usize> Default for IndexStorage<NF, NS> {
    fn default() -> Self {
        Self::new()
    }
}

/// Scanner borrowing a checked prefix of an opaque fixed slot. This is an
/// inert protocol API; it does not reserve an owner permit or acquire a pool.
pub struct BorrowedIndexArena<'storage> {
    fields: &'storage mut [FieldRecord],
    states: &'storage mut [MessageState],
    selected: IndexBudget,
    slot_bytes: usize,
}

impl BorrowedIndexArena<'_> {
    pub fn selected_budget(&self) -> IndexBudget {
        self.selected
    }

    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }
}

/// Legacy allocating scanner factory. Its Vec reserves can exceed the checked
/// ceiling before capacity is measured; it is not an owner-bounded path.
pub struct IndexArena {
    fields: Vec<FieldRecord>,
    states: Vec<MessageState>,
    retained_bytes: usize,
}

impl IndexArena {
    pub fn try_new(frame_bytes: NonZeroUsize) -> Result<Self, ScanError> {
        Self::try_new_with_ceiling(frame_bytes, usize::MAX)
    }

    /// Rejects a retained index exceeding `max_retained_bytes`. The ceiling is
    /// checked before reserve against the minimum and after each reserve
    /// against actual capacity. This zero-credit seam does not yet prove a
    /// pre-allocation bound on allocator over-reservation.
    pub fn try_new_with_ceiling(
        frame_bytes: NonZeroUsize,
        max_retained_bytes: usize,
    ) -> Result<Self, ScanError> {
        let budget = IndexBudget::checked(frame_bytes)?;
        if budget.capacity_bytes > max_retained_bytes {
            return Err(ScanError::Limit);
        }
        let mut fields = Vec::new();
        fields
            .try_reserve_exact(budget.field_capacity)
            .map_err(|_| ScanError::Allocation)?;
        let first_bytes = Layout::array::<FieldRecord>(fields.capacity())
            .map_err(|_| ScanError::Limit)?
            .size();
        if first_bytes > max_retained_bytes {
            return Err(ScanError::Limit);
        }
        fields.resize(budget.field_capacity, FieldRecord::default());
        let mut states = Vec::new();
        states
            .try_reserve_exact(budget.state_capacity)
            .map_err(|_| ScanError::Allocation)?;
        let second_bytes = Layout::array::<MessageState>(states.capacity())
            .map_err(|_| ScanError::Limit)?
            .size();
        let retained_bytes = first_bytes
            .checked_add(second_bytes)
            .filter(|sum| *sum <= max_retained_bytes)
            .ok_or(ScanError::Limit)?;
        states.resize(budget.state_capacity, MessageState::default());
        Ok(Self {
            fields,
            states,
            retained_bytes,
        })
    }

    pub fn budget(&self) -> IndexBudget {
        IndexBudget {
            field_capacity: self.fields.capacity(),
            state_capacity: self.states.capacity(),
            capacity_bytes: self.retained_bytes,
        }
    }

    pub fn scan_envelope<'view, 'body>(
        &'view mut self,
        body: &'body [u8],
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        let view = self.scan(body, Kind::Envelope)?;
        if view.arm().is_none() {
            return Err(ScanError::Malformed);
        }
        Ok(view)
    }

    /// Parse the final StreamItem payload separately, after taking its borrowed
    /// bytes and dropping the Envelope view. This raw view exposes presence;
    /// `scan_selected_tool_progress` applies the selected-minor rule.
    pub fn scan_tool_progress<'view, 'body>(
        &'view mut self,
        payload: &'body [u8],
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        self.scan(payload, Kind::ToolProgress)
    }

    /// Mirrors the existing 1.0 probe: any v1.1 percent field occurrence,
    /// including present zero or repeated values, is unsupported in 1.0.
    pub fn scan_selected_tool_progress<'view, 'body>(
        &'view mut self,
        payload: &'body [u8],
        minor: SelectedMinor,
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        let view = self.scan_tool_progress(payload)?;
        if minor == SelectedMinor::V1_0 && view.has_field(view.root(), 2) {
            return Err(ScanError::UnsupportedVariant);
        }
        Ok(view)
    }

    pub fn scan_tool_result<'view, 'body>(
        &'view mut self,
        payload: &'body [u8],
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        self.scan(payload, Kind::InvokeToolResult)
    }

    fn scan<'view, 'body>(
        &'view mut self,
        body: &'body [u8],
        root: Kind,
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        ScanCore::new(&mut self.fields[..], &mut self.states[..]).scan(body, root)
    }
}

impl BorrowedIndexArena<'_> {
    pub fn scan_envelope<'view, 'body>(
        &'view mut self,
        body: &'body [u8],
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        let view = self.scan(body, Kind::Envelope)?;
        if view.arm().is_none() {
            return Err(ScanError::Malformed);
        }
        Ok(view)
    }

    /// Parse the final StreamItem payload separately, after taking its borrowed
    /// bytes and dropping the Envelope view. This raw view exposes presence;
    /// `scan_selected_tool_progress` applies the selected-minor rule.
    pub fn scan_tool_progress<'view, 'body>(
        &'view mut self,
        payload: &'body [u8],
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        self.scan(payload, Kind::ToolProgress)
    }

    /// Mirrors the existing 1.0 probe: any v1.1 percent field occurrence,
    /// including present zero or repeated values, is unsupported in 1.0.
    pub fn scan_selected_tool_progress<'view, 'body>(
        &'view mut self,
        payload: &'body [u8],
        minor: SelectedMinor,
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        let view = self.scan_tool_progress(payload)?;
        if minor == SelectedMinor::V1_0 && view.has_field(view.root(), 2) {
            return Err(ScanError::UnsupportedVariant);
        }
        Ok(view)
    }

    pub fn scan_tool_result<'view, 'body>(
        &'view mut self,
        payload: &'body [u8],
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        self.scan(payload, Kind::InvokeToolResult)
    }

    fn scan<'view, 'body>(
        &'view mut self,
        body: &'body [u8],
        root: Kind,
    ) -> Result<IndexedMessage<'view, 'body>, ScanError> {
        ScanCore::new(&mut self.fields[..], &mut self.states[..]).scan(body, root)
    }
}

struct ScanCore<'a> {
    fields: &'a mut [FieldRecord],
    states: &'a mut [MessageState],
    used_fields: usize,
    used_states: usize,
}

impl<'a> ScanCore<'a> {
    fn new(fields: &'a mut [FieldRecord], states: &'a mut [MessageState]) -> Self {
        Self {
            fields,
            states,
            used_fields: 0,
            used_states: 0,
        }
    }

    fn scan<'body>(
        mut self,
        body: &'body [u8],
        root: Kind,
    ) -> Result<IndexedMessage<'a, 'body>, ScanError> {
        if body.len() > self.fields.len() {
            return Err(ScanError::Limit);
        }
        self.used_fields = 0;
        self.used_states = 1;
        self.states[0] = MessageState {
            kind: root,
            oneof_tag: 0,
            oneof_child: NONE,
            last_record_by_tag: [0; LAST_RECORD_SLOTS],
            ..MessageState::EMPTY
        };
        let mut stack = [ParseFrame::default(); PROST_RECURSION_LIMIT + 1];
        stack[0] = ParseFrame {
            end: body.len(),
            state: 0,
            group_tag: 0,
            is_group: false,
        };
        let mut depth = 0usize;
        let mut cursor = 0usize;
        loop {
            let frame = stack[depth];
            if cursor == frame.end {
                if frame.is_group {
                    return Err(ScanError::Malformed);
                }
                if depth == 0 {
                    break;
                }
                depth -= 1;
                continue;
            }
            if cursor > frame.end {
                return Err(ScanError::Malformed);
            }
            let key = read_varint(body, &mut cursor, frame.end)?;
            if key > u64::from(u32::MAX) {
                return Err(ScanError::Malformed);
            }
            let tag = (key as u32) >> 3;
            let wire = (key & 7) as u8;
            if tag == 0 || wire > 5 {
                return Err(ScanError::Malformed);
            }
            if wire == 4 {
                if !frame.is_group || tag != frame.group_tag {
                    return Err(ScanError::Malformed);
                }
                if depth == 0 {
                    return Err(ScanError::Malformed);
                }
                depth -= 1;
                continue;
            }
            let field = if frame.is_group {
                None
            } else {
                spec(self.states[frame.state].kind, tag)
            };
            if let Some(field) = field {
                let expected = match field.ty {
                    FieldType::Varint => 0,
                    FieldType::Fixed64 => 1,
                    FieldType::String | FieldType::Bytes | FieldType::Message(_) => 2,
                };
                if wire != expected {
                    return Err(ScanError::Malformed);
                }
                let (start, end) = read_value(body, &mut cursor, frame.end, wire)?;
                if matches!(field.ty, FieldType::String)
                    && std::str::from_utf8(&body[start..end]).is_err()
                {
                    return Err(ScanError::Malformed);
                }
                let child = if let FieldType::Message(kind) = field.ty {
                    let previous = if field.repeated {
                        None
                    } else if field.oneof {
                        let state = self.states[frame.state];
                        (state.oneof_tag == tag && state.oneof_child != NONE)
                            .then_some(state.oneof_child)
                    } else {
                        self.last_record(frame.state, tag)
                            .and_then(|record| (record.child != NONE).then_some(record.child))
                    };
                    if let Some(previous) = previous {
                        previous
                    } else {
                        if self.used_states >= self.states.len() {
                            return Err(ScanError::Limit);
                        }
                        let id = self.used_states;
                        self.used_states += 1;
                        self.states[id] = MessageState {
                            kind,
                            oneof_tag: 0,
                            oneof_child: NONE,
                            last_record_by_tag: [0; LAST_RECORD_SLOTS],
                            ..MessageState::EMPTY
                        };
                        id
                    }
                } else {
                    NONE
                };
                if self.used_fields >= self.fields.len() {
                    return Err(ScanError::Limit);
                }
                let record_number = self.used_fields.checked_add(1).ok_or(ScanError::Limit)?;
                self.fields[self.used_fields] = FieldRecord {
                    parent: frame.state,
                    tag,
                    start,
                    end,
                    child,
                    next_sibling: 0,
                };
                let parent_state = &mut self.states[frame.state];
                if parent_state.last_record == 0 {
                    parent_state.first_record = record_number;
                } else {
                    self.fields[parent_state.last_record - 1].next_sibling = record_number;
                }
                parent_state.last_record = record_number;
                self.used_fields += 1;
                *self.states[frame.state]
                    .last_record_by_tag
                    .get_mut(tag as usize)
                    .ok_or(ScanError::Limit)? = record_number;
                if field.oneof {
                    let state = &mut self.states[frame.state];
                    state.oneof_tag = tag;
                    state.oneof_child = child;
                }
                if child != NONE {
                    if depth >= PROST_RECURSION_LIMIT {
                        return Err(ScanError::Malformed);
                    }
                    depth += 1;
                    stack[depth] = ParseFrame {
                        end,
                        state: child,
                        group_tag: 0,
                        is_group: false,
                    };
                    cursor = start;
                }
            } else {
                // Prost checks the shared recursion context for every unknown
                // field, including a scalar inside a deeply nested group.
                if depth >= PROST_RECURSION_LIMIT {
                    return Err(ScanError::Malformed);
                }
                if wire == 3 {
                    depth += 1;
                    stack[depth] = ParseFrame {
                        end: frame.end,
                        state: frame.state,
                        group_tag: tag,
                        is_group: true,
                    };
                } else {
                    read_value(body, &mut cursor, frame.end, wire)?;
                }
            }
        }
        Ok(IndexedMessage {
            body,
            fields: self.fields,
            states: self.states,
            used_fields: self.used_fields,
            used_states: self.used_states,
            root: 0,
            scan_token: next_scan_token()?,
        })
    }

    fn last_record(&self, parent: usize, tag: u32) -> Option<FieldRecord> {
        let record_number = *self
            .states
            .get(parent)?
            .last_record_by_tag
            .get(tag as usize)?;
        let index = record_number.checked_sub(1)?;
        (index < self.used_fields).then(|| self.fields[index])
    }
}

fn read_varint(body: &[u8], cursor: &mut usize, end: usize) -> Result<u64, ScanError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *body
            .get(*cursor)
            .filter(|_| *cursor < end)
            .ok_or(ScanError::Malformed)?;
        *cursor += 1;
        if shift == 63 && byte >= 2 {
            return Err(ScanError::Malformed);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(ScanError::Malformed)
}

fn read_value(
    body: &[u8],
    cursor: &mut usize,
    end: usize,
    wire: u8,
) -> Result<(usize, usize), ScanError> {
    match wire {
        0 => {
            let start = *cursor;
            read_varint(body, cursor, end)?;
            Ok((start, *cursor))
        }
        1 | 5 => {
            let count = if wire == 1 { 8 } else { 4 };
            let start = *cursor;
            *cursor = cursor
                .checked_add(count)
                .filter(|next| *next <= end)
                .ok_or(ScanError::Malformed)?;
            Ok((start, *cursor))
        }
        2 => {
            let length = read_varint(body, cursor, end)?;
            let length = usize::try_from(length).map_err(|_| ScanError::Malformed)?;
            let start = *cursor;
            *cursor = cursor
                .checked_add(length)
                .filter(|next| *next <= end)
                .ok_or(ScanError::Malformed)?;
            Ok((start, *cursor))
        }
        _ => Err(ScanError::Malformed),
    }
}

/// Borrowed effective view. Every message ID carries this successful scan's
/// identity; accessors reject IDs from reused or unrelated arenas.
pub struct IndexedMessage<'view, 'body> {
    body: &'body [u8],
    fields: &'view [FieldRecord],
    states: &'view mut [MessageState],
    used_fields: usize,
    used_states: usize,
    root: usize,
    scan_token: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageId {
    index: usize,
    scan_token: u64,
}

/// A checked interval in the original transmitted Envelope body. It contains
/// no arena identifier, so it remains usable after the outer view is dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BodySpan<'body> {
    body: &'body [u8],
    start: usize,
    end: usize,
}

impl<'body> BodySpan<'body> {
    /// Reborrow from the same original body after releasing the index view.
    pub fn reborrow(self) -> Option<&'body [u8]> {
        self.body.get(self.start..self.end)
    }

    pub fn len(self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }
}

impl<'view, 'body> IndexedMessage<'view, 'body> {
    pub fn body_len(&self) -> usize {
        self.body.len()
    }
    pub fn root(&self) -> MessageId {
        MessageId {
            index: self.root,
            scan_token: self.scan_token,
        }
    }
    pub fn arm(&self) -> Option<u32> {
        (self.states[self.root].kind == Kind::Envelope)
            .then_some(self.states[self.root].oneof_tag)
            .filter(|tag| *tag != 0)
    }
    pub fn has_field(&self, parent: MessageId, tag: u32) -> bool {
        self.record(parent, tag).is_some()
    }
    /// Returns the wire varint. Callers apply the generated field's uint32,
    /// bool, enum or zigzag conversion when constructing selected DTO values.
    pub fn scalar(&self, parent: MessageId, tag: u32) -> Option<u64> {
        let record = self.record(parent, tag)?;
        let kind = self.states.get(parent.index)?.kind;
        if !matches!(spec(kind, tag)?.ty, FieldType::Varint) {
            return None;
        }
        let mut cursor = record.start;
        read_varint(self.body, &mut cursor, record.end).ok()
    }
    pub fn fixed64(&self, parent: MessageId, tag: u32) -> Option<u64> {
        let record = self.record(parent, tag)?;
        let kind = self.states.get(parent.index)?.kind;
        if !matches!(spec(kind, tag)?.ty, FieldType::Fixed64) {
            return None;
        }
        Some(u64::from_le_bytes(
            self.body.get(record.start..record.end)?.try_into().ok()?,
        ))
    }
    pub fn bytes(&self, parent: MessageId, tag: u32) -> Option<&[u8]> {
        let record = self.record(parent, tag)?;
        let kind = self.states.get(parent.index)?.kind;
        if !matches!(spec(kind, tag)?.ty, FieldType::Bytes | FieldType::String) {
            return None;
        }
        self.body.get(record.start..record.end)
    }
    /// Only protobuf bytes fields may be exposed as an opaque payload span.
    pub fn bytes_span(&self, parent: MessageId, tag: u32) -> Option<BodySpan<'body>> {
        let record = self.record(parent, tag)?;
        let kind = self.states.get(parent.index)?.kind;
        if !matches!(spec(kind, tag)?.ty, FieldType::Bytes) {
            return None;
        }
        self.body.get(record.start..record.end)?;
        Some(BodySpan {
            body: self.body,
            start: record.start,
            end: record.end,
        })
    }
    pub fn text(&self, parent: MessageId, tag: u32) -> Option<&str> {
        let record = self.record(parent, tag)?;
        let kind = self.states.get(parent.index)?.kind;
        if !matches!(spec(kind, tag)?.ty, FieldType::String) {
            return None;
        }
        std::str::from_utf8(self.body.get(record.start..record.end)?).ok()
    }
    pub fn message(&self, parent: MessageId, tag: u32) -> Option<MessageId> {
        let record = self.record(parent, tag)?;
        (record.child != NONE).then_some(MessageId {
            index: record.child,
            scan_token: self.scan_token,
        })
    }
    pub fn repeated_messages(
        &self,
        parent: MessageId,
        tag: u32,
    ) -> impl Iterator<Item = MessageId> + '_ {
        let repeated = parent.scan_token == self.scan_token
            && self
                .states
                .get(parent.index)
                .and_then(|state| spec(state.kind, tag))
                .is_some_and(|field| field.repeated);
        self.fields[..self.used_fields]
            .iter()
            .filter(move |record| {
                repeated
                    && record.parent == parent.index
                    && record.tag == tag
                    && record.child != NONE
            })
            .map(|record| MessageId {
                index: record.child,
                scan_token: self.scan_token,
            })
    }
    fn record(&self, parent: MessageId, tag: u32) -> Option<FieldRecord> {
        if parent.scan_token != self.scan_token || parent.index >= self.used_states {
            return None;
        }
        let state = self.states.get(parent.index)?;
        let field = spec(state.kind, tag)?;
        if field.oneof && state.oneof_tag != tag {
            return None;
        }
        self.last_record(parent.index, tag)
    }
    fn last_record(&self, parent: usize, tag: u32) -> Option<FieldRecord> {
        let record_number = *self
            .states
            .get(parent)?
            .last_record_by_tag
            .get(tag as usize)?;
        let index = record_number.checked_sub(1)?;
        (index < self.used_fields).then(|| self.fields[index])
    }
}

/// Internal failure: a changed generated field policy or impossible checked
/// arithmetic must not be confused with a malformed peer field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SizeError {
    UnmodeledField,
    Overflow,
}

/// Source-derived upper bound for the selected normalization traversal.
/// At most F known field records and F+1 states are scanned twice. Each
/// effective field can run at most three ten-iteration varint loops: raw
/// decode, canonical key length, and canonical value/length size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NormalizationWorkBound {
    pub index_visits: usize,
    pub varint_steps: usize,
    pub total_steps: usize,
}

impl NormalizationWorkBound {
    pub fn checked(frame_bytes: NonZeroUsize) -> Result<Self, ScanError> {
        let f = frame_bytes.get();
        let states = f.checked_add(1).ok_or(ScanError::Limit)?;
        let index_visits = f
            .checked_mul(2)
            .and_then(|records| states.checked_mul(2)?.checked_add(records))
            .ok_or(ScanError::Limit)?;
        let varint_steps = f.checked_mul(30).ok_or(ScanError::Limit)?;
        let total_steps = index_visits
            .checked_add(varint_steps)
            .ok_or(ScanError::Limit)?;
        Ok(Self {
            index_visits,
            varint_steps,
            total_steps,
        })
    }
}

#[derive(Clone, Copy)]
enum SizePolicy {
    U64 { optional: bool },
    U32,
    Bool,
    I32,
    Text { optional: bool },
    Bytes,
    Message,
}

// Audited against the checked-in v1.1 generated DTO and common-1.1 descriptor.
// Only descendants of final active Envelope arms are reachable here. In
// particular, opaque tool payloads remain bytes; their fields are not sized.
fn active_size_policy(kind: Kind, tag: u32) -> Option<SizePolicy> {
    use Kind as K;
    use SizePolicy as P;
    Some(match (kind, tag) {
        (K::Envelope, 6..=12) => P::Message,
        (K::Request, 1 | 2 | 6 | 7) => P::U64 { optional: tag != 1 },
        (K::Request, 3 | 4) => P::Message,
        (K::Request, 5) => P::Text { optional: false },
        (K::Request, 8) => P::Bytes,
        (K::CapabilityRef, 1..=3) => P::Text { optional: false },
        (K::CapabilityRef, 4) => P::Message,
        (K::CapabilityRef, 5) => P::Bytes,
        (K::CapabilityRef, 6) => P::Text { optional: true },
        (K::ProfileVersion, 1) => P::Text { optional: false },
        (K::ProfileVersion, 2 | 3) => P::U32,
        (K::EffectKey, 1..=5) => P::Text { optional: false },
        (K::EffectKey, 6 | 7) => P::U64 { optional: tag == 7 },
        (K::Response | K::StreamEnd, 1) => P::U64 { optional: false },
        (K::Response | K::StreamEnd, 2) => P::Bytes,
        (K::Response | K::StreamEnd, 3) => P::Message,
        (K::TypedError, 1) | (K::Cancel, 2) | (K::GoAway, 1) => P::I32,
        (K::TypedError, 2) => P::Text { optional: false },
        (K::StreamItem, 1 | 2) | (K::Cancel, 1) | (K::Credit, 1 | 2) => P::U64 { optional: false },
        (K::StreamItem, 3) => P::Bytes,
        (K::GoAway, 2) => P::Bool,
        _ => return None,
    })
}

fn varint_size(mut value: u64) -> usize {
    let mut size = 1;
    while value >= 128 {
        size += 1;
        value >>= 7;
    }
    size
}

impl IndexedMessage<'_, '_> {
    fn effective_record(&self, state: usize, record_number: usize, record: FieldRecord) -> bool {
        let parent = self.states[state];
        let Some(field) = spec(parent.kind, record.tag) else {
            return false;
        };
        field.repeated
            || (parent.last_record_by_tag[record.tag as usize] == record_number
                && (!field.oneof || parent.oneof_tag == record.tag))
    }

    /// Exact Prost v1.1 encoded size of a successfully scanned active Envelope.
    /// Each selected record is visited twice and each varint is read once (at
    /// most ten bytes); state passes are bounded by the scanner's F+1 slots.
    /// The per-parent sibling chain avoids repeated whole-index enumeration.
    pub(super) fn normalized_active_len(&mut self) -> Result<usize, SizeError> {
        let frame_bytes = NonZeroUsize::new(self.body.len()).ok_or(SizeError::UnmodeledField)?;
        let _work_bound =
            NormalizationWorkBound::checked(frame_bytes).map_err(|_| SizeError::Overflow)?;
        self.states[0].selected = true;
        // State IDs increase when a child is first seen, including children
        // reused by later optional/same-arm occurrences. Mark only effective
        // branches so superseded handshake arms need no size policy.
        for state in 0..self.used_states {
            if !self.states[state].selected {
                continue;
            }
            let mut number = self.states[state].first_record;
            while number != 0 {
                let record = self.fields[number - 1];
                if self.effective_record(state, number, record) && record.child != NONE {
                    self.states[record.child].selected = true;
                }
                number = record.next_sibling;
            }
        }
        // Every child ID exceeds its parent ID. The reverse pass therefore
        // knows the complete merged child size before charging the parent.
        for state in (0..self.used_states).rev() {
            if !self.states[state].selected {
                continue;
            }
            let mut total = 0usize;
            let mut number = self.states[state].first_record;
            while number != 0 {
                let record = self.fields[number - 1];
                if self.effective_record(state, number, record) {
                    let part = self.normalized_field_len(state, record)?;
                    total = total.checked_add(part).ok_or(SizeError::Overflow)?;
                }
                number = record.next_sibling;
            }
            self.states[state].encoded_size = total;
        }
        Ok(self.states[0].encoded_size)
    }

    fn normalized_field_len(&self, state: usize, record: FieldRecord) -> Result<usize, SizeError> {
        let kind = self.states[state].kind;
        let policy = active_size_policy(kind, record.tag).ok_or(SizeError::UnmodeledField)?;
        let field = spec(kind, record.tag).ok_or(SizeError::UnmodeledField)?;
        let key_wire = match field.ty {
            FieldType::Varint => 0u64,
            FieldType::Fixed64 => 1,
            FieldType::String | FieldType::Bytes | FieldType::Message(_) => 2,
        };
        let key = u64::from(record.tag)
            .checked_mul(8)
            .and_then(|key| key.checked_add(key_wire))
            .ok_or(SizeError::Overflow)?;
        let key_size = varint_size(key);
        let payload_size = match (policy, field.ty) {
            (SizePolicy::U64 { optional }, FieldType::Varint) => {
                let value = self.raw_value(record)?;
                if value == 0 && !optional && !field.oneof {
                    return Ok(0);
                }
                varint_size(value)
            }
            (SizePolicy::U32, FieldType::Varint) => {
                let value = self.raw_value(record)? as u32;
                if value == 0 && !field.oneof {
                    return Ok(0);
                }
                varint_size(u64::from(value))
            }
            (SizePolicy::Bool, FieldType::Varint) => {
                let value = self.raw_value(record)? != 0;
                if !value && !field.oneof {
                    return Ok(0);
                }
                1
            }
            (SizePolicy::I32, FieldType::Varint) => {
                let value = self.raw_value(record)? as i32;
                if value == 0 && !field.oneof {
                    return Ok(0);
                }
                varint_size(value as i64 as u64)
            }
            (SizePolicy::Text { optional }, FieldType::String) => {
                let length = record.end - record.start;
                if length == 0 && !optional && !field.oneof {
                    return Ok(0);
                }
                return self.delimited_field_size(key_size, length);
            }
            (SizePolicy::Bytes, FieldType::Bytes) => {
                let length = record.end - record.start;
                if length == 0 && !field.oneof {
                    return Ok(0);
                }
                return self.delimited_field_size(key_size, length);
            }
            (SizePolicy::Message, FieldType::Message(_)) => {
                let child = self
                    .states
                    .get(record.child)
                    .ok_or(SizeError::UnmodeledField)?;
                return self.delimited_field_size(key_size, child.encoded_size);
            }
            _ => return Err(SizeError::UnmodeledField),
        };
        key_size
            .checked_add(payload_size)
            .ok_or(SizeError::Overflow)
    }

    fn raw_value(&self, record: FieldRecord) -> Result<u64, SizeError> {
        let mut cursor = record.start;
        let value = read_varint(self.body, &mut cursor, record.end)
            .map_err(|_| SizeError::UnmodeledField)?;
        (cursor == record.end)
            .then_some(value)
            .ok_or(SizeError::UnmodeledField)
    }

    fn delimited_field_size(&self, key_size: usize, length: usize) -> Result<usize, SizeError> {
        let length64 = u64::try_from(length).map_err(|_| SizeError::Overflow)?;
        key_size
            .checked_add(varint_size(length64))
            .and_then(|size| size.checked_add(length))
            .ok_or(SizeError::Overflow)
    }
}
