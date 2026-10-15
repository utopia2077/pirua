use crate::types::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufRead, Read, Write},
};

#[derive(Serialize, Deserialize)]
pub(crate) struct Start {
    pub code: String,
    pub tools: Vec<DeclaredTool>,
    pub globals: Vec<DeclaredGlobal>,
    pub store: BTreeMap<String, String>,
    pub started_at: f64,
    pub seed: u32,
    pub controlled: bool,
    pub memory: usize,
    pub stack: usize,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Delivery {
    pub seq: u64,
    pub now: f64,
    pub id: u64,
    pub outcome: Outcome,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum Message {
    Started {
        fingerprint: String,
    },
    Call {
        call: HostCall,
    },
    Output {
        item: OutputItem,
    },
    Done {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_value"
        )]
        value: Option<Value>,
        writes: StoreWrites,
        during: Option<During>,
    },
    Failed {
        error: ExecutionError,
        during: Option<During>,
    },
    Idle {
        seq: u64,
        issued: u64,
    },
}
pub(crate) fn write_json(writer: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
pub(crate) fn read_json<T: for<'a> Deserialize<'a>>(
    reader: &mut impl BufRead,
) -> io::Result<Option<T>> {
    let mut bytes = Vec::new();
    let read = (&mut *reader)
        .take((MAX_WIRE_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if read == 0 {
        return Ok(None);
    }
    if bytes.len() > MAX_WIRE_BYTES || bytes.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CodeMode worker frame exceeds limit or is incomplete",
        ));
    }
    crate::json::decode(&bytes, MAX_JSON_DEPTH + 8)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
