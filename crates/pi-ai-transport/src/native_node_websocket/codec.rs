use super::{NodeWebSocketLimits, fail};
use crate::{TransportError, TransportErrorKind as Kind, WebSocketMessage};
use std::io::Cursor;
use tokio_tungstenite::tungstenite::protocol::frame::FrameHeader;
use zlib_rs::{Inflate, InflateFlush, Status};

pub(super) enum Input {
    Progress,
    Message(WebSocketMessage),
    Ping(Vec<u8>),
    Pong,
    Close {
        code: u16,
        reason: String,
        reply: Vec<u8>,
    },
}
pub(super) struct Codec {
    pending: Vec<u8>,
    position: usize,
    message: Vec<u8>,
    fragments: usize,
    compressed: bool,
    binary: bool,
    last_fragmented: bool,
    extension: bool,
    window: Option<u8>,
    inflater: Option<Inflate>,
    in_flight: Option<Inflating>,
    stream_end: bool,
    max_frame: usize,
    max_message: usize,
    limits: NodeWebSocketLimits,
}
struct Inflating {
    input: Vec<u8>,
    offset: usize,
    fin: bool,
}
impl Codec {
    pub fn new(
        extension: Option<&str>,
        max_frame: usize,
        max_message: usize,
        limits: NodeWebSocketLimits,
    ) -> Result<Self, TransportError> {
        // Node's parser splits on semicolons and the first '='. It is not a
        // general RFC extension parser; duplicate exact parameters keep last.
        let mut values: Vec<(&str, &str)> = Vec::new();
        if let Some(extension) = extension {
            for field in extension.split(';') {
                let mut parts = field.split('=');
                let name = parts
                    .next()
                    .unwrap_or_default()
                    .trim_start_matches([' ', '\t']);
                let value = parts
                    .next()
                    .unwrap_or_default()
                    .trim_end_matches([' ', '\t']);
                if let Some((_, old)) = values.iter_mut().find(|(key, _)| *key == name) {
                    *old = value;
                } else {
                    values.push((name, value));
                }
            }
            if !values.iter().any(|(name, _)| *name == "permessage-deflate") {
                return Err(fail(Kind::Protocol));
            }
        }
        let window = values
            .iter()
            .find(|(k, _)| *k == "server_max_window_bits")
            .map(|(_, v)| *v)
            .filter(|v| !v.is_empty())
            .map_or(Some(15), |v| {
                if !v.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let trimmed = v.trim_start_matches('0');
                trimmed.parse::<u8>().ok().filter(|n| (8..=15).contains(n))
            });
        Ok(Self {
            pending: Vec::new(),
            position: 0,
            message: Vec::new(),
            fragments: 0,
            compressed: false,
            binary: false,
            last_fragmented: false,
            extension: extension.is_some(),
            window,
            inflater: None,
            in_flight: None,
            stream_end: false,
            max_frame,
            max_message,
            limits,
        })
    }
    pub fn in_flight(&self) -> bool {
        self.in_flight.is_some()
    }
    pub fn read_capacity(&self) -> usize {
        self.max_frame
            .saturating_add(self.limits.read_buffer_bytes)
            .saturating_add(14)
            .saturating_sub(self.pending.len() - self.position)
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
        if self.position != 0 {
            self.pending.drain(..self.position);
            self.position = 0;
        }
        let limit = self
            .max_frame
            .checked_add(self.limits.read_buffer_bytes)
            .and_then(|v| v.checked_add(14))
            .ok_or_else(|| fail(Kind::LimitExceeded))?;
        if bytes.len() > limit.saturating_sub(self.pending.len()) {
            return Err(fail(Kind::LimitExceeded));
        }
        self.pending.extend_from_slice(bytes);
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Input>, TransportError> {
        if self.in_flight.is_some() {
            return self.inflate_step().map(Some);
        }
        let available = &self.pending[self.position..];
        if available.len() < 2 {
            return Ok(None);
        }
        // Node ByteParser validates INFO from exactly two bytes, before it
        // awaits any extended length (or mask). Preserve the raw length marker:
        // even a nonminimal control length of one is invalid when marked 126/127.
        let opcode = available[0] & 15;
        let fin = available[0] & 128 != 0;
        let rsv1 = available[0] & 64 != 0;
        let fragmented = !fin && opcode != 0;
        if !matches!(opcode, 0 | 1 | 2 | 8 | 9 | 10)
            || available[1] & 128 != 0
            || available[0] & 48 != 0
            || (rsv1 && !self.extension)
            || (fragmented && opcode != 1 && opcode != 2)
            || ((opcode == 1 || opcode == 2) && self.fragments != 0)
            || (self.last_fragmented && fragmented)
            || (opcode >= 8 && (available[1] & 127 > 125 || fragmented))
            || (opcode == 0 && self.fragments == 0 && !self.compressed)
        {
            return Err(fail(Kind::Protocol));
        }
        let mut cursor = Cursor::new(available);
        let Some((header, length)) =
            FrameHeader::parse(&mut cursor).map_err(|_| fail(Kind::Protocol))?
        else {
            return Ok(None);
        };
        if length > 2_147_483_647 || length > self.max_frame as u64 {
            return Err(fail(Kind::LimitExceeded));
        }
        let length = length as usize;
        let start = cursor.position() as usize;
        if available.len() - start < length {
            return Ok(None);
        }
        let body = available[start..start + length].to_vec();
        self.position += start + length;
        self.last_fragmented = fragmented;
        if opcode == 1 || opcode == 2 {
            self.binary = opcode == 2;
            self.compressed = header.rsv1;
        }
        match opcode {
            8 => {
                if body.len() == 1 {
                    return Err(fail(Kind::Protocol));
                }
                let code = if body.len() >= 2 {
                    u16::from_be_bytes([body[0], body[1]])
                } else {
                    1005
                };
                if body.len() >= 2
                    && !((1000..=1014).contains(&code) && ![1004, 1005, 1006].contains(&code)
                        || (3000..=4999).contains(&code))
                {
                    return Err(fail(Kind::Protocol));
                }
                let bytes = body.get(2..).unwrap_or_default();
                // Node removes one BOM explicitly, then its ICU fatal decoder
                // removes a further leading BOM. Text messages remove just one.
                let bytes = bytes.strip_prefix(&[239, 187, 191]).unwrap_or(bytes);
                let reason = decode_text(bytes)?;
                return Ok(Some(Input::Close {
                    code,
                    reason,
                    reply: if body.len() >= 2 {
                        body[..2].to_vec()
                    } else {
                        Vec::new()
                    },
                }));
            }
            9 => return Ok(Some(Input::Ping(body))),
            10 => return Ok(Some(Input::Pong)),
            _ => {}
        }
        if self.fragments >= self.limits.max_fragments {
            return Err(fail(Kind::LimitExceeded));
        }
        if self.compressed {
            if self.inflater.is_none() {
                self.inflater = Some(Inflate::new(
                    false,
                    self.window.ok_or_else(|| fail(Kind::Protocol))?,
                ));
            }
            let mut input = body;
            if header.is_final {
                input.extend_from_slice(&[0, 0, 255, 255]);
            }
            self.in_flight = Some(Inflating {
                input,
                offset: 0,
                fin: header.is_final,
            });
            return Ok(Some(Input::Progress));
        } else {
            if body.len() > self.max_message.saturating_sub(self.message.len()) {
                return Err(fail(Kind::LimitExceeded));
            }
            self.message.extend_from_slice(&body);
        }
        self.complete_fragment(header.is_final).map(Some)
    }
    fn complete_fragment(&mut self, fin: bool) -> Result<Input, TransportError> {
        self.fragments += 1;
        if !fin {
            return Ok(Input::Progress);
        }
        self.fragments = 0;
        let bytes = std::mem::take(&mut self.message);
        let message = if self.binary {
            WebSocketMessage::Binary(bytes)
        } else {
            WebSocketMessage::Text(decode_text(&bytes)?)
        };
        Ok(Input::Message(message))
    }
    fn inflate_step(&mut self) -> Result<Input, TransportError> {
        let work = self
            .in_flight
            .as_mut()
            .ok_or_else(|| fail(Kind::Protocol))?;
        let inflater = self.inflater.as_mut().ok_or_else(|| fail(Kind::Protocol))?;
        let complete = if self.stream_end {
            true
        } else {
            let mut output = [0u8; 8192];
            let room = (self.max_message - self.message.len())
                .saturating_add(1)
                .min(output.len());
            let before_in = inflater.total_in();
            let before_out = inflater.total_out();
            let end = work.offset.saturating_add(8192).min(work.input.len());
            let status = inflater
                .decompress(
                    &work.input[work.offset..end],
                    &mut output[..room],
                    InflateFlush::SyncFlush,
                )
                .map_err(|_| fail(Kind::Protocol))?;
            let consumed = (inflater.total_in() - before_in) as usize;
            let produced = (inflater.total_out() - before_out) as usize;
            if produced > self.max_message - self.message.len() {
                return Err(fail(Kind::LimitExceeded));
            }
            self.message.extend_from_slice(&output[..produced]);
            work.offset += consumed;
            self.stream_end = status == Status::StreamEnd;
            self.stream_end
                || work.offset == work.input.len() && produced < room
                || consumed == 0 && produced == 0
        };
        if complete {
            let fin = self
                .in_flight
                .take()
                .ok_or_else(|| fail(Kind::Protocol))?
                .fin;
            self.complete_fragment(fin)
        } else {
            Ok(Input::Progress)
        }
    }
}
fn decode_text(bytes: &[u8]) -> Result<String, TransportError> {
    let text = std::str::from_utf8(bytes).map_err(|_| fail(Kind::Protocol))?;
    Ok(text.strip_prefix('\u{feff}').unwrap_or(text).to_owned())
}

pub(super) fn encode(opcode: u8, body: &[u8]) -> Result<Vec<u8>, TransportError> {
    let mut mask = [0; 4];
    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()
        .secure_random
        .fill(&mut mask)
        .map_err(|_| fail(Kind::Connection))?;
    let mut result = Vec::with_capacity(
        body.len()
            .checked_add(14)
            .ok_or_else(|| fail(Kind::LimitExceeded))?,
    );
    result.push(0x80 | opcode);
    if body.len() < 126 {
        result.push(0x80 | body.len() as u8);
    } else if body.len() <= 65535 {
        result.push(0x80 | 126);
        result.extend_from_slice(&(body.len() as u16).to_be_bytes());
    } else {
        result.push(0x80 | 127);
        result.extend_from_slice(&(body.len() as u64).to_be_bytes());
    }
    result.extend_from_slice(&mask);
    result.extend(body.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};
    fn fixture() -> Value {
        serde_json::from_str(include_str!(
            "../../../../conformance/ai-transport/node-websocket/trace.json"
        ))
        .unwrap()
    }
    #[test]
    fn every_wire_split_matches_executed_node_messages_and_protocol_errors() {
        let fixture = fixture();
        let mut splits = 0;
        let info: Value = serde_json::from_str(include_str!(
            "../../../../conformance/ai-transport/node-websocket/info-stage.json"
        ))
        .unwrap();
        for case in fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .chain(info["cases"].as_array().unwrap())
        {
            if matches!(case["id"].as_str(), Some("partial-eof" | "plain-eof")) {
                continue;
            }
            let bytes: Vec<u8> = case["frames"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|v| STANDARD.decode(v.as_str().unwrap()).unwrap())
                .collect();
            for split in 0..=bytes.len() {
                splits += 1;
                let mut observed = vec![];
                let mut done = false;
                match Codec::new(
                    case["extension"].as_str(),
                    65536,
                    65536,
                    NodeWebSocketLimits::default(),
                ) {
                    Err(_) => observed.push(json!({"type":"error"})),
                    Ok(mut codec) => {
                        for part in [&bytes[..split], &bytes[split..]] {
                            if done {
                                break;
                            }
                            codec.push(part).unwrap();
                            loop {
                                match codec.next() {
                                    Ok(Some(Input::Progress | Input::Ping(_) | Input::Pong)) => {}
                                    Ok(Some(Input::Message(WebSocketMessage::Text(text)))) => {
                                        observed.push(json!({"type":"message","text":text}))
                                    }
                                    Ok(Some(Input::Message(WebSocketMessage::Binary(binary)))) => {
                                        observed.push(json!({"type":"message","binary":binary}))
                                    }
                                    Ok(Some(Input::Close { code, reason, .. })) => {
                                        observed.push(
                                            json!({"type":"close","code":code,"reason":reason}),
                                        );
                                        done = true;
                                        break;
                                    }
                                    Ok(None) => break,
                                    Err(_) => {
                                        observed.push(json!({"type":"error"}));
                                        done = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                assert_eq!(
                    json!(observed),
                    case["expected"],
                    "{} split {split}",
                    case["id"]
                );
            }
        }
        assert!(splits > 500);
        eprintln!("verified {splits} complete codec split observations");
    }
    #[test]
    fn compressed_decoded_frame_and_fragment_budgets_are_independent() {
        let mut frame = Codec::new(None, 4, 100, NodeWebSocketLimits::default()).unwrap();
        frame.push(&[129, 5]).unwrap();
        assert_eq!(frame.next().err().unwrap().kind(), Kind::LimitExceeded);
        let fixture = fixture();
        let compressed = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == "compressed")
            .unwrap();
        let bytes = STANDARD
            .decode(compressed["frames"][0].as_str().unwrap())
            .unwrap();
        for cap in [9, 10] {
            let mut codec = Codec::new(
                Some("permessage-deflate"),
                100,
                cap,
                NodeWebSocketLimits::default(),
            )
            .unwrap();
            codec.push(&bytes).unwrap();
            assert!(matches!(codec.next().unwrap(), Some(Input::Progress)));
            let result = codec.next();
            if cap == 9 {
                assert_eq!(result.err().unwrap().kind(), Kind::LimitExceeded);
            } else {
                assert!(
                    matches!(result.unwrap(),Some(Input::Message(WebSocketMessage::Text(s)))if s=="compressed")
                );
            }
        }
        let mut fragmented = Codec::new(
            None,
            100,
            100,
            NodeWebSocketLimits {
                max_fragments: 1,
                ..Default::default()
            },
        )
        .unwrap();
        fragmented.push(&[1, 0, 128, 0]).unwrap();
        assert!(matches!(fragmented.next().unwrap(), Some(Input::Progress)));
        assert_eq!(fragmented.next().err().unwrap().kind(), Kind::LimitExceeded);
    }
}
