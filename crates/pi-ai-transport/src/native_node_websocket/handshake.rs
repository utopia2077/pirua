use super::super::{SocketIo, io_error, latin1_header_bytes, open_socket, unbracket};
use super::{NodeWebSocketLimits, fail};
use crate::{HttpHeaders, ProxyUrl, TransportError, TransportErrorKind as Kind};
use std::{collections::BTreeSet, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{
    TlsConnector,
    rustls::{self, pki_types::ServerName},
};
use tokio_tungstenite::tungstenite::handshake::{client::generate_key, derive_accept_key};
use url::Url;

pub(super) async fn connect(
    endpoint: String,
    mut headers: HttpHeaders,
    proxy: Option<ProxyUrl>,
    tls: Arc<rustls::ClientConfig>,
    limits: NodeWebSocketLimits,
) -> Result<(Box<dyn SocketIo>, Vec<u8>, Option<String>), TransportError> {
    let mut url = Url::parse(&endpoint).map_err(|_| fail(Kind::InvalidRequest))?;
    match url.scheme() {
        "http" => url
            .set_scheme("ws")
            .map_err(|_| fail(Kind::InvalidRequest))?,
        "https" => url
            .set_scheme("wss")
            .map_err(|_| fail(Kind::InvalidRequest))?,
        "ws" | "wss" => {}
        _ => return Err(fail(Kind::InvalidRequest)),
    }
    if url.fragment().is_some() {
        return Err(fail(Kind::InvalidRequest));
    }
    let host = url.host_str().ok_or_else(|| fail(Kind::InvalidRequest))?;
    let authority = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    let key = generate_key();
    // Fetch deletes a user-supplied Host before native serialization.
    headers.set("host", authority.as_str())?;
    if headers.get_for_wire("range").is_some() {
        headers.append("accept-encoding", "identity")?;
    }
    headers.append("sec-websocket-key", key.clone())?;
    headers.append("sec-websocket-version", "13")?;
    headers.append(
        "sec-websocket-extensions",
        "permessage-deflate; client_max_window_bits",
    )?;
    for (name, value) in [
        ("accept", "*/*"),
        ("accept-language", "*"),
        ("user-agent", "node"),
        ("pragma", "no-cache"),
        ("cache-control", "no-cache"),
        (
            "accept-encoding",
            if url.scheme() == "wss" {
                "br, gzip, deflate, zstd"
            } else {
                "gzip, deflate"
            },
        ),
    ] {
        if headers.get_for_fetch(name).is_none() {
            headers.set(name, value)?;
        }
    }
    headers.set("connection", "upgrade")?;
    headers.set("upgrade", "websocket")?;
    headers.set("sec-fetch-mode", "websocket")?;
    let protocols = headers.get_for_fetch("sec-websocket-protocol");
    let path = if let Some(query) = url.query() {
        format!("{}?{query}", url.path())
    } else {
        url.path().to_owned()
    };
    let mut bytes = format!("GET {path} HTTP/1.1\r\n").into_bytes();
    let mut emitted = BTreeSet::new();
    for (name, _) in headers.iter_for_wire() {
        if !emitted.insert(name) {
            continue;
        }
        let value = headers
            .get_for_fetch(name)
            .ok_or_else(|| fail(Kind::InvalidRequest))?;
        let value = latin1_header_bytes(&value)?;
        let added = name
            .len()
            .checked_add(value.len())
            .and_then(|v| v.checked_add(4))
            .ok_or_else(|| fail(Kind::LimitExceeded))?;
        if added > limits.max_handshake_bytes.saturating_sub(bytes.len()) {
            return Err(fail(Kind::LimitExceeded));
        }
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend(value);
        bytes.extend_from_slice(b"\r\n");
    }
    if bytes.len() > limits.max_handshake_bytes.saturating_sub(2) {
        return Err(fail(Kind::LimitExceeded));
    }
    bytes.extend_from_slice(b"\r\n");
    let socket = open_socket(&url, proxy, tls.clone()).await?;
    let mut socket: Box<dyn SocketIo> = if url.scheme() == "wss" {
        let server = ServerName::try_from(unbracket(host).to_owned())
            .map_err(|_| fail(Kind::InvalidRequest))?;
        Box::new(
            TlsConnector::from(tls)
                .connect(server, socket)
                .await
                .map_err(io_error)?,
        )
    } else {
        socket
    };
    socket.write_all(&bytes).await.map_err(io_error)?;
    socket.flush().await.map_err(io_error)?;
    let mut head = Vec::new();
    let mut consumed = 0usize;
    let mut buffer = vec![0; limits.read_buffer_bytes];
    loop {
        let count = socket.read(&mut buffer).await.map_err(io_error)?;
        if count == 0 {
            return Err(fail(Kind::Connection));
        }
        head.extend_from_slice(&buffer[..count]);
        while let Some(end) = head
            .windows(4)
            .position(|v| v == b"\r\n\r\n")
            .map(|n| n + 4)
        {
            consumed = consumed
                .checked_add(end)
                .filter(|n| *n <= limits.max_handshake_bytes)
                .ok_or_else(|| fail(Kind::LimitExceeded))?;
            let (interim, extension) = validate(&head[..end], &key, protocols.as_deref())?;
            if interim {
                head.drain(..end);
                continue;
            }
            let tail = head.split_off(end);
            return Ok((socket, tail, extension));
        }
        if head.len() >= limits.max_handshake_bytes.saturating_sub(consumed) {
            return Err(fail(Kind::LimitExceeded));
        }
    }
}
fn validate(
    bytes: &[u8],
    key: &str,
    protocols: Option<&str>,
) -> Result<(bool, Option<String>), TransportError> {
    // Latin-1 is the actual native field projection, not UTF-8 lossy text.
    let text: String = bytes.iter().map(|b| char::from(*b)).collect();
    let mut lines = text.split("\r\n");
    let mut status = lines.next().unwrap_or_default().split(' ');
    if !matches!(status.next(), Some("HTTP/1.1" | "HTTP/1.0")) {
        return Err(fail(Kind::Protocol));
    }
    let code = status.next().unwrap_or_default();
    if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_digit()) || !code.starts_with('1') {
        return Err(fail(Kind::Protocol));
    }
    let mut fields: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if line
            .chars()
            .any(|c| u32::from(c) < 32 && c != '\t' || c == '\u{7f}')
        {
            return Err(fail(Kind::Protocol));
        }
        if line.starts_with([' ', '\t']) {
            let Some((_, value)) = fields.last_mut() else {
                return Err(fail(Kind::Protocol));
            };
            value.push_str(line);
            continue;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| fail(Kind::Protocol))?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
        {
            return Err(fail(Kind::Protocol));
        }
        let value = value.trim_start_matches([' ', '\t']);
        if value
            .chars()
            .any(|c| u32::from(c) < 32 && c != '\t' || c == '\u{7f}')
        {
            return Err(fail(Kind::Protocol));
        }
        fields.push((name.to_ascii_lowercase(), value.to_owned()));
    }
    if code != "101" {
        return Ok((true, None));
    }
    let get = |name: &str| {
        let values: Vec<&str> = fields
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, v)| v.as_str())
            .collect();
        (!values.is_empty()).then(|| values.join(", "))
    };
    if !get("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        || !get("connection").is_some_and(|v| v.eq_ignore_ascii_case("upgrade"))
        || get("sec-websocket-accept").as_deref()
            != Some(derive_accept_key(key.as_bytes()).as_str())
        || get("sec-websocket-protocol").is_some_and(|value| {
            !protocols.is_some_and(|offered| split_protocols(offered).contains(&value.as_str()))
        })
    {
        return Err(fail(Kind::Protocol));
    }
    Ok((false, get("sec-websocket-extensions")))
}

fn split_protocols(value: &str) -> Vec<&str> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' {
            quoted = !quoted;
        }
        if byte == b',' && !quoted {
            values.push(value[start..index].trim_matches([' ', '\t']));
            start = index + 1;
        }
    }
    if start < value.len() {
        values.push(value[start..].trim_matches([' ', '\t']));
    }
    values
}
