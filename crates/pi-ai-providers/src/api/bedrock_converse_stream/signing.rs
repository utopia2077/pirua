//! SigV4 signs the exact SDK header record before Node physical projection.
use super::{BedrockCredentials, build::whitespace};
use pi_ai_core::{ModelsError, ModelsErrorCode};
use ring::{digest, hmac};
use std::collections::BTreeMap;
fn invalid() -> ModelsError {
    ModelsError::new(ModelsErrorCode::Auth)
}
pub(super) fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(H[usize::from(b >> 4)] as char);
        out.push(H[usize::from(b & 15)] as char);
    }
    out
}
fn sha(bytes: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, bytes).as_ref())
}
fn mac(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), bytes)
        .as_ref()
        .to_vec()
}
pub(super) fn escape(value: &str) -> String {
    let mut out = String::new();
    const H: &[u8; 16] = b"0123456789ABCDEF";
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(H[usize::from(b >> 4)] as char);
            out.push(H[usize::from(b & 15)] as char);
        }
    }
    out
}
fn path(value: &str) -> String {
    let mut parts = Vec::new();
    for p in value.split('/') {
        match p {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            p => parts.push(p),
        }
    }
    let normalized = format!(
        "{}{}{}",
        if value.starts_with('/') { "/" } else { "" },
        parts.join("/"),
        if !parts.is_empty() && value.ends_with('/') {
            "/"
        } else {
            ""
        }
    );
    escape(&normalized).replace("%2F", "/")
}
pub(super) fn set(headers: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some((_, old)) = headers.iter_mut().find(|(k, _)| k == name) {
        *old = value;
    } else {
        headers.push((name.into(), value));
    }
}
pub(super) fn date(ms: f64) -> Result<(String, String), ModelsError> {
    if !ms.is_finite() || ms.abs() > 8.64e15 {
        return Err(invalid());
    }
    let seconds = (ms.trunc() as i64).div_euclid(1000);
    let days = seconds.div_euclid(86400);
    let clock = seconds.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let y = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else if year >= 10000 {
        format!("+{year:06}")
    } else {
        format!("{:06}", -year)
    };
    let long = format!(
        "{y}{month:02}{day:02}T{:02}{:02}{:02}Z",
        clock / 3600,
        clock / 60 % 60,
        clock % 60
    );
    let short = long[..8].to_owned();
    Ok((long, short))
}
pub(super) struct SignInput<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a [(String, Vec<String>)],
    pub headers: &'a [(String, String)],
    pub body: &'a [u8],
    pub region: &'a str,
    pub date_ms: f64,
}
pub(super) fn sign(
    input: SignInput<'_>,
    credentials: &BedrockCredentials,
) -> Result<Vec<(String, String)>, ModelsError> {
    let (long, short) = date(input.date_ms)?;
    let mut headers = input
        .headers
        .iter()
        .filter(|(k, _)| {
            !matches!(
                k.to_ascii_lowercase().as_str(),
                "authorization" | "x-amz-date" | "date"
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    set(&mut headers, "x-amz-date", long.clone());
    if let Some(token) = credentials
        .session_token
        .as_ref()
        .filter(|t| !t.expose_secret().is_empty())
    {
        set(
            &mut headers,
            "x-amz-security-token",
            token.expose_secret().to_owned(),
        );
    }
    let payload_hash = if let Some((_, v)) = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-content-sha256"))
    {
        v.clone()
    } else {
        let hash = sha(input.body);
        set(&mut headers, "x-amz-content-sha256", hash.clone());
        hash
    };
    let mut sorted = headers.iter().collect::<Vec<_>>();
    sorted.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
    let mut canonical = BTreeMap::new();
    for (key, value) in sorted {
        let key = key.to_ascii_lowercase();
        if matches!(
            key.as_str(),
            "authorization"
                | "cache-control"
                | "connection"
                | "expect"
                | "from"
                | "keep-alive"
                | "max-forwards"
                | "pragma"
                | "referer"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "user-agent"
                | "x-amzn-trace-id"
        ) || key.starts_with("proxy-")
            || key.starts_with("sec-")
        {
            continue;
        }
        let value = value
            .trim_matches(whitespace)
            .split(whitespace)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        canonical.insert(key, value);
    }
    let mut query = Vec::new();
    for (k, values) in input.query {
        if k.eq_ignore_ascii_case("x-amz-signature") {
            continue;
        }
        let key = escape(k);
        let mut values = values
            .iter()
            .map(|v| format!("{key}={}", escape(v)))
            .collect::<Vec<_>>();
        values.sort();
        query.push((key, values.join("&")));
    }
    query.sort_by(|a, b| a.0.cmp(&b.0));
    let query = query
        .into_iter()
        .map(|(_, v)| v)
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>()
        .join("&");
    let list = canonical.keys().cloned().collect::<Vec<_>>().join(";");
    let lines = canonical
        .iter()
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>()
        .join("\n");
    let request = format!(
        "{}\n{}\n{query}\n{lines}\n\n{list}\n{payload_hash}",
        input.method,
        path(input.path)
    );
    let scope = format!("{short}/{}/bedrock/aws4_request", input.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{long}\n{scope}\n{}",
        sha(request.as_bytes())
    );
    let mut key = format!("AWS4{}", credentials.secret_access_key.expose_secret()).into_bytes();
    for data in [short.as_str(), input.region, "bedrock", "aws4_request"] {
        key = mac(&key, data.as_bytes());
    }
    let signature = hex(&mac(&key, to_sign.as_bytes()));
    set(
        &mut headers,
        "authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={list}, Signature={signature}",
            credentials.access_key_id.expose_secret()
        ),
    );
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_ai_core::SecretString;
    use serde_json::Value;
    #[test]
    fn exact_fixed_date_sdk_signatures_bind_headers_query_path_body_and_session() {
        let source:Value=serde_json::from_str(include_str!("../../../../../conformance/provider-traces/bedrock-converse-stream/signing-vectors.json")).unwrap();
        assert_eq!(source["cases"].as_array().unwrap().len(), 8);
        for row in source["cases"].as_array().unwrap() {
            let v = &row["credentials"];
            let credentials = BedrockCredentials {
                access_key_id: SecretString::new(v["accessKeyId"].as_str().unwrap()),
                secret_access_key: SecretString::new(v["secretAccessKey"].as_str().unwrap()),
                session_token: v["sessionToken"].as_str().map(SecretString::new),
                expiration_unix_ms: None,
            };
            let input = &row["input"];
            let headers = input["headers"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                .collect::<Vec<_>>();
            let query = input["query"]
                .as_object()
                .map(|map| {
                    map.iter()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                if let Some(s) = v.as_str() {
                                    vec![s.to_owned()]
                                } else {
                                    v.as_array()
                                        .unwrap()
                                        .iter()
                                        .map(|s| s.as_str().unwrap().to_owned())
                                        .collect()
                                },
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let date_ms = if row["id"] == "clock-corrected" {
                1767326645000.0
            } else {
                1767323045000.0
            };
            let signed = sign(
                SignInput {
                    method: input["method"].as_str().unwrap(),
                    path: input["path"].as_str().unwrap(),
                    query: &query,
                    headers: &headers,
                    body: input["body"].as_str().unwrap().as_bytes(),
                    region: row["region"].as_str().unwrap(),
                    date_ms,
                },
                &credentials,
            )
            .unwrap();
            let expected = row["signed"]["headers"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                .collect::<Vec<_>>();
            assert_eq!(signed, expected, "{}", row["id"]);
        }
    }
}
