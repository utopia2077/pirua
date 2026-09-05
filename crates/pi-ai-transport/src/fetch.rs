//! Node Fetch's replayable byte-request redirect subset. URL, header and body
//! values stay transient. This does not implement browser CORS, streaming uploads,
//! cache, decompression, or response text decoding.
use crate::policy::{Timer, timed_wait};
use crate::{
    HttpHeaders, HttpRequest, HttpResponse, RawHttpClient, TransportError, TransportErrorKind,
};
use pi_ai_core::{RequestEndpoint, RequestRuntime};
use url::Url;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FetchRedirectMode {
    #[default]
    Follow,
    Manual,
}

fn invalid() -> TransportError {
    TransportError::new(TransportErrorKind::InvalidRequest)
}

fn check_url(url: &Url) -> Result<(), TransportError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid());
    }
    // A parsed HTTP URL reaches Fetch: its Request constructor rejects URL
    // credentials, and Fetch blocks unsafe ports before dispatch. Both failures
    // cross the SDK fetch-catch boundary and become APIConnectionError. Unlike
    // URL syntax/buildURL or our canonical-domain preflight, they may retry even
    // though no raw client/socket is invoked.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(TransportError::new(TransportErrorKind::Connection));
    }
    // Fetch's bad-port list (Undici constants.js), not a general egress policy.
    if url.port().is_some_and(|port| {
        [
            1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53, 69, 77, 79, 87, 95,
            101, 102, 103, 104, 109, 110, 111, 113, 115, 117, 119, 123, 135, 137, 139, 143, 161,
            179, 389, 427, 465, 512, 513, 514, 515, 526, 530, 531, 532, 540, 548, 554, 556, 563,
            587, 601, 636, 989, 990, 993, 995, 1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060,
            5061, 6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679, 6697, 10080,
        ]
        .contains(&port)
    }) {
        return Err(TransportError::new(TransportErrorKind::Connection));
    }
    Ok(())
}

fn prepare(original: &HttpRequest) -> Result<(HttpRequest, Url), TransportError> {
    let url = Url::parse(original.endpoint.expose_for_transport()).map_err(|_| invalid())?;
    check_url(&url)?;
    let mut method = original.method.clone();
    if method.is_empty()
        || !method
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
    {
        return Err(invalid());
    }
    let upper = method.to_ascii_uppercase();
    if matches!(upper.as_str(), "CONNECT" | "TRACE" | "TRACK") {
        return Err(invalid());
    }
    if matches!(
        upper.as_str(),
        "DELETE" | "GET" | "HEAD" | "OPTIONS" | "POST" | "PUT" | "QUERY"
    ) {
        method = upper;
    }
    if matches!(method.as_str(), "GET" | "HEAD") && !original.body.is_empty() {
        return Err(invalid());
    }
    let mut headers = HttpHeaders::new();
    for (name, value) in original.headers.iter_for_wire() {
        if value.chars().any(|c| c as u32 > 255) {
            return Err(invalid());
        }
        if headers.get_for_wire(name).is_none() {
            headers.append(
                name,
                original
                    .headers
                    .get_for_fetch(name)
                    .expect("existing field"),
            )?;
        }
    }
    // httpNetworkOrCacheFetch deletes a caller Host before every network send.
    headers.remove("host");
    Ok((
        HttpRequest {
            endpoint: RequestEndpoint::new_resolved(url.as_str()).map_err(|_| invalid())?,
            method,
            headers,
            body: original.body.clone(),
            proxy: original.proxy.clone(),
        },
        url,
    ))
}

pub(crate) async fn attempt(
    client: &dyn RawHttpClient,
    original: &HttpRequest,
    runtime: &RequestRuntime,
    deadline: &mut Option<Timer>,
    mode: FetchRedirectMode,
) -> Result<HttpResponse, TransportError> {
    let (mut request, mut current) = prepare(original)?;
    let mut redirects = 0;
    loop {
        let mut response = timed_wait(
            client.send(&request, runtime),
            &runtime.cancellation,
            deadline,
        )
        .await?;
        if mode == FetchRedirectMode::Manual
            || !matches!(response.status, 301 | 302 | 303 | 307 | 308)
        {
            return Ok(response);
        }
        let Some(location) = response.headers.get_for_fetch("location") else {
            return Ok(response);
        };
        response.body.close();
        // SDK fetch-catch boundaries wrap Fetch network errors as connection
        // errors. Do not broaden retry eligibility of arbitrary raw port errors.
        let network_error = || TransportError::new(TransportErrorKind::Connection);
        if redirects == 20 {
            return Err(network_error());
        }
        // responseLocationURL treats the header as a Latin1 byte string, then
        // replacement-decodes non-ASCII/control bytes. Buffer.toString keeps BOM.
        let location = if location.chars().any(|c| !(32..=126).contains(&(c as u32))) {
            let bytes: Result<Vec<_>, _> =
                location.chars().map(|c| u8::try_from(c as u32)).collect();
            String::from_utf8_lossy(&bytes.map_err(|_| network_error())?).into_owned()
        } else {
            location
        };
        let mut target = current.join(&location).map_err(|_| network_error())?;
        check_url(&target).map_err(|_| network_error())?;
        if target.fragment().is_none_or(str::is_empty) {
            target.set_fragment(current.fragment());
        }
        if matches!(response.status, 301 | 302) && request.method == "POST"
            || response.status == 303 && !matches!(request.method.as_str(), "GET" | "HEAD")
        {
            request.method = "GET".into();
            request.body.clear();
            for name in [
                "content-encoding",
                "content-language",
                "content-location",
                "content-type",
                "content-length",
            ] {
                request.headers.remove(name);
            }
        }
        if current.origin() != target.origin() {
            for name in ["authorization", "proxy-authorization", "cookie", "host"] {
                request.headers.remove(name);
            }
        }
        request.endpoint =
            RequestEndpoint::new_resolved(target.as_str()).map_err(|_| network_error())?;
        current = target;
        redirects += 1;
    }
}
