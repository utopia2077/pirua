use super::*;
use crate::api::{
    runtime::{AdapterResponseHead, cancellable},
    wire_json::stringify_wire_json,
};
use config::Memo;
use pi_ai_transport::*;
use response::{BodyOwner, Event, Reader};
use serde_json::{Value, json};
use url::Url;

struct Region {
    initial: Option<String>,
}
impl Region {
    async fn raw(
        &self,
        settings: &BedrockSettings,
        runtime: &RequestRuntime,
    ) -> Result<String, Failure> {
        match &self.initial {
            Some(v) => Ok(v.clone()),
            None => settings.region.get(runtime).await.map_err(Failure::leaf),
        }
    }
    async fn get(
        &self,
        settings: &BedrockSettings,
        runtime: &RequestRuntime,
    ) -> Result<String, Failure> {
        real_region(&self.raw(settings, runtime).await?)
    }
}
fn real_region(value: &str) -> Result<String, Failure> {
    let real = if matches!(value, "fips-aws-global" | "aws-fips") {
        "us-east-1"
    } else if let Some(v) = value.strip_prefix("fips-") {
        v.strip_prefix("dkr-")
            .or_else(|| v.strip_prefix("prod-"))
            .unwrap_or(v)
    } else {
        value.strip_suffix("-fips").unwrap_or(value)
    };
    if real != "*"
        && (real.is_empty()
            || real.len() > 63
            || real.starts_with('-')
            || real.ends_with('-')
            || !real.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
    {
        return Err(Failure::local());
    }
    Ok(real.into())
}
fn endpoint_region(endpoint: &str) -> Option<String> {
    let url = Url::parse(endpoint).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let host = host
        .strip_prefix("bedrock-runtime.")
        .or_else(|| host.strip_prefix("bedrock-runtime-fips."))?;
    let region = host
        .strip_suffix(".amazonaws.com.cn")
        .or_else(|| host.strip_suffix(".amazonaws.com"))?;
    (!region.is_empty()
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
    .then(|| region.into())
}
fn arn_region(id: &str) -> Option<String> {
    let mut parts = id.strip_prefix("arn:")?.split(':');
    let partition = parts.next()?;
    if !(partition == "aws"
        || partition.strip_prefix("aws-").is_some_and(|s| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        }))
        || parts.next()? != "bedrock"
    {
        return None;
    }
    let region = parts.next()?;
    parts.next()?;
    (!region.is_empty()
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
    .then(|| region.into())
}
fn number(value: f64) -> String {
    if value == 0.0 {
        "0".into()
    } else {
        ryu_js::Buffer::new().format_finite(value).into()
    }
}
fn source_get<'a>(headers: &'a [(String, String)], key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}
fn uuid(mut bytes: [u8; 16]) -> String {
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    let h = signing::hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}
fn suffix(region: &str, dual: bool) -> &'static str {
    let matches = |prefix: &str| {
        region
            .strip_prefix(prefix)
            .and_then(|s| s.rsplit_once('-'))
            .is_some_and(|(word, n)| {
                !word.is_empty()
                    && word.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    && !n.is_empty()
                    && n.bytes().all(|b| b.is_ascii_digit())
            })
    };
    if region == "aws-cn-global" || matches("cn-") {
        if dual {
            "api.amazonwebservices.com.cn"
        } else {
            "amazonaws.com.cn"
        }
    } else if matches("eusc-de-") {
        if dual {
            "api.amazonwebservices.eu"
        } else {
            "amazonaws.eu"
        }
    } else if region == "aws-iso-global" || matches("us-iso-") {
        if dual { "api.aws.ic.gov" } else { "c2s.ic.gov" }
    } else if region == "aws-iso-b-global" || matches("us-isob-") {
        if dual {
            "api.aws.scloud"
        } else {
            "sc2s.sgov.gov"
        }
    } else if region == "aws-iso-e-global" || matches("eu-isoe-") {
        if dual {
            "api.cloud-aws.adc-e.uk"
        } else {
            "cloud.adc-e.uk"
        }
    } else if region == "aws-iso-f-global" || matches("us-isof-") {
        if dual {
            "api.aws.hci.ic.gov"
        } else {
            "csp.hci.ic.gov"
        }
    } else if dual {
        "api.aws"
    } else {
        "amazonaws.com"
    }
}
fn ua_escape(value: &str, hash: bool) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "!$%&'*+-.^_`|~".contains(c) || (hash && c == '#') {
                c
            } else {
                '-'
            }
        })
        .collect()
}
fn ua_section(name: &str, value: Option<&str>) -> String {
    let mut parts = name
        .split('/')
        .map(|s| ua_escape(s, false))
        .collect::<Vec<_>>();
    if parts.first().is_some_and(|s| s == "api") && parts.len() > 1 {
        parts[1] = parts[1].to_lowercase();
    }
    let mut name = parts.join("/");
    if let Some(v) = value.filter(|v| !v.is_empty()) {
        name.push(if parts.len() > 1 { '#' } else { '/' });
        name.push_str(&ua_escape(v, true));
    }
    name
}

pub(super) async fn run(
    api: &BedrockApi,
    request: &TextStreamRequest,
    options: &BedrockOptions,
    producer: &AssistantStreamProducer,
    reducer: &mut reducer::Reducer,
) -> Result<(), Failure> {
    let runtime = &request.options.runtime;
    if runtime.cancellation.is_cancelled() {
        return Err(Failure::local());
    }
    let effective = request.resolved.endpoint.expose_for_transport();
    let ambient = &api.services.env;
    let profile = options
        .profile
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            request
                .auth
                .env
                .as_map()
                .get("AWS_PROFILE")
                .filter(|s| !s.is_empty())
                .map(String::as_str)
        });
    let selected_profile = profile.or_else(|| build::env(request, ambient, "AWS_PROFILE"));
    let proxy = ProxyEnvironment {
        scoped: request.auth.env.as_map().clone(),
        ambient: ambient.as_map().clone(),
    }
    .resolve(effective, false)
    .map_err(Failure::transport)?;
    let settings = api
        .configuration
        .create_session(BedrockConfigKey {
            profile: selected_profile,
            ambient,
        })
        .map_err(|_| Failure::local())?;
    let configured_region = options
        .region
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| build::env(request, ambient, "AWS_REGION"))
        .or_else(|| build::env(request, ambient, "AWS_DEFAULT_REGION"));
    let has_ambient_profile = ambient
        .as_map()
        .get("AWS_PROFILE")
        .is_some_and(|s| !s.is_empty());
    let endpoint_region = endpoint_region(effective);
    let explicit_endpoint =
        endpoint_region.is_none() || (configured_region.is_none() && !has_ambient_profile);
    let region = Region {
        initial: arn_region(request.resolved.model.id.as_str())
            .or_else(|| configured_region.map(str::to_owned))
            .or_else(|| explicit_endpoint.then(|| endpoint_region.clone()).flatten())
            .or_else(|| (!has_ambient_profile).then(|| "us-east-1".into())),
    };
    let skip = build::env(request, ambient, "AWS_BEDROCK_SKIP_AUTH") == Some("1");
    let bearer = options
        .bearer_token
        .as_ref()
        .filter(|s| !s.expose_secret().is_empty())
        .map(|s| s.expose_secret())
        .or_else(|| {
            request
                .auth
                .api_key
                .as_ref()
                .filter(|s| !s.expose_secret().is_empty())
                .map(|s| s.expose_secret())
        })
        .or_else(|| build::env(request, ambient, "AWS_BEARER_TOKEN_BEDROCK"))
        .filter(|_| !skip);
    let static_credentials = if skip {
        Some(BedrockCredentials {
            access_key_id: SecretString::new("dummy-access-key"),
            secret_access_key: SecretString::new("dummy-secret-key"),
            session_token: None,
            expiration_unix_ms: None,
        })
    } else if profile.is_none() {
        build::env(request, ambient, "AWS_ACCESS_KEY_ID")
            .zip(build::env(request, ambient, "AWS_SECRET_ACCESS_KEY"))
            .map(|(key, secret)| BedrockCredentials {
                access_key_id: SecretString::new(key),
                secret_access_key: SecretString::new(secret),
                session_token: build::env(request, ambient, "AWS_SESSION_TOKEN")
                    .map(SecretString::new),
                expiration_unix_ms: None,
            })
    } else {
        None
    };
    let static_identity =
        static_credentials.is_some() || matches!(settings.credentials, BedrockSetting::Value(_));
    let mut credentials = Memo::new(
        static_credentials.map_or_else(|| settings.credentials.clone(), BedrockSetting::Value),
    );
    let mut token = Memo::new(bearer.map_or_else(
        || settings.token.clone(),
        |s| {
            BedrockSetting::Value(BedrockToken {
                token: SecretString::new(s),
                expiration_unix_ms: None,
            })
        },
    ));
    let mut payload = BedrockPayload::new(
        build::payload(request, options, ambient).map_err(|_| Failure::local())?,
    );
    if let Some(hook) = &api.services.payload
        && let Some(replacement) = cancellable(
            hook.on_payload(request, &mut payload),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::local())?
        .map_err(|_| Failure::local())?
    {
        payload = replacement;
    }
    region.get(&settings, runtime).await?;
    let preference = if bearer.is_some() {
        vec![BedrockAuthScheme::Bearer]
    } else {
        settings
            .auth_scheme_preference
            .get(runtime)
            .await
            .map_err(Failure::leaf)?
    };
    let scheme = preference
        .first()
        .copied()
        .unwrap_or(BedrockAuthScheme::SigV4);
    let selected_token = if scheme == BedrockAuthScheme::Bearer {
        Some(token.get(runtime).await.map_err(Failure::leaf)?)
    } else {
        credentials.get(runtime).await.map_err(Failure::leaf)?;
        None
    };
    let selected_endpoint = if explicit_endpoint {
        Some(effective.to_owned())
    } else {
        settings
            .endpoint_override
            .get(runtime)
            .await
            .map_err(Failure::leaf)?
            .map(|e| e.expose_for_transport().to_owned())
    };
    let raw_region = region.raw(&settings, runtime).await?;
    let fips = if raw_region.starts_with("fips-") || raw_region.ends_with("-fips") {
        true
    } else {
        settings
            .use_fips_endpoint
            .get(runtime)
            .await
            .map_err(Failure::leaf)?
    };
    let endpoint_region = region.get(&settings, runtime).await?;
    let dual = settings
        .use_dualstack_endpoint
        .get(runtime)
        .await
        .map_err(Failure::leaf)?;
    if selected_endpoint.is_some() && (fips || dual) {
        return Err(Failure::local());
    }
    let endpoint = selected_endpoint.unwrap_or_else(|| {
        format!(
            "https://bedrock-runtime{}.{endpoint_region}.{}",
            if fips { "-fips" } else { "" },
            suffix(&endpoint_region, dual)
        )
    });
    let mut url = Url::parse(&endpoint).map_err(|_| Failure::local())?;
    let model_id = payload
        .as_value()
        .get("modelId")
        .ok_or_else(Failure::local)?;
    let model_id = match model_id {
        BedrockValue::String(v) => v.clone(),
        BedrockValue::Number(v) => number(v.get()),
        BedrockValue::Bool(v) => v.to_string(),
        BedrockValue::Null => return Err(Failure::local()),
        _ => return Err(Failure::local()),
    };
    let base = url.path().strip_suffix('/').unwrap_or(url.path());
    let path = format!(
        "{base}/model/{}/converse-stream",
        signing::escape(&model_id)
    );
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    let mut projected = sdk::serialize(payload.as_value(), "ConverseStreamRequest")
        .map_err(|_| Failure::local())?;
    projected
        .as_object_mut()
        .ok_or_else(Failure::local)?
        .shift_remove("modelId");
    let body = stringify_wire_json(&projected)
        .map_err(|_| Failure::local())?
        .into_bytes();
    let mut headers = vec![
        ("content-type".into(), "application/json".into()),
        ("content-length".into(), body.len().to_string()),
    ];
    let mode = settings
        .retry_mode
        .get(runtime)
        .await
        .map_err(Failure::leaf)?;
    let app = settings
        .user_agent_app_id
        .get(runtime)
        .await
        .map_err(Failure::leaf)?;
    let mut sections = settings
        .user_agent
        .iter()
        .map(|(k, v)| ua_section(k, v.as_deref()))
        .collect::<Vec<_>>();
    let mut features = Vec::new();
    if explicit_endpoint {
        features.push("N");
    }
    features.push(if mode == SmithyRetryMode::Adaptive {
        "F"
    } else {
        "E"
    });
    if scheme == BedrockAuthScheme::SigV4 && static_identity {
        features.push("e");
    }
    sections.push(format!("m/{}", features.join(",")));
    if let Some(app) = app.filter(|s| !s.is_empty()) {
        sections.push(ua_section("app", Some(&app)));
    }
    let normal = sections
        .iter()
        .filter(|s| s.starts_with("aws-sdk-"))
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    if !normal.is_empty() {
        headers.push(("x-amz-user-agent".into(), normal));
    }
    headers.push(("user-agent".into(), sections.join(" ")));
    let h1 =
        proxy.is_some() || build::env(request, ambient, "AWS_BEDROCK_FORCE_HTTP1") == Some("1");
    let protocol = if h1 {
        SmithyProtocol::Http1
    } else {
        SmithyProtocol::Http2
    };
    let host = url.host_str().ok_or_else(Failure::local)?;
    let authority = match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.into(),
    };
    headers.push((
        if h1 { "host" } else { ":authority" }.into(),
        authority.clone(),
    ));
    for (name, value) in request.auth.headers.iter_for_dispatch() {
        let lower = name.to_ascii_lowercase();
        if lower == "authorization"
            || lower == "host"
            || lower.starts_with("x-amz-")
            || name == "__proto__"
        {
            continue;
        }
        if let Some(value) = value {
            signing::set(&mut headers, name, value.clone());
        }
    }
    let max_attempts = settings
        .max_attempts
        .get(runtime)
        .await
        .map_err(Failure::leaf)?
        .get();
    let mut retry =
        SmithyRetryState::new(mode, settings.retry_generation, runtime.clock.now_unix_ms());
    retry
        .acquire_send_token(runtime)
        .await
        .map_err(Failure::transport)?;
    signing::set(
        &mut headers,
        "amz-sdk-invocation-id",
        uuid(
            api.entropy
                .invocation_bytes()
                .map_err(|_| Failure::local())?,
        ),
    );
    let mut offset = 0.0;
    let (mut reader, first, id) = loop {
        if runtime.cancellation.is_cancelled() {
            return Err(Failure::local());
        }
        signing::set(
            &mut headers,
            "amz-sdk-request",
            format!("attempt={}; max={}", retry.attempts(), number(max_attempts)),
        );
        let signed = if let Some(token) = &selected_token {
            if token.token.expose_secret().is_empty() {
                return Err(Failure::local());
            }
            let mut h = headers.clone();
            signing::set(
                &mut h,
                "Authorization",
                format!("Bearer {}", token.token.expose_secret()),
            );
            Ok(h)
        } else {
            let signing_region = region.get(&settings, runtime).await;
            match signing_region {
                Ok(region) => match credentials.get(runtime).await {
                    Ok(creds) => signing::sign(
                        signing::SignInput {
                            method: "POST",
                            path: url.path(),
                            query: &[],
                            headers: &headers,
                            body: &body,
                            region: &region,
                            date_ms: runtime.clock.now_unix_ms() as f64 + offset,
                        },
                        &creds,
                    )
                    .map_err(|_| Failure::local()),
                    Err(e) => Err(Failure::leaf(e)),
                },
                Err(e) => Err(e),
            }
        };
        let result = match signed {
            Ok(signed) => {
                attempt(
                    api,
                    request,
                    SignedAttempt {
                        url: &url,
                        body: &body,
                        headers: &signed,
                        proxy: proxy.as_ref(),
                        protocol,
                    },
                    &reducer.partial,
                    &mut offset,
                )
                .await
            }
            Err(failure) => Err(AttemptFailure {
                failure,
                headers: None,
            }),
        };
        match result {
            Ok(success) => {
                retry.success(runtime.clock.now_unix_ms());
                break success;
            }
            Err(failed) => {
                retry.observe_failure(failed.failure.class, runtime.clock.now_unix_ms());
                let next_max = match settings.max_attempts.get(runtime).await {
                    Ok(n) => n.get(),
                    Err(_) if runtime.cancellation.is_cancelled() => return Err(Failure::local()),
                    Err(_) => 3.0,
                };
                if !retry.can_retry(failed.failure.class, next_max) {
                    return Err(failed.failure);
                }
                let jitter = api.entropy.retry_jitter().map_err(|_| Failure::local())?;
                let after = failed
                    .headers
                    .as_ref()
                    .and_then(|h| smithy_retry_after(h, runtime.clock.now_unix_ms()));
                let Some(delay) = retry
                    .next_delay(failed.failure.class, next_max, jitter, after)
                    .map_err(Failure::transport)?
                else {
                    return Err(failed.failure);
                };
                retry
                    .acquire_send_token(runtime)
                    .await
                    .map_err(Failure::transport)?;
                smithy_retry_sleep(runtime, delay)
                    .await
                    .map_err(Failure::transport)?;
            }
        }
    };
    if let Some(Event::Modeled(kind, event)) = first
        && let Err(mut error) = reducer.consume(&kind, &event, &request.resolved.model, producer)
    {
        error.request_id = error.request_id.or(id.clone());
        return Err(error);
    }
    while let Some(event) = reader
        .next(runtime, &reducer.partial, false)
        .await
        .map_err(|mut e| {
            e.request_id = e.request_id.or(id.clone());
            e
        })?
    {
        if let Event::Modeled(kind, event) = event
            && let Err(mut e) = reducer.consume(&kind, &event, &request.resolved.model, producer)
        {
            e.request_id = e.request_id.or(id.clone());
            return Err(e);
        }
    }
    if runtime.cancellation.is_cancelled() {
        return Err(Failure::local());
    }
    reducer.finish(producer).map_err(|mut e| {
        e.request_id = e.request_id.or(id);
        e
    })
}
struct AttemptFailure {
    failure: Failure,
    headers: Option<HttpHeaders>,
}
struct SignedAttempt<'a> {
    url: &'a Url,
    body: &'a [u8],
    headers: &'a [(String, String)],
    proxy: Option<&'a ProxyUrl>,
    protocol: SmithyProtocol,
}
async fn attempt(
    api: &BedrockApi,
    request: &TextStreamRequest,
    signed: SignedAttempt<'_>,
    template: &AssistantMessage,
    offset: &mut f64,
) -> Result<(Reader, Option<Event>, Option<String>), AttemptFailure> {
    let SignedAttempt {
        url,
        body,
        headers,
        proxy,
        protocol,
    } = signed;
    let local = || AttemptFailure {
        failure: Failure::local(),
        headers: None,
    };
    let runtime = &request.options.runtime;
    let expect = source_get(headers, "Expect")
        .filter(|s| !s.is_empty())
        .or_else(|| source_get(headers, "expect"));
    let admission = if proxy.is_none() && expect == Some("100-continue") {
        SmithyBodyAdmission::WaitForContinue
    } else {
        SmithyBodyAdmission::Immediate
    };
    let profile = if protocol == SmithyProtocol::Http1 {
        SmithyRequestProfile::Http1 {
            body_admission: admission,
            connection: if proxy.is_some()
                || source_get(headers, "Expect").or_else(|| source_get(headers, "expect"))
                    == Some("100-continue")
            {
                SmithyH1Connection::Close
            } else {
                SmithyH1Connection::KeepAlive
            },
        }
    } else {
        SmithyRequestProfile::Http2 {
            pseudo_headers: SmithyH2PseudoHeaders {
                authority: source_get(headers, ":authority").ok_or_else(local)?.into(),
                scheme: source_get(headers, ":scheme").map(str::to_owned),
            },
            body_admission: admission,
        }
    };
    let mut physical = HttpHeaders::new();
    for (k, v) in headers {
        if protocol == SmithyProtocol::Http2
            && matches!(k.as_str(), ":authority" | ":scheme" | ":method" | ":path")
        {
            continue;
        }
        physical.append(k, v).map_err(|e| AttemptFailure {
            failure: Failure::transport(e),
            headers: None,
        })?;
    }
    let http = HttpRequest {
        endpoint: RequestEndpoint::new_resolved(url.as_str()).map_err(|_| local())?,
        method: "POST".into(),
        headers: physical,
        body: body.to_vec(),
        proxy: proxy.cloned(),
    };
    let response = api
        .services
        .transport
        .send_smithy(&http, runtime, &profile)
        .await
        .map_err(|e| AttemptFailure {
            failure: Failure::transport(e),
            headers: None,
        })?;
    let status = response.status;
    let raw = response.headers;
    let mut body = BodyOwner(response.body);
    let head =
        AdapterResponseHead::from_smithy(status, &raw, protocol).map_err(|e| AttemptFailure {
            failure: Failure::transport(e),
            headers: None,
        })?;
    if let Some(hook) = &api.services.response {
        cancellable(hook.on_response(request, &head), &runtime.cancellation)
            .await
            .map_err(|_| local())?
            .map_err(|_| local())?;
    }
    let id = ["x-amzn-requestid", "x-amzn-request-id", "x-amz-request-id"]
        .into_iter()
        .find_map(|k| head.headers.get_str(k))
        .and_then(reducer::diagnostic);
    let mut retry_headers = HttpHeaders::new();
    for (k, v) in head.headers.iter() {
        if matches!(k, "retry-after" | "x-amz-retry-after")
            && let Some(v) = v.as_str()
        {
            retry_headers.append(k, v).map_err(|_| local())?;
        }
    }
    let server_date = head.headers.get_str("date").map(str::to_owned);
    if status >= 300 {
        let bytes = read_body_limited(body.0.as_mut(), runtime, api.services.max_error_body_bytes)
            .await
            .map_err(|e| AttemptFailure {
                failure: Failure::transport(e),
                headers: Some(retry_headers.clone()),
            })?;
        let mut failure = http_error(status, &raw, &bytes, template);
        failure.request_id = id;
        if let Some(date) = server_date
            && skew(offset, &date, runtime)
        {
            failure.class = if failure.class == SmithyRetryClass::Throttling {
                failure.class
            } else {
                SmithyRetryClass::Transient
            };
        }
        return Err(AttemptFailure {
            failure,
            headers: Some(retry_headers),
        });
    }
    // Move the owner without allowing its Drop to close the body prematurely.
    let replacement = Box::new(EmptyBody) as Box<dyn ResponseBody>;
    let body = std::mem::replace(&mut body.0, replacement);
    let mut reader = Reader::new(body, api.limits.eventstream);
    let first = reader
        .next(runtime, template, true)
        .await
        .map_err(|mut failure| {
            failure.status = Some(status);
            failure.request_id = id.clone();
            if let Some(date) = &server_date
                && skew(offset, date, runtime)
                && failure.class != SmithyRetryClass::Throttling
            {
                failure.class = SmithyRetryClass::Transient;
            }
            AttemptFailure {
                failure,
                headers: Some(retry_headers),
            }
        })?;
    if let Some(date) = server_date {
        skew(offset, &date, runtime);
    }
    Ok((reader, first, id))
}
struct EmptyBody;
impl ResponseBody for EmptyBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&mut self) {}
}
fn skew(offset: &mut f64, date: &str, runtime: &RequestRuntime) -> bool {
    if let Some(time) = HttpRetryDateParser.unix_ms(date) {
        let now = runtime.clock.now_unix_ms() as f64;
        if (now + *offset - time).abs() >= 300000.0 {
            let next = time - now;
            let changed = *offset != next;
            *offset = next;
            return changed;
        }
    }
    false
}
fn http_error(
    status: u16,
    headers: &HttpHeaders,
    bytes: &[u8],
    template: &AssistantMessage,
) -> Failure {
    let text = String::from_utf8_lossy(bytes);
    let parsed = if bytes.is_empty() {
        Ok(json!({}))
    } else {
        serde_json::from_str::<Value>(&text)
    };
    let value = match parsed {
        Ok(value) => value,
        Err(parse_error) => {
            let body = response::truncate(text.trim_matches(build::whitespace));
            // The installed SDK appends this fixed diagnostic even to JSON parser
            // failures. Its literal text participates in messageCarriesBody.
            const SDK_DIAGNOSTIC: &str = "Deserialization error: to see the raw response, inspect the hidden field {error}.$response on this object.";
            let parser_kind = parse_error.to_string();
            let preview = !parse_error.is_eof()
                && (parser_kind.starts_with("expected value")
                    || parser_kind.starts_with("expected ident"))
                && text.encode_utf16().count() <= 20;
            let end_message = "Unexpected end of JSON input";
            let fixed_carries = SDK_DIAGNOSTIC.contains(&body)
                || (parse_error.is_eof() && end_message.contains(&body));
            let message = if preview {
                body
            } else if fixed_carries {
                if text.trim_matches(build::whitespace) == "{" {
                    format!(
                        "JSON parser position {} column {}",
                        text.encode_utf16().count(),
                        text.rsplit('\n')
                            .next()
                            .unwrap_or("")
                            .encode_utf16()
                            .count()
                            + 1
                    )
                } else {
                    String::new()
                }
            } else {
                format!("{status}: {body}")
            };
            let mut failure = Failure::message(Some(status), None, &message, template);
            failure.code = None;
            return failure;
        }
    };
    let code = headers
        .get_for_wire("x-amzn-errortype")
        .map(str::to_owned)
        .or_else(|| {
            value
                .get("code")
                .or_else(|| value.get("Code"))
                .or_else(|| value.get("__type"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| status.to_string());
    let code = code
        .split(':')
        .next()
        .unwrap_or(&code)
        .rsplit('#')
        .next()
        .unwrap_or(&code);
    let message = value
        .get("message")
        .or_else(|| value.get("Message"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("UnknownError");
    let prefix = match code {
        "InternalServerException" => "Internal server error",
        "ModelStreamErrorException" => "Model stream error",
        "ValidationException" => "Validation error",
        "ThrottlingException" => "Throttling error",
        "ServiceUnavailableException" => "Service unavailable",
        other => other,
    };
    Failure::message(
        Some(status),
        Some(code),
        &format!("{prefix}: {message}"),
        template,
    )
}
