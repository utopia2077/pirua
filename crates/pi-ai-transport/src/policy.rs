use crate::{
    HttpHeaders, HttpRequest, HttpResponse, RawHttpClient, ResponseBody, TransportError,
    TransportErrorKind,
};
use pi_ai_core::{BoxFuture, CancellationToken, Clock, RequestRuntime};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

pub(crate) type Timer = BoxFuture<'static, Result<(), pi_ai_core::Cancelled>>;

#[cfg(feature = "native")]
pub(crate) async fn wait_with_timeout<T>(
    operation: BoxFuture<'_, Result<T, TransportError>>,
    runtime: &RequestRuntime,
    timeout: Option<Duration>,
) -> Result<T, TransportError> {
    let mut deadline = timeout.map(|duration| timer(runtime.clock.clone(), duration));
    timed_wait(operation, &runtime.cancellation, &mut deadline).await
}

pub(crate) async fn cancel_wait<T>(
    operation: BoxFuture<'_, Result<T, TransportError>>,
    cancellation: &CancellationToken,
) -> Result<T, TransportError> {
    timed_wait(operation, cancellation, &mut None).await
}

pub(crate) async fn timed_wait<T>(
    mut operation: BoxFuture<'_, Result<T, TransportError>>,
    cancellation: &CancellationToken,
    timer: &mut Option<Timer>,
) -> Result<T, TransportError> {
    let mut cancelled = Box::pin(cancellation.cancelled());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(TransportError::new(TransportErrorKind::Cancelled)));
        }
        if let Some(timer) = timer.as_mut()
            && timer.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(TransportError::new(TransportErrorKind::Timeout)));
        }
        let result = operation.as_mut().poll(cx);
        if cancellation.is_cancelled() {
            Poll::Ready(Err(TransportError::new(TransportErrorKind::Cancelled)))
        } else {
            result
        }
    })
    .await
}

fn timer(clock: Arc<dyn Clock>, duration: Duration) -> Timer {
    Box::pin(async move { clock.sleep(duration, &CancellationToken::new()).await })
}

/// No implicit timeout. WholeResponse includes acquisition and body consumption;
/// Headers ends at acquisition; HeadersAndIdle restarts only the body idle timer.
#[derive(Clone, Copy, Debug, Default)]
pub enum TimeoutProfile {
    #[default]
    None,
    Headers(Duration),
    WholeResponse(Duration),
    HeadersAndIdle {
        headers: Duration,
        idle: Duration,
    },
}

/// Each adapter selects a profile. None is required for pi-messages/Mistral.
#[derive(Clone, Copy, Debug, Default)]
pub enum RetryProfile {
    #[default]
    None,
    SharedProvider {
        max_retries: u64,
        max_retry_delay_ms: Option<u64>,
    },
}

/// Injected nondeterminism. Values must be finite and in [0,1).
pub trait RetryJitter: Send + Sync {
    fn sample(&self) -> f64;
}

/// Injectable date interpretation. Defaults accept RFC HTTP dates, canonical
/// Date.toString with GMT offsets, and ISO dates/zoned date-times. Other legacy
/// forms and local-time interpretation require explicit host injection.
pub trait RetryDateParser: Send + Sync {
    fn unix_ms(&self, value: &str) -> Option<f64>;
}
pub struct HttpRetryDateParser;
impl RetryDateParser for HttpRetryDateParser {
    fn unix_ms(&self, value: &str) -> Option<f64> {
        crate::retry_date::parse(value)
    }
}

/// Request retry owner. Once response headers are returned, body errors cannot
/// reach the retry loop. No provider payload or error text is retained here.
pub struct HttpTransport {
    client: Arc<dyn RawHttpClient>,
    jitter: Arc<dyn RetryJitter>,
    date_parser: Arc<dyn RetryDateParser>,
    fetch_redirects: crate::FetchRedirectMode,
}
impl HttpTransport {
    pub fn new(client: Arc<dyn RawHttpClient>, jitter: Arc<dyn RetryJitter>) -> Self {
        Self {
            client,
            jitter,
            date_parser: Arc::new(HttpRetryDateParser),
            fetch_redirects: crate::FetchRedirectMode::Follow,
        }
    }
    pub fn with_date_parser(mut self, parser: Arc<dyn RetryDateParser>) -> Self {
        self.date_parser = parser;
        self
    }
    /// Trusted binding transports may opt out of network Fetch redirects.
    pub fn with_fetch_redirects(mut self, mode: crate::FetchRedirectMode) -> Self {
        self.fetch_redirects = mode;
        self
    }
    pub fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
        retry: RetryProfile,
        timeout: TimeoutProfile,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.send_inner(request, runtime, retry, timeout, None)
    }
    /// Fetch composition for replayable byte requests, not arbitrary RequestInit.
    /// Empty bytes mean absent body for GET/HEAD. A chain shares one attempt
    /// deadline; retry restarts the original request. Body errors never retry.
    pub fn send_fetch<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
        retry: RetryProfile,
        timeout: TimeoutProfile,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        self.send_inner(request, runtime, retry, timeout, Some(self.fetch_redirects))
    }
    fn send_inner<'a>(
        &'a self,
        request: &'a HttpRequest,
        runtime: &'a RequestRuntime,
        retry: RetryProfile,
        timeout: TimeoutProfile,
        fetch: Option<crate::FetchRedirectMode>,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let (max_retries, cap) = match retry {
                RetryProfile::None => (0, None),
                RetryProfile::SharedProvider {
                    max_retries,
                    max_retry_delay_ms,
                } => (max_retries, max_retry_delay_ms),
            };
            if max_retries > 9_007_199_254_740_991 {
                return Err(TransportError::new(TransportErrorKind::InvalidRequest));
            }
            let mut attempt = 0;
            loop {
                // Cancellation wins even against a port that never polls its token.
                if runtime.cancellation.is_cancelled() {
                    return Err(TransportError::new(TransportErrorKind::Cancelled));
                }
                let duration = match timeout {
                    TimeoutProfile::None => None,
                    TimeoutProfile::Headers(d) | TimeoutProfile::WholeResponse(d) => Some(d),
                    TimeoutProfile::HeadersAndIdle { headers, .. } => Some(headers),
                };
                let mut deadline = duration.map(|d| timer(runtime.clock.clone(), d));
                let result = if let Some(mode) = fetch {
                    crate::fetch::attempt(
                        self.client.as_ref(),
                        request,
                        runtime,
                        &mut deadline,
                        mode,
                    )
                    .await
                } else {
                    timed_wait(
                        self.client.send(request, runtime),
                        &runtime.cancellation,
                        &mut deadline,
                    )
                    .await
                };
                let retry_headers = match &result {
                    Ok(response)
                        if !(200..300).contains(&response.status)
                            && shared_retryable(Some(response.status), &response.headers) =>
                    {
                        Some(response.headers.clone())
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            TransportErrorKind::Connection | TransportErrorKind::Timeout
                        ) =>
                    {
                        Some(HttpHeaders::new())
                    }
                    _ => None,
                };
                if attempt < max_retries
                    && let Some(headers) = retry_headers
                {
                    if let Ok(mut response) = result {
                        response.body.close();
                    }
                    let delay =
                        self.retry_delay(&headers, attempt, cap, runtime.clock.now_unix_ms())?;
                    let sleep = Box::pin(async {
                        runtime
                            .clock
                            .sleep(delay, &runtime.cancellation)
                            .await
                            .map_err(|_| TransportError::new(TransportErrorKind::Cancelled))
                    });
                    cancel_wait(sleep, &runtime.cancellation).await?;
                    attempt += 1;
                    continue;
                }
                let mut response = result?;
                let whole = if matches!(timeout, TimeoutProfile::WholeResponse(_)) {
                    deadline
                } else {
                    None
                };
                let idle = match timeout {
                    TimeoutProfile::HeadersAndIdle { idle, .. } => Some(idle),
                    _ => None,
                };
                response.body = Box::new(PolicyBody {
                    inner: response.body,
                    deadline: whole,
                    idle,
                    closed: false,
                    runtime: runtime.clone(),
                });
                return Ok(response);
            }
        })
    }
    fn retry_delay(
        &self,
        headers: &HttpHeaders,
        attempt: u64,
        cap: Option<u64>,
        now: u64,
    ) -> Result<Duration, TransportError> {
        let delay = headers
            .get_for_fetch("retry-after-ms")
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(parse_float_prefix)
            .or_else(|| {
                headers
                    .get_for_fetch("retry-after")
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .map(|s| {
                        parse_float_prefix(s)
                            .map(|v| v * 1000.0)
                            .unwrap_or_else(|| {
                                self.date_parser
                                    .unix_ms(s)
                                    .map(|v| v - now as f64)
                                    .unwrap_or(f64::NAN)
                            })
                    })
            });
        if let Some(delay) = delay {
            let cap = cap.unwrap_or(60_000);
            if cap > 0 && delay > cap as f64 {
                return Err(TransportError::new(TransportErrorKind::RetryDelayExceeded));
            }
            // JS host timers coerce nonfinite/negative or overflowing delays to
            // immediate scheduling; no Duration conversion may panic.
            return Ok(Duration::from_secs_f64(
                if delay.is_finite() && delay > 0.0 && delay <= 2_147_483_647.0 {
                    delay / 1000.0
                } else {
                    0.0
                },
            ));
        }
        let jitter = self.jitter.sample();
        if !(0.0..1.0).contains(&jitter) {
            return Err(TransportError::new(TransportErrorKind::InvalidRequest));
        }
        Ok(Duration::from_secs_f64(
            (0.5 * 2f64.powi(attempt.min(4) as i32)).min(8.0) * (1.0 - jitter * 0.25),
        ))
    }
}

fn shared_retryable(status: Option<u16>, headers: &HttpHeaders) -> bool {
    match headers.get_for_fetch("x-should-retry").as_deref() {
        Some("true") => true,
        Some("false") => false,
        _ => status.is_none_or(|status| matches!(status, 408 | 409 | 429) || status >= 500),
    }
}

// Number.parseFloat prefix semantics, including incomplete exponents.
fn parse_float_prefix(value: &str) -> Option<f64> {
    let value = value.trim_start_matches(crate::is_ecma_whitespace);
    let bytes = value.as_bytes();
    let mut i = usize::from(bytes.first().is_some_and(|b| matches!(b, b'+' | b'-')));
    if value.get(i..)?.starts_with("Infinity") {
        return Some(if bytes.first() == Some(&b'-') {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    let mut digits = 0;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    let exponent = i;
    if bytes.get(i).is_some_and(|b| matches!(b, b'e' | b'E')) {
        i += 1;
        if bytes.get(i).is_some_and(|b| matches!(b, b'+' | b'-')) {
            i += 1;
        }
        let start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            i = exponent;
        }
    }
    value[..i].parse().ok()
}

struct PolicyBody {
    inner: Box<dyn ResponseBody>,
    deadline: Option<Timer>,
    idle: Option<Duration>,
    closed: bool,
    runtime: RequestRuntime,
}
impl ResponseBody for PolicyBody {
    fn next<'a>(
        &'a mut self,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            if self.closed {
                return Ok(None);
            }
            let runtime = &self.runtime;
            let mut idle = self.idle.map(|d| timer(runtime.clock.clone(), d));
            let deadline = if self.deadline.is_some() {
                &mut self.deadline
            } else {
                &mut idle
            };
            let result =
                timed_wait(self.inner.next(runtime), &runtime.cancellation, deadline).await;
            if !matches!(result, Ok(Some(_))) {
                self.close();
            }
            result
        })
    }
    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.deadline = None;
        self.inner.close();
    }
}
impl Drop for PolicyBody {
    fn drop(&mut self) {
        self.close();
    }
}
