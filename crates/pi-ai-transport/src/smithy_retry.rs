//! Invocation-owned Smithy retry quota/backoff and adaptive send-token policy.
//! Provider code retains serialization, signing and first-event priming stages.
use crate::{
    HttpHeaders, HttpRetryDateParser, RetryDateParser, TransportError, TransportErrorKind,
};
use pi_ai_core::RequestRuntime;
use std::{fmt, time::Duration};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmithyRetryMode {
    Standard,
    Adaptive,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SmithyRetryGeneration {
    #[default]
    Legacy,
    V2026,
}
impl SmithyRetryGeneration {
    pub fn captured(value: Option<&str>) -> Self {
        if value == Some("true") {
            Self::V2026
        } else {
            Self::Legacy
        }
    }
    fn base(self) -> f64 {
        if self == Self::V2026 { 50.0 } else { 100.0 }
    }
    fn throttle(self) -> f64 {
        if self == Self::V2026 { 1000.0 } else { 500.0 }
    }
    fn cost(self, class: SmithyRetryClass) -> u32 {
        match (self, class) {
            (Self::Legacy, SmithyRetryClass::Transient) => 10,
            (Self::Legacy, _) => 5,
            (Self::V2026, SmithyRetryClass::Throttling) => 5,
            (Self::V2026, _) => 14,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmithyRetryClass {
    Transient,
    Throttling,
    ServerError,
    ClientError,
}
/// Transient borrowed SDK error observation; no raw value is formatted.
pub struct SmithyRetryError<'a> {
    pub name: &'a str,
    pub message: &'a str,
    pub code: Option<&'a str>,
    pub status: Option<u16>,
    pub retryable_trait: bool,
    pub throttling_trait: bool,
    pub clock_skew_corrected: bool,
    pub type_error: bool,
    pub cause: Option<&'a SmithyRetryError<'a>>,
}
impl fmt::Debug for SmithyRetryError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SmithyRetryError([REDACTED])")
    }
}
impl SmithyRetryError<'_> {
    pub fn class(&self) -> SmithyRetryClass {
        if self.status == Some(429)
            || self.throttling_trait
            || matches!(
                self.name,
                "BandwidthLimitExceeded"
                    | "EC2ThrottledException"
                    | "LimitExceededException"
                    | "PriorRequestNotComplete"
                    | "ProvisionedThroughputExceededException"
                    | "RequestLimitExceeded"
                    | "RequestThrottled"
                    | "RequestThrottledException"
                    | "SlowDown"
                    | "ThrottledException"
                    | "Throttling"
                    | "ThrottlingException"
                    | "TooManyRequestsException"
                    | "TransactionInProgressException"
            )
        {
            SmithyRetryClass::Throttling
        } else if self.transient(0) {
            SmithyRetryClass::Transient
        } else if self.status.is_some_and(|s| (500..=599).contains(&s)) {
            SmithyRetryClass::ServerError
        } else {
            SmithyRetryClass::ClientError
        }
    }
    fn transient(&self, depth: usize) -> bool {
        self.retryable_trait
            || self.clock_skew_corrected
            || (self.name == "InvalidSignatureException"
                && self.message.contains("Signature expired"))
            || matches!(
                self.name,
                "TimeoutError" | "RequestTimeout" | "RequestTimeoutException"
            )
            || matches!(
                self.code,
                Some(
                    "ECONNRESET"
                        | "ECONNREFUSED"
                        | "EPIPE"
                        | "ETIMEDOUT"
                        | "EHOSTUNREACH"
                        | "ENETUNREACH"
                        | "ENOTFOUND"
                )
            )
            || matches!(self.status, Some(500 | 502 | 503 | 504))
            || (self.type_error
                && matches!(
                    self.message,
                    "Failed to fetch"
                        | "NetworkError when attempting to fetch resource"
                        | "The Internet connection appears to be offline"
                        | "Load failed"
                        | "Network request failed"
                ))
            || (self.code == Some("ERR_HTTP2_STREAM_ERROR")
                && self.message.contains("NGHTTP2_REFUSED_STREAM"))
            || (depth <= 10 && self.cause.is_some_and(|c| c.transient(depth + 1)))
    }
}
#[derive(Debug)]
pub struct SmithyRetryState {
    mode: SmithyRetryMode,
    generation: SmithyRetryGeneration,
    capacity: u32,
    retries: u32,
    last_cost: Option<u32>,
    rate: Rate,
}
impl SmithyRetryState {
    pub fn new(mode: SmithyRetryMode, generation: SmithyRetryGeneration, now_ms: u64) -> Self {
        Self {
            mode,
            generation,
            capacity: 500,
            retries: 0,
            last_cost: None,
            rate: Rate::new(now_ms),
        }
    }
    pub fn attempts(&self) -> u32 {
        self.retries + 1
    }
    pub fn capacity(&self) -> u32 {
        self.capacity
    }
    /// Adaptive observes failure before the separate max-attempts leaf is awaited.
    pub fn observe_failure(&mut self, class: SmithyRetryClass, now_ms: u64) {
        if self.mode == SmithyRetryMode::Adaptive {
            self.rate
                .update(class == SmithyRetryClass::Throttling, now_ms)
        }
    }
    /// Missing quota/attempt eligibility consumes no entropy; pass jitter only
    /// once the caller has determined eligibility with can_retry.
    pub fn can_retry(&self, class: SmithyRetryClass, max_attempts: f64) -> bool {
        matches!(
            class,
            SmithyRetryClass::Transient | SmithyRetryClass::Throttling
        ) && f64::from(self.attempts()) < max_attempts
            && self.capacity >= self.generation.cost(class)
    }
    pub fn next_delay(
        &mut self,
        class: SmithyRetryClass,
        max_attempts: f64,
        jitter: f64,
        retry_after_ms: Option<f64>,
    ) -> Result<Option<Duration>, TransportError> {
        if !self.can_retry(class, max_attempts) {
            return Ok(None);
        }
        if !(0.0..1.0).contains(&jitter) {
            return Err(TransportError::new(TransportErrorKind::InvalidRequest));
        }
        let base = if class == SmithyRetryClass::Throttling {
            self.generation.throttle()
        } else {
            self.generation.base()
        };
        let random =
            (jitter * (base * 2f64.powi(self.retries.min(30) as i32)).min(20000.0)).floor();
        let delay = match retry_after_ms {
            Some(hint) if hint.is_nan() => 1.0,
            Some(hint) => random.max(hint.min(random + 5000.0)).min(20000.0),
            None => random,
        };
        let cost = self.generation.cost(class);
        self.capacity -= cost;
        self.last_cost = Some(cost);
        self.retries += 1;
        Ok(Some(Duration::from_secs_f64(delay / 1000.0)))
    }
    /// Called initially and after a successful retry-token refresh, before SDK cooldown.
    pub async fn acquire_send_token(
        &mut self,
        runtime: &RequestRuntime,
    ) -> Result<(), TransportError> {
        if self.mode == SmithyRetryMode::Standard {
            return cancelled(runtime);
        }
        loop {
            cancelled(runtime)?;
            if !self.rate.enabled {
                return Ok(());
            }
            self.rate.refill(runtime.clock.now_unix_ms());
            if self.rate.tokens >= 1.0 {
                self.rate.tokens -= 1.0;
                return Ok(());
            }
            let ms = (1.0 - self.rate.tokens) / self.rate.fill * 1000.0;
            sleep(runtime, Duration::from_secs_f64(ms / 1000.0)).await?;
        }
    }
    pub fn success(&mut self, now_ms: u64) {
        if self.mode == SmithyRetryMode::Adaptive {
            self.rate.update(false, now_ms);
        }
        self.capacity = (self.capacity + self.last_cost.unwrap_or(1)).min(500);
    }
}
pub async fn smithy_retry_sleep(
    runtime: &RequestRuntime,
    delay: Duration,
) -> Result<(), TransportError> {
    sleep(runtime, delay).await
}
fn cancelled(runtime: &RequestRuntime) -> Result<(), TransportError> {
    if runtime.cancellation.is_cancelled() {
        Err(TransportError::new(TransportErrorKind::Cancelled))
    } else {
        Ok(())
    }
}
async fn sleep(runtime: &RequestRuntime, delay: Duration) -> Result<(), TransportError> {
    cancelled(runtime)?;
    crate::policy::cancel_wait(
        Box::pin(async {
            runtime
                .clock
                .sleep(delay, &runtime.cancellation)
                .await
                .map_err(|_| TransportError::new(TransportErrorKind::Cancelled))
        }),
        &runtime.cancellation,
    )
    .await
}
#[derive(Debug)]
struct Rate {
    enabled: bool,
    tokens: f64,
    last_max: f64,
    measured: f64,
    requests: u32,
    fill: f64,
    last_throttle: f64,
    last_time: f64,
    bucket: f64,
    max: f64,
    window: f64,
}
impl Rate {
    fn new(ms: u64) -> Self {
        let now = ms as f64 / 1000.0;
        Self {
            enabled: false,
            tokens: 0.0,
            last_max: 0.0,
            measured: 0.0,
            requests: 0,
            fill: 0.5,
            last_throttle: now,
            last_time: 0.0,
            bucket: now.floor(),
            max: 1.0,
            window: 0.0,
        }
    }
    fn precise(n: f64) -> f64 {
        format!("{n:.8}").parse().unwrap_or(n)
    }
    fn refill(&mut self, ms: u64) {
        let now = ms as f64 / 1000.0;
        if self.last_time == 0.0 {
            self.last_time = now;
            return;
        }
        self.tokens = self
            .max
            .min(self.tokens + (now - self.last_time) * self.fill);
        self.last_time = now;
    }
    fn update(&mut self, throttle: bool, ms: u64) {
        let now = ms as f64 / 1000.0;
        let bucket = (now * 2.0).floor() / 2.0;
        self.requests += 1;
        if bucket > self.bucket {
            self.measured = Self::precise(
                f64::from(self.requests) / (bucket - self.bucket) * 0.8 + self.measured * 0.2,
            );
            self.requests = 0;
            self.bucket = bucket;
        }
        let rate = if throttle {
            let used = if self.enabled {
                self.measured.min(self.fill)
            } else {
                self.measured
            };
            self.last_max = used;
            self.window = Self::precise((self.last_max * 0.3 / 0.4).cbrt());
            self.last_throttle = now;
            self.enabled = true;
            Self::precise(used * 0.7)
        } else {
            self.window = Self::precise((self.last_max * 0.3 / 0.4).cbrt());
            Self::precise(0.4 * (now - self.last_throttle - self.window).powi(3) + self.last_max)
        };
        let rate = rate.min(2.0 * self.measured);
        self.refill(ms);
        self.fill = rate.max(0.5);
        self.max = rate.max(1.0);
        self.tokens = self.tokens.min(self.max);
    }
}
/// Source first matching own header wins, even if its value is invalid.
pub fn smithy_retry_after(headers: &HttpHeaders, now_ms: u64) -> Option<f64> {
    for (name, value) in headers.iter_for_wire() {
        if name == "x-amz-retry-after" {
            return js_number(value).map(|v| date_hint(v, now_ms));
        }
        if name != "retry-after" {
            continue;
        }
        if value.ends_with("GMT") {
            return HttpRetryDateParser
                .unix_ms(value)
                .map(|n| n - now_ms as f64);
        }
        let candidate = value.rsplit_once(" GMT, ").map_or(value, |(_, v)| v);
        if !candidate.is_empty()
            && candidate.bytes().all(|b| b.is_ascii_digit() || b == b'.')
            && candidate.bytes().filter(|b| *b == b'.').count() <= 1
            && !candidate.starts_with('.')
            && !candidate.ends_with('.')
        {
            return candidate
                .parse::<f64>()
                .ok()
                .map(|n| date_hint(n * 1000.0, now_ms));
        }
        return HttpRetryDateParser
            .unix_ms(value)
            .filter(|n| *n >= now_ms as f64)
            .map(|n| n - now_ms as f64);
    }
    None
}
// JS Date clips nonfinite/out-of-range instants; a present Invalid Date poisons
// the source delay into NaN, which Node's timer normalizes to one millisecond.
fn date_hint(delta: f64, now: u64) -> f64 {
    let date = delta + now as f64;
    if !date.is_finite() || date.abs() > 8.64e15 {
        f64::NAN
    } else {
        date.trunc() - now as f64
    }
}
fn js_number(value: &str) -> Option<f64> {
    let value = value.trim_matches(crate::is_ecma_whitespace);
    if value.is_empty() {
        return Some(0.0);
    }
    for (prefix, radix) in [
        ("0x", 16),
        ("0X", 16),
        ("0o", 8),
        ("0O", 8),
        ("0b", 2),
        ("0B", 2),
    ] {
        if let Some(text) = value.strip_prefix(prefix) {
            if text.is_empty() {
                return None;
            }
            let mut out = 0.0;
            for c in text.chars() {
                out = out * f64::from(radix) + f64::from(c.to_digit(radix)?);
            }
            return Some(out);
        }
    }
    if !value.bytes().any(|b| b.is_ascii_digit())
        && !matches!(value, "Infinity" | "+Infinity" | "-Infinity")
    {
        return None;
    }
    value.parse().ok().filter(|v: &f64| !v.is_nan())
}
