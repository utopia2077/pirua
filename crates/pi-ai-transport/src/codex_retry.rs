//! Codex's direct-Fetch attempt owner. Provider errors remain opaque; there is
//! no SDK status classifier, jitter, lower-level retry or successful-body replay.
use crate::{HttpHeaders, RetryDateParser};
use pi_ai_core::{BoxFuture, RequestRuntime};
use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CodexRetryObservation {
    Terminal,
    Backoff,
    ServerDelay(f64),
}
pub struct CodexAttemptFailure<E> {
    pub error: E,
    pub retry: CodexRetryObservation,
}
impl<E> fmt::Debug for CodexAttemptFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CodexAttemptFailure([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct CodexRetryOptions {
    pub max_retries: u64,
    /// Missing means60000; zero disables only the server-delay cap.
    pub max_retry_delay_ms: Option<u64>,
}
pub enum CodexRetryError<E> {
    Attempt(E),
    Cancelled,
    InvalidPolicy,
    DelayExceeded { requested_ms: f64, limit_ms: u64 },
}
impl<E> fmt::Debug for CodexRetryError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CodexRetryError([REDACTED])")
    }
}
impl<E> fmt::Display for CodexRetryError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Codex request attempt failed")
    }
}
impl<E> std::error::Error for CodexRetryError<E> {}

/// Runs inclusive attempts, racing original cancellation around operation/sleep.
/// The index is supplied so the provider can preserve its last-attempt formatting
/// stage before giving the mechanism a typed retry observation.
pub fn retry_codex_request<'a, T, E, F>(
    runtime: &'a RequestRuntime,
    options: CodexRetryOptions,
    mut attempt: F,
) -> BoxFuture<'a, Result<T, CodexRetryError<E>>>
where
    T: Send + 'a,
    E: Send + 'a,
    F: FnMut(u64) -> BoxFuture<'a, Result<T, CodexAttemptFailure<E>>> + Send + 'a,
{
    Box::pin(async move {
        if options.max_retries > 9_007_199_254_740_991 {
            return Err(CodexRetryError::InvalidPolicy);
        }
        let mut index = 0;
        loop {
            if runtime.cancellation.is_cancelled() {
                return Err(CodexRetryError::Cancelled);
            }
            let result = crate::policy::cancel_wait(
                Box::pin(async { Ok(attempt(index).await) }),
                &runtime.cancellation,
            )
            .await
            .map_err(|_| CodexRetryError::Cancelled)?;
            let failure = match result {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            if index == options.max_retries || failure.retry == CodexRetryObservation::Terminal {
                return Err(CodexRetryError::Attempt(failure.error));
            }
            let millis = match failure.retry {
                CodexRetryObservation::Terminal => unreachable!(),
                CodexRetryObservation::Backoff => 1000.0 * 2f64.powi(index.min(1024) as i32),
                CodexRetryObservation::ServerDelay(delay) => {
                    if delay.is_nan() || delay < 0.0 {
                        return Err(CodexRetryError::InvalidPolicy);
                    }
                    let cap = options.max_retry_delay_ms.unwrap_or(60_000);
                    if cap > 0 && delay > cap as f64 {
                        return Err(CodexRetryError::DelayExceeded {
                            requested_ms: delay,
                            limit_ms: cap,
                        });
                    }
                    delay
                }
            };
            // Host timer coercion is independent of the server-delay cap. Node
            // schedules out-of-range/sub-ms timers immediately (1ms minimum).
            let millis = if (1.0..=2_147_483_647.0).contains(&millis) {
                millis.floor() as u64
            } else {
                1
            };
            crate::policy::cancel_wait(
                Box::pin(async {
                    runtime
                        .clock
                        .sleep(Duration::from_millis(millis), &runtime.cancellation)
                        .await
                        .map_err(|_| {
                            crate::TransportError::new(crate::TransportErrorKind::Cancelled)
                        })
                }),
                &runtime.cancellation,
            )
            .await
            .map_err(|_| CodexRetryError::Cancelled)?;
            index += 1;
        }
    })
}

/// Codex uses whole-string Number coercion, not SDK parseFloat. Date parsing is
/// explicit and reuses the existing configured deterministic date capability.
pub fn codex_retry_after_delay_ms(
    headers: &HttpHeaders,
    now: u64,
    dates: &dyn RetryDateParser,
) -> Option<f64> {
    if let Some(value) = headers.get_for_fetch("retry-after-ms")
        && let Some(number) = number(&value)
    {
        return Some(number.max(0.0));
    }
    let value = headers.get_for_fetch("retry-after")?;
    if value.is_empty() {
        return None;
    }
    if let Some(number) = number(&value) {
        return Some((number * 1000.0).max(0.0));
    }
    dates
        .unix_ms(&value)
        .filter(|date| !date.is_nan())
        .map(|date| (date - now as f64).max(0.0))
}
fn number(value: &str) -> Option<f64> {
    let value = value.trim_matches(crate::is_ecma_whitespace);
    if value.is_empty() {
        return Some(0.0);
    }
    let radix = if value.starts_with("0x") || value.starts_with("0X") {
        Some(16)
    } else if value.starts_with("0o") || value.starts_with("0O") {
        Some(8)
    } else if value.starts_with("0b") || value.starts_with("0B") {
        Some(2)
    } else {
        None
    };
    let result = if let Some(radix) = radix {
        let digits = &value[2..];
        if digits.is_empty() {
            return None;
        }
        let width = match radix {
            16 => 4,
            8 => 3,
            _ => 1,
        };
        let (mut top, mut bits, mut round, mut sticky) = (0u64, 0usize, false, false);
        for c in digits.chars() {
            let digit = c.to_digit(radix)?;
            for shift in (0..width).rev() {
                let bit = (digit >> shift) & 1;
                if bits == 0 && bit == 0 {
                    continue;
                }
                bits = bits.saturating_add(1);
                if bits <= 53 {
                    top = (top << 1) | u64::from(bit);
                } else if bits == 54 {
                    round = bit != 0;
                } else {
                    sticky |= bit != 0;
                }
            }
        }
        if bits > 1024 {
            return None;
        }
        if bits <= 53 {
            top as f64
        } else {
            if round && (sticky || top & 1 != 0) {
                top += 1;
            }
            (top as f64) * 2f64.powi((bits - 53) as i32)
        }
    } else {
        value.parse::<f64>().ok()?
    };
    result.is_finite().then_some(result)
}
