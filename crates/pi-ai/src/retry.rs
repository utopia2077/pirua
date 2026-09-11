//! Stateless SDK retries. Durable Harness retry and caller overflow policy are separate.
use pi_ai_core::{
    AssistantMessage, BoxFuture, CancellationToken, Clock, RetryPolicy, SafeUnsigned, StopReason,
    is_retryable_assistant_error,
};
use std::{error::Error, fmt, sync::Arc, time::Duration};

/// Raw ECMAScript exponential delay, including positive infinity and NaN.
/// This observation is process-local and deliberately not serializable.
#[derive(Clone, Copy, Debug)]
pub struct RetryDelay(f64);
impl RetryDelay {
    /// The exact source numeric observation, before timer admission.
    pub fn raw_milliseconds(self) -> f64 {
        self.0
    }
    /// Node timer admission: out-of-range/NaN becomes 1ms, otherwise truncate.
    pub fn node_duration(self) -> Duration {
        let ms = if self.0.is_nan() || self.0 < 1.0 || self.0 > 2_147_483_647.0 {
            1
        } else {
            self.0 as u64
        };
        Duration::from_millis(ms)
    }
    fn exponential(base: SafeUnsigned, attempt: u64) -> Self {
        let exponent = attempt - 1;
        let power = if exponent >= 1024 {
            f64::INFINITY
        } else {
            2.0_f64.powi(exponent as i32)
        };
        Self(base.get() as f64 * power)
    }
}
/// Rejected producer, observer or sleeper operation. Formatting never exposes its source.
#[derive(Clone)]
pub struct AssistantRetryError {
    source: Option<Arc<dyn Error + Send + Sync>>,
}
impl AssistantRetryError {
    pub fn new() -> Self {
        Self { source: None }
    }
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Some(Arc::new(source)),
        }
    }
}
impl Default for AssistantRetryError {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Debug for AssistantRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AssistantRetryError([REDACTED])")
    }
}
impl fmt::Display for AssistantRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("assistant retry operation failed")
    }
}
impl Error for AssistantRetryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|s| s as _)
    }
}
#[derive(Debug)]
pub enum RetrySleepError {
    Cancelled,
    Failed(AssistantRetryError),
}
/// A trusted producer owns its own request/cancellation behavior.
pub trait AssistantRetryProducer: Send + Sync {
    fn produce<'a>(&'a self) -> BoxFuture<'a, Result<AssistantMessage, AssistantRetryError>>;
}
pub trait RetrySleeper: Send + Sync {
    fn sleep<'a>(
        &'a self,
        delay: RetryDelay,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), RetrySleepError>>;
}
/// Optional observer operations. `max_attempts` is the retry budget, excluding the initial call.
pub trait RetryCallbacks: Send + Sync {
    fn on_retry_scheduled<'a>(
        &'a self,
        attempt: SafeUnsigned,
        max_attempts: SafeUnsigned,
        delay: RetryDelay,
        error_message: &'a str,
    ) -> BoxFuture<'a, Result<(), AssistantRetryError>> {
        let _ = (attempt, max_attempts, delay, error_message);
        Box::pin(async { Ok(()) })
    }
    fn on_retry_attempt_start<'a>(&'a self) -> BoxFuture<'a, Result<(), AssistantRetryError>> {
        Box::pin(async { Ok(()) })
    }
    fn on_retry_finished<'a>(
        &'a self,
        success: bool,
        attempt: SafeUnsigned,
        final_error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AssistantRetryError>> {
        let _ = (success, attempt, final_error);
        Box::pin(async { Ok(()) })
    }
}
/// Clock-backed source timer projection; cancellation belongs to this sleep stage.
pub struct ClockRetrySleeper {
    clock: Arc<dyn Clock>,
}
impl ClockRetrySleeper {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self { clock }
    }
}
impl RetrySleeper for ClockRetrySleeper {
    fn sleep<'a>(
        &'a self,
        delay: RetryDelay,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), RetrySleepError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(RetrySleepError::Cancelled);
            }
            self.clock
                .sleep(delay.node_duration(), c)
                .await
                .map_err(|_| RetrySleepError::Cancelled)
        })
    }
}
/// Preserve source produce/scheduled/sleep/start/finish order and rejection stages.
/// A sleep abort retains the previous response but supplies the inherited mandatory message.
pub async fn retry_assistant_call(
    produce: &dyn AssistantRetryProducer,
    policy: Option<&RetryPolicy>,
    cancellation: &CancellationToken,
    sleeper: &dyn RetrySleeper,
    callbacks: Option<&dyn RetryCallbacks>,
) -> Result<AssistantMessage, AssistantRetryError> {
    let budget = policy
        .filter(|p| p.enabled)
        .map_or(0, |p| p.max_retries.get());
    let mut attempt = SafeUnsigned::new(0).expect("zero is safe");
    loop {
        let mut response = produce.produce().await?;
        let terminal = response.stop_reason != StopReason::Error
            || attempt.get() >= budget
            || !is_retryable_assistant_error(&response);
        if terminal {
            if attempt.get() > 0
                && let Some(observer) = callbacks
            {
                observer
                    .on_retry_finished(
                        !matches!(
                            response.stop_reason,
                            StopReason::Error | StopReason::Aborted
                        ),
                        attempt,
                        (response.stop_reason == StopReason::Error)
                            .then_some(response.error_message.as_deref())
                            .flatten(),
                    )
                    .await?;
            }
            return Ok(response);
        }
        // Existing MaxRetries bounds guarantee both count and increment remain JS-safe.
        attempt = SafeUnsigned::new(attempt.get() + 1).expect("admitted retry budget is safe");
        let policy = policy.expect("positive retry budget requires policy");
        let delay = RetryDelay::exponential(policy.base_delay_ms, attempt.get());
        let message = response
            .error_message
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("Unknown error");
        if let Some(observer) = callbacks {
            observer
                .on_retry_scheduled(
                    attempt,
                    SafeUnsigned::new(budget).expect("retry budget is safe"),
                    delay,
                    message,
                )
                .await?;
        }
        if let Err(error) = sleeper.sleep(delay, cancellation).await {
            if let Some(observer) = callbacks {
                observer
                    .on_retry_finished(false, attempt, Some(message))
                    .await?;
            }
            match error {
                RetrySleepError::Cancelled => {
                    response.stop_reason = StopReason::Aborted;
                    response.error_message = Some("Retry backoff aborted".into());
                    return Ok(response);
                }
                RetrySleepError::Failed(error) => return Err(error),
            }
        }
        if let Some(observer) = callbacks {
            observer.on_retry_attempt_start().await?;
        }
    }
}
