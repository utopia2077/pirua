//! Instance-scoped monotonic UUIDv7 generation.

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::Clock;

const MAX_TIMESTAMP: u64 = (1_u64 << 48) - 1;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Entropy capability used by one UUIDv7 generator instance.
pub trait UuidV7Entropy: Send + Sync {
    /// Returns 16 random bytes or a redacted diagnostic error.
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError>;
}

/// Redacted entropy-provider failure.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::UuidV7EntropyError>();
/// ```
pub struct UuidV7EntropyError {
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

#[allow(clippy::new_without_default)] // The revision-36 public API is intentionally exact.
impl UuidV7EntropyError {
    /// Creates an entropy failure without a backend source.
    pub const fn new() -> Self {
        Self { source: None }
    }

    /// Creates an entropy failure retaining a source for explicit diagnosis.
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Debug for UuidV7EntropyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UuidV7EntropyError")
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for UuidV7EntropyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UUIDv7 entropy unavailable")
    }
}

impl Error for UuidV7EntropyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Stable UUIDv7 generation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UuidV7ErrorCode {
    /// Entropy acquisition failed.
    Entropy,
    /// The injected clock exceeded the RFC 9562 48-bit timestamp field.
    TimestampOutOfRange,
    /// Sequence rollover could not advance an already-maximal timestamp.
    TimestampExhausted,
}

/// Redacted UUIDv7 generation failure.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::UuidV7Error>();
/// ```
pub struct UuidV7Error {
    code: UuidV7ErrorCode,
    source: Option<UuidV7EntropyError>,
}

impl UuidV7Error {
    /// Returns the stable public error category.
    pub const fn code(&self) -> UuidV7ErrorCode {
        self.code
    }

    const fn timestamp(code: UuidV7ErrorCode) -> Self {
        Self { code, source: None }
    }
}

impl fmt::Debug for UuidV7Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UuidV7Error")
            .field("code", &self.code)
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for UuidV7Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.code {
            UuidV7ErrorCode::Entropy => "UUIDv7 entropy failed",
            UuidV7ErrorCode::TimestampOutOfRange => "UUIDv7 timestamp is out of range",
            UuidV7ErrorCode::TimestampExhausted => "UUIDv7 timestamp is exhausted",
        })
    }
}

impl Error for UuidV7Error {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

struct GeneratorState {
    last_timestamp: Option<u64>,
    sequence: u32,
}

/// Instance-owned UUIDv7 clock, entropy, and monotonic allocation state.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::UuidV7Generator>();
/// ```
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::UuidV7Generator>();
/// ```
pub struct UuidV7Generator {
    clock: Arc<dyn Clock>,
    entropy: Arc<dyn UuidV7Entropy>,
    state: Mutex<GeneratorState>,
}

impl UuidV7Generator {
    /// Creates an independent UUID authority.
    pub fn new(clock: Arc<dyn Clock>, entropy: Arc<dyn UuidV7Entropy>) -> Self {
        Self {
            clock,
            entropy,
            state: Mutex::new(GeneratorState {
                last_timestamp: None,
                sequence: 0,
            }),
        }
    }

    /// Generates one lowercase RFC 9562 UUIDv7.
    pub fn generate(&self) -> Result<String, UuidV7Error> {
        let random = self.entropy.random_bytes().map_err(|source| UuidV7Error {
            code: UuidV7ErrorCode::Entropy,
            source: Some(source),
        })?;
        let timestamp = self.clock.now_unix_ms();
        if timestamp > MAX_TIMESTAMP {
            return Err(UuidV7Error::timestamp(UuidV7ErrorCode::TimestampOutOfRange));
        }

        let (allocated_timestamp, sequence) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let next = match state.last_timestamp {
                None => (timestamp, sequence_seed(&random)),
                Some(last) if timestamp > last => (timestamp, sequence_seed(&random)),
                Some(last) => match state.sequence.checked_add(1) {
                    Some(sequence) => (last, sequence),
                    None => {
                        let Some(timestamp) = last.checked_add(1) else {
                            return Err(UuidV7Error::timestamp(
                                UuidV7ErrorCode::TimestampExhausted,
                            ));
                        };
                        if timestamp > MAX_TIMESTAMP {
                            return Err(UuidV7Error::timestamp(
                                UuidV7ErrorCode::TimestampExhausted,
                            ));
                        }
                        (timestamp, 0)
                    }
                },
            };
            state.last_timestamp = Some(next.0);
            state.sequence = next.1;
            next
        };
        Ok(format_uuid(allocated_timestamp, sequence, &random))
    }
}

impl fmt::Debug for UuidV7Generator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UuidV7Generator([PROCESS-LOCAL])")
    }
}

fn sequence_seed(random: &[u8; 16]) -> u32 {
    u32::from_be_bytes([random[6], random[7], random[8], random[9]])
}

fn format_uuid(timestamp: u64, sequence: u32, random: &[u8; 16]) -> String {
    let mut bytes = [0_u8; 16];
    let timestamp_bytes = timestamp.to_be_bytes();
    bytes[..6].copy_from_slice(&timestamp_bytes[2..]);
    bytes[6] = 0x70 | ((sequence >> 28) as u8 & 0x0f);
    bytes[7] = (sequence >> 20) as u8;
    bytes[8] = 0x80 | ((sequence >> 14) as u8 & 0x3f);
    bytes[9] = (sequence >> 6) as u8;
    bytes[10] = ((sequence as u8 & 0x3f) << 2) | (random[10] & 0x03);
    bytes[11..].copy_from_slice(&random[11..]);

    let mut output = String::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Mutex, Weak};
    use std::time::Duration;

    use crate::{BoxFuture, CancellationToken, Cancelled};

    use super::*;

    struct SequenceClock(Mutex<VecDeque<u64>>);

    impl Clock for SequenceClock {
        fn now_unix_ms(&self) -> u64 {
            self.0
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .pop_front()
                .expect("clock fixture has a value")
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(std::future::pending())
        }
    }

    struct FixedEntropy([u8; 16]);

    impl UuidV7Entropy for FixedEntropy {
        fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
            Ok(self.0)
        }
    }

    struct LockProbe {
        generator: Mutex<Option<Weak<UuidV7Generator>>>,
    }

    impl LockProbe {
        fn assert_state_unlocked(&self) {
            let generator = self
                .generator
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_ref()
                .and_then(Weak::upgrade)
                .expect("probe generator is installed");
            assert!(generator.state.try_lock().is_ok());
        }
    }

    struct ProbeClock(Arc<LockProbe>);

    impl Clock for ProbeClock {
        fn now_unix_ms(&self) -> u64 {
            self.0.assert_state_unlocked();
            1
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(std::future::pending())
        }
    }

    struct ProbeEntropy(Arc<LockProbe>);

    impl UuidV7Entropy for ProbeEntropy {
        fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
            self.0.assert_state_unlocked();
            Ok([0; 16])
        }
    }

    #[test]
    fn entropy_and_clock_run_before_the_allocation_lock() {
        let probe = Arc::new(LockProbe {
            generator: Mutex::new(None),
        });
        let generator = Arc::new(UuidV7Generator::new(
            Arc::new(ProbeClock(Arc::clone(&probe))),
            Arc::new(ProbeEntropy(Arc::clone(&probe))),
        ));
        *probe
            .generator
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::downgrade(&generator));
        assert!(generator.generate().is_ok());
    }

    #[test]
    fn private_rollover_and_exhaustion_leave_exact_state() {
        let generator = UuidV7Generator::new(
            Arc::new(SequenceClock(Mutex::new(VecDeque::from([
                10,
                9,
                MAX_TIMESTAMP,
            ])))),
            Arc::new(FixedEntropy([0; 16])),
        );
        {
            let mut state = generator
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.last_timestamp = Some(10);
            state.sequence = u32::MAX;
        }
        let rolled = generator.generate().expect("rollover advances time");
        assert!(rolled.starts_with("00000000-000b-7000-8000-"));

        {
            let mut state = generator
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.last_timestamp = Some(MAX_TIMESTAMP);
            state.sequence = u32::MAX;
        }
        let before = {
            let state = generator
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            (state.last_timestamp, state.sequence)
        };
        let error = generator
            .generate()
            .expect_err("maximum state is exhausted");
        assert_eq!(error.code(), UuidV7ErrorCode::TimestampExhausted);
        let after = {
            let state = generator
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            (state.last_timestamp, state.sequence)
        };
        assert_eq!(after, before);
    }
}
