//! Configured per-invocation leaves; no credential/configuration discovery.
use pi_ai_core::*;
use pi_ai_transport::{SmithyRetryClass, SmithyRetryGeneration, SmithyRetryMode};
use std::{fmt, sync::Arc};

pub struct BedrockLeafError {
    pub failure: ModelsError,
    pub retry_class: SmithyRetryClass,
}
impl BedrockLeafError {
    pub fn new(failure: ModelsError) -> Self {
        Self {
            failure,
            retry_class: SmithyRetryClass::ClientError,
        }
    }
}
impl fmt::Debug for BedrockLeafError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockLeafError([REDACTED])")
    }
}
impl fmt::Display for BedrockLeafError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("configured Bedrock operation failed")
    }
}
impl std::error::Error for BedrockLeafError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.failure)
    }
}
pub trait BedrockConfigLeaf<T>: Send + Sync {
    fn resolve<'a>(
        &'a self,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<T, BedrockLeafError>>;
}
pub enum BedrockSetting<T> {
    Value(T),
    Leaf(Arc<dyn BedrockConfigLeaf<T>>),
    Missing,
}
impl<T: Clone> Clone for BedrockSetting<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Value(v) => Self::Value(v.clone()),
            Self::Leaf(v) => Self::Leaf(v.clone()),
            Self::Missing => Self::Missing,
        }
    }
}
impl<T> fmt::Debug for BedrockSetting<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockSetting([REDACTED])")
    }
}
impl<T: Clone + Send + Sync> BedrockSetting<T> {
    pub(super) async fn get(&self, runtime: &RequestRuntime) -> Result<T, BedrockLeafError> {
        if runtime.cancellation.is_cancelled() {
            return Err(BedrockLeafError::new(ModelsError::new(
                ModelsErrorCode::Auth,
            )));
        }
        match self {
            Self::Value(v) => Ok(v.clone()),
            Self::Missing => Err(BedrockLeafError::new(ModelsError::new(
                ModelsErrorCode::Auth,
            ))),
            Self::Leaf(leaf) => {
                crate::api::runtime::cancellable(leaf.resolve(runtime), &runtime.cancellation)
                    .await
                    .map_err(|_| BedrockLeafError::new(ModelsError::new(ModelsErrorCode::Auth)))?
            }
        }
    }
}
#[derive(Clone)]
pub struct BedrockCredentials {
    pub access_key_id: SecretString,
    pub secret_access_key: SecretString,
    pub session_token: Option<SecretString>,
    pub expiration_unix_ms: Option<FiniteF64>,
}
#[derive(Clone)]
pub struct BedrockToken {
    pub token: SecretString,
    pub expiration_unix_ms: Option<FiniteF64>,
}
macro_rules! redacted{($($ty:ty),*)=>{$(impl fmt::Debug for $ty{fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{f.write_str(concat!(stringify!($ty),"([REDACTED])"))}})*};}
redacted!(BedrockCredentials, BedrockToken, BedrockSettings);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BedrockAuthScheme {
    SigV4,
    Bearer,
}
/// Effective SDK values or independently delayed leaves. S05 supplies captured
/// environment/profile/defaults-mode interpretation, not this transport adapter.
#[derive(Clone)]
pub struct BedrockSettings {
    pub region: BedrockSetting<String>,
    pub credentials: BedrockSetting<BedrockCredentials>,
    pub token: BedrockSetting<BedrockToken>,
    pub auth_scheme_preference: BedrockSetting<Vec<BedrockAuthScheme>>,
    pub use_fips_endpoint: BedrockSetting<bool>,
    pub use_dualstack_endpoint: BedrockSetting<bool>,
    pub endpoint_override: BedrockSetting<Option<Arc<RequestEndpoint>>>,
    pub retry_mode: BedrockSetting<SmithyRetryMode>,
    pub max_attempts: BedrockSetting<FiniteF64>,
    pub user_agent_app_id: BedrockSetting<Option<String>>,
    /// Captured platform sections. No platform identity or ambient lookup occurs.
    pub user_agent: Vec<(String, Option<String>)>,
    pub retry_generation: SmithyRetryGeneration,
}
impl Default for BedrockSettings {
    fn default() -> Self {
        Self {
            region: BedrockSetting::Missing,
            credentials: BedrockSetting::Missing,
            token: BedrockSetting::Missing,
            auth_scheme_preference: BedrockSetting::Value(Vec::new()),
            use_fips_endpoint: BedrockSetting::Value(false),
            use_dualstack_endpoint: BedrockSetting::Value(false),
            endpoint_override: BedrockSetting::Value(None),
            retry_mode: BedrockSetting::Value(SmithyRetryMode::Standard),
            max_attempts: BedrockSetting::Value(FiniteF64::new(3.0).expect("constant")),
            user_agent_app_id: BedrockSetting::Value(None),
            user_agent: vec![
                ("aws-sdk-js".into(), Some("3.1048.0".into())),
                ("api/bedrock-runtime".into(), Some("3.1048.0".into())),
            ],
            retry_generation: SmithyRetryGeneration::Legacy,
        }
    }
}
pub struct BedrockConfigKey<'a> {
    pub profile: Option<&'a str>,
    pub ambient: &'a ProviderEnv,
}
impl fmt::Debug for BedrockConfigKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockConfigKey([REDACTED])")
    }
}
pub trait BedrockConfigProvider: Send + Sync {
    /// Bounded synchronous no-I/O session construction, once per invocation.
    /// Missing required settings fail only at their later consuming stage.
    fn create_session(&self, key: BedrockConfigKey<'_>) -> Result<BedrockSettings, ModelsError>;
}
pub trait BedrockEntropy: Send + Sync {
    /// Fresh bytes for the one invocation UUID, consumed at initial retry admission.
    fn invocation_bytes(&self) -> Result<[u8; 16], ModelsError>;
    /// Finite [0,1) entropy, consumed only for an eligible retry.
    fn retry_jitter(&self) -> Result<f64, ModelsError>;
}
pub(super) trait Identity: Clone {
    fn expires(&self) -> Option<FiniteF64>;
}
impl Identity for BedrockCredentials {
    fn expires(&self) -> Option<FiniteF64> {
        self.expiration_unix_ms
    }
}
impl Identity for BedrockToken {
    fn expires(&self) -> Option<FiniteF64> {
        self.expiration_unix_ms
    }
}
/// A single invocation's serial resolver: the mutable borrow excludes overlapping
/// leaf starts, retaining one identity across initial auth and signing retries.
pub(super) struct Memo<T> {
    pub setting: BedrockSetting<T>,
    value: Option<T>,
    constant: bool,
}
impl<T: Identity + Send + Sync> Memo<T> {
    pub fn new(setting: BedrockSetting<T>) -> Self {
        Self {
            setting,
            value: None,
            constant: false,
        }
    }
    async fn refresh(&mut self, runtime: &RequestRuntime) -> Result<(), BedrockLeafError> {
        self.value = Some(self.setting.get(runtime).await?);
        self.constant = false;
        Ok(())
    }
    pub async fn get(&mut self, runtime: &RequestRuntime) -> Result<T, BedrockLeafError> {
        if self.value.is_none() {
            self.refresh(runtime).await?;
        }
        let value = self.value.as_ref().expect("resolved identity");
        if self.constant {
            return Ok(value.clone());
        }
        if let Some(expiry) = value.expires() {
            if expiry.get() - (runtime.clock.now_unix_ms() as f64) < 300000.0 {
                self.refresh(runtime).await?;
            }
        } else {
            self.constant = true;
        }
        Ok(self.value.as_ref().expect("resolved identity").clone())
    }
}
