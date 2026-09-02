//! Object-safe authentication ports and cancellation-aware provider resolution.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use crate::auth::{
    ApiKeyCredential, ApiKeyCredentialType, AuthCheck, AuthEvent, AuthPrompt, AuthResult,
    Credential, ModelAuth, ModelsErrorCode, OAuthCredential, SecretString,
};
use crate::credential_store::{
    CredentialMutationValue, CredentialStore, CredentialStoreError, CredentialStoreErrorCode,
};
use crate::error::ModelsError;
use crate::id::ProviderId;
use crate::json::SafeUnsigned;
use crate::options::ProviderEnv;
use crate::runtime::{BoxFuture, CancellationToken, Clock};

const DEFAULT_OAUTH_MINIMUM_VALIDITY_MS: u64 = 5 * 60 * 1_000;
const DEFAULT_OAUTH_REFRESH_TIMEOUT_MS: u64 = 15_000;

macro_rules! leaf_auth_error {
    ($name:ident, $message:literal) => {
        #[doc = concat!("Redacted ", $message, ".")]
        pub struct $name {
            source: Option<Box<dyn Error + Send + Sync + 'static>>,
        }

        #[allow(clippy::new_without_default)] // Revision 31 freezes exactly new/with_source.
        impl $name {
            /// Creates a failure without a backend source.
            pub const fn new() -> Self {
                Self { source: None }
            }

            /// Creates a failure while retaining a source for explicit diagnosis.
            pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
                Self {
                    source: Some(Box::new(source)),
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("source_present", &self.source.is_some())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str($message)
            }
        }

        impl Error for $name {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                self.source
                    .as_deref()
                    .map(|source| source as &(dyn Error + 'static))
            }
        }
    };
}

leaf_auth_error!(AuthContextError, "authentication context failed");
leaf_auth_error!(AuthInteractionError, "authentication interaction failed");
leaf_auth_error!(AuthProviderError, "authentication provider failed");

/// Process-local environment and file-existence access for authentication.
pub trait AuthContext: Send + Sync {
    /// Reads one ambient environment value.
    fn env<'a>(
        &'a self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>>;

    /// Checks one host path without exposing filesystem access to portable core.
    fn file_exists<'a>(
        &'a self,
        path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>>;
}

/// Process-local user interaction used by concrete login flows.
pub trait AuthInteraction: Send + Sync {
    /// Prompts for one value.
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>>;

    /// Emits a synchronous login progress event.
    fn notify(&self, event: AuthEvent) -> Result<(), AuthInteractionError>;
}

/// Borrowed API-key resolution inputs.
pub struct ApiKeyAuthInput<'a> {
    /// Authentication environment capability.
    pub context: &'a dyn AuthContext,
    /// Optional explicit or stored API-key credential.
    pub credential: Option<&'a ApiKeyCredential>,
    /// Cancellation scoped to this provider operation.
    pub cancellation: &'a CancellationToken,
}

/// Object-safe API-key authentication capability.
pub trait ApiKeyAuth: Send + Sync {
    /// Human-readable authentication method name.
    fn name(&self) -> &str;

    /// Optionally performs interactive setup.
    fn login<'a>(
        &'a self,
        _interaction: &'a dyn AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> Option<BoxFuture<'a, Result<ApiKeyCredential, AuthProviderError>>> {
        None
    }

    /// Optionally performs a side-effect-free availability check.
    fn check<'a>(
        &'a self,
        _input: ApiKeyAuthInput<'a>,
    ) -> Option<BoxFuture<'a, Result<Option<AuthCheck>, AuthProviderError>>> {
        None
    }

    /// Resolves request authentication from explicit, stored, or ambient input.
    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>>;
}

/// Object-safe OAuth authentication capability.
pub trait OAuthAuth: Send + Sync {
    /// Human-readable authentication method name.
    fn name(&self) -> &str;

    /// Returns whether the flow is backed by a provider subscription.
    fn is_subscription(&self) -> bool {
        false
    }

    /// Optional selector label for the login method.
    fn login_label(&self) -> Option<&str> {
        None
    }

    /// Performs interactive login.
    fn login<'a>(
        &'a self,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>>;

    /// Exchanges an expired or near-expiry credential.
    fn refresh<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>>;

    /// Derives request authentication without mutating the credential.
    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>>;
}

/// Invalid provider-auth capability configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProviderAuthError {
    /// Neither an API-key capability nor an OAuth capability was supplied.
    NoCapabilities,
}

impl fmt::Display for ProviderAuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider auth requires at least one capability")
    }
}

impl Error for ProviderAuthError {}

/// Validated provider authentication capabilities.
///
/// This process-local value intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ProviderAuth>();
/// ```
#[derive(Clone)]
pub struct ProviderAuth {
    api_key: Option<Arc<dyn ApiKeyAuth>>,
    oauth: Option<Arc<dyn OAuthAuth>>,
}

impl ProviderAuth {
    /// Validates and creates provider authentication capabilities.
    pub fn new(
        api_key: Option<Arc<dyn ApiKeyAuth>>,
        oauth: Option<Arc<dyn OAuthAuth>>,
    ) -> Result<Self, ProviderAuthError> {
        if api_key.is_none() && oauth.is_none() {
            Err(ProviderAuthError::NoCapabilities)
        } else {
            Ok(Self { api_key, oauth })
        }
    }

    /// Returns the optional API-key capability.
    pub fn api_key(&self) -> Option<Arc<dyn ApiKeyAuth>> {
        self.api_key.clone()
    }

    /// Returns the optional OAuth capability.
    pub fn oauth(&self) -> Option<Arc<dyn OAuthAuth>> {
        self.oauth.clone()
    }
}

impl fmt::Debug for ProviderAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderAuth")
            .field("api_key_present", &self.api_key.is_some())
            .field("oauth_present", &self.oauth.is_some())
            .finish()
    }
}

/// Per-request authentication overrides.
///
/// Debug output reports presence only for secret-bearing values, and the type
/// intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::AuthResolutionOverrides>();
/// ```
#[derive(Default)]
pub struct AuthResolutionOverrides {
    /// Explicit request API key.
    pub api_key: Option<SecretString>,
    /// Explicit request environment map; an empty map remains distinct from absence.
    pub env: Option<ProviderEnv>,
    /// Explicit minimum OAuth validity in milliseconds.
    pub min_oauth_validity_ms: Option<SafeUnsigned>,
}

impl fmt::Debug for AuthResolutionOverrides {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthResolutionOverrides")
            .field("api_key_present", &self.api_key.is_some())
            .field("env", &self.env)
            .field("min_oauth_validity_ms", &self.min_oauth_validity_ms)
            .finish()
    }
}

/// Failure resolving provider authentication.
///
/// This runtime-only error intentionally does not implement Serde.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::AuthResolutionError>();
/// ```
#[non_exhaustive]
pub enum AuthResolutionError {
    /// Caller cancellation won before an admitted commit completed.
    Cancelled,
    /// The credential backend cannot prove the provider's state.
    OutcomeUnknown,
    /// A fixed-category Models runtime failure.
    Models(ModelsError),
}

impl fmt::Debug for AuthResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("AuthResolutionError");
        match self {
            Self::Cancelled => {
                debug.field("category", &"Cancelled");
            }
            Self::OutcomeUnknown => {
                debug.field("category", &"OutcomeUnknown");
            }
            Self::Models(error) => {
                debug.field("category", &"Models").field("error", error);
            }
        }
        debug.finish()
    }
}

impl fmt::Display for AuthResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("authentication resolution cancelled"),
            Self::OutcomeUnknown => formatter.write_str("credential outcome unknown"),
            Self::Models(error) => error.fmt(formatter),
        }
    }
}

impl Error for AuthResolutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Models(error) => Some(error),
            Self::Cancelled | Self::OutcomeUnknown => None,
        }
    }
}

struct OverlayAuthContext<'a> {
    base: &'a dyn AuthContext,
    overrides: &'a ProviderEnv,
}

impl AuthContext for OverlayAuthContext<'_> {
    fn env<'a>(
        &'a self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        if let Some(value) = self
            .overrides
            .as_map()
            .get(name)
            .filter(|value| !value.is_empty())
        {
            let value = value.clone();
            Box::pin(async move { Ok(Some(value)) })
        } else {
            self.base.env(name, cancellation)
        }
    }

    fn file_exists<'a>(
        &'a self,
        path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        self.base.file_exists(path, cancellation)
    }
}

enum CancellationRace<T> {
    Cancelled,
    Completed(T),
}

async fn cancellation_first<T>(
    cancellation: &CancellationToken,
    linked: &CancellationToken,
    mut future: BoxFuture<'_, T>,
) -> Result<T, AuthResolutionError> {
    let mut cancelled = Box::pin(cancellation.cancelled());
    let outcome = poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() || cancellation.is_cancelled() {
            return Poll::Ready(CancellationRace::Cancelled);
        }
        if let Poll::Ready(value) = future.as_mut().poll(context) {
            if cancellation.is_cancelled() {
                Poll::Ready(CancellationRace::Cancelled)
            } else {
                Poll::Ready(CancellationRace::Completed(value))
            }
        } else {
            Poll::Pending
        }
    })
    .await;
    match outcome {
        CancellationRace::Cancelled => {
            linked.cancel();
            drop(future);
            Err(AuthResolutionError::Cancelled)
        }
        CancellationRace::Completed(value) => {
            drop(future);
            Ok(value)
        }
    }
}

fn map_store_error(error: CredentialStoreError) -> AuthResolutionError {
    match error.code() {
        CredentialStoreErrorCode::Cancelled => AuthResolutionError::Cancelled,
        CredentialStoreErrorCode::OutcomeUnknown => AuthResolutionError::OutcomeUnknown,
        CredentialStoreErrorCode::Failed
        | CredentialStoreErrorCode::RevisionExhausted
        | CredentialStoreErrorCode::ReconciliationNotRequired => {
            AuthResolutionError::Models(ModelsError::with_source(ModelsErrorCode::Auth, error))
        }
    }
}

async fn read_credential(
    credentials: &dyn CredentialStore,
    provider: &ProviderId,
    cancellation: &CancellationToken,
) -> Result<Option<crate::credential_store::VersionedCredential>, AuthResolutionError> {
    let linked = CancellationToken::linked(std::slice::from_ref(cancellation));
    let result =
        cancellation_first(cancellation, &linked, credentials.read(provider, &linked)).await?;
    result.map_err(map_store_error)
}

async fn resolve_api_key(
    api_key: &dyn ApiKeyAuth,
    context: &dyn AuthContext,
    credential: Option<&ApiKeyCredential>,
    cancellation: &CancellationToken,
) -> Result<Option<AuthResult>, AuthResolutionError> {
    let linked = CancellationToken::linked(std::slice::from_ref(cancellation));
    let result = cancellation_first(
        cancellation,
        &linked,
        api_key.resolve(ApiKeyAuthInput {
            context,
            credential,
            cancellation: &linked,
        }),
    )
    .await?;
    result.map_err(|error| {
        AuthResolutionError::Models(ModelsError::with_source(ModelsErrorCode::Auth, error))
    })
}

fn with_request_env(
    credential: &ApiKeyCredential,
    request_env: Option<&ProviderEnv>,
) -> ApiKeyCredential {
    let Some(request_env) = request_env else {
        return credential.clone();
    };
    let mut values = credential
        .env
        .as_ref()
        .map_or_else(BTreeMap::new, |env| env.as_map().clone());
    values.extend(
        request_env
            .as_map()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
    );
    let mut credential = credential.clone();
    credential.env = Some(ProviderEnv::new(values));
    credential
}

fn expires_soon(credential: &OAuthCredential, clock: &dyn Clock, minimum: u64) -> bool {
    clock.now_unix_ms().saturating_add(minimum) >= credential.credentials.expires.get()
}

enum RefreshRace {
    Cancelled,
    Deadline,
    Provider(Result<OAuthCredential, AuthProviderError>),
}

async fn refresh_oauth(
    oauth: &dyn OAuthAuth,
    credential: &OAuthCredential,
    clock: &dyn Clock,
    cancellation: &CancellationToken,
) -> Result<OAuthCredential, AuthResolutionError> {
    let linked = CancellationToken::linked(std::slice::from_ref(cancellation));
    let mut cancelled = Box::pin(cancellation.cancelled());
    let mut deadline = clock.sleep(
        Duration::from_millis(DEFAULT_OAUTH_REFRESH_TIMEOUT_MS),
        &linked,
    );
    let mut provider = oauth.refresh(credential, &linked);
    let outcome = poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() || cancellation.is_cancelled() {
            return Poll::Ready(RefreshRace::Cancelled);
        }
        if deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(RefreshRace::Deadline);
        }
        if let Poll::Ready(result) = provider.as_mut().poll(context) {
            if cancellation.is_cancelled() {
                Poll::Ready(RefreshRace::Cancelled)
            } else {
                Poll::Ready(RefreshRace::Provider(result))
            }
        } else {
            Poll::Pending
        }
    })
    .await;
    match outcome {
        RefreshRace::Cancelled => {
            linked.cancel();
            drop(provider);
            drop(deadline);
            Err(AuthResolutionError::Cancelled)
        }
        RefreshRace::Deadline => {
            linked.cancel();
            drop(provider);
            drop(deadline);
            Err(AuthResolutionError::Models(ModelsError::new(
                ModelsErrorCode::OAuth,
            )))
        }
        RefreshRace::Provider(result) => {
            drop(provider);
            drop(deadline);
            result.map_err(|error| {
                AuthResolutionError::Models(ModelsError::with_source(ModelsErrorCode::OAuth, error))
            })
        }
    }
}

async fn derive_oauth_auth(
    oauth: &dyn OAuthAuth,
    credential: &OAuthCredential,
    cancellation: &CancellationToken,
) -> Result<Option<AuthResult>, AuthResolutionError> {
    let linked = CancellationToken::linked(std::slice::from_ref(cancellation));
    let result =
        cancellation_first(cancellation, &linked, oauth.to_auth(credential, &linked)).await?;
    let auth = result.map_err(|error| {
        AuthResolutionError::Models(ModelsError::with_source(ModelsErrorCode::OAuth, error))
    })?;
    Ok(Some(AuthResult {
        auth,
        env: None,
        source: Some("OAuth".to_owned()),
    }))
}

async fn resolve_stored_oauth(
    provider: &ProviderId,
    oauth: &dyn OAuthAuth,
    credentials: &dyn CredentialStore,
    stored: OAuthCredential,
    clock: &dyn Clock,
    explicit_minimum: Option<SafeUnsigned>,
    cancellation: &CancellationToken,
) -> Result<Option<AuthResult>, AuthResolutionError> {
    let minimum = explicit_minimum
        .map_or(0, SafeUnsigned::get)
        .max(DEFAULT_OAUTH_MINIMUM_VALIDITY_MS);
    let credential = if expires_soon(&stored, clock, minimum) {
        let linked = CancellationToken::linked(std::slice::from_ref(cancellation));
        let lease = cancellation_first(
            cancellation,
            &linked,
            credentials.begin_mutation(provider, &linked),
        )
        .await?
        .map_err(map_store_error)?;
        let Some(Credential::OAuth(current)) =
            lease.current().map(|current| current.credential.clone())
        else {
            drop(lease);
            return Ok(None);
        };
        if !expires_soon(&current, clock, minimum) {
            drop(lease);
            current
        } else {
            let refreshed = match refresh_oauth(oauth, &current, clock, cancellation).await {
                Ok(refreshed) => refreshed,
                Err(error) => {
                    drop(lease);
                    return Err(error);
                }
            };
            if cancellation.is_cancelled() {
                drop(lease);
                return Err(AuthResolutionError::Cancelled);
            }
            let committed = lease
                .commit(CredentialMutationValue::Set(Credential::OAuth(refreshed)))
                .await
                .map_err(map_store_error)?;
            if cancellation.is_cancelled() {
                return Err(AuthResolutionError::Cancelled);
            }
            let Some(Credential::OAuth(committed)) = committed.map(|current| current.credential)
            else {
                return Ok(None);
            };
            if explicit_minimum.is_some() && expires_soon(&committed, clock, minimum) {
                return Err(AuthResolutionError::Models(ModelsError::new(
                    ModelsErrorCode::OAuth,
                )));
            }
            committed
        }
    } else {
        stored
    };
    derive_oauth_auth(oauth, &credential, cancellation).await
}

/// Resolves one provider's explicit, stored, or ambient authentication.
pub fn resolve_provider_auth<'a>(
    provider_id: &'a ProviderId,
    auth: &'a ProviderAuth,
    credentials: &'a dyn CredentialStore,
    context: &'a dyn AuthContext,
    clock: &'a dyn Clock,
    overrides: AuthResolutionOverrides,
    cancellation: &'a CancellationToken,
) -> BoxFuture<'a, Result<Option<AuthResult>, AuthResolutionError>> {
    Box::pin(async move {
        if cancellation.is_cancelled() {
            return Err(AuthResolutionError::Cancelled);
        }
        let AuthResolutionOverrides {
            api_key: explicit_api_key,
            env: request_env,
            min_oauth_validity_ms,
        } = overrides;
        let overlay = request_env.as_ref().map(|overrides| OverlayAuthContext {
            base: context,
            overrides,
        });
        let request_context: &dyn AuthContext = overlay
            .as_ref()
            .map_or(context, |overlay| overlay as &dyn AuthContext);
        let api_key = auth.api_key();
        let oauth = auth.oauth();

        if let (Some(api_key), Some(explicit_api_key)) = (api_key.as_ref(), explicit_api_key) {
            let credential = ApiKeyCredential {
                credential_type: ApiKeyCredentialType::ApiKey,
                key: Some(explicit_api_key),
                env: request_env.clone(),
            };
            return resolve_api_key(
                api_key.as_ref(),
                request_context,
                Some(&credential),
                cancellation,
            )
            .await;
        }

        let stored = read_credential(credentials, provider_id, cancellation).await?;
        if let Some(stored) = stored {
            return match stored.credential {
                Credential::ApiKey(credential) => {
                    let Some(api_key) = api_key else {
                        return Ok(None);
                    };
                    let credential = with_request_env(&credential, request_env.as_ref());
                    resolve_api_key(
                        api_key.as_ref(),
                        request_context,
                        Some(&credential),
                        cancellation,
                    )
                    .await
                }
                Credential::OAuth(credential) => {
                    let Some(oauth) = oauth else {
                        return Ok(None);
                    };
                    resolve_stored_oauth(
                        provider_id,
                        oauth.as_ref(),
                        credentials,
                        credential,
                        clock,
                        min_oauth_validity_ms,
                        cancellation,
                    )
                    .await
                }
            };
        }

        match api_key {
            Some(api_key) => {
                resolve_api_key(api_key.as_ref(), request_context, None, cancellation).await
            }
            None => Ok(None),
        }
    })
}
