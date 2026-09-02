use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pi_ai_core::{
    ApiKeyAuth, ApiKeyAuthInput, ApiKeyCredential, ApiKeyCredentialType, AuthContext,
    AuthContextError, AuthProviderError, AuthResolutionError, AuthResolutionOverrides, AuthResult,
    AuthType, BoxFuture, CancellationToken, Cancelled, Clock, Credential, CredentialMutationValue,
    CredentialStore, CredentialStoreErrorCode, InMemoryCredentialStore, InMemoryModelsStore,
    ModelAuth, ModelHeaderPolicy, ModelsErrorCode, ModelsStore, ModelsStoreEntry,
    ModelsStoreErrorCode, OAuthAuth, OAuthCredential, OAuthCredentialType, OAuthCredentials,
    OAuthExtraFields, ProviderAuth, ProviderEnv, ProviderId, RawModelsStoreEntry, SafeUnsigned,
    SecretString, resolve_provider_auth,
};

fn poll_with<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match poll_with(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future must already be ready"),
    }
}

fn provider(name: &str) -> ProviderId {
    ProviderId::new(name).expect("nonempty provider")
}

fn api_credential(value: &str) -> Credential {
    Credential::ApiKey(ApiKeyCredential {
        credential_type: ApiKeyCredentialType::ApiKey,
        key: Some(SecretString::new(value)),
        env: None,
    })
}

fn oauth_credential(access: &str, expires: u64) -> OAuthCredential {
    OAuthCredential {
        credential_type: OAuthCredentialType::OAuth,
        credentials: OAuthCredentials {
            refresh: SecretString::new("refresh-secret"),
            access: SecretString::new(access),
            expires: SafeUnsigned::new(expires).expect("safe expiry"),
            extra: OAuthExtraFields::new(BTreeMap::new()).expect("empty extras"),
        },
    }
}

fn set_credential(
    store: &dyn CredentialStore,
    provider: &ProviderId,
    credential: Credential,
) -> pi_ai_core::VersionedCredential {
    let token = CancellationToken::new();
    let lease = ready(store.begin_mutation(provider, &token)).expect("lease acquired");
    ready(lease.commit(CredentialMutationValue::Set(credential)))
        .expect("commit succeeds")
        .expect("set returns a value")
}

#[derive(Default)]
struct TestContext {
    values: BTreeMap<String, String>,
}

impl TestContext {
    fn with(values: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
        }
    }
}

impl AuthContext for TestContext {
    fn env<'a>(
        &'a self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthContextError::new());
            }
            Ok(self.values.get(name).cloned())
        })
    }

    fn file_exists<'a>(
        &'a self,
        _path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(AuthContextError::new());
            }
            Ok(false)
        })
    }
}

struct TestClock {
    now: AtomicU64,
}

impl TestClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
        }
    }
}

impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::Acquire)
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            std::future::pending::<()>().await;
            if cancellation.is_cancelled() {
                Err(Cancelled)
            } else {
                Ok(())
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ApiCall {
    key: Option<String>,
    credential_env: Option<BTreeMap<String, String>>,
    context_value: Option<String>,
}

struct RecordingApiKey {
    ambient_key: Option<String>,
    return_none: bool,
    fail: bool,
    calls: Mutex<Vec<ApiCall>>,
}

impl RecordingApiKey {
    fn new(ambient_key: Option<&str>) -> Self {
        Self {
            ambient_key: ambient_key.map(str::to_owned),
            return_none: false,
            fail: false,
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl ApiKeyAuth for RecordingApiKey {
    fn name(&self) -> &str {
        "recording api key"
    }

    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            let context_value = input
                .context
                .env("ACCOUNT", input.cancellation)
                .await
                .map_err(AuthProviderError::with_source)?;
            let key = input
                .credential
                .and_then(|credential| credential.key.as_ref())
                .map(|key| key.expose_secret().to_owned())
                .or_else(|| self.ambient_key.clone());
            let credential_env = input
                .credential
                .and_then(|credential| credential.env.as_ref())
                .map(|env| env.as_map().clone());
            self.calls
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(ApiCall {
                    key: key.clone(),
                    credential_env,
                    context_value,
                });
            if self.fail {
                return Err(AuthProviderError::new());
            }
            if self.return_none || key.is_none() {
                return Ok(None);
            }
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: key.map(SecretString::new),
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: Some("api-key".to_owned()),
            }))
        })
    }
}

struct RecordingOAuth {
    refreshes: AtomicUsize,
    derived: AtomicUsize,
    refreshed_expires: u64,
    fail_refresh: bool,
}

impl RecordingOAuth {
    fn new(refreshed_expires: u64) -> Self {
        Self {
            refreshes: AtomicUsize::new(0),
            derived: AtomicUsize::new(0),
            refreshed_expires,
            fail_refresh: false,
        }
    }
}

impl OAuthAuth for RecordingOAuth {
    fn name(&self) -> &str {
        "recording oauth"
    }

    fn login<'a>(
        &'a self,
        _interaction: &'a dyn pi_ai_core::AuthInteraction,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Err(AuthProviderError::new()) })
    }

    fn refresh<'a>(
        &'a self,
        _credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            self.refreshes.fetch_add(1, Ordering::AcqRel);
            if self.fail_refresh {
                Err(AuthProviderError::new())
            } else {
                Ok(oauth_credential("refreshed", self.refreshed_expires))
            }
        })
    }

    fn to_auth<'a>(
        &'a self,
        credential: &'a OAuthCredential,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move {
            self.derived.fetch_add(1, Ordering::AcqRel);
            Ok(ModelAuth {
                api_key: Some(credential.credentials.access.clone()),
                headers: None,
                base_url: None,
            })
        })
    }
}

fn resolve(
    provider: &ProviderId,
    auth: &ProviderAuth,
    store: &dyn CredentialStore,
    context: &dyn AuthContext,
    clock: &dyn Clock,
    overrides: AuthResolutionOverrides,
    token: &CancellationToken,
) -> Result<Option<AuthResult>, AuthResolutionError> {
    ready(resolve_provider_auth(
        provider,
        provider_auth(auth),
        store,
        context,
        clock,
        overrides,
        token,
    ))
}

fn provider_auth(auth: &ProviderAuth) -> &ProviderAuth {
    auth
}

#[test]
fn credential_revision_tombstone_order_and_detachment() {
    let store = InMemoryCredentialStore::new();
    let a = provider("a");
    let b = provider("b");
    let token = CancellationToken::new();
    assert!(
        ready(store.read(&a, &token))
            .expect("read succeeds")
            .is_none()
    );

    let first = set_credential(&store, &a, api_credential("first"));
    assert_eq!(first.revision.get(), 1);
    let equal = set_credential(&store, &a, api_credential("first"));
    assert_eq!(equal.revision.get(), 2);

    let lease = ready(store.begin_mutation(&a, &token)).expect("lease acquired");
    let unchanged = ready(lease.commit(CredentialMutationValue::Unchanged))
        .expect("unchanged succeeds")
        .expect("value remains");
    assert_eq!(unchanged.revision.get(), 2);

    set_credential(&store, &b, api_credential("b"));
    let lease = ready(store.begin_mutation(&a, &token)).expect("lease acquired");
    assert!(
        ready(lease.commit(CredentialMutationValue::Delete))
            .expect("delete succeeds")
            .is_none()
    );
    let lease = ready(store.begin_mutation(&a, &token)).expect("lease acquired");
    assert!(
        ready(lease.commit(CredentialMutationValue::Delete))
            .expect("missing delete succeeds")
            .is_none()
    );
    let reinserted = set_credential(&store, &a, api_credential("again"));
    assert_eq!(reinserted.revision.get(), 4);
    assert_eq!(
        ready(store.list(&token))
            .expect("list succeeds")
            .into_iter()
            .map(|info| (info.provider_id.into_string(), info.auth_type))
            .collect::<Vec<_>>(),
        vec![
            ("b".to_owned(), AuthType::ApiKey),
            ("a".to_owned(), AuthType::ApiKey)
        ]
    );

    let mut detached = ready(store.read(&a, &token))
        .expect("read succeeds")
        .expect("value present");
    if let Credential::ApiKey(credential) = &mut detached.credential {
        credential.key = Some(SecretString::new("mutated-detached"));
    }
    let stored = ready(store.read(&a, &token))
        .expect("read succeeds")
        .expect("value present");
    let Credential::ApiKey(stored) = stored.credential else {
        panic!("expected API key")
    };
    assert_eq!(stored.key.expect("key").expose_secret(), "again");
}

#[test]
fn credential_lease_drop_and_commit_cancellation_write_nothing() {
    let store = InMemoryCredentialStore::new();
    let provider = provider("p");
    let token = CancellationToken::new();
    let lease = ready(store.begin_mutation(&provider, &token)).expect("lease acquired");
    drop(lease);
    assert!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .is_none()
    );

    let lease = ready(store.begin_mutation(&provider, &token)).expect("lease acquired");
    let mut commit = lease.commit(CredentialMutationValue::Set(api_credential("secret")));
    token.cancel();
    assert_eq!(
        match poll_with(commit.as_mut()) {
            Poll::Ready(Err(error)) => error.code(),
            _ => panic!("cancelled commit must finish"),
        },
        CredentialStoreErrorCode::Cancelled
    );
    let active = CancellationToken::new();
    assert!(
        ready(store.read(&provider, &active))
            .expect("read succeeds")
            .is_none()
    );
}

fn raw_entry(id: &str) -> RawModelsStoreEntry {
    serde_json::from_value(serde_json::json!({
        "models": [{
            "id": id,
            "name": id,
            "api": "test-api",
            "provider": "p",
            "baseUrl": "https://example.test/v1",
            "reasoning": false,
            "input": ["text"],
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
            "contextWindow": 1000,
            "maxTokens": 100
        }],
        "etag": "opaque"
    }))
    .expect("valid raw entry")
}

fn model_entry(id: &str) -> ModelsStoreEntry {
    raw_entry(id)
        .into_store(&ModelHeaderPolicy::default())
        .expect("entry validates")
}

#[test]
fn models_store_is_detached_overwritable_deletable_and_cancel_admitted() {
    let store = InMemoryModelsStore::new();
    let provider = provider("p");
    let token = CancellationToken::new();
    assert!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .is_none()
    );

    ready(store.write(&provider, model_entry("one"), &token)).expect("write succeeds");
    let mut detached = ready(store.read(&provider, &token))
        .expect("read succeeds")
        .expect("entry present");
    detached.models[0].name = "changed".to_owned();
    assert_eq!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .expect("entry present")
            .models[0]
            .name,
        "one"
    );

    ready(store.write(&provider, model_entry("two"), &token)).expect("overwrite succeeds");
    assert_eq!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .expect("entry present")
            .models[0]
            .id
            .as_str(),
        "two"
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        ready(store.write(&provider, model_entry("cancelled"), &cancelled))
            .expect_err("pre-admission cancellation wins")
            .code(),
        ModelsStoreErrorCode::Cancelled
    );
    assert_eq!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .expect("entry present")
            .models[0]
            .id
            .as_str(),
        "two"
    );
    ready(store.delete(&provider, &token)).expect("delete succeeds");
    assert!(
        ready(store.read(&provider, &token))
            .expect("read succeeds")
            .is_none()
    );
}

#[test]
fn auth_explicit_stored_ambient_and_mismatch_precedence() {
    let primary_provider = provider("p");
    let store = InMemoryCredentialStore::new();
    let context = TestContext::default();
    let clock = TestClock::new(1_000);
    let token = CancellationToken::new();
    let api = Arc::new(RecordingApiKey::new(Some("ambient")));
    let auth = ProviderAuth::new(Some(api.clone()), None).expect("capability present");

    let explicit = resolve(
        &primary_provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides {
            api_key: Some(SecretString::new("explicit")),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect("resolution succeeds")
    .expect("explicit resolves");
    assert_eq!(
        explicit.auth.api_key.expect("key").expose_secret(),
        "explicit"
    );
    assert!(
        ready(store.read(&primary_provider, &token))
            .expect("read succeeds")
            .is_none()
    );

    set_credential(&store, &primary_provider, api_credential("stored"));
    let stored = resolve(
        &primary_provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    )
    .expect("resolution succeeds")
    .expect("stored resolves");
    assert_eq!(stored.auth.api_key.expect("key").expose_secret(), "stored");

    let mismatch_provider = provider("mismatch");
    set_credential(
        &store,
        &mismatch_provider,
        Credential::OAuth(oauth_credential("oauth", 1)),
    );
    assert!(
        resolve(
            &mismatch_provider,
            &auth,
            &store,
            &context,
            &clock,
            AuthResolutionOverrides::default(),
            &token,
        )
        .expect("mismatch is not an error")
        .is_none()
    );

    let ambient_provider = provider("ambient");
    let ambient = resolve(
        &ambient_provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    )
    .expect("resolution succeeds")
    .expect("ambient resolves");
    assert_eq!(
        ambient.auth.api_key.expect("key").expose_secret(),
        "ambient"
    );
}

#[test]
fn explicit_none_short_circuits_and_missing_capability_reads_storage() {
    let provider = provider("p");
    let store = InMemoryCredentialStore::new();
    let context = TestContext::default();
    let clock = TestClock::new(0);
    let token = CancellationToken::new();
    let mut none_api = RecordingApiKey::new(Some("ambient"));
    none_api.return_none = true;
    let none_auth = ProviderAuth::new(Some(Arc::new(none_api)), None).expect("valid auth");
    let result = resolve(
        &provider,
        &none_auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides {
            api_key: Some(SecretString::new("explicit")),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect("resolution succeeds");
    assert!(result.is_none());

    let oauth = Arc::new(RecordingOAuth::new(1_000_000));
    let oauth_only = ProviderAuth::new(None, Some(oauth)).expect("valid auth");
    set_credential(
        &store,
        &provider,
        Credential::OAuth(oauth_credential("stored-oauth", 1_000_000)),
    );
    let result = resolve(
        &provider,
        &oauth_only,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides {
            api_key: Some(SecretString::new("ignored-explicit")),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect("stored OAuth resolves")
    .expect("result present");
    assert_eq!(result.source.as_deref(), Some("OAuth"));
}

#[test]
fn auth_environment_overlay_preserves_empty_fallback_and_stored_empty_values() {
    let provider = provider("p");
    let store = InMemoryCredentialStore::new();
    let context = TestContext::with([("ACCOUNT", "base")]);
    let clock = TestClock::new(0);
    let token = CancellationToken::new();
    let api = Arc::new(RecordingApiKey::new(None));
    let auth = ProviderAuth::new(Some(api.clone()), None).expect("valid auth");
    let stored = ApiKeyCredential {
        credential_type: ApiKeyCredentialType::ApiKey,
        key: Some(SecretString::new("stored")),
        env: Some(ProviderEnv::new(BTreeMap::from([
            ("ACCOUNT".to_owned(), "stored-account".to_owned()),
            ("KEEP".to_owned(), "stored".to_owned()),
        ]))),
    };
    set_credential(&store, &provider, Credential::ApiKey(stored));

    resolve(
        &provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides {
            env: Some(ProviderEnv::new(BTreeMap::from([(
                "ACCOUNT".to_owned(),
                "request-account".to_owned(),
            )]))),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect("nonempty overlay resolves");
    assert_eq!(
        api.calls
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .last()
            .expect("call recorded")
            .context_value
            .as_deref(),
        Some("request-account")
    );

    resolve(
        &provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides {
            env: Some(ProviderEnv::new(BTreeMap::from([
                ("ACCOUNT".to_owned(), String::new()),
                ("KEEP".to_owned(), "request".to_owned()),
            ]))),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect("resolution succeeds");
    let call = api
        .calls
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .last()
        .cloned()
        .expect("call recorded");
    assert_eq!(call.context_value.as_deref(), Some("base"));
    assert_eq!(
        call.credential_env
            .expect("explicit map remains present")
            .get("ACCOUNT")
            .map(String::as_str),
        Some("")
    );
}

struct ErrorCredentialStore {
    code: CredentialStoreErrorCode,
}

impl CredentialStore for ErrorCredentialStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<
        'a,
        Result<Option<pi_ai_core::VersionedCredential>, pi_ai_core::CredentialStoreError>,
    > {
        Box::pin(async move { Err(pi_ai_core::CredentialStoreError::new(self.code)) })
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<pi_ai_core::CredentialInfo>, pi_ai_core::CredentialStoreError>>
    {
        Box::pin(async move { Err(pi_ai_core::CredentialStoreError::new(self.code)) })
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<
        'a,
        Result<Box<dyn pi_ai_core::CredentialMutation + 'static>, pi_ai_core::CredentialStoreError>,
    > {
        Box::pin(async move { Err(pi_ai_core::CredentialStoreError::new(self.code)) })
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _resolution: pi_ai_core::CredentialReconciliation,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<
        'a,
        Result<Option<pi_ai_core::VersionedCredential>, pi_ai_core::CredentialStoreError>,
    > {
        Box::pin(async move { Err(pi_ai_core::CredentialStoreError::new(self.code)) })
    }
}

#[test]
fn credential_store_error_channels_remain_distinct_and_explicit_key_bypasses_store() {
    let provider = provider("p");
    let context = TestContext::default();
    let clock = TestClock::new(0);
    let token = CancellationToken::new();
    let api = Arc::new(RecordingApiKey::new(Some("ambient")));
    let auth = ProviderAuth::new(Some(api), None).expect("valid auth");
    for (code, expected) in [
        (CredentialStoreErrorCode::Failed, "models-auth"),
        (CredentialStoreErrorCode::Cancelled, "cancelled"),
        (CredentialStoreErrorCode::OutcomeUnknown, "unknown"),
    ] {
        let error = resolve(
            &provider,
            &auth,
            &ErrorCredentialStore { code },
            &context,
            &clock,
            AuthResolutionOverrides::default(),
            &token,
        )
        .expect_err("store channel is preserved");
        match expected {
            "models-auth" => assert!(matches!(
                error,
                AuthResolutionError::Models(ref error)
                    if error.code() == ModelsErrorCode::Auth
            )),
            "cancelled" => assert!(matches!(error, AuthResolutionError::Cancelled)),
            "unknown" => assert!(matches!(error, AuthResolutionError::OutcomeUnknown)),
            _ => unreachable!(),
        }
    }

    let result = resolve(
        &provider,
        &auth,
        &ErrorCredentialStore {
            code: CredentialStoreErrorCode::OutcomeUnknown,
        },
        &context,
        &clock,
        AuthResolutionOverrides {
            api_key: Some(SecretString::new("explicit")),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect("explicit key bypasses quarantine")
    .expect("explicit key resolves");
    assert_eq!(
        result.auth.api_key.expect("key").expose_secret(),
        "explicit"
    );
}

#[test]
fn oauth_validity_boundaries_refresh_and_commit_before_explicit_validation() {
    let provider = provider("p");
    let store = InMemoryCredentialStore::new();
    let context = TestContext::default();
    let clock = TestClock::new(1_000_000);
    let token = CancellationToken::new();
    let oauth = Arc::new(RecordingOAuth::new(1_400_000));
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    set_credential(
        &store,
        &provider,
        Credential::OAuth(oauth_credential("boundary", 1_300_000)),
    );
    let result = resolve(
        &provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    )
    .expect("default short refresh token remains usable")
    .expect("OAuth resolves");
    assert_eq!(result.source.as_deref(), Some("OAuth"));
    assert_eq!(oauth.refreshes.load(Ordering::Acquire), 1);

    let error = resolve(
        &provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides {
            min_oauth_validity_ms: Some(SafeUnsigned::new(600_000).expect("safe")),
            ..AuthResolutionOverrides::default()
        },
        &token,
    )
    .expect_err("explicit validity is enforced after rotation");
    assert!(matches!(
        error,
        AuthResolutionError::Models(ref error) if error.code() == ModelsErrorCode::OAuth
    ));
    let stored = ready(store.read(&provider, &token))
        .expect("read succeeds")
        .expect("credential remains");
    let Credential::OAuth(stored) = stored.credential else {
        panic!("expected OAuth")
    };
    assert_eq!(stored.credentials.access.expose_secret(), "refreshed");
}

#[test]
fn valid_oauth_does_not_mutate_and_refresh_failure_preserves_old_value() {
    let primary_provider = provider("p");
    let store = InMemoryCredentialStore::new();
    let context = TestContext::default();
    let clock = TestClock::new(1_000_000);
    let token = CancellationToken::new();
    let oauth = Arc::new(RecordingOAuth::new(2_000_000));
    let auth = ProviderAuth::new(None, Some(oauth.clone())).expect("valid auth");
    let first = set_credential(
        &store,
        &primary_provider,
        Credential::OAuth(oauth_credential("valid", 1_300_001)),
    );
    let result = resolve(
        &primary_provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    )
    .expect("valid OAuth resolves")
    .expect("result present");
    assert_eq!(result.source.as_deref(), Some("OAuth"));
    assert_eq!(oauth.refreshes.load(Ordering::Acquire), 0);
    assert_eq!(
        ready(store.read(&primary_provider, &token))
            .expect("read succeeds")
            .expect("value present")
            .revision,
        first.revision
    );

    let failing_provider = provider("failing");
    set_credential(
        &store,
        &failing_provider,
        Credential::OAuth(oauth_credential("old", 0)),
    );
    let mut failing = RecordingOAuth::new(2_000_000);
    failing.fail_refresh = true;
    let failing_auth = ProviderAuth::new(None, Some(Arc::new(failing))).expect("valid auth");
    let error = resolve(
        &failing_provider,
        &failing_auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    )
    .expect_err("refresh failure is OAuth");
    assert!(matches!(
        error,
        AuthResolutionError::Models(ref error) if error.code() == ModelsErrorCode::OAuth
    ));
    let stored = ready(store.read(&failing_provider, &token))
        .expect("read succeeds")
        .expect("old credential remains");
    let Credential::OAuth(stored) = stored.credential else {
        panic!("expected OAuth")
    };
    assert_eq!(stored.credentials.access.expose_secret(), "old");
}

#[test]
fn auth_provider_failure_maps_to_fixed_models_error() {
    let provider = provider("p");
    let store = InMemoryCredentialStore::new();
    let context = TestContext::default();
    let clock = TestClock::new(0);
    let token = CancellationToken::new();
    let mut failing = RecordingApiKey::new(Some("key"));
    failing.fail = true;
    let auth = ProviderAuth::new(Some(Arc::new(failing)), None).expect("valid auth");
    let error = resolve(
        &provider,
        &auth,
        &store,
        &context,
        &clock,
        AuthResolutionOverrides::default(),
        &token,
    )
    .expect_err("provider failure maps to auth");
    assert_eq!(error.to_string(), "authentication failed");
    assert!(matches!(
        error,
        AuthResolutionError::Models(ref error) if error.code() == ModelsErrorCode::Auth
    ));
    assert!(error.source().is_some());
}
