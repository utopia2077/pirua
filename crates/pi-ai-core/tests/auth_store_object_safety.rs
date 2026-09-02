use std::error::Error;
use std::fmt;
use std::sync::Arc;

use pi_ai_core::{
    ApiKeyAuth, AuthContext, AuthContextError, AuthInteraction, AuthInteractionError,
    AuthProviderError, AuthResolutionError, AuthResolutionOverrides, CredentialMutation,
    CredentialRevision, CredentialStore, CredentialStoreError, CredentialStoreErrorCode,
    InMemoryCredentialStore, InMemoryModelsStore, ModelsError, ModelsErrorCode, ModelsStore,
    ModelsStoreError, ModelsStoreErrorCode, OAuthAuth, ProviderAuth, ProviderAuthError,
    SecretString,
};

const INVENTORY: &str = include_str!("../../../conformance/ai-models/auth-store-inventory.json");
const AUTH_TEST_SOURCE: &str = include_str!("auth_store.rs");
const CONCURRENCY_TEST_SOURCE: &str = include_str!("auth_store_concurrency.rs");
const CREDENTIAL_SOURCE: &str = include_str!("../src/credential_store.rs");
const OBJECT_SAFETY_TEST_SOURCE: &str = include_str!("auth_store_object_safety.rs");
const SECRET_SENTINEL: &str = "AUTH_STORE_SECRET_SENTINEL_7a420d";

#[derive(Debug)]
struct SentinelError;

impl fmt::Display for SentinelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(SECRET_SENTINEL)
    }
}

impl Error for SentinelError {}

fn assert_send_sync<T: Send + Sync>() {}

fn accept_credential_store(_store: &dyn CredentialStore) {}
fn accept_models_store(_store: &dyn ModelsStore) {}
fn accept_auth_context(_context: &dyn AuthContext) {}
fn accept_auth_interaction(_interaction: &dyn AuthInteraction) {}
fn accept_api_key(_auth: &dyn ApiKeyAuth) {}
fn accept_oauth(_auth: &dyn OAuthAuth) {}
fn accept_mutation(_mutation: &dyn CredentialMutation) {}

#[test]
fn dynamic_ports_are_object_safe_and_store_implementations_are_send_sync() {
    assert_send_sync::<InMemoryCredentialStore>();
    assert_send_sync::<InMemoryModelsStore>();
    let credential_store = InMemoryCredentialStore::new();
    let models_store = InMemoryModelsStore::new();
    accept_credential_store(&credential_store);
    accept_models_store(&models_store);

    let context_slot: Option<&dyn AuthContext> = None;
    let interaction_slot: Option<&dyn AuthInteraction> = None;
    let api_key_slot: Option<&dyn ApiKeyAuth> = None;
    let oauth_slot: Option<&dyn OAuthAuth> = None;
    let mutation_slot: Option<&dyn CredentialMutation> = None;
    if let Some(context) = context_slot {
        accept_auth_context(context);
    }
    if let Some(interaction) = interaction_slot {
        accept_auth_interaction(interaction);
    }
    if let Some(api_key) = api_key_slot {
        accept_api_key(api_key);
    }
    if let Some(oauth) = oauth_slot {
        accept_oauth(oauth);
    }
    if let Some(mutation) = mutation_slot {
        accept_mutation(mutation);
    }
}

#[test]
fn exact_error_text_debug_redaction_and_sources_are_stable() {
    let credential_messages = [
        (
            CredentialStoreErrorCode::Cancelled,
            "credential store operation cancelled",
        ),
        (
            CredentialStoreErrorCode::Failed,
            "credential store operation failed",
        ),
        (
            CredentialStoreErrorCode::OutcomeUnknown,
            "credential store outcome unknown",
        ),
        (
            CredentialStoreErrorCode::RevisionExhausted,
            "credential revision exhausted",
        ),
        (
            CredentialStoreErrorCode::ReconciliationNotRequired,
            "credential reconciliation not required",
        ),
    ];
    for (code, expected) in credential_messages {
        let error = CredentialStoreError::new(code);
        assert_eq!(error.to_string(), expected);
        assert_eq!(error.code(), code);
        assert_eq!(
            format!("{error:?}"),
            format!("CredentialStoreError {{ code: {code:?}, source_present: false }}")
        );
    }
    let credential = CredentialStoreError::with_source(SentinelError);
    assert_eq!(credential.code(), CredentialStoreErrorCode::Failed);
    assert_eq!(credential.to_string(), "credential store operation failed");
    assert!(!format!("{credential:?}").contains(SECRET_SENTINEL));
    assert_eq!(
        credential.source().expect("source retained").to_string(),
        SECRET_SENTINEL
    );

    for (code, expected) in [
        (
            ModelsStoreErrorCode::Cancelled,
            "models store operation cancelled",
        ),
        (
            ModelsStoreErrorCode::Failed,
            "models store operation failed",
        ),
    ] {
        let error = ModelsStoreError::new(code);
        assert_eq!(error.to_string(), expected);
        assert_eq!(error.code(), code);
    }
    let models_store = ModelsStoreError::with_source(SentinelError);
    assert!(!format!("{models_store:?}").contains(SECRET_SENTINEL));
    assert_eq!(
        models_store.source().expect("source retained").to_string(),
        SECRET_SENTINEL
    );

    let leaf_errors: Vec<Box<dyn Error>> = vec![
        Box::new(AuthContextError::with_source(SentinelError)),
        Box::new(AuthInteractionError::with_source(SentinelError)),
        Box::new(AuthProviderError::with_source(SentinelError)),
    ];
    assert_eq!(
        leaf_errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec![
            "authentication context failed",
            "authentication interaction failed",
            "authentication provider failed",
        ]
    );
    for error in leaf_errors {
        assert!(!format!("{error:?}").contains(SECRET_SENTINEL));
        assert_eq!(
            error.source().expect("source retained").to_string(),
            SECRET_SENTINEL
        );
    }
}

#[test]
fn provider_auth_and_resolution_debug_expose_only_safe_categories() {
    assert_eq!(
        ProviderAuth::new(None, None).expect_err("capability required"),
        ProviderAuthError::NoCapabilities
    );
    assert_eq!(
        ProviderAuthError::NoCapabilities.to_string(),
        "provider auth requires at least one capability"
    );

    struct UnusedApi;
    impl ApiKeyAuth for UnusedApi {
        fn name(&self) -> &str {
            "unused"
        }

        fn resolve<'a>(
            &'a self,
            _input: pi_ai_core::ApiKeyAuthInput<'a>,
        ) -> pi_ai_core::BoxFuture<'a, Result<Option<pi_ai_core::AuthResult>, AuthProviderError>>
        {
            Box::pin(async { Ok(None) })
        }
    }
    let auth = ProviderAuth::new(Some(Arc::new(UnusedApi)), None).expect("valid auth");
    assert_eq!(
        format!("{auth:?}"),
        "ProviderAuth { api_key_present: true, oauth_present: false }"
    );
    let overrides = AuthResolutionOverrides {
        api_key: Some(SecretString::new(SECRET_SENTINEL)),
        ..AuthResolutionOverrides::default()
    };
    let debug = format!("{overrides:?}");
    assert!(debug.contains("api_key_present: true"));
    assert!(!debug.contains(SECRET_SENTINEL));

    let models = AuthResolutionError::Models(ModelsError::with_source(
        ModelsErrorCode::Auth,
        SentinelError,
    ));
    assert_eq!(models.to_string(), "authentication failed");
    assert!(format!("{models:?}").contains("category: \"Models\""));
    assert!(!format!("{models:?}").contains(SECRET_SENTINEL));
    assert_eq!(
        models.source().expect("ModelsError source").to_string(),
        "authentication failed"
    );
    assert_eq!(
        AuthResolutionError::Cancelled.to_string(),
        "authentication resolution cancelled"
    );
    assert_eq!(
        AuthResolutionError::OutcomeUnknown.to_string(),
        "credential outcome unknown"
    );
}

#[test]
fn credential_revision_boundaries_are_checked() {
    assert_eq!(CredentialRevision::new(0), None);
    assert_eq!(CredentialRevision::FIRST.get(), 1);
    assert_eq!(
        CredentialRevision::new(u64::MAX - 1)
            .expect("nonzero")
            .checked_next()
            .expect("successor exists")
            .get(),
        u64::MAX
    );
    assert_eq!(
        CredentialRevision::new(u64::MAX)
            .expect("nonzero")
            .checked_next(),
        None
    );
}

#[test]
fn inventory_has_exact_20_8_30_partition_and_unique_required_fields() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "auth-store");
    assert_eq!(document["expectedRows"], 58);
    let rows = document["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 58);
    let mut ids = std::collections::BTreeSet::new();
    let mut counts = [0_usize; 3];
    for row in rows {
        let id = row["id"].as_str().expect("string id");
        assert!(ids.insert(id), "duplicate inventory id {id}");
        assert!(
            row["source"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(row["test"].as_str().is_some_and(|value| !value.is_empty()));
        assert!(
            row["polarity"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(row["owner"], "S03-auth-store");
        assert!(
            row["expected"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        let source = row["source"].as_str().expect("source string");
        let (_, line) = source.rsplit_once(':').expect("source has an exact line");
        assert!(line.parse::<usize>().is_ok_and(|line| line > 0));
        let test = row["test"].as_str().expect("test string");
        let declaration = format!("fn {test}(");
        assert!(
            [
                AUTH_TEST_SOURCE,
                CONCURRENCY_TEST_SOURCE,
                CREDENTIAL_SOURCE,
                OBJECT_SAFETY_TEST_SOURCE,
            ]
            .iter()
            .any(|source| source.contains(&declaration)),
            "inventory test binding does not exist: {test}"
        );
        if id.starts_with("credential.") {
            counts[0] += 1;
        } else if id.starts_with("models-store.") {
            counts[1] += 1;
        } else if id.starts_with("auth.") {
            counts[2] += 1;
        } else {
            panic!("unexpected inventory prefix {id}");
        }
    }
    assert_eq!(counts, [20, 8, 30]);
}
