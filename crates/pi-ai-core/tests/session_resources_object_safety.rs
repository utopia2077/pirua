use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use pi_ai_core::{
    SessionResource, SessionResourceError, SessionResourceHandle, SessionResourceRegistry,
    SessionResourceRegistryError, SessionResourceRegistryErrorCode,
};

const INVENTORY: &str =
    include_str!("../../../conformance/ai-domain/session-resources-inventory.json");
const TESTS: &str = include_str!("session_resources.rs");
const EXPECTED_IDS: [&str; 12] = [
    "session-resource.registration.duplicate-object",
    "session-resource.cleanup.insertion-order",
    "session-resource.cleanup.session-id-none",
    "session-resource.cleanup.session-id-exact",
    "session-resource.handle.explicit-removal",
    "session-resource.handle.drop",
    "session-resource.cleanup.registration-next-pass",
    "session-resource.cleanup.removal-next-pass",
    "session-resource.cleanup.reentrancy",
    "session-resource.cleanup.failure-continuation",
    "session-resource.cleanup.ordered-aggregate",
    "session-resource.cleanup.removal-wins",
];

fn invoke(
    resource: &dyn SessionResource,
    session_id: Option<&str>,
) -> Result<(), SessionResourceError> {
    resource.cleanup(session_id)
}

fn assert_error<T: fmt::Debug + Error + Send + Sync + 'static>() {}
fn assert_send_sync<T: Send + Sync + 'static>() {}

#[derive(Debug)]
struct SecretSource;

impl fmt::Display for SecretSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SESSION_RESOURCE_SECRET_SENTINEL")
    }
}

impl Error for SecretSource {}

#[test]
fn session_resource_public_api_is_exact_object_safe_and_redacted() {
    type Register = fn(
        &SessionResourceRegistry,
        Arc<dyn SessionResource>,
    ) -> Result<SessionResourceHandle, SessionResourceRegistryError>;
    type Cleanup = for<'a, 'b> fn(
        &'a SessionResourceRegistry,
        Option<&'b str>,
    ) -> Result<(), SessionResourceRegistryError>;
    let _new: fn() -> SessionResourceRegistry = SessionResourceRegistry::new;
    let _register: Register = SessionResourceRegistry::register;
    let _cleanup: Cleanup = SessionResourceRegistry::cleanup;
    let _remove: fn(&mut SessionResourceHandle) -> bool = SessionResourceHandle::remove;
    let _invoke: for<'a, 'b> fn(
        &'a dyn SessionResource,
        Option<&'b str>,
    ) -> Result<(), SessionResourceError> = invoke;
    fn accepts_resource(_: &dyn SessionResource) {}
    let _accepts: fn(&dyn SessionResource) = accepts_resource;

    assert_error::<SessionResourceError>();
    assert_error::<SessionResourceRegistryError>();
    assert_send_sync::<SessionResourceRegistry>();
    assert_send_sync::<SessionResourceHandle>();

    let plain = SessionResourceError::new();
    assert_eq!(plain.to_string(), "session resource cleanup failed");
    assert_eq!(
        format!("{plain:?}"),
        "SessionResourceError { source_present: false }"
    );
    let sourced = SessionResourceError::with_source(SecretSource);
    assert_eq!(
        format!("{sourced:?}"),
        "SessionResourceError { source_present: true }"
    );
    assert!(!format!("{sourced:?} {sourced}").contains("SESSION_RESOURCE_SECRET_SENTINEL"));
    assert_eq!(
        Error::source(&sourced).map(ToString::to_string),
        Some("SESSION_RESOURCE_SECRET_SENTINEL".to_owned())
    );

    let _code: fn(&SessionResourceRegistryError) -> SessionResourceRegistryErrorCode =
        SessionResourceRegistryError::code;
    let _failures: fn(&SessionResourceRegistryError) -> &[SessionResourceError] =
        SessionResourceRegistryError::failures;
}

#[test]
fn session_resources_inventory_has_exact_rows_and_live_bindings() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "session-resources");
    assert_eq!(document["expectedRows"], 12);
    let rows = document["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), EXPECTED_IDS.len());
    let keys = ["expected", "id", "owner", "polarity", "source", "test"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for (row, expected_id) in rows.iter().zip(EXPECTED_IDS) {
        let row = row.as_object().expect("row object");
        assert_eq!(
            row.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            keys
        );
        assert_eq!(row["id"], expected_id);
        assert_eq!(row["owner"], "S03-session-resources");
        assert!(matches!(
            row["polarity"].as_str(),
            Some("positive" | "negative" | "boundary" | "race")
        ));
        let source = row["source"].as_str().expect("source");
        assert!(
            source.contains("@853a80d26c90a14c1886f0ebb8ffaae133ca2185")
                || source.contains("docs/contracts/ai-domain/")
                || source.contains("upstream-map.toml:")
        );
        let binding = row["test"].as_str().expect("binding");
        assert!(TESTS.contains(&format!("fn {binding}(")));
        assert!(!row["expected"].as_str().expect("expected").is_empty());
    }
}
