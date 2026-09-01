use pi_agent_schema::{
    OperationState, ProviderHeaderClassificationError, ProviderHeaderPolicy, RawOperationState,
    RawRegister, RawRegisterSet, RawTransaction, RawWrite, Register, RegisterSet, Transaction,
    Write,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct StateCase {
    name: String,
    value: serde_json::Value,
}

fn state_case(name: &str) -> serde_json::Value {
    let cases: Vec<StateCase> = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .expect("state fixture must be valid JSON");
    cases
        .into_iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("missing state case {name}"))
        .value
}

fn insert_headers(value: &mut serde_json::Value, path: &[&str], name: &str) {
    let mut options = value;
    for key in path {
        options = options
            .get_mut(*key)
            .unwrap_or_else(|| panic!("missing stream-option path component {key}"));
    }
    options
        .as_object_mut()
        .expect("stream options object")
        .insert(
            "headers".to_owned(),
            serde_json::json!({(name): "public-value"}),
        );
}

fn public_policy() -> ProviderHeaderPolicy {
    ProviderHeaderPolicy::from_allowlist(["x-public".to_owned()])
}

#[test]
fn generation_deferred_and_summary_states_reopen_with_the_same_policy() {
    let cases: &[(&str, &[&str])] = &[
        (
            "run.assistant.effect_pending",
            &["phase", "generation", "context", "streamOptions"],
        ),
        (
            "run.deferred.effect_pending",
            &["phase", "deferred", "streamOptions"],
        ),
        (
            "compaction.generating.ready",
            &["structural", "generation", "context", "streamOptions"],
        ),
    ];

    for (name, path) in cases {
        let mut value = state_case(name);
        insert_headers(&mut value, path, "X-Public");
        let raw: RawOperationState =
            serde_json::from_value(value).unwrap_or_else(|error| panic!("{name}: {error}"));
        let durable = raw
            .into_durable(&public_policy())
            .expect("policy classifies state");
        let bytes = serde_json::to_vec(&durable).expect("durable state serializes");

        assert!(serde_json::from_slice::<OperationState>(&bytes).is_err());
        let reopened_raw: RawOperationState =
            serde_json::from_slice(&bytes).expect("raw state reopens");
        let reopened = reopened_raw
            .into_durable(&public_policy())
            .expect("same policy reclassifies state");
        assert_eq!(
            serde_json::to_vec(&reopened).expect("reopened state serializes"),
            bytes,
            "{name} changed durable bytes"
        );

        let changed: RawOperationState =
            serde_json::from_slice(&bytes).expect("raw state reopens for changed policy");
        assert!(
            changed
                .into_durable(&ProviderHeaderPolicy::default())
                .is_err()
        );
    }
}

fn headerful_state_value(header_name: &str) -> serde_json::Value {
    let mut state = state_case("run.assistant.effect_pending");
    insert_headers(
        &mut state,
        &["phase", "generation", "context", "streamOptions"],
        header_name,
    );
    state
}

#[test]
fn every_containing_root_reopens_headerful_operation_state() {
    let state = headerful_state_value("X-Public");
    let register_value = serde_json::json!({
        "namespace": "op.state",
        "key": "op-1",
        "value": state,
        "seq": 0
    });
    let raw_register: RawRegister =
        serde_json::from_value(register_value).expect("raw register decodes");
    let register = raw_register
        .into_durable(&public_policy())
        .expect("register classifies");
    assert_root_reopens::<Register, RawRegister>(
        &register,
        |raw| raw.into_durable(&public_policy()),
        |raw| raw.into_durable(&ProviderHeaderPolicy::default()),
    );

    let register_set_value = serde_json::json!({
        "namespace": "op.state",
        "key": "op-1",
        "value": headerful_state_value("X-Public")
    });
    let raw_set: RawRegisterSet =
        serde_json::from_value(register_set_value).expect("raw register set decodes");
    let register_set = raw_set
        .into_durable(&public_policy())
        .expect("register set classifies");
    assert_root_reopens::<RegisterSet, RawRegisterSet>(
        &register_set,
        |raw| raw.into_durable(&public_policy()),
        |raw| raw.into_durable(&ProviderHeaderPolicy::default()),
    );

    let write_value = serde_json::json!({
        "kind": "register",
        "op": "set",
        "namespace": "op.state",
        "key": "op-1",
        "value": headerful_state_value("X-Public")
    });
    let raw_write: RawWrite = serde_json::from_value(write_value).expect("raw write decodes");
    let write = raw_write
        .into_durable(&public_policy())
        .expect("write classifies");
    assert_root_reopens::<Write, RawWrite>(
        &write,
        |raw| raw.into_durable(&public_policy()),
        |raw| raw.into_durable(&ProviderHeaderPolicy::default()),
    );

    let transaction_value = serde_json::json!({
        "writes": [{
            "kind": "register",
            "op": "set",
            "namespace": "op.state",
            "key": "op-1",
            "value": headerful_state_value("X-Public")
        }]
    });
    let raw_transaction: RawTransaction =
        serde_json::from_value(transaction_value).expect("raw transaction decodes");
    let transaction = raw_transaction
        .into_durable(&public_policy())
        .expect("transaction classifies");
    assert_root_reopens::<Transaction, RawTransaction>(
        &transaction,
        |raw| raw.into_durable(&public_policy()),
        |raw| raw.into_durable(&ProviderHeaderPolicy::default()),
    );
}

fn assert_root_reopens<D, R>(
    durable: &D,
    same_policy: impl FnOnce(R) -> Result<D, ProviderHeaderClassificationError>,
    changed_policy: impl FnOnce(R) -> Result<D, ProviderHeaderClassificationError>,
) where
    D: serde::Serialize + serde::de::DeserializeOwned,
    R: serde::de::DeserializeOwned,
{
    let bytes = serde_json::to_vec(durable).expect("durable root serializes");
    assert!(serde_json::from_slice::<D>(&bytes).is_err());
    let raw: R = serde_json::from_slice(&bytes).expect("raw root reopens");
    let reopened = same_policy(raw).expect("same policy reclassifies root");
    assert_eq!(
        serde_json::to_vec(&reopened).expect("reopened root serializes"),
        bytes
    );
    let changed: R = serde_json::from_slice(&bytes).expect("raw root reopens for changed policy");
    assert!(changed_policy(changed).is_err());
}

#[test]
fn hard_denied_header_cannot_enter_nested_state_or_transaction() {
    let state: RawOperationState = serde_json::from_value(headerful_state_value("Authorization"))
        .expect("raw hard-denied state decodes");
    let allow = ProviderHeaderPolicy::from_allowlist(["authorization".to_owned()]);
    assert!(matches!(
        state.into_durable(&allow),
        Err(ProviderHeaderClassificationError::HardDenied(_))
    ));

    let transaction: RawTransaction = serde_json::from_value(serde_json::json!({
        "writes": [{
            "kind": "register",
            "op": "set",
            "namespace": "op.state",
            "key": "op-1",
            "value": headerful_state_value("Authorization")
        }]
    }))
    .expect("raw hard-denied transaction decodes");
    assert!(matches!(
        transaction.into_durable(&allow),
        Err(ProviderHeaderClassificationError::HardDenied(_))
    ));
}
