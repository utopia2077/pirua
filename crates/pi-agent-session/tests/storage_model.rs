#[path = "../../../conformance/session/model.rs"]
mod model;
mod support;
#[test]
fn exhaustive_four_step_independent_storage_model() {
    assert_eq!(model::enumerate(&support::Factory), 16_384);
}
