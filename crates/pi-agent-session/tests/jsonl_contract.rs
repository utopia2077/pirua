#[path = "../../../conformance/session/model.rs"]
mod model;
#[path = "jsonl_support/mod.rs"]
mod support;
use support::*;
#[test]
fn jsonl_literal_query_contract() {
    queries(&Factory::default(), &Observations::default());
}
#[test]
fn jsonl_atomicity_contract() {
    atomicity(&Factory::default(), &Observations::default());
}
#[test]
fn jsonl_register_and_multikey_cas_contract() {
    registers_and_cas(&Factory::default(), &Observations::default());
}
#[test]
fn jsonl_ledger_contract() {
    ledger(&Factory::default(), &Observations::default());
}
#[test]
fn jsonl_exhaustive_four_step_independent_model() {
    assert_eq!(model::enumerate(&Factory::default()), 16_384);
}
