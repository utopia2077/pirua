#[path = "../../../conformance/session/model.rs"]
mod model;
#[path = "support/mod.rs"]
mod support;
use support::*;
#[test]
fn sqlite_literal_query_contract() {
    queries(&Factory::default(), &Observations::default());
}
#[test]
fn sqlite_atomicity_contract() {
    atomicity(&Factory::default(), &Observations::default());
}
#[test]
fn sqlite_register_and_multikey_cas_contract() {
    registers_and_cas(&Factory::default(), &Observations::default());
}
#[test]
fn sqlite_ledger_contract() {
    ledger(&Factory::default(), &Observations::default());
}
#[test]
fn sqlite_exhaustive_four_step_independent_model() {
    assert_eq!(model::enumerate(&Factory::default()), 16_384);
}
