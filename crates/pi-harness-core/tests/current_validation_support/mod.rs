use pi_agent_schema::*;
use pi_harness_core::*;
use serde_json::Value;
use std::collections::BTreeMap;

pub fn cases() -> Vec<Value> {
    serde_json::from_str(include_str!("cases.json")).unwrap()
}
pub fn case(name: &str) -> Value {
    cases().into_iter().find(|c| c["name"] == name).unwrap()
}
pub struct Loaded {
    pub roots: CurrentRoots,
    pub entries: BTreeMap<EntryId, Entry>,
    pub registers: BTreeMap<(RegisterNamespace, String), Register>,
}
impl Loaded {
    pub fn new(case: &Value) -> Self {
        let r = &case["roots"];
        Self {
            roots: CurrentRoots {
                lane: serde_json::from_value(r["lane"].clone()).unwrap(),
                configuration: serde_json::from_value(r["configuration"].clone()).unwrap(),
                lane_state: serde_json::from_value(r["lane_state"].clone()).unwrap(),
                leaf: serde_json::from_value(r["leaf"].clone()).unwrap(),
                operation_meta: serde_json::from_value(r["operation_meta"].clone()).unwrap(),
                operation_state: serde_json::from_value(r["operation_state"].clone()).unwrap(),
            },
            entries: case["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| {
                    (
                        serde_json::from_value(e["id"].clone()).unwrap(),
                        serde_json::from_value(e.clone()).unwrap(),
                    )
                })
                .collect(),
            registers: case["registers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        (
                            serde_json::from_value(r["namespace"].clone()).unwrap(),
                            r["key"].as_str().unwrap().to_owned(),
                        ),
                        serde_json::from_value(r.clone()).unwrap(),
                    )
                })
                .collect(),
        }
    }
    pub fn validate(&self) -> Result<Option<CurrentOperation>, CurrentValidationError> {
        validate_current_entries_and_registers(&self.roots, &self.entries, &self.registers)
    }
}
pub fn entry_mut<'a>(case: &'a mut Value, id: &str) -> &'a mut Value {
    case["entries"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|e| e["id"] == id)
        .unwrap()
}
pub fn good(case: &Value) {
    assert!(Loaded::new(case).validate().is_ok(), "{}", case["name"]);
}
pub fn bad(case: &Value, error: CurrentValidationError) {
    assert_eq!(
        Loaded::new(case).validate().unwrap_err(),
        error,
        "{}",
        case["name"]
    );
}
