use pi_coding_session::{SessionSnapshot, project};
use serde_json::Value;

#[test]
fn projections_match_pinned_upstream_functions() {
    let trace: Value = serde_json::from_str(include_str!(
        "../../../conformance/coding-session/trace.json"
    ))
    .unwrap();
    assert_eq!(
        trace["upstream"],
        "4a080252f06caf63a525d7873c5d94d3c23887e0"
    );
    for case in trace["cases"].as_array().unwrap() {
        let snapshot: SessionSnapshot = serde_json::from_value(case["snapshot"].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(project(&snapshot).unwrap()).unwrap(),
            case["expected"],
            "{}",
            case["name"]
        );
    }
}
