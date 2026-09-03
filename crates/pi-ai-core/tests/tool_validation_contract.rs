use std::collections::BTreeSet;

use pi_ai_core::{
    JsonObject, Tool, ToolCall, ToolValidationError, ToolValidationIssue, ToolValidationReport,
    validate_tool_arguments, validate_tool_call,
};

const INVENTORY: &str =
    include_str!("../../../conformance/ai-domain/tool-validation-inventory.json");
const CORE_TESTS: &str = include_str!("tool_validation.rs");
const CONTRACT_TESTS: &str = include_str!("tool_validation_contract.rs");
const FACADE_TESTS: &str = include_str!("../../pi-ai/tests/tool_validation_facade.rs");

const EXPECTED_ROWS: [(&str, &str); 30] = [
    (
        "validation.runtime.offline-no-code-generation",
        "offline_validator_requires_no_code_generation_and_bounds_cache",
    ),
    (
        "validation.coerce.number-string",
        "plain_number_coerces_finite_numeric_string",
    ),
    (
        "validation.coerce.number-true",
        "plain_number_coerces_true_to_one",
    ),
    (
        "validation.coerce.number-null",
        "plain_number_coerces_null_to_zero",
    ),
    (
        "validation.coerce.integer-string",
        "plain_integer_coerces_integral_string",
    ),
    (
        "validation.coerce.boolean-string-true",
        "plain_boolean_coerces_true_string",
    ),
    (
        "validation.coerce.boolean-string-false",
        "plain_boolean_coerces_false_string",
    ),
    ("validation.coerce.boolean-one", "plain_boolean_coerces_one"),
    (
        "validation.coerce.boolean-zero",
        "plain_boolean_coerces_zero",
    ),
    (
        "validation.coerce.string-null",
        "plain_string_coerces_null_to_empty",
    ),
    (
        "validation.coerce.string-primitive",
        "plain_string_coerces_boolean",
    ),
    (
        "validation.coerce.null-empty-string",
        "plain_null_coerces_empty_string",
    ),
    ("validation.coerce.null-zero", "plain_null_coerces_zero"),
    ("validation.coerce.null-false", "plain_null_coerces_false"),
    (
        "validation.type-union.preserve-matching",
        "type_union_preserves_existing_matching_arm",
    ),
    (
        "validation.type-union.coerce-later-arm",
        "type_union_tries_later_coercible_arm",
    ),
    (
        "validation.optional-null.object-recursion",
        "optional_null_normalization_recurses",
    ),
    (
        "validation.optional-null.local-ref-nullable",
        "local_nullable_reference_preserves_optional_null",
    ),
    (
        "validation.union.anyof-preserve-null",
        "any_of_preserves_existing_null_arm",
    ),
    (
        "validation.union.oneof-preserve-null",
        "one_of_preserves_existing_null_arm",
    ),
    (
        "validation.union.anyof-coerce-number",
        "any_of_coerces_to_first_valid_arm",
    ),
    (
        "validation.type-union.nullable-array",
        "nullable_array_type_preserves_null",
    ),
    (
        "validation.reject.boolean-string-one",
        "boolean_rejects_numeric_one_string",
    ),
    (
        "validation.reject.boolean-string-zero",
        "boolean_rejects_numeric_zero_string",
    ),
    ("validation.reject.null-string", "null_rejects_null_string"),
    (
        "validation.reject.fractional-integer",
        "integer_rejects_fractional_string",
    ),
    (
        "validation.tool.known",
        "validate_tool_call_selects_known_exact_name",
    ),
    (
        "validation.tool.unknown",
        "validate_tool_call_rejects_unknown_name",
    ),
    (
        "validation.issue.nested-required-path",
        "nested_required_issue_has_bounded_dotted_path",
    ),
    (
        "validation.arguments.nonmutation",
        "validation_never_mutates_arguments_or_schema",
    ),
];

fn assert_value_contract<T>()
where
    T: Clone + std::fmt::Debug + Eq + Send + Sync + 'static,
{
}

fn assert_error_contract<T>()
where
    T: Clone + std::fmt::Debug + Eq + std::error::Error + Send + Sync + 'static,
{
}

#[test]
fn tool_validation_public_surface_is_exact_and_thread_safe() {
    type ValidateCall = fn(&[Tool], &ToolCall) -> Result<JsonObject, ToolValidationError>;
    type ValidateArguments = fn(&Tool, &ToolCall) -> Result<JsonObject, ToolValidationError>;
    let _validate_call: ValidateCall = validate_tool_call;
    let _validate_arguments: ValidateArguments = validate_tool_arguments;
    let _path: for<'a> fn(&'a ToolValidationIssue) -> &'a str = ToolValidationIssue::path;
    let _keyword: for<'a> fn(&'a ToolValidationIssue) -> &'a str = ToolValidationIssue::keyword;
    let _message: for<'a> fn(&'a ToolValidationIssue) -> &'a str = ToolValidationIssue::message;
    let _issues: for<'a> fn(&'a ToolValidationReport) -> &'a [ToolValidationIssue] =
        ToolValidationReport::issues;
    let _omitted: fn(&ToolValidationReport) -> pi_ai_core::SafeUnsigned =
        ToolValidationReport::omitted_issue_count;

    assert_value_contract::<ToolValidationIssue>();
    assert_value_contract::<ToolValidationReport>();
    assert_error_contract::<ToolValidationError>();
    for error in [
        ToolValidationError::ToolNotFound,
        ToolValidationError::InvalidSchema,
    ] {
        assert!(std::error::Error::source(&error).is_none());
    }
    assert_eq!(
        ToolValidationError::ToolNotFound.to_string(),
        "tool not found"
    );
    assert_eq!(
        ToolValidationError::InvalidSchema.to_string(),
        "invalid tool schema"
    );
}

#[test]
fn tool_validation_inventory_has_exact_rows_fields_order_and_live_bindings() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(
        document
            .as_object()
            .expect("inventory object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        ["expectedRows", "rows", "suite"].into_iter().collect()
    );
    assert_eq!(document["suite"], "tool-validation");
    assert_eq!(document["expectedRows"], 30);
    let rows = document["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 30);

    let exact_keys = ["expected", "id", "owner", "polarity", "source", "test"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for (row, (expected_id, expected_test)) in rows.iter().zip(EXPECTED_ROWS) {
        let object = row.as_object().expect("row object");
        assert_eq!(
            object.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            exact_keys
        );
        assert_eq!(row["id"], expected_id);
        assert_eq!(row["test"], expected_test);
        assert_eq!(row["owner"], "S03-tool-validation");
        assert!(matches!(
            row["polarity"].as_str(),
            Some("positive" | "negative" | "boundary" | "race")
        ));
        let source = row["source"].as_str().expect("source string");
        assert!(source.contains("@853a80d26c90a14c1886f0ebb8ffaae133ca2185"));
        assert!(
            !row["expected"]
                .as_str()
                .expect("expected string")
                .is_empty()
        );
        let declaration = format!("fn {expected_test}(");
        assert!(
            [CORE_TESTS, CONTRACT_TESTS, FACADE_TESTS]
                .iter()
                .any(|source| source.contains(&declaration)),
            "missing test binding {expected_test}"
        );
    }
}
