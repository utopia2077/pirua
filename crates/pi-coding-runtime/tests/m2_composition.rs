use std::collections::BTreeMap;

use pi_agent::schema::AgentMessage;
use pi_ai::{InputContent, Message, UserContent};
use pi_coding_runtime::{
    BuildSystemPromptOptions, CodingMessageConverter, ContextFile, DocumentationPaths,
    PromptTemplate, Resources, Skill, ToolPromptMetadata, build_coding_agent_harness_system_prompt,
    build_system_prompt, normalize_resources,
};
use serde_json::json;

fn message(value: serde_json::Value) -> AgentMessage {
    serde_json::from_value(value).expect("fixture validates as AgentMessage")
}

fn converter() -> CodingMessageConverter {
    CodingMessageConverter::new().expect("coding schemas compile offline")
}

#[test]
fn source_roles_project_with_exact_wrappers_and_timestamps() {
    let converter = converter();
    let input = vec![
        message(json!({
            "role": "bashExecution", "command": "printf hi", "output": "hi",
            "exitCode": 3, "cancelled": false, "truncated": true,
            "fullOutputPath": "/tmp/full.txt", "timestamp": 101
        })),
        message(json!({
            "role": "custom", "customType": "notice", "content": "hello",
            "display": true, "timestamp": 102
        })),
        message(json!({
            "role": "branchSummary", "summary": "branch text", "fromId": "entry-1",
            "timestamp": 103
        })),
        message(json!({
            "role": "compactionSummary", "summary": "compact text", "tokensBefore": 42,
            "timestamp": 104
        })),
    ];
    let output = converter
        .convert_messages(input)
        .expect("projection succeeds");
    assert_eq!(output.len(), 4);
    let Message::User(bash) = &output[0] else {
        panic!("bash is user")
    };
    assert_eq!(bash.timestamp.get(), 101);
    let UserContent::Blocks(blocks) = &bash.content else {
        panic!("bash blocks")
    };
    let InputContent::Text(text) = &blocks[0] else {
        panic!("bash text")
    };
    assert_eq!(
        text.text,
        "Ran `printf hi`\n```\nhi\n```\n\nCommand exited with code 3\n\n[Output truncated. Full output: /tmp/full.txt]"
    );
    let Message::User(custom) = &output[1] else {
        panic!("custom is user")
    };
    assert_eq!(custom.timestamp.get(), 102);
    let Message::User(branch) = &output[2] else {
        panic!("branch is user")
    };
    let UserContent::Blocks(blocks) = &branch.content else {
        panic!("branch blocks")
    };
    let InputContent::Text(text) = &blocks[0] else {
        panic!("branch text")
    };
    assert_eq!(
        text.text,
        "The following is a summary of a branch that this conversation came back from:\n\n<summary>\nbranch text</summary>"
    );
    let Message::User(compaction) = &output[3] else {
        panic!("compaction is user")
    };
    let UserContent::Blocks(blocks) = &compaction.content else {
        panic!("compaction blocks")
    };
    let InputContent::Text(text) = &blocks[0] else {
        panic!("compaction text")
    };
    assert_eq!(
        text.text,
        "The conversation history before this point was compacted into the following summary:\n\n<summary>\ncompact text\n</summary>"
    );
}

#[test]
fn excluded_bash_never_appears_even_when_fixture_is_mutated() {
    let converter = converter();
    let excluded = message(json!({
        "role": "bashExecution", "command": "secret-command", "output": "secret-output",
        "cancelled": false, "truncated": false, "excludeFromContext": true, "timestamp": 1
    }));
    let mut mutated = excluded.clone();
    if let AgentMessage::Custom(ref mut custom) = mutated {
        let mut fields = custom.fields().as_map().clone();
        fields.insert(
            "output".to_owned(),
            pi_ai::JsonValue::String("mutated-secret".to_owned()),
        );
        mutated = AgentMessage::Custom(
            pi_agent::schema::CustomAgentMessage::new(
                custom.role().clone(),
                pi_ai::JsonObject::new(fields),
            )
            .expect("role and fields remain detached"),
        );
    }
    assert!(
        converter
            .project(excluded)
            .expect("valid excluded message")
            .is_none()
    );
    assert!(
        converter
            .project(mutated)
            .expect("valid mutated excluded message")
            .is_none()
    );
}

#[test]
fn custom_image_content_is_retained_and_schema_rejects_extra_fields() {
    let converter = converter();
    let image = message(json!({
        "role": "custom", "customType": "image", "content": [
            {"type": "text", "text": "look"},
            {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"}
        ], "display": true, "timestamp": 9
    }));
    let Message::User(projected) = converter.project(image).expect("image projects").unwrap()
    else {
        panic!("custom projects to user")
    };
    assert!(
        matches!(projected.content, UserContent::Blocks(blocks) if matches!(blocks[1], InputContent::Image(_)))
    );
    let invalid = message(json!({
        "role": "custom", "customType": "bad", "content": "x", "display": true,
        "timestamp": 9, "unexpected": "reject"
    }));
    assert!(converter.project(invalid).is_err());
}

#[test]
fn builtins_are_preserved_byte_for_byte_including_content() {
    let converter = converter();
    let builtin = message(
        json!({"role":"user","content":[{"type":"image","data":"aA==","mimeType":"image/png"}],"timestamp":7}),
    );
    let expected = match builtin.clone() {
        AgentMessage::BuiltIn(message) => message,
        AgentMessage::Custom(_) => panic!("fixture is built-in"),
    };
    assert_eq!(converter.project(builtin).unwrap(), Some(expected));
}

#[test]
fn prompt_composition_matches_source_order_and_read_gate() {
    let docs = DocumentationPaths {
        readme: "/pi/README.md".to_owned(),
        docs: "/pi/docs".to_owned(),
        examples: "/pi/examples".to_owned(),
    };
    let mut options = BuildSystemPromptOptions::new(r"C:\repo\project", docs);
    options.selected_tools = Some(vec![
        "bash".to_owned(),
        "read".to_owned(),
        "dynamic".to_owned(),
    ]);
    options.tool_snippets = BTreeMap::from([
        ("bash".to_owned(), "Execute\n shell".to_owned()),
        ("dynamic".to_owned(), "Dynamic summary".to_owned()),
    ]);
    options.prompt_guidelines = vec![
        "  first  ".to_owned(),
        "first".to_owned(),
        "second".to_owned(),
    ];
    options.context_files = vec![ContextFile {
        path: "AGENTS.md".to_owned(),
        content: "rules".to_owned(),
    }];
    options.resources = Resources {
        skills: Some(vec![
            Skill {
                name: "visible".to_owned(),
                description: "A & B".to_owned(),
                content: "ignored".to_owned(),
                file_path: "/skills/SKILL.md".to_owned(),
                disable_model_invocation: None,
            },
            Skill {
                name: "hidden".to_owned(),
                description: "hidden".to_owned(),
                content: "ignored".to_owned(),
                file_path: "/skills/h.md".to_owned(),
                disable_model_invocation: Some(true),
            },
        ]),
        prompt_templates: Some(vec![PromptTemplate {
            name: "x".to_owned(),
            content: "template".to_owned(),
            description: None,
        }]),
    };
    options.append_system_prompt = Some("append".to_owned());
    let prompt = build_system_prompt(&options);
    assert!(prompt.contains("- dynamic: Dynamic summary"));
    assert!(!prompt.contains("- read:"));
    assert_eq!(prompt.matches("- first").count(), 1);
    assert!(prompt.contains("<name>visible</name>"));
    assert!(!prompt.contains("<name>hidden</name>"));
    assert!(prompt.contains("Current working directory: C:/repo/project"));
    assert!(!prompt.contains("<prompt_template>"));

    options.selected_tools = Some(vec!["bash".to_owned()]);
    let no_read = build_system_prompt(&options);
    assert!(!no_read.contains("<available_skills>"));
}

#[test]
fn harness_prompt_adapter_normalizes_snippets_and_keeps_active_order() {
    let mut options = BuildSystemPromptOptions::new(
        "/work",
        DocumentationPaths {
            readme: "r".into(),
            docs: "d".into(),
            examples: "e".into(),
        },
    );
    let prompt = build_coding_agent_harness_system_prompt(
        &options,
        &[
            ToolPromptMetadata {
                name: "one".into(),
                prompt_snippet: Some(" one\n two ".into()),
                prompt_guidelines: vec!["g".into()],
            },
            ToolPromptMetadata {
                name: "two".into(),
                prompt_snippet: None,
                prompt_guidelines: vec!["g".into(), "h".into()],
            },
        ],
        &["two".into(), "one".into(), "missing".into()],
    );
    assert!(prompt.contains("Available tools:\n- one: one two"));
    assert!(!prompt.contains("missing"));
    assert_eq!(prompt.matches("- g").count(), 1);
    assert!(prompt.contains("- h"));
    options.custom_prompt = Some("custom".into());
    let custom = build_system_prompt(&options);
    assert!(custom.ends_with("Current working directory: /work\n"));
}

#[test]
fn resources_are_detached_through_existing_schema() {
    let resources = Resources {
        prompt_templates: Some(vec![PromptTemplate {
            name: "p".into(),
            content: "body".into(),
            description: Some("d".into()),
        }]),
        skills: None,
    };
    assert_eq!(normalize_resources(&resources).unwrap(), resources);
}
