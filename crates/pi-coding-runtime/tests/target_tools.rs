//! Offline expectations captured by running the exact Pi v1.4.0 source.
use pi_ai::{Model, ModelThinkingLevel};
use pi_coding_runtime::{provider_composition::*, resources::*, system_prompt::*, tools::*};
use serde_json::{Value, json};

fn model(value: &Value) -> Model {
    serde_json::from_value(json!({
        "provider":value["provider"],"id":value["id"],"name":value["name"],
        "api":"openai-completions", "baseUrl":"https://example.invalid/v1", "reasoning":true,
        "input":["text","image"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},
        "contextWindow":128000,"maxTokens":8192
    }))
    .unwrap()
}
fn skill(value: &Value) -> Skill {
    Skill {
        name: value["name"].as_str().unwrap().into(),
        description: value["description"].as_str().unwrap().into(),
        file_path: value["filePath"].as_str().unwrap().into(),
        content: String::new(),
        disable_model_invocation: value["disableModelInvocation"].as_bool(),
    }
}
fn map_strings(value: &Value) -> std::collections::BTreeMap<String, String> {
    value
        .as_object()
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap().into()))
        .collect()
}
fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| item.as_str().unwrap().into())
        .collect()
}
fn small_model(model: &Model) -> Value {
    json!({"provider":model.provider,"id":model.id,"name":model.name})
}
fn reference_result(result: ModelReferenceResult<'_>) -> Value {
    match result {
        ModelReferenceResult::Resolved {
            model,
            thinking_level,
            warning,
        } => {
            let mut value = json!({"status":"resolved","model":small_model(model)});
            if let Some(level) = thinking_level {
                value["thinkingLevel"] = json!(level);
            }
            if let Some(warning) = warning {
                value["warning"] = json!(warning);
            }
            value
        }
        ModelReferenceResult::Ambiguous {
            candidates,
            usable_count,
            thinking_level,
            warning,
        } => {
            let mut value = json!({"status":"ambiguous","candidates":candidates.into_iter().map(small_model).collect::<Vec<_>>(),"usableCount":usable_count});
            if let Some(level) = thinking_level {
                value["thinkingLevel"] = json!(level);
            }
            if let Some(warning) = warning {
                value["warning"] = json!(warning);
            }
            value
        }
        ModelReferenceResult::NotFound { target } => {
            let mut value = json!({"status":"not-found"});
            if let Some(target) = target {
                let mut target_value = json!({"provider":target.provider,"providerModels":target.provider_models.into_iter().map(small_model).collect::<Vec<_>>(),"modelText":target.model_text,"modelId":target.model_id});
                if let Some(level) = target.thinking_level {
                    target_value["thinkingLevel"] = json!(level);
                }
                value["target"] = target_value;
            }
            value
        }
    }
}

#[test]
fn pinned_source_fixtures_cover_truncation_resources_prompt_and_model_resolution() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../../../conformance/coding-tools/cases.json")).unwrap();
    let trace: Value =
        serde_json::from_str(include_str!("../../../conformance/coding-tools/trace.json")).unwrap();
    assert_eq!(
        trace["upstreamCommit"],
        "4a080252f06caf63a525d7873c5d94d3c23887e0"
    );
    for row in cases {
        let id = row["id"].as_str().unwrap();
        let actual = match row["kind"].as_str().unwrap() {
            "truncate" => {
                let text = row["text"]
                    .as_str()
                    .unwrap()
                    .repeat(row["repeat"].as_u64().unwrap_or(1) as usize);
                let (content, truncation) = if row["tail"] == true {
                    truncate_tail(&text)
                } else {
                    truncate_head(&text)
                };
                let mut value = serde_json::to_value(truncation).unwrap();
                value["content"] = json!(content);
                value
            }
            "args" => {
                let parsed = parse_command_args(row["arguments"].as_str().unwrap());
                json!({"expanded":substitute_args(row["template"].as_str().unwrap(),&parsed),"parsed":parsed})
            }
            "skills" => {
                let skills: Vec<_> = row["skills"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(skill)
                    .collect();
                json!(format_skills_for_prompt_with_reader(
                    &skills,
                    match row["reader"].as_str().unwrap() {
                        "bash" => SkillFileReader::Bash,
                        "indirect" => SkillFileReader::Indirect,
                        _ => SkillFileReader::Read,
                    }
                ))
            }
            "prompt" => {
                let input = &row["options"];
                let mut options = BuildSystemPromptOptions::new(
                    input["cwd"].as_str().unwrap(),
                    DocumentationPaths {
                        readme: "/pi/README.md".into(),
                        docs: "/pi/docs".into(),
                        examples: "/pi/examples".into(),
                    },
                );
                options.custom_prompt = input["customPrompt"].as_str().map(str::to_owned);
                options.force_system_prompt =
                    input["forceSystemPrompt"].as_str().map(str::to_owned);
                options.append_system_prompt =
                    input["appendSystemPrompt"].as_str().map(str::to_owned);
                options.selected_tools = input.get("selectedTools").map(strings);
                options.hidden_tools = strings(&input["hiddenTools"]);
                options.tool_snippets = map_strings(&input["toolSnippets"]);
                options.tool_guidelines = input["toolGuidelines"]
                    .as_object()
                    .into_iter()
                    .flatten()
                    .map(|(key, value)| (key.clone(), strings(value)))
                    .collect();
                options.prompt_guidelines = strings(&input["promptGuidelines"]);
                options.sections = map_strings(&input["sections"]).into_iter().collect();
                options.context_files = input["contextFiles"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|file| serde_json::from_value(file.clone()).unwrap())
                    .collect();
                options.resources.skills = Some(
                    input["skills"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(skill)
                        .collect(),
                );
                match build_system_prompt_state(&options) {
                    Ok(state) => {
                        let mut value = json!({"content":state.content});
                        if let Some(sections) = state.sections {
                            value["sections"] = serde_json::to_value(sections).unwrap();
                        }
                        value
                    }
                    Err(error) => json!({"error":error.to_string()}),
                }
            }
            "edit" => {
                let dir = std::env::temp_dir()
                    .join(format!("pirua-target-edit-{}-{id}", std::process::id()));
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("fixture.txt"), row["content"].as_str().unwrap()).unwrap();
                let tool =
                    pi_coding_runtime::create_edit_tool(pi_coding_runtime::ExecutionToolContext {
                        env: std::sync::Arc::new(pi_coding_runtime::LocalExecutionEnv::new(&dir)),
                    });
                let arguments =
                    serde_json::from_value(json!({"path":"fixture.txt","edits":row["edits"]}))
                        .unwrap();
                let result = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on((tool.execute)(pi_agent_schema::ToolInvocation {
                        call_id: id.into(),
                        arguments,
                        cancellation: pi_ai::CancellationToken::new(),
                        on_update: std::sync::Arc::new(|_| {}),
                    }));
                let value = match result {
                    Ok(result) => {
                        let details = serde_json::to_value(result.details).unwrap();
                        json!({"baseContent":row["content"],"newContent":std::fs::read_to_string(dir.join("fixture.txt")).unwrap(),"diff":details["diff"],"patch":details["patch"],"firstChangedLine":details["firstChangedLine"]})
                    }
                    Err(error) => {
                        assert_eq!(
                            std::fs::read_to_string(dir.join("fixture.txt")).unwrap(),
                            row["content"].as_str().unwrap()
                        );
                        json!({"error":error.to_string()})
                    }
                };
                std::fs::remove_dir_all(&dir).unwrap();
                value
            }
            "default-model" => {
                let models = row["models"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(model)
                    .collect();
                let composition = ProviderComposition::new(ProviderCompositionInputs {
                    models,
                    ..Default::default()
                })
                .unwrap();
                let mut value = json!({"thinkingLevel":"medium"});
                if let Some(model) = composition.select_default_model() {
                    value["model"] = small_model(model);
                }
                value
            }
            "model" => {
                let models: Vec<_> = row["models"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(model)
                    .collect();
                let request = &row["request"];
                reference_result(resolve_model_reference(
                    &ModelReferenceRequest {
                        text: request["text"].as_str().unwrap(),
                        provider: request["provider"].as_str(),
                        fuzzy: request["fuzzy"].as_bool().unwrap(),
                        thinking_suffix: match request["thinkingSuffix"].as_str().unwrap() {
                            "none" => ThinkingSuffix::None,
                            "lenient" => ThinkingSuffix::Lenient,
                            _ => ThinkingSuffix::Strict,
                        },
                    },
                    &models,
                    |provider| {
                        row["usable"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|value| value == provider.as_str())
                    },
                ))
            }
            other => panic!("unhandled fixture {other}"),
        };
        assert_eq!(actual, trace["observations"][id], "fixture {id}");
    }
}

#[test]
fn prompt_sections_preserve_order_diff_removals_and_opaque_empty_force() {
    let mut options = BuildSystemPromptOptions::new(
        "/work",
        DocumentationPaths {
            readme: "readme".into(),
            docs: "docs".into(),
            examples: "examples".into(),
        },
    );
    options.custom_prompt = Some("custom".into());
    options.sections = vec![("zeta".into(), "z".into()), ("alpha".into(), "a".into())];
    let previous = build_system_prompt_sections(&options).unwrap();
    assert_eq!(
        previous
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["preamble", "cwd", "zeta", "alpha"]
    );
    assert!(diff_system_prompt_sections(&previous, &previous).is_none());
    options.sections = vec![("alpha".into(), "new".into())];
    let current = build_system_prompt_sections(&options).unwrap();
    let patch = diff_system_prompt_sections(&previous, &current).unwrap();
    assert_eq!(
        serde_json::to_value(patch).unwrap(),
        json!({"alpha":"<alpha>\nnew\n</alpha>","zeta":null})
    );
    options.force_system_prompt = Some(String::new());
    assert_eq!(build_system_prompt(&options).unwrap(), "");
}

#[test]
fn open_configured_models_overlay_metadata_and_credentials_remain_detached() {
    let mut configured =
        model(&json!({"provider":"private-gateway","id":"team/model:variant","name":"private"}));
    configured.reasoning = false;
    let composition = ProviderComposition::new(ProviderCompositionInputs {
        models: vec![configured.clone()],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        composition
            .resolve_reference("private-gateway/team/model:variant")
            .unwrap(),
        &configured
    );
    assert_eq!(
        composition
            .resolve_selection("private-gateway/team/model:variant:high")
            .unwrap()
            .thinking_level,
        ModelThinkingLevel::High
    );
    assert_eq!(
        pi_coding_runtime::selected_text_provider_ids().collect::<Vec<_>>(),
        ["openai", "google", "anthropic", "deepseek"]
    );
    for excluded in ["openrouter", "openai-codex", "azure", "private-gateway"] {
        assert!(
            pi_coding_runtime::builtin_models(excluded)
                .unwrap()
                .is_empty()
        );
    }
}
