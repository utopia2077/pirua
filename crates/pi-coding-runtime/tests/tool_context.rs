use super::*;

fn default_tool_settings() -> CodingSettingsSnapshot {
    let mut configured = settings();
    configured.retry.enabled = false;
    configured.active_tools = ["read", "bash", "edit", "write"]
        .into_iter()
        .map(|name| pi_ai::ToolName::new(name).unwrap())
        .collect();
    configured
}

fn tool_names(messages: &[Message]) -> Vec<String> {
    pi_ai::get_current_tools(messages)
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect()
}

fn declared_tools(session: &CodingSession) -> Vec<String> {
    tool_names(&convert_to_llm(session.messages().unwrap()).unwrap())
}

fn declaration_records(session: &CodingSession) -> Vec<serde_json::Value> {
    session
        .projection()
        .messages
        .into_iter()
        .filter(|message| {
            message["role"] == "system"
                && (message["toolsAdded"].is_array() || message["toolsRemoved"].is_array())
        })
        .collect()
}

#[test]
fn default_tools_reach_the_first_native_read_cycle_without_duplicate_declarations() {
    runtime().block_on(async {
        let root = std::env::temp_dir().join(format!(
            "pirua-initial-tools-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("fixture.txt"), "native tool boundary fixture").unwrap();
        let (stream, requests) = scripted(vec![
            json!({"stopReason":"toolUse","content":[{"type":"toolCall","id":"initial-read","name":"read","arguments":{"path":"fixture.txt"}}]}),
            json!({"content":[{"type":"text","text":"read completed"}]}),
        ]);
        // Match the application's default native coding-tool composition: no
        // replacement tool list and no set_active_tools workaround after build.
        let session = CodingSessionBuilder::new(
            SessionManager::create(Arc::new(MemoryStore::new()), root.to_string_lossy()).unwrap(),
            model(),
            stream,
        )
        .with_settings(default_tool_settings())
        .with_cache_warming(CacheWarmingMode::Off)
        .build()
        .await
        .unwrap();
        let expected = vec!["read", "bash", "edit", "write"];
        assert_eq!(session.active_tool_names(), expected);
        let before = session.messages().unwrap();
        let initial_agent = session.agent().state().messages;
        let (ticket, driver) = session.tracked_prompt("read fixture.txt", PromptOptions::default());
        driver.await.unwrap();
        assert!(matches!(ticket.resolution.await.unwrap(), InputResolution::Inserted { .. }));

        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert_eq!(tool_names(request.context.messages()), expected);
        }
        assert_eq!(serde_json::to_value(initial_agent).unwrap(), serde_json::to_value(before).unwrap());
        let result = requests[1].context.messages().iter().find_map(|message| {
            if let Message::ToolResult(result) = message { Some(result) } else { None }
        }).unwrap();
        assert!(!result.is_error);
        assert_eq!(result.tool_call_id.as_str(), "initial-read");
        assert!(serde_json::to_value(result).unwrap()["content"][0]["text"].as_str().unwrap().contains("native tool boundary fixture"));
        assert_eq!(session.last_assistant_text().as_deref(), Some("read completed"));
        assert_eq!(declared_tools(&session), expected);
        let records = declaration_records(&session);
        assert_eq!(records.len(), 1, "initial tools must be retained in one ordinary system record");
        assert_eq!(records[0]["toolsAdded"].as_array().unwrap().len(), 4);
        assert!(records[0]["toolsRemoved"].is_null());
        session.dispose().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    });
}

#[test]
fn initial_tools_and_later_loadout_removal_stay_anchored_across_reopen() {
    runtime().block_on(async {
        let store = Arc::new(MemoryStore::new());
        let (stream, requests) = scripted(vec![json!({}), json!({}), json!({})]);
        let session = CodingSessionBuilder::new(
            SessionManager::create(store.clone(), "/tmp").unwrap(),
            model(),
            stream,
        )
        .with_settings(default_tool_settings())
        .with_cache_warming(CacheWarmingMode::Off)
        .build()
        .await
        .unwrap();
        session
            .prompt("first", PromptOptions::default())
            .await
            .unwrap();
        let original = declaration_records(&session);
        assert_eq!(original.len(), 1);
        assert_eq!(original[0]["toolsAdded"].as_array().unwrap().len(), 4);
        session.set_active_tools(vec!["read".into()]).unwrap();
        session
            .prompt("restricted", PromptOptions::default())
            .await
            .unwrap();
        session
            .prompt("unchanged", PromptOptions::default())
            .await
            .unwrap();
        let records = declaration_records(&session);
        assert_eq!(records.len(), 2, "unchanged tools must not be redeclared");
        assert_eq!(
            records[0], original[0],
            "the initial declaration stays at its original anchor"
        );
        assert!(records[1]["toolsAdded"].is_null());
        assert_eq!(
            records[1]["toolsRemoved"],
            json!([{"name":"bash"}, {"name":"edit"}, {"name":"write"}])
        );
        let requests = requests.lock().unwrap().clone();
        assert_eq!(
            tool_names(requests[0].context.messages()),
            vec!["read", "bash", "edit", "write"]
        );
        for request in &requests[1..] {
            assert_eq!(tool_names(request.context.messages()), vec!["read"]);
        }
        let id = session.session_id();
        let retained = session.snapshot();
        session.dispose().await.unwrap();

        let (stream, requests) = scripted(vec![json!({})]);
        let reopened =
            CodingSessionBuilder::new(SessionManager::open(store, &id).unwrap(), model(), stream)
                .with_settings(default_tool_settings()) // Restored canonical loadout wins.
                .with_cache_warming(CacheWarmingMode::Off)
                .build()
                .await
                .unwrap();
        assert_eq!(reopened.snapshot().entries.len(), retained.entries.len());
        assert_eq!(reopened.active_tool_names(), vec!["read"]);
        reopened
            .prompt("resumed", PromptOptions::default())
            .await
            .unwrap();
        assert_eq!(
            tool_names(requests.lock().unwrap()[0].context.messages()),
            vec!["read"]
        );
        assert_eq!(declaration_records(&reopened), records);
        assert_eq!(declared_tools(&reopened), vec!["read"]);
        reopened.dispose().await.unwrap();
    });
}
