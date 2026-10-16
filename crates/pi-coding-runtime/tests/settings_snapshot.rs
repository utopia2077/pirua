use pi_ai::{
    ModelId, ModelThinkingLevel, ProviderId, RetryPolicy, SafeUnsigned, StreamOptionsWire, ToolName,
};
use pi_coding_runtime::{
    CodingSettingsSnapshot, CompactionSettings, ModelRef, QueueMode, ThinkingLevel,
};

fn id<T>(value: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    value.parse().expect("validated identity")
}

#[test]
fn snapshot_is_detached_and_preserves_provider_neutral_values() {
    let snapshot = CodingSettingsSnapshot::new(
        ModelRef::new(id::<ProviderId>("openai"), id::<ModelId>("gpt-test")),
        ThinkingLevel::Medium,
        vec![id::<ToolName>("read"), id::<ToolName>("write")],
        RetryPolicy {
            enabled: true,
            max_retries: pi_ai::MaxRetries::new(2).expect("safe retry count"),
            base_delay_ms: SafeUnsigned::new(50).expect("safe delay"),
            max_agent_delay_ms: Some(SafeUnsigned::new(300_000).expect("safe agent delay")),
        },
        CompactionSettings {
            enabled: true,
            reserve_tokens: SafeUnsigned::new(512).expect("safe reserve"),
            keep_recent_tokens: SafeUnsigned::new(2_048).expect("safe tail"),
        },
        StreamOptionsWire::default(),
        QueueMode::OneAtATime,
        QueueMode::All,
    );

    assert_eq!(snapshot.model.provider.as_str(), "openai");
    assert_eq!(snapshot.model.model_id.as_str(), "gpt-test");
    assert_eq!(snapshot.thinking_level, ThinkingLevel::Medium);
    assert_eq!(snapshot.active_tools[0].as_str(), "read");
    assert_eq!(snapshot.active_tools[1].as_str(), "write");
    assert_eq!(snapshot.steering_mode, QueueMode::OneAtATime);
    assert_eq!(snapshot.follow_up_mode, QueueMode::All);

    let cloned = snapshot.clone();
    assert_eq!(cloned, snapshot);
}

#[test]
fn model_ref_rejects_empty_identities() {
    assert!("".parse::<ProviderId>().is_err());
    assert!("".parse::<ModelId>().is_err());
}

#[test]
fn thinking_alias_keeps_existing_model_levels() {
    let level: ThinkingLevel = ModelThinkingLevel::Off;
    assert_eq!(level, ThinkingLevel::Off);
}
