use pi_agent_schema::{
    AgentHarnessStreamOptions, AgentHarnessStreamOptionsPatch, ProviderHeaderClassificationError,
    ProviderHeaderPolicy, RawProviderHeaderPatch, RawProviderHeaders,
};
use pi_ai_core::{JsonObject, Presence};

/// Apply one complete request patch atomically under the current host's policy.
/// Exact header spelling and set/delete/null distinctions are preserved.
pub fn apply_stream_options_patch(
    base: &AgentHarnessStreamOptions,
    patch: &AgentHarnessStreamOptionsPatch,
    policy: &ProviderHeaderPolicy,
) -> Result<AgentHarnessStreamOptions, ProviderHeaderClassificationError> {
    let mut candidate = base.clone();
    macro_rules! field {
        ($($field:ident),+) => { $(if let Some(value) = &patch.$field {
            candidate.$field = Some(value.clone());
        })+ };
    }
    field!(
        transport,
        timeout_ms,
        max_retries,
        max_retry_delay_ms,
        cache_retention,
        deferred
    );
    match &patch.headers {
        Presence::Missing => (),
        Presence::Null => candidate.headers = None,
        Presence::Value(patch) => {
            // Validate deletion keys too, even when absent from the final map.
            RawProviderHeaderPatch::new(patch.as_map().clone())?.classify(policy)?;
            let mut headers = candidate
                .headers
                .as_ref()
                .map(|h| h.as_map().clone())
                .unwrap_or_default();
            for (key, value) in patch.as_map() {
                if let Some(value) = value {
                    headers.insert(key.clone(), value.clone());
                } else {
                    headers.remove(key);
                }
            }
            candidate.headers = Some(RawProviderHeaders::new(headers)?.classify(policy)?);
        }
    }
    match &patch.metadata {
        Presence::Missing => (),
        Presence::Null => candidate.metadata = None,
        Presence::Value(patch) => {
            let mut metadata = candidate
                .metadata
                .as_ref()
                .map(|m| m.as_map().clone())
                .unwrap_or_default();
            for key in patch.delete() {
                metadata.remove(key);
            }
            metadata.extend(patch.set().as_map().clone());
            candidate.metadata = Some(JsonObject::new(metadata));
        }
    }
    if let Some(headers) = &candidate.headers {
        RawProviderHeaders::new(headers.as_map().clone())?.classify(policy)?;
    }
    Ok(candidate)
}
