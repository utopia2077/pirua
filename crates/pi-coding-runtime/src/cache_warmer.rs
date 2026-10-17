//! Best-effort cache refreshes, bounded by the release's economics and safety windows.
use crate::{
    CodingSession,
    hooks::SessionEvent,
    session::{Inner, lock},
};
use pi_agent::{AgentRequest, CancellationToken};
use pi_ai::{CacheRetention, Model, SafeUnsigned, SimpleStreamOptionsWire, StopReason, Usage};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheWarmingMode {
    Off,
    #[default]
    Streaming,
    Idle,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheWarmingAction {
    Warm,
    Stop,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheWarmingPhase {
    Streaming,
    Idle,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CacheWarmingDecision {
    pub phase: CacheWarmingPhase,
    pub warm_cost: f64,
    pub miss_cost: f64,
    pub continuation_probability: f64,
    pub expected_savings: f64,
    pub economics_available: bool,
    pub action: CacheWarmingAction,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CacheWarmingStatus {
    pub state: CacheWarmingState,
    pub reason: Option<String>,
    pub next_warm_at: Option<u64>,
    pub decision: Option<CacheWarmingDecision>,
    pub extension_override: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheWarmingState {
    Inactive,
    Scheduled,
    Refreshing,
}
pub fn cache_warming_delay_ms(ttl: u64) -> Option<u64> {
    (ttl > 10000).then(|| ((ttl as f64 * 0.9).floor() as u64).min(ttl - 10000).max(1))
}
pub fn prompt_cache_ttl_ms(model: &Model, options: &SimpleStreamOptionsWire) -> Option<u64> {
    let retention = options
        .stream
        .cache_retention
        .unwrap_or(CacheRetention::Short);
    let cache = model.prompt_cache.as_ref()?;
    let seconds = match retention {
        CacheRetention::None => None,
        CacheRetention::Short => cache.short,
        CacheRetention::Long => cache.long,
    }?;
    Some((seconds.get() * 1000.0) as u64)
}
pub fn is_cache_replayable(model: &Model, options: &SimpleStreamOptionsWire) -> bool {
    options.reasoning.is_none()
        || model.api.as_str() != "anthropic-messages"
        || model.compat.as_ref().is_some_and(|c| {
            serde_json::to_value(c)
                .ok()
                .is_some_and(|v| v["forceAdaptiveThinking"] == true)
        })
}
pub fn evaluate_cache_warming(
    model: &Model,
    prompt_tokens: u64,
    phase: CacheWarmingPhase,
) -> CacheWarmingDecision {
    let price = |cache_read: u64, cache_write: u64, input: u64, output: u64| {
        let mut usage = Usage::zero();
        usage.cache_read = SafeUnsigned::new(cache_read).expect("safe usage");
        usage.cache_write = SafeUnsigned::new(cache_write).expect("safe usage");
        usage.input = SafeUnsigned::new(input).expect("safe usage");
        usage.output = SafeUnsigned::new(output).expect("safe usage");
        pi_ai::calculate_cost(model, &mut usage)
            .map(|c| c.total.get())
            .unwrap_or(0.0)
    };
    let hit = price(prompt_tokens, 0, 0, 0);
    let miss = if model.cost.cache_write.get() > 0.0 {
        price(0, prompt_tokens, 0, 0)
    } else {
        price(0, 0, prompt_tokens, 0)
    };
    let warm_cost = price(prompt_tokens, 0, 0, 1);
    let miss_cost = (miss - hit).max(0.0);
    let continuation_probability = if phase == CacheWarmingPhase::Idle {
        0.15
    } else {
        1.0
    };
    let expected_savings = continuation_probability * miss_cost - warm_cost;
    CacheWarmingDecision {
        phase,
        warm_cost,
        miss_cost,
        continuation_probability,
        expected_savings,
        economics_available: prompt_tokens > 0 && (hit > 0.0 || miss > 0.0),
        action: if expected_savings >= 0.05 {
            CacheWarmingAction::Warm
        } else {
            CacheWarmingAction::Stop
        },
    }
}
pub(crate) struct Warmer {
    pub mode: CacheWarmingMode,
    pub status: CacheWarmingStatus,
    pub token: Option<CancellationToken>,
    pub generation: u64,
    pub phase: CacheWarmingPhase,
    pub started_at: Option<u64>,
    observation: Option<WarmObservation>,
}
#[derive(Clone)]
struct WarmObservation {
    model: Model,
    messages: Vec<serde_json::Value>,
    thinking_level: pi_ai::ModelThinkingLevel,
}
impl Warmer {
    pub fn new(mode: CacheWarmingMode) -> Self {
        Self {
            mode,
            status: inactive("waiting for first request"),
            token: None,
            generation: 0,
            phase: CacheWarmingPhase::Streaming,
            started_at: None,
            observation: None,
        }
    }
    pub fn stop(&mut self, reason: &str) {
        if let Some(token) = self.token.take() {
            token.cancel();
        }
        self.generation += 1;
        self.status = inactive(reason);
        self.started_at = None;
        self.observation = None;
    }
}
fn inactive(reason: &str) -> CacheWarmingStatus {
    CacheWarmingStatus {
        state: CacheWarmingState::Inactive,
        reason: Some(reason.into()),
        next_warm_at: None,
        decision: None,
        extension_override: false,
    }
}
impl CodingSession {
    pub fn cache_warming_status(&self) -> CacheWarmingStatus {
        let (mut status, phase, observation) = {
            let warmer = lock(&self.inner.warmer);
            (
                warmer.status.clone(),
                warmer.phase,
                warmer.observation.clone(),
            )
        };
        let Some(observation) = observation else {
            return status;
        };
        let model = self.model();
        if self.is_disposed()
            || model.provider != observation.model.provider
            || model.id != observation.model.id
            || self.thinking_level() != observation.thinking_level
            || !self
                .projection()
                .messages
                .starts_with(&observation.messages)
        {
            return inactive("conversation context changed");
        }
        let decision = evaluate_cache_warming(&observation.model, self.last_prompt_tokens(), phase);
        if status.state == CacheWarmingState::Scheduled && !decision.economics_available {
            return inactive("cache economics unavailable");
        }
        status.decision = Some(decision);
        status
    }
    pub fn set_cache_warming_mode(&self, mode: CacheWarmingMode) {
        let mut w = lock(&self.inner.warmer);
        w.mode = mode;
        if mode == CacheWarmingMode::Off {
            w.stop("cache warming disabled");
        } else if mode == CacheWarmingMode::Streaming && w.phase == CacheWarmingPhase::Idle {
            w.stop("agent run settled");
        }
    }
    pub(crate) fn cancel_cache_warming(&self) {
        lock(&self.inner.warmer).stop("conversation context changed");
    }
    pub(crate) fn on_cache_settled(&self) {
        let mut w = lock(&self.inner.warmer);
        if w.mode == CacheWarmingMode::Streaming {
            w.stop("agent run settled");
        } else {
            w.phase = CacheWarmingPhase::Idle;
            if let Some(started) = w.started_at
                && w.status
                    .next_warm_at
                    .is_some_and(|next| next > started + 1800000)
            {
                w.stop("30-minute idle safety limit reached");
            }
        }
    }
    fn last_prompt_tokens(&self) -> u64 {
        lock(&self.inner.manager)
            .get_branch()
            .iter()
            .rev()
            .find_map(|entry| {
                if let pi_coding_session::EntryContent::Message { message } = &entry.content {
                    (message["role"] == "assistant").then(|| {
                        ["input", "cacheRead", "cacheWrite"]
                            .iter()
                            .map(|key| message["usage"][key].as_u64().unwrap_or(0))
                            .sum()
                    })
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }
    pub(crate) fn start_cache_warming(&self, request: AgentRequest) {
        let selected = self.model();
        let expected = self.projection().messages;
        let thinking_level = self.thinking_level();
        let mut warmer = lock(&self.inner.warmer);
        warmer.stop("inactive");
        if selected.provider != request.model.provider || selected.id != request.model.id {
            warmer.status = inactive("conversation context changed");
            return;
        }
        warmer.phase = CacheWarmingPhase::Streaming;
        if warmer.mode == CacheWarmingMode::Off {
            warmer.status = inactive("cache warming disabled");
            return;
        }
        if !is_cache_replayable(&request.model, &request.options) {
            warmer.status = inactive("request cannot be replayed safely");
            return;
        }
        let Some(ttl) = prompt_cache_ttl_ms(&request.model, &request.options) else {
            warmer.status = inactive("cache lifetime unavailable");
            return;
        };
        let Some(delay) = cache_warming_delay_ms(ttl) else {
            warmer.status = inactive("cache lifetime unavailable");
            return;
        };
        let token = CancellationToken::new();
        warmer.token = Some(token.clone());
        let generation = warmer.generation;
        let started = crate::messages::now();
        if delay > 3600000 {
            warmer.stop("one-hour safety limit reached");
            return;
        }
        warmer.started_at = Some(started);
        warmer.observation = Some(WarmObservation {
            model: request.model.clone(),
            messages: expected.clone(),
            thinking_level,
        });
        warmer.status = CacheWarmingStatus {
            state: CacheWarmingState::Scheduled,
            reason: None,
            next_warm_at: Some(started + delay),
            decision: None,
            extension_override: false,
        };
        drop(warmer);
        let weak = Arc::downgrade(&self.inner);
        let selection = (selected.provider, selected.id, thinking_level);
        tokio::spawn(async move {
            warm_loop(
                weak, request, expected, selection, token, generation, started, ttl, delay,
            )
            .await;
        });
    }
}
#[allow(clippy::too_many_arguments)]
async fn warm_loop(
    weak: Weak<Inner>,
    request: AgentRequest,
    expected: Vec<serde_json::Value>,
    selection: (pi_ai::ProviderId, pi_ai::ModelId, pi_ai::ModelThinkingLevel),
    token: CancellationToken,
    generation: u64,
    started: u64,
    ttl: u64,
    delay: u64,
) {
    loop {
        if !crate::session_lifecycle::sleep_abortable(delay, &token).await {
            return;
        }
        let Some(inner) = weak.upgrade() else {
            return;
        };
        let s = CodingSession { inner };
        let now = crate::messages::now();
        let phase = lock(&s.inner.warmer).phase;
        let horizon = if phase == CacheWarmingPhase::Idle {
            1800000
        } else {
            3600000
        };
        let current = |s: &CodingSession| {
            let w = lock(&s.inner.warmer);
            if w.generation != generation || w.mode == CacheWarmingMode::Off {
                return false;
            }
            drop(w);
            let model = s.model();
            let messages = s.projection().messages;
            model.provider == selection.0
                && model.id == selection.1
                && s.thinking_level() == selection.2
                && messages.starts_with(&expected)
                && !s.is_disposed()
        };
        if !current(&s) || now > started + horizon {
            let mut w = lock(&s.inner.warmer);
            if w.generation == generation {
                w.stop("cache warming safety limit or context changed");
            }
            return;
        }
        let deadline = lock(&s.inner.warmer).status.next_warm_at.unwrap_or(now) + (ttl - delay) / 2;
        if now > deadline {
            lock(&s.inner.warmer).stop("cache refresh deadline missed");
            return;
        }
        let prompt_tokens = s.last_prompt_tokens();
        let decision = evaluate_cache_warming(&request.model, prompt_tokens, phase);
        let action = s
            .inner
            .hooks
            .cache_warming_decision(decision.clone())
            .await
            .unwrap_or(decision.action);
        let override_action = action != decision.action;
        if !current(&s) {
            return;
        }
        if action == CacheWarmingAction::Stop {
            let mut w = lock(&s.inner.warmer);
            w.stop(if override_action {
                "stopped by extension"
            } else if decision.economics_available {
                "expected savings below threshold"
            } else {
                "cache economics unavailable"
            });
            w.status.decision = Some(decision);
            w.status.extension_override = override_action;
            return;
        }
        {
            let mut w = lock(&s.inner.warmer);
            w.status.state = CacheWarmingState::Refreshing;
            w.status.decision = Some(decision);
            w.status.extension_override = override_action;
        }
        let mut warm = request.clone();
        warm.options.stream.max_tokens = Some(SafeUnsigned::new(1).expect("one"));
        warm.options.stream.max_retries = Some(SafeUnsigned::new(0).expect("zero"));
        warm.cancellation = token.clone();
        if let Ok(stream) = (s.inner.stream)(warm).await
            && let message = stream.result().await
            && current(&s)
            && !matches!(message.stop_reason, StopReason::Error | StopReason::Aborted)
        {
            let result = s.commit_contents(vec![pi_coding_session::EntryContent::Usage {
                kind: "cache_warm".into(),
                provider: message.provider.to_string(),
                model: message.response_model.unwrap_or(message.model).to_string(),
                usage: serde_json::to_value(message.usage).expect("usage"),
                note: override_action.then(|| "extension override".into()),
            }]);
            if let Ok(entries) = result {
                for entry in entries {
                    let _ = s.emit(SessionEvent::CacheWarmed { entry }).await;
                }
            }
        }
        if !current(&s) {
            return;
        }
        let mut w = lock(&s.inner.warmer);
        let next = crate::messages::now() + delay;
        let horizon = if w.phase == CacheWarmingPhase::Idle {
            1800000
        } else {
            3600000
        };
        if next > started + horizon {
            let reason = if w.phase == CacheWarmingPhase::Idle {
                "30-minute idle safety limit reached"
            } else {
                "one-hour safety limit reached"
            };
            w.stop(reason);
            return;
        }
        w.status.state = CacheWarmingState::Scheduled;
        w.status.next_warm_at = Some(next);
        drop(w);
        drop(s);
    }
}
