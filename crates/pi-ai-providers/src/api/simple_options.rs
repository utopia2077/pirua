//! Pinned provider-neutral simple request normalization.
//! Runtime, credentials and middleware stay on the nominal resolved request.

use pi_ai_core::{
    Context, ContextEstimateInput, JsonObject, Model, NumericError, SafeUnsigned,
    StreamOptionsWire, ThinkingBudgets, ThinkingLevel, estimate_context_tokens,
};

pub const MIN_ANSWER_TOKENS: u64 = 1024;
pub const DEFAULT_THINKING_BUDGETS: ThinkingBudgets = ThinkingBudgets {
    minimal: Some(safe(1024)),
    low: Some(safe(2048)),
    medium: Some(safe(8192)),
    high: Some(safe(16384)),
};

const fn safe(value: u64) -> SafeUnsigned {
    match SafeUnsigned::new(value) {
        Ok(value) => value,
        Err(_) => panic!("constant safe count"),
    }
}

pub fn clamp_max_tokens_to_context(
    model: &Model,
    context: &Context,
    max_tokens: SafeUnsigned,
) -> Result<SafeUnsigned, NumericError> {
    if model.context_window.get() == 0 {
        return SafeUnsigned::new(max_tokens.get().max(1));
    }
    let estimate = estimate_context_tokens(ContextEstimateInput::Context(context))?;
    SafeUnsigned::new(
        max_tokens.get().min(
            model
                .context_window
                .get()
                .saturating_sub(estimate.tokens.get())
                .saturating_sub(4096)
                .max(1),
        ),
    )
}

/// Named neutral options are cloned; only sampling merge and token cap change.
pub fn build_base_options(
    model: &Model,
    context: &Context,
    options: &StreamOptionsWire,
) -> Result<StreamOptionsWire, NumericError> {
    let mut result = options.clone();
    if model.sampling_params.is_some() || options.sampling_params.is_some() {
        let mut sampling = model
            .sampling_params
            .as_ref()
            .map(|v| v.as_map().clone())
            .unwrap_or_default();
        if let Some(overrides) = &options.sampling_params {
            sampling.extend(overrides.as_map().clone());
        }
        result.sampling_params = Some(JsonObject::new(sampling));
    }
    result.max_tokens = Some(clamp_max_tokens_to_context(
        model,
        context,
        options.max_tokens.unwrap_or(model.max_tokens),
    )?);
    Ok(result)
}

pub fn clamp_reasoning(effort: Option<ThinkingLevel>) -> Option<ThinkingLevel> {
    effort.map(|level| match level {
        ThinkingLevel::ExtraHigh | ThinkingLevel::Max => ThinkingLevel::High,
        _ => level,
    })
}

pub fn thinking_budget_for_level(
    level: ThinkingLevel,
    custom: Option<&ThinkingBudgets>,
) -> SafeUnsigned {
    let budgets = custom.unwrap_or(&DEFAULT_THINKING_BUDGETS);
    match level {
        ThinkingLevel::Minimal => budgets.minimal.unwrap_or(safe(1024)),
        ThinkingLevel::Low => budgets.low.unwrap_or(safe(2048)),
        ThinkingLevel::Medium => budgets.medium.unwrap_or(safe(8192)),
        ThinkingLevel::High | ThinkingLevel::ExtraHigh | ThinkingLevel::Max => {
            budgets.high.unwrap_or(safe(16384))
        }
    }
}

pub fn clamp_thinking_budget_to_answer_room(
    thinking_budget: SafeUnsigned,
    ceiling: SafeUnsigned,
) -> SafeUnsigned {
    safe(
        thinking_budget
            .get()
            .min(ceiling.get().saturating_sub(MIN_ANSWER_TOKENS)),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdjustedThinkingTokens {
    pub max_tokens: SafeUnsigned,
    pub thinking_budget: SafeUnsigned,
}

pub fn adjust_max_tokens_for_thinking(
    base_max_tokens: Option<SafeUnsigned>,
    model_max_tokens: SafeUnsigned,
    reasoning_level: ThinkingLevel,
    custom_budgets: Option<&ThinkingBudgets>,
) -> AdjustedThinkingTokens {
    let mut thinking_budget = thinking_budget_for_level(reasoning_level, custom_budgets);
    // Both operands are JS-safe. If their sum exceeds that domain, min with the
    // safe model ceiling selects the ceiling regardless of JS rounding.
    let max_tokens = base_max_tokens.map_or(model_max_tokens, |base| {
        safe((base.get() + thinking_budget.get()).min(model_max_tokens.get()))
    });
    if max_tokens <= thinking_budget {
        thinking_budget = clamp_thinking_budget_to_answer_room(thinking_budget, max_tokens);
    }
    AdjustedThinkingTokens {
        max_tokens,
        thinking_budget,
    }
}
