use super::LegacyError;
use pi_agent_schema::session_values::{FiniteF64, SafeUnsigned, Usage};
fn add(a: SafeUnsigned, b: SafeUnsigned) -> Result<SafeUnsigned, LegacyError> {
    SafeUnsigned::new(a.get() + b.get()).map_err(|_| LegacyError::Overflow)
}
fn optional(
    a: Option<SafeUnsigned>,
    b: Option<SafeUnsigned>,
) -> Result<Option<SafeUnsigned>, LegacyError> {
    match (a, b) {
        (None, None) => Ok(None),
        (Some(v), None) | (None, Some(v)) => Ok(Some(v)),
        (Some(a), Some(b)) => Ok(Some(add(a, b)?)),
    }
}
fn cost(a: FiniteF64, b: FiniteF64) -> Result<FiniteF64, LegacyError> {
    FiniteF64::new(a.get() + b.get()).map_err(|_| LegacyError::Overflow)
}
pub(super) fn accumulate(total: &mut Usage, value: &Usage) -> Result<(), LegacyError> {
    total.input = add(total.input, value.input)?;
    total.output = add(total.output, value.output)?;
    total.cache_read = add(total.cache_read, value.cache_read)?;
    total.cache_write = add(total.cache_write, value.cache_write)?;
    total.total_tokens = add(total.total_tokens, value.total_tokens)?;
    total.cache_write_1h = optional(total.cache_write_1h, value.cache_write_1h)?;
    total.reasoning = optional(total.reasoning, value.reasoning)?;
    total.cost.input = cost(total.cost.input, value.cost.input)?;
    total.cost.output = cost(total.cost.output, value.cost.output)?;
    total.cost.cache_read = cost(total.cost.cache_read, value.cost.cache_read)?;
    total.cost.cache_write = cost(total.cost.cache_write, value.cost.cache_write)?;
    total.cost.total = cost(total.cost.total, value.cost.total)?;
    Ok(())
}
