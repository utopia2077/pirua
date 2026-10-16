use std::{error::Error, fmt};
/// Sanitized factory construction failure, retaining its typed source.
pub struct ProviderFactoryError(Box<dyn Error + Send + Sync>);
impl ProviderFactoryError {
    #[cfg(any(
        feature = "all-providers",
        feature = "provider-anthropic",
        feature = "provider-deepseek",
        feature = "provider-google",
        feature = "provider-openai",
    ))]
    pub(super) fn from_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(error))
    }
}
impl fmt::Debug for ProviderFactoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderFactoryError([REDACTED])")
    }
}
impl fmt::Display for ProviderFactoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("provider construction failed")
    }
}
impl Error for ProviderFactoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
