use pi_ai_core::{AuthContext, AuthContextError, BoxFuture, CancellationToken};
use std::{fmt, sync::Arc};
/// Source default-context whitespace policy over explicit host operations.
pub struct DefaultProviderAuthContext {
    host: Arc<dyn AuthContext>,
}
impl DefaultProviderAuthContext {
    pub fn new(host: Arc<dyn AuthContext>) -> Self {
        Self { host }
    }
}
impl fmt::Debug for DefaultProviderAuthContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DefaultProviderAuthContext([REDACTED])")
    }
}
impl AuthContext for DefaultProviderAuthContext {
    fn env<'a>(
        &'a self,
        name: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(AuthContextError::new());
            }
            let value = self.host.env(name, c).await?;
            if c.is_cancelled() {
                return Err(AuthContextError::new());
            }
            Ok(value.filter(|v| !v.trim_matches(ecma_whitespace).is_empty()))
        })
    }
    fn file_exists<'a>(
        &'a self,
        path: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(AuthContextError::new());
            }
            let found = self.host.file_exists(path, c).await.unwrap_or(false);
            if c.is_cancelled() {
                return Err(AuthContextError::new());
            }
            Ok(found)
        })
    }
}
fn ecma_whitespace(c: char) -> bool {
    matches!(c,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
/// Explicit OS context. Callers supply home-directory expansion; no discovery occurs.
#[cfg(feature = "native")]
pub struct NativeProviderAuthContext {
    home_directory: Option<String>,
}
#[cfg(feature = "native")]
impl NativeProviderAuthContext {
    pub fn new(home_directory: Option<String>) -> Self {
        Self { home_directory }
    }
}
#[cfg(feature = "native")]
impl fmt::Debug for NativeProviderAuthContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeProviderAuthContext([REDACTED])")
    }
}
#[cfg(feature = "native")]
impl AuthContext for NativeProviderAuthContext {
    fn env<'a>(
        &'a self,
        name: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(AuthContextError::new());
            }
            Ok(std::env::var(name).ok())
        })
    }
    fn file_exists<'a>(
        &'a self,
        path: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async move {
            if c.is_cancelled() {
                return Err(AuthContextError::new());
            }
            let resolved = if let Some(rest) = path.strip_prefix('~') {
                let Some(home) = &self.home_directory else {
                    return Ok(false);
                };
                format!("{home}{rest}")
            } else {
                path.to_owned()
            };
            Ok(std::fs::metadata(resolved).is_ok())
        })
    }
}
