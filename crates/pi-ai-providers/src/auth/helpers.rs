use pi_ai_core::{
    ApiKeyAuth, ApiKeyAuthInput, ApiKeyCredential, AuthContext, AuthInteraction, AuthPrompt,
    AuthProviderError, AuthResult, BoxFuture, CancellationToken, ModelAuth, ProviderEnv,
    ProviderHeaders, SecretString,
};

pub const ANTHROPIC_AUTH_TOKEN_ENV: &str = "ANTHROPIC_AUTH_TOKEN";
pub const ANTHROPIC_OAUTH_TOKEN_ENV: &str = "ANTHROPIC_OAUTH_TOKEN";
pub const ANTHROPIC_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";
/// Generic environment helper with source truthiness (no trimming).
pub struct EnvApiKeyAuth {
    name: String,
    env_vars: Vec<String>,
    anthropic: bool,
}
pub fn env_api_key_auth(name: impl Into<String>, env_vars: Vec<String>) -> EnvApiKeyAuth {
    EnvApiKeyAuth {
        name: name.into(),
        env_vars,
        anthropic: false,
    }
}
pub fn anthropic_api_key_auth() -> EnvApiKeyAuth {
    EnvApiKeyAuth {
        name: "Anthropic API key".into(),
        env_vars: vec![
            ANTHROPIC_AUTH_TOKEN_ENV.into(),
            ANTHROPIC_OAUTH_TOKEN_ENV.into(),
            ANTHROPIC_API_KEY_ENV.into(),
        ],
        anthropic: true,
    }
}
fn check(c: &CancellationToken) -> Result<(), AuthProviderError> {
    if c.is_cancelled() {
        Err(AuthProviderError::new())
    } else {
        Ok(())
    }
}
impl ApiKeyAuth for EnvApiKeyAuth {
    fn name(&self) -> &str {
        &self.name
    }
    fn login<'a>(
        &'a self,
        i: &'a dyn AuthInteraction,
        c: &'a CancellationToken,
    ) -> Option<BoxFuture<'a, Result<ApiKeyCredential, AuthProviderError>>> {
        Some(Box::pin(async move {
            check(c)?;
            let key = i
                .prompt(
                    AuthPrompt::Secret {
                        message: format!("Enter {}", self.name),
                        placeholder: None,
                    },
                    c,
                )
                .await
                .map_err(AuthProviderError::with_source)?;
            check(c)?;
            Ok(ApiKeyCredential {
                key: Some(SecretString::new(key)),
                ..Default::default()
            })
        }))
    }
    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            check(input.cancellation)?;
            if let Some(credential) = input.credential
                && let Some(key) = &credential.key
                && !key.expose_secret().is_empty()
            {
                return Ok(Some(AuthResult {
                    auth: ModelAuth {
                        api_key: Some(key.clone()),
                        ..Default::default()
                    },
                    env: credential.env.clone(),
                    source: Some("stored credential".into()),
                }));
            }
            for name in &self.env_vars {
                let value = input
                    .context
                    .env(name, input.cancellation)
                    .await
                    .map_err(AuthProviderError::with_source)?;
                check(input.cancellation)?;
                if let Some(value) = value.filter(|v| !v.is_empty()) {
                    let auth = if self.anthropic && name == ANTHROPIC_AUTH_TOKEN_ENV {
                        let headers = ProviderHeaders::new(std::collections::BTreeMap::from([(
                            "Authorization".into(),
                            Some(format!("Bearer {value}")),
                        )]));
                        ModelAuth {
                            headers: Some(headers),
                            ..Default::default()
                        }
                    } else {
                        ModelAuth {
                            api_key: Some(SecretString::new(value)),
                            ..Default::default()
                        }
                    };
                    return Ok(Some(AuthResult {
                        auth,
                        env: None,
                        source: Some(name.clone()),
                    }));
                }
            }
            Ok(None)
        })
    }
}
/// Selected legacy discovery keys; unknown/open IDs return no configured mapping.
pub fn selected_api_key_env_vars(provider: &str) -> &'static [&'static str] {
    match provider {
        "openai" => &["OPENAI_API_KEY"],
        "google" => &["GEMINI_API_KEY"],
        "deepseek" => &["DEEPSEEK_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        "anthropic" => &[
            ANTHROPIC_AUTH_TOKEN_ENV,
            ANTHROPIC_OAUTH_TOKEN_ENV,
            ANTHROPIC_API_KEY_ENV,
        ],
        _ => &[],
    }
}
pub async fn get_provider_env_value(
    name: &str,
    env: Option<&ProviderEnv>,
    ctx: &dyn AuthContext,
    c: &CancellationToken,
) -> Result<Option<String>, AuthProviderError> {
    check(c)?;
    if let Some(value) = env
        .and_then(|e| e.as_map().get(name))
        .filter(|s| !s.is_empty())
    {
        return Ok(Some(value.clone()));
    }
    let value = ctx
        .env(name, c)
        .await
        .map_err(AuthProviderError::with_source)?;
    check(c)?;
    Ok(value.filter(|v| !v.is_empty()))
}
pub async fn find_env_keys(
    provider: &str,
    env: Option<&ProviderEnv>,
    ctx: &dyn AuthContext,
    c: &CancellationToken,
) -> Result<Option<Vec<String>>, AuthProviderError> {
    let mut found = Vec::new();
    for name in selected_api_key_env_vars(provider) {
        if get_provider_env_value(name, env, ctx, c).await?.is_some() {
            found.push((*name).into());
        }
    }
    Ok((!found.is_empty()).then_some(found))
}
pub async fn get_env_api_key(
    provider: &str,
    env: Option<&ProviderEnv>,
    ctx: &dyn AuthContext,
    c: &CancellationToken,
) -> Result<Option<SecretString>, AuthProviderError> {
    let found = find_env_keys(provider, env, ctx, c).await?;
    let name = found.as_ref().and_then(|keys| {
        keys.iter()
            .find(|key| provider != "anthropic" || key.as_str() != ANTHROPIC_AUTH_TOKEN_ENV)
    });
    match name {
        Some(name) => Ok(get_provider_env_value(name, env, ctx, c)
            .await?
            .map(SecretString::new)),
        None => Ok(None),
    }
}
