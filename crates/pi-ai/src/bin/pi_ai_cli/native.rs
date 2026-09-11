use super::{Error, Login, Selected};
use pi_ai_core::{
    AuthInteraction, BoxFuture, CancellationToken, OAuthAuth, OAuthCredential, SpawnError,
    TaskSpawner,
};
use pi_ai_providers::auth::{
    OAuthServices,
    native_host::*,
    oauth::{AnthropicOAuth, OpenAICodexOAuth, OpenRouterOAuth},
};
use std::sync::Arc;
use tokio::runtime::Handle;
pub struct Spawner(pub Handle);
impl TaskSpawner for Spawner {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(future);
        Ok(())
    }
}
pub fn services(handle: Handle) -> OAuthServices {
    OAuthServices {
        transport: Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(handle.clone())),
            Arc::new(NativeRetryJitter),
        )),
        clock: Arc::new(TokioClock::new(handle.clone())),
        spawner: Arc::new(Spawner(handle.clone())),
        crypto: Arc::new(NativeOAuthCrypto),
        callbacks: Arc::new(NativeCallbackListener::new(handle)),
        callback_host: "127.0.0.1".to_owned(),
        callback_limits: CallbackLimits::default(),
        max_token_response_bytes: 1024 * 1024,
    }
}
pub struct NativeLogin;
impl Login for NativeLogin {
    fn login<'a>(
        &'a self,
        provider: Selected,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, Error>> {
        Box::pin(async move {
            let services = services(Handle::current());
            let auth: Box<dyn OAuthAuth> = match provider {
                Selected::Anthropic => Box::new(AnthropicOAuth::new(services)),
                Selected::Codex => Box::new(OpenAICodexOAuth::new(services)),
                Selected::OpenRouter => Box::new(OpenRouterOAuth::new(services)),
            };
            auth.login(interaction, cancellation)
                .await
                .map_err(|_| Error::Login)
        })
    }
}
