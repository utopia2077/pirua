//! Canonical Google Vertex adapter with explicitly configured credential custody.
use super::{google_sdk as sdk, google_stream as engine, runtime::AdapterServices};
use pi_ai_core::*;
use pi_ai_transport::HttpHeaders;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

pub use engine::GoogleGenAiLimits as GoogleVertexLimits;
pub use engine::{GoogleThinking, GoogleToolCallCounter, GoogleToolChoice};

fn nonnull<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected a non-null optional field"))
}
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoogleVertexOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub tool_choice: Option<GoogleToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub thinking: Option<GoogleThinking>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub project: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub location: Option<String>,
}
impl fmt::Debug for GoogleVertexOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleVertexOptions([REDACTED])")
    }
}

/// No-I/O session configuration. Scope is always cloud-platform, never caller-selected.
pub struct GoogleVertexAuthConfig<'a> {
    pub key_filename: Option<&'a str>,
}
impl fmt::Debug for GoogleVertexAuthConfig<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleVertexAuthConfig([REDACTED])")
    }
}
/// Bounded synchronous construction only; S05 owns concrete discovery/factory policy.
pub trait GoogleVertexAuthProvider: Send + Sync {
    fn create_session(
        &self,
        config: GoogleVertexAuthConfig<'_>,
    ) -> Result<Box<dyn GoogleVertexAuthSession>, ModelsError>;
}
/// One invocation's credential owner, reused for each outer request attempt.
pub trait GoogleVertexAuthSession: Send + Sync {
    fn get_request_headers<'a>(
        &'a self,
        endpoint: &'a RequestEndpoint,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpHeaders, GoogleVertexAuthError>>;
}
pub enum GoogleVertexAuthError {
    Other(ModelsError),
    Provider {
        status: Option<u16>,
        failure: ModelsError,
    },
}
impl fmt::Debug for GoogleVertexAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleVertexAuthError([REDACTED])")
    }
}
impl fmt::Display for GoogleVertexAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Google Vertex credential operation failed")
    }
}
impl std::error::Error for GoogleVertexAuthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Other(error) | Self::Provider { failure: error, .. } => error,
        })
    }
}

#[derive(Clone)]
pub struct GoogleVertexApi {
    pub services: AdapterServices,
    pub defaults: GoogleVertexOptions,
    pub limits: GoogleVertexLimits,
    auth_provider: Option<Arc<dyn GoogleVertexAuthProvider>>,
    counter: Option<Arc<GoogleToolCallCounter>>,
    api: ApiId,
}
impl fmt::Debug for GoogleVertexApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleVertexApi([REDACTED])")
    }
}
impl GoogleVertexApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            services,
            defaults: GoogleVertexOptions::default(),
            limits: GoogleVertexLimits::default(),
            auth_provider: None,
            counter: None,
            api: "google-vertex".parse().expect("constant API"),
        }
    }
    pub fn with_auth_provider(mut self, provider: Arc<dyn GoogleVertexAuthProvider>) -> Self {
        self.auth_provider = Some(provider);
        self
    }
    pub fn with_tool_call_counter(mut self, counter: Arc<GoogleToolCallCounter>) -> Self {
        self.counter = Some(counter);
        self
    }
    fn engine(&self, options: GoogleVertexOptions) -> engine::GoogleGenAiApi {
        let mut api = engine::GoogleGenAiApi::vertex(
            self.services.clone(),
            Invocation {
                options: options.clone(),
                provider: self.auth_provider.clone(),
            },
        );
        api.defaults = engine::GoogleOptions {
            tool_choice: options.tool_choice,
            thinking: options.thinking,
        };
        api.limits = self.limits;
        if let Some(counter) = &self.counter {
            api = api.with_tool_call_counter(counter.clone());
        }
        api
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: GoogleVertexOptions,
    ) -> AssistantStream {
        self.engine(options).stream(request)
    }
}
impl ApiAdapter for GoogleVertexApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.engine(GoogleVertexOptions::default())
            .stream_simple(request)
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async {
            Err(ModelsError::with_source(
                ModelsErrorCode::Stream,
                CapabilityError::Unsupported,
            ))
        })
    }
}

#[derive(Clone)]
pub(crate) struct Invocation {
    pub options: GoogleVertexOptions,
    pub provider: Option<Arc<dyn GoogleVertexAuthProvider>>,
}
pub(crate) struct Client {
    pub routing: sdk::VertexRouting,
    pub session: Option<Box<dyn GoogleVertexAuthSession>>,
}
impl Invocation {
    pub(crate) fn client(
        &self,
        request: &TextStreamRequest,
        ambient: &ProviderEnv,
    ) -> Result<Client, ModelsError> {
        let failure = || ModelsError::new(ModelsErrorCode::Auth);
        let key = request
            .auth
            .api_key
            .as_ref()
            .map(|v| v.expose_secret().trim_matches(sdk::whitespace))
            .filter(|s| {
                !s.is_empty()
                    && *s != "gcp-vertex-credentials"
                    && !(s.starts_with('<')
                        && s.ends_with('>')
                        && s.len() > 2
                        && !s[1..s.len() - 1].contains('>'))
            });
        let env = |name: &str| {
            request
                .auth
                .env
                .as_map()
                .get(name)
                .filter(|v| !v.is_empty())
                .or_else(|| ambient.as_map().get(name).filter(|v| !v.is_empty()))
                .cloned()
        };
        let (project, location, session) = if key.is_some() {
            (None, None, None)
        } else {
            let project = self
                .options
                .project
                .clone()
                .filter(|s| !s.is_empty())
                .or_else(|| env("GOOGLE_CLOUD_PROJECT"))
                .or_else(|| env("GCLOUD_PROJECT"))
                .ok_or_else(failure)?;
            let location = self
                .options
                .location
                .clone()
                .filter(|s| !s.is_empty())
                .or_else(|| env("GOOGLE_CLOUD_LOCATION"))
                .ok_or_else(failure)?;
            let key_filename = env("GOOGLE_APPLICATION_CREDENTIALS");
            let provider = self.provider.as_ref().ok_or_else(failure)?;
            let session = provider.create_session(GoogleVertexAuthConfig {
                key_filename: key_filename.as_deref(),
            })?;
            (Some(project), Some(location), Some(session))
        };
        let routing = sdk::VertexRouting::new(request, ambient, key, project, location);
        Ok(Client { routing, session })
    }
}
