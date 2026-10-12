//! Concrete coding Harness composition over the existing runtime factory.
//!
//! M4 deliberately accepts the concrete [`ModelsRegistry`] required by
//! [`RuntimeFactory`].  The host owns the registry, session and factory; this
//! module only assembles detached coding values into `AgentHarnessOptions`.
//! It does not add a durable session owner or extension fields.

use crate::{
    CodingRuntimeError, CodingSettingsSnapshot, ExecutionEnv, ExecutionToolContext,
    ProviderComposition, Resources, create_tools,
};
use pi_agent::schema::{
    AgentHarness, AgentHarnessCreate, AgentHarnessOptions, AgentHarnessStreamOptions,
    AgentHarnessToolContextSource, AgentHarnessToolHandle, AgentLane, DriveMode,
    HarnessRuntimeError, ProviderMessageConverter, ToolExecutionMode,
};
use pi_agent::schema::{SessionMetadata, SessionMetadataRecord};
use pi_agent_session::{session::RuntimeSession, storage::RuntimeStorage};
use pi_ai::{Model, ModelId, ModelsRegistry, ProviderId};
use pi_harness_runtime::owner::{RuntimeFactory, RuntimeHarness};
use pi_telemetry::TelemetryContext;
use std::{collections::BTreeMap, fmt, sync::Arc};

/// Errors raised while assembling or creating a coding Harness.
#[derive(Debug)]
pub enum CodingHarnessError {
    /// The host registry did not contain the requested provider/model pair.
    MissingModel {
        /// Provider identity requested by the detached settings.
        provider: ProviderId,
        /// Model identity requested by the detached settings.
        model_id: ModelId,
    },
    /// The detached stream options failed schema conversion.
    InvalidStreamOptions,
    /// The existing runtime factory rejected creation or restoration.
    Runtime(HarnessRuntimeError),
    /// A required detached coding-runtime capability was absent before
    /// RuntimeFactory admission.
    Composition(CodingRuntimeError),
}

impl fmt::Display for CodingHarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingModel { .. } => formatter.write_str("coding model is not registered"),
            Self::InvalidStreamOptions => formatter.write_str("coding stream options are invalid"),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Composition(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CodingHarnessError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingModel { .. } | Self::InvalidStreamOptions => None,
            Self::Runtime(error) => Some(error),
            Self::Composition(error) => Some(error),
        }
    }
}

impl From<HarnessRuntimeError> for CodingHarnessError {
    fn from(error: HarnessRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

/// A built coding Harness and the typed session it is attached to.
///
/// The wrapper is process-local convenience. Durable entries, runtime claims,
/// leases and storage remain owned by `RuntimeSession` and the existing
/// `RuntimeHarness` implementation.
pub struct CodingHarness<S, M = SessionMetadata>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    harness: Arc<RuntimeHarness<ExecutionToolContext, S, M>>,
    session: Arc<RuntimeSession<S, M>>,
    models: Arc<ModelsRegistry>,
    /// Exact validated model snapshot supplied to the assembled Harness.
    ///
    /// The host registry remains authoritative for provider adapter lookup,
    /// while this detached snapshot preserves overlay metadata for readback.
    model: Model,
}

impl<S, M> CodingHarness<S, M>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    /// Returns the existing runtime Harness surface.
    pub fn harness(&self) -> &Arc<RuntimeHarness<ExecutionToolContext, S, M>> {
        &self.harness
    }

    /// Returns the typed session used by this Harness.
    pub fn session(&self) -> &Arc<RuntimeSession<S, M>> {
        &self.session
    }

    /// Returns the exact validated model snapshot assembled into the Harness.
    ///
    /// Provider adapter lookup remains host-owned through `ModelsRegistry`;
    /// this readback preserves detached overlay metadata such as the model
    /// name and public base URL.
    pub fn assembled_model(&self) -> &Model {
        &self.model
    }

    /// Resolves and durably applies the model used by the next run.
    pub async fn switch_model(
        &self,
        provider: ProviderId,
        model_id: ModelId,
    ) -> Result<(), CodingHarnessError> {
        let model = self
            .models
            .get_model(&provider, &model_id)
            .ok_or_else(|| CodingHarnessError::MissingModel { provider, model_id })?;
        self.harness.set_model(model).await?;
        Ok(())
    }

    /// Connects one host-authenticated extension tool/profile route through
    /// the existing runtime pump.
    #[cfg(feature = "extensions")]
    pub async fn connect_extension_tool(
        &self,
        input: crate::extensions::ExtensionToolInput,
    ) -> Result<crate::extensions::CodingExtensionAdapter, crate::extensions::CodingExtensionError>
    {
        crate::extensions::CodingExtensionAdapter::connect_tool(self, input).await
    }

    /// Connects one host-authenticated retained extension tool/profile route
    /// through the existing runtime pump.
    #[cfg(feature = "extensions")]
    pub async fn connect_extension_tool_retained(
        &self,
        input: crate::extensions::ExtensionRetainedToolInput,
    ) -> Result<crate::extensions::CodingExtensionAdapter, crate::extensions::CodingExtensionError>
    {
        crate::extensions::CodingExtensionAdapter::connect_tool_retained(self, input).await
    }

    /// Connects one host-authenticated service/profile route through the
    /// existing runtime pump.
    #[cfg(feature = "extensions")]
    pub async fn connect_extension_service(
        &self,
        input: crate::extensions::ExtensionServiceInput,
    ) -> Result<crate::extensions::CodingExtensionAdapter, crate::extensions::CodingExtensionError>
    {
        crate::extensions::CodingExtensionAdapter::connect_service(self, input).await
    }

    /// Attaches ordered, already-authorized extension registrations.
    #[cfg(feature = "extensions")]
    pub async fn attach_extensions(
        &self,
        adapter: crate::extensions::CodingExtensionAdapter,
        registrations: Vec<crate::extensions::ExtensionRegistration>,
    ) -> Result<crate::extensions::CodingExtensionAttachment, crate::extensions::CodingExtensionError>
    {
        adapter.attach(self, registrations).await
    }

    pub(crate) async fn teardown(&self) -> Result<(), HarnessRuntimeError> {
        // Abort is idempotent and may return an expected `NoActiveOperation`
        // result. The outer runtime error still indicates a real failure.
        let _ = self.harness.abort().await?;
        self.harness.wait_for_idle().await?;
        self.harness.close().await
    }
}

/// Builder for one concrete coding Harness attachment.
///
/// `RuntimeFactory` is retained by the host in an `Arc` so the same factory
/// can be reused for create, replace, fork, import and restart. The builder
/// accepts `Arc<RuntimeSession<S, M>>` directly because that is the ownership
/// shape implemented by the current session/runtime crates; no second
/// `SessionHandle` abstraction is introduced.
pub struct CodingHarnessBuilder<S, M = SessionMetadata>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    factory: Arc<RuntimeFactory<ExecutionToolContext, S, M>>,
    session: Arc<RuntimeSession<S, M>>,
    models: Arc<ModelsRegistry>,
    execution_env: Arc<dyn ExecutionEnv>,
    resources: Resources,
    settings: CodingSettingsSnapshot,
    tools: Option<Vec<AgentHarnessToolHandle<ExecutionToolContext>>>,
    system_prompt: Option<String>,
    telemetry_context: Arc<dyn TelemetryContext>,
    to_provider_messages: Option<Arc<dyn ProviderMessageConverter>>,
    provider_composition: Option<Arc<ProviderComposition>>,
    model_override: Option<Model>,
    entry_projectors:
        BTreeMap<pi_agent::schema::CustomType, Arc<dyn pi_agent::schema::EntryProjector>>,
    drive: DriveMode,
    tool_execution: ToolExecutionMode,
    first_attachment: bool,
}

impl<S, M> CodingHarnessBuilder<S, M>
where
    S: RuntimeStorage + 'static,
    M: SessionMetadataRecord + 'static,
{
    /// Creates a builder from host-owned runtime/session/model/environment
    /// handles and detached coding values.
    pub fn new(
        factory: Arc<RuntimeFactory<ExecutionToolContext, S, M>>,
        session: Arc<RuntimeSession<S, M>>,
        models: Arc<ModelsRegistry>,
        execution_env: Arc<dyn ExecutionEnv>,
        resources: Resources,
        settings: CodingSettingsSnapshot,
    ) -> Self {
        Self {
            factory,
            session,
            models,
            execution_env,
            resources,
            settings,
            tools: None,
            system_prompt: None,
            telemetry_context: Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            to_provider_messages: None,
            provider_composition: None,
            model_override: None,
            entry_projectors: BTreeMap::new(),
            drive: DriveMode::Automatic,
            tool_execution: ToolExecutionMode::Parallel,
            first_attachment: false,
        }
    }

    /// Supplies executable coding tools. The default is [`create_tools`].
    pub fn with_tools(mut self, tools: Vec<AgentHarnessToolHandle<ExecutionToolContext>>) -> Self {
        self.tools = Some(tools);
        self
    }

    /// Supplies the already-built static coding system prompt.
    pub fn with_system_prompt(mut self, system_prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(system_prompt.into());
        self
    }

    /// Supplies the telemetry context captured by the host.
    pub fn with_telemetry(mut self, telemetry: Arc<dyn TelemetryContext>) -> Self {
        self.telemetry_context = telemetry;
        self
    }

    /// Supplies the coding message converter used at provider admission.
    pub fn with_message_converter(mut self, converter: Arc<dyn ProviderMessageConverter>) -> Self {
        self.to_provider_messages = Some(converter);
        self
    }

    /// Supplies the detached model/provider composition used for pre-start
    /// capability checks. The host retains ownership and no credentials or
    /// network are consulted by this preflight.
    pub fn with_provider_composition(mut self, composition: Arc<ProviderComposition>) -> Self {
        self.provider_composition = Some(composition);
        self
    }

    /// Supplies the detached model selected by the coding-runtime
    /// composition. The selected identity must still be present in the
    /// host-owned registry so provider adapter lookup remains host-owned.
    pub fn with_model_override(mut self, model: Model) -> Self {
        self.model_override = Some(model);
        self
    }

    /// Selects manual or automatic Harness driving.
    pub fn with_drive_mode(mut self, drive: DriveMode) -> Self {
        self.drive = drive;
        self
    }

    /// Selects the generic Harness tool execution mode.
    pub fn with_tool_execution(mut self, mode: ToolExecutionMode) -> Self {
        self.tool_execution = mode;
        self
    }

    /// Marks this build as the explicit first attachment that may seed a
    /// missing main lane configuration.
    pub fn first_attachment(mut self) -> Self {
        self.first_attachment = true;
        self
    }

    /// Builds the concrete runtime attachment without starting a provider run.
    pub async fn build(self) -> Result<CodingHarness<S, M>, CodingHarnessError> {
        if let Some(composition) = self.provider_composition.as_ref() {
            composition
                .require_capability("models.resolve")
                .map_err(CodingHarnessError::Composition)?;
        }
        let stream_options = harness_stream_options(self.settings.stream)?;
        let model = match self.model_override {
            Some(model) => {
                if self.models.get_model(&model.provider, &model.id).is_none() {
                    return Err(CodingHarnessError::MissingModel {
                        provider: model.provider.clone(),
                        model_id: model.id.clone(),
                    });
                }
                model
            }
            None => self
                .models
                .get_model(&self.settings.model.provider, &self.settings.model.model_id)
                .ok_or_else(|| CodingHarnessError::MissingModel {
                    provider: self.settings.model.provider.clone(),
                    model_id: self.settings.model.model_id.clone(),
                })?,
        };
        let tools = self.tools.unwrap_or_else(create_tools);
        let options = AgentHarnessOptions {
            session: self.session.clone(),
            models: self.models.clone(),
            model: model.clone(),
            thinking_level: Some(self.settings.thinking_level),
            active_tool_names: Some(self.settings.active_tools.clone()),
            tools,
            tool_context: Some(AgentHarnessToolContextSource::Static(
                ExecutionToolContext {
                    env: self.execution_env,
                },
            )),
            system_prompt: self
                .system_prompt
                .map(pi_agent::schema::SystemPromptSource::Static),
            resources: self.resources,
            stream_options,
            retry: self.settings.retry,
            compaction: self.settings.compaction,
            steering_mode: self.settings.steering_mode,
            follow_up_mode: self.settings.follow_up_mode,
            tool_execution: self.tool_execution,
            drive: self.drive,
            to_provider_messages: self.to_provider_messages,
            entry_projectors: self.entry_projectors,
            telemetry_context: self.telemetry_context,
        };
        let created: AgentHarnessCreate<RuntimeHarness<ExecutionToolContext, S, M>> =
            if self.first_attachment {
                self.factory.create_first_attachment(options).await?
            } else {
                self.factory.create_configured(options).await?
            };
        Ok(CodingHarness {
            harness: created.harness,
            session: self.session,
            models: self.models,
            model,
        })
    }
}

fn harness_stream_options(
    wire: pi_ai::StreamOptionsWire,
) -> Result<AgentHarnessStreamOptions, CodingHarnessError> {
    // `StreamOptionsWire` is provider-neutral and contains fields that are
    // intentionally outside the current curated Harness stream surface. Do
    // not silently drop those values at the application boundary.
    if wire.temperature.is_some()
        || wire.sampling_params.is_some()
        || wire.max_tokens.is_some()
        || wire.session_id.is_some()
        || wire.websocket_connect_timeout_ms.is_some()
    {
        return Err(CodingHarnessError::InvalidStreamOptions);
    }
    Ok(AgentHarnessStreamOptions {
        transport: wire.transport,
        timeout_ms: wire.timeout_ms,
        max_retries: wire.max_retries,
        max_retry_delay_ms: wire.max_retry_delay_ms,
        headers: None,
        metadata: wire.metadata,
        cache_retention: wire.cache_retention,
        deferred: None,
    })
}
