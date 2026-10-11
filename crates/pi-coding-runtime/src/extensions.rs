//! Bounded composition of host-authenticated extension profiles.
//!
//! This adapter consumes an already authenticated and Ready
//! [`ProfileConnection`].  It does not load extension modules, authenticate a
//! transport, resolve credentials, start a process, or own a Session.
//!
//! Tool registration is intentionally limited to the deterministic `echo`
//! domain implemented by `pi-harness-runtime`.  The adapter therefore makes
//! no claim about arbitrary foreign tool execution.  Provider registration is
//! likewise catalog/no-auth only; request credentials, headers, runtime
//! endpoints, and provider HTTP remain host-owned.

use crate::builder::CodingHarness;
use pi_agent::schema::{
    AgentHarness, AgentHarnessToolHandle, HookRegistrationError, HookSubscription,
    SessionMetadataRecord,
};
use pi_agent_session::storage::RuntimeStorage;
use pi_ai::{Model, ModelBaseUrl, ModelHeaderPolicy};
use pi_extension_protocol::{
    jcs::PortableSchema, profile_live::Reservations, profiles::wire_v1_1::CapabilityRef,
};
use pi_harness_runtime::extensions::{
    profiles::{
        connection::ProfileConnection, hooks::RemoteHook, provider::ProfileProviderSnapshot,
    },
    tool_driver::{DriverError, EchoRouteTiming, ReadyToolRoute, SingleToolOwner},
    transport::{MonotonicClock, OrderedByteChannel, PumpConfig, RetainedOrderedByteChannel},
};
use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    sync::Arc,
};

/// Inputs for connecting a Ready tool/profile route through the existing
/// [`RuntimeHarness`] pump.
pub struct ExtensionToolInput {
    /// Host-established authenticated authority.
    pub host: pi_extension_protocol::activation::HostAuthority,
    /// Host-owned single-tool owner.
    pub owner: SingleToolOwner,
    /// Ordered host channel.
    pub channel: Arc<dyn OrderedByteChannel>,
    /// Host monotonic clock.
    pub clock: Arc<dyn MonotonicClock>,
    /// Finite pump limits.
    pub config: PumpConfig,
    /// Finite profile reservations.
    pub reservations: Reservations,
    /// Host deadline for the connection.
    pub remaining_ms: u64,
}

/// Inputs for connecting a Ready tool/profile route through the retained
/// receive path owned by the existing [`RuntimeHarness`] pump.
pub struct ExtensionRetainedToolInput {
    /// Host-established authenticated authority.
    pub host: pi_extension_protocol::activation::HostAuthority,
    /// Host-owned single-tool owner.
    pub owner: SingleToolOwner,
    /// Ordered host channel with retained receive notifications.
    pub channel: Arc<dyn RetainedOrderedByteChannel>,
    /// Host monotonic clock.
    pub clock: Arc<dyn MonotonicClock>,
    /// Finite pump limits.
    pub config: PumpConfig,
    /// Finite profile reservations.
    pub reservations: Reservations,
    /// Host deadline for the connection.
    pub remaining_ms: u64,
}

/// Inputs for connecting a service-only profile route through the existing
/// [`RuntimeHarness`] pump.
pub struct ExtensionServiceInput {
    /// Host-established authenticated authority.
    pub host: pi_extension_protocol::activation::HostAuthority,
    /// Ordered host channel.
    pub channel: Arc<dyn OrderedByteChannel>,
    /// Host monotonic clock.
    pub clock: Arc<dyn MonotonicClock>,
    /// Finite pump limits.
    pub config: PumpConfig,
    /// Finite profile reservations.
    pub reservations: Reservations,
    /// Host deadline for the connection.
    pub remaining_ms: u64,
}

/// One ordered profile registration requested by the host.
#[derive(Clone)]
pub enum ExtensionRegistration {
    /// Register the one supported deterministic remote `echo` tool.
    EchoTool {
        /// The local Harness handle whose descriptor must exactly match the
        /// host-approved `echo` declaration.
        handle: AgentHarnessToolHandle<crate::ExecutionToolContext>,
        /// Finite route timing supplied by the host.
        timing: EchoRouteTiming,
    },
    /// Register one remote hook against the existing ordered hook registry.
    Hook {
        /// The immutable granted capability reference.
        capability: CapabilityRef,
        /// Host model-header policy used by the native validator.
        model_policy: ModelHeaderPolicy,
    },
    /// Register one catalog/no-auth provider profile.
    Provider {
        /// The immutable granted capability reference.
        capability: CapabilityRef,
        /// Detached same-provider, header-free catalog models.
        models: Vec<Model>,
        /// Host model-header policy used by the provider request validator.
        model_policy: ModelHeaderPolicy,
        /// Optional host-approved provider payload schema.
        payload_schema: Option<PortableSchema>,
    },
}

impl ExtensionRegistration {
    /// Creates a bounded `echo` registration.
    pub fn echo_tool(
        handle: AgentHarnessToolHandle<crate::ExecutionToolContext>,
        timing: EchoRouteTiming,
    ) -> Self {
        Self::EchoTool { handle, timing }
    }

    /// Creates a remote hook registration with the default native header
    /// policy.
    pub fn hook(capability: CapabilityRef) -> Self {
        Self::Hook {
            capability,
            model_policy: ModelHeaderPolicy::default(),
        }
    }

    /// Creates a catalog/no-auth provider registration with the default native
    /// header policy.
    pub fn provider(capability: CapabilityRef, models: Vec<Model>) -> Self {
        Self::Provider {
            capability,
            models,
            model_policy: ModelHeaderPolicy::default(),
            payload_schema: None,
        }
    }

    /// Returns the stable kind used in registration-order witnesses.
    pub fn kind(&self) -> ExtensionRegistrationKind {
        match self {
            Self::EchoTool { .. } => ExtensionRegistrationKind::EchoTool,
            Self::Hook { .. } => ExtensionRegistrationKind::Hook,
            Self::Provider { .. } => ExtensionRegistrationKind::Provider,
        }
    }
}

/// Stable kind of one ordered registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtensionRegistrationKind {
    /// Bounded remote `echo` tool.
    EchoTool,
    /// Remote hook.
    Hook,
    /// Catalog/no-auth provider.
    Provider,
}

/// Stable coding-runtime policy for an echo tool conflict across attachments.
///
/// The lower Harness route can replace an echo route, but the coding adapter
/// refuses a second attached echo registration so callers cannot accidentally
/// lose a live binding. The first attachment remains installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EchoConflictPolicy {
    /// Refuse an attachment when the Harness already exposes an echo tool.
    RejectExisting,
}

/// Refusal raised before or during extension composition.
#[derive(Debug)]
pub enum CodingExtensionError {
    /// A requested profile was not present in the Ready grant.
    MissingGrant { kind: String, name: String },
    /// A registration input violated the bounded adapter contract.
    InvalidRegistration(&'static str),
    /// The supplied Ready route was already draining or closed.
    ConnectionClosed,
    /// The existing Harness rejected a bounded tool route.
    Tool(DriverError),
    /// The existing Harness rejected a hook registration.
    Hook(HookRegistrationError),
    /// The existing Harness rejected a provider or connection operation.
    Runtime(String),
}

impl fmt::Display for CodingExtensionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingGrant { kind, name } => {
                write!(formatter, "extension grant missing: {kind}:{name}")
            }
            Self::InvalidRegistration(reason) => {
                write!(formatter, "invalid extension registration: {reason}")
            }
            Self::ConnectionClosed => formatter.write_str("extension connection is closed"),
            Self::Tool(error) => error.fmt(formatter),
            Self::Hook(error) => error.fmt(formatter),
            Self::Runtime(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for CodingExtensionError {}

/// A host-authenticated extension connection and, when present, its bounded
/// tool route.
pub struct CodingExtensionAdapter {
    connection: Arc<ProfileConnection>,
    route: Option<ReadyToolRoute>,
}

impl CodingExtensionAdapter {
    /// Returns the explicit cross-attachment echo conflict policy.
    pub const fn echo_conflict_policy() -> EchoConflictPolicy {
        EchoConflictPolicy::RejectExisting
    }

    /// Wraps an already Ready service connection.
    ///
    /// The connection type is only produced after the existing Hello,
    /// Manifest, and Ready handshake has completed.
    pub fn from_connection(connection: Arc<ProfileConnection>) -> Self {
        Self {
            connection,
            route: None,
        }
    }

    /// Wraps an already Ready tool connection and its route.
    pub fn from_tool_connection(
        connection: Arc<ProfileConnection>,
        route: ReadyToolRoute,
    ) -> Result<Self, CodingExtensionError> {
        if !connection.same_tool_connection(&route) || !route.is_current() {
            return Err(CodingExtensionError::ConnectionClosed);
        }
        Ok(Self {
            connection,
            route: Some(route),
        })
    }

    /// Connects through the existing authenticated tool/profile pump.
    pub async fn connect_tool<S, M>(
        harness: &CodingHarness<S, M>,
        input: ExtensionToolInput,
    ) -> Result<Self, CodingExtensionError>
    where
        S: RuntimeStorage + 'static,
        M: SessionMetadataRecord + 'static,
    {
        let (connection, route) = harness
            .harness()
            .connect_profile_tool(
                &input.owner,
                input.host,
                input.channel,
                input.clock,
                input.config,
                input.reservations,
                input.remaining_ms,
            )
            .await
            .map_err(runtime_error)?;
        Self::from_tool_connection(connection, route)
    }

    /// Connects through the existing authenticated retained tool/profile pump.
    pub async fn connect_tool_retained<S, M>(
        harness: &CodingHarness<S, M>,
        input: ExtensionRetainedToolInput,
    ) -> Result<Self, CodingExtensionError>
    where
        S: RuntimeStorage + 'static,
        M: SessionMetadataRecord + 'static,
    {
        let (connection, route) = harness
            .harness()
            .connect_profile_tool_retained(
                &input.owner,
                input.host,
                input.channel,
                input.clock,
                input.config,
                input.reservations,
                input.remaining_ms,
            )
            .await
            .map_err(runtime_error)?;
        Self::from_tool_connection(connection, route)
    }

    /// Connects through the existing authenticated service/profile pump.
    pub async fn connect_service<S, M>(
        harness: &CodingHarness<S, M>,
        input: ExtensionServiceInput,
    ) -> Result<Self, CodingExtensionError>
    where
        S: RuntimeStorage + 'static,
        M: SessionMetadataRecord + 'static,
    {
        let connection = harness
            .harness()
            .connect_profile_service(
                input.host,
                input.channel,
                input.clock,
                input.config,
                input.reservations,
                input.remaining_ms,
            )
            .await
            .map_err(runtime_error)?;
        Ok(Self::from_connection(connection))
    }

    /// Returns the immutable Ready grant consumed by this adapter.
    pub fn grant(&self) -> &pi_extension_protocol::activation::ActiveGrant {
        self.connection.grant()
    }

    /// Returns the process-local profile connection.
    pub fn connection(&self) -> &Arc<ProfileConnection> {
        &self.connection
    }

    /// Attaches ordered registrations to an already-built coding Harness.
    ///
    /// All grant, shape, provider-scope, duplicate, and route checks run
    /// before the first Harness registration. Hooks are retained in the
    /// returned attachment so close/drop can unsubscribe them.
    pub async fn attach<S, M>(
        self,
        harness: &CodingHarness<S, M>,
        registrations: Vec<ExtensionRegistration>,
    ) -> Result<CodingExtensionAttachment, CodingExtensionError>
    where
        S: RuntimeStorage + 'static,
        M: SessionMetadataRecord + 'static,
    {
        let original_tools = harness.harness().get_tools().await.map_err(runtime_error)?;
        self.preflight(harness, &registrations, &original_tools)
            .await?;

        let provider_snapshots = self.provider_snapshots(harness, &registrations)?;

        let mut hooks = Vec::new();
        let mut order = Vec::with_capacity(registrations.len());
        let mut tool_attempted = false;
        for registration in registrations {
            let kind = registration.kind();
            if kind == ExtensionRegistrationKind::EchoTool {
                tool_attempted = true;
            }
            let result = match registration {
                ExtensionRegistration::EchoTool { handle, timing } => harness
                    .harness()
                    .register_ready_echo_tool(
                        handle,
                        Arc::new(self.route.as_ref().expect("preflight route").clone()),
                        timing,
                    )
                    .await
                    .map_err(runtime_error)
                    .and_then(|result| result.map_err(CodingExtensionError::Tool))
                    .map(|_| ()),
                ExtensionRegistration::Hook {
                    capability,
                    model_policy,
                } => RemoteHook::new(self.connection.clone(), capability, model_policy)
                    .map_err(runtime_error)
                    .and_then(|hook| {
                        harness
                            .harness()
                            .register_profile_hook(hook)
                            .map_err(CodingExtensionError::Hook)
                    })
                    .map(|subscription| {
                        hooks.push(subscription);
                    }),
                ExtensionRegistration::Provider {
                    capability,
                    models,
                    model_policy,
                    payload_schema,
                } => harness
                    .harness()
                    .register_profile_provider(
                        self.connection.clone(),
                        capability,
                        models,
                        model_policy,
                        payload_schema,
                    )
                    .await
                    .map_err(runtime_error),
            };
            if let Err(error) = result {
                for hook in hooks {
                    hook.unsubscribe();
                }
                let mut rollback_error = None;
                if tool_attempted
                    && let Err(rollback) = harness.harness().set_tools(original_tools.clone()).await
                {
                    rollback_error = Some(runtime_error(rollback));
                }
                for snapshot in provider_snapshots.into_iter().rev() {
                    if let Err(rollback) =
                        harness.harness().restore_profile_provider(snapshot).await
                    {
                        rollback_error = Some(runtime_error(rollback));
                    }
                }
                self.connection.close();
                return Err(rollback_error.unwrap_or(error));
            }
            order.push(kind);
        }

        Ok(CodingExtensionAttachment {
            connection: self.connection,
            route: self.route,
            hooks,
            order,
        })
    }

    async fn preflight<S, M>(
        &self,
        harness: &CodingHarness<S, M>,
        registrations: &[ExtensionRegistration],
        original_tools: &[AgentHarnessToolHandle<crate::ExecutionToolContext>],
    ) -> Result<(), CodingExtensionError>
    where
        S: RuntimeStorage + 'static,
        M: SessionMetadataRecord + 'static,
    {
        if !self.connection.current() {
            return Err(CodingExtensionError::ConnectionClosed);
        }
        let mut capabilities = HashSet::new();
        let mut providers = BTreeSet::new();
        let mut tool_count = 0usize;
        for registration in registrations {
            match registration {
                ExtensionRegistration::EchoTool { handle, timing } => {
                    tool_count = tool_count.saturating_add(1);
                    if tool_count > 1 {
                        return Err(CodingExtensionError::InvalidRegistration(
                            "only one bounded echo route may be attached",
                        ));
                    }
                    if Self::echo_conflict_policy() == EchoConflictPolicy::RejectExisting
                        && original_tools
                            .iter()
                            .any(|tool| tool.descriptor().name.as_str() == "echo")
                    {
                        return Err(CodingExtensionError::InvalidRegistration(
                            "echo tool conflict: existing attachment wins",
                        ));
                    }
                    let route =
                        self.route
                            .as_ref()
                            .ok_or(CodingExtensionError::InvalidRegistration(
                                "echo registration requires a tool connection",
                            ))?;
                    harness
                        .harness()
                        .validate_ready_echo_tool(handle, route, *timing)
                        .map_err(CodingExtensionError::Tool)?;
                }
                ExtensionRegistration::Hook {
                    capability,
                    model_policy: _,
                } => {
                    validate_capability(&self.connection, capability, "hook")?;
                    if !capabilities.insert(capability_key(capability)) {
                        return Err(CodingExtensionError::InvalidRegistration(
                            "duplicate capability registration",
                        ));
                    }
                    RemoteHook::new(
                        self.connection.clone(),
                        capability.clone(),
                        ModelHeaderPolicy::default(),
                    )
                    .map_err(runtime_error)?
                    .stable_id()
                    .map_err(CodingExtensionError::Hook)?;
                }
                ExtensionRegistration::Provider {
                    capability,
                    models,
                    model_policy: _,
                    payload_schema: _,
                } => {
                    validate_capability(&self.connection, capability, "provider")?;
                    if !capabilities.insert(capability_key(capability)) {
                        return Err(CodingExtensionError::InvalidRegistration(
                            "duplicate capability registration",
                        ));
                    }
                    validate_provider_models(capability, models)?;
                    let provider = models.first().map(|model| model.provider.clone()).ok_or(
                        CodingExtensionError::InvalidRegistration("provider catalog is empty"),
                    )?;
                    if !providers.insert(provider) {
                        return Err(CodingExtensionError::InvalidRegistration(
                            "duplicate provider registration",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn provider_snapshots<S, M>(
        &self,
        harness: &CodingHarness<S, M>,
        registrations: &[ExtensionRegistration],
    ) -> Result<Vec<ProfileProviderSnapshot>, CodingExtensionError>
    where
        S: RuntimeStorage + 'static,
        M: SessionMetadataRecord + 'static,
    {
        let mut providers = BTreeSet::new();
        let mut snapshots = Vec::new();
        for registration in registrations {
            let ExtensionRegistration::Provider { models, .. } = registration else {
                continue;
            };
            let Some(model) = models.first() else {
                continue;
            };
            if providers.insert(model.provider.clone()) {
                snapshots.push(harness.harness().snapshot_profile_provider(&model.provider));
            }
        }
        Ok(snapshots)
    }
}

/// Retained attachment state. Dropping or closing it unsubscribes hooks and
/// closes the underlying generation through the existing connection owner.
pub struct CodingExtensionAttachment {
    connection: Arc<ProfileConnection>,
    route: Option<ReadyToolRoute>,
    hooks: Vec<Arc<dyn HookSubscription>>,
    order: Vec<ExtensionRegistrationKind>,
}

impl CodingExtensionAttachment {
    /// Returns the Ready connection.
    pub fn connection(&self) -> &Arc<ProfileConnection> {
        &self.connection
    }

    /// Returns the supplied registration order.
    pub fn registration_order(&self) -> &[ExtensionRegistrationKind] {
        &self.order
    }

    /// Closes the generation and unsubscribes registered hooks.
    pub fn close(mut self) {
        self.close_inner();
    }

    fn close_inner(&mut self) {
        for hook in self.hooks.drain(..) {
            hook.unsubscribe();
        }
        self.connection.close();
        self.route.take();
    }
}

impl Drop for CodingExtensionAttachment {
    fn drop(&mut self) {
        self.close_inner();
    }
}

fn capability_key(capability: &CapabilityRef) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        capability.kind,
        capability.name,
        capability.handler_id.as_deref().unwrap_or_default(),
        capability.authenticated_extension_id
    )
}

fn validate_capability(
    connection: &ProfileConnection,
    capability: &CapabilityRef,
    expected_kind: &str,
) -> Result<(), CodingExtensionError> {
    if capability.kind != expected_kind
        || capability.name.is_empty()
        || capability.authenticated_extension_id.is_empty()
        || capability.handler_id.as_ref().is_none_or(String::is_empty)
        || capability.profile.is_none()
    {
        return Err(CodingExtensionError::InvalidRegistration(
            "malformed capability reference",
        ));
    }
    if capability.authenticated_extension_id != connection.grant().wire().authenticated_extension_id
        || !connection
            .grant()
            .profiles()
            .iter()
            .any(|profile| profile.reference() == capability)
    {
        return Err(CodingExtensionError::MissingGrant {
            kind: capability.kind.clone(),
            name: capability.name.clone(),
        });
    }
    Ok(())
}

fn validate_provider_models(
    capability: &CapabilityRef,
    models: &[Model],
) -> Result<(), CodingExtensionError> {
    let first = models
        .first()
        .ok_or(CodingExtensionError::InvalidRegistration(
            "provider catalog is empty",
        ))?;
    if first.provider.as_str() != capability.name
        || !crate::models::is_selected_text_provider(first.provider.as_str())
    {
        return Err(CodingExtensionError::InvalidRegistration(
            "provider is outside the selected catalog scope",
        ));
    }
    if models.iter().any(|model| {
        model.provider != first.provider
            || model.headers.is_some()
            || matches!(model.base_url, ModelBaseUrl::RuntimeResolved)
    }) {
        return Err(CodingExtensionError::InvalidRegistration(
            "provider models must be same-provider, header-free, public-endpoint catalog values",
        ));
    }
    Ok(())
}

fn runtime_error(error: impl fmt::Display) -> CodingExtensionError {
    CodingExtensionError::Runtime(error.to_string())
}
