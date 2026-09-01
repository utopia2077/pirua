//! Behavior-free Harness-wide surface signatures.

use std::collections::BTreeMap;
use std::sync::Arc;

use pi_ai_core::{JsonObject, JsonValue, Message, Model, ToolCallId, ToolName};

use crate::surface::BoxFuture;
use crate::{
    AgentHarnessStreamOptions, AgentLane, AgentMessage, AgentTool, AgentToolResult,
    CompactionSettings, CreateLaneExpectedError, CustomEntry, CustomType, EventListener,
    HandlerError, HandlerId, HarnessCall, HarnessEventType, HarnessRuntimeError,
    HookRegistrationError, HookSpec, LaneId, LaneInfo, QueueMode, Resources, RetryPolicy, Session,
    SessionSnapshot, SuspendedOperation, ToolExecutionError, ToolExecutionMode, WatchHandle,
};

/// Automatic or manually gated Harness drive mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DriveMode {
    /// Procedures drive automatically.
    #[default]
    Automatic,
    /// Every effect boundary is manually released.
    Manual,
}

/// Invocation-scoped cancellation observation for tools and assistant streams.
pub trait Cancellation: Send + Sync {
    /// Returns whether cancellation has been requested.
    fn is_cancelled(&self) -> bool;
    /// Resolves once cancellation is requested.
    fn cancelled(&self) -> BoxFuture<'_, ()>;
}

/// Invocation-scoped tool progress sink.
pub trait ToolProgressSink: Send + Sync {
    /// Publishes a partial result while the invocation remains active.
    fn update(&self, partial: AgentToolResult) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
}

/// Executable Harness tool binding with an application-defined context.
pub trait AgentHarnessTool<C>: Send + Sync {
    /// Returns the callback-free descriptor used for model visibility and persistence decisions.
    fn descriptor(&self) -> &AgentTool;
    /// Applies the deterministic compatibility argument shim before schema validation.
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError>;
    /// Executes the selected tool with its fifth context argument.
    fn execute<'a>(
        &'a self,
        tool_call_id: ToolCallId,
        params: JsonObject,
        cancellation: &'a dyn Cancellation,
        progress: &'a dyn ToolProgressSink,
        context: &'a C,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>>;
}

/// Shared executable Harness tool handle.
pub type AgentHarnessToolHandle<C> = Arc<dyn AgentHarnessTool<C>>;

/// Process-local provider for one tool-context snapshot.
pub trait ToolContextProvider<C>: Send + Sync {
    /// Resolves one context value for a live/restored batch.
    fn resolve(&self) -> BoxFuture<'_, Result<C, HarnessRuntimeError>>;
}

/// Static tool context or process-local provider.
pub enum AgentHarnessToolContextSource<C> {
    /// Static context cloned or borrowed by runtime ownership policy.
    Static(C),
    /// Context resolved once for each tool batch.
    Provider(Arc<dyn ToolContextProvider<C>>),
}

/// Process-local system-prompt callback.
pub trait SystemPromptProvider<C>: Send + Sync {
    /// Resolves the prompt for one request.
    fn resolve<'a>(&'a self, context: &'a C) -> BoxFuture<'a, Result<String, HarnessRuntimeError>>;
}

/// Static system prompt or process-local per-request provider.
pub enum SystemPromptSource<C> {
    /// Static prompt.
    Static(String),
    /// Context-bound provider.
    Provider(Arc<dyn SystemPromptProvider<C>>),
}

/// Process-local Agent-to-provider message converter.
pub trait ProviderMessageConverter: Send + Sync {
    /// Converts projected Agent messages into provider-neutral messages.
    fn convert(
        &self,
        messages: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<Message>, HarnessRuntimeError>>;
}

/// Process-local custom-entry projector.
pub trait EntryProjector: Send + Sync {
    /// Projects one custom entry into zero or more Agent messages.
    fn project(
        &self,
        entry: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>>;
}

/// Process-local Harness creation options. No callback or handle is serializable.
pub struct AgentHarnessOptions<C, T, S, M>
where
    S: Session + ?Sized,
    M: Send + Sync + ?Sized,
{
    /// Typed session owner.
    pub session: Arc<S>,
    /// Runtime-selected Models service handle.
    pub models: Arc<M>,
    /// Immutable new-lane model seed.
    pub model: Model,
    /// Optional new-lane thinking seed.
    pub thinking_level: Option<crate::AgentThinkingLevel>,
    /// Optional new-lane active-tool seed.
    pub active_tool_names: Option<Vec<ToolName>>,
    /// Initial executable tool bindings.
    pub tools: Vec<AgentHarnessToolHandle<C>>,
    /// Optional context source for tool execution and prompt providers.
    pub tool_context: Option<AgentHarnessToolContextSource<C>>,
    /// Optional static or context-bound system prompt.
    pub system_prompt: Option<SystemPromptSource<C>>,
    /// Initial resources.
    pub resources: Resources,
    /// Initial curated stream options.
    pub stream_options: AgentHarnessStreamOptions,
    /// Initial retry policy.
    pub retry: RetryPolicy,
    /// Initial compaction settings.
    pub compaction: CompactionSettings,
    /// Initial steering mode.
    pub steering_mode: QueueMode,
    /// Initial follow-up mode.
    pub follow_up_mode: QueueMode,
    /// Initial tool execution mode.
    pub tool_execution: ToolExecutionMode,
    /// Automatic or manual drive.
    pub drive: DriveMode,
    /// Optional custom provider-message conversion.
    pub to_provider_messages: Option<Arc<dyn ProviderMessageConverter>>,
    /// Open custom-type projectors in deterministic key order.
    pub entry_projectors: BTreeMap<CustomType, Arc<dyn EntryProjector>>,
    /// Opaque process-local telemetry context.
    pub telemetry_context: T,
}

/// Result of restore-only Harness creation.
pub struct AgentHarnessCreate<H>
where
    H: ?Sized,
{
    /// Created Harness handle.
    pub harness: Arc<H>,
    /// One descriptor for each open restored operation.
    pub suspended: Vec<SuspendedOperation>,
}

/// Behavior-free factory projection for Harness creation.
pub trait AgentHarnessFactory<C, T>: Send + Sync {
    /// Concrete or erased Session type.
    type Session: Session + ?Sized + 'static;
    /// Runtime Models service type.
    type Models: Send + Sync + ?Sized + 'static;
    /// Concrete or erased Harness type.
    type Harness: AgentHarness<C> + ?Sized + 'static;

    /// Creates a Harness and restores without starting external effects.
    fn create(
        &self,
        options: AgentHarnessOptions<C, T, Self::Session, Self::Models>,
    ) -> BoxFuture<'_, Result<AgentHarnessCreate<Self::Harness>, HarnessRuntimeError>>;
}

/// Exact Harness factory method inventory.
pub const AGENT_HARNESS_FACTORY_METHODS: &[&str] = &["create"];

/// One typed process-local hook handler.
pub trait HookHandler<S>: Send + Sync
where
    S: HookSpec,
{
    /// Runs one invocation through its hook-specific result mapping.
    fn call(&self, event: S::Event) -> BoxFuture<'_, Result<S::Result, HandlerError>>;
}

/// Unsubscribe handle for one hook registration.
pub trait HookSubscription: Send + Sync {
    /// Removes the registered handler.
    fn unsubscribe(&self);
}

/// Object-safe typed registration surface for all eleven hooks.
pub trait Hooks: Send + Sync {
    /// Registers `before_run`.
    fn before_run(
        &self,
        id: HandlerId,
        handler: Arc<dyn HookHandler<crate::BeforeRun>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `before_resume`.
    fn before_resume(
        &self,
        id: HandlerId,
        handler: Arc<dyn HookHandler<crate::BeforeResume>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `before_run_end`.
    fn before_run_end(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::BeforeRunEnd>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `transform_context`.
    fn transform_context(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::TransformContext>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `before_request`.
    fn before_request(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::BeforeRequest>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `before_payload`.
    fn before_payload(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::BeforePayload>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `after_response`.
    fn after_response(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::AfterResponse>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers fail-closed `before_tool`.
    fn before_tool(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::BeforeTool>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `after_tool`.
    fn after_tool(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::AfterTool>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `before_compaction`.
    fn before_compaction(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::BeforeCompaction>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
    /// Registers `before_navigation`.
    fn before_navigation(
        &self,
        id: Option<HandlerId>,
        handler: Arc<dyn HookHandler<crate::BeforeNavigation>>,
    ) -> Result<Arc<dyn HookSubscription>, HookRegistrationError>;
}

/// Unsubscribe handle for one passive event registration.
pub trait EventSubscription: Send + Sync {
    /// Removes the listener.
    fn unsubscribe(&self);
}

/// Object-safe passive event registration surface.
pub trait Events: Send + Sync {
    /// Registers a listener for one closed event tag.
    fn on(
        &self,
        event_type: HarnessEventType,
        listener: Arc<dyn EventListener>,
    ) -> Arc<dyn EventSubscription>;
}

/// Nested lane-creation call.
pub type CreateLaneCall<L> = HarnessCall<Arc<L>, CreateLaneExpectedError>;

/// Behavior-free Harness-wide public surface.
pub trait AgentHarness<C>: AgentLane {
    /// Concrete or erased lane handle.
    type Lane: AgentLane + ?Sized + 'static;

    /// Looks up a lane without creating it.
    fn lane(
        &self,
        name: LaneId,
    ) -> BoxFuture<'_, Result<Option<Arc<Self::Lane>>, HarnessRuntimeError>>;
    /// Creates a lane at an existing entry or root.
    fn create_lane(
        &self,
        name: LaneId,
        at: Option<crate::EntryId>,
    ) -> BoxFuture<'_, CreateLaneCall<Self::Lane>>;
    /// Lists all lanes, including `main`.
    fn lanes(&self) -> BoxFuture<'_, Result<Vec<LaneInfo>, HarnessRuntimeError>>;
    /// Reads the process-local tool registry.
    fn get_tools(
        &self,
    ) -> BoxFuture<'_, Result<Vec<AgentHarnessToolHandle<C>>, HarnessRuntimeError>>;
    /// Replaces only the process-local tool registry.
    fn set_tools(
        &self,
        tools: Vec<AgentHarnessToolHandle<C>>,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads current prompt resources.
    fn get_resources(&self) -> BoxFuture<'_, Result<Resources, HarnessRuntimeError>>;
    /// Replaces current prompt resources.
    fn set_resources(&self, resources: Resources)
    -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads current base stream options.
    fn get_stream_options(
        &self,
    ) -> BoxFuture<'_, Result<AgentHarnessStreamOptions, HarnessRuntimeError>>;
    /// Replaces current base stream options.
    fn set_stream_options(
        &self,
        options: AgentHarnessStreamOptions,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads current public retry policy.
    fn get_retry_policy(&self) -> BoxFuture<'_, Result<RetryPolicy, HarnessRuntimeError>>;
    /// Replaces current retry policy.
    fn set_retry_policy(
        &self,
        policy: RetryPolicy,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads current compaction settings.
    fn get_compaction_settings(
        &self,
    ) -> BoxFuture<'_, Result<CompactionSettings, HarnessRuntimeError>>;
    /// Replaces current compaction settings.
    fn set_compaction_settings(
        &self,
        settings: CompactionSettings,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads steering drain mode.
    fn get_steering_mode(&self) -> BoxFuture<'_, Result<QueueMode, HarnessRuntimeError>>;
    /// Replaces steering drain mode.
    fn set_steering_mode(&self, mode: QueueMode) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Reads follow-up drain mode.
    fn get_follow_up_mode(&self) -> BoxFuture<'_, Result<QueueMode, HarnessRuntimeError>>;
    /// Replaces follow-up drain mode.
    fn set_follow_up_mode(&self, mode: QueueMode)
    -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
    /// Creates a snapshot-first session watch handle.
    fn watch_session(
        &self,
    ) -> BoxFuture<'_, Result<Arc<dyn WatchHandle<SessionSnapshot>>, HarnessRuntimeError>>;
    /// Returns the typed hook registration surface.
    fn hooks(&self) -> &dyn Hooks;
    /// Returns the passive event registration surface.
    fn events(&self) -> &dyn Events;
    /// Detaches without durable cancellation or settlement.
    fn close(&self) -> BoxFuture<'_, Result<(), HarnessRuntimeError>>;
}

/// Exact Harness-own method/property inventory, excluding inherited AgentLane methods.
pub const AGENT_HARNESS_METHODS: &[&str] = &[
    "lane",
    "create_lane",
    "lanes",
    "get_tools",
    "set_tools",
    "get_resources",
    "set_resources",
    "get_stream_options",
    "set_stream_options",
    "get_retry_policy",
    "set_retry_policy",
    "get_compaction_settings",
    "set_compaction_settings",
    "get_steering_mode",
    "set_steering_mode",
    "get_follow_up_mode",
    "set_follow_up_mode",
    "watch_session",
    "hooks",
    "events",
    "close",
];

/// Harness properties exposing callback registration surfaces.
pub const AGENT_HARNESS_PROPERTIES: &[&str] = &["hooks", "events"];
