//! Detached coding-runtime settings captured for later Harness composition.
//!
//! The runtime receives these values from its host.  It does not read settings
//! files, persist credentials, or perform provider/network work while creating
//! a snapshot.  Provider-neutral IDs, stream options, retry policy, and
//! thinking levels come directly from `pi-ai-core`; queue and compaction settings
//! are reused from the Agent schema through its public facade.

use pi_ai::{ModelId, ModelThinkingLevel, ProviderId, RetryPolicy, StreamOptionsWire, ToolName};

/// A provider-scoped model identity selected for a coding run.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelRef {
    /// Stable provider identity.
    pub provider: ProviderId,
    /// Provider-scoped model identity.
    pub model_id: ModelId,
}

impl ModelRef {
    /// Creates a model reference from validated provider and model identities.
    pub const fn new(provider: ProviderId, model_id: ModelId) -> Self {
        Self { provider, model_id }
    }
}

/// The public model thinking level, including disabled thinking.
pub type ThinkingLevel = ModelThinkingLevel;

pub use pi_agent::{CompactionSettings, QueueMode};

/// Detached normalized settings required by the later coding Harness builder.
///
/// This type contains no handles to a provider, credential store, session
/// writer, filesystem, process, or network client.  A builder may therefore
/// clone it at a composition boundary and apply it to a Harness-owned lane.
#[derive(Clone, Debug, PartialEq)]
pub struct CodingSettingsSnapshot {
    /// Provider-scoped model selected for the next run.
    pub model: ModelRef,
    /// Requested model thinking level.
    pub thinking_level: ThinkingLevel,
    /// Model-visible coding tools in host-supplied order.
    pub active_tools: Vec<ToolName>,
    /// Normalized provider retry policy.
    pub retry: RetryPolicy,
    /// Compaction thresholds for this run.
    pub compaction: CompactionSettings,
    /// Provider-neutral stream options.
    pub stream: StreamOptionsWire,
    /// Steering queue drain mode.
    pub steering_mode: QueueMode,
    /// Follow-up queue drain mode.
    pub follow_up_mode: QueueMode,
}

impl CodingSettingsSnapshot {
    /// Captures already-normalized host values without side effects.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        model: ModelRef,
        thinking_level: ThinkingLevel,
        active_tools: Vec<ToolName>,
        retry: RetryPolicy,
        compaction: CompactionSettings,
        stream: StreamOptionsWire,
        steering_mode: QueueMode,
        follow_up_mode: QueueMode,
    ) -> Self {
        Self {
            model,
            thinking_level,
            active_tools,
            retry,
            compaction,
            stream,
            steering_mode,
            follow_up_mode,
        }
    }
}
