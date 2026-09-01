//! Behavior-free storage, session, planner, lane, harness, and loop interfaces.
//!
//! Process-local plans, callback ports, live bindings, and runtime errors never implement Serde.
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::{Action, AssistantEffectPlan, PendingGeneration};
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<PendingGeneration>();
//! assert_serde::<AssistantEffectPlan<()>>();
//! assert_serde::<Action<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::HookEffectOutput;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<HookEffectOutput>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::{
//!     AgentHarnessOptions, AgentHarnessTool, AgentHarnessToolContextSource, Cancellation,
//!     Session, SessionMetadata, StreamAssistantConfig, ToolBatchInput,
//! };
//! use serde::Serialize;
//! fn assert_serde<T: Serialize + ?Sized>() {}
//! type Options = AgentHarnessOptions<
//!     (), (), dyn Session<Metadata = SessionMetadata>, ()
//! >;
//! assert_serde::<ToolBatchInput<'static, ()>>();
//! assert_serde::<AgentHarnessToolContextSource<()>>();
//! assert_serde::<Options>();
//! assert_serde::<StreamAssistantConfig<(), ()>>();
//! assert_serde::<dyn Cancellation>();
//! assert_serde::<dyn AgentHarnessTool<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::{CallbackError, HarnessRuntimeError, RunWhenIdleError};
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<CallbackError>();
//! assert_serde::<HarnessRuntimeError>();
//! assert_serde::<RunWhenIdleError>();
//! ```
//!
//! The individual guards below prevent one remaining negative from masking another.
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::PendingGeneration;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<PendingGeneration>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::AssistantEffectPlan;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<AssistantEffectPlan<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::Action;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<Action<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::PendingSummaryGeneration;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<PendingSummaryGeneration>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::PendingDeferred;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<PendingDeferred>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::EffectPlan;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<EffectPlan<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::ToolBatchInput;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<ToolBatchInput<'static, ()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::AgentHarnessToolContextSource;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<AgentHarnessToolContextSource<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::{AgentHarnessOptions, Session, SessionMetadata};
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! type Options = AgentHarnessOptions<
//!     (), (), dyn Session<Metadata = SessionMetadata>, ()
//! >;
//! assert_serde::<Options>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::StreamAssistantConfig;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<StreamAssistantConfig<(), ()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::Cancellation;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize + ?Sized>() {}
//! assert_serde::<dyn Cancellation>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::AgentHarnessTool;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize + ?Sized>() {}
//! assert_serde::<dyn AgentHarnessTool<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::CallbackError;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<CallbackError>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::HarnessRuntimeError;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<HarnessRuntimeError>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::RunWhenIdleError;
//! use serde::Serialize;
//! fn assert_serde<T: Serialize>() {}
//! assert_serde::<RunWhenIdleError>();
//! ```
//!
//! Deserialization has independent anti-masking guards for the same families.
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::PendingGeneration;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<PendingGeneration>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::AssistantEffectPlan;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<AssistantEffectPlan<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::Action;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<Action<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::PendingSummaryGeneration;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<PendingSummaryGeneration>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::PendingDeferred;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<PendingDeferred>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::EffectPlan;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<EffectPlan<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::HookEffectOutput;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<HookEffectOutput>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::ToolBatchInput;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<ToolBatchInput<'static, ()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::AgentHarnessToolContextSource;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<AgentHarnessToolContextSource<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::{AgentHarnessOptions, Session, SessionMetadata};
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! type Options = AgentHarnessOptions<
//!     (), (), dyn Session<Metadata = SessionMetadata>, ()
//! >;
//! assert_serde::<Options>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::StreamAssistantConfig;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<StreamAssistantConfig<(), ()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::Cancellation;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned + ?Sized>() {}
//! assert_serde::<dyn Cancellation>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::AgentHarnessTool;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned + ?Sized>() {}
//! assert_serde::<dyn AgentHarnessTool<()>>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::CallbackError;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<CallbackError>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::HarnessRuntimeError;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<HarnessRuntimeError>();
//! ```
//!
//! ```compile_fail,E0277
//! use pi_agent_schema::RunWhenIdleError;
//! use serde::de::DeserializeOwned;
//! fn assert_serde<T: DeserializeOwned>() {}
//! assert_serde::<RunWhenIdleError>();
//! ```

use std::future::Future;
use std::pin::Pin;

/// Sendable boxed future used by object-safe asynchronous interfaces.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub mod agent_loop;
pub mod harness;
pub mod lane;
pub mod planner;
pub mod search;
pub mod session;
pub mod storage;

pub use agent_loop::*;
pub use harness::*;
pub use lane::*;
pub use planner::*;
pub use search::*;
pub use session::*;
pub use storage::*;
