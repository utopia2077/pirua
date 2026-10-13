//! Private per-batch shell identity composition. The Harness/session own identity;
//! the supplied environment continues to own every filesystem and process call.

use crate::tools::{EnvResult, ProcessedImage};
use crate::{ExecutionEnv, ExecutionToolContext, FileInfo, ProcessOptions, ProcessOutput};
use pi_agent::schema::{
    AgentLane, BoxFuture, ErrorContext, HarnessClosed, HarnessFault, HarnessRuntimeError, Register,
    RegisterNamespace, Session, SessionMetadataRecord, ToolContextProvider,
};
use pi_agent_session::{session::RuntimeSession, storage::RuntimeStorage};
use pi_harness_runtime::owner::RuntimeHarness;
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock, Weak},
};

pub(crate) struct ShellContextProvider<S: RuntimeStorage, M: SessionMetadataRecord> {
    env: Arc<dyn ExecutionEnv>,
    session: Weak<RuntimeSession<S, M>>,
    harness: OnceLock<Weak<RuntimeHarness<ExecutionToolContext, S, M>>>,
}

impl<S: RuntimeStorage + 'static, M: SessionMetadataRecord> ShellContextProvider<S, M> {
    pub(crate) fn new(env: Arc<dyn ExecutionEnv>, session: &Arc<RuntimeSession<S, M>>) -> Self {
        Self {
            env,
            session: Arc::downgrade(session),
            harness: OnceLock::new(),
        }
    }

    pub(crate) fn bind(&self, harness: &Arc<RuntimeHarness<ExecutionToolContext, S, M>>) {
        // Factory creation only hydrates suspended work; it does not drive it.
        // Bind synchronously before the builder exposes the attachment. A weak
        // link avoids Owner -> context provider -> Harness -> Owner retention.
        assert!(
            self.harness.set(Arc::downgrade(harness)).is_ok(),
            "shell context bound once"
        );
    }
}

impl<S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    ToolContextProvider<ExecutionToolContext> for ShellContextProvider<S, M>
{
    fn resolve(&self) -> BoxFuture<'_, Result<ExecutionToolContext, HarnessRuntimeError>> {
        Box::pin(async move {
            let harness = self
                .harness
                .get()
                .and_then(Weak::upgrade)
                .ok_or(HarnessRuntimeError::Closed(HarnessClosed))?;
            let session = self
                .session
                .upgrade()
                .ok_or(HarnessRuntimeError::Closed(HarnessClosed))?;
            // Check the live owner's read admission before consulting its session.
            // Read identity and thinking from one authoritative durable record;
            // registry availability must not erase a persisted model identity.
            harness.get_thinking_level().await?;
            let configuration = session
                .get_register(RegisterNamespace::LaneConfig, harness.name().as_str())
                .await
                .map_err(|_| context_fault())?;
            let Some(Register::LaneConfig {
                value: configuration,
                ..
            }) = configuration
            else {
                return Err(context_fault());
            };
            let metadata = session.metadata();
            // SessionMetadataRecord exposes base identity but no host file path.
            // A generic attachment cannot infer a session file from cwd or ID.
            let env = [
                ("PI_SESSION_ID", metadata.base_metadata().id.as_str()),
                ("PI_SESSION_FILE", ""),
                ("PI_PROVIDER", configuration.model.provider.as_str()),
                ("PI_MODEL", configuration.model.model_id.as_str()),
                (
                    "PI_REASONING_LEVEL",
                    match configuration.thinking_level {
                        pi_ai::ModelThinkingLevel::Off => "off",
                        pi_ai::ModelThinkingLevel::Minimal => "minimal",
                        pi_ai::ModelThinkingLevel::Low => "low",
                        pi_ai::ModelThinkingLevel::Medium => "medium",
                        pi_ai::ModelThinkingLevel::High => "high",
                        pi_ai::ModelThinkingLevel::ExtraHigh => "xhigh",
                        pi_ai::ModelThinkingLevel::Max => "max",
                    },
                ),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
            Ok(ExecutionToolContext {
                env: Arc::new(ShellIdentityEnv {
                    host: self.env.clone(),
                    env,
                }),
            })
        })
    }
}

fn context_fault() -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(HarnessFault::new(
        ErrorContext::new("shell context configuration is unavailable").expect("static context"),
    ))
}

struct ShellIdentityEnv {
    host: Arc<dyn ExecutionEnv>,
    env: BTreeMap<String, String>,
}

impl ExecutionEnv for ShellIdentityEnv {
    fn absolute_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.host.absolute_path(path)
    }
    fn exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<bool>> {
        self.host.exists(path)
    }
    fn read_binary_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<u8>>> {
        self.host.read_binary_file(path)
    }
    fn process_image<'a>(
        &'a self,
        bytes: &'a [u8],
        mime_type: &'a str,
        auto_resize: bool,
    ) -> BoxFuture<'a, EnvResult<Option<ProcessedImage>>> {
        self.host.process_image(bytes, mime_type, auto_resize)
    }
    fn read_text_file<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.host.read_text_file(path)
    }
    fn write_file<'a>(&'a self, path: &'a str, content: &'a [u8]) -> BoxFuture<'a, EnvResult<()>> {
        self.host.write_file(path, content)
    }
    fn list_dir<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<Vec<FileInfo>>> {
        self.host.list_dir(path)
    }
    fn file_info<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<FileInfo>> {
        self.host.file_info(path)
    }
    fn canonical_path<'a>(&'a self, path: &'a str) -> BoxFuture<'a, EnvResult<String>> {
        self.host.canonical_path(path)
    }
    fn exec<'a>(
        &'a self,
        shell: &'a str,
        command: &'a str,
        mut options: ProcessOptions,
    ) -> BoxFuture<'a, EnvResult<ProcessOutput>> {
        options.env.extend(self.env.clone());
        self.host.exec(shell, command, options)
    }
    fn create_temp_file<'a>(
        &'a self,
        prefix: &'a str,
        content: &'a [u8],
    ) -> BoxFuture<'a, EnvResult<String>> {
        self.host.create_temp_file(prefix, content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shell_context_rejects_unbound_and_expired_authority() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let provider = ShellContextProvider::<
                pi_agent_session::memory::MemoryStorage,
                pi_agent::schema::SessionMetadata,
            > {
                env: Arc::new(crate::LocalExecutionEnv::new(std::env::temp_dir())),
                session: Weak::new(),
                harness: OnceLock::new(),
            };
            assert!(matches!(
                provider.resolve().await,
                Err(HarnessRuntimeError::Closed(_))
            ));
            assert!(provider.harness.set(Weak::new()).is_ok());
            assert!(matches!(
                provider.resolve().await,
                Err(HarnessRuntimeError::Closed(_))
            ));
        });
    }
}
