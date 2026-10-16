//! Explicit shell identity projection from a host-owned detached snapshot.
//!
//! The host resolves current session/model state before each invocation. This
//! module retains no session handle, scheduler lock or runtime authority.

use crate::tools::{
    EnvResult, ExecutionEnv, ExecutionToolContext, FileInfo, ProcessOptions, ProcessOutput,
    ProcessedImage,
};
use pi_ai::BoxFuture;
use std::{collections::BTreeMap, sync::Arc};

/// Host-resolved process identity for one invocation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShellIdentity {
    pub session_id: String,
    pub session_file: String,
    pub provider: String,
    pub model: String,
    pub reasoning_level: String,
}

/// Add authoritative PI_* values while preserving the host process options.
pub fn with_shell_identity(
    host: Arc<dyn ExecutionEnv>,
    identity: ShellIdentity,
) -> ExecutionToolContext {
    let env = [
        ("PI_SESSION_ID", identity.session_id),
        ("PI_SESSION_FILE", identity.session_file),
        ("PI_PROVIDER", identity.provider),
        ("PI_MODEL", identity.model),
        ("PI_REASONING_LEVEL", identity.reasoning_level),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value))
    .collect();
    ExecutionToolContext {
        env: Arc::new(ShellIdentityEnv { host, env }),
    }
}

struct ShellIdentityEnv {
    host: Arc<dyn ExecutionEnv>,
    env: BTreeMap<String, String>,
}

impl ExecutionEnv for ShellIdentityEnv {
    fn walk_files<'a>(
        &'a self,
        root: &'a str,
        options: crate::tools::SearchOptions,
    ) -> Option<BoxFuture<'a, EnvResult<Vec<FileInfo>>>> {
        self.host.walk_files(root, options)
    }
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
        options: &'a crate::tools::ReadToolOptions,
    ) -> BoxFuture<'a, EnvResult<Option<ProcessedImage>>> {
        self.host.process_image(bytes, mime_type, options)
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
