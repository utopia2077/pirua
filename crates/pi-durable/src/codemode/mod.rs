//! Persistent local JavaScript tool orchestration over the native CodeMode worker.
mod describe;
mod documents;
mod execution;
mod output;
mod search;
pub use documents::*;

use crate::*;
use pi_codemode::Sandbox;
use serde_json::Value;
use std::sync::Arc;

pub const CODEMODE_TOOL_NAME: &str = "codemode";
pub const DEFAULT_MAX_OUTPUT_TOKENS: usize = 10_000;
pub const DEFAULT_INLINE_BUDGET: usize = 3000;

#[derive(Clone, Debug)]
pub struct CodeModeNamespace {
    pub name: String,
    pub prefix: String,
    pub description: Option<String>,
    pub instructions: Option<String>,
}
/// Explicit host global; `None` is JS undefined and `Some(Null)` is JSON null.
pub type CodeModeGlobalFn =
    dyn Fn(Option<Value>, ToolExecutionApi) -> RuntimeFuture<Option<Value>> + Send + Sync;
#[derive(Clone)]
pub struct CodeModeGlobal {
    pub name: String,
    pub spread: bool,
    pub execute: Arc<CodeModeGlobalFn>,
}
#[derive(Clone)]
pub struct CodeModeOptions {
    pub sandbox: Sandbox,
    pub namespaces: Vec<CodeModeNamespace>,
    pub globals: Vec<CodeModeGlobal>,
    pub inline_budget: usize,
    /// Host entropy used only when first recording immutable start data.
    pub seed: Arc<dyn Fn() -> Result<u32> + Send + Sync>,
    #[cfg(test)]
    observe: Option<Arc<dyn Fn(Value) + Send + Sync>>,
}
impl CodeModeOptions {
    pub fn new(sandbox: Sandbox) -> Self {
        Self {
            #[cfg(test)]
            observe: None,
            sandbox,
            namespaces: Vec::new(),
            globals: Vec::new(),
            inline_budget: DEFAULT_INLINE_BUDGET,
            seed: Arc::new(|| {
                use std::io::Read;
                let mut seed = [0; 4];
                std::fs::File::open("/dev/urandom")
                    .and_then(|mut f| f.read_exact(&mut seed))
                    .map_err(|e| Error::request(format!("CodeMode seed: {e}")))?;
                Ok(u32::from_ne_bytes(seed))
            }),
        }
    }
}
pub fn create_codemode_tool(options: CodeModeOptions) -> Result<ToolRegistration> {
    let declaration = serde_json::from_value(
        serde_json::json!({"name":CODEMODE_TOOL_NAME,"description":describe::DESCRIPTION,"parameters":{"type":"object","properties":{"code":{"type":"string","description":"Raw JavaScript source."}},"required":["code"],"additionalProperties":false}}),
    )?;
    let mut tool = ToolRegistration::new(declaration, move |inv| {
        let options = options.clone();
        Box::pin(async move {
            let arguments = serde_json::to_value(&inv.call.call.arguments)?;
            let source = arguments
                .get("code")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::request("CodeMode expects {code: string}"))?
                .to_owned();
            execution::execute(options, inv.api, source).await
        })
    });
    tool.replay = ReplayPolicy::Safe;
    tool.callers = Some(vec![ToolCaller::Model]);
    tool.output_limits = OutputLimits {
        max_bytes: 4 * 1024 * 1024,
        max_lines: 100_000,
        retain: OutputRetention::Head,
    };
    Ok(tool)
}
pub fn create_codemode_extension(options: CodeModeOptions) -> Result<Extension> {
    let section_options = options.clone();
    Ok(Extension {
        name: "codemode".into(),
        tools: vec![create_codemode_tool(options)?],
        sections: vec![PromptSection {
            key: "codemode".into(),
            tag: true,
            render: Arc::new(move |input| {
                let options = section_options.clone();
                Box::pin(async move {
                    Ok(describe::section(
                        &input.agent.callable,
                        &options.namespaces,
                        options.inline_budget,
                    ))
                })
            }),
        }],
        ..Default::default()
    })
}

#[cfg(test)]
mod tests;
