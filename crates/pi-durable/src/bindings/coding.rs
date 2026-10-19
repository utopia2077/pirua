//! Direct bindings of reusable CodingTool environment operations.
use super::presence;
use crate::*;
use pi_ai::{InputContent, JsonValue};
use pi_coding_runtime::tools::{CodingTool, ExecutionToolContext, ToolOutput, ToolOutputUpdate};
use std::sync::{Arc, Mutex};
type AdmissionState = Arc<Mutex<(bool, Option<Error>)>>;
struct AdmissionGuard {
    state: AdmissionState,
    cancellation: pi_ai::CancellationToken,
}
impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.state.lock().unwrap().0 = false;
        self.cancellation.cancel();
    }
}

/// Bind an operation directly. The runtime invokes prepare_arguments before
/// validation and commits final arguments/replay policy before this executor.
/// Replay defaults to Unsafe; an embedding host may explicitly choose Safe.
pub fn bind_coding_tool(
    operation: Arc<dyn CodingTool>,
    context: ExecutionToolContext,
) -> ToolRegistration {
    let declaration = operation.descriptor().clone();
    let prepare = operation.clone();
    let schema = operation.output_schema();
    let mut tool = ToolRegistration::new(declaration, move |invocation| {
        let operation = operation.clone();
        let context = context.clone();
        Box::pin(async move {
            invocation.api.check_live()?;
            let context = invocation.api.env()?.unwrap_or(context);
            let state = Arc::new(Mutex::new((true, None)));
            let callback_state = state.clone();
            let (updates, mut receiver) = tokio::sync::mpsc::channel(32);
            let parent_signal = invocation.api.signal();
            let signal = pi_ai::CancellationToken::new();
            let _admission = AdmissionGuard {
                state: state.clone(),
                cancellation: signal.clone(),
            };
            let stop = signal.clone();
            let on_update: ToolOutputUpdate = Arc::new(move |update| {
                let mut state = callback_state.lock().unwrap();
                if !state.0 {
                    return;
                }
                if updates.try_send(update).is_err() {
                    state.1.get_or_insert_with(|| {
                        Error::request("Coding operation progress queue exhausted")
                    });
                    stop.cancel();
                }
            });
            let execute = operation.execute(
                invocation.call.call.arguments,
                &signal,
                &on_update,
                &context,
            );
            tokio::pin!(execute);
            let mut previous = String::new();
            let result = loop {
                tokio::select! {
                    biased;
                    _ = parent_signal.cancelled() => { signal.cancel(); return Err(Error::request("Coding operation cancelled")); },
                    update = receiver.recv() => {
                        if let Some(update) = update { apply_update(&invocation.api, &mut previous, update).await?; }
                    }
                    result = &mut execute => break result,
                }
            };
            // A retained callback cannot publish after execute returns. Acquiring
            // this lock also joins callbacks already inside their admission section.
            state.lock().unwrap().0 = false;
            while let Ok(update) = receiver.try_recv() {
                apply_update(&invocation.api, &mut previous, update).await?;
            }
            if let Some(error) = state.lock().unwrap().1.take() {
                return Err(error);
            }
            invocation.api.check_live()?;
            Ok(convert(result.map_err(|e| Error::request(e.message))?))
        })
    });
    tool.structured_output_schema = schema;
    tool.prepare_arguments = Some(Arc::new(move |args| {
        prepare
            .prepare_arguments(JsonValue::Object(args))
            .map_err(|e| Error::request(e.message))
    }));
    tool
}
async fn apply_update(
    api: &ToolExecutionApi,
    previous: &mut String,
    value: ToolOutput,
) -> Result<()> {
    api.check_live()?;
    let next = output_text(&value.content);
    let shared = overlap(previous, &next);
    if shared < next.len() {
        let chunk = OutputChunk::Text(next[shared..].into());
        if api.progress_options().enabled {
            api.report_progress(ToolProgress::Output {
                chunk,
                skipped: None,
            })
            .await?;
        } else {
            api.output(chunk, None)?;
        }
    }
    *previous = next;
    if let Some(details) = value.details {
        api.details(details).await?;
    }
    Ok(())
}
fn convert(value: ToolOutput) -> ToolExecutionResult {
    ToolExecutionResult {
        output: Some(value.content),
        details: presence(value.details),
        structured_output: presence(value.structured_content),
        usage: value.usage,
        is_error: value.is_error,
        control: value.terminate.map(|terminate| ToolControl {
            terminate: Some(terminate),
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn output_text(content: &[InputContent]) -> String {
    content
        .iter()
        .filter_map(|c| match c {
            InputContent::Text(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn overlap(previous: &str, next: &str) -> usize {
    let max = previous.len().min(next.len());
    (0..=max)
        .rev()
        .find(|&n| {
            previous.is_char_boundary(previous.len() - n)
                && next.is_char_boundary(n)
                && previous[previous.len() - n..] == next[..n]
        })
        .unwrap_or(0)
}
