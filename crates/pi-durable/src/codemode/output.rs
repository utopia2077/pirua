use super::{
    documents::{field, read},
    *,
};
use base64::Engine;
use pi_ai::{ImageContent, InputContent, Presence, TextContent};
use pi_codemode::{ExecutionResult, OutputItem};
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn text(s: impl Into<String>) -> InputContent {
    InputContent::Text(TextContent {
        content_type: Default::default(),
        text: s.into(),
        text_signature: None,
    })
}
fn join(items: Vec<InputContent>) -> Vec<InputContent> {
    let mut result = Vec::new();
    let mut parts = String::new();
    let mut pending = false;
    for item in items {
        match item {
            InputContent::Text(t) => {
                if !parts.is_empty() && !parts.ends_with('\n') {
                    parts.push('\n')
                }
                parts.push_str(&t.text);
                pending = true;
            }
            other => {
                if pending {
                    result.push(text(std::mem::take(&mut parts)));
                    pending = false;
                }
                result.push(other);
            }
        }
    }
    if pending {
        result.push(text(parts))
    }
    result
}
fn slice(s: &str, units: usize, tail: bool) -> String {
    let mut out = Vec::new();
    let mut used = 0;
    let chars: Box<dyn Iterator<Item = char>> = if tail {
        Box::new(s.chars().rev())
    } else {
        Box::new(s.chars())
    };
    for c in chars {
        if used + c.len_utf16() > units {
            break;
        }
        used += c.len_utf16();
        out.push(c);
    }
    if tail {
        out.reverse()
    }
    out.into_iter().collect()
}
async fn write(
    api: &ToolExecutionApi,
    name: &str,
    data: &[u8],
) -> Result<std::result::Result<String, String>> {
    api.check_live()?;
    let Some(context) = api.env()? else {
        return Ok(Err("no execution environment".into()));
    };
    let scope = crate::session::Scope::Task {
        task_id: api.identity().invocation.task_id,
    };
    let scope_copy = scope.clone();
    let recorded = api
        .commit(move |tx| {
            Ok(
                read::<CodeModeRunState>(tx, &codemode_run_doc(), scope_copy)?
                    .1
                    .output_dir,
            )
        })
        .await?;
    let directory = if let Some(recorded) = recorded {
        recorded
    } else {
        // Retain the reservation file. The existing host's allocator provides a
        // process-local PID/counter name, not an exclusive cross-process claim.
        let path = match context.env.create_temp_file("pi-codemode", &[]).await {
            Ok(path) => path,
            Err(e) => return Ok(Err(e.message)),
        };
        api.check_live()?;
        let candidate = format!("{path}-output");
        api.commit(move |tx| {
            let (id, run) = read::<CodeModeRunState>(tx, &codemode_run_doc(), scope)?;
            if let Some(path) = run.output_dir {
                return Ok(path);
            }
            field(tx, id, "outputDir", &candidate)?;
            Ok(candidate)
        })
        .await?
    };
    let path = format!("{directory}/{name}");
    let written = context.env.write_file(&path, data).await;
    api.check_live()?;
    Ok(written.map(|()| path).map_err(|e| e.message))
}
pub(super) async fn format(
    api: &ToolExecutionApi,
    start: &CodeModeStart,
    result: ExecutionResult,
    calls: Vec<Value>,
) -> Result<ToolExecutionResult> {
    let success = result.is_ok();
    let (mut output, error) = match result {
        ExecutionResult::Success {
            value, mut output, ..
        } => {
            if let Some(value) = value {
                output.push(OutputItem::Text {
                    text: value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                    console: false,
                });
            }
            (output, None)
        }
        ExecutionResult::Error { output, error, .. } => (output, Some(error)),
    };
    let total = output
        .iter()
        .filter(|i| matches!(i, OutputItem::Text { console: false, .. }))
        .count();
    let mut items = Vec::new();
    let mut console = Vec::new();
    let mut i = 0;
    for item in output.drain(..) {
        match item {
            OutputItem::Text {
                text: t,
                console: false,
            } => {
                i += 1;
                items.push(text(if total > 1 {
                    format!("==> text {i}/{total} <==\n{t}")
                } else {
                    t
                }));
            }
            OutputItem::Text {
                text,
                console: true,
            } => console.push(text),
            OutputItem::Image { data, mime_type } => {
                items.push(InputContent::Image(ImageContent {
                    content_type: Default::default(),
                    data,
                    mime_type,
                }))
            }
        }
    }
    if !console.is_empty() {
        items.push(text(format!(
            "<console_output>\n{}\n</console_output>",
            console.join("\n")
        )))
    }
    if let Some(e) = error {
        let head = match e.kind {
            pi_codemode::ErrorKind::Script => e.stack.unwrap_or_else(|| {
                format!(
                    "{}: {}",
                    e.name.unwrap_or_else(|| "Error".into()),
                    e.message
                )
            }),
            pi_codemode::ErrorKind::Timeout => format!("Script timed out: {}", e.message),
            pi_codemode::ErrorKind::Aborted => format!("Script aborted: {}", e.message),
            pi_codemode::ErrorKind::Sandbox => format!("Script sandbox failed: {}", e.message),
        };
        let summary = if calls.is_empty() {
            "No tool calls were made.".into()
        } else {
            format!(
                "Tool calls made before the failure (they are not undone): {}",
                calls
                    .iter()
                    .map(|c| format!(
                        "{} ({})",
                        c["name"].as_str().unwrap_or(""),
                        c["status"].as_str().unwrap_or("")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        items.push(text(format!("Script error:\n{head}\n\n{summary}")));
    }
    let mut items = join(items);
    let combined = items
        .iter()
        .filter_map(|i| {
            if let InputContent::Text(t) = i {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let units = combined.encode_utf16().count();
    let lines = combined.split('\n').count();
    let budget = start
        .max_output_tokens
        .saturating_mul(4)
        .min(4 * 1024 * 1024 / 4);
    let line_budget = (100_000 - 100) / 2;
    let mut full_path = None;
    if units > budget || lines > 2 * line_budget {
        let head = slice(&combined, budget / 2, false)
            .split('\n')
            .take(line_budget)
            .collect::<Vec<_>>()
            .join("\n");
        let tail = slice(&combined, budget - budget / 2, true)
            .split('\n')
            .rev()
            .take(line_budget)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        let removed = units - head.encode_utf16().count() - tail.encode_utf16().count();
        let mut cut = format!(
            "Warning: truncated output (original token count: {})\nTotal output lines: {lines}\n\n{head}…{} tokens truncated…{tail}",
            units.div_ceil(4),
            removed.div_ceil(4)
        );
        match write(api, "output.txt", combined.as_bytes()).await? {
            Ok(path) => {
                cut.push_str(&format!(
                    "\n\n[Full output: {path} (read with offset/limit)]"
                ));
                full_path = Some(path);
            }
            Err(error) => cut.push_str(&format!("\n\n[Could not save the full output: {error}]")),
        }
        items.retain(|i| matches!(i, InputContent::Image(_)));
        items.insert(0, text(cut));
    }
    let mut saved = BTreeMap::<String, String>::new();
    let mut presented = Vec::new();
    for item in items {
        if let InputContent::Image(image) = &item {
            let label = if let Some(label) = saved.get(&image.data) {
                label.clone()
            } else {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(&image.data)
                    .map_err(|e| Error::validation(format!("CodeMode image: {e}")))?;
                let extension = match image.mime_type.as_str() {
                    "image/png" => "png",
                    "image/jpeg" => "jpg",
                    "image/gif" => "gif",
                    "image/webp" => "webp",
                    _ => "bin",
                };
                let name = format!("image-{}.{extension}", saved.len() + 1);
                let kind = format!("{}, {} bytes", image.mime_type, bytes.len());
                let label = match write(api, &name, &bytes).await? {
                    Ok(path) => format!("[Image saved to {path} ({kind})]"),
                    Err(e) => format!("[Image ({kind}) could not be saved: {e}]"),
                };
                saved.insert(image.data.clone(), label.clone());
                label
            };
            presented.push(text(label));
        }
        presented.push(item);
    }
    let mut content = vec![text(format!(
        "Script {}\nWall time {:.1} seconds\nOutput:\n",
        if success { "completed" } else { "failed" },
        (api.now()? - start.started_at) / 1000.
    ))];
    content.extend(join(presented));
    let mut details = json!({"calls":calls});
    if let Some(path) = full_path {
        details["fullOutputPath"] = json!(path)
    }
    Ok(ToolExecutionResult {
        output: Some(content),
        is_error: Some(!success),
        details: Presence::Value(serde_json::from_value(details)?),
        ..Default::default()
    })
}
