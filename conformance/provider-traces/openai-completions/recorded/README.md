# Recorded OpenAI-compatible reasoning streams

Authority: response payloads observed from real servers, plus one stream generated from
vLLM's parser code. None of these files is produced by the pinned upstream oracle. The
assertions in `crates/pi-ai-providers/tests/openai_completions.rs` take their content,
tool calls and token counts from the recorded frames; event order, thinking signatures
and the usage mapping are the adapter's existing semantics applied to those values.
They pin how DeepSeek V4 reasoning arrives from two server families and which assistant
history field each family reads back.

| File | Source | Captured | Reasoning field | SHA-256 |
|---|---|---|---|---|
| `sse_deepseek_v4_thinking_with_tools.txt` | new-api gateway relaying DeepSeek's official API, request model `deepseek-v4-flash`, default thinking | 2026-09-13 | `reasoning_content` | `85a20ec86238d5c21a9238a335bb6577c4ad3a0eec64566bd0d4be84baab6314` |
| `sse_deepseek_v4_nonthinking_with_tools.txt` | same gateway and prompt, `reasoning_effort=none` | 2026-09-13 | none | `506b543752c7fa352a7ff9938b75efe6820c8a97f70e35b87edf195dd758ff1c` |
| `sse_deepseek_v4_nonthinking_puretool.txt` | as above, plus a system instruction forbidding prose | 2026-09-13 | none | `c9e72b0f89ec8e3960e2bf25b4a20b5b81828cd4a38d33b2a0d8dff1b86cb0c6` |
| `sse_vllm021_delta_reasoning.txt` | on-premises vLLM 0.21 serving DeepSeek V4 Flash (`system_fingerprint` `vllm-0.21.0-dp8-ep-231fc9d9`) | 2026-07-13 (frame `created`), archived 2026-07-19 | `reasoning` | `577aee9e1bd88885d16e1ffe74924e440b1e40f69ae03202c97c180a557f3ce0` |
| `synthetic_vllm021_tools_thinking.sse` | **synthetic**: generated with the vLLM v0.21.0 `DeepSeekV4ToolParser` and end-frame logic in the outer format of the vLLM recording | — | `reasoning` | `ec12a139db7d83c3107176477d86232738fa75f78132f3d7ec43d27bfcc90b1b` |

The three DeepSeek streams share one prompt and one tool definition. They differ in the
thinking setting, and the tool-only stream also adds a system instruction forbidding
prose. Each contains one `read_file {"path": "/etc/hosts"}` call delivered over 13
tool-call frames, and the response model is `deepseek-flash`. The gateway re-frames the
SSE stream but leaves each JSON payload unchanged. Only response bodies were kept;
request headers and credentials were never recorded. The vLLM stream has no tool call
and no usage frame, and the parameters of its request were not recorded, so it does not
show whether that server honours `stream_options.include_usage`. The synthetic stream
supplies the vLLM tool-call shape: each call arrives complete in one frame, content
frames carry `"tool_calls": []`, and usage arrives in a final `choices: []` chunk.

## Server contracts the tests rely on

vLLM v0.21.0 sources, verified at that tag:

- Assistant history is read from `reasoning` only; a request's `reasoning_content` is
  ignored ([chat_utils.py L1753-L1782](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/entrypoints/chat_utils.py#L1753-L1782)).
  In thinking mode the DeepSeek V4 encoder renders that reasoning
  ([deepseek_v4_encoding.py L350-L352](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/tokenizers/deepseek_v4_encoding.py#L350-L352)),
  for every assistant turn when tools are present
  ([L554-L557](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/tokenizers/deepseek_v4_encoding.py#L554-L557)).
- Thinking is enabled only by the `thinking` or `enable_thinking` chat-template argument
  ([deepseek_v4.py L32-L35](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/tokenizers/deepseek_v4.py#L32-L35)).
  A top-level `reasoning_effort` is merged into the template arguments when present
  ([protocol.py L460-L466](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/entrypoints/openai/chat_completion/protocol.py#L460-L466)),
  and other unknown top-level fields, including DeepSeek's `thinking` object, are ignored
  ([engine/protocol.py L27-L56](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/entrypoints/openai/engine/protocol.py#L27-L56)).
- The DeepSeek V4 encoder renders a `developer` message as a user turn
  ([deepseek_v4_encoding.py L278-L289](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/tokenizers/deepseek_v4_encoding.py#L278-L289))
  and, in thinking mode without tools, drops it before the last user message
  ([L589-L603](https://github.com/vllm-project/vllm/blob/v0.21.0/vllm/tokenizers/deepseek_v4_encoding.py#L589-L603)).

DeepSeek's official API returns reasoning as `reasoning_content` and requires that field
on assistant history; the catalog's DeepSeek compatibility profile already declares it.

## What the replays establish

Each stream is replayed as one body and again in 7-byte reads, which split
multi-byte UTF-8 in every stream except the ASCII-only tool-only recording; both
settle identically. The tests check the terminal content, thinking signature, tool call
identity and arguments, stop reason, response model and usage, plus the event order
with repeated deltas collapsed. A follow-up request built from the replayed message
must carry the reasoning text under the captured field: `reasoning` for vLLM,
`reasoning_content` for DeepSeek. For history, a DeepSeek profile in front of a vLLM
backend is harmless: it sends both keys, with `reasoning_content` empty, and vLLM reads
the populated one. That profile cannot turn vLLM's thinking on, because vLLM ignores the
`thinking` object; request thinking through the chat-template argument instead.

Boundaries: these are vLLM 0.21 and DeepSeek V4 observations; re-check the contracts
above before relying on another server version. A stream without a usage frame settles
with zero usage, matching the release adapter; no local estimate is made. vLLM 0.21 can
drop earlier calls and truncate the last one when several tool calls finish in the same
end frame; that server defect is outside these fixtures.

Run with
`cargo test -p pi-ai-providers --features wire-openai-completions,native --test openai_completions`.
