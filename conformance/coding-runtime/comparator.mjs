const AUTHORITY = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";
const ENVELOPE_KEYS = ["schemaVersion", "authority", "caseId", "input", "events", "messages", "outcome", "observations"];
const INPUT_KEYS = ["kind", "operation", "reference", "capability", "thinking_level", "active_tools", "resources", "tool", "tools", "queue", "max_retries", "retry_enabled", "custom_instructions"];
const OUTCOME_KEYS = ["kind", "error", "terminal"];
const ERROR_KEYS = ["type", "capability"];
const OBSERVATION_KEYS = [
  "provider_calls", "tool_invocations", "session_publication_events", "durable_entries",
  "model", "resources_attached", "settings_detached", "availability_transition",
  "credential_boundary", "event_count", "event_types", "terminal_kind",
  "runtime_model_match", "resources_match", "settings_match", "session_stats_delta",
  "environment_calls", "tool_evidence", "tool_names", "files_after",
  "queued_result", "queue_order", "queued_item", "queue_snapshot", "provider_contexts", "cleanup",
  "retry_scheduled", "clock", "adapter_lifecycle", "provider_responses", "overflow_response", "summary_request",
  "abort_outcome", "active_snapshot", "active_streams", "compaction_entries", "summary_requests", "summary_responses", "continued_request", "session_entries_before", "session_entries_after", "before_compaction_leaf",
];
const MODEL_KEYS = ["provider", "id", "name", "base_url", "source", "available"];
const EVENT_KEYS = ["index", "type", "role", "message_id", "tool_call_id", "tool_name", "detail", "payload"];
const EVENT_TYPES = new Set([
  "run_start", "run_resume", "run_suspend", "run_abort", "run_end", "fault",
  "handler_error", "turn_start", "turn_end", "retry_scheduled", "retry_start",
  "retry_end", "message_start", "message_update", "message_end", "tool_start",
  "tool_update", "tool_end", "entry_added", "write_pending", "queue_update",
  "fact_update", "config_update", "compaction_start", "compaction_end",
  "navigation_start", "navigation_end", "lane_created", "usage",
]);
const MESSAGE_KEYS = ["role", "id", "content", "stopReason", "errorCategory", "errorMessage"];

export class ConformanceMismatch extends Error {
  constructor(path, message, expected, actual) {
    super(`${message} at ${path}; expected ${JSON.stringify(expected)}, actual ${JSON.stringify(actual)}`);
    this.path = path;
  }
}

function object(value) { return value !== null && typeof value === "object" && !Array.isArray(value); }
function allowed(value, keys, path) {
  if (!object(value)) throw new ConformanceMismatch(path, "expected object", "object", value);
  const allowedKeys = new Set(keys);
  for (const key of Object.keys(value)) {
    if (!allowedKeys.has(key)) throw new ConformanceMismatch(`${path}.${key}`, "unknown field", keys, value[key]);
  }
}
function string(value, path) { if (typeof value !== "string") throw new ConformanceMismatch(path, "expected string", "string", value); }
function bool(value, path) { if (typeof value !== "boolean") throw new ConformanceMismatch(path, "expected boolean", "boolean", value); }
function integer(value, path) { if (!Number.isInteger(value) || value < 0) throw new ConformanceMismatch(path, "expected non-negative integer", "integer", value); }
function exactKeys(value, keys, path) {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new ConformanceMismatch(`${path}.keys`, "object keys mismatch", expected, actual);
}

function validateInput(input, path) {
  allowed(input, INPUT_KEYS, path);
  string(input.kind, `${path}.kind`); string(input.operation, `${path}.operation`);
  if (input.reference !== undefined) string(input.reference, `${path}.reference`);
  if (input.capability !== undefined) string(input.capability, `${path}.capability`);
  if (input.thinking_level !== undefined) string(input.thinking_level, `${path}.thinking_level`);
  if (input.active_tools !== undefined) {
    if (!Array.isArray(input.active_tools) || input.active_tools.some((item) => typeof item !== "string")) throw new ConformanceMismatch(`${path}.active_tools`, "expected string array", "string[]", input.active_tools);
  }
  if (input.resources !== undefined) {
    allowed(input.resources, ["skills", "prompt_templates"], `${path}.resources`);
    for (const key of ["skills", "prompt_templates"]) {
      if (!Array.isArray(input.resources[key]) || input.resources[key].some((item) => typeof item !== "string")) throw new ConformanceMismatch(`${path}.resources.${key}`, "expected string array", "string[]", input.resources[key]);
    }
  }
  for (const key of ["tool", "queue", "custom_instructions"]) if (input[key] !== undefined) string(input[key], `${path}.${key}`);
  if (input.max_retries !== undefined) integer(input.max_retries, `${path}.max_retries`);
  if (input.retry_enabled !== undefined) bool(input.retry_enabled, `${path}.retry_enabled`);
  if (input.tools !== undefined && (!Array.isArray(input.tools) || input.tools.some((item) => typeof item !== "string"))) throw new ConformanceMismatch(`${path}.tools`, "expected string array", "string[]", input.tools);
}

function validateOutcome(outcome, path) {
  allowed(outcome, OUTCOME_KEYS, path);
  string(outcome.kind, `${path}.kind`); bool(outcome.terminal, `${path}.terminal`);
  if (outcome.error !== null) {
    allowed(outcome.error, ERROR_KEYS, `${path}.error`);
    string(outcome.error.type, `${path}.error.type`);
    if (outcome.error.capability !== undefined) string(outcome.error.capability, `${path}.error.capability`);
  }
}

function validateObservations(observations, path) {
  allowed(observations, OBSERVATION_KEYS, path);
  for (const key of ["provider_calls", "tool_invocations", "session_publication_events", "durable_entries"]) integer(observations[key], `${path}.${key}`);
  if (observations.model !== undefined) {
    allowed(observations.model, MODEL_KEYS, `${path}.model`);
    for (const key of ["provider", "id", "name", "base_url", "source"]) string(observations.model[key], `${path}.model.${key}`);
    bool(observations.model.available, `${path}.model.available`);
  }
  for (const key of ["resources_attached", "settings_detached"]) {
    if (observations[key] !== undefined) bool(observations[key], `${path}.${key}`);
  }
  if (observations.availability_transition !== undefined) {
    if (!Array.isArray(observations.availability_transition) || observations.availability_transition.some((item) => typeof item !== "string")) {
      throw new ConformanceMismatch(`${path}.availability_transition`, "expected string array", "string[]", observations.availability_transition);
    }
  }
  if (observations.credential_boundary !== undefined) string(observations.credential_boundary, `${path}.credential_boundary`);
  if (observations.event_count !== undefined) integer(observations.event_count, `${path}.event_count`);
  if (observations.event_types !== undefined) {
    if (!Array.isArray(observations.event_types) || observations.event_types.some((item) => typeof item !== "string")) {
      throw new ConformanceMismatch(`${path}.event_types`, "expected string array", "string[]", observations.event_types);
    }
  }
  if (observations.terminal_kind !== undefined) string(observations.terminal_kind, `${path}.terminal_kind`);
  for (const key of ["runtime_model_match", "resources_match", "settings_match"]) if (observations[key] !== undefined) bool(observations[key], `${path}.${key}`);
  if (observations.session_stats_delta !== undefined) { shape(observations.session_stats_delta, ["message_count", "usage_total"], `${path}.session_stats_delta`); integer(observations.session_stats_delta.message_count, `${path}.session_stats_delta.message_count`); integer(observations.session_stats_delta.usage_total, `${path}.session_stats_delta.usage_total`); }

}

function validateEvents(events, path) {
  if (!Array.isArray(events)) throw new ConformanceMismatch(path, "expected array", "array", events);
  for (const [index, event] of events.entries()) {
    allowed(event, EVENT_KEYS, `${path}[${index}]`);
    integer(event.index, `${path}[${index}].index`); string(event.type, `${path}[${index}].type`);
    if (!EVENT_TYPES.has(event.type)) {
      throw new ConformanceMismatch(`${path}[${index}].type`, "unknown Harness event type", [...EVENT_TYPES], event.type);
    }
    if (event.payload !== undefined) validatePayload(event.type, event.payload, `${path}[${index}].payload`);
    for (const key of ["role", "message_id", "tool_call_id", "tool_name", "detail"]) if (event[key] !== undefined) string(event[key], `${path}[${index}].${key}`);
  }
}

function validateMessages(messages, path) {
  if (!Array.isArray(messages)) throw new ConformanceMismatch(path, "expected array", "array", messages);
  for (const [index, message] of messages.entries()) {
    allowed(message, MESSAGE_KEYS, `${path}[${index}]`);
    string(message.role, `${path}[${index}].role`);
    requireFact(["user", "assistant", "toolResult"].includes(message.role), `${path}[${index}].role`, "unknown message role", message.role);
    string(message.id, `${path}[${index}].id`);
    if (!Object.prototype.hasOwnProperty.call(message, "content")) {
      throw new ConformanceMismatch(`${path}[${index}].content`, "message content is required", "present", undefined);
    }
    content(message.content, `${path}[${index}].content`, message.role === "assistant");
    for (const key of ["stopReason", "errorCategory", "errorMessage"]) {
      if (message[key] !== undefined && message[key] !== null) string(message[key], `${path}[${index}].${key}`);
    }
  }
}

export function validateObservation(value, expectedCaseId = undefined) {
  allowed(value, ENVELOPE_KEYS, "observation");
  exactKeys(value, ENVELOPE_KEYS, "observation");
  if (value.schemaVersion !== 1) throw new ConformanceMismatch("observation.schemaVersion", "unsupported schema version", 1, value.schemaVersion);
  if (value.authority !== AUTHORITY) throw new ConformanceMismatch("observation.authority", "authority mismatch", AUTHORITY, value.authority);
  string(value.caseId, "observation.caseId");
  if (expectedCaseId !== undefined && value.caseId !== expectedCaseId) throw new ConformanceMismatch("observation.caseId", "case ID mismatch", expectedCaseId, value.caseId);
  validateInput(value.input, "observation.input");
  validateEvents(value.events, "observation.events");
  validateMessages(value.messages, "observation.messages");
  validateOutcome(value.outcome, "observation.outcome");
  validateObservations(value.observations, "observation.observations");
  if (value.caseId === "capability.required_prestart_failure") {
    if (value.outcome.error?.type !== "MissingCapability" || value.outcome.error?.capability !== "models.resolve") throw new ConformanceMismatch("observation.outcome.error", "canonical capability error required", { type: "MissingCapability", capability: "models.resolve" }, value.outcome.error);
    for (const key of ["provider_calls", "tool_invocations", "session_publication_events", "durable_entries"]) if (value.observations[key] !== 0) throw new ConformanceMismatch(`observation.observations.${key}`, "pre-start failure must have zero side effects", 0, value.observations[key]);
  }
  validateOperation(value);
  return value;
}

function firstMismatch(expected, actual, path = "$") {
  if (Object.is(expected, actual)) return null;
  if (typeof expected !== typeof actual || expected === null || actual === null) return new ConformanceMismatch(path, "exact value mismatch", expected, actual);
  if (Array.isArray(expected) !== Array.isArray(actual)) return new ConformanceMismatch(path, "array/object mismatch", expected, actual);
  if (typeof expected !== "object") return new ConformanceMismatch(path, "exact value mismatch", expected, actual);
  if (Array.isArray(expected)) {
    if (expected.length !== actual.length) return new ConformanceMismatch(`${path}.length`, "array length mismatch", expected.length, actual.length);
    for (let i = 0; i < expected.length; i += 1) { const mismatch = firstMismatch(expected[i], actual[i], `${path}[${i}]`); if (mismatch) return mismatch; }
    return null;
  }
  const ek = Object.keys(expected).sort(), ak = Object.keys(actual).sort();
  if (JSON.stringify(ek) !== JSON.stringify(ak)) return new ConformanceMismatch(`${path}.keys`, "object keys mismatch", ek, ak);
  for (const key of ek) { const mismatch = firstMismatch(expected[key], actual[key], `${path}.${key}`); if (mismatch) return mismatch; }
  return null;
}

export function compareObservation(expected, actual) {
  validateObservation(expected, expected.caseId);
  validateObservation(actual, expected.caseId);
  const mismatch = firstMismatch(expected, actual);
  if (mismatch) throw mismatch;
}

export function firstFailure(expected, actual) {
  try { compareObservation(expected, actual); return null; } catch (error) { return error; }
}

const BASE_OPERATION_KEYS = ["provider_calls", "tool_invocations", "session_publication_events", "durable_entries", "event_count", "event_types", "terminal_kind", "session_stats_delta"];
const ROW_KEYS = {
  "tools.validate_progress_settle": ["tool_evidence", "environment_calls"],
  "tools.coding_operations": ["tool_evidence", "environment_calls", "tool_names", "files_after"],
  "queue.steer_during_turn": ["queued_result", "queue_order", "queued_item", "queue_snapshot", "provider_contexts", "cleanup"],
  "queue.follow_up_after_turn": ["queued_result", "queue_order", "queued_item", "queue_snapshot", "provider_contexts", "cleanup"],
  "retry.provider_recovery": ["retry_scheduled", "clock", "adapter_lifecycle", "provider_responses", "cleanup"],
  "retry.overflow_precedence": ["overflow_response", "retry_scheduled", "clock", "provider_contexts", "summary_request", "compaction_entries", "adapter_lifecycle"],
  "abort.active_stream_cleanup": ["abort_outcome", "active_snapshot", "adapter_lifecycle", "cleanup", "active_streams"],
  "compaction.context_projection": ["summary_requests", "summary_responses", "continued_request", "compaction_entries", "session_entries_before", "session_entries_after", "before_compaction_leaf"],
};
const ROW_INPUT = {
  "tools.validate_progress_settle": { operation: "validate_progress_settle", keys: ["tool"] },
  "tools.coding_operations": { operation: "coding_operations", keys: ["tools"] },
  "queue.steer_during_turn": { operation: "steer_during_turn", keys: ["queue"] },
  "queue.follow_up_after_turn": { operation: "follow_up_after_turn", keys: ["queue"] },
  "retry.provider_recovery": { operation: "provider_recovery", keys: ["max_retries"] },
  "retry.overflow_precedence": { operation: "overflow_precedence", keys: ["retry_enabled"] },
  "abort.active_stream_cleanup": { operation: "abort_active_stream", keys: [] },
  "compaction.context_projection": { operation: "context_projection", keys: ["custom_instructions"] },
};
function requireFact(condition, path, message, actual) {
  if (!condition) throw new ConformanceMismatch(path, message, "required runtime witness", actual);
}
function equalFact(left, right, path, message = "runtime witness mismatch") {
  requireFact(firstMismatch(left, right) === null, path, message, { left, right });
}
function array(value, path) { requireFact(Array.isArray(value), path, "expected array", value); }
function strings(value, path) { array(value, path); value.forEach((item, index) => string(item, `${path}[${index}]`)); }
function shape(value, keys, path) { allowed(value, keys, path); exactKeys(value, keys, path); }
function nullableString(value, path) { if (value !== null) string(value, path); }
function content(value, path, allowToolCalls = false) {
  if (typeof value === "string") return;
  array(value, path);
  for (const [index, block] of value.entries()) {
    allowed(block, ["type", "text", "id", "name", "arguments"], `${path}[${index}]`);
    if (block.type === "text") { shape(block, ["type", "text"], `${path}[${index}]`); string(block.text, `${path}[${index}].text`); }
    else if (block.type === "toolCall") { requireFact(allowToolCalls, `${path}[${index}].type`, "tool calls require assistant content", block.type); shape(block, ["type", "id", "name", "arguments"], `${path}[${index}]`); string(block.id, `${path}[${index}].id`); string(block.name, `${path}[${index}].name`); toolArguments(block.name, block.arguments, `${path}[${index}].arguments`); }
    else requireFact(false, `${path}[${index}].type`, "unknown content block", block.type);
  }
}
function projectedMessage(value, path) {
  allowed(value, ["role", "content", "stop_reason", "error_message", "tool_call_id", "tool_name", "is_error"], path);
  requireFact(["user", "assistant", "toolResult"].includes(value.role), `${path}.role`, "unknown projected message role", value.role);
  const keys = value.role === "user" ? ["role", "content"] : value.role === "assistant" ? ["role", "content", "stop_reason", ...(value.stop_reason === "error" || value.stop_reason === "aborted" ? ["error_message"] : [])] : ["role", "content", "tool_call_id", "tool_name", "is_error"];
  exactKeys(value, keys, path); content(value.content, `${path}.content`, value.role === "assistant");
  if (value.role === "assistant") { requireFact(["stop", "toolUse", "error", "aborted", "length"].includes(value.stop_reason), `${path}.stop_reason`, "unknown stop reason", value.stop_reason); if (value.error_message !== undefined) string(value.error_message, `${path}.error_message`); }
  if (value.role === "toolResult") { string(value.tool_call_id, `${path}.tool_call_id`); string(value.tool_name, `${path}.tool_name`); bool(value.is_error, `${path}.is_error`); }
}
function toolResult(value, path) {
  shape(value, ["content", "details"], path); content(value.content, `${path}.content`);
  if (value.details !== null) {
    shape(value.details, ["diff", "patch"], `${path}.details`);
    string(value.details.diff, `${path}.details.diff`); string(value.details.patch, `${path}.details.patch`);
  }
}
function toolCall(value, path) {
  shape(value, ["tool_call_id", "tool_name", "args"], path);
  string(value.tool_call_id, `${path}.tool_call_id`); string(value.tool_name, `${path}.tool_name`);
  toolArguments(value.tool_name, value.args, `${path}.args`);
}
function toolArguments(name, args, path) {
  const tools = { read: ["path"], write: ["path", "content"], edit: ["path", "edits"], grep: ["path", "pattern", "literal"], find: ["path", "pattern"], ls: ["path"], bash: ["command"] };
  requireFact(Object.hasOwn(tools, name), path, "unknown coding tool", name);
  shape(args, tools[name], path);
  for (const key of ["path", "content", "pattern", "command"]) if (args[key] !== undefined) string(args[key], `${path}.${key}`);
  if (args.literal !== undefined) bool(args.literal, `${path}.literal`);
  if (args.edits !== undefined) { array(args.edits, `${path}.edits`); args.edits.forEach((edit, index) => { shape(edit, ["oldText", "newText"], `${path}.edits[${index}]`); string(edit.oldText, `${path}.edits[${index}].oldText`); string(edit.newText, `${path}.edits[${index}].newText`); }); }
}
function toolUpdate(value, path) {
  shape(value, ["tool_call_id", "tool_name", "partial_result"], path);
  string(value.tool_call_id, `${path}.tool_call_id`); string(value.tool_name, `${path}.tool_name`); toolResult(value.partial_result, `${path}.partial_result`);
}
function toolEnd(value, path) {
  shape(value, ["tool_call_id", "tool_name", "result", "is_error", "terminate"], path);
  string(value.tool_call_id, `${path}.tool_call_id`); string(value.tool_name, `${path}.tool_name`); toolResult(value.result, `${path}.result`); bool(value.is_error, `${path}.is_error`); bool(value.terminate, `${path}.terminate`);
}
function queuedItems(value, path) {
  array(value, path); value.forEach((item, index) => { shape(item, ["entry_id", "message"], `${path}[${index}]`); string(item.entry_id, `${path}[${index}].entry_id`); projectedMessage(item.message, `${path}[${index}].message`); });
}
function queues(value, path) {
  shape(value, ["steer", "follow_up", "next_run"], path);
  for (const key of ["steer", "follow_up", "next_run"]) queuedItems(value[key], `${path}.${key}`);
}
function retryScheduled(value, path) {
  shape(value, ["step", "attempt", "max_attempts", "delay_ms", "error_message"], path);
  string(value.step, `${path}.step`); string(value.error_message, `${path}.error_message`);
  for (const key of ["attempt", "max_attempts", "delay_ms"]) integer(value[key], `${path}.${key}`);
}
function usage(value, path) {
  shape(value, ["input", "output", "cacheRead", "cacheWrite", "totalTokens", "cost"], path);
  for (const key of ["input", "output", "cacheRead", "cacheWrite", "totalTokens"]) integer(value[key], `${path}.${key}`);
  shape(value.cost, ["input", "output", "cacheRead", "cacheWrite", "total"], `${path}.cost`);
  for (const [key, cost] of Object.entries(value.cost)) requireFact(typeof cost === "number" && Number.isFinite(cost) && cost >= 0, `${path}.cost.${key}`, "invalid cost", cost);
}
function compactionEntry(value, path) {
  shape(value, ["id", "parent_id", "seq", "timestamp", "summary", "retained_tail", "tokens_before", "usage", "from_hook"], path);
  string(value.id, `${path}.id`); nullableString(value.parent_id, `${path}.parent_id`); string(value.summary, `${path}.summary`);
  for (const key of ["seq", "timestamp", "tokens_before"]) integer(value[key], `${path}.${key}`);
  bool(value.from_hook, `${path}.from_hook`); array(value.retained_tail, `${path}.retained_tail`); value.retained_tail.forEach((message, index) => projectedMessage(message, `${path}.retained_tail[${index}]`));
  if (value.usage !== null) usage(value.usage, `${path}.usage`);
}
function requestContext(value, path) {
  shape(value, ["system_prompt", "messages", "tools"], path); nullableString(value.system_prompt, `${path}.system_prompt`); strings(value.tools, `${path}.tools`); array(value.messages, `${path}.messages`); value.messages.forEach((message, index) => projectedMessage(message, `${path}.messages[${index}]`));
}
function laneSnapshot(value, path) {
  shape(value, ["operation", "steer", "follow_up", "next_run", "pending_writes", "faulted", "leaf_id"], path);
  if (value.operation !== null) { shape(value.operation, ["id", "kind", "status", "running_tools"], `${path}.operation`); string(value.operation.id, `${path}.operation.id`); requireFact(value.operation.kind === "run", `${path}.operation.kind`, "expected active run", value.operation.kind); requireFact(value.operation.status === "running", `${path}.operation.status`, "expected running operation", value.operation.status); integer(value.operation.running_tools, `${path}.operation.running_tools`); }
  for (const key of ["steer", "follow_up", "next_run"]) queuedItems(value[key], `${path}.${key}`);
  integer(value.pending_writes, `${path}.pending_writes`); bool(value.faulted, `${path}.faulted`); nullableString(value.leaf_id, `${path}.leaf_id`);
}
function cleanup(value, path) {
  shape(value, ["lane", "register_counts"], path); laneSnapshot(value.lane, `${path}.lane`);
  shape(value.register_counts, ["op.meta", "op.state", "op.tool_args", "op.preparation", "pending.entry"], `${path}.register_counts`);
  requireFact(value.lane.operation === null && !value.lane.faulted && value.lane.pending_writes === 0 && ["steer", "follow_up", "next_run"].every((key) => value.lane[key].length === 0), `${path}.lane`, "idle lane must have no operation, queue, or pending writes", value.lane);
  for (const [key, count] of Object.entries(value.register_counts)) requireFact(count === 0, `${path}.register_counts.${key}`, "cleanup must remove durable pending/operation registers", count);
}
function clock(value, path) {
  shape(value, ["before_ms", "after_ms", "sleeps_ms"], path); integer(value.before_ms, `${path}.before_ms`); integer(value.after_ms, `${path}.after_ms`); array(value.sleeps_ms, `${path}.sleeps_ms`); value.sleeps_ms.forEach((sleep, index) => integer(sleep, `${path}.sleeps_ms[${index}]`));
  equalFact(value.after_ms - value.before_ms, value.sleeps_ms.reduce((sum, delay) => sum + delay, 0), path, "clock advancement must equal observed sleeps");
}
function fileInfo(value, path) { shape(value, ["name", "path", "is_dir"], path); string(value.name, `${path}.name`); string(value.path, `${path}.path`); bool(value.is_dir, `${path}.is_dir`); }
function hostCalls(value, path) {
  array(value, path);
  for (const [index, call] of value.entries()) {
    const at = `${path}[${index}]`; shape(call, ["method", "args", "result", "error"], at); string(call.method, `${at}.method`); nullableString(call.error, `${at}.error`);
    const methods = { absolute_path: ["path"], canonical_path: ["path"], exists: ["path"], read_binary_file: ["path"], read_text_file: ["path"], write_file: ["path", "content"], file_info: ["path"], list_dir: ["path"], exec: ["shell", "command", "cwd", "timeout_ms", "env", "abort_signal"], create_temp_file: ["prefix", "content"] };
    requireFact(methods[call.method] !== undefined, `${at}.method`, "unknown environment method", call.method); shape(call.args, methods[call.method], `${at}.args`);
    for (const key of ["path", "content", "shell", "command", "prefix"]) if (call.args[key] !== undefined) string(call.args[key], `${at}.args.${key}`);
    if (call.error !== null) { requireFact(call.result === null, `${at}.result`, "failed environment call must have null result", call.result); continue; }
    if (["absolute_path", "canonical_path", "read_binary_file", "read_text_file", "create_temp_file"].includes(call.method)) string(call.result, `${at}.result`);
    if (call.method === "write_file") requireFact(call.result === null, `${at}.result`, "write result must be null", call.result);
    if (call.method === "exists") bool(call.result, `${at}.result`);
    if (call.method === "file_info") fileInfo(call.result, `${at}.result`);
    if (call.method === "list_dir") { array(call.result, `${at}.result`); call.result.forEach((file, i) => fileInfo(file, `${at}.result[${i}]`)); }
    if (call.method === "exec") { nullableString(call.args.cwd, `${at}.args.cwd`); if (call.args.timeout_ms !== null) integer(call.args.timeout_ms, `${at}.args.timeout_ms`); shape(call.args.env, ["PI_SESSION_ID", "PI_SESSION_FILE", "PI_PROVIDER", "PI_MODEL", "PI_REASONING_LEVEL"], `${at}.args.env`); for (const [key, value] of Object.entries(call.args.env)) string(value, `${at}.args.env.${key}`); bool(call.args.abort_signal, `${at}.args.abort_signal`); shape(call.result, ["stdout", "stderr", "merged", "exit_code"], `${at}.result`); string(call.result.stdout, `${at}.result.stdout`); string(call.result.stderr, `${at}.result.stderr`); nullableString(call.result.merged, `${at}.result.merged`); integer(call.result.exit_code, `${at}.result.exit_code`); }
  }
}
function validatePayload(type, payload, path) {
  if (type === "tool_start") return toolCall(payload, path);
  if (type === "tool_update") return toolUpdate(payload, path);
  if (type === "tool_end") return toolEnd(payload, path);
  if (type === "queue_update") return queues(payload, path);
  if (type === "retry_scheduled") return retryScheduled(payload, path);
  if (type === "message_end") { shape(payload, ["entry_id", "message"], path); nullableString(payload.entry_id, `${path}.entry_id`); projectedMessage(payload.message, `${path}.message`); return; }
  if (type === "compaction_start") { shape(payload, ["reason"], path); requireFact(["manual", "threshold", "overflow"].includes(payload.reason), `${path}.reason`, "unknown compaction reason", payload.reason); return; }
  if (type === "compaction_end") { shape(payload, ["reason", "outcome", "entry", "from_hook"], path); requireFact(["manual", "overflow"].includes(payload.reason) && payload.outcome === "completed", path, "completed compaction payload required", payload); compactionEntry(payload.entry, `${path}.entry`); bool(payload.from_hook, `${path}.from_hook`); equalFact(payload.from_hook, payload.entry.from_hook, path); return; }
  if (type === "run_abort") { shape(payload, ["run_id", "steer", "follow_up"], path); string(payload.run_id, `${path}.run_id`); for (const key of ["steer", "follow_up"]) { array(payload[key], `${path}.${key}`); payload[key].forEach((message, index) => projectedMessage(message, `${path}.${key}[${index}]`)); } return; }
  if (type === "run_end") { shape(payload, ["run_id", "outcome", "leaf_id"], path); string(payload.run_id, `${path}.run_id`); nullableString(payload.leaf_id, `${path}.leaf_id`); requireFact(["completed", "aborted"].includes(payload.outcome), `${path}.outcome`, "unknown terminal outcome", payload.outcome); return; }
  requireFact(false, path, "unexpected event payload", payload);
}
const PAYLOAD_EVENTS = new Set(["tool_start", "tool_update", "tool_end", "queue_update", "retry_scheduled", "message_end", "compaction_start", "compaction_end", "run_abort", "run_end"]);
function eventPayloads(value, type) { return value.events.filter((event) => event.type === type).map((event) => event.payload); }
function textResult(result) { return result.content.map((block) => block.text).join(""); }
function validateOperation(value) {
  const extra = ROW_KEYS[value.caseId]; if (!extra) return;
  const obs = value.observations, path = "observation.observations";
  const input = ROW_INPUT[value.caseId];
  exactKeys(value.input, ["kind", "operation", ...input.keys], "observation.input");
  equalFact(value.input.kind, "operation", "observation.input.kind");
  equalFact(value.input.operation, input.operation, "observation.input.operation");
  exactKeys(obs, [...BASE_OPERATION_KEYS, ...extra], path);
  for (const [index, event] of value.events.entries()) {
    requireFact(event.index === index, `observation.events[${index}].index`, "event indices must follow delivery order", event.index);
    exactKeys(event, ["index", "type", ...(PAYLOAD_EVENTS.has(event.type) ? ["payload"] : [])], `observation.events[${index}]`);
  }
  equalFact(obs.event_count, value.events.length, `${path}.event_count`);
  equalFact(obs.event_types, value.events.map((event) => event.type), `${path}.event_types`);
  equalFact(obs.tool_invocations, eventPayloads(value, "tool_start").length, `${path}.tool_invocations`);
  equalFact(obs.session_publication_events, value.events.filter((event) => event.type === "entry_added").length, `${path}.session_publication_events`);
  equalFact(obs.session_publication_events, obs.durable_entries, `${path}.durable_entries`);
  equalFact(obs.session_stats_delta.message_count, obs.durable_entries - (obs.compaction_entries?.length ?? 0), `${path}.session_stats_delta.message_count`);
  equalFact(obs.terminal_kind, value.outcome.kind, `${path}.terminal_kind`);
  requireFact(value.outcome.terminal && eventPayloads(value, "run_end").some((event) => event.outcome === obs.terminal_kind), "observation.outcome", "terminal outcome must come from RunEnd", value.outcome);
  if (obs.tool_evidence !== undefined) {
    if (value.caseId === "tools.validate_progress_settle") equalFact(value.input.tool, "read", "observation.input.tool");
    shape(obs.tool_evidence, ["provider_calls", "starts", "updates", "ends"], `${path}.tool_evidence`);
    for (const [key, validate] of [["provider_calls", toolCall], ["starts", toolCall], ["updates", toolUpdate], ["ends", toolEnd]]) { array(obs.tool_evidence[key], `${path}.tool_evidence.${key}`); obs.tool_evidence[key].forEach((entry, index) => validate(entry, `${path}.tool_evidence.${key}[${index}]`)); }
    equalFact(obs.tool_evidence.provider_calls, obs.tool_evidence.starts, `${path}.tool_evidence.provider_calls`);
    const messageCalls = eventPayloads(value, "message_end").filter((event) => event.message.role === "assistant" && Array.isArray(event.message.content)).flatMap((event) => event.message.content.filter((block) => block.type === "toolCall").map((block) => ({ tool_call_id: block.id, tool_name: block.name, args: block.arguments })));
    equalFact(messageCalls, obs.tool_evidence.provider_calls, "observation.events.message_end", "assistant MessageEnd calls must match observed provider calls and ToolStart");
    for (const [key, type] of [["starts", "tool_start"], ["updates", "tool_update"], ["ends", "tool_end"]]) equalFact(obs.tool_evidence[key], eventPayloads(value, type), `${path}.tool_evidence.${key}`);
    const required = value.caseId === "tools.coding_operations" ? ["read", "write", "edit", "grep", "find", "ls", "bash"] : ["read"];
    equalFact(obs.tool_evidence.starts.map((entry) => entry.tool_name), required, `${path}.tool_evidence.starts`, "all frozen tool bindings must execute in order");
    equalFact(obs.tool_evidence.ends.length, required.length, `${path}.tool_evidence.ends`);
    equalFact(obs.provider_calls, required.length + 1, `${path}.provider_calls`);
    for (const [index, start] of obs.tool_evidence.starts.entries()) {
      const end = obs.tool_evidence.ends[index]; requireFact(start.tool_name === end.tool_name && start.tool_call_id === end.tool_call_id && !end.is_error && !end.terminate && end.result.content.length > 0, `${path}.tool_evidence.ends[${index}]`, "tool settlement must match actual call and succeed", end);
      const startIndex = value.events.find((event) => event.type === "tool_start" && event.payload.tool_call_id === start.tool_call_id).index;
      const endIndex = value.events.find((event) => event.type === "tool_end" && event.payload.tool_call_id === start.tool_call_id).index;
      requireFact(startIndex < endIndex, `${path}.tool_evidence`, "start must precede settlement", { startIndex, endIndex });
    }
    for (const name of required.filter((name) => ["read", "bash"].includes(name))) {
      const end = obs.tool_evidence.ends.find((item) => item.tool_name === name), updates = obs.tool_evidence.updates.filter((item) => item.tool_name === name);
      requireFact(updates.length === 1 && updates[0].tool_call_id === end.tool_call_id, `${path}.tool_evidence.updates`, "actual progress payload required", updates);
      equalFact(textResult(updates[0].partial_result), name === "read" ? "" : textResult(end.result), `${path}.tool_evidence.updates`);
      const indices = ["tool_start", "tool_update", "tool_end"].map((type) => value.events.find((event) => event.type === type && event.payload.tool_call_id === end.tool_call_id).index);
      requireFact(indices[0] < indices[1] && indices[1] < indices[2], `${path}.tool_evidence.updates`, "progress must follow start and precede settlement", indices);
    }
    hostCalls(obs.environment_calls, `${path}.environment_calls`);
    const calls = obs.environment_calls, read = obs.tool_evidence.starts.find((item) => item.tool_name === "read");
    const readHost = calls.find((call) => call.method === "read_binary_file" && call.args.path === read.args.path && call.error === null);
    requireFact(readHost !== undefined, `${path}.environment_calls`, "read must reach binary host read", calls);
    equalFact(readHost.result, textResult(obs.tool_evidence.ends.find((item) => item.tool_name === "read").result), `${path}.environment_calls`, "host read must equal actual tool result");
    if (value.caseId === "tools.coding_operations") {
      strings(obs.tool_names, `${path}.tool_names`); equalFact(obs.tool_names, required, `${path}.tool_names`); equalFact(value.input.tools, required, "observation.input.tools");
      shape(obs.files_after, ["fixture.txt", "out.txt"], `${path}.files_after`); for (const [name, data] of Object.entries(obs.files_after)) string(data, `${path}.files_after.${name}`);
      for (const method of ["write_file", "read_text_file", "canonical_path", "file_info", "list_dir", "exec"]) requireFact(calls.some((call) => call.method === method), `${path}.environment_calls`, "required host method missing", method);
      const write = obs.tool_evidence.starts.find((item) => item.tool_name === "write"), edit = obs.tool_evidence.starts.find((item) => item.tool_name === "edit"), bash = obs.tool_evidence.starts.find((item) => item.tool_name === "bash"), grep = obs.tool_evidence.starts.find((item) => item.tool_name === "grep");
      const writes = calls.filter((call) => call.method === "write_file" && call.args.path === write.args.path);
      requireFact(writes.length === 2 && writes[0].args.content === write.args.content, `${path}.environment_calls`, "write arguments must reach host mutation", writes);
      const edited = edit.args.edits.reduce((body, change) => body.replace(change.oldText, change.newText), write.args.content);
      equalFact(writes[1].args.content, edited, `${path}.environment_calls`, "edit must produce observed second write"); equalFact(obs.files_after[edit.args.path], edited, `${path}.files_after`);
      const exec = calls.find((call) => call.method === "exec");
      // Generic SessionMetadataRecord supplies identity, with no host session file path.
      equalFact(exec.args.env, { PI_SESSION_ID: "m6-session", PI_SESSION_FILE: "", PI_PROVIDER: "openai", PI_MODEL: "offline-model", PI_REASONING_LEVEL: "off" }, `${path}.environment_calls.exec.env`, "shell must reach host with current Harness/session identity");
      equalFact(exec.args.command, bash.args.command, `${path}.environment_calls`); equalFact(exec.args.shell, bash.tool_name, `${path}.environment_calls`); equalFact(exec.result.stdout, textResult(obs.tool_evidence.ends.find((item) => item.tool_name === "bash").result), `${path}.environment_calls`, "process output must equal tool settlement");
      requireFact(textResult(obs.tool_evidence.ends.find((item) => item.tool_name === "grep").result).includes(grep.args.pattern) && calls.some((call) => call.method === "read_text_file" && call.result === edited), `${path}.tool_evidence`, "grep must observe edited file through host read", obs.tool_evidence);
      const listing = calls.find((call) => call.method === "list_dir").result.map((file) => file.name).sort().join("\n");
      equalFact(textResult(obs.tool_evidence.ends.find((item) => item.tool_name === "find").result), listing, `${path}.tool_evidence`); equalFact(textResult(obs.tool_evidence.ends.find((item) => item.tool_name === "ls").result), listing, `${path}.tool_evidence`);
    }
  }
  if (obs.queue_snapshot !== undefined) {
    laneSnapshot(obs.queue_snapshot, `${path}.queue_snapshot`); cleanup(obs.cleanup, `${path}.cleanup`); shape(obs.queued_result, ["entry_id"], `${path}.queued_result`); string(obs.queued_result.entry_id, `${path}.queued_result.entry_id`); queuedItems([obs.queued_item], `${path}.queued_item`);
    const placement = value.caseId === "queue.steer_during_turn" ? "steer" : "follow_up";
    equalFact(obs.queue_order, placement, `${path}.queue_order`); equalFact(value.input.queue, placement, "observation.input.queue");
    requireFact(obs.queue_snapshot.operation !== null, `${path}.queue_snapshot`, "admission snapshot must show active operation", obs.queue_snapshot);
    equalFact(obs.queue_snapshot[placement], [obs.queued_item], `${path}.queue_snapshot.${placement}`); equalFact(obs.queued_result.entry_id, obs.queued_item.entry_id, `${path}.queued_result`);
    for (const queue of ["steer", "follow_up", "next_run"].filter((key) => key !== placement)) equalFact(obs.queue_snapshot[queue], [], `${path}.queue_snapshot.${queue}`);
    const updates = value.events.filter((event) => event.type === "queue_update"); requireFact(updates.length === 2, "observation.events", "admission and drain QueueUpdate required", updates);
    equalFact(updates[0].payload[placement], [obs.queued_item], "observation.events.queue_update"); equalFact(updates[1].payload[placement], [], "observation.events.queue_update");
    const turns = value.events.filter((event) => event.type === "turn_end"); requireFact(turns.length === 2 && updates[0].index < turns[0].index && turns[0].index < updates[1].index && updates[1].index < turns[1].index, "observation.events", "queue admission/drain must bracket first turn settlement", value.events);
    array(obs.provider_contexts, `${path}.provider_contexts`); obs.provider_contexts.forEach((context, index) => requestContext(context, `${path}.provider_contexts[${index}]`)); equalFact(obs.provider_calls, obs.provider_contexts.length, `${path}.provider_calls`); equalFact(obs.provider_contexts.length, 2, `${path}.provider_contexts`);
    requireFact(!obs.provider_contexts[0].messages.some((message) => JSON.stringify(message) === JSON.stringify(obs.queued_item.message)) && obs.provider_contexts[1].messages.some((message) => JSON.stringify(message) === JSON.stringify(obs.queued_item.message)), `${path}.provider_contexts`, "queued message must appear in subsequent provider context", obs.provider_contexts);
  }
  if (obs.clock !== undefined) { clock(obs.clock, `${path}.clock`); array(obs.retry_scheduled, `${path}.retry_scheduled`); obs.retry_scheduled.forEach((entry, index) => retryScheduled(entry, `${path}.retry_scheduled[${index}]`)); equalFact(obs.retry_scheduled, eventPayloads(value, "retry_scheduled"), `${path}.retry_scheduled`); strings(obs.adapter_lifecycle, `${path}.adapter_lifecycle`); equalFact(obs.adapter_lifecycle.filter((event) => event.startsWith("dispatch:")).length, obs.provider_calls, `${path}.adapter_lifecycle`); }
  if (value.caseId === "retry.provider_recovery") {
    cleanup(obs.cleanup, `${path}.cleanup`); array(obs.provider_responses, `${path}.provider_responses`); obs.provider_responses.forEach((message, index) => projectedMessage(message, `${path}.provider_responses[${index}]`));
    equalFact(obs.provider_calls, 2, `${path}.provider_calls`); equalFact(obs.provider_responses.length, 2, `${path}.provider_responses`); equalFact(obs.retry_scheduled.length, 1, `${path}.retry_scheduled`);
    const scheduled = obs.retry_scheduled[0]; requireFact(scheduled.attempt === 2 && scheduled.max_attempts === value.input.max_retries + 1 && scheduled.delay_ms > 0 && obs.provider_responses[0].stop_reason === "error" && obs.provider_responses[1].stop_reason === "stop", `${path}.retry_scheduled`, "actual provider failure and retry attempt required", obs);
    equalFact(scheduled.error_message, obs.provider_responses[0].error_message, `${path}.retry_scheduled`); equalFact(obs.clock.sleeps_ms, [scheduled.delay_ms], `${path}.clock`);
    equalFact(eventPayloads(value, "retry_start"), [], "observation.events"); equalFact(eventPayloads(value, "retry_end"), [], "observation.events");
    equalFact(obs.adapter_lifecycle.filter((event) => event.startsWith("terminal:")), ["terminal:1:error", "terminal:2:stop"], `${path}.adapter_lifecycle`);
    const failure = value.events.find((event) => event.type === "message_end" && event.payload.message.stop_reason === "error"), recovery = value.events.find((event) => event.type === "message_end" && event.payload.message.stop_reason === "stop"), retry = value.events.find((event) => event.type === "retry_scheduled");
    requireFact(failure && recovery && retry && failure.index < retry.index && retry.index < recovery.index, "observation.events", "retry scheduling must follow failure and precede recovery", value.events);
  }
  if (value.caseId === "retry.overflow_precedence") {
    equalFact(value.input.retry_enabled, true, "observation.input.retry_enabled");
    projectedMessage(obs.overflow_response, `${path}.overflow_response`); requireFact(obs.overflow_response.stop_reason === "error" && obs.overflow_response.error_message.includes("exceeds the context window"), `${path}.overflow_response`, "observed overflow error required", obs.overflow_response);
    equalFact(obs.retry_scheduled, [], `${path}.retry_scheduled`); equalFact(obs.clock.sleeps_ms, [], `${path}.clock`); equalFact(obs.provider_calls, 3, `${path}.provider_calls`);
    array(obs.provider_contexts, `${path}.provider_contexts`); obs.provider_contexts.forEach((context, index) => requestContext(context, `${path}.provider_contexts[${index}]`)); equalFact(obs.provider_contexts.length, obs.provider_calls, `${path}.provider_contexts`); requestContext(obs.summary_request, `${path}.summary_request`); equalFact(obs.summary_request, obs.provider_contexts[1], `${path}.summary_request`);
    const errorEvent = value.events.find((event) => event.type === "message_end" && event.payload.message.stop_reason === "error"), start = value.events.find((event) => event.type === "compaction_start");
    requireFact(errorEvent && start && errorEvent.index < start.index && start.payload.reason === "overflow", "observation.events", "overflow event must trigger compaction before recovery", value.events); equalFact(errorEvent.payload.message, obs.overflow_response, `${path}.overflow_response`);
    requireFact(!value.events.some((event) => ["retry_scheduled", "retry_start", "retry_end"].includes(event.type)), "observation.events", "overflow must suppress ordinary retry path", value.events);
    requireFact(obs.provider_contexts[2].messages.some((message) => message.content === obs.compaction_entries[0].summary), `${path}.provider_contexts`, "overflow recovery request must use actual compaction summary", obs.provider_contexts[2]);
  }
  if (obs.compaction_entries !== undefined) {
    array(obs.compaction_entries, `${path}.compaction_entries`); equalFact(obs.compaction_entries.length, 1, `${path}.compaction_entries`); obs.compaction_entries.forEach((entry, index) => compactionEntry(entry, `${path}.compaction_entries[${index}]`));
    equalFact(obs.compaction_entries, eventPayloads(value, "compaction_end").map((event) => event.entry), `${path}.compaction_entries`, "durable compaction entries must match actual completed event");
    requireFact(obs.compaction_entries[0].summary.length > 0 && obs.compaction_entries[0].tokens_before > 0 && !obs.compaction_entries[0].from_hook && obs.compaction_entries[0].usage !== null, `${path}.compaction_entries`, "generated summary metadata required", obs.compaction_entries);
    const summaryRequests = obs.summary_requests ?? [obs.summary_request];
    summaryRequests.forEach((context, index) => { requestContext(context, `${path}.summary_requests[${index}]`); requireFact(context.tools.length === 0 && context.messages.length === 1 && context.messages[0].role === "user" && typeof context.messages[0].content === "string" && context.messages[0].content.includes("<conversation>"), `${path}.summary_requests`, "summary provider request must carry one conversation prompt without tools", context); });
  }
  if (value.caseId === "compaction.context_projection") {
    array(obs.summary_requests, `${path}.summary_requests`); equalFact(obs.summary_requests.length, 2, `${path}.summary_requests`); array(obs.summary_responses, `${path}.summary_responses`); equalFact(obs.summary_responses.length, 2, `${path}.summary_responses`); obs.summary_responses.forEach((message, index) => { projectedMessage(message, `${path}.summary_responses[${index}]`); requireFact(message.stop_reason === "stop", `${path}.summary_responses[${index}]`, "summary response must settle", message); });
    requireFact(obs.summary_requests[0].messages[0].content.includes(value.input.custom_instructions) && obs.summary_requests[0].messages[0].content.includes("history one") && obs.summary_requests[1].messages[0].content.includes("history two"), `${path}.summary_requests`, "summary request must project history and custom instructions", obs.summary_requests);
    const summary = obs.compaction_entries[0].summary; requireFact(obs.summary_responses.every((message) => summary.includes(message.content.map((block) => block.text).join(""))), `${path}.compaction_entries`, "durable summary must use actual provider summary responses", obs.compaction_entries);
    nullableString(obs.before_compaction_leaf, `${path}.before_compaction_leaf`); equalFact(obs.compaction_entries[0].parent_id, obs.before_compaction_leaf, `${path}.compaction_entries`); integer(obs.session_entries_before, `${path}.session_entries_before`); integer(obs.session_entries_after, `${path}.session_entries_after`); equalFact(obs.session_entries_after, obs.session_entries_before + 1, `${path}.session_entries_after`);
    requestContext(obs.continued_request, `${path}.continued_request`); equalFact(obs.continued_request.messages.slice(0, 1), [{ role: "user", content: summary }], `${path}.continued_request`); equalFact(obs.continued_request.messages.slice(1, 2), obs.compaction_entries[0].retained_tail, `${path}.continued_request`); equalFact(obs.provider_calls, 5, `${path}.provider_calls`);
  }
  if (value.caseId === "abort.active_stream_cleanup") {
    validatePayload("run_abort", obs.abort_outcome, `${path}.abort_outcome`); equalFact(eventPayloads(value, "run_abort"), [obs.abort_outcome], `${path}.abort_outcome`); laneSnapshot(obs.active_snapshot, `${path}.active_snapshot`); requireFact(obs.active_snapshot.operation !== null, `${path}.active_snapshot`, "abort must begin with active operation", obs.active_snapshot); equalFact(obs.active_snapshot.operation.id, obs.abort_outcome.run_id, `${path}.abort_outcome`);
    cleanup(obs.cleanup, `${path}.cleanup`); strings(obs.adapter_lifecycle, `${path}.adapter_lifecycle`); requireFact(obs.adapter_lifecycle.includes("cancelled:1") && !obs.adapter_lifecycle.some((event) => event.startsWith("terminal:")), `${path}.adapter_lifecycle`, "adapter must observe runtime cancellation without provider terminal", obs.adapter_lifecycle); equalFact(obs.active_streams, 0, `${path}.active_streams`); equalFact(obs.provider_calls, 1, `${path}.provider_calls`); equalFact(value.outcome.kind, "aborted", "observation.outcome.kind");
  }
}
