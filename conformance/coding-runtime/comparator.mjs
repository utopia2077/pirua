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

function validateObservations(observations, path, caseId) {
  // M6-C fields are valid only on their own closed lifecycle row. Keep the
  // previously accepted rows' shared observation language unchanged.
  const lifecycleKeys = LIFECYCLE_ROWS.has(caseId)
    ? ["fixture_boundary", "provider_requests", LIFECYCLE_FIELDS[caseId]]
    : [];
  allowed(observations, [...OBSERVATION_KEYS, ...lifecycleKeys], path);
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

function validateEvents(events, path, lifecycle = false, extension = false) {
  if (!Array.isArray(events)) throw new ConformanceMismatch(path, "expected array", "array", events);
  for (const [index, event] of events.entries()) {
    allowed(event, EVENT_KEYS, `${path}[${index}]`);
    integer(event.index, `${path}[${index}].index`); string(event.type, `${path}[${index}].type`);
    if (!EVENT_TYPES.has(event.type)) {
      throw new ConformanceMismatch(`${path}[${index}].type`, "unknown Harness event type", [...EVENT_TYPES], event.type);
    }
    if (event.payload !== undefined) (lifecycle ? lifecyclePayload : validatePayload)(event.type, event.payload, `${path}[${index}].payload`, extension);
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
  validateEvents(value.events, "observation.events", LIFECYCLE_ROWS.has(value.caseId), value.caseId === "extension.participation_ordering");
  validateMessages(value.messages, "observation.messages");
  validateOutcome(value.outcome, "observation.outcome");
  validateObservations(value.observations, "observation.observations", value.caseId);
  if (value.caseId === "capability.required_prestart_failure") {
    if (value.outcome.error?.type !== "MissingCapability" || value.outcome.error?.capability !== "models.resolve") throw new ConformanceMismatch("observation.outcome.error", "canonical capability error required", { type: "MissingCapability", capability: "models.resolve" }, value.outcome.error);
    for (const key of ["provider_calls", "tool_invocations", "session_publication_events", "durable_entries"]) if (value.observations[key] !== 0) throw new ConformanceMismatch(`observation.observations.${key}`, "pre-start failure must have zero side effects", 0, value.observations[key]);
  }
  validateFoundation(value);
  validateOperation(value);
  validateLifecycle(value);
  return value;
}

// These four assembled fixtures emit only the fields below. The *_match flags
// come from Harness readbacks; attached/detached/source fields describe the
// prepared fixture. Model metadata is the request-visible offline overlay.
// These are bounded operation facts, not general resource/settings policy.
const FOUNDATION_INPUTS = {
  "model.credentials_catalog": { kind: "assemble", operation: "resolve_overlay", reference: "openai:offline-model" },
  "prompt.system_resources": { kind: "assemble", operation: "system_resources", resources: { skills: ["visible"], prompt_templates: ["build"] } },
  "prompt.settings_snapshot": { kind: "assemble", operation: "settings_snapshot", thinking_level: "off", active_tools: ["read"] },
  "capability.required_prestart_failure": { kind: "assemble", operation: "preflight", capability: "models.resolve" },
};
const FOUNDATION_EFFECTS = ["provider_calls", "tool_invocations", "session_publication_events", "durable_entries"];
const FOUNDATION_MATCHES = ["runtime_model_match", "resources_match", "settings_match"];
const FOUNDATION_FIELDS = {
  "model.credentials_catalog": ["model", "availability_transition", "credential_boundary"],
  "prompt.system_resources": ["resources_attached"],
  "prompt.settings_snapshot": ["settings_detached"],
  "capability.required_prestart_failure": [],
};
function validateFoundation(value) {
  const input = FOUNDATION_INPUTS[value.caseId];
  if (!input) return;
  const obs = value.observations, path = "observation.observations";
  const prestart = value.caseId === "capability.required_prestart_failure";
  shape(value.input, Object.keys(input), "observation.input");
  if (value.input.resources !== undefined) {
    shape(value.input.resources, ["skills", "prompt_templates"], "observation.input.resources");
    strings(value.input.resources.skills, "observation.input.resources.skills");
    strings(value.input.resources.prompt_templates, "observation.input.resources.prompt_templates");
  }
  equalFact(value.input, input, "observation.input", "foundation input must match the assembled operation");
  shape(value.outcome, OUTCOME_KEYS, "observation.outcome");
  equalFact(value.events, [], "observation.events", "foundation bridge emits measured counters, not event payloads");
  equalFact(value.messages, [], "observation.messages", "foundation bridge emits no message projection");
  shape(obs, [...FOUNDATION_EFFECTS, ...(prestart ? [] : ["terminal_kind", ...FOUNDATION_MATCHES]), ...FOUNDATION_FIELDS[value.caseId]], path);
  if (prestart) {
    shape(value.outcome.error, ERROR_KEYS, "observation.outcome.error");
    equalFact(value.outcome, { kind: "error", error: { type: "MissingCapability", capability: "models.resolve" }, terminal: true }, "observation.outcome");
    for (const key of FOUNDATION_EFFECTS) equalFact(obs[key], 0, `${path}.${key}`, "pre-start failure must have measured zero effects");
    return;
  }
  equalFact(value.outcome, { kind: value.caseId === "model.credentials_catalog" ? "resolved" : "assembled", error: null, terminal: true }, "observation.outcome");
  equalFact(obs.terminal_kind, "completed", `${path}.terminal_kind`);
  for (const key of FOUNDATION_MATCHES) equalFact(obs[key], true, `${path}.${key}`, "positive operation must match its Harness readback");
  // The deterministic bridge sends one text prompt, receives one assistant,
  // and invokes no tool. Both entries are measured after real idle settlement.
  for (const [key, count] of Object.entries({ provider_calls: 1, tool_invocations: 0, session_publication_events: 2, durable_entries: 2 })) equalFact(obs[key], count, `${path}.${key}`, "prepared offline operation must retain its measured effect count");
  if (value.caseId === "model.credentials_catalog") {
    shape(obs.model, MODEL_KEYS, `${path}.model`);
    equalFact(obs.model.provider, "openai", `${path}.model.provider`);
    equalFact(obs.model.id, "offline-model", `${path}.model.id`);
    equalFact(obs.model.name, "offline-model-detached-overlay", `${path}.model.name`);
    equalFact(obs.model.base_url, "https://overlay.example.invalid/v1", `${path}.model.base_url`);
    equalFact(obs.model.source, "overlay", `${path}.model.source`);
    equalFact(obs.model.available, true, `${path}.model.available`);
    strings(obs.availability_transition, `${path}.availability_transition`);
    equalFact(obs.availability_transition, ["unavailable", "available"], `${path}.availability_transition`);
    equalFact(obs.credential_boundary, "host-owned credential inputs retained without reads", `${path}.credential_boundary`);
  }
  if (value.caseId === "prompt.system_resources") equalFact(obs.resources_attached, true, `${path}.resources_attached`);
  if (value.caseId === "prompt.settings_snapshot") equalFact(obs.settings_detached, true, `${path}.settings_detached`);
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

// M6-C uses its own closed payload projection, preserving all accepted M6-B checks.
const LIFECYCLE_ROWS = new Set(["branch.fork_atomic_publish", "restart.recover_session", "legacy_import.format3", "model_switch.next_run", "extension.participation_ordering", "lifecycle.builder_replace"]);
const LIFECYCLE_FIELDS = {"branch.fork_atomic_publish":"fork", "restart.recover_session":"restart", "legacy_import.format3":"legacy_import", "model_switch.next_run":"model_switch", "extension.participation_ordering":"extension", "lifecycle.builder_replace":"replacement"};
function lifecycleMessage(value, path) {
  allowed(value, ["role","content","stop_reason","error_message","tool_call_id","tool_name","is_error"],path);
  requireFact(["user","assistant","toolResult"].includes(value.role),path,"invalid lifecycle message role",value.role);
  exactKeys(value,value.role === "user" ? ["role","content"] : value.role === "assistant" ? ["role","content","stop_reason",...(["error","aborted"].includes(value.stop_reason)?["error_message"]:[])] : ["role","content","tool_call_id","tool_name","is_error"],path);
  if(typeof value.content === "string") string(value.content,`${path}.content`);
  else {
    array(value.content,`${path}.content`);
    value.content.forEach((block,index)=>{
      const at=`${path}.content[${index}]`;
      if(block.type === "text") {shape(block,["type","text",...(block.textSignature===undefined?[]:["textSignature"])],at);string(block.text,`${at}.text`);if(block.textSignature!==undefined) string(block.textSignature,`${at}.textSignature`);}
      else { requireFact(value.role==="assistant" && block.type==="toolCall",at,"only assistant echo tool calls are supported",block);shape(block,["type","id","name","arguments"],at);string(block.id,`${at}.id`);equalFact(block.name,"echo",`${at}.name`);shape(block.arguments,["text"],`${at}.arguments`);string(block.arguments.text,`${at}.arguments.text`); }
    });
  }
  if(value.role === "assistant") {requireFact(["stop","toolUse","aborted"].includes(value.stop_reason),path,"unsupported lifecycle assistant terminal",value.stop_reason);if(value.error_message!==undefined)string(value.error_message,`${path}.error_message`);}
  if(value.role === "toolResult") {string(value.tool_call_id,`${path}.tool_call_id`);equalFact(value.tool_name,"echo",`${path}.tool_name`);bool(value.is_error,`${path}.is_error`);}
}
function lifecycleContext(value,path) {
  shape(value,["system_prompt","messages","tools"],path);nullableString(value.system_prompt,`${path}.system_prompt`);strings(value.tools,`${path}.tools`);array(value.messages,`${path}.messages`);value.messages.forEach((message,index)=>lifecycleMessage(message,`${path}.messages[${index}]`));
}
function requestModel(value,path) {shape(value,["provider","id","name"],path);for(const [key,item] of Object.entries(value)) string(item,`${path}.${key}`);}
function lifecycleEntry(value,path) {
  requireFact(["message","custom"].includes(value.type),path,"unsupported lifecycle entry",value.type);
  shape(value,["type","id","parent_id","seq",...(value.type==="message"?["message"]:["custom_type","data"])],path);
  string(value.id,`${path}.id`);nullableString(value.parent_id,`${path}.parent_id`);integer(value.seq,`${path}.seq`);
  if(value.type==="message") lifecycleMessage(value.message,`${path}.message`);
  else {equalFact(value.custom_type,"note",`${path}.custom_type`);shape(value.data,["opaque"],`${path}.data`);equalFact(value.data.opaque,"old",`${path}.data.opaque`);}
}
function laneModel(value,path) {shape(value,["provider","modelId"],path);string(value.provider,`${path}.provider`);string(value.modelId,`${path}.modelId`);}
function lifecycleSnapshot(value,path,active=false) {
  shape(value,["session_id","parent_session_id","cwd","leaf_id","entries","lane_config","lane_state","message_count","usage_total"],path);
  string(value.session_id,`${path}.session_id`);for(const key of ["parent_session_id","cwd","leaf_id"]) nullableString(value[key],`${path}.${key}`);
  array(value.entries,`${path}.entries`);value.entries.forEach((entry,index)=>lifecycleEntry(entry,`${path}.entries[${index}]`));
  requireFact(new Set(value.entries.map(e=>e.id)).size===value.entries.length,path,"duplicate durable entry identity",value.entries);
  value.entries.forEach((entry,index)=> {equalFact(entry.parent_id,index===0?null:value.entries[index-1].id,`${path}.entries[${index}].parent_id`);if(index)requireFact(entry.seq>value.entries[index-1].seq,path,"entries must preserve publication sequence",value.entries);});
  equalFact(value.leaf_id,value.entries.at(-1)?.id??null,`${path}.leaf_id`);
  integer(value.message_count,`${path}.message_count`);integer(value.usage_total,`${path}.usage_total`);equalFact(value.message_count,value.entries.filter(e=>e.type==="message").length,`${path}.message_count`);
  if(value.lane_config!==null) {shape(value.lane_config,["model","thinkingLevel","activeToolNames"],`${path}.lane_config`);laneModel(value.lane_config.model,`${path}.lane_config.model`);equalFact(value.lane_config.thinkingLevel,"off",`${path}.lane_config.thinkingLevel`);strings(value.lane_config.activeToolNames,`${path}.lane_config.activeToolNames`);}
  if(value.lane_state!==null) {shape(value.lane_state,["currentOperationId","pendingNextRun"],`${path}.lane_state`);nullableString(value.lane_state.currentOperationId,`${path}.lane_state.currentOperationId`);equalFact(value.lane_state.pendingNextRun,[],`${path}.lane_state.pendingNextRun`);requireFact(active?value.lane_state.currentOperationId!==null:value.lane_state.currentOperationId===null,`${path}.lane_state.currentOperationId`,"lane operation state must reflect lifecycle phase",value.lane_state);}
}
function lifecycleToolResult(value,path) {
  shape(value,["content","details"],path);content(value.content,`${path}.content`);equalFact(value.details,{},`${path}.details`);
}
function lifecyclePayload(type,value,path,extension=false) {
  if(type==="entry_added") {shape(value,["entry"],path);return lifecycleEntry(value.entry,`${path}.entry`);}
  if(type==="message_end") {shape(value,["entry_id","message"],path);nullableString(value.entry_id,`${path}.entry_id`);return lifecycleMessage(value.message,`${path}.message`);}
  if(type==="config_update") {if(value.property==="tools"){shape(value,["property"],path);return;}shape(value,["property","value","previous"],path);if(value.property==="model"){laneModel(value.value,`${path}.value`);laneModel(value.previous,`${path}.previous`);}else{equalFact(value.property,"activeTools",`${path}.property`);strings(value.value,`${path}.value`);strings(value.previous,`${path}.previous`);}return;}
  if(["tool_start","tool_update","tool_end"].includes(type)) {
    const fields=type==="tool_start"?["tool_call_id","tool_name","args"]:type==="tool_update"?["tool_call_id","tool_name",...(extension?["lane","run_id","turn_id"]:[]),"partial_result"]:["tool_call_id","tool_name","result","is_error","terminate"];
    shape(value,fields,path);string(value.tool_call_id,`${path}.tool_call_id`);equalFact(value.tool_name,"echo",`${path}.tool_name`);
    if(type==="tool_start"){shape(value.args,["text"],`${path}.args`);string(value.args.text,`${path}.args.text`);}else {lifecycleToolResult(value.partial_result??value.result,`${path}.result`);if(type==="tool_update"&&extension)for(const key of ["lane","run_id","turn_id"])string(value[key],`${path}.${key}`);if(type==="tool_end"){equalFact(value.is_error,false,`${path}.is_error`);equalFact(value.terminate,false,`${path}.terminate`);}}return;
  }
  return validatePayload(type,value,path);
}
function validateLifecycle(value) {
  if(!LIFECYCLE_ROWS.has(value.caseId))return;
  const obs=value.observations,path="observation.observations",field=LIFECYCLE_FIELDS[value.caseId],data=obs[field];
  shape(value.input,["kind","operation"],"observation.input");equalFact(value.input.kind,"lifecycle","observation.input.kind");equalFact(value.input.operation,value.caseId.split(".")[1],"observation.input.operation");
  shape(obs,["provider_calls","tool_invocations","session_publication_events","durable_entries","event_count","event_types","terminal_kind","provider_requests","fixture_boundary",field],path);
  string(obs.fixture_boundary,`${path}.fixture_boundary`);requireFact(obs.fixture_boundary.length>20,path,"explicit host authority boundary required",obs.fixture_boundary);
  value.events.forEach((event,index)=>{equalFact(event.index,index,`observation.events[${index}].index`);shape(event,["index","type",...(["message_end","entry_added","run_end","run_abort","config_update","tool_start","tool_update","tool_end","queue_update"].includes(event.type)?["payload"]:[])],`observation.events[${index}]`);});
  equalFact(obs.event_count,value.events.length,`${path}.event_count`);equalFact(obs.event_types,value.events.map(e=>e.type),`${path}.event_types`);
  const publications=eventPayloads(value,"entry_added").map(e=>e.entry);
  equalFact(obs.durable_entries,publications.length,`${path}.durable_entries`);equalFact(obs.session_publication_events,publications.length,`${path}.session_publication_events`);
  equalFact(obs.tool_invocations,eventPayloads(value,"tool_start").length,`${path}.tool_invocations`);
  const endedMessages=eventPayloads(value,"message_end");
  equalFact(endedMessages.length,publications.length,"observation.events.message_end");
  endedMessages.forEach((event,index)=>{equalFact(event.entry_id,publications[index].id,`observation.events.message_end[${index}].entry_id`);equalFact(event.message,publications[index].message,`observation.events.message_end[${index}].message`);});
  for(const event of value.events.filter(e=>e.type==="run_end"))equalFact(event.payload.leaf_id,value.events.filter(e=>e.type==="entry_added"&&e.index<event.index).at(-1)?.payload.entry.id??null,"observation.events.run_end.leaf_id");

  equalFact(obs.terminal_kind,"completed",`${path}.terminal_kind`);equalFact(value.outcome,{kind:"completed",error:null,terminal:true},"observation.outcome");
  requireFact(eventPayloads(value,"run_end").some(e=>e.outcome==="completed"),"observation.events","real completed run required",value.events);
  array(obs.provider_requests,`${path}.provider_requests`);obs.provider_requests.forEach((request,index)=>{shape(request,["model","context"],`${path}.provider_requests[${index}]`);requestModel(request.model,`${path}.provider_requests[${index}].model`);lifecycleContext(request.context,`${path}.provider_requests[${index}].context`);});
  const checkSnapshots=(keys,activeKey)=>keys.forEach(key=>lifecycleSnapshot(data[key],`${path}.${field}.${key}`,key===activeKey));
  const continuation=(before,after,request)=>{equalFact(after.entries.slice(0,before.entries.length),before.entries,`${path}.${field}`);equalFact(after.entries.length,before.entries.length+2,`${path}.${field}`);equalFact(request.context.messages,after.entries.filter(e=>e.type==="message").slice(0,-1).map(e=>e.message),`${path}.provider_requests`,"continued provider context must project actual durable history");};
  if(field!=="extension")equalFact(obs.provider_calls,obs.provider_requests.length,`${path}.provider_calls`);
  if(field==="fork") {
    shape(data,["source","published","attached","continued","source_after_continue"],`${path}.fork`);checkSnapshots(Object.keys(data));
    equalFact(data.source,data.source_after_continue,`${path}.fork.source_after_continue`);equalFact(data.published,data.attached,`${path}.fork.attached`);
    equalFact(data.published.parent_session_id,data.source.session_id,`${path}.fork.published.parent_session_id`);requireFact(data.source.session_id!==data.published.session_id,`${path}.fork`,"fork destination needs a distinct session",data);
    for(const key of ["entries","leaf_id","lane_config","lane_state"])equalFact(data.published[key],data.source[key],`${path}.fork.published.${key}`);
    equalFact(obs.provider_calls,2,`${path}.provider_calls`);continuation(data.attached,data.continued,obs.provider_requests[1]);equalFact(publications,[...data.source.entries,...data.continued.entries.slice(data.published.entries.length)],`${path}.fork`);
  }
  if(field==="restart") {
    shape(data,["before_close","reopened","recovered","continued","new_backend"],`${path}.restart`);checkSnapshots(["before_close","reopened","recovered","continued"]);equalFact(data.new_backend,true,`${path}.restart.new_backend`);equalFact(data.before_close,data.reopened,`${path}.restart.reopened`);equalFact(data.reopened,data.recovered,`${path}.restart.recovered`);equalFact(obs.provider_calls,2,`${path}.provider_calls`);continuation(data.recovered,data.continued,obs.provider_requests[1]);equalFact(publications,data.continued.entries,`${path}.restart`);
  }
  if(field==="legacy_import") {
    shape(data,["source_version","source_preserved_before_write","opened","adopted","continued","persisted_version","provenance"],`${path}.legacy_import`);checkSnapshots(["opened","adopted","continued"]);equalFact(data.source_version,3,`${path}.legacy_import.source_version`);equalFact(data.persisted_version,4,`${path}.legacy_import.persisted_version`);equalFact(data.source_preserved_before_write,true,`${path}.legacy_import.source_preserved_before_write`);
    for(const key of ["entries","leaf_id","session_id","usage_total"])equalFact(data.opened[key],data.adopted[key],`${path}.legacy_import.adopted.${key}`);
    equalFact(data.opened.usage_total,99,`${path}.legacy_import.opened.usage_total`);equalFact(data.continued.usage_total,99,`${path}.legacy_import.continued.usage_total`);equalFact(data.opened.entries[1].message.content[0].textSignature,"keep",`${path}.legacy_import.opened.entries[1].message.content`);requireFact(data.opened.entries.length===2&&data.opened.entries.every(e=>e.id!=="old"&&e.id!=="assistant"),`${path}.legacy_import.opened`,"legacy identities must be reminted",data.opened);
    array(data.provenance,`${path}.legacy_import.provenance`);requireFact(data.provenance.length===1,`${path}.legacy_import.provenance`,"one legacy usage adoption provenance record required",data.provenance);
    shape(data.provenance[0],["source"],`${path}.legacy_import.provenance[0]`);equalFact(data.provenance[0].source,"v3-import",`${path}.legacy_import.provenance`);equalFact(obs.provider_calls,1,`${path}.provider_calls`);continuation(data.adopted,data.continued,obs.provider_requests[0]);equalFact(publications,data.continued.entries.slice(2),`${path}.legacy_import`);
  }
  if(field==="model_switch") {
    shape(data,["before","switched","continued","readback"],`${path}.model_switch`);checkSnapshots(["before","switched","continued"]);requestModel(data.readback,`${path}.model_switch.readback`);equalFact(data.before.entries,data.switched.entries,`${path}.model_switch.switched.entries`);
    equalFact(obs.provider_calls,2,`${path}.provider_calls`);equalFact(obs.provider_requests[0].model.name,"initial overlay",`${path}.provider_requests[0].model.name`);equalFact(obs.provider_requests[1].model,data.readback,`${path}.model_switch.readback`);
    equalFact(data.switched.lane_config.model,{provider:data.readback.provider,modelId:data.readback.id},`${path}.model_switch.switched.lane_config.model`);requireFact(data.before.lane_config.model.modelId!==data.readback.id,`${path}.model_switch`,"switch must change dispatched model identity",data);
    const config=eventPayloads(value,"config_update");equalFact(config,[{property:"model",previous:data.before.lane_config.model,value:data.switched.lane_config.model}],"observation.events.config_update");continuation(data.switched,data.continued,obs.provider_requests[1]);equalFact(publications,data.continued.entries,`${path}.model_switch`);
    const configIndex=value.events.find(e=>e.type==="config_update").index, runs=value.events.filter(e=>e.type==="run_start");requireFact(runs.length===2&&runs[0].index<configIndex&&configIndex<runs[1].index,"observation.events","configuration mutation must precede the next real run",value.events);
  }
  if(field==="replacement") {
    shape(data,["active_source","settled_source","continued_destination","source_claim_busy","source_claim_released","destination_claim_busy","adapter_lifecycle","active_streams","cleanup_register_counts"],`${path}.replacement`);checkSnapshots(["active_source","settled_source","continued_destination"],"active_source");for(const key of ["source_claim_busy","source_claim_released","destination_claim_busy"])equalFact(data[key],true,`${path}.replacement.${key}`);
    shape(data.cleanup_register_counts,["op.meta","op.state","op.tool_args","op.preparation","pending.entry"],`${path}.replacement.cleanup_register_counts`);for(const [key,count] of Object.entries(data.cleanup_register_counts))equalFact(count,0,`${path}.replacement.cleanup_register_counts.${key}`);
    equalFact(data.active_streams,0,`${path}.replacement.active_streams`);strings(data.adapter_lifecycle,`${path}.replacement.adapter_lifecycle`);requireFact(data.adapter_lifecycle.includes("cancelled:1")&&data.adapter_lifecycle.indexOf("cancelled:1")<data.adapter_lifecycle.indexOf("dispatch:2"),`${path}.replacement.adapter_lifecycle`,"old stream cancellation must finish before replacement dispatch",data.adapter_lifecycle);
    requireFact(data.active_source.session_id!==data.continued_destination.session_id,`${path}.replacement`,"replacement must attach destination",data);equalFact(obs.provider_calls,2,`${path}.provider_calls`);equalFact(publications,[...data.settled_source.entries,...data.continued_destination.entries],`${path}.replacement`);
    equalFact(eventPayloads(value,"run_end").map(e=>e.outcome),["aborted","completed"],"observation.events.run_end");equalFact(eventPayloads(value,"run_abort").length,1,"observation.events.run_abort");equalFact(obs.provider_requests[1].context.messages,data.continued_destination.entries.slice(0,-1).map(e=>e.message),`${path}.provider_requests[1]`);
  }
  if(field==="extension")validateExtension(value,data,path);
}
function validateExtension(value,data,path) {
  const obs=value.observations,at=`${path}.extension`;
  shape(data,["grant","registration_order","hook_count","catalog","peer_requests","progress_items","terminal_items","tool_invocations","max_frame_bytes","connection_before_close","connection_after_close","hooks_after_close","peer_closed","session","provider_terminal","progress_frames","credits","tool_event_identities"],at);
  shape(data.grant,["principal","generation","ready","frame_bytes","aggregate_decoded_bytes"],`${at}.grant`);equalFact(data.grant.frame_bytes,8192,`${at}.grant.frame_bytes`);equalFact(data.grant.aggregate_decoded_bytes,16384,`${at}.grant.aggregate_decoded_bytes`);equalFact(data.grant.principal,"peer-a",`${at}.grant.principal`);equalFact(data.grant.generation,7,`${at}.grant.generation`);equalFact(data.grant.ready,true,`${at}.grant.ready`);
  equalFact(data.registration_order,["provider","echo_tool","hook"],`${at}.registration_order`);equalFact(data.hook_count,1,`${at}.hook_count`);
  shape(data.catalog,["provider","id","name","no_auth"],`${at}.catalog`);for(const key of ["provider","id","name"])string(data.catalog[key],`${at}.catalog.${key}`);equalFact(data.catalog.no_auth,true,`${at}.catalog.no_auth`);
  shape(data.provider_terminal,["entry_id","provider","model","usage_total","message"],`${at}.provider_terminal`);
  equalFact(data.provider_terminal.entry_id,data.session.leaf_id,`${at}.provider_terminal.entry_id`);
  equalFact(data.provider_terminal.provider,data.catalog.provider,`${at}.provider_terminal.provider`);
  equalFact(data.provider_terminal.model,data.catalog.id,`${at}.provider_terminal.model`);
  equalFact(data.provider_terminal.usage_total,5,`${at}.provider_terminal.usage_total`);
  equalFact(data.provider_terminal.usage_total,data.session.usage_total,`${at}.session.usage_total`);
  lifecycleMessage(data.provider_terminal.message,`${at}.provider_terminal.message`);
  equalFact(data.provider_terminal.message,data.session.entries.at(-1).message,`${at}.provider_terminal.message`);
  lifecycleSnapshot(data.session,`${at}.session`);equalFact(data.session.lane_config.model,{provider:data.catalog.provider,modelId:data.catalog.id},`${at}.session.lane_config.model`);
  for(const key of ["progress_items","terminal_items","tool_invocations"])equalFact(data[key],1,`${at}.${key}`);
  integer(data.max_frame_bytes,`${at}.max_frame_bytes`);requireFact(data.max_frame_bytes>0&&data.max_frame_bytes<=8192,`${at}.max_frame_bytes`,"actual finite peer frames required",data.max_frame_bytes);
  equalFact(data.connection_before_close,true,`${at}.connection_before_close`);equalFact(data.connection_after_close,false,`${at}.connection_after_close`);equalFact(data.hooks_after_close,0,`${at}.hooks_after_close`);equalFact(data.peer_closed,true,`${at}.peer_closed`);
  array(data.peer_requests,`${at}.peer_requests`);equalFact(data.peer_requests.map(r=>r.method),["before_run","tool.invoke","before_run","provider.invoke"],`${at}.peer_requests`);
  data.peer_requests.forEach((request,index)=>{
    const req=`${at}.peer_requests[${index}]`;shape(request,["method","capability","request_id","input","frame_index"],req);integer(request.frame_index,`${req}.frame_index`);if(index)requireFact(request.frame_index>data.peer_requests[index-1].frame_index,req,"actual outbound request frames must preserve order",request);integer(request.request_id,`${req}.request_id`);requireFact(request.request_id>0,req,"real assigned peer request identity required",request);if(index)requireFact(request.request_id>data.peer_requests[index-1].request_id,req,"peer request order must follow assigned identity",request);
    shape(request.capability,["kind","name","principal","handler"],`${req}.capability`);equalFact(request.capability.principal,data.grant.principal,`${req}.capability.principal`);
    if(request.method==="before_run") {equalFact(request.capability,{kind:"hook",name:"before_run",principal:"peer-a",handler:"hook-before_run-handler"},`${req}.capability`);shape(request.input,["name","run_id","prompt"],`${req}.input`);equalFact(request.input.name,"before_run",`${req}.input.name`);string(request.input.run_id,`${req}.input.run_id`);array(request.input.prompt,`${req}.input.prompt`);request.input.prompt.forEach((message,i)=>lifecycleMessage(message,`${req}.input.prompt[${i}]`));requireFact(request.input.prompt.length===1,`${req}.input.prompt`,"real accepted hook prompt required",request.input.prompt);}
    if(request.method==="tool.invoke") {equalFact(request.capability,{kind:"tool",name:"echo",principal:"peer-a",handler:"echo-handler"},`${req}.capability`);shape(request.input,["text"],`${req}.input`);string(request.input.text,`${req}.input.text`);}
    if(request.method==="provider.invoke") {equalFact(request.capability,{kind:"provider",name:data.catalog.provider,principal:"peer-a",handler:`provider-${data.catalog.provider}-handler`},`${req}.capability`);shape(request.input,["model","context","auth_mode","encoded_bytes","decoded_bytes"],`${req}.input`);equalFact(request.input.encoded_bytes,1903,`${req}.input.encoded_bytes`);equalFact(request.input.decoded_bytes,9963,`${req}.input.decoded_bytes`);requireFact(request.input.encoded_bytes<=data.grant.frame_bytes&&request.input.decoded_bytes<=data.grant.aggregate_decoded_bytes,`${req}.input`,"real provider payload must fit its granted finite limits",request.input);requestModel(request.input.model,`${req}.input.model`);lifecycleContext(request.input.context,`${req}.input.context`);equalFact(request.input.auth_mode,"none",`${req}.input.auth_mode`);equalFact(request.input.model,{provider:data.catalog.provider,id:data.catalog.id,name:data.catalog.name},`${req}.input.model`);}
  });
  const starts=eventPayloads(value,"tool_start"),updates=eventPayloads(value,"tool_update"),ends=eventPayloads(value,"tool_end");equalFact(starts.length,1,"observation.events.tool_start");equalFact(updates.length,1,"observation.events.tool_update","real extension progress must publish through the Harness sink");equalFact(ends.length,1,"observation.events.tool_end");
  equalFact(starts[0].args,data.peer_requests[1].input,"observation.events.tool_start");equalFact(starts[0].tool_call_id,ends[0].tool_call_id,"observation.events.tool_end");
  equalFact(updates[0].tool_call_id,starts[0].tool_call_id,"observation.events.tool_update.tool_call_id");equalFact(updates[0].tool_name,starts[0].tool_name,"observation.events.tool_update.tool_name");equalFact(updates[0].tool_name,ends[0].tool_name,"observation.events.tool_end.tool_name");equalFact(updates[0].lane,"main","observation.events.tool_update.lane");equalFact(updates[0].run_id,data.peer_requests[0].input.run_id,"observation.events.tool_update.run_id");
  shape(data.tool_event_identities,["start","update","end"],`${at}.tool_event_identities`);for(const [phase,identity] of Object.entries(data.tool_event_identities)){shape(identity,["tool_call_id","tool_name","lane","run_id","turn_id"],`${at}.tool_event_identities.${phase}`);for(const [key,item] of Object.entries(identity)){string(item,`${at}.tool_event_identities.${phase}.${key}`);requireFact(item.length>0,`${at}.tool_event_identities.${phase}.${key}`,"real nonempty tool event identity required",item);}}
  equalFact(data.tool_event_identities.start,data.tool_event_identities.update,`${at}.tool_event_identities.update`);equalFact(data.tool_event_identities.start,data.tool_event_identities.end,`${at}.tool_event_identities.end`);equalFact(data.tool_event_identities.update,Object.fromEntries(["tool_call_id","tool_name","lane","run_id","turn_id"].map(key=>[key,updates[0][key]])),"observation.events.tool_update","actual event identities must match the serialized update");
  const call=data.session.entries.flatMap(entry=>entry.message?.role==="assistant"?entry.message.content.filter(block=>block.type==="toolCall"):[]);equalFact(call.length,1,`${at}.session`);equalFact(starts[0].tool_call_id,call[0].id,"observation.events.tool_start.tool_call_id");equalFact(starts[0].args,call[0].arguments,"observation.events.tool_start.args");
  equalFact(textResult(ends[0].result),data.peer_requests[1].input.text,"observation.events.tool_end");
  const toolIndices=["tool_start","tool_update","tool_end"].map(type=>value.events.find(e=>e.type===type).index);requireFact(toolIndices[0]<toolIndices[1]&&toolIndices[1]<toolIndices[2],"observation.events","actual extension start, progress and settlement must preserve order",toolIndices);
  array(data.progress_frames,`${at}.progress_frames`);equalFact(data.progress_frames.length,1,`${at}.progress_frames`);
  const progress=data.progress_frames[0];shape(progress,["request_id","sequence","body_bytes","payload"],`${at}.progress_frames[0]`);equalFact(progress.request_id,data.peer_requests[1].request_id,`${at}.progress_frames[0].request_id`);equalFact(progress.sequence,0,`${at}.progress_frames[0].sequence`);integer(progress.body_bytes,`${at}.progress_frames[0].body_bytes`);requireFact(progress.body_bytes>0&&progress.body_bytes<=8192,`${at}.progress_frames[0].body_bytes`,"real finite progress body required",progress.body_bytes);lifecycleToolResult(progress.payload,`${at}.progress_frames[0].payload`);equalFact(textResult(progress.payload),`progress: ${JSON.stringify(data.peer_requests[1].input.text)}`,`${at}.progress_frames[0].payload`);
  equalFact(updates[0].partial_result,progress.payload,"observation.events.tool_update.partial_result","published typed progress must retain the actual peer payload");
  array(data.credits,`${at}.credits`);data.credits.forEach((credit,index)=>{shape(credit,["frame_index","request_id","additional_bytes"],`${at}.credits[${index}]`);for(const [key,item] of Object.entries(credit))integer(item,`${at}.credits[${index}].${key}`);requireFact(credit.additional_bytes>0&&credit.additional_bytes<=4096,`${at}.credits[${index}]`,"actual bounded host credit required",credit);});
  requireFact(data.credits.some(credit=>credit.request_id===progress.request_id&&credit.additional_bytes===progress.body_bytes&&credit.frame_index>data.peer_requests[1].frame_index&&credit.frame_index<data.peer_requests[2].frame_index),`${at}.credits`,"runtime must return exact progress body credit before the next hook request",data.credits);
  equalFact(eventPayloads(value,"config_update")[0],{property:"tools"},"observation.events.config_update");
  equalFact([data.peer_requests[0].input.run_id,data.peer_requests[2].input.run_id],eventPayloads(value,"run_end").map(event=>event.run_id),`${at}.peer_requests`,"hook requests must retain actual operation identities");
  equalFact(obs.provider_requests.length,2,`${path}.provider_requests`);equalFact(obs.provider_calls,obs.provider_requests.length+data.peer_requests.filter(r=>r.method==="provider.invoke").length,`${path}.provider_calls`);
  equalFact(obs.provider_requests[0].context.messages,data.peer_requests[0].input.prompt,`${path}.provider_requests[0]`);
  equalFact(data.peer_requests[3].input.context.messages,data.session.entries.slice(0,-1).map(e=>e.message),`${at}.peer_requests[3]`);equalFact(data.peer_requests[2].input.prompt,data.session.entries.filter(e=>e.message.role==="user").slice(-1).map(e=>e.message),`${at}.peer_requests[2]`);
  equalFact(eventPayloads(value,"entry_added").map(e=>e.entry),data.session.entries,`${at}.session`);
  requireFact(data.session.entries.at(-1).message.content.some(block=>block.text==="remote completion"),`${at}.session`,"real authenticated provider terminal must publish",data.session);
}
