const AUTHORITY = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";
const ENVELOPE_KEYS = ["schemaVersion", "authority", "caseId", "input", "events", "messages", "outcome", "observations"];
const INPUT_KEYS = ["kind", "operation", "reference", "capability", "thinking_level", "active_tools", "resources"];
const OUTCOME_KEYS = ["kind", "error", "terminal"];
const ERROR_KEYS = ["type", "capability"];
const OBSERVATION_KEYS = [
  "provider_calls", "tool_invocations", "session_publication_events", "durable_entries",
  "model", "resources_attached", "settings_detached", "availability_transition",
  "credential_boundary", "event_count", "event_types", "terminal_kind",
  "runtime_model_match", "resources_match", "settings_match",
];
const MODEL_KEYS = ["provider", "id", "source", "available"];
const EVENT_KEYS = ["index", "type", "role", "message_id", "tool_call_id", "tool_name", "detail"];
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
    for (const key of ["provider", "id", "source"]) string(observations.model[key], `${path}.model.${key}`);
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
  for (const key of ["runtime_model_match", "resources_match", "settings_match"]) {
    if (observations[key] !== undefined) bool(observations[key], `${path}.${key}`);
  }
}

function validateEvents(events, path) {
  if (!Array.isArray(events)) throw new ConformanceMismatch(path, "expected array", "array", events);
  for (const [index, event] of events.entries()) {
    allowed(event, EVENT_KEYS, `${path}[${index}]`);
    integer(event.index, `${path}[${index}].index`); string(event.type, `${path}[${index}].type`);
    if (!EVENT_TYPES.has(event.type)) {
      throw new ConformanceMismatch(`${path}[${index}].type`, "unknown Harness event type", [...EVENT_TYPES], event.type);
    }
    for (const key of ["role", "message_id", "tool_call_id", "tool_name", "detail"]) if (event[key] !== undefined) string(event[key], `${path}[${index}].${key}`);
  }
}

function validateMessages(messages, path) {
  if (!Array.isArray(messages)) throw new ConformanceMismatch(path, "expected array", "array", messages);
  for (const [index, message] of messages.entries()) {
    allowed(message, MESSAGE_KEYS, `${path}[${index}]`);
    string(message.role, `${path}[${index}].role`);
    string(message.id, `${path}[${index}].id`);
    if (!Object.prototype.hasOwnProperty.call(message, "content")) {
      throw new ConformanceMismatch(`${path}[${index}].content`, "message content is required", "present", undefined);
    }
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
