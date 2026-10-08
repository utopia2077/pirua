const AUTHORITY = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";

const ENVELOPE_KEYS = [
  "schemaVersion", "authority", "caseId", "input", "events", "messages",
  "outcome", "normalizers", "queue", "observations",
];
const INPUT_KEYS = [
  "kind", "messages", "context", "tools", "executionMode", "transform",
  "providerStopReason", "beforeHook", "queueMode", "abort", "providerFailure",
];
const MESSAGE_KEYS = [
  "role", "id", "content", "stopReason", "errorCategory", "toolCallId",
  "toolName", "isError", "terminate",
];
const OUTCOME_KEYS = [
  "kind", "stopReason", "errorCategory", "errorMessage", "terminal", "executed",
  "abortBoundary",
];
const OBSERVATION_KEYS = [
  "providerCalls", "transformBeforeConvert", "completionOrder", "sourceOrder",
  "completionMayDiffer", "toolExecuted", "immediateReason", "forcedSequential",
  "queueDeliveredAfterToolResults", "queueDeliveredByContinue", "lateProgressSuppressed",
  "newExecutionAfterAbort", "retryCount", "replayedInput", "userMessageEvents",
];
const QUEUE_KEYS = ["kind", "mode", "items"];
const QUEUE_ITEM_KEYS = ["sequence", "payloadDigest", "admittedAt", "deliveredAt"];
const EVENT_FIELDS = {
  agent_start: ["index", "type"],
  agent_end: ["index", "type"],
  turn_start: ["index", "type"],
  turn_end: ["index", "type", "toolResults"],
  message_start: ["index", "type", "role", "messageId", "toolCallId", "toolName"],
  message_end: ["index", "type", "role", "messageId", "toolCallId", "toolName", "stopReason", "errorCategory"],
  tool_execution_start: ["index", "type", "toolCallId", "toolName"],
  tool_execution_update: ["index", "type", "toolCallId", "toolName", "progress"],
  tool_execution_end: ["index", "type", "toolCallId", "toolName", "isError", "executed", "immediate", "terminate"],
  queue_admit: ["index", "type", "kind", "sequence", "payloadDigest"],
  queue_deliver: ["index", "type", "kind", "sequence", "deliverySequence", "payloadDigest"],
  abort_signal: ["index", "type", "boundary"],
  provider_error: ["index", "type", "category"],
};
const ACTUAL_OBSERVATION_KEYS = [
  "schemaVersion", "authority", "caseId", "input", "events", "messages", "outcome",
  "observations",
];
const ACTUAL_EVENT_FIELDS = {
  agent_start: ["index", "type"],
  agent_end: ["index", "type"],
  turn_start: ["index", "type"],
  turn_end: ["index", "type", "toolResults"],
  message_start: ["index", "type", "role", "messageId", "toolCallId", "toolName"],
  message_end: ["index", "type", "role", "messageId", "toolCallId", "toolName", "stopReason", "errorCategory"],
  tool_execution_start: ["index", "type", "toolCallId", "toolName"],
  tool_execution_update: ["index", "type", "toolCallId", "toolName", "progress"],
  tool_execution_end: ["index", "type", "toolCallId", "toolName", "isError", "executed", "immediate", "terminate"],
};
const ACTUAL_OUTCOME_KEYS = OUTCOME_KEYS;
const ACTUAL_EVENT_TYPES = new Set(Object.keys(ACTUAL_EVENT_FIELDS));
const NORMALIZER_KINDS = new Set(["timestamp", "generatedId"]);
const PROTECTED_PATH_PARTS = new Set([
  "type", "index", "toolCallId", "toolName", "kind", "stopReason", "errorCategory",
  "role", "completionOrder", "sourceOrder",
]);

export class TraceMismatch extends Error {
  constructor(path, message, expected, actual) {
    super(`${message} at ${path}; expected ${format(expected)}, actual ${format(actual)}`);
    this.name = "TraceMismatch";
    this.path = path;
    this.expected = expected;
    this.actual = actual;
  }
}

function format(value) {
  return JSON.stringify(value);
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function ownKeys(value) {
  return Object.keys(value).sort();
}

function assertObject(value, path) {
  if (!isObject(value)) throw new TraceMismatch(path, "expected object", "object", value);
}

function assertAllowedKeys(value, allowed, path) {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(value)) {
    if (!allowedSet.has(key)) {
      throw new TraceMismatch(`${path}.${key}`, "unknown field", "declared field", value[key]);
    }
  }
}

function assertString(value, path) {
  if (typeof value !== "string") throw new TraceMismatch(path, "expected string", "string", value);
}

function assertBoolean(value, path) {
  if (typeof value !== "boolean") throw new TraceMismatch(path, "expected boolean", "boolean", value);
}

function assertInteger(value, path) {
  if (!Number.isInteger(value)) throw new TraceMismatch(path, "expected integer", "integer", value);
}

function assertStringArray(value, path) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new TraceMismatch(path, "expected string array", "string[]", value);
  }
}

function validateMessage(message, path) {
  assertObject(message, path);
  assertAllowedKeys(message, MESSAGE_KEYS, path);
  if (!["user", "assistant", "toolResult"].includes(message.role)) {
    throw new TraceMismatch(`${path}.role`, "unknown message role", "user|assistant|toolResult", message.role);
  }
  assertString(message.role, `${path}.role`);
  if (message.id !== undefined) assertString(message.id, `${path}.id`);
  if (message.content !== undefined && typeof message.content !== "string") {
    throw new TraceMismatch(`${path}.content`, "expected string content", "string", message.content);
  }
  if (message.stopReason !== undefined) assertString(message.stopReason, `${path}.stopReason`);
  if (message.errorCategory !== undefined && message.errorCategory !== null) assertString(message.errorCategory, `${path}.errorCategory`);
  if (message.toolCallId !== undefined) assertString(message.toolCallId, `${path}.toolCallId`);
  if (message.toolName !== undefined) assertString(message.toolName, `${path}.toolName`);
  if (message.isError !== undefined) assertBoolean(message.isError, `${path}.isError`);
  if (message.terminate !== undefined) assertBoolean(message.terminate, `${path}.terminate`);
  if (message.role === "toolResult" && (typeof message.toolCallId !== "string" || typeof message.toolName !== "string")) {
    throw new TraceMismatch(path, "tool result requires toolCallId and toolName", "tool result identity", message);
  }
}

function validateInput(input, path) {
  assertObject(input, path);
  assertAllowedKeys(input, INPUT_KEYS, path);
  if (input.kind !== undefined && !["prompt", "continue"].includes(input.kind)) {
    throw new TraceMismatch(`${path}.kind`, "unknown input kind", "prompt|continue", input.kind);
  }
  if (input.messages !== undefined) {
    if (!Array.isArray(input.messages)) throw new TraceMismatch(`${path}.messages`, "expected array", "array", input.messages);
    input.messages.forEach((message, index) => validateMessage(message, `${path}.messages[${index}]`));
  }
  if (input.context !== undefined) {
    if (!Array.isArray(input.context)) throw new TraceMismatch(`${path}.context`, "expected array", "array", input.context);
    input.context.forEach((message, index) => validateMessage(message, `${path}.context[${index}]`));
  }
  if (input.tools !== undefined) {
    if (!Array.isArray(input.tools)) throw new TraceMismatch(`${path}.tools`, "expected array", "array", input.tools);
    input.tools.forEach((tool, index) => {
      assertObject(tool, `${path}.tools[${index}]`);
      assertAllowedKeys(tool, ["name", "executionMode"], `${path}.tools[${index}]`);
      assertString(tool.name, `${path}.tools[${index}].name`);
      if (tool.executionMode !== undefined && !["parallel", "sequential"].includes(tool.executionMode)) {
        throw new TraceMismatch(`${path}.tools[${index}].executionMode`, "unknown execution mode", "parallel|sequential", tool.executionMode);
      }
    });
  }
}

function validateEvent(event, path) {
  assertObject(event, path);
  if (typeof event.type !== "string" || !Object.prototype.hasOwnProperty.call(EVENT_FIELDS, event.type)) {
    throw new TraceMismatch(`${path}.type`, "unknown event type", Object.keys(EVENT_FIELDS), event.type);
  }
  assertAllowedKeys(event, EVENT_FIELDS[event.type], path);
  assertInteger(event.index, `${path}.index`);
  if (event.type === "turn_end") {
    if (!Array.isArray(event.toolResults)) throw new TraceMismatch(`${path}.toolResults`, "expected string array", "string[]", event.toolResults);
    assertStringArray(event.toolResults, `${path}.toolResults`);
  }
  for (const key of ["messageId", "toolCallId", "toolName", "boundary", "category", "progress", "immediate"]) {
    if (event[key] !== undefined) assertString(event[key], `${path}.${key}`);
  }
  for (const key of ["isError", "executed", "terminate"]) {
    if (event[key] !== undefined) assertBoolean(event[key], `${path}.${key}`);
  }
  if (event.role !== undefined && !["user", "assistant", "toolResult"].includes(event.role)) {
    throw new TraceMismatch(`${path}.role`, "unknown event message role", "user|assistant|toolResult", event.role);
  }
  if (event.kind !== undefined && !["steer", "followUp"].includes(event.kind)) {
    throw new TraceMismatch(`${path}.kind`, "unknown queue kind", "steer|followUp", event.kind);
  }
  for (const key of ["sequence", "deliverySequence"]) {
    if (event[key] !== undefined) assertInteger(event[key], `${path}.${key}`);
  }
  if (event.payloadDigest !== undefined) assertString(event.payloadDigest, `${path}.payloadDigest`);
  if (event.stopReason !== undefined) assertString(event.stopReason, `${path}.stopReason`);
  if (event.errorCategory !== undefined && event.errorCategory !== null) assertString(event.errorCategory, `${path}.errorCategory`);
}

function validateActualEvent(event, path) {
  assertObject(event, path);
  if (typeof event.type !== "string" || !ACTUAL_EVENT_TYPES.has(event.type)) {
    throw new TraceMismatch(
      `${path}.type`,
      "actual observation contains a non-AgentEvent type",
      [...ACTUAL_EVENT_TYPES],
      event.type,
    );
  }
  assertAllowedKeys(event, ACTUAL_EVENT_FIELDS[event.type], path);
  assertInteger(event.index, `${path}.index`);
  if (event.type === "turn_end") {
    if (!Array.isArray(event.toolResults)) {
      throw new TraceMismatch(`${path}.toolResults`, "expected string array", "string[]", event.toolResults);
    }
    assertStringArray(event.toolResults, `${path}.toolResults`);
  }
  for (const key of ["messageId", "toolCallId", "toolName", "progress", "immediate"]) {
    if (event[key] !== undefined) assertString(event[key], `${path}.${key}`);
  }
  for (const key of ["isError", "executed", "terminate"]) {
    if (event[key] !== undefined) assertBoolean(event[key], `${path}.${key}`);
  }
  if (event.role !== undefined && !["user", "assistant", "toolResult"].includes(event.role)) {
    throw new TraceMismatch(`${path}.role`, "unknown actual event message role", "user|assistant|toolResult", event.role);
  }
  if (event.stopReason !== undefined) assertString(event.stopReason, `${path}.stopReason`);
  if (event.errorCategory !== undefined && event.errorCategory !== null) {
    assertString(event.errorCategory, `${path}.errorCategory`);
  }
}

function validateQueue(queue, path) {
  assertObject(queue, path);
  assertAllowedKeys(queue, QUEUE_KEYS, path);
  if (!["steer", "followUp"].includes(queue.kind)) throw new TraceMismatch(`${path}.kind`, "unknown queue kind", "steer|followUp", queue.kind);
  if (!["all", "oldest"].includes(queue.mode)) throw new TraceMismatch(`${path}.mode`, "unknown queue mode", "all|oldest", queue.mode);
  if (!Array.isArray(queue.items)) throw new TraceMismatch(`${path}.items`, "expected array", "array", queue.items);
  queue.items.forEach((item, index) => {
    assertObject(item, `${path}.items[${index}]`);
    assertAllowedKeys(item, QUEUE_ITEM_KEYS, `${path}.items[${index}]`);
    assertInteger(item.sequence, `${path}.items[${index}].sequence`);
    assertString(item.payloadDigest, `${path}.items[${index}].payloadDigest`);
    assertInteger(item.admittedAt, `${path}.items[${index}].admittedAt`);
    assertInteger(item.deliveredAt, `${path}.items[${index}].deliveredAt`);
  });
}

function validateNormalizers(normalizers, path) {
  if (!Array.isArray(normalizers)) throw new TraceMismatch(path, "expected array", "array", normalizers);
  for (const [index, normalizer] of normalizers.entries()) {
    const p = `${path}[${index}]`;
    assertObject(normalizer, p);
    assertAllowedKeys(normalizer, ["path", "kind"], p);
    if (!Array.isArray(normalizer.path) || normalizer.path.some((part) => typeof part !== "string")) {
      throw new TraceMismatch(`${p}.path`, "expected string path array", "string[]", normalizer.path);
    }
    if (!NORMALIZER_KINDS.has(normalizer.kind)) throw new TraceMismatch(`${p}.kind`, "unknown normalizer", [...NORMALIZER_KINDS], normalizer.kind);
    for (const part of normalizer.path) {
      if (PROTECTED_PATH_PARTS.has(part)) throw new TraceMismatch(`${p}.path`, "protected field cannot be normalized", "unprotected path", normalizer.path);
    }
    const last = normalizer.path.at(-1);
    if (normalizer.kind === "timestamp" && last !== "timestamp") throw new TraceMismatch(`${p}.path`, "timestamp normalizer must target timestamp", "...timestamp", normalizer.path);
    if (normalizer.kind === "generatedId" && !/id$/i.test(last ?? "")) throw new TraceMismatch(`${p}.path`, "generatedId normalizer must target an id field", "...id", normalizer.path);
  }
}

export function validateTrace(trace, expectedCaseId = undefined) {
  assertObject(trace, "trace");
  assertAllowedKeys(trace, ENVELOPE_KEYS, "trace");
  if (trace.schemaVersion !== 1) throw new TraceMismatch("trace.schemaVersion", "unsupported schema version", 1, trace.schemaVersion);
  if (trace.authority !== AUTHORITY) throw new TraceMismatch("trace.authority", "authority mismatch", AUTHORITY, trace.authority);
  assertString(trace.caseId, "trace.caseId");
  if (expectedCaseId !== undefined && trace.caseId !== expectedCaseId) throw new TraceMismatch("trace.caseId", "case ID mismatch", expectedCaseId, trace.caseId);
  validateInput(trace.input, "trace.input");
  if (!Array.isArray(trace.events)) throw new TraceMismatch("trace.events", "expected array", "array", trace.events);
  trace.events.forEach((event, index) => validateEvent(event, `trace.events[${index}]`));
  if (!Array.isArray(trace.messages)) throw new TraceMismatch("trace.messages", "expected array", "array", trace.messages);
  trace.messages.forEach((message, index) => validateMessage(message, `trace.messages[${index}]`));
  assertObject(trace.outcome, "trace.outcome");
  assertAllowedKeys(trace.outcome, OUTCOME_KEYS, "trace.outcome");
  for (const key of ["kind", "stopReason", "errorCategory", "errorMessage", "abortBoundary"]) {
    if (trace.outcome[key] !== undefined && trace.outcome[key] !== null) assertString(trace.outcome[key], `trace.outcome.${key}`);
  }
  if (trace.outcome.terminal !== undefined) assertBoolean(trace.outcome.terminal, "trace.outcome.terminal");
  if (trace.outcome.executed !== undefined) assertBoolean(trace.outcome.executed, "trace.outcome.executed");
  validateNormalizers(trace.normalizers, "trace.normalizers");
  if (trace.queue !== undefined) validateQueue(trace.queue, "trace.queue");
  if (trace.observations !== undefined) {
    assertObject(trace.observations, "trace.observations");
    assertAllowedKeys(trace.observations, OBSERVATION_KEYS, "trace.observations");
    for (const key of ["completionOrder", "sourceOrder"]) {
      if (trace.observations[key] !== undefined) assertStringArray(trace.observations[key], `trace.observations.${key}`);
    }
  }
  return trace;
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function setAtPath(value, path, replacement) {
  let cursor = value;
  for (let index = 0; index < path.length - 1; index += 1) {
    if (cursor === null || cursor === undefined) throw new TraceMismatch(`$.${path.join(".")}`, "normalizer path missing", "existing path", cursor);
    cursor = cursor[path[index]];
  }
  const leaf = path.at(-1);
  if (cursor === null || cursor === undefined || !(leaf in cursor)) {
    throw new TraceMismatch(`$.${path.join(".")}`, "normalizer path missing", "existing path", cursor);
  }
  cursor[leaf] = replacement;
}

function getAtPath(value, path) {
  let cursor = value;
  for (const part of path) {
    if (cursor === null || cursor === undefined || !(part in cursor)) return undefined;
    cursor = cursor[part];
  }
  return cursor;
}

export function normalizedClone(trace) {
  const result = clone(trace);
  for (const normalizer of trace.normalizers) {
    const current = getAtPath(result, normalizer.path);
    if (current === undefined) throw new TraceMismatch(`$.${normalizer.path.join(".")}`, "normalizer path missing", "existing path", current);
    setAtPath(result, normalizer.path, normalizer.kind === "timestamp" ? 0 : "<generated-id>");
  }
  return result;
}

function firstDeepMismatch(expected, actual, path = "$") {
  if (Object.is(expected, actual)) return null;
  if (typeof expected !== typeof actual || expected === null || actual === null) {
    return new TraceMismatch(path, "exact value mismatch", expected, actual);
  }
  if (Array.isArray(expected) !== Array.isArray(actual)) return new TraceMismatch(path, "array/object mismatch", expected, actual);
  if (typeof expected !== "object") return new TraceMismatch(path, "exact value mismatch", expected, actual);
  if (Array.isArray(expected)) {
    if (expected.length !== actual.length) return new TraceMismatch(`${path}.length`, "array length mismatch", expected.length, actual.length);
    for (let index = 0; index < expected.length; index += 1) {
      const mismatch = firstDeepMismatch(expected[index], actual[index], `${path}[${index}]`);
      if (mismatch) return mismatch;
    }
    return null;
  }
  const expectedKeys = ownKeys(expected);
  const actualKeys = ownKeys(actual);
  if (format(expectedKeys) !== format(actualKeys)) return new TraceMismatch(`${path}.keys`, "object keys mismatch", expectedKeys, actualKeys);
  for (const key of expectedKeys) {
    const mismatch = firstDeepMismatch(expected[key], actual[key], `${path}.${key}`);
    if (mismatch) return mismatch;
  }
  return null;
}

function lifecycleInvariant(trace) {
  const events = trace.events;
  if (events.length === 0) throw new TraceMismatch("$.events", "empty lifecycle", "at least one event", events);
  if (events[0].type !== "agent_start") throw new TraceMismatch("$.events[0].type", "lifecycle must start with agent_start", "agent_start", events[0].type);
  if (events.at(-1).type !== "agent_end") throw new TraceMismatch(`$.events[${events.length - 1}].type`, "lifecycle must end with agent_end", "agent_end", events.at(-1).type);
  for (const [index, event] of events.entries()) {
    if (event.index !== index) throw new TraceMismatch(`$.events[${index}].index`, "event index must match array position", index, event.index);
  }
  const firstTurn = events.findIndex((event) => event.type === "turn_start");
  const lastTurnEnd = events.findLastIndex((event) => event.type === "turn_end");
  if (firstTurn < 0 || lastTurnEnd < firstTurn) throw new TraceMismatch("$.events", "lifecycle requires turn start/end", "turn_start ... turn_end", events);
  const firstAssistantStart = events.findIndex((event) => event.type === "message_start" && event.role === "assistant");
  const firstAssistantEnd = events.findIndex((event) => event.type === "message_end" && event.role === "assistant");
  if (firstAssistantStart >= 0 && firstAssistantEnd < firstAssistantStart) throw new TraceMismatch("$.events", "assistant message pairing order", "start before end", events);
}

function messagePairInvariant(trace) {
  const open = new Map();
  for (const event of trace.events) {
    if (event.type === "message_start") {
      const key = event.role === "toolResult" ? `tool:${event.toolCallId}` : `${event.role}:${event.messageId}`;
      if (open.has(key)) throw new TraceMismatch(`$.events[${event.index}]`, "duplicate message start", "one start", event);
      open.set(key, event);
    } else if (event.type === "message_end") {
      const key = event.role === "toolResult" ? `tool:${event.toolCallId}` : `${event.role}:${event.messageId}`;
      const start = open.get(key);
      if (!start) throw new TraceMismatch(`$.events[${event.index}]`, "message end without matching start", "matching start", event);
      for (const field of ["role", "messageId", "toolCallId", "toolName"]) {
        if (start[field] !== undefined && event[field] !== start[field]) {
          throw new TraceMismatch(`$.events[${event.index}].${field}`, "message start/end identity mismatch", start[field], event[field]);
        }
      }
      open.delete(key);
    }
  }
  if (open.size) throw new TraceMismatch("$.events", "unclosed message", "all messages paired", [...open.keys()]);
}

function toolPairInvariant(trace) {
  const starts = new Map();
  const ends = new Map();
  const updates = new Map();
  for (const event of trace.events) {
    if (event.type === "tool_execution_start") {
      if (starts.has(event.toolCallId)) throw new TraceMismatch(`$.events[${event.index}]`, "duplicate tool start", "one start", event.toolCallId);
      starts.set(event.toolCallId, event);
    } else if (event.type === "tool_execution_update") {
      if (!starts.has(event.toolCallId) || ends.has(event.toolCallId)) throw new TraceMismatch(`$.events[${event.index}]`, "tool update outside start/end pair", "between pair", event.toolCallId);
      updates.set(event.toolCallId, (updates.get(event.toolCallId) ?? 0) + 1);
    } else if (event.type === "tool_execution_end") {
      const start = starts.get(event.toolCallId);
      if (!start) throw new TraceMismatch(`$.events[${event.index}]`, "tool end without start", "matching start", event.toolCallId);
      if (ends.has(event.toolCallId)) throw new TraceMismatch(`$.events[${event.index}]`, "duplicate tool end", "one end", event.toolCallId);
      if (event.toolName !== start.toolName) throw new TraceMismatch(`$.events[${event.index}].toolName`, "tool name mismatch", start.toolName, event.toolName);
      ends.set(event.toolCallId, event);
    }
  }
  for (const id of starts.keys()) if (!ends.has(id)) throw new TraceMismatch("$.events", "tool start without end", "paired tool end", id);
  const resultIds = trace.events.filter((event) => event.type === "message_end" && event.role === "toolResult").map((event) => event.toolCallId);
  const endIds = [...ends.keys()];
  if (resultIds.length !== endIds.length || new Set(resultIds).size !== resultIds.length) {
    throw new TraceMismatch("$.events", "tool result pairing mismatch", endIds, resultIds);
  }
  for (const id of endIds) if (!resultIds.includes(id)) throw new TraceMismatch("$.events", "tool result missing for tool end", id, resultIds);
  const turnResultIds = trace.events
    .filter((event) => event.type === "turn_end")
    .flatMap((event) => event.toolResults);
  if (JSON.stringify(turnResultIds) !== JSON.stringify(resultIds)) {
    throw new TraceMismatch("$.events.turn_end.toolResults", "turn-end source order mismatch", resultIds, turnResultIds);
  }
  return { completionOrder: [...ends.entries()].sort((a, b) => a[1].index - b[1].index).map(([id]) => id), sourceOrder: resultIds };
}

function queueInvariant(trace) {
  if (trace.queue === undefined) return;
  const admissions = new Map();
  const deliveries = new Map();
  for (const event of trace.events) {
    if (event.type === "queue_admit") admissions.set(event.sequence, event);
    if (event.type === "queue_deliver") deliveries.set(event.sequence, event);
  }
  for (const item of trace.queue.items) {
    const admit = admissions.get(item.sequence);
    const deliver = deliveries.get(item.sequence);
    if (!admit || !deliver) throw new TraceMismatch("$.queue.items", "queue item lacks admission/delivery event", item, { admit, deliver });
    if (admit.kind !== trace.queue.kind || deliver.kind !== trace.queue.kind) throw new TraceMismatch("$.queue.kind", "queue event kind mismatch", trace.queue.kind, { admit, deliver });
    if (admit.payloadDigest !== item.payloadDigest || deliver.payloadDigest !== item.payloadDigest) throw new TraceMismatch("$.queue.items", "queue payload digest mismatch", item.payloadDigest, { admit, deliver });
    if (admit.index >= deliver.index || item.admittedAt !== admit.index || item.deliveredAt !== deliver.index) throw new TraceMismatch("$.queue.items", "queue admission/delivery boundary mismatch", item, { admit, deliver });
    const latestToolEnd = Math.max(-1, ...trace.events.filter((event) => event.type === "tool_execution_end").map((event) => event.index));
    if (latestToolEnd >= deliver.index) throw new TraceMismatch("$.queue.items", "queue delivery precedes final tool end", latestToolEnd, deliver.index);
  }
}

function abortInvariant(trace) {
  if (trace.outcome.stopReason !== "aborted") return;
  const aborts = trace.events.filter((event) => event.type === "abort_signal");
  if (aborts.length !== 1) throw new TraceMismatch("$.events", "aborted outcome requires one abort boundary", 1, aborts.length);
  const boundary = aborts[0].index;
  if (trace.outcome.abortBoundary !== aborts[0].boundary) throw new TraceMismatch("$.outcome.abortBoundary", "abort outcome boundary mismatch", aborts[0].boundary, trace.outcome.abortBoundary);
  for (const event of trace.events) {
    if (event.index > boundary && ["tool_execution_start", "tool_execution_update"].includes(event.type)) {
      throw new TraceMismatch(`$.events[${event.index}]`, "no new tool execution or late update after abort", "no post-abort tool event", event);
    }
  }
}

function providerInvariant(trace) {
  if (trace.outcome.errorCategory !== "provider") return;
  const errors = trace.events.filter((event) => event.type === "provider_error");
  if (errors.length !== 1) throw new TraceMismatch("$.events", "provider error outcome requires one provider_error event", 1, errors.length);
  if (trace.outcome.stopReason !== "error" && trace.outcome.stopReason !== "aborted") throw new TraceMismatch("$.outcome.stopReason", "provider category requires terminal error/abort stop reason", "error|aborted", trace.outcome.stopReason);
}

export function assertInvariants(trace) {
  lifecycleInvariant(trace);
  messagePairInvariant(trace);
  const projections = toolPairInvariant(trace);
  if (trace.observations?.completionOrder !== undefined && JSON.stringify(trace.observations.completionOrder) !== JSON.stringify(projections.completionOrder)) {
    throw new TraceMismatch("$.observations.completionOrder", "completion-order projection mismatch", projections.completionOrder, trace.observations.completionOrder);
  }
  if (trace.observations?.sourceOrder !== undefined && JSON.stringify(trace.observations.sourceOrder) !== JSON.stringify(projections.sourceOrder)) {
    throw new TraceMismatch("$.observations.sourceOrder", "source-order projection mismatch", projections.sourceOrder, trace.observations.sourceOrder);
  }
  queueInvariant(trace);
  abortInvariant(trace);
  providerInvariant(trace);
  return projections;
}

export function compareTrace(expected, actual) {
  validateTrace(expected, expected.caseId);
  validateTrace(actual, expected.caseId);
  assertInvariants(expected);
  assertInvariants(actual);
  const normalizedExpected = normalizedClone(expected);
  const normalizedActual = normalizedClone(actual);
  const mismatch = firstDeepMismatch(normalizedExpected, normalizedActual);
  if (mismatch) throw mismatch;
  return { ok: true, projections: toolPairInvariant(actual) };
}

function compareActualEventFields(expected, actual, path) {
  const expectedKeys = ownKeys(expected);
  const actualKeys = ownKeys(actual);
  if (format(expectedKeys) !== format(actualKeys)) {
    throw new TraceMismatch(`${path}.keys`, "actual AgentEvent projection keys mismatch", expectedKeys, actualKeys);
  }
  for (const key of expectedKeys) {
    const mismatch = firstDeepMismatch(expected[key], actual[key], `${path}.${key}`);
    if (mismatch) throw mismatch;
  }
}

function validateActualObservation(actual, expectedCaseId) {
  assertObject(actual, "actualObservation");
  assertAllowedKeys(actual, ACTUAL_OBSERVATION_KEYS, "actualObservation");
  if (actual.schemaVersion !== 1) {
    throw new TraceMismatch("actualObservation.schemaVersion", "unsupported actual schema version", 1, actual.schemaVersion);
  }
  if (actual.authority !== AUTHORITY) {
    throw new TraceMismatch("actualObservation.authority", "actual authority mismatch", AUTHORITY, actual.authority);
  }
  assertString(actual.caseId, "actualObservation.caseId");
  if (actual.caseId !== expectedCaseId) {
    throw new TraceMismatch("actualObservation.caseId", "actual case ID mismatch", expectedCaseId, actual.caseId);
  }
  validateInput(actual.input, "actualObservation.input");
  if (!Array.isArray(actual.events)) {
    throw new TraceMismatch("actualObservation.events", "expected array", "array", actual.events);
  }
  actual.events.forEach((event, index) => validateActualEvent(event, `actualObservation.events[${index}]`));
  if (!Array.isArray(actual.messages)) {
    throw new TraceMismatch("actualObservation.messages", "expected array", "array", actual.messages);
  }
  actual.messages.forEach((message, index) => validateMessage(message, `actualObservation.messages[${index}]`));
  assertObject(actual.outcome, "actualObservation.outcome");
  assertAllowedKeys(actual.outcome, ACTUAL_OUTCOME_KEYS, "actualObservation.outcome");
  for (const key of ["kind", "stopReason", "errorCategory", "errorMessage"]) {
    if (actual.outcome[key] !== undefined && actual.outcome[key] !== null) {
      assertString(actual.outcome[key], `actualObservation.outcome.${key}`);
    }
  }
  for (const key of ["kind", "stopReason", "errorCategory", "errorMessage", "terminal"]) {
    if (!Object.prototype.hasOwnProperty.call(actual.outcome, key)) {
      throw new TraceMismatch(
        `actualObservation.outcome.${key}`,
        "actual outcome omits a required field",
        "declared field present",
        actual.outcome,
      );
    }
  }
  assertString(actual.outcome.stopReason, "actualObservation.outcome.stopReason");
  if (actual.outcome.errorCategory !== undefined && actual.outcome.errorCategory !== null) {
    assertString(actual.outcome.errorCategory, "actualObservation.outcome.errorCategory");
  }
  if (actual.outcome.terminal !== undefined) {
    assertBoolean(actual.outcome.terminal, "actualObservation.outcome.terminal");
  }
  if (actual.outcome.executed !== undefined) {
    assertBoolean(actual.outcome.executed, "actualObservation.outcome.executed");
  }
  assertObject(actual.observations, "actualObservation.observations");
  assertAllowedKeys(actual.observations, OBSERVATION_KEYS, "actualObservation.observations");
  return actual;
}

/**
 * Compare an implementation-generated actual Agent observation against one
 * checked-in fixture. The actual envelope contains only fields captured from
 * the fresh Agent run plus test-owned side-channel observations; queue/abort/
 * provider pseudo-events are rejected here.
 */
export function compareObservation(expected, actualObservation) {
  validateTrace(expected, expected.caseId);
  assertInvariants(expected);
  const actual = validateActualObservation(actualObservation, expected.caseId);
  lifecycleInvariant(actual);
  messagePairInvariant(actual);
  toolPairInvariant(actual);

  const unrepresentableExpectedEvents = expected.events.filter((event) => !ACTUAL_EVENT_TYPES.has(event.type));
  if (unrepresentableExpectedEvents.length > 0) {
    throw new TraceMismatch(
      "$.events",
      "fixture contains side-channel records that are not AgentEvent projections",
      [...ACTUAL_EVENT_TYPES],
      unrepresentableExpectedEvents.map((event) => event.type),
    );
  }
  if (expected.events.length !== actual.events.length) {
    throw new TraceMismatch("$.events.length", "actual event count mismatch", expected.events.length, actual.events.length);
  }
  for (let index = 0; index < expected.events.length; index += 1) {
    compareActualEventFields(expected.events[index], actual.events[index], `$.events[${index}]`);
  }

  const expectedOutcomeKeys = ownKeys(expected.outcome);
  const actualOutcomeKeys = ownKeys(actual.outcome);
  if (format(expectedOutcomeKeys) !== format(actualOutcomeKeys)) {
    throw new TraceMismatch(
      "$.outcome.keys",
      "actual outcome keys mismatch",
      expectedOutcomeKeys,
      actualOutcomeKeys,
    );
  }
  for (const key of expectedOutcomeKeys) {
    const expectedValue = expected.outcome[key] ?? null;
    const actualValue = actual.outcome[key] ?? null;
    const mismatch = firstDeepMismatch(expectedValue, actualValue, `$.outcome.${key}`);
    if (mismatch) throw mismatch;
  }

  const inputMismatch = firstDeepMismatch(expected.input, actual.input, "$.input");
  if (inputMismatch) throw inputMismatch;

  const messagesMismatch = firstDeepMismatch(expected.messages, actual.messages, "$.messages");
  if (messagesMismatch) throw messagesMismatch;

  const expectedObservationKeys = ownKeys(expected.observations ?? {});
  const actualObservationKeys = ownKeys(actual.observations);
  if (format(expectedObservationKeys) !== format(actualObservationKeys)) {
    throw new TraceMismatch(
      "$.observations.keys",
      "actual side-channel observation keys mismatch",
      expectedObservationKeys,
      actualObservationKeys,
    );
  }
  for (const key of expectedObservationKeys) {
    const mismatch = firstDeepMismatch(
      expected.observations[key],
      actual.observations[key],
      `$.observations.${key}`,
    );
    if (mismatch) throw mismatch;
  }
  return { ok: true, caseId: expected.caseId };
}

export function firstFailure(expected, actual) {
  try {
    compareTrace(expected, actual);
    return null;
  } catch (error) {
    if (error instanceof TraceMismatch) return { path: error.path, message: error.message };
    throw error;
  }
}

export { AUTHORITY };
