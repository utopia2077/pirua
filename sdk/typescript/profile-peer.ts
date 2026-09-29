// A process peer: no Session, Effects, credentials, or durable authority.
import { readFileSync, readSync, writeSync } from "node:fs";
import {
  Codec, varint, payloadObject, sha, jsonValue, wireValue,
} from "./profiles/wire.ts";
import { Shapes, probe, canonical, portable } from "./profiles/shapes.ts";
const [mode, repo, coreText, minorText, scenario = "all-methods"] =
  process.argv.slice(2);
if (mode === "--probe") {
  writeSync(1, JSON.stringify(probe(repo)));
  process.exit(0);
}
if (mode === "--key-fidelity-probe") {
  writeSync(1, JSON.stringify(keyFidelityProbe(repo, coreText)));
  process.exit(0);
}
if (mode !== "--serve") throw Error("peer mode");
const core = Number(coreText),
  minor = Number(minorText),
  codec = new Codec(repo, core),
  profileCodec = new Codec(repo, minor),
  shapes = new Shapes(repo);
const corpus = JSON.parse(
  readFileSync(
    `${repo}/conformance/extension-protocol/profiles-runtime-v1.json`,
    "utf8",
  ),
);
const runtimeTelemetry = scenario === "runtime-native-telemetry" ||
  scenario === "runtime-native-telemetry-bad-ack" ||
  scenario === "runtime-native-telemetry-bad-flush" ||
  scenario === "runtime-native-before-resume" ||
  scenario === "runtime-native-host-snapshot" ||
  scenario === "runtime-native-event-sink" ||
  scenario === "runtime-native-event-sink-wrong-id";
const runtimeEvent = scenario === "runtime-native-event-sink" ||
  scenario === "runtime-native-event-sink-wrong-id";
const runtimeSnapshot = scenario === "runtime-native-host-snapshot" || runtimeEvent;
const runtimeResume = scenario === "runtime-native-before-resume";
const runtimeOpenLeaves = runtimeResume ? {
  ...corpus.exposure.open_leaves,
  "hook.output:before_run.result.resumeData": {
    type: "object", properties: { marker: { type: "string", enum: ["peer-before-resume"] } },
    required: ["marker"], additionalProperties: false,
  },
  "hook.input:before_resume.event.resumeData": {
    type: "object", properties: { marker: { type: "string", enum: ["peer-before-resume"] } },
    required: ["marker"], additionalProperties: false,
  },
} : corpus.exposure.open_leaves;
const runtimeChanged = scenario === "runtime-native-changed" ||
  scenario === "runtime-native-changed-wrong-scope" || runtimeTelemetry;
if (
  !(corpus.scenarios.includes(scenario) || scenario === "runtime-native" || runtimeChanged) ||
  ![0, 1].includes(core) ||
  ![0, 1].includes(minor)
)
  throw Error("selection");
let inbound = 0,
  outbound = 0,
  onceDone = false;
const MAX = 262144;
function readExact(n: number): Buffer {
  const b = Buffer.alloc(n);
  let at = 0;
  while (at < n) {
    const got = readSync(0, b, at, n - at, null);
    if (!got) throw Error("EOF");
    at += got;
  }
  return b;
}
function recv(): any {
  let n = 0n,
    prefix = Buffer.alloc(0);
  for (let i = 0; i < 10; i++) {
    let b: Buffer;
    try {
      b = readExact(1);
    } catch (e) {
      if (prefix.length === 0) return null;
      throw e;
    }
    prefix = Buffer.concat([prefix, b]);
    n |= BigInt(b[0] & 127) << BigInt(i * 7);
    if (b[0] < 128) {
      if (!prefix.equals(varint(n)) || n === 0n || n > BigInt(MAX))
        throw Error("frame cap");
      if (++inbound > 128) throw Error("frame count");
      return codec.decode("Envelope", readExact(Number(n)));
    }
  }
  throw Error("prefix cap");
}
function send(e: any) {
  const b = codec.encode("Envelope", e);
  if (b.length > MAX || ++outbound > 128) throw Error("outbound cap");
  const raw = Buffer.concat([varint(b.length), b]);
  let at = 0;
  while (at < raw.length) at += writeSync(1, raw, at, raw.length - at);
}
function sortedMaps(obj: any): any {
  if (Array.isArray(obj)) return obj.map(sortedMaps);
  if (!obj || typeof obj !== "object" || Buffer.isBuffer(obj)) return obj;
  const out: any = {};
  for (const [k, v] of Object.entries(obj)) out[k] = sortedMaps(v);
  if (out.map_value)
    out.map_value.entries.sort((a: any, b: any) =>
      a.key < b.key ? -1 : a.key > b.key ? 1 : 0,
    );
  return out;
}
function boundCheck(obj: any) {
  if (Array.isArray(obj)) {
    obj.forEach(boundCheck);
    return;
  }
  if (!obj || typeof obj !== "object" || Buffer.isBuffer(obj)) return;
  if (Object.hasOwn(obj, "projection_id")) {
    const root = obj.projection_id;
    if (
      !shapes.registry.projections[root] ||
      obj.schema_digest_sha256?.toString("hex") !==
        shapes.registry.projections[root].proposed_digest_sha256 ||
      !obj.value ||
      !shapes.check(root, obj.value) ||
      !shapes.exposed(root, obj.value, runtimeOpenLeaves)
    )
      throw Error("selected projection");
  }
  for (const x of Object.values(obj)) boundCheck(x);
}
const runtimeMethods = [
  "resource.list", "resource.read", "before_run", "before_request",
  "transform_context", "provider.invoke", "after_response", "before_run_end",
  ...(runtimeChanged ? ["resource.list", "resource.read"] : []),
];
let runtimeIndex = 0;
let runtimeRunId: string | null = null;
let runtimeProviderId: number | null = null;
let changedList: any = null;
let changedAcked = false;
let telemetryBatches = 0;
let telemetryLastSequence: number | null = null;
let telemetryFlushed = false;
let resumePhase = 0;
const resumeMethods = [
  "before_run", "before_request", "transform_context", "provider.invoke",
  "after_response", "before_resume", "before_request", "provider.invoke",
  "after_response", "before_run_end",
];
let suspendedRunId: string | null = null;
let suspendedProviderId: number | null = null;
let deferredProviderId: number | null = null;
let snapshotParent: any = null;
let snapshotPhase = 0;
let eventSubscriptionId: string | null = null;
let eventPhase = 0;
let eventCredit = 0;
let eventGapLast = -1;
function awaitSignal(phase: string) {
  const path = process.env.PI_PROFILE_PEER_SIGNAL;
  if (!path) throw Error("snapshot signal missing");
  const deadline = Date.now() + 4000;
  while (Date.now() < deadline) {
    try {
      if (readFileSync(path, "utf8") === phase) return;
    } catch (error: any) {
      if (error.code !== "ENOENT") throw error;
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2);
  }
  throw Error(`snapshot signal timeout: ${phase}`);
}
function nativeSnapshotRequest(withParent: boolean) {
  const method = "snapshot.get";
  const ref = granted.selected_capabilities.find((c: any) =>
    c.profile.name === "host-action.v1" && c.name === "actions");
  if (!ref || ref.handler_id !== "qualified_handler")
    throw Error("selected snapshot grant");
  const request: any = {
    request_id: withParent ? 1000 : 1001,
    capability_ref: ref,
    method,
    deadline_after_ms: 2000,
    initial_stream_credit_bytes: 0,
    payload: profileCodec.encode(profileCodec.methods[method].request,
      { lane: "main", visibility: "captured_policy" }),
  };
  if (withParent) request.parent_request_id = snapshotParent.request_id;
  send({ request });
}
function nativeEventRequest(method: "event.subscribe" | "event.resync") {
  const ref = granted.selected_capabilities.find((c: any) =>
    c.profile.name === "event-sink.v1" && c.name === "events");
  if (!ref || ref.handler_id !== "qualified_handler" ||
      ref.schema_digest_sha256.toString("hex") !== profileCodec.digests["event-sink.v1"])
    throw Error("selected event grant");
  const request = method === "event.subscribe"
    ? { scope: "captured_scope" }
    : { subscription_id: scenario.endsWith("wrong-id")
      ? `${eventSubscriptionId}-wrong` : eventSubscriptionId };
  send({ request: {
    request_id: method === "event.subscribe" ? 3000 : 3001,
    capability_ref: ref, method, deadline_after_ms: 5000,
    initial_stream_credit_bytes: 0,
    payload: profileCodec.encode(profileCodec.methods[method].request, request),
  } });
  if (method === "event.subscribe") {
    awaitSignal("event-credit");
    eventCredit = 1048576;
    send({ credit: { request_id: 3000, additional_bytes: eventCredit } });
  }
}
const runtimeIds = new Set<number>();
function runtimeProjection(value: any, id: string): any {
  if (value?.projection_id !== id || !value.value)
    throw Error("runtime projection identity");
  return jsonValue(value.value);
}
function validateRuntime(r: any, m: any, decoded: any) {
  const encoded = profileCodec.encode(m.request, sortedMaps(decoded));
  const again = profileCodec.decode(m.request, encoded);
  boundCheck(again);
  if (!profileCodec.encode(m.request, sortedMaps(again)).equals(encoded))
    throw Error("runtime DTO round trip");
  if (runtimeIndex < runtimeMethods.length) {
    if (r.method !== runtimeMethods[runtimeIndex++])
      throw Error(`runtime method order: ${r.method}`);
  } else if (runtimeResume && telemetryFlushed) {
    if (r.method !== resumeMethods[resumePhase])
      throw Error(`runtime resume phase ${resumePhase}: ${r.method}`);
    resumePhase++;
  } else if (!runtimeTelemetry || telemetryFlushed ||
      (r.method !== "telemetry.emit_batch" &&
        (r.method !== "telemetry.flush" || telemetryBatches === 0))) {
    throw Error(`runtime telemetry order: ${r.method}`);
  }
  if (
    !Number.isSafeInteger(r.request_id) || r.request_id <= 0 ||
    runtimeIds.has(r.request_id) ||
    (r.method === "after_response"
      ? r.parent_request_id !== runtimeProviderId
      : r.parent_request_id !== undefined) ||
    r.initial_stream_credit_bytes !== 0
  ) throw Error("runtime request identity");
  runtimeIds.add(r.request_id);
  if (r.method === "provider.invoke") {
    runtimeProviderId = r.request_id;
    if (runtimeResume && telemetryFlushed) {
      if (r.effect_key?.effect_kind === "assistant") suspendedProviderId = r.request_id;
      if (r.effect_key?.effect_kind === "deferred") deferredProviderId = r.request_id;
    }
  }
  const name = m.profile === "resource.v1" ? "resources" :
    m.profile === "provider.v1" ? "remote" :
    m.profile === "telemetry-sink.v1" ? "metrics" : r.method;
  const selected = granted.selected_capabilities.find(
    (c: any) => c.profile.name === m.profile && c.name === name,
  );
  if (
    !selected || selected.handler_id !== "qualified_handler" ||
    !codec.encode("CapabilityRef", r.capability_ref).equals(
      codec.encode("CapabilityRef", selected),
    )
  ) throw Error("runtime selected reference");
  if (m.profile === "telemetry-sink.v1") {
    if (!runtimeTelemetry || r.effect_key !== undefined ||
      !Number.isSafeInteger(r.deadline_after_ms) ||
      r.deadline_after_ms <= 0 || r.deadline_after_ms > 5000)
      throw Error("runtime telemetry binding");
    if (r.method === "telemetry.emit_batch") {
      if (decoded.schema_version !== 1 ||
        decoded.sequence !== telemetryBatches ||
        !Array.isArray(decoded.records) || decoded.records.length === 0)
        throw Error("runtime telemetry batch");
      for (const record of decoded.records) {
        const value = runtimeProjection(record, "telemetry.record.v1");
        if (JSON.stringify(value).includes("SENTINEL"))
          throw Error("runtime telemetry exposure");
      }
      telemetryLastSequence = decoded.sequence;
      telemetryBatches++;
    } else {
      if (decoded.through_sequence !== telemetryLastSequence)
        throw Error("runtime telemetry flush sequence");
      telemetryFlushed = true;
    }
    return;
  }
  if (m.profile === "resource.v1") {
    const expected = payloadObject(corpus.methods[r.method].request);
    if (!profileCodec.encode(m.request, sortedMaps(decoded)).equals(
      profileCodec.encode(m.request, expected),
    )) throw Error("runtime resource request");
    return;
  }
  if (m.profile === "hook.v1") {
    if (decoded.handler_id !== "qualified_handler")
      throw Error("runtime handler");
    const value = runtimeProjection(decoded.input, `hook.input:${r.method}`);
    if (value.name !== r.method || value.event?.lane !== "main")
      throw Error("runtime hook event");
    const event = value.event;
    if (r.method === "before_run") {
      if (
        typeof event.runId !== "string" ||
        !/^runtime-id-[0-9]+$/.test(event.runId) ||
        !Array.isArray(event.prompt) ||
        !JSON.stringify(event.prompt).includes("native profile peer dynamic run") ||
        (event.systemPrompt !== null && typeof event.systemPrompt !== "string")
      ) throw Error("runtime before_run fields");
      runtimeRunId = event.runId;
      if (runtimeResume && telemetryFlushed) suspendedRunId = event.runId;
    } else if (event.runId !== runtimeRunId) {
      throw Error("runtime hook run");
    }
    if (r.method === "before_resume" && (
      event.kind !== "run" || event.runId !== suspendedRunId ||
      !JSON.stringify(event.prompt).includes("native profile peer dynamic run deferred source") ||
      JSON.stringify(event.resumeData) !== JSON.stringify({ marker: "peer-before-resume" }) ||
      !suspendedProviderId || r.effect_key !== undefined
    )) throw Error("runtime before_resume fields");
    if (r.method === "before_request" && (
      event.model?.id !== "m" || event.model.provider !== "remote" ||
      event.model.api !== "openai-completions" ||
      event.step !== (runtimeResume && resumePhase === 7 ? "deferred" : "assistant") ||
      !Number.isSafeInteger(event.attempt) ||
      event.attempt < 1
    )) throw Error("runtime before_request fields");
    if (r.method === "after_response" && (
      event.message?.role !== "assistant" ||
      event.message.provider !== "remote" || event.message.model !== "m"
    )) throw Error("runtime after_response fields");
    if (["transform_context", "before_run_end"].includes(r.method) &&
        !Array.isArray(event.messages)) throw Error("runtime hook messages");
    return;
  }
  if (m.profile !== "provider.v1" || decoded.model_id !== "m" ||
      decoded.auth_mode !== "none") throw Error("runtime provider identity");
  const model = runtimeProjection(decoded.model, "provider.model.v1");
  const context = runtimeProjection(decoded.context, "provider.context.v1");
  const options = runtimeProjection(decoded.options, "provider.options.v1");
  const tools = runtimeProjection(decoded.tools, "provider.tools.v1");
  if (
    model.id !== "m" || model.provider !== "remote" ||
    model.api !== "openai-completions" ||
    !Array.isArray(context.messages) ||
    !(runtimeResume && r.effect_key?.effect_kind === "deferred"
      ? JSON.stringify(context.messages).includes("native-source-handle")
      : JSON.stringify(context.messages).includes("native profile peer dynamic run")) ||
    !options || typeof options !== "object" || Array.isArray(options) ||
    !Array.isArray(tools) || tools.length !== 0
  ) throw Error("runtime provider fields");
  if (runtimeResume && telemetryFlushed) {
    const key = r.effect_key;
    if (!key || key.operation_id !== suspendedRunId ||
      !["assistant", "deferred"].includes(key.effect_kind) ||
      (key.effect_kind === "deferred" && (
        !suspendedProviderId || !JSON.stringify(context.messages).includes("native-source-handle") ||
        options.deferred !== false
      ))) throw Error("runtime deferred provider binding");
  }
}
const hooks = Object.keys(corpus.methods).filter(
  (n) => profileCodec.methods[n].profile === "hook.v1",
);
const entries = [
  { profile: "host-action.v1", name: "actions-resource" },
  ...hooks.map((name) => ({ profile: "hook.v1", name })),
  ...[
    "provider",
    "resource",
    "host-action",
    "event-sink",
    "telemetry-sink",
  ].map((p) => ({
    profile: p + ".v1",
    name: {
      provider: "remote",
      resource: "resources",
      "host-action": "actions",
      "event-sink": "events",
      "telemetry-sink": "metrics",
    }[p],
  })),
];
const range = (name: string) => ({
  name,
  major: 1,
  min_minor: minor,
  max_minor: minor,
});
send({
  hello: {
    protocol: { major: 1, min_minor: core, max_minor: core },
    supported_profiles: [...new Set(entries.map((x) => x.profile))].map(range),
  },
});
const hello = recv();
if (hello?.hello_ack?.selected_protocol?.minor !== core)
  throw Error("HelloAck");
const caps = entries.map((e) => ({
  kind: e.profile.slice(0, -3),
  name: e.name,
  profile: range(e.profile),
  handler_id: "qualified_handler",
  replay: 1,
  execution:
    e.profile === "provider.v1"
      ? 2
      : ["event-sink.v1", "telemetry-sink.v1"].includes(e.profile)
        ? 3
        : 1,
  required: true,
  supported_descriptor_digests: [
    {
      minor,
      descriptor_digest_sha256: Buffer.from(
        profileCodec.digests[e.profile],
        "hex",
      ),
    },
  ],
}));
send({
  manifest: {
    declared_extension_id: "peer-claimed",
    extension_version: "1",
    protocol: { major: 1, min_minor: core, max_minor: core },
    capabilities: caps,
    permissions: corpus.exposure.permission_scopes.map(
      ([name, scope]: string[]) => ({
        name,
        scope,
        required: scenario !== "host-ungranted" || name !== "queue.steer",
      }),
    ),
  },
});
const granted = recv()?.grant;
if (
  !granted ||
  granted.authenticated_extension_id !== "peer-a" ||
  granted.connection_generation !== 7
)
  throw Error("grant identity");
for (const cap of granted.selected_capabilities) {
  if (
    cap.profile.minor !== minor ||
    cap.schema_digest_sha256.toString("hex") !==
      profileCodec.digests[cap.profile.name]
  )
    throw Error("grant digest");
}
send({ ready: {} });
const streams = new Map<number, any>();
let hostIndex = 0,
  parent: any = null;
let passiveIndex = 0,
  passiveSequence = 0,
  passiveCredit = 1048576,
  passiveValidated = 0;
const passiveNames =
  scenario === "event-gap"
    ? ["event.subscribe", "event.resync"]
    : ["resource.changed", "event.subscribe", "event.resync"];
function passiveRequest() {
  const method = passiveNames[passiveIndex];
  if (!method) return;
  const m = profileCodec.methods[method],
    ref = granted.selected_capabilities.find(
      (c: any) => c.profile.name === m.profile,
    );
  send({
    request: {
      request_id: 2000 + passiveIndex,
      capability_ref: ref,
      method,
      initial_stream_credit_bytes: m.mode === 2 ? 1048576 : undefined,
      payload: profileCodec.encode(
        m.request,
        payloadObject(corpus.methods[method].request),
      ),
    },
  });
}
function nativeChangedRequest() {
  const method = "resource.changed";
  const ref = granted.selected_capabilities.find(
    (c: any) => c.profile.name === "resource.v1" && c.name === "resources",
  );
  if (!ref || changedList === null) throw Error("native changed state");
  const request = payloadObject(corpus.methods[method].request);
  if (scenario === "runtime-native-changed-wrong-scope") request.scope = "wrong-scope";
  send({ request: {
    request_id: 2000,
    capability_ref: ref,
    method,
    initial_stream_credit_bytes: 0,
    payload: profileCodec.encode(profileCodec.methods[method].request, request),
  } });
}
if (["passive-origin", "event-gap"].includes(scenario)) passiveRequest();
function resultFor(method: string) {
  const v =
    corpus.scenario_payloads?.[scenario]?.[method] ?? corpus.methods[method];
  return payloadObject(
    (scenario === "none" || scenario.startsWith("runtime-native")) && profileCodec.methods[method].profile === "hook.v1"
      ? v.default_terminal
      : (v.positive_terminal ?? v.terminal),
  );
}
function terminal(r: any, decoded?: any) {
  const m = profileCodec.methods[r.method];
  let obj = resultFor(r.method);
  if (runtimeResume && telemetryFlushed && r.method === "before_run") {
    obj = resultFor(r.method);
    obj.output = payloadObject(corpus.methods.before_run.positive_terminal.output);
    obj.output.value = wireValue({
      name: "before_run", result: { resumeData: { marker: "peer-before-resume" } },
    });
  }
  if (runtimeResume && telemetryFlushed && r.method === "provider.invoke" &&
      r.effect_key?.effect_kind === "assistant") {
    const message = jsonValue(obj.assistant_message.value);
    message.stopReason = "deferred";
    message.deferred = {
      provider: "remote", api: "openai-completions", modelId: "m",
      id: "native-source-handle",
    };
    obj.assistant_message.value = wireValue(message);
  }
  if (runtimeTelemetry && r.method === "telemetry.emit_batch") {
    if (!decoded) throw Error("runtime telemetry payload");
    obj = { accepted: decoded.records.length, dropped: 0 };
    if (scenario === "runtime-native-telemetry-bad-ack" && telemetryBatches === 1)
      obj.accepted++;
  }
  if (runtimeTelemetry && r.method === "telemetry.flush") {
    if (!decoded) throw Error("runtime telemetry flush payload");
    obj = { flushed_through: decoded.through_sequence };
    if (scenario === "runtime-native-telemetry-bad-flush")
      obj.flushed_through++;
  }
  if (scenario === "pending" && r.method === "provider.invoke")
    obj.assistant_message.value.map_value.entries.find(
      (e: any) => e.key === "stopReason",
    ).value = { text_value: "pending" };
  if (scenario === "wrong-projection" && r.method === "provider.invoke")
    obj.assistant_message.projection_id = "peer-alias";
  if (scenario === "bad-ack" && r.method === "telemetry.emit_batch")
    obj.accepted = 2;
  if (scenario === "bad-flush" && r.method === "telemetry.flush")
    obj.flushed_through = 2;
  if (scenario === "bad-resource" && r.method === "resource.read")
    obj.text = "y";
  if (scenario === "sentinel" && r.method === "before_tool")
    obj.output.value.map_value.entries.find(
      (e: any) => e.key === "result",
    ).value = {
      map_value: {
        entries: [
          {
            key: "args",
            value: {
              map_value: {
                entries: [
                  { key: "authorization", value: { text_value: "SENTINEL" } },
                ],
              },
            },
          },
        ],
      },
    };
  const payload = profileCodec.encode(m.terminal, obj);
  if (runtimeResume) {
    const roundTrip = profileCodec.decode(m.terminal, payload);
    boundCheck(roundTrip);
    if (!profileCodec.encode(m.terminal, roundTrip).equals(payload))
      throw Error("runtime terminal DTO round trip");
  }
  const end: any = { request_id: r.request_id };
  if (scenario === "error" || scenario === "unknown-enum")
    end.error = {
      code: scenario === "unknown-enum" ? 777 : 14,
      safe_message: "handler failed",
    };
  else end.result = payload;
  const arm = m.mode === 2 ? "stream_end" : "response";
  send({ [arm]: end });
  if (runtimeEvent && r.method === "telemetry.flush") {
    awaitSignal("event-subscribe");
    nativeEventRequest("event.subscribe");
  }
  if (runtimeChanged && r.method === "resource.read" && runtimeIndex === 10) {
    if (!changedAcked) throw Error("native changed acknowledgement missing");
  }
  if (scenario === "duplicate") send({ [arm]: end });
  if (scenario === "single-provider") onceDone = true;
}
function progress(r: any) {
  const v = corpus.methods[r.method];
  return r.method === "event.subscribe" && scenario === "event-gap"
    ? [v.first_item, v.event_item, v.gap_item]
    : [v.progress ?? v.first_item];
}
function flush(state: any) {
  while (state.items.length) {
    const payload = profileCodec.encode(
      profileCodec.methods[state.r.method].progress,
      payloadObject(state.items[0]),
    );
    const item = {
      request_id: state.r.request_id,
      sequence: scenario === "sequence" ? state.sequence + 1 : state.sequence,
      payload,
    };
    const charge = codec.encode("Envelope", { stream_item: item }).length;
    if (state.credit < charge) break;
    state.credit -= charge;
    state.sequence++;
    state.items.shift();
    send({ stream_item: item });
  }
  if (
    !state.items.length ||
    !["credit", "cancel-progress", "silence"].includes(scenario)
  ) {
    terminal(state.r);
    streams.delete(state.r.request_id);
  }
}
function hostRequest() {
  const names = [
    "snapshot.get",
    "resource.get",
    "queue.steer",
    "queue.follow_up",
    "queue.next_run",
    "operation.abort",
  ];
  let method =
    scenario === "host-actions"
      ? names[hostIndex]
      : scenario === "host-secret"
        ? "secret.resolve"
        : scenario === "host-unknown"
          ? "unfamiliar.method"
          : "queue.steer";
  if (!method) {
    terminal(parent);
    return;
  }
  const ref = granted.selected_capabilities.find(
    (c: any) =>
      c.profile.name === "host-action.v1" &&
      c.name === (method === "resource.get" ? "actions-resource" : "actions"),
  );
  const r: any = {
    request_id: 1000 + hostIndex,
    parent_request_id:
      scenario === "host-parent" ? undefined : parent.request_id,
    capability_ref: ref,
    method,
    deadline_after_ms: 100,
    payload: profileCodec.methods[method]
      ? profileCodec.encode(
          profileCodec.methods[method].request,
          payloadObject(corpus.methods[method].request),
        )
      : Buffer.alloc(0),
  };
  if (scenario === "host-reference")
    r.capability_ref = { ...ref, authenticated_extension_id: "wrong" };
  send({ request: r });
}
for (;;) {
  if (onceDone) break;
  const e = recv();
  if (!e) break;
  if (e.request) {
    const r = e.request,
      m = profileCodec.methods[r.method];
    if (!m) throw Error("unknown selected method");
    const decoded = profileCodec.decode(m.request, r.payload);
    boundCheck(decoded);
    if (scenario === "runtime-native" || runtimeChanged) {
      validateRuntime(r, m, decoded);
    } else {
      const expected = payloadObject(
        (
          corpus.scenario_payloads?.[scenario]?.[r.method] ??
          corpus.methods[r.method]
        ).request,
      );
      if (
        !profileCodec
          .encode(m.request, sortedMaps(decoded))
          .equals(profileCodec.encode(m.request, expected))
      )
        throw Error("request semantic mismatch");
    }
    if (
      r.capability_ref.profile.minor !== minor ||
      r.capability_ref.schema_digest_sha256.toString("hex") !==
        profileCodec.digests[m.profile]
    )
      throw Error("request reference");
    if (runtimeChanged && runtimeIndex === 9 && r.method === "resource.list") {
      changedList = r;
      nativeChangedRequest();
      continue;
    }
    if (runtimeSnapshot && r.method === "provider.invoke") {
      if (snapshotParent || runtimeIndex !== 6 || !r.effect_key?.operation_id)
        throw Error("snapshot provider parent");
      snapshotParent = r;
      awaitSignal("actions");
      nativeSnapshotRequest(true);
      continue;
    }
    if (scenario.startsWith("host-") && r.method === "provider.invoke") {
      parent = r;
      hostRequest();
      continue;
    }
    if (scenario === "silence") continue;
    if (m.mode === 2) {
      const state = {
        r,
        credit:
          scenario === "overcredit"
            ? 1048576
            : (r.initial_stream_credit_bytes ?? 0),
        sequence: 0,
        items: progress(r),
      };
      streams.set(r.request_id, state);
      if (!["cancel", "cancel-progress", "loss"].includes(scenario))
        flush(state);
    } else terminal(r, decoded);
  } else if (e.credit) {
    const s = streams.get(e.credit.request_id);
    if (!s) throw Error("unowned Credit");
    s.credit += e.credit.additional_bytes;
    if (s.credit > granted.limits.max_stream_credit_bytes)
      throw Error("credit ceiling");
    flush(s);
  } else if (e.cancel) {
    codec.selectedEnum("CancelReason", e.cancel.reason);
    const s = streams.get(e.cancel.request_id);
    if (s) {
      if (scenario === "cancel-progress") flush(s);
      else {
        terminal(s.r);
        streams.delete(s.r.request_id);
      }
    }
  } else if (runtimeSnapshot && e.response && snapshotParent && snapshotPhase < 2) {
    const response = e.response;
    if (response.request_id !== 1000 + snapshotPhase)
      throw Error("snapshot response identity");
    if (snapshotPhase === 0) {
      if (!("result" in response)) throw Error("snapshot action denied");
      const m = profileCodec.methods["snapshot.get"];
      const decoded = profileCodec.decode(m.terminal, response.result);
      boundCheck(decoded);
      if (!profileCodec.encode(m.terminal, sortedMaps(decoded)).equals(response.result))
        throw Error("snapshot result DTO round trip");
      const value = runtimeProjection(decoded.filtered_snapshot, "host.filtered_snapshot.v1");
      if (value.operation?.id !== snapshotParent.effect_key.operation_id ||
          value.lane !== "main" || JSON.stringify(value).includes("SENTINEL"))
        throw Error("snapshot live filtered operation");
      snapshotPhase = 1;
      nativeSnapshotRequest(false);
    } else {
      if (response.error?.code !== 8 || response.error.safe_message !== "request failed" ||
          "result" in response)
        throw Error("snapshot missing-parent refusal");
      snapshotPhase = 2;
      awaitSignal("provider");
      terminal(snapshotParent);
      snapshotParent = null;
    }
  } else if (runtimeEvent && e.stream_item) {
    const item = e.stream_item;
    if (item.request_id !== 3000 || item.sequence !== eventPhase ||
        eventCredit < codec.encode("Envelope", e).length)
      throw Error("event sequence or charged credit");
    eventCredit -= codec.encode("Envelope", e).length;
    const m = profileCodec.methods["event.subscribe"];
    const decoded = profileCodec.decode(m.progress, item.payload);
    boundCheck(decoded);
    if (!profileCodec.encode(m.progress, sortedMaps(decoded)).equals(item.payload))
      throw Error("event item DTO round trip");
    if (eventPhase === 0) {
      const snapshot = decoded.snapshot;
      if (!snapshot?.subscription_id || snapshot.next_event_sequence !== 0 ||
          !Number.isSafeInteger(snapshot.snapshot_revision) ||
          runtimeProjection(snapshot.filtered_snapshot, "event.filtered_snapshot.v1").lane !== "main")
        throw Error("native event initial snapshot");
      eventSubscriptionId = snapshot.subscription_id;
      writeSync(2, JSON.stringify({ stage: "event-snapshot", subscription_id: eventSubscriptionId }) + "\n");
    } else if (eventPhase === 1) {
      const gap = decoded.gap;
      if (!gap || gap.subscription_id !== eventSubscriptionId ||
          gap.first_dropped > gap.last_dropped || gap.first_dropped < 0)
        throw Error("native reserved Gap");
      eventGapLast = gap.last_dropped;
      writeSync(2, JSON.stringify({ stage: "event-gap", last_dropped: eventGapLast }) + "\n");
      awaitSignal("event-resync");
      nativeEventRequest("event.resync");
    } else throw Error("stale event after Gap");
    eventPhase++;
  } else if (runtimeEvent && e.response && e.response.request_id === 3001) {
    const response = e.response;
    if (eventPhase !== 2) throw Error("event resync before Gap");
    if (scenario.endsWith("wrong-id")) {
      if (response.error?.code !== 8 || response.error.safe_message !== "request failed" ||
          "result" in response) throw Error("wrong subscription refusal");
    } else {
      if (!("result" in response)) throw Error("native event resync Result");
      const m = profileCodec.methods["event.resync"];
      const decoded = profileCodec.decode(m.terminal, response.result);
      boundCheck(decoded);
      if (!profileCodec.encode(m.terminal, sortedMaps(decoded)).equals(response.result))
        throw Error("event resync DTO round trip");
      const snapshot = decoded.snapshot;
      const value = runtimeProjection(snapshot?.filtered_snapshot, "event.filtered_snapshot.v1");
      if (snapshot.subscription_id !== eventSubscriptionId ||
          snapshot.next_event_sequence <= eventGapLast ||
          !Array.isArray(value.queues?.nextRun) || value.queues.nextRun.length !== 2)
        throw Error("atomic event baseline");
    }
    eventPhase = 3;
    writeSync(2, JSON.stringify({ stage: "event-resync-result" }) + "\n");
  } else if (runtimeChanged && e.response && changedList) {
    const response = e.response,
      method = "resource.changed",
      m = profileCodec.methods[method];
    if (response.request_id !== 2000 || !("result" in response))
      throw Error("native changed response");
    const decoded = profileCodec.decode(m.terminal, response.result);
    boundCheck(decoded);
    if (!profileCodec.encode(m.terminal, sortedMaps(decoded)).equals(
      profileCodec.encode(m.terminal, resultFor(method)),
    )) throw Error("native changed acknowledgement");
    changedAcked = true;
    const list = changedList;
    changedList = null;
    terminal(list);
  } else if (e.response && parent) {
    if (scenario === "host-actions") {
      if (!("result" in e.response)) throw Error("host-action denied");
      const names = [
        "snapshot.get",
        "resource.get",
        "queue.steer",
        "queue.follow_up",
        "queue.next_run",
        "operation.abort",
      ];
      const n = names[hostIndex];
      const decoded = profileCodec.decode(
        profileCodec.methods[n].terminal,
        e.response.result,
      );
      boundCheck(decoded);
      if (
        !profileCodec
          .encode(profileCodec.methods[n].terminal, sortedMaps(decoded))
          .equals(
            profileCodec.encode(profileCodec.methods[n].terminal, resultFor(n)),
          )
      )
        throw Error("host result");
      hostIndex++;
      hostRequest();
    } else terminal(parent);
  } else if (
    ["passive-origin", "event-gap"].includes(scenario) &&
    e.stream_item
  ) {
    const method = passiveNames[passiveIndex],
      m = profileCodec.methods[method],
      item = e.stream_item;
    if (
      item.request_id !== 2000 + passiveIndex ||
      item.sequence !== passiveSequence++
    )
      throw Error("passive sequence");
    const charge = codec.encode("Envelope", e).length;
    if (charge > passiveCredit) throw Error("passive credit");
    passiveCredit -= charge;
    const decoded = profileCodec.decode(m.progress, item.payload);
    boundCheck(decoded);
    const expected =
      scenario === "event-gap"
        ? [
            corpus.methods[method].first_item,
            corpus.methods[method].event_item,
            corpus.methods[method].gap_item,
          ][item.sequence]
        : corpus.methods[method].first_item;
    if (
      !profileCodec
        .encode(m.progress, sortedMaps(decoded))
        .equals(profileCodec.encode(m.progress, payloadObject(expected)))
    )
      throw Error("passive projection");
    passiveValidated++;
  } else if (
    ["passive-origin", "event-gap"].includes(scenario) &&
    (e.response || e.stream_end)
  ) {
    const end = e.response ?? e.stream_end,
      method = passiveNames[passiveIndex],
      m = profileCodec.methods[method];
    if (end.request_id !== 2000 + passiveIndex || !("result" in end))
      throw Error("passive terminal");
    const decoded = profileCodec.decode(m.terminal, end.result);
    boundCheck(decoded);
    if (
      !profileCodec
        .encode(m.terminal, sortedMaps(decoded))
        .equals(profileCodec.encode(m.terminal, resultFor(method)))
    )
      throw Error("passive terminal value");
    passiveValidated++;
    passiveIndex++;
    passiveRequest();
  } else if (e.go_away) {
    codec.selectedEnum("GoAwayReason", e.go_away.reason);
    break;
  } else throw Error("unexpected envelope");
}
if (runtimeResume && (
  runtimeIndex !== runtimeMethods.length || !changedAcked || !telemetryFlushed ||
  resumePhase !== resumeMethods.length || !suspendedRunId ||
  !suspendedProviderId || !deferredProviderId
)) throw Error("incomplete native resume history");
if (runtimeEvent && (eventPhase !== 3 || !changedAcked || !telemetryFlushed ||
    snapshotPhase !== 2 || runtimeIndex !== runtimeMethods.length))
  throw Error("incomplete native event history");
writeSync(
  2,
  JSON.stringify({
    language: "TypeScript",
    core,
    minor,
    scenario,
    inbound,
    outbound,
    passiveValidated,
    codec_messages: codec.messages.size,
    descriptor_sha256: sha(
      readFileSync(
        `${repo}/crates/pi-extension-protocol/schema/descriptors/common-1.${core}.pb`,
      ),
    ),
  }) + "\n",
);

// Inputs contain independent Rust-produced selected-descriptor bytes, never verdicts.
function keyFidelityProbe(repo: string, inputPath: string) {
  const input = JSON.parse(readFileSync(inputPath, "utf8")),
    shapes = new Shapes(repo);
  const attempt = (f: () => any) => {
    try {
      return { value: f() };
    } catch (e) {
      return { error: (e as Error).message };
    }
  };
  return input.rows.map((row: any) => {
    const codec = new Codec(repo, row.minor),
      raw = Buffer.from(row.hex, "hex"),
      decoded = codec.decode(row.message, raw),
      w = row.message === "WireValue"
        ? decoded
        : (decoded.input ?? decoded.item).value;
    const result: any = {
      id: row.id,
      roundtripHex: codec.encode(row.message, decoded).toString("hex"),
      shape: attempt(() => shapes.check(row.type, w)),
      json: attempt(() => jsonValue(w)),
      canonical: attempt(() => canonical(jsonValue(w))),
      rebuiltHex: attempt(() =>
        codec.encode("WireValue", wireValue(jsonValue(w))).toString("hex")),
    };
    if (row.leaves)
      result.exposed = attempt(() => shapes.exposed(row.type, w, row.leaves));
    if (row.portable)
      result.portable = attempt(() =>
        portable(row.portable.schema, row.portable.value));
    return result;
  });
}
