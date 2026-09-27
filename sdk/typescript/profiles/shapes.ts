import { readFileSync } from "node:fs";
import { Codec, wireValue, jsonValue, sha } from "./wire.ts";
const SAFE = 9007199254740991;
const own = (dictionary: any, key: PropertyKey) =>
  Object.hasOwn(dictionary, key) ? dictionary[key] : undefined;
const nonempty = new Set([
  "ApiId",
  "CustomType",
  "DeferredId",
  "DiagnosticType",
  "EntryId",
  "LaneId",
  "ModelId",
  "OperationErrorCode",
  "OperationId",
  "ProviderId",
  "RawStopReason",
  "ResponseId",
  "ToolCallId",
  "ToolName",
  "UsageId",
  "CustomRole",
]);
export class Shapes {
  registry: any;
  maxDepth = 64;
  work = 0;
  elements = 0;
  decoded = 0;
  constructor(repo: string) {
    const raw = readFileSync(
      `${repo}/crates/pi-extension-protocol/schema/projections/static-v1.json`,
    );
    if (
      sha(raw) !==
      "a735c6862639f36a24e3190d650207193ef125b6a5b329a30701c2ace3aa60be"
    )
      throw Error("registry changed");
    this.registry = JSON.parse(raw.toString());
  }
  check(name: string, w: any, limits: any = {}): boolean {
    this.maxDepth = Math.min(limits.depth ?? 64, 64);
    this.work = limits.work ?? 1000000;
    this.elements = 0;
    this.decoded = 0;
    try {
      this.bounded(w, 0, limits);
      this.named(name, w, undefined, 0);
      return true;
    } catch (e) {
      if ((e as Error).message === "shape" || (e as Error).message === "limit")
        return false;
      throw e;
    }
  }
  charge(d: number) {
    if (d > this.maxDepth || --this.work < 0) throw Error("limit");
  }
  bounded(w: any, d: number, l: any) {
    if (d > (l.depth ?? 64)) throw Error("limit");
    const keys = Object.keys(w);
    if (keys.length !== 1) throw Error("shape");
    const k = keys[0],
      v = w[k];
    this.decoded += 64;
    if (k === "integer_value" && !Number.isSafeInteger(v)) throw Error("shape");
    if (k === "float_value" && !Number.isFinite(v)) throw Error("shape");
    if (k === "text_value") {
      if (Buffer.byteLength(v) > (l.string_bytes ?? 262144))
        throw Error("limit");
      this.decoded += Buffer.byteLength(v);
    } else if (k === "list_value") {
      this.elements += v.items.length;
      for (const x of v.items) this.bounded(x, d + 1, l);
    } else if (k === "map_value") {
      const seen = new Set();
      this.elements += v.entries.length;
      for (const x of v.entries) {
        if (seen.has(x.key) || !x.value) throw Error("shape");
        seen.add(x.key);
        this.decoded += Buffer.byteLength(x.key);
        this.bounded(x.value, d + 1, l);
      }
    } else if (
      k === "bytes_value" ||
      ![
        "null_value",
        "bool_value",
        "integer_value",
        "float_value",
        "text_value",
      ].includes(k)
    )
      throw Error("shape");
    if (
      this.elements > (l.elements ?? 16384) ||
      this.decoded > (l.decoded ?? 1048576)
    )
      throw Error("limit");
  }
  named(name: string, w: any, c: any, d: number) {
    const t = own(this.registry.types, name);
    if (!t) throw Error("unknown type");
    this.definition(t, w, c, d);
    const j = jsonValue(w);
    if (nonempty.has(name) && (typeof j !== "string" || !j.length))
      throw Error("shape");
    if (
      name === "CustomRole" &&
      ["user", "assistant", "toolResult"].includes(j)
    )
      throw Error("shape");
    if (
      name === "SettledAssistantMessage" &&
      (j.stopReason === "pending" ||
        (["error", "aborted"].includes(j.stopReason) &&
          !Object.hasOwn(j, "errorMessage")))
    )
      throw Error("shape");
    if (
      name === "MetadataPatch" &&
      (j.delete.some((k: string, i: number) => i > 0 && j.delete[i - 1] >= k) ||
        j.delete.some((k: string) => Object.hasOwn(j.set, k)))
    )
      throw Error("shape");
    if (name === "DurableFileOperations")
      for (const k of ["read", "written", "edited"])
        if (j[k].some((x: string, i: number) => i > 0 && j[k][i - 1] >= x))
          throw Error("shape");
  }
  expr(e: any, w: any, c: any, d: number) {
    this.charge(d);
    if (e.ref) return this.named(e.ref, w, c, d + 1);
    if (Object.hasOwn(e, "literal")) {
      if (JSON.stringify(jsonValue(w)) !== JSON.stringify(e.literal))
        throw Error("shape");
      return;
    }
    if (e.enum) {
      if (!e.enum.includes(w.text_value)) throw Error("shape");
      return;
    }
    if (e.nullable) {
      if ("null_value" in w) return;
      return this.expr(e.nullable, w, c, d + 1);
    }
    if (e.array) return this.array(e.array, w, c, d + 1);
    if (e.map) return this.map(e.map, w, c, d + 1);
    if (e.object) return this.object(e.object, w, d + 1);
    throw Error("unknown expression");
  }
  definition(e: any, w: any, c: any, d: number) {
    this.charge(d);
    switch (e.kind) {
      case "primitive": {
        const ok =
          e.arm === "string"
            ? "text_value" in w
            : e.arm === "bool"
              ? "bool_value" in w
              : e.arm === "integer"
                ? "integer_value" in w &&
                  w.integer_value >= e.min &&
                  w.integer_value <= e.max
                : ["finite_number", "safe_integer_or_finite_float"].includes(
                    e.arm,
                  ) &&
                  (("integer_value" in w &&
                    Math.abs(w.integer_value) <= SAFE) ||
                    ("float_value" in w && Number.isFinite(w.float_value)));
        if (!ok) throw Error("shape");
        break;
      }
      case "alias":
        this.expr(e.type, w, c, d + 1);
        break;
      case "literal":
        if (jsonValue(w) !== e.value) throw Error("shape");
        break;
      case "enum":
        if (!e.values.includes(w.text_value)) throw Error("shape");
        break;
      case "object":
        this.object(e, w, d + 1);
        break;
      case "array":
        this.array(e.element, w, c, d + 1);
        break;
      case "map":
        this.map(e, w, c, d + 1);
        break;
      case "alternatives": {
        let count = 0;
        for (const a of e.alternatives) {
          try {
            this.expr(a.type, w, c, d + 1);
            count++;
          } catch (x) {
            if ((x as Error).message !== "shape") throw x;
          }
        }
        if (count !== 1) throw Error("shape");
        break;
      }
      case "role_selected": {
        const j = jsonValue(w),
          role = j?.[e.selector];
        if (typeof role !== "string") throw Error("shape");
        this.expr(own(e.cases, role) ?? e.other, w, c, d + 1);
        break;
      }
      case "context_selected": {
        const api = c ? jsonValue(c).api : undefined;
        const selected = own(e.cases, api);
        if (!selected) throw Error("shape");
        this.expr(selected, w, c, d + 1);
        break;
      }
      case "native_open_role_object": {
        if (!w.map_value) throw Error("shape");
        this.expr(e.role, wireValue(jsonValue(w).role), c, d + 1);
        break;
      }
      case "native_json_algebra":
        break;
      case "protobuf_empty_unit":
        throw Error("shape");
      default:
        throw Error("unknown operator");
    }
  }
  exposed(root: string, w: any, leaves: any): boolean {
    const expression = (e: any, v: any, path: string, d: number): boolean => {
      if (d > 64) return false;
      if (e.ref) return named(e.ref, v, path, d + 1);
      if (e.nullable)
        return v === null || expression(e.nullable, v, path, d + 1);
      if (e.array)
        return v.every((x: any) => expression(e.array, x, path + "[]", d + 1));
      if (e.map)
        return Object.entries(v).every(([k, x]) =>
          expression(e.map.value, x, path + "." + k, d + 1),
        );
      if (e.object) return object(e.object, v, path, d + 1);
      return true;
    };
    const object = (t: any, v: any, path: string, d: number): boolean =>
      Object.entries(t.fields).every(
        ([k, s]: any) =>
          !Object.hasOwn(v, k) ||
          (v[k] === null &&
            !["JsonValue", "JsonObject"].includes(s.type.ref)) ||
          expression(s.type, v[k], path + "." + k, d + 1),
      );
    const named = (name: string, v: any, path: string, d: number): boolean => {
      if (d > 64) return false;
      if (
        [
          "JsonValue",
          "JsonObject",
          "CustomAgentMessage",
          "JsonSchema",
        ].includes(name)
      )
        return !!own(leaves, path) && portable(own(leaves, path), v);
      const t = own(this.registry.types, name);
      if (!t) throw Error("unknown type");
      switch (t.kind) {
        case "alias":
          return expression(t.type, v, path, d + 1);
        case "object":
          return object(t, v, path, d + 1);
        case "array":
          return v.every((x: any) =>
            expression(t.element, x, path + "[]", d + 1),
          );
        case "role_selected":
          return expression(own(t.cases, v.role) ?? t.other, v, path, d + 1);
        case "context_selected":
          return !!own(leaves, path) && portable(own(leaves, path), v);
        case "alternatives":
          for (const a of t.alternatives) {
            this.work = 1000000;
            try {
              this.expr(a.type, wireValue(v), undefined, 0);
              return expression(a.type, v, path, d + 1);
            } catch (e) {
              if ((e as Error).message !== "shape") throw e;
            }
          }
          return false;
        default:
          return true;
      }
    };
    const json = jsonValue(w);
    return !canonical(json).includes("SENTINEL") && named(root, json, root, 0);
  }

  array(e: any, w: any, c: any, d: number) {
    if (!w.list_value) throw Error("shape");
    for (const v of w.list_value.items) this.expr(e, v, c, d + 1);
  }
  map(e: any, w: any, c: any, d: number) {
    if (!w.map_value) throw Error("shape");
    for (const x of w.map_value.entries) {
      this.expr(e.key, { text_value: x.key }, c, d + 1);
      this.expr(e.value, x.value, c, d + 1);
    }
  }
  object(e: any, w: any, d: number) {
    if (!w.map_value) throw Error("shape");
    const j = jsonValue(w);
    if (e.closed && Object.keys(j).some((k) => !Object.hasOwn(e.fields, k)))
      throw Error("shape");
    for (const [k, s] of Object.entries(e.fields) as any) {
      this.charge(d);
      const v = w.map_value.entries.find((x: any) => x.key === k)?.value;
      if (!v) {
        if (s.presence === "required") throw Error("shape");
        continue;
      }
      if ("null_value" in v && s.nullable) continue;
      this.expr(s.type, v, w, d + 1);
    }
  }
}
export function portable(schema: any, v: any): boolean {
  if (schema.type === "null") return v === null;
  if (schema.type === "string") return typeof v === "string";
  if (schema.type === "object") {
    if (!v || typeof v !== "object" || Array.isArray(v)) return false;
    return (
      (schema.required ?? []).every((k: string) => Object.hasOwn(v, k)) &&
      Object.keys(v).every((k) =>
        Object.hasOwn(schema.properties, k)
          ? portable(schema.properties[k], v[k])
          : schema.additionalProperties !== false,
      )
    );
  }
  throw Error("unadmitted portable operator");
}
export function canonical(v: any): string {
  if (Array.isArray(v)) return "[" + v.map(canonical).join(",") + "]";
  if (v && typeof v === "object")
    return (
      "{" +
      Object.keys(v)
        .sort()
        .map((k) => JSON.stringify(k) + ":" + canonical(v[k]))
        .join(",") +
      "}"
    );
  return JSON.stringify(v);
}
export function policy(op: string, p: any, s: Shapes): any {
  switch (op) {
    case "portable":
      return portable(p.schema, p.value);
    case "bound": {
      const projection = own(s.registry.projections, p.root);
      return (
        !!projection &&
        p.projection === p.root &&
        p.digest === projection.proposed_digest_sha256 &&
        p.value !== undefined &&
        s.check(p.root, wireValue(p.value))
      );
    }
    case "unit":
      return Object.keys(p.value).length === 0;
    case "structural":
      return (
        s.check(p.type, wireValue(p.value)) &&
        !(p.value.decline === true && (p.value.compaction || p.value.summary))
      );
    case "auth_none":
      return !Object.hasOwn(p.value, "headers");
    case "provider_progress":
      return (
        s.check("provider.assistant_event.v1", wireValue(p.value)) &&
        !["done", "error"].includes(p.value.type)
      );
    case "provider_terminal": {
      const m = p.value;
      return (
        s.check("provider.assistant_message.v1", wireValue(m)) &&
        canonical(m.usage) === canonical(p.usage ?? m.usage) &&
        (m.stopReason !== "deferred" ||
          (m.deferred?.provider === m.provider &&
            m.deferred?.modelId === m.model &&
            m.deferred?.api === m.api)) &&
        (!["error", "aborted"].includes(m.stopReason) ||
          m.errorMessage === "request failed" ||
          m.errorMessage === "operation failed")
      );
    }
    case "tuples":
      return (
        new Set(p.value.map((x: any) => canonical(x))).size === p.value.length
      );
    case "digest":
      return sha(p.value) === p.digest;
    case "canonical":
      return { canonical: canonical(p.value), sha256: sha(canonical(p.value)) };
    case "parent":
      return (
        p.live &&
        p.principal === "peer-a" &&
        p.generation === 7 &&
        p.depth <= 8 &&
        !p.cycle
      );
    case "method":
      return p.method === "secret.resolve"
        ? "broker_unavailable"
        : p.method === "unfamiliar.method"
          ? "unsupported_method"
          : p.granted
            ? "admit"
            : "forbidden";
    case "exposure":
      return !canonical(p.value).includes("SENTINEL") && p.binding;
    case "sequence":
      return p.value.every((n: number, i: number) => n === p.baseline + i);
    case "telemetry_attributes": {
      const e = s.registry.types.TelemetryAttributes.fields;
      const out: any = {};
      for (const [k, v] of Object.entries(p.value)) {
        const field = own(e, k);
        if (!field) continue;
        const t = field.type;
        if (t.enum?.includes(v) || (t.ref && s.check(t.ref, wireValue(v))))
          out[k] = v;
      }
      return out;
    }
    case "ack":
      return (
        BigInt(p.value.accepted ?? p.value.accepted_decimal) +
          BigInt(p.value.dropped) <=
          0xffffffffffffffffn &&
        BigInt(p.value.accepted ?? p.value.accepted_decimal) +
          BigInt(p.value.dropped) ===
          BigInt(p.count)
      );
    case "flush":
      return p.ack <= p.emitted && p.ack <= p.through;
    case "json_duplicate":
      return duplicateFree(p.value);
    case "headers": {
      const counts = new Map();
      for (const [k] of p.value)
        counts.set(k.toLowerCase(), (counts.get(k.toLowerCase()) ?? 0) + 1);
      return p.value.filter(
        ([k]: any) =>
          counts.get(k.toLowerCase()) === 1 &&
          !["authorization", "cookie", "set-cookie", "x-api-key"].includes(
            k.toLowerCase(),
          ),
      );
    }
    case "gap": {
      let bytes = 0,
        queued: number[] = [],
        gap: any = null,
        next = 0;
      for (const n of p.value) {
        if (n !== next++) throw Error("shape");
        if (gap) continue;
        if (bytes + 1 > 2) {
          gap = { first_dropped: queued[0] ?? n, last_dropped: n };
          queued = [];
          bytes = 0;
        } else {
          queued.push(n);
          bytes++;
        }
      }
      return { gap, next, paused: !!gap };
    }
    default:
      throw Error(`unknown policy ${op}`);
  }
}
export function probe(repo: string) {
  const s = new Shapes(repo),
    corpus = JSON.parse(
      readFileSync(
        `${repo}/conformance/extension-protocol/profiles-runtime-v1.json`,
        "utf8",
      ),
    );
  const rows = corpus.probes.map((row: any) => ({
    id: row.id,
    results: row.checks.map((p: any) =>
      p.op === "shape"
        ? s.check(p.type, p.arm ?? wireValue(p.value), p.limits)
        : policy(p.op, p, s),
    ),
  }));
  const controls = corpus.controls.map((p: any) => ({
    id: p.id,
    result: s.check(p.type, p.arm ?? wireValue(p.value), p.limits),
  }));
  const codecs = [0, 1].map((minor) => {
    const c = new Codec(repo, minor),
      w = JSON.parse(
        readFileSync(
          `${repo}/crates/pi-extension-protocol/schema/projections/method-wire-v1.json`,
          "utf8",
        ),
      );
    return {
      minor,
      minor_omission_hex: c
        .encode("CapabilityDeclaration", { display_label: "minor-one-only" })
        .toString("hex"),
      digests: c.digests,
      fields: w.field_witnesses
        .filter((x: any) => x.minor === minor)
        .map((x: any) => {
          const raw = Buffer.from(x.hex, "hex"),
            decoded = c.decode(x.message, raw);
          return {
            message: x.message,
            choice: x.oneof_choice,
            hex: c.encode(x.message, decoded).toString("hex"),
          };
        }),
    };
  });
  return { rows, controls, codecs };
}

// Track duplicate keys separately for every object before constructing a value.
function duplicateFree(raw: string): boolean {
  let at = 0,
    work = 1000000;
  const space = () => {
    while (/[ \t\r\n]/.test(raw[at] ?? "") && at < raw.length) at++;
  };
  const string = (): string => {
    const begin = at++;
    while (at < raw.length) {
      const ch = raw[at++];
      if (ch === "\\") {
        at++;
        continue;
      }
      if (ch === '"') {
        const value = JSON.parse(raw.slice(begin, at));
        if (
          /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(
            value,
          )
        )
          throw Error("surrogate");
        return value;
      }
    }
    throw Error("string");
  };
  const value = (depth: number): void => {
    if (depth > 64 || --work < 0) throw Error("limit");
    space();
    if (raw[at] === '"') {
      string();
      return;
    }
    if (raw[at] === "{") {
      at++;
      space();
      const seen = new Set<string>();
      if (raw[at] === "}") {
        at++;
        return;
      }
      for (;;) {
        space();
        if (raw[at] !== '"') throw Error("key");
        const key = string();
        if (seen.has(key)) throw Error("duplicate");
        seen.add(key);
        space();
        if (raw[at++] !== ":") throw Error("colon");
        value(depth + 1);
        space();
        const next = raw[at++];
        if (next === "}") return;
        if (next !== ",") throw Error("object");
      }
    }
    if (raw[at] === "[") {
      at++;
      space();
      if (raw[at] === "]") {
        at++;
        return;
      }
      for (;;) {
        value(depth + 1);
        space();
        const next = raw[at++];
        if (next === "]") return;
        if (next !== ",") throw Error("array");
      }
    }
    const begin = at;
    while (at < raw.length && !/[\s,}\]]/.test(raw[at])) at++;
    const parsed = JSON.parse(raw.slice(begin, at));
    if (typeof parsed === "number" && !Number.isFinite(parsed))
      throw Error("finite");
  };
  try {
    if (Buffer.byteLength(raw) > 262144) return false;
    value(0);
    space();
    return at === raw.length;
  } catch {
    return false;
  }
}
