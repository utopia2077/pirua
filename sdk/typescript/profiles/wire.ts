// The selected canonical FileDescriptorSet supplies every field number and type.
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
export const sha = (b: Uint8Array | string) =>
  createHash("sha256").update(b).digest("hex");
export function varint(n: bigint | number): Buffer {
  let v = BigInt(n);
  if (v < 0n || v > 0xffffffffffffffffn) throw Error("unsigned range");
  const a: number[] = [];
  do {
    a.push(Number(v & 127n) | (v > 127n ? 128 : 0));
    v >>= 7n;
  } while (v);
  return Buffer.from(a);
}
export function rawFields(b: Buffer): [number, number, any][] {
  const out: [number, number, any][] = [];
  let at = 0;
  function uint(): bigint {
    let n = 0n;
    let i = 0;
    for (; i < 10; i++) {
      if (at >= b.length) throw Error("truncated varint");
      const x = b[at++];
      if (i === 9 && x > 1) throw Error("varint range");
      n |= BigInt(x & 127) << BigInt(i * 7);
      if (x < 128) return n;
    }
    throw Error("varint length");
  }
  while (at < b.length) {
    const tag = Number(uint()),
      wire = tag & 7,
      num = tag >>> 3;
    if (!num) throw Error("zero tag");
    let v: any;
    if (wire === 0) v = uint();
    else if (wire === 2) {
      const len = Number(uint());
      if (!Number.isSafeInteger(len) || len > b.length - at)
        throw Error("length");
      v = b.subarray(at, at + len);
      at += len;
    } else if (wire === 1 || wire === 5) {
      const len = wire === 1 ? 8 : 4;
      if (len > b.length - at) throw Error("fixed length");
      v = b.subarray(at, at + len);
      at += len;
    } else throw Error("wire type");
    out.push([num, wire, v]);
  }
  return out;
}
const text = (b: Buffer) => {
  const s = new TextDecoder("utf-8", { fatal: true }).decode(b);
  return s;
};
export class Codec {
  messages = new Map<string, any[]>();
  enums = new Map<string, number[]>();
  digests: Record<string, string> = {};
  methods: Record<string, any> = {};
  constructor(repo: string, minor: number) {
    for (const file of [
      "common",
      "hook",
      "provider",
      "resource",
      "host_action",
      "event_sink",
      "telemetry_sink",
    ]) {
      const bytes = readFileSync(
        `${repo}/crates/pi-extension-protocol/schema/descriptors/${file}-1.${minor}.pb`,
      );
      const profile = file.replaceAll("_", "-") + ".v1";
      this.digests[profile] = sha(
        Buffer.concat([Buffer.from(`${profile}\0${"1." + minor}\0`), bytes]),
      );
      for (const [, , fd] of rawFields(bytes).filter((x) => x[0] === 1)) {
        const f = rawFields(fd);
        const pkg = text(f.find((x) => x[0] === 2)![2]);
        if (pkg !== "pi.extension.v1") continue;
        const enumDef = (b: Buffer, prefix: string) => {
          const e = rawFields(b),
            name = `${prefix}.${text(e.find((x) => x[0] === 1)![2])}`;
          this.enums.set(
            name,
            e
              .filter((x) => x[0] === 2)
              .map((x) => Number(rawFields(x[2]).find((y) => y[0] === 2)![2])),
          );
        };
        const message = (b: Buffer, prefix: string) => {
          const m = rawFields(b),
            name = `${prefix}.${text(m.find((x) => x[0] === 1)![2])}`;
          const fields = m
            .filter((x) => x[0] === 2)
            .map((x) => {
              const ff = rawFields(x[2]);
              const get = (n: number) => ff.find((y) => y[0] === n)?.[2];
              return {
                name: text(get(1)),
                number: Number(get(3)),
                label: Number(get(4)),
                type: Number(get(5)),
                type_name: get(6) ? text(get(6)) : "",
                oneof: get(9) === undefined ? null : Number(get(9)),
                optional: get(17) === 1n,
              };
            });
          this.messages.set(name, fields);
          for (const x of m.filter((x) => x[0] === 3)) message(x[2], name);
          for (const x of m.filter((x) => x[0] === 4)) enumDef(x[2], name);
        };
        for (const x of f.filter((x) => x[0] === 4)) message(x[2], `.${pkg}`);
        for (const x of f.filter((x) => x[0] === 5)) enumDef(x[2], `.${pkg}`);
        for (const x of f.filter((x) => x[0] === 6))
          for (const [, , mb] of rawFields(x[2]).filter((y) => y[0] === 2)) {
            const m = rawFields(mb),
              opts = rawFields(m.find((y) => y[0] === 4)![2]),
              method = text(opts.find((y) => y[0] === 51001)![2]);
            this.methods[method] = {
              request: text(m.find((y) => y[0] === 2)![2]),
              terminal: text(m.find((y) => y[0] === 3)![2]),
              direction: Number(opts.find((y) => y[0] === 51002)![2]),
              mode: Number(opts.find((y) => y[0] === 51003)![2]),
              progress: opts.find((y) => y[0] === 51004)
                ? text(opts.find((y) => y[0] === 51004)![2])
                : undefined,
              profile,
            };
          }
      }
    }
  }
  full(n: string) {
    return n.startsWith(".") ? n : ".pi.extension.v1." + n;
  }
  encode(name: string, obj: any): Buffer {
    const fields = this.messages.get(this.full(name));
    if (!fields) throw Error(`unknown message ${name}`);
    const out: Buffer[] = [];
    for (const f of fields) {
      const val = obj[f.name];
      if (val === undefined || val === null) continue;
      const vals = f.label === 3 ? val : [val];
      for (const v of vals) {
        const present = f.oneof !== null || f.optional || f.type === 11;
        if (
          !present &&
          (v === "" ||
            v === false ||
            v === 0 ||
            v === 0n ||
            (Buffer.isBuffer(v) && v.length === 0))
        )
          continue;
        let wire: number, b: Buffer;
        if (f.type === 11) {
          wire = 2;
          b = this.encode(f.type_name, v);
        } else if (f.type === 9) {
          wire = 2;
          b = Buffer.from(v, "utf8");
        } else if (f.type === 12) {
          wire = 2;
          b = Buffer.from(v);
        } else if (f.type === 1) {
          wire = 1;
          b = Buffer.alloc(8);
          b.writeDoubleLE(v);
        } else if ([3, 4, 5, 8, 13, 14, 17, 18].includes(f.type)) {
          wire = 0;
          let n = BigInt(v);
          if (f.type === 8) n = v ? 1n : 0n;
          if (f.type === 17 || f.type === 18) n = (n << 1n) ^ (n >> 63n);
          if (n < 0n) n = BigInt.asUintN(64, n);
          b = varint(n);
        } else throw Error("unsupported descriptor scalar");
        out.push(varint((f.number << 3) | wire));
        if (wire === 2) out.push(varint(b.length));
        out.push(b);
      }
    }
    return Buffer.concat(out);
  }
  decode(name: string, b: Buffer): any {
    const fields = this.messages.get(this.full(name));
    if (!fields) throw Error(`unknown message ${name}`);
    const out: any = {};
    for (const f of fields) {
      if (f.label === 3) out[f.name] = [];
      else if (f.oneof === null && !f.optional && f.type !== 11)
        out[f.name] =
          f.type === 9
            ? ""
            : f.type === 12
              ? Buffer.alloc(0)
              : f.type === 8
                ? false
                : 0;
    }
    for (const [num, wire, v] of rawFields(b)) {
      const f = fields.find((f) => f.number === num);
      if (!f) continue;
      let value: any;
      const want = [9, 11, 12].includes(f.type) ? 2 : f.type === 1 ? 1 : 0;
      if (wire !== want) throw Error("wrong wire arm");
      if (f.type === 11) value = this.decode(f.type_name, v);
      else if (f.type === 9) value = text(v);
      else if (f.type === 12) value = v;
      else if (f.type === 1) value = v.readDoubleLE();
      else {
        let n = v;
        if (f.type === 17 || f.type === 18) n = (n >> 1n) ^ -(n & 1n);
        else if (f.type === 3 || f.type === 5)
          n = BigInt.asIntN(f.type === 5 ? 32 : 64, n);
        value =
          f.type === 8
            ? n !== 0n
            : n <= BigInt(Number.MAX_SAFE_INTEGER) &&
                n >= BigInt(Number.MIN_SAFE_INTEGER)
              ? Number(n)
              : n;
      }
      if (f.oneof !== null)
        for (const peer of fields.filter((x) => x.oneof === f.oneof))
          delete out[peer.name];
      if (f.label === 3) out[f.name].push(value);
      else out[f.name] = value;
    }
    return out;
  }
  selectedEnum(name: string, n: number) {
    if (!this.enums.get(this.full(name))?.includes(n) || n === 0)
      throw Error("unsupported_variant");
  }
}
export function wireValue(v: any): any {
  if (v === null) return { null_value: {} };
  if (typeof v === "boolean") return { bool_value: v };
  if (typeof v === "number")
    return Number.isInteger(v) ? { integer_value: v } : { float_value: v };
  if (typeof v === "string") return { text_value: v };
  if (Array.isArray(v)) return { list_value: { items: v.map(wireValue) } };
  return {
    map_value: {
      entries: Object.keys(v)
        .sort()
        .map((k) => ({ key: k, value: wireValue(v[k]) })),
    },
  };
}
export function jsonValue(w: any): any {
  if ("null_value" in w) return null;
  if ("bool_value" in w) return w.bool_value;
  if ("integer_value" in w) return w.integer_value;
  if ("float_value" in w) return w.float_value;
  if ("text_value" in w) return w.text_value;
  if ("list_value" in w) return w.list_value.items.map(jsonValue);
  if ("map_value" in w) {
    const out: any = Object.create(null);
    for (const e of w.map_value.entries) {
      if (Object.hasOwn(out, e.key) || !e.value)
        throw Error("duplicate or missing map");
      out[e.key] = jsonValue(e.value);
    }
    return out;
  }
  throw Error("non-json arm");
}
export function payloadObject(v: any): any {
  if (Array.isArray(v)) return v.map(payloadObject);
  if (v && typeof v === "object") {
    const o: any = {};
    for (const [k, x] of Object.entries(v)) {
      if (k === "schema_id") o.projection_id = x;
      else if (k.endsWith("_hex"))
        o[k.slice(0, -4)] = Buffer.from(x as string, "hex");
      else if (k === "value" && "schema_id" in v) o.value = wireValue(x);
      else o[k] = payloadObject(x);
    }
    return o;
  }
  return v;
}
