"""Selected canonical descriptor tables drive the minimal protobuf codec."""

import hashlib
import struct
from pathlib import Path


def sha(raw):
    return hashlib.sha256(raw.encode() if isinstance(raw, str) else raw).hexdigest()


def varint(n):
    if not isinstance(n, int) or not 0 <= n <= 2**64 - 1:
        raise ValueError("unsigned range")
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def raw_fields(raw):
    at = 0

    def uint():
        nonlocal at
        n = 0
        for i in range(10):
            if at >= len(raw):
                raise ValueError("truncated varint")
            x = raw[at]
            at += 1
            if i == 9 and x > 1:
                raise ValueError("varint range")
            n |= (x & 127) << (i * 7)
            if x < 128:
                return n
        raise ValueError("varint length")

    out = []
    while at < len(raw):
        tag = uint()
        num, wire = tag >> 3, tag & 7
        if not num:
            raise ValueError("zero tag")
        if wire == 0:
            value = uint()
        elif wire in (1, 2, 5):
            length = uint() if wire == 2 else 8 if wire == 1 else 4
            if length > len(raw) - at:
                raise ValueError("length")
            value = raw[at : at + length]
            at += length
        else:
            raise ValueError("wire type")
        out.append((num, wire, value))
    return out


def field(raw, n, default=None):
    return next((v for num, _, v in raw if num == n), default)


class Codec:
    def __init__(self, repo, minor, include_tool=False):
        self.messages, self.enums, self.methods, self.digests = {}, {}, {}, {}
        for file in (
            "common",
            "hook",
            "provider",
            *(("tool",) if include_tool else ()),
            "resource",
            "host_action",
            "event_sink",
            "telemetry_sink",
        ):
            raw = (
                Path(repo)
                / f"crates/pi-extension-protocol/schema/descriptors/{file}-1.{minor}.pb"
            ).read_bytes()
            profile = file.replace("_", "-") + ".v1"
            self.digests[profile] = sha(
                f"{profile}\0".encode() + f"1.{minor}\0".encode() + raw
            )
            for num, _, fd in raw_fields(raw):
                if num != 1:
                    continue
                ff = raw_fields(fd)
                pkg = field(ff, 2).decode()
                if pkg != "pi.extension.v1":
                    continue

                def enum_def(raw, prefix):
                    ee = raw_fields(raw)
                    name = prefix + "." + field(ee, 1).decode()
                    self.enums[name] = [
                        field(raw_fields(x), 2) for n, _, x in ee if n == 2
                    ]

                def message(raw, prefix):
                    mm = raw_fields(raw)
                    name = prefix + "." + field(mm, 1).decode()
                    fields = []
                    for n, _, x in mm:
                        if n != 2:
                            continue
                        f = raw_fields(x)
                        fields.append(
                            {
                                "name": field(f, 1).decode(),
                                "number": field(f, 3),
                                "label": field(f, 4),
                                "type": field(f, 5),
                                "type_name": field(f, 6, b"").decode(),
                                "oneof": field(f, 9),
                                "optional": field(f, 17) == 1,
                            }
                        )
                    self.messages[name] = fields
                    for n, _, x in mm:
                        if n == 3:
                            message(x, name)
                        elif n == 4:
                            enum_def(x, name)

                for n, _, x in ff:
                    if n == 4:
                        message(x, "." + pkg)
                    elif n == 5:
                        enum_def(x, "." + pkg)
                    elif n == 6:
                        for mn, _, mb in raw_fields(x):
                            if mn != 2:
                                continue
                            m = raw_fields(mb)
                            opts = raw_fields(field(m, 4))
                            method = field(opts, 51001).decode()
                            self.methods[method] = {
                                "request": field(m, 2).decode(),
                                "terminal": field(m, 3).decode(),
                                "direction": field(opts, 51002),
                                "mode": field(opts, 51003),
                                "progress": field(opts, 51004, b"").decode() or None,
                                "profile": profile,
                            }

    @staticmethod
    def full(name):
        return name if name.startswith(".") else ".pi.extension.v1." + name

    def encode(self, name, obj):
        out = bytearray()
        for f in self.messages[self.full(name)]:
            value = obj.get(f["name"])
            if value is None:
                continue
            vals = value if f["label"] == 3 else [value]
            for v in vals:
                t = f["type"]
                present = f["oneof"] is not None or f["optional"] or t == 11
                if not present and v in ("", False, 0, b""):
                    continue
                if t == 11:
                    wire, b = 2, self.encode(f["type_name"], v)
                elif t in (9, 12):
                    wire, b = 2, v.encode() if t == 9 else v
                elif t == 1:
                    wire, b = 1, struct.pack("<d", v)
                elif t in (3, 4, 5, 8, 13, 14, 17, 18):
                    n = int(v)
                    if t in (17, 18):
                        n = (n << 1) ^ (n >> 63)
                    wire, b = 0, varint(n % 2**64)
                else:
                    raise ValueError("unsupported descriptor scalar")
                out += varint((f["number"] << 3) | wire)
                if wire == 2:
                    out += varint(len(b))
                out += b
        return bytes(out)

    def decode(self, name, raw):
        fields = self.messages[self.full(name)]
        out = {}
        for f in fields:
            t = f["type"]
            if f["label"] == 3:
                out[f["name"]] = []
            elif f["oneof"] is None and not f["optional"] and t != 11:
                out[f["name"]] = (
                    "" if t == 9 else b"" if t == 12 else False if t == 8 else 0
                )
        for num, wire, v in raw_fields(raw):
            f = next((f for f in fields if f["number"] == num), None)
            if not f:
                continue
            t = f["type"]
            want = 2 if t in (9, 11, 12) else 1 if t == 1 else 0
            if wire != want:
                raise ValueError("wrong wire arm")
            if t == 11:
                value = self.decode(f["type_name"], v)
            elif t == 9:
                value = v.decode("utf-8", errors="strict")
            elif t == 12:
                value = v
            elif t == 1:
                value = struct.unpack("<d", v)[0]
            else:
                if t in (17, 18):
                    v = (v >> 1) ^ -(v & 1)
                elif t in (3, 5):
                    bits = 32 if t == 5 else 64
                    v %= 2**bits
                    if v >= 2 ** (bits - 1):
                        v -= 2**bits
                value = bool(v) if t == 8 else v
            if f["oneof"] is not None:
                for peer in fields:
                    if peer["oneof"] == f["oneof"]:
                        out.pop(peer["name"], None)
            if f["label"] == 3:
                out[f["name"]].append(value)
            else:
                out[f["name"]] = value
        return out

    def selected_enum(self, name, value):
        if value == 0 or value not in self.enums[self.full(name)]:
            raise ValueError("unsupported_variant")


def wire_value(value):
    if value is None:
        return {"null_value": {}}
    if isinstance(value, bool):
        return {"bool_value": value}
    if isinstance(value, int):
        return {"integer_value": value}
    if isinstance(value, float):
        return {"float_value": value}
    if isinstance(value, str):
        return {"text_value": value}
    if isinstance(value, list):
        return {"list_value": {"items": [wire_value(v) for v in value]}}
    return {
        "map_value": {
            "entries": [
                {"key": k, "value": wire_value(value[k])} for k in sorted(value)
            ]
        }
    }


def json_value(w):
    if "null_value" in w:
        return None
    for key in ("bool_value", "integer_value", "float_value", "text_value"):
        if key in w:
            return w[key]
    if "list_value" in w:
        return [json_value(v) for v in w["list_value"]["items"]]
    if "map_value" in w:
        out = {}
        for e in w["map_value"]["entries"]:
            if e["key"] in out or "value" not in e:
                raise ValueError("duplicate or missing map")
            out[e["key"]] = json_value(e["value"])
        return out
    raise ValueError("non-json arm")


def payload_object(v):
    if isinstance(v, list):
        return [payload_object(x) for x in v]
    if isinstance(v, dict):
        o = {}
        for k, x in v.items():
            if k == "schema_id":
                o["projection_id"] = x
            elif k.endswith("_hex"):
                o[k[:-4]] = bytes.fromhex(x)
            elif k == "value" and "schema_id" in v:
                o[k] = wire_value(x)
            else:
                o[k] = payload_object(x)
        return o
    return v
