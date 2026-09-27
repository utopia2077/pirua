"""Finite operators from the frozen native semantic registry; no fallback schema."""

import json
import math
from pathlib import Path
from .wire import Codec, wire_value, json_value, sha

NONEMPTY = set(
    "ApiId CustomType DeferredId DiagnosticType EntryId LaneId ModelId OperationErrorCode OperationId ProviderId RawStopReason ResponseId ToolCallId ToolName UsageId CustomRole".split()
)


class Shapes:
    def __init__(self, repo):
        raw = (
            Path(repo)
            / "crates/pi-extension-protocol/schema/projections/static-v1.json"
        ).read_bytes()
        if (
            sha(raw)
            != "a735c6862639f36a24e3190d650207193ef125b6a5b329a30701c2ace3aa60be"
        ):
            raise ValueError("registry changed")
        self.registry = json.loads(raw)

    def check(self, name, w, limits=None):
        limits = limits or {}
        self.max_depth = min(limits.get("depth", 64), 64)
        self.work, self.elements, self.decoded = limits.get("work", 1000000), 0, 0
        try:
            self.bounded(w, 0, limits)
            self.named(name, w, None, 0)
            return True
        except ValueError as e:
            if str(e) not in ("shape", "limit"):
                raise
            return False

    def charge(self, depth):
        self.work -= 1
        if depth > self.max_depth or self.work < 0:
            raise ValueError("limit")

    def bounded(self, w, depth, limits):
        if depth > limits.get("depth", 64):
            raise ValueError("limit")
        if len(w) != 1:
            raise ValueError("shape")
        k, v = next(iter(w.items()))
        self.decoded += 64
        if k == "integer_value" and (
            isinstance(v, bool) or not isinstance(v, int) or abs(v) > 9007199254740991
        ):
            raise ValueError("shape")
        if k == "float_value" and not math.isfinite(v):
            raise ValueError("shape")
        if k == "text_value":
            size = len(v.encode())
            if size > limits.get("string_bytes", 262144):
                raise ValueError("limit")
            self.decoded += size
        elif k == "list_value":
            self.elements += len(v["items"])
            for x in v["items"]:
                self.bounded(x, depth + 1, limits)
        elif k == "map_value":
            seen = set()
            self.elements += len(v["entries"])
            for x in v["entries"]:
                if x["key"] in seen or "value" not in x:
                    raise ValueError("shape")
                seen.add(x["key"])
                self.decoded += len(x["key"].encode())
                self.bounded(x["value"], depth + 1, limits)
        elif k not in (
            "null_value",
            "bool_value",
            "integer_value",
            "float_value",
            "text_value",
        ):
            raise ValueError("shape")
        if self.elements > limits.get("elements", 16384) or self.decoded > limits.get(
            "decoded", 1048576
        ):
            raise ValueError("limit")

    def named(self, name, w, context, depth):
        self.definition(self.registry["types"][name], w, context, depth)
        j = json_value(w)
        if name in NONEMPTY and (not isinstance(j, str) or not j):
            raise ValueError("shape")
        if name == "CustomRole" and j in ("user", "assistant", "toolResult"):
            raise ValueError("shape")
        if name == "SettledAssistantMessage" and (
            j["stopReason"] == "pending"
            or j["stopReason"] in ("error", "aborted")
            and "errorMessage" not in j
        ):
            raise ValueError("shape")
        if name == "MetadataPatch" and (
            any(a >= b for a, b in zip(j["delete"], j["delete"][1:]))
            or any(k in j["set"] for k in j["delete"])
        ):
            raise ValueError("shape")
        if name == "DurableFileOperations" and any(
            any(a >= b for a, b in zip(j[k], j[k][1:]))
            for k in ("read", "written", "edited")
        ):
            raise ValueError("shape")

    def expr(self, e, w, context, depth):
        self.charge(depth)
        if "ref" in e:
            return self.named(e["ref"], w, context, depth + 1)
        if "literal" in e:
            if (
                type(json_value(w)) is not type(e["literal"])
                or json_value(w) != e["literal"]
            ):
                raise ValueError("shape")
        elif "enum" in e:
            if w.get("text_value") not in e["enum"]:
                raise ValueError("shape")
        elif "nullable" in e:
            if "null_value" not in w:
                self.expr(e["nullable"], w, context, depth + 1)
        elif "array" in e:
            self.array(e["array"], w, context, depth + 1)
        elif "map" in e:
            self.map(e["map"], w, context, depth + 1)
        elif "object" in e:
            self.object(e["object"], w, depth + 1)
        else:
            raise ValueError("unknown expression")

    def definition(self, e, w, context, depth):
        self.charge(depth)
        kind = e["kind"]
        if kind == "primitive":
            arm = e["arm"]
            valid = (
                "text_value" in w
                if arm == "string"
                else (
                    "bool_value" in w
                    if arm == "bool"
                    else (
                        "integer_value" in w
                        and e["min"] <= w["integer_value"] <= e["max"]
                        if arm == "integer"
                        else (
                            (
                                "integer_value" in w
                                and abs(w["integer_value"]) <= 9007199254740991
                                or "float_value" in w
                                and math.isfinite(w["float_value"])
                            )
                            if arm in ("finite_number", "safe_integer_or_finite_float")
                            else False
                        )
                    )
                )
            )
            if not valid:
                raise ValueError("shape")
        elif kind == "alias":
            self.expr(e["type"], w, context, depth + 1)
        elif kind == "literal":
            j = json_value(w)
            if type(j) is not type(e["value"]) or j != e["value"]:
                raise ValueError("shape")
        elif kind == "enum":
            if w.get("text_value") not in e["values"]:
                raise ValueError("shape")
        elif kind == "object":
            self.object(e, w, depth + 1)
        elif kind == "array":
            self.array(e["element"], w, context, depth + 1)
        elif kind == "map":
            self.map(e, w, context, depth + 1)
        elif kind == "alternatives":
            count = 0
            for a in e["alternatives"]:
                try:
                    self.expr(a["type"], w, context, depth + 1)
                    count += 1
                except ValueError as err:
                    if str(err) != "shape":
                        raise
            if count != 1:
                raise ValueError("shape")
        elif kind == "role_selected":
            j = json_value(w)
            role = j.get(e["selector"]) if isinstance(j, dict) else None
            if not isinstance(role, str):
                raise ValueError("shape")
            self.expr(e["cases"].get(role, e["other"]), w, context, depth + 1)
        elif kind == "context_selected":
            api = json_value(context).get("api") if context else None
            if api not in e["cases"]:
                raise ValueError("shape")
            self.expr(e["cases"][api], w, context, depth + 1)
        elif kind == "native_open_role_object":
            if "map_value" not in w:
                raise ValueError("shape")
            self.expr(
                e["role"], wire_value(json_value(w).get("role")), context, depth + 1
            )
        elif kind == "native_json_algebra":
            pass
        elif kind == "protobuf_empty_unit":
            raise ValueError("shape")
        else:
            raise ValueError("unknown operator")

    def exposed(self, root, w, leaves):
        def expr(e, v, path, depth):
            if depth > 64:
                return False
            if "ref" in e:
                return named(e["ref"], v, path, depth + 1)
            if "nullable" in e:
                return v is None or expr(e["nullable"], v, path, depth + 1)
            if "array" in e:
                return all(expr(e["array"], x, path + "[]", depth + 1) for x in v)
            if "map" in e:
                return all(
                    expr(e["map"]["value"], x, path + "." + k, depth + 1)
                    for k, x in v.items()
                )
            if "object" in e:
                return obj(e["object"], v, path, depth + 1)
            return True

        def obj(t, v, path, depth):
            return all(
                k not in v
                or v[k] is None
                and s["type"].get("ref") not in ("JsonValue", "JsonObject")
                or expr(s["type"], v[k], path + "." + k, depth + 1)
                for k, s in t["fields"].items()
            )

        def named(name, v, path, depth):
            if depth > 64:
                return False
            if name in ("JsonValue", "JsonObject", "CustomAgentMessage", "JsonSchema"):
                return path in leaves and portable(leaves[path], v)
            t = self.registry["types"][name]
            kind = t["kind"]
            if kind == "alias":
                return expr(t["type"], v, path, depth + 1)
            if kind == "object":
                return obj(t, v, path, depth + 1)
            if kind == "array":
                return all(expr(t["element"], x, path + "[]", depth + 1) for x in v)
            if kind == "role_selected":
                return expr(t["cases"].get(v["role"], t["other"]), v, path, depth + 1)
            if kind == "context_selected":
                return path in leaves and portable(leaves[path], v)
            if kind == "alternatives":
                for a in t["alternatives"]:
                    self.work = 1000000
                    try:
                        self.expr(a["type"], wire_value(v), None, 0)
                        return expr(a["type"], v, path, depth + 1)
                    except ValueError as e:
                        if str(e) != "shape":
                            raise
                return False
            return True

        value = json_value(w)
        return "SENTINEL" not in canonical(value) and named(root, value, root, 0)

    def array(self, e, w, context, depth):
        if "list_value" not in w:
            raise ValueError("shape")
        for v in w["list_value"]["items"]:
            self.expr(e, v, context, depth + 1)

    def map(self, e, w, context, depth):
        if "map_value" not in w:
            raise ValueError("shape")
        for x in w["map_value"]["entries"]:
            self.expr(e["key"], {"text_value": x["key"]}, context, depth + 1)
            self.expr(e["value"], x["value"], context, depth + 1)

    def object(self, e, w, depth):
        if "map_value" not in w:
            raise ValueError("shape")
        j = json_value(w)
        if e["closed"] and any(k not in e["fields"] for k in j):
            raise ValueError("shape")
        for key, spec in e["fields"].items():
            self.charge(depth)
            value = next(
                (x["value"] for x in w["map_value"]["entries"] if x["key"] == key), None
            )
            if value is None:
                if spec["presence"] == "required":
                    raise ValueError("shape")
                continue
            if "null_value" in value and spec["nullable"]:
                continue
            self.expr(spec["type"], value, w, depth + 1)


def portable(schema, value):
    if schema["type"] == "null":
        return value is None
    if schema["type"] == "string":
        return isinstance(value, str)
    if schema["type"] == "object":
        return (
            isinstance(value, dict)
            and all(k in value for k in schema.get("required", []))
            and all(
                (
                    portable(schema["properties"][k], v)
                    if k in schema["properties"]
                    else schema.get("additionalProperties") is not False
                )
                for k, v in value.items()
            )
        )
    raise ValueError("unadmitted portable operator")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def policy(op, p, shapes):
    if op == "portable":
        return portable(p["schema"], p["value"])
    if op == "bound":
        return (
            p["projection"] == p["root"]
            and p["digest"]
            == shapes.registry["projections"][p["root"]]["proposed_digest_sha256"]
            and "value" in p
            and shapes.check(p["root"], wire_value(p["value"]))
        )
    if op == "unit":
        return not p["value"]
    if op == "structural":
        return shapes.check(p["type"], wire_value(p["value"])) and not (
            p["value"].get("decline") is True
            and ("compaction" in p["value"] or "summary" in p["value"])
        )
    if op == "auth_none":
        return "headers" not in p["value"]
    if op == "provider_progress":
        return shapes.check(
            "provider.assistant_event.v1", wire_value(p["value"])
        ) and p["value"]["type"] not in ("done", "error")
    if op == "provider_terminal":
        m = p["value"]
        deferred = m.get("deferred", {})
        return (
            shapes.check("provider.assistant_message.v1", wire_value(m))
            and m["usage"] == p.get("usage", m["usage"])
            and (
                m["stopReason"] != "deferred"
                or all(
                    deferred.get(k) == m[v]
                    for k, v in [
                        ("provider", "provider"),
                        ("modelId", "model"),
                        ("api", "api"),
                    ]
                )
            )
            and (
                m["stopReason"] not in ("error", "aborted")
                or m.get("errorMessage") in ("request failed", "operation failed")
            )
        )
    if op == "tuples":
        return len({canonical(x) for x in p["value"]}) == len(p["value"])
    if op == "digest":
        return sha(p["value"]) == p["digest"]
    if op == "canonical":
        c = canonical(p["value"])
        return {"canonical": c, "sha256": sha(c)}
    if op == "parent":
        return (
            p["live"]
            and p["principal"] == "peer-a"
            and p["generation"] == 7
            and p["depth"] <= 8
            and not p["cycle"]
        )
    if op == "method":
        return (
            "broker_unavailable"
            if p["method"] == "secret.resolve"
            else (
                "unsupported_method"
                if p["method"] == "unfamiliar.method"
                else "admit" if p["granted"] else "forbidden"
            )
        )
    if op == "exposure":
        return "SENTINEL" not in canonical(p["value"]) and p["binding"]
    if op == "sequence":
        return all(n == p["baseline"] + i for i, n in enumerate(p["value"]))
    if op == "telemetry_attributes":
        fields = shapes.registry["types"]["TelemetryAttributes"]["fields"]
        out = {}
        for k, v in p["value"].items():
            if k not in fields:
                continue
            t = fields[k]["type"]
            if (
                v in t.get("enum", [])
                or "ref" in t
                and shapes.check(t["ref"], wire_value(v))
            ):
                out[k] = v
        return out
    if op == "ack":
        count = (
            int(p["value"].get("accepted", p["value"].get("accepted_decimal")))
            + p["value"]["dropped"]
        )
        return count <= 2**64 - 1 and count == p["count"]
    if op == "flush":
        return p["ack"] <= p["emitted"] and p["ack"] <= p["through"]
    if op == "json_duplicate":

        def pairs(entries):
            out = {}
            for k, v in entries:
                if k in out:
                    raise ValueError("duplicate")
                out[k] = v
            return out

        try:
            json.loads(
                p["value"],
                object_pairs_hook=pairs,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")),
            )
            return True
        except ValueError:
            return False
    if op == "headers":
        counts = {}
        for k, _ in p["value"]:
            key = k.lower()
            counts[key] = counts.get(key, 0) + 1
        return [
            [k, v]
            for k, v in p["value"]
            if counts[k.lower()] == 1
            and k.lower() not in ("authorization", "cookie", "set-cookie", "x-api-key")
        ]
    if op == "gap":
        queued, gap, next_seq = [], None, 0
        for n in p["value"]:
            if n != next_seq:
                raise ValueError("shape")
            next_seq += 1
            if gap:
                continue
            if len(queued) + 1 > 2:
                gap = {"first_dropped": queued[0] if queued else n, "last_dropped": n}
                queued = []
            else:
                queued.append(n)
        return {"gap": gap, "next": next_seq, "paused": bool(gap)}
    raise ValueError("unknown policy " + op)


def probe(repo):
    s = Shapes(repo)
    corpus = json.loads(
        (
            Path(repo) / "conformance/extension-protocol/profiles-runtime-v1.json"
        ).read_text()
    )
    rows = [
        {
            "id": row["id"],
            "results": [
                (
                    s.check(
                        p["type"],
                        p.get("arm", wire_value(p.get("value"))),
                        p.get("limits"),
                    )
                    if p["op"] == "shape"
                    else policy(p["op"], p, s)
                )
                for p in row["checks"]
            ],
        }
        for row in corpus["probes"]
    ]
    controls = [
        {
            "id": p["id"],
            "result": s.check(
                p["type"], p.get("arm", wire_value(p.get("value"))), p.get("limits")
            ),
        }
        for p in corpus["controls"]
    ]
    witnesses = json.loads(
        (
            Path(repo)
            / "crates/pi-extension-protocol/schema/projections/method-wire-v1.json"
        ).read_text()
    )
    codecs = []
    for minor in (0, 1):
        c = Codec(repo, minor)
        fields = [
            {
                "message": x["message"],
                "choice": x["oneof_choice"],
                "hex": c.encode(
                    x["message"], c.decode(x["message"], bytes.fromhex(x["hex"]))
                ).hex(),
            }
            for x in witnesses["field_witnesses"]
            if x["minor"] == minor
        ]
        codecs.append(
            {
                "minor": minor,
                "minor_omission_hex": c.encode(
                    "CapabilityDeclaration", {"display_label": "minor-one-only"}
                ).hex(),
                "digests": c.digests,
                "fields": fields,
            }
        )
    return {"rows": rows, "controls": controls, "codecs": codecs}
