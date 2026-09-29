"""Framed six-profile reference process. Rust remains the effect owner."""

import json
import os
import sys
import time
from pathlib import Path
from profiles.wire import Codec, varint, payload_object, sha, wire_value, json_value
from profiles.shapes import Shapes, probe, canonical

mode, repo = sys.argv[1:3]
if mode == "--probe":
    sys.stdout.write(json.dumps(probe(repo), ensure_ascii=False, separators=(",", ":")))
    sys.exit(0)
if mode != "--serve":
    raise ValueError("peer mode")
core, minor = map(int, sys.argv[3:5])
scenario = sys.argv[5]
codec, profile_codec, shapes = Codec(repo, core), Codec(repo, minor), Shapes(repo)
corpus = json.loads(
    (Path(repo) / "conformance/extension-protocol/profiles-runtime-v1.json").read_text()
)
runtime_event = scenario in ("runtime-native-event-sink", "runtime-native-event-sink-wrong-id")
runtime_telemetry = scenario in ("runtime-native-telemetry", "runtime-native-telemetry-bad-ack", "runtime-native-telemetry-bad-flush", "runtime-native-before-resume", "runtime-native-host-snapshot", "runtime-native-host-queue") or runtime_event
runtime_queue = scenario == "runtime-native-host-queue"
runtime_structural = scenario.startswith("runtime-native-structural")
structural_case = scenario.removeprefix("runtime-native-structural").removeprefix("-") or "positive"
runtime_snapshot = scenario == "runtime-native-host-snapshot" or runtime_queue or runtime_event
runtime_resource = scenario.startswith("runtime-native-host-resource-get")
runtime_resource_r2 = scenario == "runtime-native-resource-r2"
resource_case = scenario.removeprefix("runtime-native-host-resource-get").removeprefix("-") or "positive"
runtime_resume = scenario == "runtime-native-before-resume"
runtime_payload = scenario in ("runtime-native-before-payload", "runtime-native-before-payload-malformed")
payload_schema = {
    "type": "object", "properties": {
        "model": {"type": "object"}, "context": {"type": "object"},
        "tools": {"type": "array", "items": {"type": "object"}, "maxItems": 0},
        "options": {"type": "object"},
    }, "required": ["model", "context", "tools", "options"], "additionalProperties": False,
}
if runtime_payload and sha(canonical(payload_schema).encode()) != "beccbba37a9f577ac7013fe76c88177e09a61858a91ed098076972629707643f":
    raise ValueError("payload schema digest")


def exact_schema(value):
    if value is None:
        return {"type": "null"}
    if isinstance(value, dict):
        return {"type": "object", "properties": {k: exact_schema(v) for k, v in value.items()}, "required": list(value), "additionalProperties": False}
    if isinstance(value, list):
        return {"type": "array", "items": exact_schema(value[0] if value else None), "minItems": len(value), "maxItems": len(value)}
    return {"type": "boolean" if isinstance(value, bool) else "number" if isinstance(value, (int, float)) else "string", "enum": [value]}


runtime_open_leaves = dict(corpus["exposure"]["open_leaves"])
if runtime_resume:
    saved_schema = {
        "type": "object",
        "properties": {"marker": {"type": "string", "enum": ["peer-before-resume"]}},
        "required": ["marker"],
        "additionalProperties": False,
    }
    runtime_open_leaves["hook.output:before_run.result.resumeData"] = saved_schema
    runtime_open_leaves["hook.input:before_resume.event.resumeData"] = saved_schema
if runtime_payload:
    runtime_open_leaves["hook.input:before_payload.event.payload"] = payload_schema
    runtime_open_leaves["hook.output:before_payload.result.payload"] = payload_schema
    model = corpus["methods"]["provider.invoke"]["request"]["model"]["value"]
    for part in ("compat", "samplingParams"):
        runtime_open_leaves[f"hook.input:before_payload.event.model.{part}"] = exact_schema(model.get(part))


def payload_shape(value):
    return (isinstance(value, dict) and set(value) == {"model", "context", "tools", "options"}
            and all(isinstance(value[part], dict) for part in ("model", "context", "options"))
            and value["tools"] == [])


def payload_exposure(root, wire):
    value = json_value(wire)
    if root == "hook.input:before_payload":
        event = value["event"]
        model = corpus["methods"]["provider.invoke"]["request"]["model"]["value"]
        return (payload_shape(event.get("payload"))
                and event["model"].get("compat") == model.get("compat")
                and event["model"].get("samplingParams") == model.get("samplingParams"))
    return payload_shape(value.get("result", {}).get("payload"))
runtime_changed = scenario in ("runtime-native-changed", "runtime-native-changed-wrong-scope") or runtime_telemetry
if not (scenario in (*corpus["scenarios"], "runtime-native", "runtime-native-changed", "runtime-native-changed-wrong-scope", "runtime-native-telemetry", "runtime-native-telemetry-bad-ack", "runtime-native-telemetry-bad-flush", "runtime-native-before-resume", "runtime-native-host-snapshot", "runtime-native-host-queue", "runtime-native-event-sink", "runtime-native-event-sink-wrong-id", "runtime-native-before-payload", "runtime-native-before-payload-malformed") or runtime_resource_r2 or runtime_resource and resource_case in ("positive", "missing", "scope", "version", "parent") or runtime_structural and structural_case in ("positive", "compaction-decline", "navigation-decline")) or core not in (0, 1) or minor not in (0, 1):
    raise ValueError("selection")
inbound, outbound = 0, 0
once_done = False


def read_exact(n):
    chunks = bytearray()
    while len(chunks) < n:
        chunk = os.read(0, n - len(chunks))
        if not chunk:
            raise EOFError("EOF")
        chunks += chunk
    return bytes(chunks)


def recv():
    global inbound
    n, prefix = 0, bytearray()
    for i in range(10):
        try:
            b = read_exact(1)[0]
        except EOFError:
            if not prefix:
                return None
            raise
        prefix.append(b)
        n |= (b & 127) << (7 * i)
        if b < 128:
            if bytes(prefix) != varint(n) or not 0 < n <= 262144:
                raise ValueError("frame cap")
            inbound += 1
            if inbound > 128:
                raise ValueError("frame count")
            return codec.decode("Envelope", read_exact(n))
    raise ValueError("prefix cap")


def send(e):
    global outbound
    body = codec.encode("Envelope", e)
    outbound += 1
    if len(body) > 262144 or outbound > 128:
        raise ValueError("outbound cap")
    raw = varint(len(body)) + body
    at = 0
    while at < len(raw):
        at += os.write(1, raw[at:])


def sorted_maps(obj):
    if isinstance(obj, list):
        return [sorted_maps(x) for x in obj]
    if isinstance(obj, dict):
        out = {k: sorted_maps(v) for k, v in obj.items()}
        if "map_value" in out:
            out["map_value"]["entries"].sort(key=lambda e: e["key"])
        return out
    return obj


def bound_check(obj):
    if isinstance(obj, list):
        for x in obj:
            bound_check(x)
    elif isinstance(obj, dict):
        if "projection_id" in obj:
            root = obj["projection_id"]
            if (
                root not in shapes.registry["projections"]
                or obj["schema_digest_sha256"].hex()
                != shapes.registry["projections"][root]["proposed_digest_sha256"]
                or "value" not in obj
                or not shapes.check(root, obj["value"])
                or not (payload_exposure(root, obj["value"])
                        if runtime_payload and root in ("hook.input:before_payload", "hook.output:before_payload")
                        else shapes.exposed(root, obj["value"], runtime_open_leaves))
            ):
                raise ValueError("selected projection")
        for x in obj.values():
            bound_check(x)


runtime_methods = ["resource.list", "resource.read"] if runtime_resource_r2 else [
    "resource.list", "resource.read", "before_run", "before_request",
    "transform_context", *(["before_payload"] if runtime_payload else []),
    "provider.invoke", "after_response", "before_run_end",
] + (["resource.list", "resource.read"] if runtime_changed else [])
runtime_index = 0
runtime_run_id = None
runtime_provider_id = None
structural_expected = "before_compaction"
structural_kind = None
structural_run_id = None
structural_last_provider_id = None
structural_compaction_id = None
structural_navigation_id = None
structural_summaries_left = 0
structural_compaction_summaries = 0
structural_navigation_summaries = 0
runtime_ids = set()
changed_list = None
changed_acked = False
telemetry_batches = 0
telemetry_last_sequence = None
telemetry_flushed = False
resume_phase = 0
resume_methods = (
    "before_run", "before_request", "transform_context", "provider.invoke",
    "after_response", "before_resume", "before_request", "provider.invoke",
    "after_response", "before_run_end",
)
suspended_run_id = None
suspended_provider_id = None
deferred_provider_id = None
snapshot_parent = None
snapshot_phase = 0
resource_phase = 0
resource_parent = None
queue_phase = 0
event_subscription_id = None
event_phase = 0
event_credit = 0
event_gap_last = -1


def await_signal(phase):
    path = os.environ.get("PI_PROFILE_PEER_SIGNAL")
    if not path:
        raise ValueError("snapshot signal missing")
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        try:
            if Path(path).read_text() == phase:
                return
        except FileNotFoundError:
            pass
        time.sleep(.002)
    raise ValueError("snapshot signal timeout: " + phase)


def native_snapshot_request(with_parent):
    method = "snapshot.get"
    ref = next((c for c in granted["selected_capabilities"]
                if c["profile"]["name"] == "host-action.v1" and c["name"] == "actions"), None)
    if ref is None or ref["handler_id"] != "qualified_handler":
        raise ValueError("selected snapshot grant")
    request = {
        "request_id": 1000 if with_parent else 1001,
        "capability_ref": ref,
        "method": method,
        "deadline_after_ms": 2000,
        "initial_stream_credit_bytes": 0,
        "payload": profile_codec.encode(profile_codec.methods[method]["request"],
                                        {"lane": "main", "visibility": "captured_policy"}),
    }
    if with_parent:
        request["parent_request_id"] = snapshot_parent["request_id"]
    send({"request": request})


def native_resource_request(request_id):
    method = "resource.get"
    ref = next((c for c in granted["selected_capabilities"]
                if c["profile"]["name"] == "host-action.v1" and c["name"] == "actions-resource"), None)
    if (ref is None or ref["handler_id"] != "qualified_handler"
            or ref["schema_digest_sha256"].hex() != profile_codec.digests["host-action.v1"]):
        raise ValueError("selected resource action grant")
    payload = {"scope": "allowed", "id": "s", "version": "1"}
    if request_id == 1501:
        if resource_case == "missing":
            payload["id"] = "missing-s"
            del payload["version"]
        if resource_case == "scope":
            payload["scope"] = "wrong-scope"
        if resource_case == "version":
            payload["version"] = "2"
    request = {
        "request_id": request_id, "capability_ref": ref, "method": method,
        "deadline_after_ms": 2000, "initial_stream_credit_bytes": 0,
        "payload": profile_codec.encode(profile_codec.methods[method]["request"], payload),
    }
    if request_id == 1000 or resource_case != "parent":
        request["parent_request_id"] = resource_parent["request_id"]
    send({"request": request})


def native_resource_changed():
    method = "resource.changed"
    ref = next((c for c in granted["selected_capabilities"]
                if c["profile"]["name"] == "resource.v1" and c["name"] == "resources"), None)
    if ref is None:
        raise ValueError("selected resource changed grant")
    send({"request": {
        "request_id": 1500, "capability_ref": ref, "method": method,
        "deadline_after_ms": 2000, "initial_stream_credit_bytes": 0,
        "payload": profile_codec.encode(profile_codec.methods[method]["request"], {"scope": "allowed"}),
    }})


def native_queue_request(request_id, lane, with_parent):
    method = "queue.next_run"
    ref = next((c for c in granted["selected_capabilities"]
                if c["profile"]["name"] == "host-action.v1" and c["name"] == "actions"), None)
    if (ref is None or ref["handler_id"] != "qualified_handler"
            or ref["schema_digest_sha256"].hex() != profile_codec.digests["host-action.v1"]):
        raise ValueError("selected queue grant")
    projection = shapes.registry["projections"]["host.queue_item.v1"]
    item = {
        "projection_id": "host.queue_item.v1",
        "schema_digest_sha256": bytes.fromhex(projection["proposed_digest_sha256"]),
        "value": wire_value({"role": "user", "content": "native queued item", "timestamp": 7}),
    }
    bound_check(item)
    method_codec = profile_codec.methods[method]
    payload = profile_codec.encode(method_codec["request"], {"lane": lane, "item": item})
    decoded = profile_codec.decode(method_codec["request"], payload)
    bound_check(decoded)
    if profile_codec.encode(method_codec["request"], sorted_maps(decoded)) != payload:
        raise ValueError("queue request DTO round trip")
    request = {
        "request_id": request_id, "capability_ref": ref, "method": method,
        "deadline_after_ms": 2000, "initial_stream_credit_bytes": 0, "payload": payload,
    }
    if with_parent:
        request["parent_request_id"] = snapshot_parent["request_id"]
    send({"request": request})


def native_event_request(method):
    global event_credit
    ref = next((c for c in granted["selected_capabilities"]
                if c["profile"]["name"] == "event-sink.v1" and c["name"] == "events"), None)
    if (ref is None or ref["handler_id"] != "qualified_handler"
            or ref["schema_digest_sha256"].hex() != profile_codec.digests["event-sink.v1"]):
        raise ValueError("selected event grant")
    request = ({"scope": "captured_scope"} if method == "event.subscribe"
               else {"subscription_id": event_subscription_id + ("-wrong" if scenario.endswith("wrong-id") else "")})
    send({"request": {
        "request_id": 3000 if method == "event.subscribe" else 3001,
        "capability_ref": ref, "method": method, "deadline_after_ms": 5000,
        "initial_stream_credit_bytes": 0,
        "payload": profile_codec.encode(profile_codec.methods[method]["request"], request),
    }})
    if method == "event.subscribe":
        await_signal("event-credit")
        event_credit = 1048576
        send({"credit": {"request_id": 3000, "additional_bytes": event_credit}})


def runtime_projection(value, projection):
    if not value or value.get("projection_id") != projection or "value" not in value:
        raise ValueError("runtime projection identity")
    from profiles.wire import json_value
    return json_value(value["value"])


def advance_structural(name, event=None):
    global structural_expected, structural_kind, structural_summaries_left
    global structural_compaction_summaries, structural_navigation_summaries
    if name == "before_compaction":
        p = event["preparation"]
        split = p["isSplitTurn"] and len(p["turnPrefixMessages"]) > 0
        structural_summaries_left = (int(not split or len(p["messagesToSummarize"]) > 0)
                                     + int(split))
        if not 1 <= structural_summaries_left <= 2:
            raise ValueError("finite compaction summary selection")
        structural_kind = "compaction"
        structural_expected = None if structural_case == "compaction-decline" else "transform_context"
    elif name == "before_navigation":
        structural_kind = "navigation"
        structural_summaries_left = 1
        structural_expected = None if structural_case == "navigation-decline" else "transform_context"
    elif name == "transform_context":
        structural_expected = "before_request"
    elif name == "before_request":
        structural_expected = "provider.invoke"
    elif name == "provider.invoke":
        structural_expected = "after_response"
    elif name == "after_response":
        structural_summaries_left -= 1
        if structural_kind == "compaction":
            structural_compaction_summaries += 1
        else:
            structural_navigation_summaries += 1
        structural_expected = ("transform_context" if structural_summaries_left > 0 else
                               "before_navigation" if structural_kind == "compaction" else None)
    else:
        raise ValueError("structural phase method")


def validate_runtime(request, method, decoded):
    global runtime_index, runtime_run_id, runtime_provider_id
    global structural_run_id, structural_compaction_id, structural_navigation_id
    global structural_last_provider_id
    global telemetry_batches, telemetry_last_sequence, telemetry_flushed
    global resume_phase, suspended_run_id, suspended_provider_id, deferred_provider_id
    encoded = profile_codec.encode(method["request"], sorted_maps(decoded))
    again = profile_codec.decode(method["request"], encoded)
    bound_check(again)
    if profile_codec.encode(method["request"], sorted_maps(again)) != encoded:
        raise ValueError("runtime DTO round trip")
    name = request["method"]
    structural_tail = runtime_structural and runtime_index >= len(runtime_methods)
    if structural_tail:
        if name != structural_expected:
            raise ValueError(f"structural method order: {name}, wanted {structural_expected}")
    elif runtime_index < len(runtime_methods):
        if name != runtime_methods[runtime_index]:
            raise ValueError("runtime method order: " + name)
        runtime_index += 1
    elif runtime_resume and telemetry_flushed:
        if resume_phase >= len(resume_methods) or name != resume_methods[resume_phase]:
            raise ValueError(f"runtime resume phase {resume_phase}: {name}")
        resume_phase += 1
    elif not runtime_telemetry or telemetry_flushed or (
        name != "telemetry.emit_batch" and
        (name != "telemetry.flush" or telemetry_batches == 0)
    ):
        raise ValueError("runtime telemetry order: " + name)
    request_id = request["request_id"]
    expected_parent = (runtime_provider_id if name == "after_response" else
                       structural_last_provider_id if structural_tail and name in ("transform_context", "before_request") else None)
    parent_invalid = (request.get("parent_request_id") != expected_parent
                      if structural_tail or name == "after_response" else "parent_request_id" in request)
    if (
        not isinstance(request_id, int) or request_id <= 0
        or request_id in runtime_ids or parent_invalid
        or request.get("initial_stream_credit_bytes") != 0
    ):
        raise ValueError("runtime request identity")
    runtime_ids.add(request_id)
    if name == "provider.invoke":
        runtime_provider_id = request_id
        if structural_tail:
            structural_last_provider_id = request_id
        if runtime_resume and telemetry_flushed:
            kind = request.get("effect_key", {}).get("effect_kind")
            if kind == "assistant":
                suspended_provider_id = request_id
            if kind == "deferred":
                deferred_provider_id = request_id
    capability_name = (
        "resources" if method["profile"] == "resource.v1" else
        "remote" if method["profile"] == "provider.v1" else name
    )
    if method["profile"] == "telemetry-sink.v1":
        capability_name = "metrics"
    selected = next(
        (c for c in granted["selected_capabilities"]
         if c["profile"]["name"] == method["profile"]
         and c["name"] == capability_name), None
    )
    if (
        selected is None or selected["handler_id"] != "qualified_handler"
        or codec.encode("CapabilityRef", request["capability_ref"])
        != codec.encode("CapabilityRef", selected)
    ):
        raise ValueError("runtime selected reference")
    if method["profile"] == "telemetry-sink.v1":
        deadline = request.get("deadline_after_ms")
        if (
            not runtime_telemetry or "effect_key" in request or
            not isinstance(deadline, int) or deadline <= 0 or deadline > 5000
        ):
            raise ValueError("runtime telemetry binding")
        if name == "telemetry.emit_batch":
            if (
                decoded["schema_version"] != 1 or
                decoded["sequence"] != telemetry_batches or
                not decoded["records"]
            ):
                raise ValueError("runtime telemetry batch")
            for record in decoded["records"]:
                value = runtime_projection(record, "telemetry.record.v1")
                if "SENTINEL" in json.dumps(value):
                    raise ValueError("runtime telemetry exposure")
            telemetry_last_sequence = decoded["sequence"]
            telemetry_batches += 1
        else:
            if decoded["through_sequence"] != telemetry_last_sequence:
                raise ValueError("runtime telemetry flush sequence")
            telemetry_flushed = True
        return
    if method["profile"] == "resource.v1":
        expected = payload_object(corpus["methods"][name]["request"])
        if runtime_resource_r2 and name == "resource.read":
            expected["version"] = "2"
            expected["expected_digest_sha256"] = bytes.fromhex(sha(b"y"))
        if profile_codec.encode(method["request"], sorted_maps(decoded)) != profile_codec.encode(
            method["request"], expected
        ):
            raise ValueError("runtime resource request")
        return
    if method["profile"] == "hook.v1":
        if decoded["handler_id"] != "qualified_handler":
            raise ValueError("runtime handler")
        value = runtime_projection(decoded["input"], "hook.input:" + name)
        event = value.get("event", {})
        if value.get("name") != name or event.get("lane") != "main":
            raise ValueError("runtime hook event")
        if name == "before_run":
            run_id = event.get("runId")
            if (
                not isinstance(run_id, str) or not run_id.startswith("runtime-id-")
                or not run_id[11:].isdigit()
                or not isinstance(event.get("prompt"), list)
                or "native profile peer dynamic run" not in json.dumps(event["prompt"])
                or event.get("systemPrompt") is not None
                and not isinstance(event["systemPrompt"], str)
            ):
                raise ValueError("runtime before_run fields")
            runtime_run_id = run_id
            if runtime_resume and telemetry_flushed:
                suspended_run_id = run_id
        elif not structural_tail and event.get("runId") != runtime_run_id:
            raise ValueError("runtime hook run")
        if structural_tail:
            key = request.get("effect_key", {})
            expected_kind = "hook" if name in ("before_compaction", "before_navigation") else "summary"
            deadline = request.get("deadline_after_ms")
            if (key.get("operation_id") != event.get("runId")
                    or key.get("effect_kind") != expected_kind
                    or key.get("lane") != "main" or key.get("session_id") != "runtime-session"
                    or not isinstance(deadline, int) or not 0 < deadline <= 5000):
                raise ValueError("structural hook key/deadline")
            if name == "before_compaction":
                p = event.get("preparation", {})
                if (not isinstance(event.get("runId"), str)
                        or not event["runId"].startswith("runtime-id-")
                        or event["runId"] == runtime_run_id or event.get("reason") != "manual"
                        or not all(isinstance(p.get(k), list) for k in
                                   ("messagesToSummarize", "turnPrefixMessages", "retainedTail"))
                        or not isinstance(p.get("fileOps"), dict)
                        or not isinstance(p.get("settings"), dict)):
                    raise ValueError("real compaction preparation")
                structural_compaction_id = event["runId"]
                structural_run_id = event["runId"]
                structural_last_provider_id = None
            elif name == "before_navigation":
                p = event.get("preparation", {})
                if (not isinstance(event.get("runId"), str)
                        or not event["runId"].startswith("runtime-id-")
                        or event["runId"] == structural_compaction_id
                        or not isinstance(event.get("targetId"), str)
                        or not event["targetId"].startswith("runtime-id-")
                        or not isinstance(p.get("messages"), list) or not p["messages"]
                        or not isinstance(p.get("fileOps"), dict)
                        or not isinstance(p.get("totalTokens"), int)):
                    raise ValueError("real navigation preparation")
                structural_navigation_id = event["runId"]
                structural_run_id = event["runId"]
                structural_last_provider_id = None
            elif event.get("runId") != structural_run_id:
                raise ValueError("structural summary callback run")
        if name == "before_resume" and (
            event.get("kind") != "run" or event.get("runId") != suspended_run_id
            or "native profile peer dynamic run deferred source" not in json.dumps(event.get("prompt"))
            or event.get("resumeData") != {"marker": "peer-before-resume"}
            or suspended_provider_id is None or "effect_key" in request
        ):
            raise ValueError("runtime before_resume fields")
        if name == "before_request" and (
            event.get("model", {}).get("id") != "m"
            or event["model"].get("provider") != "remote"
            or event["model"].get("api") != "openai-completions"
            or event.get("step") != (
                "compaction" if structural_tail and structural_kind == "compaction" else
                "branch_summary" if structural_tail else
                "deferred" if runtime_resume and resume_phase == 7 else "assistant"
            )
            or type(event.get("attempt")) is not int or event["attempt"] < 1
        ):
            raise ValueError("runtime before_request fields")
        if name == "before_payload":
            payload = event.get("payload")
            key = request.get("effect_key", {})
            if (
                event.get("model", {}).get("id") != "m"
                or event["model"].get("provider") != "remote"
                or not payload_shape(payload)
                or payload.get("tools") != []
                or not isinstance(payload.get("context", {}).get("messages"), list)
                or payload["context"]["messages"][0].get("role") != "user"
                or not isinstance(payload["context"]["messages"][0].get("content"), str)
                or "native profile peer dynamic run" not in json.dumps(payload["context"]["messages"])
                or not isinstance(request.get("deadline_after_ms"), int)
                or not 0 < request["deadline_after_ms"] <= 5000
                or key.get("operation_id") != runtime_run_id
                or key.get("effect_kind") != "assistant"
            ):
                raise ValueError("runtime before_payload fields")
        if name == "after_response" and (
            event.get("message", {}).get("role") != "assistant"
            or event["message"].get("provider") != "remote"
            or event["message"].get("model") != "m"
        ):
            raise ValueError("runtime after_response fields")
        if name in ("transform_context", "before_run_end") and not isinstance(
            event.get("messages"), list
        ):
            raise ValueError("runtime hook messages")
        if structural_tail:
            advance_structural(name, event)
        return
    if (
        method["profile"] != "provider.v1"
        or decoded.get("model_id") != "m"
        or decoded.get("auth_mode") != "none"
    ):
        raise ValueError("runtime provider identity")
    model = runtime_projection(decoded["model"], "provider.model.v1")
    context = runtime_projection(decoded["context"], "provider.context.v1")
    options = runtime_projection(decoded["options"], "provider.options.v1")
    tools = runtime_projection(decoded["tools"], "provider.tools.v1")
    if (
        model.get("id") != "m" or model.get("provider") != "remote"
        or model.get("api") != "openai-completions"
        or not isinstance(context.get("messages"), list)
        or not (
            isinstance(context["messages"][0].get("content"), str)
            and bool(context["messages"][0]["content"])
            and context["messages"][0].get("role") == "user"
            and request.get("effect_key", {}).get("effect_kind") == "summary"
            and request["effect_key"].get("operation_id") == structural_run_id
            if structural_tail else
            context["messages"][0].get("content") == "native payload replacement"
            if runtime_payload else
            "native-source-handle" in json.dumps(context["messages"])
            if runtime_resume and request.get("effect_key", {}).get("effect_kind") == "deferred"
            else "native profile peer dynamic run" in json.dumps(context["messages"])
        )
        or not isinstance(options, dict)
        or not isinstance(tools, list) or tools
    ):
        raise ValueError("runtime provider fields")
    if runtime_resume and telemetry_flushed:
        key = request.get("effect_key", {})
        if (
            key.get("operation_id") != suspended_run_id
            or key.get("effect_kind") not in ("assistant", "deferred")
            or key.get("effect_kind") == "deferred" and (
                suspended_provider_id is None
                or "native-source-handle" not in json.dumps(context["messages"])
                or options.get("deferred") is not False
            )
        ):
            raise ValueError("runtime deferred provider binding")
    if structural_tail:
        advance_structural(name)


hooks = [
    name
    for name in corpus["methods"]
    if profile_codec.methods[name]["profile"] == "hook.v1"
]
entries = (
    [{"profile": "host-action.v1", "name": "actions-resource"}]
    + [{"profile": "hook.v1", "name": name} for name in hooks]
    + [
        {"profile": p + ".v1", "name": n}
        for p, n in [
            ("provider", "remote"),
            ("resource", "resources"),
            ("host-action", "actions"),
            ("event-sink", "events"),
            ("telemetry-sink", "metrics"),
        ]
    ]
)
profile_range = lambda name: {
    "name": name,
    "major": 1,
    "min_minor": minor,
    "max_minor": minor,
}
send(
    {
        "hello": {
            "protocol": {"major": 1, "min_minor": core, "max_minor": core},
            "supported_profiles": [
                profile_range(p) for p in dict.fromkeys(e["profile"] for e in entries)
            ],
        }
    }
)
hello = recv()
if hello.get("hello_ack", {}).get("selected_protocol", {}).get("minor") != core:
    raise ValueError("HelloAck")
caps = [
    {
        "kind": e["profile"][:-3],
        "name": e["name"],
        "profile": profile_range(e["profile"]),
        "handler_id": "qualified_handler",
        "replay": 1,
        "execution": (
            2
            if e["profile"] == "provider.v1"
            else 3 if e["profile"] in ("event-sink.v1", "telemetry-sink.v1") else 1
        ),
        "required": True,
        "supported_descriptor_digests": [
            {
                "minor": minor,
                "descriptor_digest_sha256": bytes.fromhex(
                    profile_codec.digests[e["profile"]]
                ),
            }
        ],
    }
    for e in entries
]
send(
    {
        "manifest": {
            "declared_extension_id": "peer-claimed",
            "extension_version": "1",
            "protocol": {"major": 1, "min_minor": core, "max_minor": core},
            "capabilities": caps,
            "permissions": [
                {
                    "name": name,
                    "scope": scope,
                    "required": scenario != "host-ungranted" or name != "queue.steer",
                }
                for name, scope in corpus["exposure"]["permission_scopes"]
            ],
        }
    }
)
granted = recv().get("grant")
if (
    not granted
    or granted["authenticated_extension_id"] != "peer-a"
    or granted["connection_generation"] != 7
):
    raise ValueError("grant identity")
for cap in granted["selected_capabilities"]:
    if (
        cap["profile"]["minor"] != minor
        or cap["schema_digest_sha256"].hex()
        != profile_codec.digests[cap["profile"]["name"]]
    ):
        raise ValueError("grant digest")
send({"ready": {}})
streams, host_index, parent = {}, 0, None
passive_index, passive_sequence, passive_credit, passive_validated = 0, 0, 1048576, 0
passive_names = (
    ["event.subscribe", "event.resync"]
    if scenario == "event-gap"
    else ["resource.changed", "event.subscribe", "event.resync"]
)


def passive_request():
    if passive_index >= len(passive_names):
        return
    method = passive_names[passive_index]
    m = profile_codec.methods[method]
    ref = next(
        c
        for c in granted["selected_capabilities"]
        if c["profile"]["name"] == m["profile"]
    )
    request = {
        "request_id": 2000 + passive_index,
        "capability_ref": ref,
        "method": method,
        "payload": profile_codec.encode(
            m["request"], payload_object(corpus["methods"][method]["request"])
        ),
    }
    if m["mode"] == 2:
        request["initial_stream_credit_bytes"] = 1048576
    send({"request": request})


def native_changed_request():
    method = "resource.changed"
    ref = next(
        (c for c in granted["selected_capabilities"]
         if c["profile"]["name"] == "resource.v1" and c["name"] == "resources"),
        None,
    )
    if ref is None or changed_list is None:
        raise ValueError("native changed state")
    request = payload_object(corpus["methods"][method]["request"])
    if scenario == "runtime-native-changed-wrong-scope":
        request["scope"] = "wrong-scope"
    send({"request": {
        "request_id": 2000,
        "capability_ref": ref,
        "method": method,
        "initial_stream_credit_bytes": 0,
        "payload": profile_codec.encode(profile_codec.methods[method]["request"], request),
    }})


if scenario in ("passive-origin", "event-gap"):
    passive_request()


def result_for(method):
    v = (
        corpus.get("scenario_payloads", {})
        .get(scenario, {})
        .get(method, corpus["methods"][method])
    )
    return payload_object(
        v["default_terminal"]
        if (scenario == "none" or scenario.startswith("runtime-native")) and profile_codec.methods[method]["profile"] == "hook.v1"
        else v.get("positive_terminal", v.get("terminal"))
    )


def terminal(request, decoded=None):
    global once_done, resource_phase
    m = profile_codec.methods[request["method"]]
    obj = result_for(request["method"])
    if runtime_structural and request["method"] in ("before_compaction", "before_navigation"):
        obj = {"output": payload_object(corpus["methods"][request["method"]]["positive_terminal"]["output"])}
        if (request["method"] == "before_compaction" and structural_case == "compaction-decline"
                or request["method"] == "before_navigation" and structural_case == "navigation-decline"):
            obj["output"]["value"] = wire_value({"name": request["method"], "result": {"decline": True}})
    if runtime_resource_r2 and request["method"] == "resource.list":
        obj["revision"] = "r2"
        obj["resources"][0]["version"] = "2"
        obj["resources"][0]["digest_sha256"] = bytes.fromhex(sha(b"y"))
    if runtime_resource_r2 and request["method"] == "resource.read":
        obj["digest_sha256"] = bytes.fromhex(sha(b"y"))
        obj["text"] = "y"
    if runtime_payload and request["method"] == "before_payload":
        value = runtime_projection(decoded["input"], "hook.input:before_payload")
        replacement = json.loads(json.dumps(value["event"]["payload"]))
        replacement["context"]["messages"][0]["content"] = "native payload replacement"
        obj = {"output": payload_object(corpus["methods"]["before_payload"]["positive_terminal"]["output"])}
        obj["output"]["value"] = wire_value({
            "name": "before_payload", "result": {"payload":
                {"request": "malformed"} if scenario == "runtime-native-before-payload-malformed" else replacement},
        })
    if runtime_resume and telemetry_flushed and request["method"] == "before_run":
        obj["output"] = payload_object(corpus["methods"]["before_run"]["positive_terminal"]["output"])
        obj["output"]["value"] = wire_value({
            "name": "before_run", "result": {"resumeData": {"marker": "peer-before-resume"}}
        })
    if (
        runtime_resume and telemetry_flushed and request["method"] == "provider.invoke"
        and request.get("effect_key", {}).get("effect_kind") == "assistant"
    ):
        message = json_value(obj["assistant_message"]["value"])
        message["stopReason"] = "deferred"
        message["deferred"] = {
            "provider": "remote", "api": "openai-completions", "modelId": "m",
            "id": "native-source-handle",
        }
        obj["assistant_message"]["value"] = wire_value(message)
    if runtime_telemetry and request["method"] == "telemetry.emit_batch":
        if decoded is None:
            raise ValueError("runtime telemetry payload")
        obj = {"accepted": len(decoded["records"]), "dropped": 0}
        if scenario == "runtime-native-telemetry-bad-ack" and telemetry_batches == 1:
            obj["accepted"] += 1
    if runtime_telemetry and request["method"] == "telemetry.flush":
        if decoded is None:
            raise ValueError("runtime telemetry flush payload")
        obj = {"flushed_through": decoded["through_sequence"]}
        if scenario == "runtime-native-telemetry-bad-flush":
            obj["flushed_through"] += 1
    if scenario == "pending" and request["method"] == "provider.invoke":
        next(
            e
            for e in obj["assistant_message"]["value"]["map_value"]["entries"]
            if e["key"] == "stopReason"
        )["value"] = {"text_value": "pending"}
    if scenario == "wrong-projection" and request["method"] == "provider.invoke":
        obj["assistant_message"]["projection_id"] = "peer-alias"
    if scenario == "bad-ack" and request["method"] == "telemetry.emit_batch":
        obj["accepted"] = 2
    if scenario == "bad-flush" and request["method"] == "telemetry.flush":
        obj["flushed_through"] = 2
    if scenario == "bad-resource" and request["method"] == "resource.read":
        obj["text"] = "y"
    if scenario == "sentinel" and request["method"] == "before_tool":
        next(
            e
            for e in obj["output"]["value"]["map_value"]["entries"]
            if e["key"] == "result"
        )["value"] = {
            "map_value": {
                "entries": [
                    {
                        "key": "args",
                        "value": {
                            "map_value": {
                                "entries": [
                                    {
                                        "key": "authorization",
                                        "value": {"text_value": "SENTINEL"},
                                    }
                                ]
                            }
                        },
                    }
                ]
            }
        }
    payload = profile_codec.encode(m["terminal"], obj)
    if runtime_resume or runtime_payload or runtime_structural:
        round_trip = profile_codec.decode(m["terminal"], payload)
        if scenario != "runtime-native-before-payload-malformed" or request["method"] != "before_payload":
            bound_check(round_trip)
        if profile_codec.encode(m["terminal"], round_trip) != payload:
            raise ValueError("runtime terminal DTO round trip")
    end = {"request_id": request["request_id"]}
    if scenario in ("error", "unknown-enum"):
        end["error"] = {
            "code": 777 if scenario == "unknown-enum" else 14,
            "safe_message": "handler failed",
        }
    else:
        end["result"] = payload
    arm = "stream_end" if m["mode"] == 2 else "response"
    send({arm: end})
    if runtime_resource_r2 and request["method"] == "resource.read":
        once_done = True
    if runtime_event and request["method"] == "telemetry.flush":
        await_signal("event-subscribe")
        native_event_request("event.subscribe")
    if runtime_changed and request["method"] == "resource.read" and runtime_index == 10:
        if not changed_acked:
            raise ValueError("native changed acknowledgement missing")
    if scenario == "duplicate":
        send({arm: end})
    if scenario == "single-provider":
        once_done = True


def progress(request):
    v = corpus["methods"][request["method"]]
    return (
        [v["first_item"], v["event_item"], v["gap_item"]]
        if request["method"] == "event.subscribe" and scenario == "event-gap"
        else [v.get("progress", v.get("first_item"))]
    )


def flush(state):
    while state["items"]:
        payload = profile_codec.encode(
            profile_codec.methods[state["r"]["method"]]["progress"],
            payload_object(state["items"][0]),
        )
        item = {
            "request_id": state["r"]["request_id"],
            "sequence": (
                state["sequence"] + 1 if scenario == "sequence" else state["sequence"]
            ),
            "payload": payload,
        }
        charge = len(codec.encode("Envelope", {"stream_item": item}))
        if state["credit"] < charge:
            break
        state["credit"] -= charge
        state["sequence"] += 1
        state["items"].pop(0)
        send({"stream_item": item})
    if not state["items"] or scenario not in ("credit", "cancel-progress", "silence"):
        terminal(state["r"])
        streams.pop(state["r"]["request_id"], None)


def host_request():
    names = [
        "snapshot.get",
        "resource.get",
        "queue.steer",
        "queue.follow_up",
        "queue.next_run",
        "operation.abort",
    ]
    method = (
        names[host_index]
        if scenario == "host-actions" and host_index < len(names)
        else (
            None
            if scenario == "host-actions"
            else (
                "secret.resolve"
                if scenario == "host-secret"
                else (
                    "unfamiliar.method" if scenario == "host-unknown" else "queue.steer"
                )
            )
        )
    )
    if method is None:
        terminal(parent)
        return
    ref = next(
        c
        for c in granted["selected_capabilities"]
        if c["profile"]["name"] == "host-action.v1"
        and c["name"] == ("actions-resource" if method == "resource.get" else "actions")
    )
    request = {
        "request_id": 1000 + host_index,
        "capability_ref": ref,
        "method": method,
        "deadline_after_ms": 100,
        "payload": (
            profile_codec.encode(
                profile_codec.methods[method]["request"],
                payload_object(corpus["methods"][method]["request"]),
            )
            if method in profile_codec.methods
            else b""
        ),
    }
    if scenario != "host-parent":
        request["parent_request_id"] = parent["request_id"]
    if scenario == "host-reference":
        request["capability_ref"] = dict(ref, authenticated_extension_id="wrong")
    send({"request": request})


while not once_done:
    e = recv()
    if e is None:
        break
    if "request" in e:
        request = e["request"]
        m = profile_codec.methods[request["method"]]
        decoded = profile_codec.decode(m["request"], request["payload"])
        bound_check(decoded)
        if scenario == "runtime-native" or runtime_changed or runtime_payload or runtime_resource or runtime_resource_r2 or runtime_structural:
            validate_runtime(request, m, decoded)
        else:
            expected = payload_object(
                corpus.get("scenario_payloads", {})
                .get(scenario, {})
                .get(request["method"], corpus["methods"][request["method"]])["request"]
            )
            if profile_codec.encode(
                m["request"], sorted_maps(decoded)
            ) != profile_codec.encode(m["request"], expected):
                raise ValueError("request semantic mismatch")
        if (
            request["capability_ref"]["profile"]["minor"] != minor
            or request["capability_ref"]["schema_digest_sha256"].hex()
            != profile_codec.digests[m["profile"]]
        ):
            raise ValueError("request reference")
        if runtime_changed and runtime_index == 9 and request["method"] == "resource.list":
            changed_list = request
            native_changed_request()
            continue
        if runtime_snapshot and request["method"] == "provider.invoke":
            if snapshot_parent is not None or runtime_index != 6 or not request.get("effect_key", {}).get("operation_id"):
                raise ValueError("snapshot provider parent")
            snapshot_parent = request
            await_signal("actions")
            native_snapshot_request(True)
            continue
        if runtime_resource and request["method"] == "provider.invoke":
            if resource_parent is not None or runtime_index != 6 or not request.get("effect_key", {}).get("operation_id"):
                raise ValueError("resource provider parent")
            resource_parent = request
            await_signal("actions")
            native_resource_request(1000)
            continue
        if scenario.startswith("host-") and request["method"] == "provider.invoke":
            parent = request
            host_request()
            continue
        if scenario == "silence":
            continue
        if m["mode"] == 2:
            state = {
                "r": request,
                "credit": (
                    1048576
                    if scenario == "overcredit"
                    else request.get("initial_stream_credit_bytes", 0)
                ),
                "sequence": 0,
                "items": progress(request),
            }
            streams[request["request_id"]] = state
            if scenario not in ("cancel", "cancel-progress", "loss"):
                flush(state)
        else:
            terminal(request, decoded)
    elif "credit" in e:
        cr = e["credit"]
        state = streams[cr["request_id"]]
        state["credit"] += cr["additional_bytes"]
        if state["credit"] > granted["limits"]["max_stream_credit_bytes"]:
            raise ValueError("credit ceiling")
        flush(state)
    elif "cancel" in e:
        cr = e["cancel"]
        codec.selected_enum("CancelReason", cr["reason"])
        state = streams.get(cr["request_id"])
        if state:
            if scenario == "cancel-progress":
                flush(state)
            else:
                terminal(state["r"])
                streams.pop(cr["request_id"], None)
    elif runtime_resource and "response" in e and resource_parent is not None:
        response = e["response"]
        if resource_phase in (0, 4):
            expected_id = 1000 if resource_phase == 0 else 1501
            if response["request_id"] != expected_id:
                raise ValueError("resource action response identity")
            if resource_phase == 4 and resource_case != "positive":
                code = {"missing": 11, "scope": 10, "version": 12, "parent": 8}[resource_case]
                if (response.get("error", {}).get("code") != code
                        or response.get("error", {}).get("safe_message") != "request failed"
                        or "result" in response):
                    raise ValueError("resource action typed refusal")
            else:
                if "result" not in response:
                    raise ValueError("resource action denied")
                m = profile_codec.methods["resource.get"]
                decoded = profile_codec.decode(m["terminal"], response["result"])
                bound_check(decoded)
                if profile_codec.encode(m["terminal"], sorted_maps(decoded)) != response["result"]:
                    raise ValueError("resource Result DTO round trip")
                value = runtime_projection(decoded["snapshot"], "host.resource_snapshot.v1")
                if value != {"id": "s", "version": "1", "kind": "skill", "value": "x", "digest": sha(b"x")}:
                    raise ValueError("captured r1 resource content")
            if resource_phase == 0:
                resource_phase = 1
                await_signal("resource-changed")
                native_resource_changed()
                resource_phase = 2
            else:
                resource_phase = 5
                await_signal("provider")
                terminal(resource_parent)
                resource_parent = None
        elif resource_phase == 2:
            if response["request_id"] != 1500 or "result" not in response:
                raise ValueError("resource.changed response identity")
            m = profile_codec.methods["resource.changed"]
            decoded = profile_codec.decode(m["terminal"], response["result"])
            bound_check(decoded)
            if profile_codec.encode(m["terminal"], sorted_maps(decoded)) != response["result"]:
                raise ValueError("resource.changed Ack round trip")
            resource_phase = 3
            await_signal("resource-second")
            native_resource_request(1501)
            resource_phase = 4
        else:
            raise ValueError("resource response phase")
    elif runtime_snapshot and "response" in e and snapshot_parent is not None and snapshot_phase < 2:
        response = e["response"]
        if response["request_id"] != 1000 + snapshot_phase:
            raise ValueError("snapshot response identity")
        if snapshot_phase == 0:
            if "result" not in response:
                raise ValueError("snapshot action denied")
            m = profile_codec.methods["snapshot.get"]
            decoded = profile_codec.decode(m["terminal"], response["result"])
            bound_check(decoded)
            if profile_codec.encode(m["terminal"], sorted_maps(decoded)) != response["result"]:
                raise ValueError("snapshot result DTO round trip")
            value = runtime_projection(decoded["filtered_snapshot"], "host.filtered_snapshot.v1")
            if (value.get("operation", {}).get("id") != snapshot_parent["effect_key"]["operation_id"]
                    or value.get("lane") != "main" or "SENTINEL" in json.dumps(value)):
                raise ValueError("snapshot live filtered operation")
            snapshot_phase = 1
            native_snapshot_request(False)
        else:
            if response.get("error", {}).get("code") != 8 or response["error"].get("safe_message") != "request failed" or "result" in response:
                raise ValueError("snapshot missing-parent refusal")
            snapshot_phase = 2
            if runtime_queue:
                queue_phase = 1
                native_queue_request(1002, "main", True)
            else:
                await_signal("provider")
                terminal(snapshot_parent)
                snapshot_parent = None
    elif runtime_queue and "response" in e and snapshot_parent is not None and 0 < queue_phase < 5:
        response = e["response"]
        expected_id = 1002 + (0 if queue_phase == 1 else 1 if queue_phase == 3 else 2)
        if response["request_id"] != expected_id:
            raise ValueError("queue response identity")
        if queue_phase == 1:
            if "result" not in response:
                raise ValueError("native queue action denied")
            method = profile_codec.methods["queue.next_run"]
            decoded = profile_codec.decode(method["terminal"], response["result"])
            bound_check(decoded)
            if (not decoded["accepted"] or not decoded["item_id"]
                    or profile_codec.encode(method["terminal"], sorted_maps(decoded)) != response["result"]):
                raise ValueError("native queue result DTO round trip")
            queue_phase = 2
            await_signal("queue-denials")
            queue_phase = 3
            native_queue_request(1003, "main", False)
        else:
            code = 8 if queue_phase == 3 else 10
            if (response.get("error", {}).get("code") != code
                    or response["error"].get("safe_message") != "request failed"
                    or "result" in response):
                raise ValueError("native queue refusal")
            if queue_phase == 3:
                await_signal("queue-wrong-lane")
                queue_phase = 4
                native_queue_request(1004, "other", True)
            else:
                queue_phase = 5
                await_signal("provider")
                terminal(snapshot_parent)
                snapshot_parent = None
    elif runtime_event and "stream_item" in e:
        item = e["stream_item"]
        charge = len(codec.encode("Envelope", e))
        if (item["request_id"] != 3000 or item["sequence"] != event_phase
                or event_credit < charge):
            raise ValueError("event sequence or charged credit")
        event_credit -= charge
        m = profile_codec.methods["event.subscribe"]
        decoded = profile_codec.decode(m["progress"], item["payload"])
        bound_check(decoded)
        if profile_codec.encode(m["progress"], sorted_maps(decoded)) != item["payload"]:
            raise ValueError("event item DTO round trip")
        if event_phase == 0:
            snapshot = decoded.get("snapshot")
            if (not snapshot or not snapshot.get("subscription_id")
                    or snapshot["next_event_sequence"] != 0
                    or not isinstance(snapshot["snapshot_revision"], int)
                    or runtime_projection(snapshot["filtered_snapshot"], "event.filtered_snapshot.v1").get("lane") != "main"):
                raise ValueError("native event initial snapshot")
            event_subscription_id = snapshot["subscription_id"]
            sys.stderr.write(json.dumps({"stage": "event-snapshot", "subscription_id": event_subscription_id}) + "\n")
            sys.stderr.flush()
        elif event_phase == 1:
            gap = decoded.get("gap")
            if (not gap or gap["subscription_id"] != event_subscription_id
                    or gap["first_dropped"] > gap["last_dropped"]
                    or gap["first_dropped"] < 0):
                raise ValueError("native reserved Gap")
            event_gap_last = gap["last_dropped"]
            sys.stderr.write(json.dumps({"stage": "event-gap", "last_dropped": event_gap_last}) + "\n")
            sys.stderr.flush()
            await_signal("event-resync")
            native_event_request("event.resync")
        else:
            raise ValueError("stale event after Gap")
        event_phase += 1
    elif runtime_event and "response" in e and e["response"]["request_id"] == 3001:
        response = e["response"]
        if event_phase != 2:
            raise ValueError("event resync before Gap")
        if scenario.endswith("wrong-id"):
            if (response.get("error", {}).get("code") != 8
                    or response["error"].get("safe_message") != "request failed"
                    or "result" in response):
                raise ValueError("wrong subscription refusal")
        else:
            if "result" not in response:
                raise ValueError("native event resync Result")
            m = profile_codec.methods["event.resync"]
            decoded = profile_codec.decode(m["terminal"], response["result"])
            bound_check(decoded)
            if profile_codec.encode(m["terminal"], sorted_maps(decoded)) != response["result"]:
                raise ValueError("event resync DTO round trip")
            snapshot = decoded["snapshot"]
            value = runtime_projection(snapshot["filtered_snapshot"], "event.filtered_snapshot.v1")
            if (snapshot["subscription_id"] != event_subscription_id
                    or snapshot["next_event_sequence"] <= event_gap_last
                    or not isinstance(value.get("queues", {}).get("nextRun"), list)
                    or len(value["queues"]["nextRun"]) != 2):
                raise ValueError("atomic event baseline")
        event_phase = 3
        sys.stderr.write(json.dumps({"stage": "event-resync-result"}) + "\n")
        sys.stderr.flush()
    elif runtime_changed and "response" in e and changed_list is not None:
        response = e["response"]
        method = "resource.changed"
        m = profile_codec.methods[method]
        if response["request_id"] != 2000 or "result" not in response:
            raise ValueError("native changed response")
        decoded = profile_codec.decode(m["terminal"], response["result"])
        bound_check(decoded)
        if profile_codec.encode(m["terminal"], sorted_maps(decoded)) != profile_codec.encode(
            m["terminal"], result_for(method)
        ):
            raise ValueError("native changed acknowledgement")
        changed_acked = True
        pending = changed_list
        changed_list = None
        terminal(pending)
    elif "response" in e and parent:
        if scenario == "host-actions":
            response = e["response"]
            if "result" not in response:
                raise ValueError("host-action denied")
            names = [
                "snapshot.get",
                "resource.get",
                "queue.steer",
                "queue.follow_up",
                "queue.next_run",
                "operation.abort",
            ]
            name = names[host_index]
            decoded = profile_codec.decode(
                profile_codec.methods[name]["terminal"], response["result"]
            )
            bound_check(decoded)
            if profile_codec.encode(
                profile_codec.methods[name]["terminal"], sorted_maps(decoded)
            ) != profile_codec.encode(
                profile_codec.methods[name]["terminal"], result_for(name)
            ):
                raise ValueError("host result")
            host_index += 1
            host_request()
        else:
            terminal(parent)
    elif scenario in ("passive-origin", "event-gap") and "stream_item" in e:
        method = passive_names[passive_index]
        m, item = profile_codec.methods[method], e["stream_item"]
        if (
            item["request_id"] != 2000 + passive_index
            or item["sequence"] != passive_sequence
        ):
            raise ValueError("passive sequence")
        passive_sequence += 1
        charge = len(codec.encode("Envelope", e))
        if charge > passive_credit:
            raise ValueError("passive credit")
        passive_credit -= charge
        decoded = profile_codec.decode(m["progress"], item["payload"])
        bound_check(decoded)
        v = corpus["methods"][method]
        expected = (
            [v["first_item"], v["event_item"], v["gap_item"]][item["sequence"]]
            if scenario == "event-gap"
            else v["first_item"]
        )
        if profile_codec.encode(
            m["progress"], sorted_maps(decoded)
        ) != profile_codec.encode(m["progress"], payload_object(expected)):
            raise ValueError("passive projection")
        passive_validated += 1
    elif scenario in ("passive-origin", "event-gap") and (
        "response" in e or "stream_end" in e
    ):
        end = e.get("response", e.get("stream_end"))
        method = passive_names[passive_index]
        m = profile_codec.methods[method]
        if end["request_id"] != 2000 + passive_index or "result" not in end:
            raise ValueError("passive terminal")
        decoded = profile_codec.decode(m["terminal"], end["result"])
        bound_check(decoded)
        if profile_codec.encode(
            m["terminal"], sorted_maps(decoded)
        ) != profile_codec.encode(m["terminal"], result_for(method)):
            raise ValueError("passive terminal value")
        passive_validated += 1
        passive_index += 1
        passive_request()
    elif "go_away" in e:
        codec.selected_enum("GoAwayReason", e["go_away"]["reason"])
        break
    else:
        raise ValueError("unexpected envelope")
if runtime_resume and (
    runtime_index != len(runtime_methods) or not changed_acked or not telemetry_flushed
    or resume_phase != len(resume_methods) or suspended_run_id is None
    or suspended_provider_id is None or deferred_provider_id is None
):
    raise ValueError("incomplete native resume history")
if runtime_event and (
    event_phase != 3 or not changed_acked or not telemetry_flushed
    or snapshot_phase != 2 or runtime_index != len(runtime_methods)
):
    raise ValueError("incomplete native event history")
if runtime_resource and (resource_phase != 5 or resource_parent is not None
                         or runtime_index != len(runtime_methods)):
    raise ValueError("incomplete native resource history")
if runtime_queue and (queue_phase != 5 or snapshot_phase != 2
                      or runtime_index != len(runtime_methods)):
    raise ValueError("incomplete native queue history")
if runtime_structural and (runtime_index != len(runtime_methods)
                           or structural_expected is not None or structural_compaction_id is None
                           or structural_case != "compaction-decline" and
                           (structural_navigation_id is None or structural_compaction_summaries < 1)
                           or structural_case == "positive" and structural_navigation_summaries != 1):
    raise ValueError("incomplete native structural history")
sys.stderr.write(
    json.dumps(
        {
            "language": "Python",
            "core": core,
            "minor": minor,
            "scenario": scenario,
            "inbound": inbound,
            "outbound": outbound,
            "passiveValidated": passive_validated,
            "codec_messages": len(codec.messages),
            "descriptor_sha256": sha(
                (
                    Path(repo)
                    / f"crates/pi-extension-protocol/schema/descriptors/common-1.{core}.pb"
                ).read_bytes()
            ),
        }
    )
    + "\n"
)
