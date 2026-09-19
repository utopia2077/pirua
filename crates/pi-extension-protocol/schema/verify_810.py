#!/usr/bin/env python3
"""Advisory 810 descriptor, method-table, bounded echo and frame checks."""
import argparse
import hashlib
import json
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent
PROFILES = (
    ("tool", "tool.v1", "ToolProfile", {"tool.invoke"}),
    ("hook", "hook.v1", "HookProfile", {
        "before_run", "before_resume", "before_run_end", "transform_context",
        "before_request", "before_payload", "after_response", "before_tool",
        "after_tool", "before_compaction", "before_navigation",
    }),
    ("provider", "provider.v1", "ProviderProfile", {"provider.invoke"}),
    ("event_sink", "event-sink.v1", "EventSinkProfile", {"event.subscribe", "event.resync"}),
    ("resource", "resource.v1", "ResourceProfile", {"resource.list", "resource.read", "resource.changed"}),
    ("host_action", "host-action.v1", "HostActionProfile", {
        "snapshot.get", "resource.get", "queue.steer", "queue.follow_up",
        "queue.next_run", "operation.abort",
    }),
    ("telemetry_sink", "telemetry-sink.v1", "TelemetrySinkProfile", {
        "telemetry.emit_batch", "telemetry.flush",
    }),
)
ROOTS = ("common",) + tuple(item[0] for item in PROFILES)


def read_set(root, minor):
    raw = (ROOT / "descriptors" / f"{root}-{minor}.pb").read_bytes()
    fds = descriptor_pb2.FileDescriptorSet.FromString(raw)
    pool = descriptor_pool.DescriptorPool()
    for file in fds.file:
        pool.Add(file)
    return raw, fds, pool


def method_table(root, minor, service):
    _, _, pool = read_set(root, minor)
    options_type = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("google.protobuf.MethodOptions")
    )
    extensions = {
        name: pool.FindExtensionByName("pi.extension.v1." + name)
        for name in (
            "wire_method", "wire_direction", "wire_mode", "wire_stream_item_type"
        )
    }
    entries = {}
    for method in pool.FindServiceByName("pi.extension.v1." + service).methods:
        options = options_type.FromString(method.GetOptions().SerializeToString())
        wire_name = options.Extensions[extensions["wire_method"]]
        direction = options.Extensions[extensions["wire_direction"]]
        mode = options.Extensions[extensions["wire_mode"]]
        item = options.Extensions[extensions["wire_stream_item_type"]]
        assert wire_name and wire_name not in entries
        assert direction in (1, 2) and mode in (1, 2)
        if mode == 2:
            assert item and pool.FindMessageTypeByName(item.lstrip("."))
        else:
            assert not item
        entries[wire_name] = {
            "direction": {1: "core_to_extension", 2: "extension_to_core"}[direction],
            "mode": {1: "unary", 2: "stream"}[mode],
            "request": method.input_type.name,
            "item": item.rsplit(".", 1)[-1] if item else None,
            "terminal": method.output_type.name,
        }
    return dict(sorted(entries.items()))


def fields_by_name(fds):
    return {
        (file.name, message.name): {
            field.name: (
                field.number, field.type, field.label,
                field.type_name, field.proto3_optional,
            )
            for field in message.field
        }
        for file in fds.file
        for message in file.message_type
    }


def enums_by_name(fds):
    return {
        (file.name, enum.name): {value.name: value.number for value in enum.value}
        for file in fds.file
        for enum in file.enum_type
    }


def assert_additive(old, new, root):
    old_fields, new_fields = fields_by_name(old), fields_by_name(new)
    for key, fields in old_fields.items():
        assert key in new_fields, (root, key)
        old_numbers = {value[0] for value in fields.values()}
        for name, value in fields.items():
            assert new_fields[key].get(name) == value, (root, key, name)
        for name, value in new_fields[key].items():
            if name not in fields:
                assert value[0] not in old_numbers and value[4], (root, key, name)
    old_enums, new_enums = enums_by_name(old), enums_by_name(new)
    for key, values in old_enums.items():
        assert key in new_enums
        for name, number in values.items():
            assert new_enums[key].get(name) == number, (root, key, name)


def varint(number):
    out = bytearray()
    while number >= 128:
        out.append((number & 127) | 128)
        number >>= 7
    out.append(number)
    return bytes(out)


def framed(body):
    prefix = varint(len(body))
    return {
        "body_bytes": len(body),
        "prefix_hex": prefix.hex(),
        "body_hex": body.hex(),
        "frame_hex": (prefix + body).hex(),
    }


def reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


def bounded_ascii_json(value):
    if isinstance(value, dict):
        for key, child in value.items():
            assert key.isascii() and all(32 <= ord(c) < 127 for c in key)
            bounded_ascii_json(child)
    elif isinstance(value, list):
        for child in value:
            bounded_ascii_json(child)
    elif isinstance(value, str):
        assert value.isascii() and all(32 <= ord(c) < 127 for c in value)
    elif isinstance(value, int) and not isinstance(value, bool):
        assert abs(value) <= 9007199254740991
    else:
        assert value is None or isinstance(value, bool), type(value)


def echo_schema(name):
    path = ROOT / "dynamic" / (name.replace(":", "_") + ".schema.json")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)
    bounded_ascii_json(value)
    canonical = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return value, hashlib.sha256(canonical).digest(), hashlib.sha256(path.read_bytes()).hexdigest()


def set_wire(target, value):
    if value is None:
        target.null_value.SetInParent()
    elif isinstance(value, bool):
        target.bool_value = value
    elif isinstance(value, int):
        target.integer_value = value
    elif isinstance(value, str):
        target.text_value = value
    elif isinstance(value, list):
        for child in value:
            set_wire(target.list_value.items.add(), child)
    elif isinstance(value, dict):
        for key, child in sorted(value.items()):
            entry = target.map_value.entries.add()
            entry.key = key
            set_wire(entry.value, child)
    else:
        raise ValueError(type(value))


def class_from(pool, name):
    return message_factory.GetMessageClass(
        pool.FindMessageTypeByName("pi.extension.v1." + name)
    )


def encode_echo():
    _, _, pool_10 = read_set("tool", "1.0")
    _, _, pool_11 = read_set("tool", "1.1")
    EchoRequest = class_from(pool_10, "InvokeToolRequest")
    EchoProgress = class_from(pool_10, "ToolProgress")
    EchoResult = class_from(pool_10, "InvokeToolResult")
    Envelope = class_from(pool_10, "Envelope")
    EchoProgress11 = class_from(pool_11, "ToolProgress")
    schemas = {}
    for name in ("tool.arguments:echo", "tool.progress:echo", "tool.result:echo"):
        schema, digest, file_sha = echo_schema(name)
        schemas[name] = {
            "schema": schema, "digest": digest, "file_sha256": file_sha,
        }
    source = json.loads(
        (ROOT / "fixtures" / "echo-source.json").read_text(encoding="utf-8")
    )
    values = {entry["projection"]: entry["value"] for entry in source["positive"]}
    for name, value in values.items():
        assert Draft202012Validator(schemas[name]["schema"]).is_valid(value), name
    for entry in source["negative"]:
        assert not Draft202012Validator(
            schemas[entry["projection"]]["schema"]
        ).is_valid(entry["value"]), entry["id"]

    def bind(target, name, value):
        target.projection_id = name
        target.schema_digest_sha256 = schemas[name]["digest"]
        set_wire(target.value, value)

    request = EchoRequest()
    request.tool_id = "echo"
    request.tool_call_id = "call-1"
    bind(request.arguments, "tool.arguments:echo", values["tool.arguments:echo"])
    progress = EchoProgress()
    bind(progress.event, "tool.progress:echo", values["tool.progress:echo"])
    result = EchoResult()
    bind(result.raw_result, "tool.result:echo", values["tool.result:echo"])
    progress11 = EchoProgress11()
    progress11.event.ParseFromString(progress.event.SerializeToString(deterministic=True))
    progress11.percent_complete = 0
    assert progress11.HasField("percent_complete")

    base = Envelope()
    base.request.request_id = 17
    base.request.method = "tool.invoke"
    base.request.capability_ref.authenticated_extension_id = "peer-a"
    base.request.capability_ref.kind = "tool"
    base.request.capability_ref.name = "echo"
    base.request.capability_ref.profile.name = "tool.v1"
    base.request.capability_ref.profile.major = 1
    base.request.capability_ref.profile.minor = 0
    raw_descriptor = read_set("tool", "1.0")[0]
    selected_profile_digest = hashlib.sha256(
        b"tool.v1\0" + b"1.0\0" + raw_descriptor
    ).digest()
    base.request.capability_ref.schema_digest_sha256 = selected_profile_digest
    base.request.effect_key.session_id = "session"
    base.request.effect_key.lane = "main"
    base.request.effect_key.operation_id = "op"
    base.request.effect_key.effect_kind = "tool"
    base.request.effect_key.step_or_task_id = "step"
    base.request.effect_key.logical_attempt = 1
    base.request.effect_key.item_index = 0
    base.request.initial_stream_credit_bytes = 0
    base.request.payload = request.SerializeToString(deterministic=True)
    stream = Envelope()
    stream.stream_item.request_id = 17
    stream.stream_item.sequence = 0
    stream.stream_item.payload = progress.SerializeToString(deterministic=True)
    terminal = Envelope()
    terminal.stream_end.request_id = 17
    terminal.stream_end.result = result.SerializeToString(deterministic=True)
    bad_enum = Envelope()
    bad_enum.response.request_id = 17
    bad_enum.response.error.code = 127
    unsafe = class_from(pool_10, "WireValue")()
    unsafe.integer_value = 9007199254740992
    unsafe_request = EchoRequest()
    unsafe_request.CopyFrom(request)
    unsafe_entry = unsafe_request.arguments.value.map_value.entries.add()
    unsafe_entry.key = "unsafe"
    unsafe_entry.value.integer_value = 9007199254740992
    unsafe_envelope = Envelope()
    unsafe_envelope.request.CopyFrom(base.request)
    unsafe_envelope.request.payload = unsafe_request.SerializeToString(deterministic=True)
    future_progress = Envelope()
    future_progress.stream_item.request_id = 17
    future_progress.stream_item.sequence = 0
    future_progress.stream_item.payload = progress11.SerializeToString(deterministic=True)

    raw = {
        "request": framed(base.SerializeToString(deterministic=True)),
        "progress": framed(stream.SerializeToString(deterministic=True)),
        "terminal": framed(terminal.SerializeToString(deterministic=True)),
        "unknown_error_code": framed(bad_enum.SerializeToString(deterministic=True)),
        "unsafe_wire_value": {
            "protobuf_hex": unsafe.SerializeToString(deterministic=True).hex(),
            "enveloped_tool_request": framed(
                unsafe_envelope.SerializeToString(deterministic=True)
            ),
            "expected": "reject unsafe integer before schema dispatch",
        },
        "selected_minor_1_0_forbids_1_1_tool_progress": {
            "tool_progress_1_1_hex": progress11.SerializeToString(deterministic=True).hex(),
            "enveloped_progress": framed(
                future_progress.SerializeToString(deterministic=True)
            ),
            "field": "percent_complete",
            "present_value": 0,
            "expected": "selected 1.0 sender omits field 2 even if local SDK knows 1.1",
        },
    }
    body_len = raw["progress"]["body_bytes"]
    raw["credit_edge"] = {
        "charged_envelope_body_bytes": body_len,
        "prefix_bytes_excluded": len(bytes.fromhex(raw["progress"]["prefix_hex"])),
        "credit_below": body_len - 1,
        "credit_below_expected": "sender stops; no item",
        "credit_exact": body_len,
        "credit_exact_expected": "one item accepted; credit becomes zero",
        "zero_credit_terminal_expected": "StreamEnd accepted with complete result",
    }
    assert request.arguments.schema_digest_sha256 != selected_profile_digest
    return raw, {
        name: {
            "file_sha256": data["file_sha256"],
            "bounded_ascii_schema_digest_sha256": data["digest"].hex(),
        }
        for name, data in schemas.items()
    }


def validation_defaults():
    return {
        "status": "advisory behavioral table; protobuf parser alone enforces none of these",
        "rules": [
            {"field": "Envelope.frame", "absent": "invalid frame", "invalid": "unknown closed arm -> unsupported_variant; unknown optional non-discriminant ignored"},
            {"field": "Hello.protocol / Manifest.protocol", "absent": "invalid handshake", "invalid": "major/range mismatch fails atomic activation"},
            {"field": "CapabilityDeclaration.supported_descriptor_digests", "absent": "malformed manifest; reject whole activation regardless required", "invalid": "duplicate/gapped/missing advertised minor or non-32-byte digest rejects whole activation regardless required"},
            {"field": "CapabilityDeclaration.tool_schema_sets", "absent": "tool capability without sets is malformed; non-tool with sets is malformed", "invalid": "duplicate/gapped/missing advertised tool minor, projection ID or non-32-byte digest rejects whole activation regardless required; tool_name must equal capability name"},
            {"field": "structurally complete selected descriptor/dynamic digest or host-ceiling mismatch", "absent": "not applicable", "invalid": "required capability denies whole activation; optional capability omitted from grant"},
            {"field": "CapabilityDeclaration.replay / execution for tool.v1", "absent": "UNSPECIFIED=0 rejects whole activation as invalid_required_semantic_value", "invalid": "unknown numeric rejects whole activation as unsupported_variant; known replay IDEMPOTENT_HANDLER or execution UNARY/PASSIVE rejects whole activation as profile_inapplicable_variant; only replay SAFE/NEVER and execution STREAMING are profile-valid; known NEVER versus approved SAFE reaches required-denial/optional-omission"},
            {"field": "CapabilityRef.schema_digest_sha256", "absent": "invalid selected reference", "invalid": "must equal selected minor descriptor digest, not dynamic schema digest"},
            {"field": "Request.request_id", "absent": "invalid request", "invalid": "zero/duplicate/live collision rejected"},
            {"field": "Request.parent_request_id", "absent": "no parent for root request", "invalid": "missing/terminal/forged/cyclic parent -> invalid_parent"},
            {"field": "Request.capability_ref", "absent": "invalid request", "invalid": "principal/generation/grant mismatch starts no work"},
            {"field": "Request.effect_key", "absent": "per-method; required for effect dispatch", "invalid": "wrong durable key starts/settles no effect"},
            {"field": "Request.deadline_after_ms", "absent": "finite method default", "invalid": "checked monotonic minimum with method/connection/parent"},
            {"field": "Request.initial_stream_credit_bytes", "absent": 0, "invalid": "over per-invocation maximum -> credit_violation"},
            {"field": "Request.method", "absent": "unsupported_method", "invalid": "must match descriptor MethodOptions for selected profile minor"},
            {"field": "Request.payload / Response.result / StreamItem.payload / StreamEnd.result", "absent": "method-role dependent", "invalid": "decode only descriptor-selected DTO; no generic SDK object"},
            {"field": "StreamItem.sequence", "absent": 0, "invalid": "first is 0 then contiguous; gap/duplicate closes malformed generation"},
            {"field": "StreamItem credit", "absent": "no progress", "invalid": "charge exact serialized Envelope body bytes; prefix excluded"},
            {"field": "Credit.additional_bytes", "absent": 0, "invalid": "checked u64 addition/max, overflow closes malformed generation"},
            {"field": "Response/StreamEnd terminal", "absent": "invalid terminal", "invalid": "exactly one result or error; first validated terminal retained until durable settlement/no-longer-expected"},
            {"field": "ErrorCode/ReplayContract/ExecutionMode/CancelReason/GoAwayReason/MethodDirection/MethodMode", "absent": "UNSPECIFIED is invalid when semantic value required", "invalid": "unknown numeric -> unsupported_variant, never alias zero"},
            {"field": "WireValue.integer_value", "absent": "not selected", "invalid": "outside +/-9007199254740991 rejected before schema dispatch"},
            {"field": "WireValue.float_value", "absent": "not selected", "invalid": "NaN/Infinity rejected"},
            {"field": "WireValue.map_value", "absent": "not selected", "invalid": "duplicate keys/depth/elements/byte limits rejected"},
            {"field": "ToolProgress.percent_complete", "absent": "unknown progress percentage", "invalid": "selected 1.0 sender omits field 2; selected 1.1 present value must be 0..100"},
            {"field": "InvokeToolRequest.tool_call_id", "absent": "invalid tool invocation", "invalid": "validated source call identity distinct from request_id and item_index"},
            {"field": "InvokeProviderRequest.auth_mode", "absent": "invalid provider invocation", "invalid": "exactly none; no credential fields"},
            {"field": "InvokeHookResult.output", "absent": "skip optional result; mandatory for present result", "invalid": "before_resume uses BeforeResumeUnitResult; before_tool block object is inside validated output"},
        ],
    }


def terminal_retention_fixture(raw):
    return {
        "status": "expected state trace, not executed against Rust runtime",
        "effect_key": {
            "session_id": "session", "lane": "main", "operation_id": "op",
            "effect_kind": "tool", "step_or_task_id": "step",
            "logical_attempt": 1, "item_index": 0,
        },
        "live_binding": {
            "authenticated_extension_id": "peer-a", "capability": "tool/echo",
            "connection_generation": 7, "invocation_id": 17,
        },
        "first_terminal_frame_hex": raw["terminal"]["frame_hex"],
        "steps": [
            {"action": "receive first validated StreamEnd on G7", "state": "terminal_received", "retains": "exact binding + complete raw result"},
            {"action": "settlement CAS conflicts", "state": "terminal_received", "retains": "same binding + first output; no redispatch"},
            {"action": "G8 activates; G7 drains or disconnects", "state": "terminal_received", "retains": "first output remains eligible; generation age/loss alone is not stale"},
            {"action": "retry settlement with current durable state awaiting exact EffectKey", "state": "retired", "effect": "one durable settlement"},
            {"action": "second terminal or late progress on G7", "state": "retired", "effect": "stale; no mutation"},
        ],
    }


def negotiation_vectors(dynamic):
    _, _, pool = read_set("tool", "1.0")
    Manifest = class_from(pool, "Manifest")
    Grant = class_from(pool, "CapabilityGrant")
    digests = {
        minor: hashlib.sha256(
            b"tool.v1\0" + minor.encode("ascii") + b"\0" +
            read_set("tool", minor)[0]
        ).digest()
        for minor in ("1.0", "1.1")
    }
    manifest = Manifest()
    manifest.declared_extension_id = "metadata-only"
    manifest.extension_version = "0.1"
    manifest.protocol.major = 1
    manifest.protocol.min_minor = 0
    manifest.protocol.max_minor = 1
    declared = manifest.capabilities.add()
    declared.kind = "tool"
    declared.name = "echo"
    declared.profile.name = "tool.v1"
    declared.profile.major = 1
    declared.profile.min_minor = 0
    declared.profile.max_minor = 1
    declared.handler_id = "echo"
    declared.replay = 1
    declared.execution = 2
    declared.required = True
    for minor_number, version in enumerate(("1.0", "1.1")):
        supported = declared.supported_descriptor_digests.add()
        supported.minor = minor_number
        supported.descriptor_digest_sha256 = digests[version]
        schema_set = declared.tool_schema_sets.add()
        schema_set.tool_name = "echo"
        schema_set.minor = minor_number
        for field_name, projection in (
            ("arguments", "tool.arguments:echo"),
            ("progress", "tool.progress:echo"),
            ("raw_result", "tool.result:echo"),
        ):
            field = getattr(schema_set, field_name)
            field.projection_id = projection
            field.schema_digest_sha256 = bytes.fromhex(
                dynamic[projection]["bounded_ascii_schema_digest_sha256"]
            )
    assert len(declared.supported_descriptor_digests) == 2
    assert {v.minor for v in declared.supported_descriptor_digests} == {0, 1}
    assert all(len(v.descriptor_digest_sha256) == 32 for v in declared.supported_descriptor_digests)
    assert {v.minor for v in declared.tool_schema_sets} == {0, 1}
    assert all(v.tool_name == "echo" for v in declared.tool_schema_sets)

    def grant(minor, digest):
        value = Grant()
        value.authenticated_extension_id = "peer-a"
        value.selected_protocol.major = 1
        value.selected_protocol.minor = minor
        selected = value.selected_capabilities.add()
        selected.authenticated_extension_id = "peer-a"
        selected.kind = "tool"
        selected.name = "echo"
        selected.profile.name = "tool.v1"
        selected.profile.major = 1
        selected.profile.minor = minor
        selected.schema_digest_sha256 = digest
        selected.handler_id = "echo"
        value.connection_generation = 7
        limits = value.limits
        limits.pre_handshake_frame_bytes = 4096
        limits.frame_bytes = 65536
        limits.aggregate_decoded_bytes = 65536
        limits.value_depth = 8
        limits.value_elements = 1024
        limits.string_bytes = 4096
        limits.bytes_length = 4096
        limits.in_flight_requests = 4
        limits.blocking_queue_bytes = 4096
        limits.progress_queue_bytes = 4096
        limits.passive_queue_bytes = 4096
        limits.telemetry_queue_bytes = 4096
        limits.nesting_depth = 8
        limits.requests_per_second = 16
        limits.handshake_ms = 1000
        limits.cancellation_grace_ms = 100
        limits.max_stream_credit_bytes = 65536
        return value

    grant_10 = grant(0, digests["1.0"])
    grant_11 = grant(1, digests["1.1"])
    forged_11 = grant(1, b"\xff" * 32)
    for selected_minor, value in ((0, grant_10), (1, grant_11)):
        declared_digest = next(
            item.descriptor_digest_sha256
            for item in declared.supported_descriptor_digests
            if item.minor == selected_minor
        )
        assert value.selected_capabilities[0].schema_digest_sha256 == declared_digest
    assert forged_11.selected_capabilities[0].schema_digest_sha256 != digests["1.1"]
    assert bytes.fromhex(
        dynamic["tool.arguments:echo"]["bounded_ascii_schema_digest_sha256"]
    ) not in digests.values()
    return {
        "declared_range": "tool.v1/1.0..1.1",
        "per_minor_descriptor_digest": {
            minor: value.hex() for minor, value in digests.items()
        },
        "manifest_pb_hex": manifest.SerializeToString(deterministic=True).hex(),
        "grant_selected_1_0_pb_hex": grant_10.SerializeToString(deterministic=True).hex(),
        "grant_selected_1_1_pb_hex": grant_11.SerializeToString(deterministic=True).hex(),
        "forged_1_1_grant_pb_hex": forged_11.SerializeToString(deterministic=True).hex(),
        "forged_1_1_expected": "required digest mismatch rejects atomic activation; optional would be omitted",
        "dynamic_echo_args_digest": dynamic["tool.arguments:echo"]["bounded_ascii_schema_digest_sha256"],
        "dynamic_digest_location": "ToolSchemaSet advertises dynamic digests at activation; SchemaBoundValue repeats the selected triplet at invocation; CapabilityRef remains profile descriptor only",
        "scope": "representational/digest vector; no negotiation runtime executed",
    }


def main(write):
    expected = {
        f"{root}-{minor}.pb" for root in ROOTS for minor in ("1.0", "1.1")
    }
    actual = {path.name for path in (ROOT / "descriptors").glob("*.pb")}
    assert actual == expected, (sorted(actual - expected), sorted(expected - actual))
    for root in ROOTS:
        assert_additive(read_set(root, "1.0")[1], read_set(root, "1.1")[1], root)
    common10 = fields_by_name(read_set("common", "1.0")[1])
    assert ("common.proto", "InvocationBinding") not in common10
    cap = common10[("common.proto", "CapabilityDeclaration")]
    assert cap["supported_descriptor_digests"][0] == 9
    assert cap["tool_schema_sets"][0] == 11
    assert "schema_digest_sha256" not in cap
    tool = fields_by_name(read_set("tool", "1.0")[1])
    assert tool[("tool.proto", "InvokeToolRequest")]["tool_call_id"][0] == 3
    assert "block" not in tool.get(("hook.proto", "InvokeHookResult"), {})
    provider = fields_by_name(read_set("provider", "1.0")[1])
    assert provider[("provider.proto", "InvokeProviderRequest")]["model"][0] == 6
    hook = fields_by_name(read_set("hook", "1.0")[1])
    assert "block" not in hook[("hook.proto", "InvokeHookResult")]
    assert ("hook.proto", "BeforeResumeUnitResult") in hook

    table = {}
    for minor in ("1.0", "1.1"):
        table[minor] = {}
        for root, profile, service, expected_methods in PROFILES:
            methods = method_table(root, minor, service)
            assert set(methods) == expected_methods, (profile, minor)
            table[minor][profile] = methods
    assert table["1.0"] == table["1.1"]
    method_path = ROOT / "methods.json"
    method_bytes = (json.dumps(table, indent=2, sort_keys=True) + "\n").encode()
    if write:
        method_path.write_bytes(method_bytes)
    else:
        assert method_path.read_bytes() == method_bytes, "method table differs from descriptor authority"

    raw, dynamic = encode_echo()
    raw_path = ROOT / "fixtures" / "raw-vectors.json"
    raw_bytes = (json.dumps(raw, indent=2, sort_keys=True) + "\n").encode()
    if write:
        raw_path.write_bytes(raw_bytes)
    else:
        assert raw_path.read_bytes() == raw_bytes, "raw vectors differ from descriptor/source"

    negotiation = negotiation_vectors(dynamic)
    negotiation_path = ROOT / "fixtures" / "negotiation.json"
    negotiation_bytes = (json.dumps(negotiation, indent=2, sort_keys=True) + "\n").encode()
    if write:
        negotiation_path.write_bytes(negotiation_bytes)
    else:
        assert negotiation_path.read_bytes() == negotiation_bytes, "negotiation vectors differ"

    defaults_path = ROOT / "fixtures" / "validation-defaults.json"
    defaults_bytes = (json.dumps(validation_defaults(), indent=2, sort_keys=True) + "\n").encode()
    retention_path = ROOT / "state" / "terminal-retention.json"
    retention_bytes = (json.dumps(terminal_retention_fixture(raw), indent=2, sort_keys=True) + "\n").encode()
    if write:
        defaults_path.write_bytes(defaults_bytes)
        retention_path.write_bytes(retention_bytes)
    else:
        assert defaults_path.read_bytes() == defaults_bytes, "validation/default table differs"
        assert retention_path.read_bytes() == retention_bytes, "terminal state vector differs"

    manifest = {
        "compiler": "libprotoc 3.20.3",
        "descriptors": {},
        "method_table_sha256": hashlib.sha256(method_bytes).hexdigest(),
        "raw_vectors_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "negotiation_vectors_sha256": hashlib.sha256(negotiation_bytes).hexdigest(),
        "validation_defaults_sha256": hashlib.sha256(defaults_bytes).hexdigest(),
        "terminal_retention_sha256": hashlib.sha256(retention_bytes).hexdigest(),
        "dynamic_fixture": dynamic,
    }
    for root in ROOTS:
        for minor in ("1.0", "1.1"):
            raw_desc, _, _ = read_set(root, minor)
            entry = {
                "bytes": len(raw_desc),
                "file_sha256": hashlib.sha256(raw_desc).hexdigest(),
            }
            profile = next((p for r, p, _, _ in PROFILES if r == root), None)
            if profile:
                entry["profile_digest"] = hashlib.sha256(
                    profile.encode() + b"\0" + minor.encode() + b"\0" + raw_desc
                ).hexdigest()
            manifest["descriptors"][f"{root}-{minor}"] = entry
    manifest_path = ROOT / "fixtures" / "manifest.json"
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    if write:
        manifest_path.write_bytes(manifest_bytes)
    else:
        assert manifest_path.read_bytes() == manifest_bytes, "manifest differs from checked bytes"
    print("PASS 16 descriptors; 7 descriptor-derived profile tables; echo and raw vectors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="create checked draft projections")
    main(parser.parse_args().write)
