#!/usr/bin/env python3
"""Advisory host-catalog/manifest activation model; not an endpoint runtime."""
import argparse
import copy
import hashlib
import json
from pathlib import Path

from verify_810 import class_from, echo_schema, read_set, varint

ROOT = Path(__file__).resolve().parent
PROJECTIONS = {
    "arguments": "tool.arguments:echo",
    "progress": "tool.progress:echo",
    "raw_result": "tool.result:echo",
}


def canonical_json(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def create_catalog():
    schemas = {}
    for slot, name in PROJECTIONS.items():
        schema, digest, file_sha = echo_schema(name)
        schemas[slot] = {
            "projection_id": name,
            "schema": schema,
            "schema_digest_sha256": digest.hex(),
            "source_file_sha256": file_sha,
        }
    entry = {
        "authenticated_extension_id": "peer-a",
        "tool_name": "echo",
        "agentTool": {
            "name": "echo",
            "label": "Echo",
            "description": "Return the supplied text as a text tool result.",
            "parameters": schemas["arguments"]["schema"],
            "executionMode": "sequential",
            "replay": "safe",
        },
        "selected_minor_schema_sets": {
            "1.0": copy.deepcopy(schemas),
            "1.1": copy.deepcopy(schemas),
        },
        "selected_minor_profile_digests": {
            minor: hashlib.sha256(
                b"tool.v1\0" + minor.encode("ascii") + b"\0" +
                read_set("tool", minor)[0]
            ).hexdigest()
            for minor in ("1.0", "1.1")
        },
    }
    assert entry["agentTool"]["parameters"] == entry[
        "selected_minor_schema_sets"
    ]["1.0"]["arguments"]["schema"]
    return {
        "authority": "host-established immutable process-local capability ceiling",
        "entries": [entry],
    }


def digest_field(field):
    return field.schema_digest_sha256.hex()


def activate(manifest, catalog, selected_minor, required=None, authenticated_principal="peer-a"):
    """Malformed structure rejects whole activation; complete selected mismatch may omit optional."""
    entry = catalog["entries"][0]
    capabilities = list(manifest.capabilities)
    if len(capabilities) != 1:
        return {"status": "reject", "reason": "manifest_capability_count"}
    cap = capabilities[0]
    required = cap.required if required is None else required
    if entry["authenticated_extension_id"] != authenticated_principal:
        return {"status": "reject", "reason": "host_catalog_principal_mismatch"}
    if cap.kind != "tool" or cap.profile.name != "tool.v1":
        return {"status": "reject", "reason": "non_tool_or_profile_mismatch"}
    if not cap.name:
        return {"status": "reject", "reason": "missing_tool_name"}
    if cap.profile.major != 1 or (cap.profile.min_minor, cap.profile.max_minor) != (0, 1):
        return {"status": "reject", "reason": "unsupported_range"}
    # Closed enum classification is structural and precedes optional host mismatch.
    if cap.replay not in (0, 1, 2, 3) or cap.execution not in (0, 1, 2, 3):
        return {"status": "reject", "reason": "unsupported_variant"}
    if cap.replay == 0 or cap.execution == 0:
        return {"status": "reject", "reason": "invalid_required_semantic_value"}
    if cap.replay not in (1, 2) or cap.execution != 2:
        return {"status": "reject", "reason": "profile_inapplicable_variant"}
    # AgentTool.executionMode is sequential/parallel, independent of transport mode 2=stream.
    agent_tool = entry["agentTool"]
    if agent_tool["executionMode"] not in ("sequential", "parallel") or agent_tool["replay"] != "safe":
        return {"status": "reject", "reason": "host_descriptor_invalid"}
    if not isinstance(agent_tool["description"], str) or not isinstance(agent_tool["label"], str):
        return {"status": "reject", "reason": "host_descriptor_invalid"}
    if not isinstance(agent_tool["parameters"], dict):
        return {"status": "reject", "reason": "host_parameters_invalid"}

    expected_minors = {0, 1}
    descriptors = list(cap.supported_descriptor_digests)
    sets = list(cap.tool_schema_sets)
    if len(descriptors) != 2 or {item.minor for item in descriptors} != expected_minors:
        return {"status": "reject", "reason": "duplicate_or_missing_profile_minor"}
    if len(sets) != 2 or {item.minor for item in sets} != expected_minors:
        return {"status": "reject", "reason": "duplicate_or_missing_tool_schema_minor"}
    expected_ids = {
        f"tool.arguments:{cap.name}",
        f"tool.progress:{cap.name}",
        f"tool.result:{cap.name}",
    }
    for item in descriptors:
        if len(item.descriptor_digest_sha256) != 32:
            return {"status": "reject", "reason": "missing_profile_digest"}
    for schema_set in sets:
        if schema_set.tool_name != cap.name:
            return {"status": "reject", "reason": "tool_schema_name_mismatch"}
        ids = []
        for slot in PROJECTIONS:
            if not schema_set.HasField(slot):
                return {"status": "reject", "reason": "missing_projection"}
            field = getattr(schema_set, slot)
            if not field.projection_id or len(field.schema_digest_sha256) != 32:
                return {"status": "reject", "reason": "missing_projection_id_or_digest"}
            ids.append(field.projection_id)
        if len(set(ids)) != 3 or set(ids) != expected_ids:
            return {"status": "reject", "reason": "duplicate_or_wrong_projection_id"}

    if selected_minor not in expected_minors:
        return {"status": "reject", "reason": "selected_minor_outside_range"}
    version = f"1.{selected_minor}"
    descriptor = next(item for item in descriptors if item.minor == selected_minor)
    selected_set = next(item for item in sets if item.minor == selected_minor)
    mismatch = cap.name != entry["tool_name"]
    mismatch |= cap.replay != 1
    mismatch |= descriptor.descriptor_digest_sha256.hex() != entry["selected_minor_profile_digests"][version]
    host_set = entry["selected_minor_schema_sets"][version]
    for slot in PROJECTIONS:
        field = getattr(selected_set, slot)
        expected = host_set[slot]
        mismatch |= field.projection_id != expected["projection_id"]
        mismatch |= digest_field(field) != expected["schema_digest_sha256"]
    mismatch |= agent_tool["parameters"] != host_set["arguments"]["schema"]
    # 1.1-only display_label is checked if present, never used as authority.
    if cap.DESCRIPTOR.fields_by_name.get("display_label") and cap.HasField("display_label"):
        mismatch |= cap.display_label != agent_tool["label"]
    if mismatch:
        if required:
            return {"status": "reject", "reason": "required_capability_denied", "selected_minor": version}
        return {"status": "active", "omitted": [cap.name], "selected": [], "selected_minor": version}
    return {
        "status": "active",
        "selected": [cap.name],
        "omitted": [],
        "selected_minor": version,
        "authenticated_extension_id": entry["authenticated_extension_id"],
        "catalog_backed_agent_tool": copy.deepcopy(agent_tool),
        "selected_projection_triplet": {
            slot: {
                "projection_id": host_set[slot]["projection_id"],
                "schema_digest_sha256": host_set[slot]["schema_digest_sha256"],
            }
            for slot in PROJECTIONS
        },
    }


def dispatch_tool_id(tool_id, selected):
    return {
        "status": "accept" if tool_id == selected["catalog_backed_agent_tool"]["name"] else "reject",
        "reason": None if tool_id == selected["catalog_backed_agent_tool"]["name"] else "wrong_tool_id_before_dispatch",
    }


def receive_result(projection_id, digest_hex, selected):
    result = selected["selected_projection_triplet"]["raw_result"]
    valid = projection_id == result["projection_id"] and digest_hex == result["schema_digest_sha256"]
    return {
        "status": "accept_for_domain_validation" if valid else "reject_before_domain_validation",
        "settlement_eligible": valid,
    }


def fixtures():
    catalog = create_catalog()
    _, _, pool = read_set("tool", "1.1")
    Manifest = class_from(pool, "Manifest")
    base = Manifest.FromString(bytes.fromhex(
        json.loads((ROOT / "fixtures" / "negotiation.json").read_text())["manifest_pb_hex"]
    ))
    correct = Manifest()
    correct.CopyFrom(base)
    correct.capabilities[0].display_label = "Echo"
    forged = Manifest()
    forged.CopyFrom(correct)
    forged.capabilities[0].tool_schema_sets[1].raw_result.schema_digest_sha256 = b"\xaa" * 32
    optional = Manifest()
    optional.CopyFrom(forged)
    optional.capabilities[0].required = False
    optional_missing_minor = Manifest()
    optional_missing_minor.CopyFrom(correct)
    optional_missing_minor.capabilities[0].required = False
    del optional_missing_minor.capabilities[0].tool_schema_sets[1]
    optional_duplicate_minor = Manifest()
    optional_duplicate_minor.CopyFrom(correct)
    optional_duplicate_minor.capabilities[0].required = False
    optional_duplicate_minor.capabilities[0].tool_schema_sets.add().CopyFrom(
        optional_duplicate_minor.capabilities[0].tool_schema_sets[1]
    )
    optional_missing_profile_digest = Manifest()
    optional_missing_profile_digest.CopyFrom(correct)
    optional_missing_profile_digest.capabilities[0].required = False
    optional_missing_profile_digest.capabilities[0].supported_descriptor_digests[1].descriptor_digest_sha256 = b""
    optional_missing_projection_id = Manifest()
    optional_missing_projection_id.CopyFrom(correct)
    optional_missing_projection_id.capabilities[0].required = False
    optional_missing_projection_id.capabilities[0].tool_schema_sets[1].progress.projection_id = ""
    assert correct.capabilities[0].name == forged.capabilities[0].name == "echo"
    assert [
        (item.minor, item.descriptor_digest_sha256)
        for item in correct.capabilities[0].supported_descriptor_digests
    ] == [
        (item.minor, item.descriptor_digest_sha256)
        for item in forged.capabilities[0].supported_descriptor_digests
    ]
    duplicate_profile_minor = Manifest()
    duplicate_profile_minor.CopyFrom(correct)
    duplicate_profile_minor.capabilities[0].supported_descriptor_digests.add().CopyFrom(
        duplicate_profile_minor.capabilities[0].supported_descriptor_digests[1]
    )
    missing_profile_digest = Manifest()
    missing_profile_digest.CopyFrom(correct)
    missing_profile_digest.capabilities[0].supported_descriptor_digests[1].descriptor_digest_sha256 = b""
    duplicate_minor = Manifest()
    duplicate_minor.CopyFrom(correct)
    duplicate_minor.capabilities[0].tool_schema_sets.add().CopyFrom(
        duplicate_minor.capabilities[0].tool_schema_sets[1]
    )
    missing_minor = Manifest()
    missing_minor.CopyFrom(correct)
    del missing_minor.capabilities[0].tool_schema_sets[1]
    duplicate_id = Manifest()
    duplicate_id.CopyFrom(correct)
    duplicate_id.capabilities[0].tool_schema_sets[1].raw_result.projection_id = "tool.progress:echo"
    missing_digest = Manifest()
    missing_digest.CopyFrom(correct)
    missing_digest.capabilities[0].tool_schema_sets[1].arguments.schema_digest_sha256 = b""
    missing_id = Manifest()
    missing_id.CopyFrom(correct)
    missing_id.capabilities[0].tool_schema_sets[1].progress.projection_id = ""
    wrong_tool_name = Manifest()
    wrong_tool_name.CopyFrom(correct)
    wrong_tool_name.capabilities[0].tool_schema_sets[0].tool_name = "other"
    smuggled_non_tool = Manifest()
    smuggled_non_tool.CopyFrom(correct)
    smuggled_non_tool.capabilities[0].kind = "hook"
    wrong_label = Manifest()
    wrong_label.CopyFrom(correct)
    wrong_label.capabilities[0].display_label = "Impostor"
    unapproved_required = Manifest()
    unapproved_required.CopyFrom(correct)
    unapproved_required.capabilities[0].name = "other"
    for schema_set in unapproved_required.capabilities[0].tool_schema_sets:
        schema_set.tool_name = "other"
        for slot in ("arguments", "progress", "raw_result"):
            projection = getattr(schema_set, slot)
            projection.projection_id = projection.projection_id.replace(":echo", ":other")
    unapproved_optional = Manifest()
    unapproved_optional.CopyFrom(unapproved_required)
    unapproved_optional.capabilities[0].required = False
    def with_closed_enum(field, value, required=False):
        candidate = Manifest()
        candidate.CopyFrom(correct)
        candidate.capabilities[0].required = required
        setattr(candidate.capabilities[0], field, value)
        return candidate
    optional_replay_zero = with_closed_enum("replay", 0)
    optional_replay_unknown = with_closed_enum("replay", 127)
    optional_execution_zero = with_closed_enum("execution", 0)
    optional_execution_unknown = with_closed_enum("execution", 127)
    optional_replay_inapplicable = with_closed_enum("replay", 3)
    optional_execution_unary = with_closed_enum("execution", 1)
    optional_execution_passive = with_closed_enum("execution", 3)
    replay_never_required = with_closed_enum("replay", 2, required=True)
    replay_never_optional = with_closed_enum("replay", 2)
    selected_10 = activate(forged, catalog, 0)
    selected_11 = activate(correct, catalog, 1)
    checks = {
        "valid_1_1": (correct, 1, "active", None),
        "forged_1_1_required": (forged, 1, "reject", "required_capability_denied"),
        "forged_1_1_optional": (optional, 1, "active", None),
        "optional_malformed_missing_minor": (optional_missing_minor, 1, "reject", "duplicate_or_missing_tool_schema_minor"),
        "optional_malformed_duplicate_minor": (optional_duplicate_minor, 1, "reject", "duplicate_or_missing_tool_schema_minor"),
        "optional_malformed_missing_profile_digest": (optional_missing_profile_digest, 1, "reject", "missing_profile_digest"),
        "optional_malformed_missing_projection_id": (optional_missing_projection_id, 1, "reject", "missing_projection_id_or_digest"),
        "forged_1_1_selected_1_0": (forged, 0, "active", None),
        "duplicate_profile_minor": (duplicate_profile_minor, 1, "reject", "duplicate_or_missing_profile_minor"),
        "missing_profile_digest": (missing_profile_digest, 1, "reject", "missing_profile_digest"),
        "duplicate_minor": (duplicate_minor, 1, "reject", "duplicate_or_missing_tool_schema_minor"),
        "missing_minor": (missing_minor, 1, "reject", "duplicate_or_missing_tool_schema_minor"),
        "duplicate_projection_id": (duplicate_id, 1, "reject", "duplicate_or_wrong_projection_id"),
        "missing_projection_digest": (missing_digest, 1, "reject", "missing_projection_id_or_digest"),
        "missing_projection_id": (missing_id, 1, "reject", "missing_projection_id_or_digest"),
        "wrong_tool_schema_name": (wrong_tool_name, 1, "reject", "tool_schema_name_mismatch"),
        "non_tool_schema_smuggle": (smuggled_non_tool, 1, "reject", "non_tool_or_profile_mismatch"),
        "wrong_display_label": (wrong_label, 1, "reject", "required_capability_denied"),
        "unapproved_host_ceiling_required": (unapproved_required, 1, "reject", "required_capability_denied"),
        "unapproved_host_ceiling_optional": (unapproved_optional, 1, "active", None),
        "optional_replay_unspecified": (optional_replay_zero, 1, "reject", "invalid_required_semantic_value"),
        "optional_replay_unknown_127": (optional_replay_unknown, 1, "reject", "unsupported_variant"),
        "optional_execution_unspecified": (optional_execution_zero, 1, "reject", "invalid_required_semantic_value"),
        "optional_execution_unknown_127": (optional_execution_unknown, 1, "reject", "unsupported_variant"),
        "optional_replay_known_inapplicable": (optional_replay_inapplicable, 1, "reject", "profile_inapplicable_variant"),
        "optional_execution_unary_inapplicable": (optional_execution_unary, 1, "reject", "profile_inapplicable_variant"),
        "optional_execution_passive_inapplicable": (optional_execution_passive, 1, "reject", "profile_inapplicable_variant"),
        "replay_never_required_host_safe": (replay_never_required, 1, "reject", "required_capability_denied"),
        "replay_never_optional_host_safe": (replay_never_optional, 1, "active", None),
    }
    outcomes = {}
    for name, (manifest, minor, status, reason) in checks.items():
        result = activate(manifest, catalog, minor)
        assert result["status"] == status, (name, result)
        if reason:
            assert result.get("reason") == reason, (name, result)
        if name in ("forged_1_1_optional", "replay_never_optional_host_safe"):
            assert result["omitted"] == ["echo"] and result["selected"] == []
        if name == "unapproved_host_ceiling_optional":
            assert result["omitted"] == ["other"] and result["selected"] == []
        outcomes[name] = {
            "selected_minor": f"1.{minor}",
            "manifest_sha256": hashlib.sha256(
                manifest.SerializeToString(deterministic=True)
            ).hexdigest(),
            "manifest_pb_hex": manifest.SerializeToString(deterministic=True).hex(),
            "model_result": result,
        }
    assert selected_10["catalog_backed_agent_tool"] == catalog["entries"][0]["agentTool"]
    assert selected_10["catalog_backed_agent_tool"]["parameters"] == catalog["entries"][0][
        "selected_minor_schema_sets"
    ]["1.0"]["arguments"]["schema"]
    wrong_id = dispatch_tool_id("other", selected_10)
    assert wrong_id["status"] == "reject"
    wrong_principal = activate(correct, catalog, 1, authenticated_principal="peer-b")
    assert wrong_principal == {
        "status": "reject", "reason": "host_catalog_principal_mismatch"
    }
    raw = json.loads((ROOT / "fixtures" / "raw-vectors.json").read_text())
    Envelope = class_from(pool, "Envelope")
    ToolRequest = class_from(pool, "InvokeToolRequest")
    ToolResult = class_from(pool, "InvokeToolResult")
    wrong_request_frame = Envelope.FromString(bytes.fromhex(raw["request"]["body_hex"]))
    wrong_request = ToolRequest.FromString(wrong_request_frame.request.payload)
    wrong_request.tool_id = "other"
    wrong_request_frame.request.payload = wrong_request.SerializeToString(deterministic=True)
    assert dispatch_tool_id(wrong_request.tool_id, selected_10) == wrong_id
    correct_result = selected_11["selected_projection_triplet"]["raw_result"]
    wrong_terminal_frame = Envelope.FromString(bytes.fromhex(raw["terminal"]["body_hex"]))
    wrong_terminal = ToolResult.FromString(wrong_terminal_frame.stream_end.result)
    assert wrong_terminal.raw_result.projection_id == correct_result["projection_id"]
    wrong_terminal.raw_result.schema_digest_sha256 = b"\xbb" * 32
    wrong_terminal_frame.stream_end.result = wrong_terminal.SerializeToString(deterministic=True)
    wrong_result_digest = receive_result(
        wrong_terminal.raw_result.projection_id,
        wrong_terminal.raw_result.schema_digest_sha256.hex(),
        selected_11,
    )
    assert wrong_result_digest == {
        "status": "reject_before_domain_validation", "settlement_eligible": False
    }
    return catalog, {
        "status": "deterministic reference model only; no endpoint execution",
        "cases": outcomes,
        "wrong_tool_id": wrong_id,
        "wrong_authenticated_principal": wrong_principal,
        "wrong_result_schema_bound_digest": wrong_result_digest,
        "wrong_tool_id_raw_frame_hex": (
            varint(len(wrong_request_frame.SerializeToString(deterministic=True))) +
            wrong_request_frame.SerializeToString(deterministic=True)
        ).hex(),
        "wrong_result_digest_raw_frame_hex": (
            varint(len(wrong_terminal_frame.SerializeToString(deterministic=True))) +
            wrong_terminal_frame.SerializeToString(deterministic=True)
        ).hex(),
        "correct_result_schema_bound_digest": receive_result(
            correct_result["projection_id"],
            correct_result["schema_digest_sha256"],
            selected_11,
        ),
    }


def main(write):
    catalog, vectors = fixtures()
    for name, value in (("host-catalog.json", catalog), ("activation-vectors.json", vectors)):
        target = ROOT / "fixtures" / name
        contents = canonical_json(value)
        if write:
            target.write_bytes(contents)
        else:
            assert target.read_bytes() == contents, name + " differs from model"
    print(f"PASS host catalog + {len(vectors['cases'])} activation variants + wrong tool/result binding")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    main(parser.parse_args().write)
