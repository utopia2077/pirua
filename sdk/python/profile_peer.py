"""Framed six-profile reference process. Rust remains the effect owner."""

import json
import os
import sys
from pathlib import Path
from profiles.wire import Codec, varint, payload_object, sha
from profiles.shapes import Shapes, probe

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
if scenario not in corpus["scenarios"] or core not in (0, 1) or minor not in (0, 1):
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
                or not shapes.exposed(
                    root, obj["value"], corpus["exposure"]["open_leaves"]
                )
            ):
                raise ValueError("selected projection")
        for x in obj.values():
            bound_check(x)


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
        if scenario == "none" and profile_codec.methods[method]["profile"] == "hook.v1"
        else v.get("positive_terminal", v.get("terminal"))
    )


def terminal(request):
    global once_done
    m = profile_codec.methods[request["method"]]
    obj = result_for(request["method"])
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
            terminal(request)
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
