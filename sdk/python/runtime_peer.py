#!/usr/bin/env python3
"""Finite actual-runtime actor using the unchanged canonical descriptor peer helpers."""
import argparse
import json
from pathlib import Path
import sys
import time
import peer

FIX = json.loads((peer.ROOT / 'conformance/extension-protocol/runtime-settlement-v1.json').read_text())


def actor(pair, fault, release, replay, generation):
    core, tool = pair['core_minor'], pair['tool_minor']
    _, _, cls = peer.context(core)
    hello = cls('Hello')()
    hello.protocol.major = 1
    hello.protocol.min_minor = hello.protocol.max_minor = core
    profile = hello.supported_profiles.add()
    profile.name = 'tool.v1'
    profile.major = 1
    profile.min_minor = profile.max_minor = tool
    peer.send(cls, 'hello', hello)
    ack = peer.recv(cls, 'hello_ack')
    assert (ack.selected_protocol.major, ack.selected_protocol.minor) == (1, core)
    manifest = peer.manifest(cls, core, tool, 'normal')
    manifest.capabilities[0].replay = 1 if replay == 'safe' else 2
    peer.send(cls, 'manifest', manifest)
    grant = peer.recv(cls, 'grant')
    assert grant.authenticated_extension_id == FIX['principal']
    assert grant.connection_generation == generation
    assert (grant.selected_protocol.major, grant.selected_protocol.minor) == (1, core)
    assert len(grant.selected_capabilities) == 1
    peer.send(cls, 'ready', cls('Ready')())
    request = peer.recv(cls, 'request')
    assert request.request_id > 0 and request.method == 'tool.invoke'
    assert not request.HasField('parent_request_id')
    assert request.HasField('initial_stream_credit_bytes') and request.initial_stream_credit_bytes == 0
    assert request.HasField('deadline_after_ms') and request.deadline_after_ms > 0
    key = request.effect_key
    assert key.session_id == 'runtime-session' and key.lane == 'main' and key.effect_kind == 'tool'
    assert key.operation_id and key.step_or_task_id and key.logical_attempt == 0
    assert key.HasField('item_index') and key.item_index == 0
    ref = request.capability_ref
    assert ref == grant.selected_capabilities[0]
    assert (ref.profile.name, ref.profile.major, ref.profile.minor) == ('tool.v1', 1, tool)
    assert ref.schema_digest_sha256 == peer.profile(tool)
    _, _, tool_cls = peer.context(tool)
    invocation = tool_cls('InvokeToolRequest').FromString(request.payload)
    assert invocation.tool_id == 'echo' and invocation.tool_call_id == FIX['tool_call_id']
    arguments = invocation.arguments
    assert arguments.projection_id == 'tool.arguments:echo'
    assert arguments.schema_digest_sha256 == peer.schema(tool, 'arguments')
    entries = arguments.value.map_value.entries
    assert len(entries) == 1 and entries[0].key == 'text'
    text = entries[0].value.text_value
    assert text == FIX['effective_text']
    print(json.dumps({'stage': 'request', 'request_id': request.request_id,
                      'key': [key.session_id, key.lane, key.operation_id, key.effect_kind, key.step_or_task_id],
                      'logical_attempt': key.logical_attempt, 'item_index': key.item_index, 'generation': generation,
                      'core': core, 'tool': tool, 'args': {'text': text}}), file=sys.stderr, flush=True)
    started = time.monotonic()
    while not Path(release).exists():
        assert (time.monotonic() - started) * 1000 < FIX['bounded_case_timeout_ms'], 'release deadline'
        time.sleep(0.005)
    result = peer.result(tool_cls, text, tool, fault)
    usage = result.raw_result.value.map_value.entries.add()
    usage.key = 'usage'
    for name, value in [('cacheRead', 0), ('cacheWrite', 0), ('input', FIX['tokens']), ('output', 0), ('totalTokens', FIX['tokens'])]:
        entry = usage.value.map_value.entries.add()
        entry.key = name
        entry.value.integer_value = value
    cost = usage.value.map_value.entries.add()
    cost.key = 'cost'
    for name in ['cacheRead', 'cacheWrite', 'input', 'output', 'total']:
        entry = cost.value.map_value.entries.add()
        entry.key = name
        entry.value.integer_value = 0
    end = cls('StreamEnd')()
    end.request_id = request.request_id + (100 if fault == 'wrong_request' else 0)
    end.result = result.SerializeToString()
    wire = peer.frame(cls, 'stream_end', end)
    import os
    os.write(1, wire)
    if fault == 'duplicate_terminal':
        os.write(1, wire)
    print(json.dumps({'stage': 'terminal', 'request_id': request.request_id, 'text': text,
                      'tokens': FIX['tokens'], 'core': core, 'tool': tool, 'fault': fault}), file=sys.stderr, flush=True)
    time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', required=True)
    parser.add_argument('--fault', choices=FIX['faults'], required=True)
    parser.add_argument('--release', required=True)
    parser.add_argument('--generation', required=True, type=int)
    parser.add_argument('--replay', choices=['safe', 'never'], required=True)
    args = parser.parse_args()
    pair = next(pair for pair in FIX['version_pairs'] if pair['id'] == args.case)
    actor(pair, args.fault, args.release, args.replay, args.generation)


if __name__ == '__main__':
    try:
        main()
    except (AssertionError, EOFError, ValueError, OSError) as error:
        print(f'runtime peer failure: {error}', file=sys.stderr)
        sys.exit(2)
