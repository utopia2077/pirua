#!/usr/bin/env python3
"""Protocol-only descriptor-backed echo peer and independent wire inspector."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import FieldDescriptor

ROOT = Path(__file__).resolve().parents[2]
FIX = json.loads((ROOT/'conformance/extension-protocol/echo-v1.json').read_text())
CAT = json.loads((ROOT/'crates/pi-extension-protocol/schema/fixtures/host-catalog.json').read_text())['entries'][0]
MODES = {'normal','manifest_profile_digest_wrong','manifest_dynamic_digest_wrong','terminal_projection_wrong','terminal_digest_wrong','duplicate_terminal'}

def selected(case):
    p, i = case.split('/')
    pair = next(x for x in FIX['version_pairs'] if x['id'] == p)
    item = next(x for x in FIX['inputs'] if x['id'] == i)
    assert i in pair['inputs']
    return pair, item

def context(minor):
    raw = (ROOT/f'crates/pi-extension-protocol/schema/descriptors/tool-1.{minor}.pb').read_bytes()
    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_pb2.FileDescriptorSet.FromString(raw).file:
        pool.Add(file)
    def cls(name):
        return message_factory.GetMessageClass(pool.FindMessageTypeByName('pi.extension.v1.'+name))
    return raw, pool, cls

def profile(minor):
    raw, _, _ = context(minor)
    return hashlib.sha256(b'tool.v1\0'+f'1.{minor}'.encode()+b'\0'+raw).digest()

def schema(minor, slot):
    selected_schema = CAT['selected_minor_schema_sets'][f'1.{minor}'][slot]
    raw = json.dumps(selected_schema['schema'], sort_keys=True, separators=(',',':')).encode()
    digest = hashlib.sha256(raw).digest()
    assert digest.hex() == selected_schema['schema_digest_sha256']
    return digest

def encvar(n):
    out = bytearray()
    while n >= 128:
        out.append((n & 127) | 128); n >>= 7
    out.append(n)
    return bytes(out)

def decvar(raw, pos=0):
    n = 0
    for shift in range(0,70,7):
        assert pos < len(raw)
        b = raw[pos]; pos += 1; n |= (b & 127) << shift
        if b < 128:
            assert len(encvar(n)) == shift//7+1
            return n,pos
    raise AssertionError('long prefix')

def readframe():
    prefix = bytearray()
    while len(prefix) < 10:
        b = os.read(0,1)
        if not b: raise EOFError('missing frame')
        prefix += b
        if b[0] < 128:
            n,_ = decvar(prefix)
            assert 0 < n <= 8192
            data = bytearray()
            while len(data) < n:
                part = os.read(0,n-len(data))
                if not part: raise EOFError('short frame')
                data += part
            return bytes(data)
    raise AssertionError('long prefix')

def frame(cls, arm, value):
    env = cls('Envelope')(); getattr(env,arm).CopyFrom(value)
    body = env.SerializeToString(); assert len(body) <= 8192
    return encvar(len(body))+body

def send(cls, arm, value): os.write(1,frame(cls,arm,value))
def recv(cls, arm):
    env = cls('Envelope').FromString(readframe())
    assert env.WhichOneof('frame') == arm
    return getattr(env,arm)

def manifest(cls, core, tool, fault):
    m=cls('Manifest')(); m.declared_extension_id='peer-a'; m.extension_version='0.1'
    m.protocol.major=1; m.protocol.min_minor=0; m.protocol.max_minor=1
    c=m.capabilities.add(); c.kind='tool'; c.name='echo'; c.handler_id='echo'
    c.profile.name='tool.v1'; c.profile.major=1; c.profile.min_minor=0; c.profile.max_minor=1
    c.replay=1; c.execution=2; c.required=True
    if core==1: c.display_label='Echo'
    for minor in (0,1):
        pd=c.supported_descriptor_digests.add(); pd.minor=minor; pd.descriptor_digest_sha256=profile(minor)
        if fault=='manifest_profile_digest_wrong' and minor==tool:
            pd.descriptor_digest_sha256=bytes([pd.descriptor_digest_sha256[0]^1])+pd.descriptor_digest_sha256[1:]
        ss=c.tool_schema_sets.add(); ss.tool_name='echo'; ss.minor=minor
        for slot,name in [('arguments','tool.arguments:echo'),('progress','tool.progress:echo'),('raw_result','tool.result:echo')]:
            d=getattr(ss,slot); d.projection_id=name; d.schema_digest_sha256=schema(minor,slot)
            if fault=='manifest_dynamic_digest_wrong' and minor==tool and slot=='raw_result':
                d.schema_digest_sha256=bytes([d.schema_digest_sha256[0]^1])+d.schema_digest_sha256[1:]
    return m

def result(cls, text, tool, fault):
    r=cls('InvokeToolResult')(); v=r.raw_result
    v.projection_id='tool.progress:echo' if fault=='terminal_projection_wrong' else 'tool.result:echo'
    v.schema_digest_sha256=schema(tool,'raw_result')
    if fault=='terminal_digest_wrong': v.schema_digest_sha256=bytes([v.schema_digest_sha256[0]^1])+v.schema_digest_sha256[1:]
    root=v.value.map_value
    content=root.entries.add(); content.key='content'
    obj=content.value.list_value.items.add().map_value
    e=obj.entries.add(); e.key='text'; e.value.text_value=text
    e=obj.entries.add(); e.key='type'; e.value.text_value='text'
    details=root.entries.add(); details.key='details'; details.value.map_value.SetInParent()
    return r

def actor(case,fault):
    pair,item=selected(case); core=pair['core_minor']; tool=pair['tool_minor']
    _,_,cls=context(core)
    h=cls('Hello')(); h.protocol.major=1; h.protocol.min_minor=0; h.protocol.max_minor=1
    p=h.supported_profiles.add(); p.name='tool.v1'; p.major=1; p.min_minor=0; p.max_minor=1
    send(cls,'hello',h)
    ack=recv(cls,'hello_ack'); assert (ack.selected_protocol.major,ack.selected_protocol.minor)==(1,core)
    send(cls,'manifest',manifest(cls,core,tool,fault))
    if fault.startswith('manifest_'): return
    g=recv(cls,'grant')
    assert g.authenticated_extension_id=='peer-a' and g.connection_generation==item['generation']
    assert (g.selected_protocol.major,g.selected_protocol.minor)==(1,core)
    assert len(g.selected_capabilities)==1
    send(cls,'ready',cls('Ready')())
    req=recv(cls,'request')
    assert req.request_id==item['request_id'] and req.method=='tool.invoke'
    assert not req.HasField('initial_stream_credit_bytes') and not req.HasField('parent_request_id')
    assert req.HasField('deadline_after_ms') and req.deadline_after_ms>0
    for name,want in item['effect_key'].items(): assert getattr(req.effect_key,name)==want
    ref=req.capability_ref; assert ref==g.selected_capabilities[0]
    assert (ref.authenticated_extension_id,ref.kind,ref.name)==('peer-a','tool','echo')
    assert (ref.profile.name,ref.profile.major,ref.profile.minor)==('tool.v1',1,tool)
    assert ref.schema_digest_sha256==profile(tool)
    _,_,tool_cls=context(tool)
    inner=tool_cls('InvokeToolRequest').FromString(req.payload)
    assert inner.tool_id=='echo' and inner.tool_call_id==item['tool_call_id']
    arg=inner.arguments
    assert arg.projection_id=='tool.arguments:echo' and arg.schema_digest_sha256==schema(tool,'arguments')
    entries=arg.value.map_value.entries
    assert len(entries)==1 and entries[0].key=='text'
    text=entries[0].value.text_value
    assert text==item['text']
    r=result(tool_cls,text,tool,fault)
    end=cls('StreamEnd')(); end.request_id=req.request_id; end.result=r.SerializeToString()
    wire=frame(cls,'stream_end',end); os.write(1,wire)
    if fault=='duplicate_terminal': os.write(1,wire)

def walk(raw, descriptor):
    pos=0; seen=[]
    while pos<len(raw):
        tag,pos=decvar(raw,pos); number=tag>>3; wire=tag&7
        field=descriptor.fields_by_number.get(number); assert field is not None,(descriptor.full_name,number)
        expected=2 if field.type in (9,12,11) else 1 if field.type in (1,6,16) else 0
        assert wire==expected,(descriptor.full_name,number,wire,expected)
        seen.append(number)
        if wire==0: _,pos=decvar(raw,pos)
        elif wire==1: pos+=8
        elif wire==2:
            length,pos=decvar(raw,pos); assert pos+length<=len(raw)
            if field.type==FieldDescriptor.TYPE_MESSAGE: walk(raw[pos:pos+length],field.message_type)
            pos+=length
        else: raise AssertionError('wire type')
        assert pos<=len(raw)
    return seen

def inspect(case,fault,stage):
    pair,item=selected(case); core=pair['core_minor']; tool=pair['tool_minor']
    _,pool,cls=context(core)
    raw=sys.stdin.buffer.read(8193); assert 0<len(raw)<=8192
    length,pos=decvar(raw); assert length==len(raw)-pos
    body=raw[pos:]; walk(body,pool.FindMessageTypeByName('pi.extension.v1.Envelope'))
    env=cls('Envelope').FromString(body); assert env.WhichOneof('frame')==stage
    if stage=='hello':
        assert env.hello.protocol.major==1 and env.hello.protocol.max_minor==1
        assert len(env.hello.supported_profiles)==1 and env.hello.supported_profiles[0].name=='tool.v1'
    elif stage=='manifest':
        m=env.manifest; assert m.declared_extension_id=='peer-a' and len(m.capabilities)==1
        c=m.capabilities[0]; assert (c.kind,c.name,c.handler_id,c.required,c.replay,c.execution)==('tool','echo','echo',True,1,2)
        assert len(c.supported_descriptor_digests)==len(c.tool_schema_sets)==2
        assert c.HasField('display_label') and c.display_label=='Echo' if core==1 else 10 not in c.DESCRIPTOR.fields_by_number
        for minor in (0,1):
            pd=next(x for x in c.supported_descriptor_digests if x.minor==minor)
            expected=profile(minor)
            if fault=='manifest_profile_digest_wrong' and minor==tool: expected=bytes([expected[0]^1])+expected[1:]
            assert pd.descriptor_digest_sha256==expected
            ss=next(x for x in c.tool_schema_sets if x.minor==minor)
            for slot,name in [('arguments','tool.arguments:echo'),('progress','tool.progress:echo'),('raw_result','tool.result:echo')]:
                d=getattr(ss,slot); expected=schema(minor,slot)
                if fault=='manifest_dynamic_digest_wrong' and minor==tool and slot=='raw_result': expected=bytes([expected[0]^1])+expected[1:]
                assert d.projection_id==name and d.schema_digest_sha256==expected
    elif stage=='ready': assert env.ready.SerializeToString()==b''
    elif stage=='stream_end':
        assert env.stream_end.request_id==item['request_id'] and env.stream_end.WhichOneof('terminal')=='result'
        _,tp,tc=context(tool)
        options_cls=message_factory.GetMessageClass(tp.FindMessageTypeByName('google.protobuf.MethodOptions'))
        method=tp.FindServiceByName('pi.extension.v1.ToolProfile').FindMethodByName('Invoke')
        options=options_cls.FromString(method.GetOptions().SerializeToString())
        assert options.Extensions[tp.FindExtensionByName('pi.extension.v1.wire_method')]=='tool.invoke'
        assert method.output_type.name=='InvokeToolResult'
        payload=env.stream_end.result
        walk(payload,tp.FindMessageTypeByName('pi.extension.v1.InvokeToolResult'))
        v=tc('InvokeToolResult').FromString(payload).raw_result
        name='tool.progress:echo' if fault=='terminal_projection_wrong' else 'tool.result:echo'
        digest=schema(tool,'raw_result')
        if fault=='terminal_digest_wrong': digest=bytes([digest[0]^1])+digest[1:]
        assert v.projection_id==name and v.schema_digest_sha256==digest
        entries={e.key:e.value for e in v.value.map_value.entries}
        assert set(entries)=={'content','details'} and entries['details'].WhichOneof('value')=='map_value'
        assert len(entries['details'].map_value.entries)==0
        content=entries['content'].list_value.items; assert len(content)==1
        assert {e.key:e.value.text_value for e in content[0].map_value.entries}=={'text':item['text'],'type':'text'}
    else: raise AssertionError(stage)

def main():
    p=argparse.ArgumentParser(); p.add_argument('--case',required=True); p.add_argument('--fault',required=True,choices=sorted(MODES))
    p.add_argument('--inspect-wire',choices=['hello','manifest','ready','stream_end'])
    a=p.parse_args()
    if a.inspect_wire: inspect(a.case,a.fault,a.inspect_wire)
    else: actor(a.case,a.fault)

if __name__=='__main__':
    try: main()
    except (AssertionError,EOFError,ValueError) as exc:
        print(f'peer failure: {exc}',file=sys.stderr); sys.exit(2)
