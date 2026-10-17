"""Generate current Coding projection closures, using ASCII-key RFC 8785 preimages.

Run from any working directory. Generated identity is independent of old Harness
schema/projections/static-v1.json and never adds fields to that retired inventory.
"""
import json,hashlib
from pathlib import Path
p=Path(__file__).with_name('coding-v1.json')
t={
'String':{'kind':'primitive','arm':'string'},
'Bool':{'kind':'primitive','arm':'bool'},
'Unsigned':{'kind':'primitive','arm':'integer','min':0,'max':9007199254740991},
'JsonValue':{'kind':'native_json_algebra','native_validation':'Mandatory current Coding DTO deserialization and context-sensitive validation before application.'},
'Unit':{'kind':'object','closed':True,'fields':{}},
}
def ref(n):return {'ref':n}
def obj(fields):return {'kind':'object','closed':True,'fields':{k:{'type':v[0] if isinstance(v,tuple) else v,'presence':'optional' if isinstance(v,tuple) else 'required','nullable':isinstance(v,tuple)} for k,v in fields.items()}}
def arr(n):return {'array':ref(n)}
def opt(n):return (ref(n),)
# Current native structures. Native callbacks are mandatory for all semantic leaves.
t['NativeMessage']={'kind':'native_open_role_object','role':{'enum':['system','user','assistant','toolResult','custom','bashExecution','branchSummary','compactionSummary']},'native_type':'current Coding message admission; role preservation is invocation-sensitive'}
t['NativeImage']=obj({'type':{'literal':'image'},'data':ref('String'),'mimeType':ref('String')})
t['NativeText']=obj({'type':{'literal':'text'},'text':ref('String'),'textSignature':opt('JsonValue')})
t['NativeInputContent']={'kind':'alternatives','alternatives':[{'type':ref('NativeText')},{'type':ref('NativeImage')}]}
t['NativeToolResult']=obj({'content':arr('NativeInputContent'),'details':opt('JsonValue'),'structuredContent':opt('JsonValue'),'usage':opt('JsonValue'),'isError':opt('Bool'),'terminate':opt('Bool')})
t['NativeHeader']=obj({'id':ref('String'),'cwd':ref('String'),'timestamp':ref('Unsigned'),'parentSession':opt('String')})
t['NativeSnapshot']=obj({'header':ref('NativeHeader'),'revision':ref('Unsigned'),'leafId':opt('String'),'entries':arr('JsonValue')})
t['Input']=obj({'text':ref('String'),'images':arr('NativeImage'),'source':{'enum':['interactive','extension','rpc']},'delivery':({'enum':['steer','followUp','nextTurn']},)})
t['InputResult']=obj({'action':{'enum':['continue','handled','transform']},'text':opt('String'),'images':(arr('NativeImage'),)})
t['BeforeAgentStart']=obj({'message':ref('NativeMessage'),'systemPrompt':ref('String'),'activeTools':arr('String')})
t['BeforeAgentStartResult']=obj({'messages':(arr('NativeMessage'),),'systemPrompt':opt('String'),'activeTools':(arr('String'),)})
t['Messages']=obj({'messages':arr('NativeMessage')})
t['Message']=obj({'message':ref('NativeMessage')})
t['BeforeTool']=obj({'toolCallId':ref('String'),'toolName':ref('String'),'args':ref('JsonValue'),'parentToolCallId':opt('String')})
t['BeforeToolResult']=obj({'block':ref('Bool'),'reason':opt('String'),'terminate':ref('Bool'),'args':opt('JsonValue')})
t['AfterTool']=obj({'toolCallId':ref('String'),'toolName':ref('String'),'args':ref('JsonValue'),'result':ref('NativeToolResult'),'isError':ref('Bool'),'parentToolCallId':opt('String')})
t['AfterToolResult']=obj({'content':(arr('NativeInputContent'),),'details':opt('JsonValue'),'structuredContent':opt('JsonValue'),'usage':opt('JsonValue'),'isError':opt('Bool'),'terminate':opt('Bool')})
t['ToolRequest']=obj({'toolCallId':ref('String'),'arguments':ref('JsonValue')})
t['ToolResult']=obj({'result':ref('NativeToolResult')})
t['BoundaryContext']=obj({'kind':{'enum':['turn_end','agent_before_settle']},'turnIndex':ref('Unsigned'),'message':opt('NativeMessage'),'toolResults':arr('NativeMessage'),'messageEntryId':opt('String'),'toolResultEntryIds':arr('String'),'snapshot':ref('NativeSnapshot'),'contextMessages':arr('NativeMessage'),'pendingMessages':arr('NativeMessage'),'canContinue':ref('Bool')})
t['CustomDraft']=obj({'type':{'literal':'custom'},'customType':ref('String'),'data':opt('JsonValue')})
t['CustomMessageDraft']=obj({'type':{'literal':'custom_message'},'customType':ref('String'),'content':ref('JsonValue'),'display':ref('Bool'),'details':opt('JsonValue')})
t['ContextEditDraft']=obj({'type':{'literal':'context_edit'},'targetId':ref('String'),'replacement':opt('JsonValue')})
t['CompactionDraft']=obj({'type':{'literal':'compaction'},'summary':ref('String'),'firstKeptEntryId':opt('String'),'details':opt('JsonValue'),'usage':opt('JsonValue')})
t['BoundaryDraft']={'kind':'alternatives','alternatives':[{'type':ref(n)} for n in ['CustomDraft','CustomMessageDraft','ContextEditDraft','CompactionDraft']]}
t['BoundaryResult']=obj({'entries':(arr('BoundaryDraft'),),'continueRun':opt('Bool')})
t['CompactionPreparation']=obj({'firstKeptEntryId':ref('String'),'messagesToSummarize':arr('NativeMessage'),'turnPrefixMessages':arr('NativeMessage'),'isSplitTurn':ref('Bool'),'tokensBefore':ref('Unsigned'),'previousSummary':opt('String'),'fileOps':ref('JsonValue'),'settings':ref('JsonValue')})
t['CompactionResult']=obj({'summary':ref('String'),'firstKeptEntryId':opt('String'),'tokensBefore':ref('Unsigned'),'details':opt('JsonValue'),'usage':opt('JsonValue')})
t['BeforeCompact']=obj({'preparation':ref('CompactionPreparation'),'reason':{'enum':['manual','threshold','overflow']}})
t['BeforeCompactResult']=obj({'cancel':opt('Bool'),'compaction':opt('CompactionResult')})
t['TreePreparation']=obj({'targetId':ref('String'),'oldLeafId':opt('String'),'commonAncestorId':opt('String'),'entriesToSummarize':arr('JsonValue'),'summarize':ref('Bool'),'customInstructions':opt('String'),'replaceInstructions':ref('Bool'),'label':opt('String')})
t['SummaryResult']=obj({'summary':ref('String'),'details':opt('JsonValue'),'usage':opt('JsonValue')})
t['BeforeTreeResult']=obj({'cancel':opt('Bool'),'summary':opt('SummaryResult'),'customInstructions':opt('String'),'replaceInstructions':opt('Bool'),'label':opt('String')})
t['SessionChange']=obj({'reason':{'enum':['new','switch','fork']},'snapshot':ref('NativeSnapshot')})
t['FiniteNumber']={'kind':'primitive','arm':'finite_number'}
t['CacheAction']={'kind':'enum','values':['warm','stop']}
t['CacheDecision']=obj({'phase':{'enum':['streaming','idle']},'warmCost':ref('FiniteNumber'),'missCost':ref('FiniteNumber'),'continuationProbability':ref('FiniteNumber'),'expectedSavings':ref('FiniteNumber'),'economicsAvailable':ref('Bool'),'action':ref('CacheAction')})
t['Loadout']=obj({'declared':arr('JsonValue'),'callable':arr('JsonValue'),'registered':arr('JsonValue')})
t['LoadoutChanges']=obj({'descriptions':({'map':{'key':ref('String'),'value':ref('String')}},),'hiddenDeclarations':(arr('String'),)})
t['SessionEvent']=obj({'type':{'enum':['agent','agent_end','bash_execution_update','nested_tool','entry_appended','queue_update','agent_settled','session_start','session_switch','session_fork','session_info_changed','session_tree','model_select','thinking_level_select','compaction_start','compaction_end','session_compact','auto_retry_start','auto_retry_end','cache_warmed']},'data':ref('JsonValue')})
def envelope(name):
 key='Envelope'+name;t[key]=obj({'value':ref(name)});return key
methods={'input':('Input','InputResult'),'before_agent_start':('BeforeAgentStart','BeforeAgentStartResult'),'context':('Messages','Messages'),'message_start':('Message','Message'),'message_end':('Message','Message'),'before_tool':('BeforeTool','BeforeToolResult'),'after_tool':('AfterTool','AfterToolResult')}
for m,i,o in [('turn_end','BoundaryContext','BoundaryResult'),('agent_before_settle','BoundaryContext','BoundaryResult'),('before_compact','BeforeCompact','BeforeCompactResult'),('before_tree','TreePreparation','BeforeTreeResult'),('before_session_change','SessionChange','Bool'),('cache_warming_decision','CacheDecision','CacheAction'),('prepare_loadout','Loadout','LoadoutChanges')]:
 methods[m]=(envelope(i),envelope(o))
methods['event']=(envelope('SessionEvent'),'Unit')
methods['tool.call']=('ToolRequest','ToolResult')
methods['session.snapshot']=('Unit',envelope('NativeSnapshot'))
projections={}
for m,(i,o) in methods.items():
 for stage,n in [('input',i),('output',o)]+([('progress','ToolResult')] if m=='tool.call' else []):
  root=f'coding.{stage}:{m}';t[root]={'kind':'alias','type':ref(n)}
  closure=set()
  def walk(n):
   if n in closure:return
   closure.add(n)
   def refs(v):
    if isinstance(v,dict):
     if 'ref' in v:walk(v['ref'])
     for q in v.values():refs(q)
    elif isinstance(v,list):
     for q in v:refs(q)
   refs(t[n])
  walk(root)
  pre={'descriptor_format':1,'root':root,'types':{n:t[n] for n in sorted(closure)},'policy':'coding.v1'}
  digest=hashlib.sha256(json.dumps(pre,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
  projections[root]={'root':root,'profile':'coding.v1','minors':[1],'type_universe':sorted(closure),'proposed_digest_sha256':digest}
doc={'descriptor_format':1,'target':'Pi v1.4.0 Agent/Coding; no Harness payload compatibility','native_validation':'Mandatory for every request, progress and terminal. JSON content leaves must be deserialized into current native types and validated against captured invocation/session facts.','types':t,'projections':projections,'canonical_wire_inventory':{'coding.v1':{'methods':[{'method':m,'direction':'EXTENSION_TO_CORE' if m.startswith('session.') else 'CORE_TO_EXTENSION','mode':'METHOD_STREAM' if m=='tool.call' else 'METHOD_UNARY'} for m in methods]}}}
p.write_text(json.dumps(doc,indent=2)+'\n')
