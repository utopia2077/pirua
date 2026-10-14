import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';
const source=execFileSync('git',['show','853a80d26c90a14c1886f0ebb8ffaae133ca2185:packages/ai/src/api/anthropic-messages.ts'],{cwd:upstreamRoot,encoding:'utf8'});
const context=vm.createContext({TextDecoder});
vm.runInContext(stripTypeScriptTypes(source.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,'')),context);
const raw='event: message_stop\r\ndata: {"type":"message_stop"}\r\n\r\n';
for(const [id,chunks] of [['whole',[raw]],['split-crlf',[raw.slice(0,20),raw.slice(20)]],['bytewise',[...raw]],['trailing',['event: message_stop\ndata: {"type":"message_stop"}']]]){
 const body=new ReadableStream({start(c){for(const text of chunks)c.enqueue(new TextEncoder().encode(text));c.close();}});
 const events=[];for await(const event of context.iterateSseMessages(body))events.push(event);
 console.log(JSON.stringify({id,chunks,events}));
}
