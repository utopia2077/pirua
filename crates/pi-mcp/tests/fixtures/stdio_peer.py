import json, os, sys, time, signal, subprocess

def send(value):
    data = json.dumps(value, ensure_ascii=False).encode('utf-8') + b'\r\n'
    # Split arbitrary byte boundaries, including UTF-8 and CRLF.
    for i in range(0, len(data), 7):
        sys.stdout.buffer.write(data[i:i+7])
        sys.stdout.buffer.flush()

if len(sys.argv) > 1 and sys.argv[1] == 'stubborn':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = subprocess.Popen([sys.executable, '-c', 'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(600)'])
    print('descendant=' + str(child.pid), file=sys.stderr, flush=True)
print('fixture ready', file=sys.stderr, flush=True)
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion':'2025-11-25','capabilities':{'tools':{}},'serverInfo':{'name':'stdio-fixture','version':'1'}}
    elif method == 'tools/list':
        result = {'tools':[{'name':'echo','inputSchema':{'type':'object'}}]}
    elif method == 'tools/call':
        token = request.get('params',{}).get('_meta',{}).get('progressToken')
        if token is not None:
            send({'jsonrpc':'2.0','method':'notifications/progress','params':{'progressToken':token,'progress':1,'total':1}})
        result = {'content':[{'type':'text','text':'你好 ' + json.dumps(request['params']['arguments'])}]}
    elif method == 'ping':
        result = {}
    else:
        continue
    send({'jsonrpc':'2.0','id':request['id'],'result':result})
if len(sys.argv)>1 and sys.argv[1]=='stubborn':
    while True:
        time.sleep(1)
