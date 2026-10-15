"""Offline fixture conformance: inspect an immutable candidate's exported enum."""
import json
from pathlib import Path
import re
import subprocess

case = json.loads(Path('case.json').read_text())
result = subprocess.run(['git', '-C', case['store'], 'show', case['commit'] + ':' + case['path']], capture_output=True, check=True)
source = result.stdout.decode()
# This expected contract is independent of the drift report and its item IDs.
assert re.search(r'export\s+enum\s+Color\s*\{\s*Red\s*,\s*Blue\s*\}', source), 'candidate enum contract missing'
if case.get('intake_path'):
    added = subprocess.run(['git', '-C', case['store'], 'show', case['commit'] + ':' + case['intake_path']], capture_output=True, check=True).stdout.decode()
    assert re.search(r'export\s+const\s+acceptedNewFile\s*=\s*1\s*;', added), 'candidate intake contract missing'
Path('artifact.json').write_text(json.dumps({'contract':'public-color-enum', 'members':['Red', 'Blue']}, sort_keys=True) + '\n')
print('public-color-enum: passed')
