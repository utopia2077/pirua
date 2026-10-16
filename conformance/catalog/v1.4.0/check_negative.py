#!/usr/bin/env python3
"""Exercise release checker rejection without modifying the repository corpus."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
with tempfile.TemporaryDirectory(prefix='pi-v140-catalog-negative-') as temporary:
    fixture = Path(temporary)
    data = fixture / 'conformance/catalog/v1.4.0'
    shutil.copytree(ROOT / 'conformance/catalog/v1.4.0', data)
    shutil.copytree(ROOT / 'conformance/catalog/generated/text',
                    fixture / 'conformance/catalog/generated/text')

    def check():
        return subprocess.run(['python3', str(data / 'check.py')],
                              capture_output=True, check=False)

    assert check().returncode == 0
    for target in [data / 'pi-ai-1.4.0.tgz', data / 'package-data/openai.json',
                   fixture / 'conformance/catalog/generated/text/openai.json']:
        original = target.read_bytes()
        target.write_bytes(original + b'\n')
        assert check().returncode != 0, target
        target.write_bytes(original)

    target = data / 'provenance.json'
    original = target.read_bytes()
    manifest = json.loads(original)
    manifest['upstreamCommit'] = '0' * 40
    target.write_text(json.dumps(manifest))
    assert check().returncode != 0, 'incorrect release source commit'
    target.write_bytes(original)
    assert check().returncode == 0
print('Pi v1.4.0 catalog rejects modified archive, input, output and source identity')
