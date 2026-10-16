#!/usr/bin/env python3
"""Verify exact released selected chat data offline; never fetch mutable feeds."""
import base64
import hashlib
import json
import io
import tarfile
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
DATA = Path(__file__).resolve().parent
manifest = json.loads((DATA / 'provenance.json').read_text())
assert manifest['upstreamCommit'] == '4a080252f06caf63a525d7873c5d94d3c23887e0'
attestations = json.loads((DATA / 'attestations.json').read_text())
statement = next(json.loads(base64.b64decode(item['bundle']['dsseEnvelope']['payload']))
                 for item in attestations['attestations'] if item['predicateType'] == 'https://slsa.dev/provenance/v1')
assert statement['predicate']['buildDefinition']['resolvedDependencies'][0]['digest']['gitCommit'] == manifest['upstreamCommit']
assert statement['subject'][0]['digest']['sha512'] == base64.b64decode(manifest['integrity'].split('-', 1)[1]).hex()
archive = (DATA / 'pi-ai-1.4.0.tgz').read_bytes()
assert hashlib.sha512(archive).digest() == base64.b64decode(manifest['integrity'].split('-', 1)[1])
package = tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz')
for provider, evidence in manifest['files'].items():
    source = (DATA / 'package-data' / f'{provider}.json').read_bytes()
    assert package.extractfile(f'package/dist/providers/data/{provider}.json').read() == source, provider
    assert hashlib.sha256(source).hexdigest() == evidence['packageDataSha256'], provider
    models = [model for group in json.loads(source).values() for model in group.values() if model.get('type', 'chat') == 'chat']
    output = (ROOT / 'conformance/catalog/generated/text' / f'{provider}.json').read_bytes()
    assert hashlib.sha256(output).hexdigest() == evidence['selectedSha256'], provider
    assert json.loads(output) == models, provider
    assert len(models) == evidence['chatModels'], provider
print('Pi v1.4.0 selected catalog provenance and projection verified (85 chat models)')
