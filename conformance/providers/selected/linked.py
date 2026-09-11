#!/usr/bin/env python3
"""Run the same causal assembly cases through actual linked pi_ai::builtins."""
import os
from pathlib import Path
import subprocess
import tempfile
import json
import shutil
ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "crates/pi-ai-providers/tests/selected_factories.rs"
with tempfile.TemporaryDirectory(prefix="pi-selected-linked-") as directory:
    temp = Path(directory)
    manifest = '''[package]
name="selected-facade-probe"
version="0.0.0"
edition="2024"
publish=false
[workspace]
[features]
default=["all-providers","all-images","all-oauth"]
all-providers=[]
all-images=[]
all-oauth=[]
oauth-anthropic=[]
oauth-openai-codex=[]
oauth-openrouter=[]
[dependencies]
'''
    for crate, features in (("pi-ai", ["all-providers", "all-images", "all-oauth"]), ("pi-ai-providers", ["all-providers", "all-images", "all-oauth"]), ("pi-ai-core", []), ("pi-ai-transport", [])):
        manifest += f'{crate}={{path={json.dumps(str(ROOT / "crates" / crate))},features={json.dumps(features)}}}\n'
    manifest += 'serde_json="=1.0.151"\ntokio={version="=1.53.1",features=["rt","time","sync"]}\n'
    (temp / "Cargo.toml").write_text(manifest)
    source = SOURCE.read_text()
    marker = '#[path = "../../pi-ai/src/builtins.rs"]\nmod builtins;'
    assert source.count(marker) == 1
    source = source.replace(marker, 'use pi_ai::builtins;')
    source = source.replace('../../../conformance/providers/selected/trace.json', str(ROOT / 'conformance/providers/selected/trace.json'))
    (temp / "src").mkdir()
    (temp / "src/lib.rs").write_text(source)
    env = {**os.environ, "CARGO_TARGET_DIR": str(ROOT / "target/ai-conformance-probe"), "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0", "CARGO_BUILD_JOBS": "2"}
    result = subprocess.run(["cargo", "test", "--manifest-path", str(temp / "Cargo.toml"), "--all-features", "--offline", "--lib"], env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout)
    if result.returncode:
        raise SystemExit(result.returncode)
    assert '5 passed; 0 failed' in result.stdout
    print('ACTUAL_LINKED_FACADE_FIVE_TESTS_PASSED')
    for feature in ("catalog-openai", "provider-deepseek", "images-runtime", "images-openrouter"):
        command = ["cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "-p", "pi-ai", "--no-default-features", "--features", feature, "--test", "selected_catalog_profiles", "--offline"]
        result = subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(feature, result.stdout)
        if result.returncode:
            raise SystemExit(result.returncode)
        assert "actual_facade_catalog_selection_order_unknowns_and_capture_timestamp ... ok" in result.stdout
