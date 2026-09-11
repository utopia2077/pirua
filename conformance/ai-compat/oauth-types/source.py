#!/usr/bin/env python3
"""Validate the complete pinned type-only module and its export entry points."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

PIN = "853a80d26c90a14c1886f0ebb8ffaae133ca2185"
MODULE = "packages/ai/src/compat/extension-oauth-types.ts"
FIELDS = {
    "OAuthPrompt": ["message:string", "placeholder?:string", "allowEmpty?:boolean"],
    "OAuthAuthInfo": ["url:string", "instructions?:string"],
    "OAuthDeviceCodeInfo": ["userCode:string", "verificationUri:string", "intervalSeconds?:number", "expiresInSeconds?:number"],
    "OAuthSelectOption": ["id:string", "label:string"],
    "OAuthSelectPrompt": ["message:string", "options:OAuthSelectOption[]"],
    "OAuthLoginCallbacks": ["onAuth(info:OAuthAuthInfo):void", "onDeviceCode(info:OAuthDeviceCodeInfo):void", "onPrompt(prompt:OAuthPrompt):Promise<string>", "onProgress?(message:string):void", "onManualCodeInput?():Promise<string>", "onSelect(prompt:OAuthSelectPrompt):Promise<string|undefined>", "signal?:AbortSignal"],
}
EXPORTS = ["OAuthAuthInfo", "OAuthCredentials", "OAuthDeviceCodeInfo", "OAuthLoginCallbacks", "OAuthPrompt", "OAuthSelectOption", "OAuthSelectPrompt"]


def compact(text):
    return re.sub(r"\s+", "", text)


def shape(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    declarations = re.findall(r"export interface (\w+)\s*\{([^}]*)\}", text)
    names = [name for name, _ in declarations]
    assert names == list(FIELDS), "interface set/order changed"
    for name, body in declarations:
        fields = [compact(field) for field in body.split(";") if field.strip()]
        assert fields == FIELDS[name], f"{name} field/signature/optionality/order changed"
    rest = re.sub(r"export interface (\w+)\s*\{([^}]*)\}", "", text)
    assert compact(rest) == 'importtype{OAuthCredentials}from"../auth/types.ts";exporttype{OAuthCredentials};', "credential alias or extra declaration changed"
    return FIELDS


def export_names(text):
    blocks = re.findall(r'export type\s*\{([^}]+)\}\s*from\s*"\./compat/extension-oauth-types.ts";', text)
    assert len(blocks) == 1, "compatibility export block missing/duplicated"
    return [s.strip() for s in blocks[0].split(",") if s.strip()]


def capture():
    sources = {}
    for path in [MODULE, "packages/ai/src/oauth.ts", "packages/ai/src/index.ts"]:
        sources[path] = subprocess.check_output(["/usr/bin/git", "-C", "upstream/pi-mono", "show", f"{PIN}:{path}"], env={"PATH": "/usr/bin:/bin"}).decode()
    shape(sources[MODULE])
    assert export_names(sources["packages/ai/src/oauth.ts"]) == EXPORTS
    assert export_names(sources["packages/ai/src/index.ts"]) == [s for s in EXPORTS if s != "OAuthCredentials"]
    assert 'export * from "./auth/types.ts";' in sources["packages/ai/src/index.ts"]
    return sources, {"pin": PIN, "sha256": {p: hashlib.sha256(s.encode()).hexdigest() for p, s in sources.items()}, "declarations": FIELDS, "oauthExports": EXPORTS, "rootCompatExports": [s for s in EXPORTS if s != "OAuthCredentials"], "credentialOrigin": "packages/ai/src/auth/types.ts"}


def controls(source):
    assert "Legacy extension OAuth prompt" in source
    shape(source.replace("Legacy extension OAuth prompt", "Synthetic type comment"))
    shape(source.replace("message: string;", "message :  string ;"))
    variants = [
        ("required-progress", "onProgress?(", "onProgress("),
        ("select-no-undefined", "Promise<string | undefined>", "Promise<string>"),
        ("extra-description", "label: string;", "label: string; description?: string;"),
        ("required-allow-empty", "allowEmpty?:", "allowEmpty:"),
        ("async-auth", "onAuth(info: OAuthAuthInfo): void", "onAuth(info: OAuthAuthInfo): Promise<void>"),
        ("wrong-credential-origin", "../auth/types.ts", "../other.ts"),
        ("reordered-options", "id: string;\n\tlabel: string;", "label: string;\n\tid: string;"),
    ]
    for label, before, after in variants:
        assert source.count(before) == 1, label
        try:
            shape(source.replace(before, after))
        except AssertionError:
            print(f"{label}: rejected by declaration shape")
        else:
            raise AssertionError(f"{label}: accepted changed source shape")
    print("two positive shape controls admitted")


if __name__ == "__main__":
    sources, observed = capture()
    if sys.argv[1:] == ["--capture"]:
        print(json.dumps(observed, indent=2))
    else:
        assert sys.argv[1:] in (["--check"], ["--causal"])
        expected = json.loads(Path(__file__).with_name("source-shape.json").read_text())
        assert observed == expected, "whole-file fingerprint/declaration/export mismatch"
        if sys.argv[1:] == ["--causal"]:
            controls(sources[MODULE])
        else:
            print("seven symbols: complete pinned declaration and export proof matched")
