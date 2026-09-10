#!/usr/bin/env python3
"""Two bounded mutations must fail the unchanged complete-source comparison."""
import os
from pathlib import Path
import subprocess
root = Path(__file__).resolve().parents[3]
name = "complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages"
command = ["cargo", "test", "-p", "pi-ai-providers", "--features", "wire-openrouter-images", "--test", "openrouter_images", "--locked", "--offline", name, "--", "--exact"]
mutations = [
    ("callback-abort", "crates/pi-ai-providers/src/images/openrouter.rs", "    wire_response::reduce(value, &request.resolved.model, output)", "    if request.options.runtime.cancellation.is_cancelled() { return Err(Failure::Transport); }\n    wire_response::reduce(value, &request.resolved.model, output)", '"response-abort"'),
    ("missing-body", "crates/pi-ai-providers/src/images/wire_request.rs", "FetchBody::Absent", "FetchBody::Text(String::new())", '"payload-false"'),
]
for label, relative, before, after, witness in mutations:
    path = root / relative
    original = path.read_bytes()
    try:
        text = original.decode()
        assert text.count(before) == 1
        path.write_text(text.replace(before, after))
        result = subprocess.run(command, cwd=root, env=os.environ.copy(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert result.returncode != 0 and "test result: FAILED" in result.stdout and witness in result.stdout, result.stdout
        print(f"{label}: rejected by unchanged source fixture ({witness})")
    finally:
        path.write_bytes(original)
result = subprocess.run(command, cwd=root, env=os.environ.copy())
assert result.returncode == 0
print("Image causal controls restored and passing")
