#!/usr/bin/env python3
"""Mutations must fail unchanged complete-source runtime comparisons."""
import os
from pathlib import Path
import subprocess
root=Path(__file__).resolve().parents[3]
generation='complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge'
refresh='source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries'
live='complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry'
nested='complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing'
base=['cargo','test','-p','pi-ai-providers','--test','images_runtime','--features','wire-openrouter-images','--locked','--offline']
models='crates/pi-ai-providers/src/images/models.rs'
provider='crates/pi-ai-providers/src/images/provider.rs'
mutations=[
 ('catch-no-auth-rejection',models,generation,'            } else {\n                call.await\n','            } else {\n                Ok(call.await.unwrap_or_else(|_| failure.output()))\n'),
 ('fold-header-case',models,generation,'find(|(name, _)| name == k)','find(|(name, _)| name.eq_ignore_ascii_case(k))'),
 ('retry-synchronous-throw',provider,refresh,'Err(error) => guard.cache_launch_failure(error)','Err(error) => guard.finish(Err(error))'),
 ('cache-asynchronous-rejection',provider,refresh,'guard.finish(future.await);','match future.await { Ok(models) => guard.finish(Ok(models)), Err(error) => guard.cache_launch_failure(error) }'),
 ('snapshot-listing',models,live,'let mut models = Vec::new();\n        for provider in self.live_providers() {','let mut models = Vec::new();\n        for provider in self.get_providers() {'),
 ('snapshot-refresh-mapper',models,live,'let mut calls = Vec::new();\n                    for provider in self.live_providers() {','let mut calls = Vec::new();\n                    for provider in self.get_providers() {'),
 ('poll-before-admission-completes',models,live,'let gate = Arc::new(AtomicBool::new(false));','let gate = Arc::new(AtomicBool::new(true));'),
 ('join-same-thread-launch',provider,nested,'launch.thread != thread','true'),
 ('fence-nested-completion',provider,nested,'let flight = catalog.inflight.take();','let flight = if catalog.inflight.as_ref().is_some_and(|v| Arc::ptr_eq(v, &self.completion)) { catalog.inflight.take() } else { None };'),
]
for label,relative,name,before,after in mutations:
 path=root/relative
 original=path.read_bytes()
 try:
  text=original.decode();assert text.count(before)==1
  path.write_text(text.replace(before,after))
  result=subprocess.run(base+[name,'--','--exact'],cwd=root,env=os.environ.copy(),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
  assert result.returncode!=0 and 'test result: FAILED' in result.stdout and name in result.stdout,result.stdout
  print(label+': rejected by unchanged source comparison')
 finally:path.write_bytes(original)
result=subprocess.run(base,cwd=root,env=os.environ.copy());assert result.returncode==0
print('Images runtime causal controls restored and passing')
