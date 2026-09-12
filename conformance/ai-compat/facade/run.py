#!/usr/bin/env python3
"""Actual linked facade test crate; no reverse production/dev dependency edge."""
import os,json,subprocess,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
with tempfile.TemporaryDirectory(prefix="pi-compat-facade-") as directory:
 p=Path(directory);(p/'src').mkdir()
 manifest='[package]\nname="compat-facade-probe"\nversion="0.0.0"\nedition="2024"\npublish=false\n[workspace]\n[features]\ndefault=[]\nnative=["pi-ai-transport/native"]\n[dependencies]\n'
 for name,features in [('pi-ai',['compat']),('pi-ai-providers',[]),('pi-ai-core',[]),('pi-ai-transport',[])]:manifest+=name+'={path='+json.dumps(str(ROOT/'crates'/name))+',default-features=false,features='+json.dumps(features)+'}\n'
 manifest+='serde_json="=1.0.151"\ntokio={version="=1.53.1",default-features=false,features=["rt-multi-thread","net","io-util","sync","time"]}\ntokio-tungstenite={version="=0.30.0",default-features=false,features=["connect","rustls-tls-webpki-roots"]}\n'
 (p/'Cargo.toml').write_text(manifest);(p/'src/lib.rs').write_text((ROOT/'conformance/ai-compat/facade/runtime.rs').read_text())
 env={**os.environ,'CARGO_TARGET_DIR':str(ROOT/'target/ai-conformance-probe'),'CARGO_PROFILE_DEV_DEBUG':'0','CARGO_PROFILE_TEST_DEBUG':'0','CARGO_BUILD_JOBS':'2'}
 command=['cargo','test','--manifest-path',str(p/'Cargo.toml'),'--offline','--lib','--no-run','--message-format=json']
 native='--native' in __import__('sys').argv
 if native:command+=['--features','native']
 result=subprocess.run(command,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 if result.returncode:print(result.stdout,result.stderr);raise SystemExit(result.returncode)
 artifacts=[]
 for line in result.stdout.splitlines():
  try:entry=json.loads(line)
  except json.JSONDecodeError:continue
  if entry.get('reason')=='compiler-artifact' and entry.get('manifest_path')==str(p/'Cargo.toml') and entry.get('profile',{}).get('test') and entry.get('executable'):
   artifacts.append(entry['executable'])
 assert len(artifacts)==1,artifacts
 names=['exact_registration_remembered_override_shortcut_and_binding_snapshot','captured_before_pending_env_explicit_bytes_marker_and_missing_api','all_sixteen_aliases_reach_real_families_with_typed_raw_options','image_upsert_cached_module_and_no_env_preserve_distinct_registry','cleanup_tickets_preserve_each_failure_task_drop_and_two_explicit_scopes','queued_task_lifetime_survives_scope_and_observer_drop','bedrock_module_retains_accepted_override_and_fallback','registry_host_identity_and_displaced_destructor_reenter_outside_lock','close_ticket_completion_wakes_reentrant_observer_outside_lock']
 if native:names+=['native_close::actual_facade_busy_close_and_new_same_session_own_distinct_physical_snapshots','native_close::never_polled_facade_close_drop_aborts_retired_native_socket']
 for name in names:
  result=subprocess.run([artifacts[0],'--exact',name,'--nocapture'],env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=30)
  print(result.stdout)
  assert result.returncode==0,(name,result.stdout)
  assert 'test '+name+' ... ok' in result.stdout,name
  assert 'test result: ok. 1 passed; 0 failed; 0 ignored;' in result.stdout,name
 print('ACTUAL_LINKED_FACADE_'+str(len(names))+'_EXACT_TESTS_PASSED')
