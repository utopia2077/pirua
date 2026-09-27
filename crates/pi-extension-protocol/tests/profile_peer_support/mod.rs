#[path = "../profile_support/mod.rs"]
mod shared;
pub mod transport;
pub use shared::{bound as shared_bound, exposure, limits, schema};
pub fn registry() -> Registry {
    static R: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    R.get_or_init(shared::registry).clone()
}
use pi_extension_protocol::{
    activation::HostAuthority,
    descriptor::canonical_profile_digest,
    endpoint::{ActiveFrame, Endpoint, EndpointEvent},
    jcs,
    profile_live::*,
    profiles::{
        self, ApprovedProfile, NativeError, NativeValidator, ProfileApproval, Registry, Stage,
        ValidatedPayload,
    },
    wire::{v1_0, v1_1, value},
};
use prost::Message;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub fn corpus() -> Value {
    serde_json::from_slice(include_bytes!(
        "../../../../conformance/extension-protocol/profiles-runtime-v1.json"
    ))
    .unwrap()
}
pub fn method_wire() -> Value {
    serde_json::from_slice(include_bytes!(
        "../../schema/projections/method-wire-v1.json"
    ))
    .unwrap()
}
pub fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
pub fn sha(raw: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, raw)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub fn canonical(v: &Value) -> jcs::CanonicalJson {
    jcs::canonical_json(&serde_json::to_vec(v).unwrap(), jcs::Limits::default()).unwrap()
}
pub fn payload(method: &str, stage: &str, minor: u32) -> Vec<u8> {
    let w = method_wire();
    let row = w["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["method"] == method && x["minor"] == minor)
        .unwrap();
    unhex(row["payloads"][stage]["hex"].as_str().unwrap())
}
pub fn profile_name(method: &str) -> String {
    let r: Value = serde_json::from_slice(profiles::STATIC_REGISTRY).unwrap();
    r["canonical_wire_inventory"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, e)| {
            e["methods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["method"] == method)
        })
        .unwrap()
        .0
        .clone()
}
pub fn approved(minor: u32) -> Vec<ApprovedProfile> {
    static ZERO: std::sync::OnceLock<Vec<ApprovedProfile>> = std::sync::OnceLock::new();
    static ONE: std::sync::OnceLock<Vec<ApprovedProfile>> = std::sync::OnceLock::new();
    (if minor == 0 { &ZERO } else { &ONE })
        .get_or_init(|| build_approved(minor))
        .clone()
}
fn build_approved(minor: u32) -> Vec<ApprovedProfile> {
    let mut ps = Vec::new();
    for (p,e)in serde_json::from_slice::<Value>(profiles::STATIC_REGISTRY).unwrap()["canonical_wire_inventory"].as_object().unwrap(){let names=if p=="hook.v1" {e["methods"].as_array().unwrap().iter().map(|m|m["method"].as_str().unwrap()).collect()}else{vec![match p.as_str(){"provider.v1"=>"remote","resource.v1"=>"resources","host-action.v1"=>"actions","event-sink.v1"=>"events",_=>"metrics"}]};
 for name in names {ps.push(approval(p,name,minor));}}
    ps.push(approval("host-action.v1", "actions-resource", minor));
    ps
}
fn approval(p: &str, name: &str, minor: u32) -> ApprovedProfile {
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: p.trim_end_matches(".v1").into(),
        name: name.into(),
        profile: p.into(),
        handler: "qualified_handler".into(),
        minors: BTreeSet::from([minor]),
        replay: 1,
        execution: match p {
            "provider.v1" => 2,
            "event-sink.v1" | "telemetry-sink.v1" => 3,
            _ => 1,
        },
        scope: match (p, name) {
            ("host-action.v1", "actions-resource") => "allowed",
            ("host-action.v1", _) => "main",
            ("event-sink.v1", _) => "captured_scope",
            _ => "allowed",
        }
        .into(),
        permissions: BTreeSet::new(),
        exposure: exposure(),
    })
    .unwrap()
}
pub fn authority(core: u32, minor: u32, scenario: &str) -> HostAuthority {
    let permissions = corpus()["exposure"]["permission_scopes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| scenario != "host-ungranted" || p[0] != "queue.steer")
        .map(|p| v1_1::PermissionGrant {
            name: p[0].as_str().unwrap().into(),
            scope: p[1].as_str().unwrap().into(),
        })
        .collect();
    HostAuthority::new(
        "peer-a".into(),
        7,
        core,
        minor,
        limits(),
        vec![],
        permissions,
    )
    .unwrap()
    .with_profiles(approved(minor))
    .unwrap()
}
pub fn reservations() -> Reservations {
    Reservations {
        blocking_bytes: 1_048_576,
        control_bytes: 8_388_608,
        terminal_bytes: 8_388_608,
        progress_bytes: 1_048_576,
        passive_bytes: 1_048_576,
        telemetry_bytes: 1_048_576,
    }
}
pub fn handshake(
    peer: &mut transport::Peer,
    core: u32,
    minor: u32,
    scenario: &str,
) -> (Endpoint, Owner) {
    let mut e = Endpoint::new(authority(core, minor, scenario), 0);
    for now in 1..=2 {
        let raw = peer.recv();
        let s = e.read_one(&raw, now).unwrap();
        assert_eq!(s.consumed, raw.len());
        let Some(EndpointEvent::Send(reply)) = s.event else {
            panic!("handshake send")
        };
        peer.send(reply.bytes);
    }
    let raw = peer.recv();
    assert!(matches!(
        e.read_one(&raw, 3).unwrap().event,
        Some(EndpointEvent::Ready)
    ));
    let o = e.profile_owner(reservations(), 3, 20_000).unwrap();
    (e, o)
}
pub fn active(e: &mut Endpoint, raw: &[u8], now: u64) -> ActiveFrame {
    let s = e.read_one(raw, now).unwrap();
    assert_eq!(s.consumed, raw.len());
    let Some(EndpointEvent::Active(f)) = s.event else {
        panic!("active frame")
    };
    *f
}
pub fn key() -> v1_1::EffectKey {
    v1_1::EffectKey {
        session_id: "s".into(),
        lane: "main".into(),
        operation_id: "o".into(),
        effect_kind: "assistant".into(),
        step_or_task_id: "step".into(),
        logical_attempt: 0,
        item_index: Some(0),
    }
}
pub fn start(method: &str, minor: u32, now: u64, parent: Option<Binding>) -> Start {
    let p = profile_name(method);
    let name = if p == "hook.v1" {
        method
    } else {
        match p.as_str() {
            "provider.v1" => "remote",
            "resource.v1" => "resources",
            "event-sink.v1" => "events",
            "telemetry-sink.v1" => "metrics",
            _ => {
                if method == "resource.get" {
                    "actions-resource"
                } else {
                    "actions"
                }
            }
        }
    };
    let a = approved(minor)
        .into_iter()
        .find(|a| a.profile() == p && a.name() == name)
        .unwrap();
    let role = match p.as_str() {
        "hook.v1" if method == "before_run" => Role::Preaccept,
        "hook.v1" | "provider.v1" => Role::Planned,
        "host-action.v1" => Role::HostAction,
        _ => Role::Passive,
    };
    Start {
        capability: a.reference(minor).unwrap(),
        method: method.into(),
        direction: registry().method(&p, method).unwrap().0,
        role,
        key: if role == Role::Planned {
            Some(key())
        } else {
            None
        },
        parent,
        scope: a.scope().into(),
        payload: payload(method, "request", minor),
        now_ms: now,
        requested_deadline_ms: None,
        method_default_ms: 5_000,
        method_limit_ms: 10_000,
        initial_credit: None,
        generation: 7,
    }
}
#[derive(Default)]
pub struct Native {
    pub seen: Vec<ValidatedPayload>,
}
impl NativeValidator for Native {
    fn validate(&mut self, c: &ValidatedPayload) -> Result<(), NativeError> {
        if c.method == "provider.invoke" && c.stage == Stage::Terminal {
            let m = &c.projections["provider.assistant_message.v1"];
            if m["stopReason"] == "deferred"
                && (m["deferred"]["provider"] != m["provider"]
                    || m["deferred"]["modelId"] != m["model"]
                    || m["deferred"]["api"] != m["api"])
            {
                return Err(NativeError::Rejected);
            }
        }
        self.seen.push(c.clone());
        Ok(())
    }
}
pub fn expected(method: &str, stage: Stage, minor: u32) -> ValidatedPayload {
    let p = profile_name(method);
    let a = approved(minor)
        .into_iter()
        .find(|p| {
            p.profile() == profile_name(method) && (p.profile() != "hook.v1" || p.name() == method)
        })
        .unwrap();
    let mut n = Native::default();
    let bytes = payload(
        method,
        match stage {
            Stage::Request => "request",
            Stage::Progress => "progress",
            Stage::Terminal => "terminal",
        },
        minor,
    );
    let c = a
        .validate_payload(
            minor,
            method,
            stage,
            &bytes,
            value::Limits::default(),
            jcs::Limits::default(),
            &mut n,
        )
        .unwrap();
    assert_eq!(a.profile(), p);
    c
}
pub fn arm(p: &Value) -> v1_1::WireValue {
    use v1_1::wire_value::Value as W;
    let w = if let Some(v) = p.get("integer_value") {
        W::IntegerValue(v.as_i64().unwrap())
    } else if let Some(v) = p.get("float_value") {
        W::FloatValue(v.as_f64().unwrap())
    } else if let Some(v) = p.get("bool_value") {
        W::BoolValue(v.as_bool().unwrap())
    } else if let Some(v) = p.get("bytes_value") {
        W::BytesValue(
            v.as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_u64().unwrap() as u8)
                .collect(),
        )
    } else if let Some(v) = p.get("map_value") {
        W::MapValue(v1_1::WireMap {
            entries: v["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| v1_1::WireMapEntry {
                    key: e["key"].as_str().unwrap().into(),
                    value: e.get("value").map(arm),
                })
                .collect(),
        })
    } else if p.get("null_value").is_some() {
        W::NullValue(v1_1::NullValue {})
    } else {
        panic!("control arm")
    };
    v1_1::WireValue { value: Some(w) }
}
pub fn shape(r: &Registry, p: &Value) -> bool {
    let w = p
        .get("arm")
        .map_or_else(|| profiles::json_wire(&p["value"]).unwrap(), arm);
    let mut l = value::Limits::default();
    for (k, v) in p["limits"].as_object().into_iter().flatten() {
        let n = v.as_u64().unwrap() as usize;
        match k.as_str() {
            "depth" => l.depth = n,
            "elements" => l.elements = n,
            "decoded" => l.decoded_bytes = n,
            "string_bytes" => l.string_bytes = n,
            "work" => {}
            _ => panic!("limit"),
        }
    }
    r.validate_type(
        p["type"].as_str().unwrap(),
        &w,
        l,
        p["limits"]["work"].as_u64().unwrap_or(1_000_000) as usize,
    )
    .is_ok()
}
pub fn policy(r: &Registry, p: &Value) -> Value {
    let v = &p["value"];
    let valid = match p["op"].as_str().unwrap() {
        "shape" => shape(r, p),
        "bound" => {
            let b = v1_1::SchemaBoundValue {
                projection_id: p["projection"].as_str().unwrap().into(),
                schema_digest_sha256: unhex(p["digest"].as_str().unwrap()),
                value: p.get("value").map(|v| profiles::json_wire(v).unwrap()),
            };
            r.validate_v1_1(
                p["root"].as_str().unwrap(),
                &b,
                value::Limits::default(),
                1_000_000,
            )
            .is_ok()
        }
        "unit" => v.as_object().unwrap().is_empty(),
        "portable" => schema(p["schema"].clone())
            .validate_instance(&serde_json::to_vec(v).unwrap(), jcs::Limits::default())
            .is_ok(),
        "structural" => {
            shape(r, p)
                && !(v["decline"] == true
                    && (v.get("compaction").is_some() || v.get("summary").is_some()))
        }
        "auth_none" => v.get("headers").is_none(),
        "provider_progress" => {
            r.validate_type(
                "provider.assistant_event.v1",
                &profiles::json_wire(v).unwrap(),
                value::Limits::default(),
                1_000_000,
            )
            .is_ok()
                && !matches!(v["type"].as_str(), Some("done" | "error"))
        }
        "provider_terminal" => {
            r.validate_type(
                "provider.assistant_message.v1",
                &profiles::json_wire(v).unwrap(),
                value::Limits::default(),
                1_000_000,
            )
            .is_ok()
                && p.get("usage").is_none_or(|u| u == &v["usage"])
                && (v["stopReason"] != "deferred"
                    || (v["deferred"]["provider"] == v["provider"]
                        && v["deferred"]["modelId"] == v["model"]
                        && v["deferred"]["api"] == v["api"]))
                && (!matches!(v["stopReason"].as_str(), Some("error" | "aborted"))
                    || matches!(
                        v["errorMessage"].as_str(),
                        Some("request failed" | "operation failed")
                    ))
        }
        "tuples" => {
            v.as_array()
                .unwrap()
                .iter()
                .map(|x| x.to_string())
                .collect::<BTreeSet<_>>()
                .len()
                == v.as_array().unwrap().len()
        }
        "digest" => sha(v.as_str().unwrap().as_bytes()) == p["digest"],
        "canonical" => {
            let c = canonical(v);
            return json!({"canonical":std::str::from_utf8(c.bytes()).unwrap(),"sha256":sha(c.bytes())});
        }
        "parent" => {
            p["live"] == true
                && p["principal"] == "peer-a"
                && p["generation"] == 7
                && p["depth"].as_u64().unwrap() <= 8
                && p["cycle"] == false
        }
        "method" => {
            return json!(if p["method"] == "secret.resolve" {
                "broker_unavailable"
            } else if p["method"] == "unfamiliar.method" {
                "unsupported_method"
            } else if p["granted"] == true {
                "admit"
            } else {
                "forbidden"
            });
        }
        "exposure" => !v.to_string().contains("SENTINEL") && p["binding"] == true,
        "sequence" => v
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .all(|(i, n)| n.as_u64() == Some(p["baseline"].as_u64().unwrap() + i as u64)),
        "telemetry_attributes" => {
            let s: Value = serde_json::from_slice(profiles::STATIC_REGISTRY).unwrap();
            let fields = s["types"]["TelemetryAttributes"]["fields"]
                .as_object()
                .unwrap();
            let mut out = serde_json::Map::new();
            for (k, v) in v.as_object().unwrap() {
                if let Some(f) = fields.get(k) {
                    let t = &f["type"];
                    if t["enum"].as_array().is_some_and(|a| a.contains(v))
                        || t["ref"].as_str().is_some_and(|name| {
                            r.validate_type(
                                name,
                                &profiles::json_wire(v).unwrap(),
                                value::Limits::default(),
                                1000000,
                            )
                            .is_ok()
                        })
                    {
                        out.insert(k.clone(), v.clone());
                    }
                }
            }
            return Value::Object(out);
        }
        "ack" => {
            let accepted = v["accepted"]
                .as_u64()
                .or_else(|| v["accepted_decimal"].as_str().unwrap().parse().ok())
                .unwrap();
            accepted.checked_add(v["dropped"].as_u64().unwrap()) == p["count"].as_u64()
        }
        "flush" => {
            p["ack"].as_u64() <= p["emitted"].as_u64() && p["ack"].as_u64() <= p["through"].as_u64()
        }
        "json_duplicate" => {
            jcs::canonical_json(v.as_str().unwrap().as_bytes(), jcs::Limits::default()).is_ok()
        }
        "headers" => {
            let entries = v.as_array().unwrap();
            let mut counts = BTreeMap::new();
            for e in entries {
                *counts
                    .entry(e[0].as_str().unwrap().to_ascii_lowercase())
                    .or_insert(0usize) += 1;
            }
            return Value::Array(
                entries
                    .iter()
                    .filter(|e| {
                        let k = e[0].as_str().unwrap().to_ascii_lowercase();
                        counts[&k] == 1
                            && !matches!(
                                k.as_str(),
                                "authorization" | "cookie" | "set-cookie" | "x-api-key"
                            )
                    })
                    .cloned()
                    .collect(),
            );
        }
        "gap" => {
            let mut q = EventQueue::new("s1".into(), 7, 2, 64, 0, 0, 100).unwrap();
            for n in v.as_array().unwrap() {
                let n = n.as_u64().unwrap();
                q.publish(7, n, n, vec![1], n).unwrap();
            }
            let g = q
                .gap()
                .map(|g| json!({"first_dropped":g.first_dropped,"last_dropped":g.last_dropped}));
            return json!({"gap":g,"next":q.cursor(),"paused":q.paused()});
        }
        x => panic!("policy {x}"),
    };
    json!(valid)
}
pub fn digest_rows() -> Value {
    let w = method_wire();
    Value::Array((0..=1).map(|minor|{let mut digests=serde_json::Map::new();for name in ["common","hook","provider","resource","host_action","event_sink","telemetry_sink"]{let p=name.replace('_',"-")+".v1";let bytes=std::fs::read(transport::root().join(format!("crates/pi-extension-protocol/schema/descriptors/{name}-1.{minor}.pb"))).unwrap();let mut raw=format!("{p}\01.{minor}\0").into_bytes();raw.extend(bytes);digests.insert(p,json!(sha(&raw)));}let fields=w["field_witnesses"].as_array().unwrap().iter().filter(|x|x["minor"]==minor).map(|x|json!({"message":x["message"],"choice":x["oneof_choice"],"hex":x["hex"]})).collect::<Vec<_>>();json!({"minor":minor,"minor_omission_hex":if minor==0 {sha_hex(&v1_0::CapabilityDeclaration::default().encode_to_vec())}else{sha_hex(&v1_1::CapabilityDeclaration{display_label:Some("minor-one-only".into()),..Default::default()}.encode_to_vec())},"digests":digests,"fields":fields})}).collect())
}
pub fn descriptor_ref_check(p: &v1_1::CapabilityRef) {
    let v = p.profile.as_ref().unwrap();
    assert_eq!(
        p.schema_digest_sha256,
        canonical_profile_digest(&v.name, v.major, v.minor).unwrap()
    );
}

fn sha_hex(raw: &[u8]) -> String {
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

// The Python counterpart only imports the existing immutable production helpers.
const KEY_FIDELITY_PYTHON: &str = r#"
import sys,json
from pathlib import Path
repo,input_path=sys.argv[1:]
sys.path.insert(0,str(Path(repo)/'sdk/python'))
from profiles.wire import Codec,json_value,wire_value
from profiles.shapes import Shapes,canonical,portable
shapes=Shapes(repo)
def attempt(f):
    try:return {'value':f()}
    except Exception as e:return {'error':str(e)}
rows=[]
for row in json.loads(Path(input_path).read_text())['rows']:
    codec=Codec(repo,row['minor']);raw=bytes.fromhex(row['hex'])
    decoded=codec.decode(row['message'],raw)
    w=decoded if row['message']=='WireValue' else decoded.get('input',decoded.get('item'))['value']
    result={'id':row['id'],'roundtripHex':codec.encode(row['message'],decoded).hex(),
      'shape':attempt(lambda:shapes.check(row['type'],w)),
      'json':attempt(lambda:json_value(w)),
      'canonical':attempt(lambda:canonical(json_value(w))),
      'rebuiltHex':attempt(lambda:codec.encode('WireValue',wire_value(json_value(w))).hex())}
    if 'leaves' in row:result['exposed']=attempt(lambda:shapes.exposed(row['type'],w,row['leaves']))
    if 'portable' in row:result['portable']=attempt(lambda:portable(row['portable']['schema'],row['portable']['value']))
    rows.append(result)
print(json.dumps(rows,separators=(',',':')))
"#;

pub fn key_fidelity_row(id: &str, minor: u32, ty: &str, v: &Value) -> Value {
    json!({"id":format!("{minor}:{id}"),"minor":minor,"type":ty,"message":"WireValue",
        "hex":sha_hex(&profiles::json_wire(v).unwrap().encode_to_vec())})
}

pub fn key_fidelity_probe(language: &str, name: &str, rows: &[Value]) -> Value {
    use std::io::Read;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    fn kill_group(pid: u32) {
        let mut child = Command::new("/usr/bin/kill")
            .args(["-KILL", "--", &format!("-{pid}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let start = Instant::now();
        while child.try_wait().unwrap().is_none() {
            if start.elapsed() > Duration::from_secs(3) {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    struct Guard {
        child: Option<std::process::Child>,
        out: Option<std::thread::JoinHandle<Vec<u8>>>,
        err: Option<std::thread::JoinHandle<Vec<u8>>>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                kill_group(child.id());
                let _ = child.kill();
                let _ = child.wait();
            }
            for worker in [&mut self.out, &mut self.err] {
                if let Some(worker) = worker.take() {
                    let _ = worker.join();
                }
            }
        }
    }
    fn pipe(mut p: impl Read) -> Vec<u8> {
        let mut bytes = Vec::new();
        p.by_ref().take(262_145).read_to_end(&mut bytes).unwrap();
        assert!(bytes.len() <= 262_144, "key probe output bound");
        bytes
    }
    let base = std::env::var_os("PI_PROFILE_PEER_EVIDENCE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("profile-keys-{}", std::process::id()))
        });
    let directory = base.join(format!("{name}-{language}"));
    std::fs::create_dir_all(&directory).unwrap();
    let input = directory.join("input.json");
    let bytes = serde_json::to_vec(&json!({"rows":rows})).unwrap();
    assert!(bytes.len() <= 262_144);
    std::fs::write(&input, &bytes).unwrap();
    let mut command = if language == "node" {
        let mut c = transport::command("node");
        c.args(["--key-fidelity-probe", transport::root().to_str().unwrap()]);
        c
    } else {
        let mut c = Command::new("python3");
        c.args([
            "-B",
            "-c",
            KEY_FIDELITY_PYTHON,
            transport::root().to_str().unwrap(),
        ]);
        c.current_dir(transport::root());
        c
    };
    command
        .arg(&input)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let argv = format!("{command:?}");
    let mut child = command.spawn().unwrap();
    let pid = child.id();
    let out = child.stdout.take().unwrap();
    let err = child.stderr.take().unwrap();
    let mut guard = Guard {
        child: Some(child),
        out: None,
        err: None,
    };
    guard.out = Some(std::thread::spawn(move || pipe(out)));
    guard.err = Some(std::thread::spawn(move || pipe(err)));
    let start = Instant::now();
    let (status, forced) = loop {
        if let Some(status) = guard.child.as_mut().unwrap().try_wait().unwrap() {
            break (status, false);
        }
        if start.elapsed() > Duration::from_secs(3) {
            kill_group(pid);
            let _ = guard.child.as_mut().unwrap().kill();
            break (guard.child.as_mut().unwrap().wait().unwrap(), true);
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    kill_group(pid);
    guard.child.take();
    let out = guard.out.take().unwrap().join().unwrap();
    let err = guard.err.take().unwrap().join().unwrap();
    assert_eq!(
        std::fs::read(&input).unwrap(),
        bytes,
        "peer input bytes changed"
    );
    std::fs::write(directory.join("stdout.json"), &out).unwrap();
    std::fs::write(directory.join("stderr.log"), &err).unwrap();
    std::fs::write(
        directory.join("cleanup.json"),
        serde_json::to_vec_pretty(&json!({
            "pid":pid,"pgid":pid,"reaped":true,"group_cleanup":true,"forced":forced,
            "status":status.code(),"argv":argv,"elapsed_ms":start.elapsed().as_millis(),
            "input_sha256":sha(&bytes),"python_probe_sha256":sha(KEY_FIDELITY_PYTHON.as_bytes())
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(
        status.success() && !forced,
        "key probe normal completion: {status}, {}",
        String::from_utf8_lossy(&err)
    );
    serde_json::from_slice(&out).unwrap()
}
