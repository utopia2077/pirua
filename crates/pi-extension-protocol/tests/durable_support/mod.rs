#![allow(dead_code)]
use pi_extension_protocol::{
    activation::HostAuthority,
    descriptor::canonical_profile_digest,
    host::{self, HostConnection, OrderedTransport, TransportFuture},
    profiles::{ApprovedProfile, Exposure, ProfileApproval, Registry},
    wire::{
        framing::{self, FrameReader},
        v1_1 as w,
    },
};
use prost::Message;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc};
pub struct Pipe {
    pub tx: mpsc::Sender<Vec<u8>>,
    pub rx: Mutex<mpsc::Receiver<Vec<u8>>>,
}
impl OrderedTransport for Pipe {
    fn send(&self, b: Vec<u8>) -> TransportFuture<'_, ()> {
        Box::pin(async move { self.tx.send(b).await.map_err(|_| host::Error::Closed) })
    }
    fn receive(&self) -> TransportFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move { Ok(self.rx.lock().await.recv().await) })
    }
}
pub struct Peer {
    pipe: Arc<Pipe>,
    reader: FrameReader<w::Envelope>,
    tail: Vec<u8>,
}
impl Peer {
    pub async fn send(&self, f: w::envelope::Frame) {
        self.pipe.send(frame(f)).await.unwrap()
    }
    pub async fn recv(&mut self) -> w::envelope::Frame {
        loop {
            if self.tail.is_empty() {
                self.tail = self.pipe.receive().await.unwrap().unwrap();
            }
            let s = self.reader.read_one(&self.tail).unwrap();
            self.tail.drain(..s.consumed);
            if let Some(f) = s.frame {
                return f.envelope.frame.unwrap();
            }
        }
    }
    pub async fn answer(&self, id: u64, method: &str, value: Value, stream: bool) {
        let b = host::bound(
            &Registry::durable().unwrap(),
            &format!("durable.output:{method}"),
            &value,
        )
        .unwrap()
        .encode_to_vec();
        self.send(if stream {
            w::envelope::Frame::StreamEnd(w::StreamEnd {
                request_id: id,
                terminal: Some(w::stream_end::Terminal::Result(b)),
            })
        } else {
            w::envelope::Frame::Response(w::Response {
                request_id: id,
                terminal: Some(w::response::Terminal::Result(b)),
            })
        })
        .await
    }
    pub async fn progress(&self, id: u64, sequence: u64, value: Value) {
        let b = host::bound(
            &Registry::durable().unwrap(),
            "durable.progress:tool.call",
            &value,
        )
        .unwrap()
        .encode_to_vec();
        self.send(w::envelope::Frame::StreamItem(w::StreamItem {
            request_id: id,
            sequence,
            payload: b,
        }))
        .await;
    }
    pub async fn handshake(&mut self, names: &[&str], scope: &str, extra: bool) {
        self.send(w::envelope::Frame::Hello(w::Hello {
            protocol: Some(w::VersionRange {
                major: 1,
                min_minor: 1,
                max_minor: 1,
            }),
            supported_profiles: vec![w::ProfileRange {
                name: "durable.v1".into(),
                major: 1,
                min_minor: 1,
                max_minor: 1,
            }],
        }))
        .await;
        assert!(matches!(self.recv().await, w::envelope::Frame::HelloAck(_)));
        let mut caps = names
            .iter()
            .map(|n| declaration(n, false))
            .collect::<Vec<_>>();
        if extra {
            caps.push(declaration("manifest_only", false));
        }
        self.send(w::envelope::Frame::Manifest(w::Manifest {
            declared_extension_id: "claims-admin".into(),
            extension_version: "current".into(),
            protocol: Some(w::VersionRange {
                major: 1,
                min_minor: 1,
                max_minor: 1,
            }),
            capabilities: caps,
            permissions: if names.contains(&"session.snapshot") {
                vec![w::PermissionRequest {
                    name: "session.snapshot".into(),
                    scope: scope.into(),
                    required: true,
                }]
            } else {
                vec![]
            },
            subscriptions: vec![],
        }))
        .await;
        let w::envelope::Frame::Grant(g) = self.recv().await else {
            panic!("expected grant")
        };
        assert_eq!(g.authenticated_extension_id, "peer-a");
        assert_eq!(g.selected_capabilities.len(), names.len());
        self.send(w::envelope::Frame::Ready(w::Ready {})).await;
    }
}
pub fn declaration(name: &str, required: bool) -> w::CapabilityDeclaration {
    w::CapabilityDeclaration {
        kind: "durable".into(),
        name: name.into(),
        profile: Some(w::ProfileRange {
            name: "durable.v1".into(),
            major: 1,
            min_minor: 1,
            max_minor: 1,
        }),
        handler_id: format!("handler:{name}"),
        replay: 1,
        execution: if name.starts_with("tool:") { 2 } else { 1 },
        required,
        supported_descriptor_digests: vec![w::SupportedProfileDigest {
            minor: 1,
            descriptor_digest_sha256: canonical_profile_digest("durable.v1", 1, 1)
                .unwrap()
                .to_vec(),
        }],
        display_label: None,
        tool_schema_sets: vec![],
    }
}
pub fn limits() -> w::ProtocolLimits {
    w::ProtocolLimits {
        pre_handshake_frame_bytes: 262144,
        frame_bytes: 262144,
        aggregate_decoded_bytes: 1048576,
        value_depth: 64,
        value_elements: 16384,
        string_bytes: 262144,
        bytes_length: 262144,
        in_flight_requests: 8,
        blocking_queue_bytes: 1048576,
        progress_queue_bytes: 1048576,
        passive_queue_bytes: 1048576,
        telemetry_queue_bytes: 1048576,
        nesting_depth: 8,
        requests_per_second: 100,
        handshake_ms: 1000,
        cancellation_grace_ms: 20,
        max_stream_credit_bytes: 1048576,
    }
}
pub fn authority(names: &[&str], scope: &str) -> HostAuthority {
    authority_with_limits(names, scope, limits())
}
pub fn authority_with_limits(
    names: &[&str],
    scope: &str,
    protocol_limits: w::ProtocolLimits,
) -> HostAuthority {
    let permissions = if names.contains(&"session.snapshot") {
        vec![w::PermissionGrant {
            name: "session.snapshot".into(),
            scope: scope.into(),
        }]
    } else {
        vec![]
    };
    let profiles = names
        .iter()
        .map(|n| {
            ApprovedProfile::durable(ProfileApproval {
                principal: "peer-a".into(),
                kind: "durable".into(),
                name: (*n).into(),
                profile: "durable.v1".into(),
                handler: format!("handler:{n}"),
                minors: BTreeSet::from([1]),
                replay: 1,
                execution: if n.starts_with("tool:") { 2 } else { 1 },
                scope: scope.into(),
                permissions: permissions
                    .iter()
                    .map(|p| (p.name.clone(), p.scope.clone()))
                    .collect(),
                exposure: Exposure::new(BTreeMap::new(), BTreeSet::new()).unwrap(),
            })
            .unwrap()
        })
        .collect();
    HostAuthority::new(
        "peer-a".into(),
        7,
        1,
        1,
        protocol_limits,
        vec![],
        permissions,
    )
    .unwrap()
    .with_profiles(profiles)
    .unwrap()
}
pub fn pair() -> (Arc<Pipe>, Peer) {
    let (at, ar) = mpsc::channel(32);
    let (bt, br) = mpsc::channel(32);
    (
        Arc::new(Pipe {
            tx: at,
            rx: Mutex::new(br),
        }),
        Peer {
            pipe: Arc::new(Pipe {
                tx: bt,
                rx: Mutex::new(ar),
            }),
            reader: FrameReader::new(NonZeroUsize::new(262144).unwrap()),
            tail: vec![],
        },
    )
}
pub fn frame(f: w::envelope::Frame) -> Vec<u8> {
    framing::encode(
        &w::Envelope { frame: Some(f) },
        NonZeroUsize::new(262144).unwrap(),
    )
    .unwrap()
    .bytes
}
pub async fn ready(names: &[&str], scope: &str) -> (HostConnection, Peer) {
    let (channel, mut peer) = pair();
    let owned = names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    let s = scope.to_string();
    let t = tokio::spawn(async move {
        let refs = owned.iter().map(String::as_str).collect::<Vec<_>>();
        peer.handshake(&refs, &s, true).await;
        peer
    });
    let c = HostConnection::connect(
        authority(names, scope),
        channel,
        host::ConnectionConfig {
            io_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            lifetime: Duration::from_secs(30),
        },
    )
    .await
    .unwrap();
    (c, t.await.unwrap())
}
pub fn value(bytes: &[u8]) -> Value {
    let b = w::SchemaBoundValue::decode(bytes).unwrap();
    let root = b.projection_id.clone();
    Registry::durable()
        .unwrap()
        .validate_v1_1(&root, &b, Default::default(), 1_000_000)
        .unwrap()
}
pub fn input() -> Value {
    json!({"text":"old","images":[],"source":"interactive"})
}
pub fn validator() -> host::Validator {
    Arc::new(|p| {
        if p.fields.is_object() {
            Ok(())
        } else {
            Err(pi_extension_protocol::profiles::NativeError::Rejected)
        }
    })
}
