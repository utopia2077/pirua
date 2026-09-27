//! Complete validated resource revisions; captured revisions never change.
use pi_agent_schema::{PromptTemplate, Resources, Skill};
use pi_extension_protocol::profiles::{Stage, ValidatedPayload};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// Own a completed passive reply through its native consumer's transfer or Drop.
/// Error, cancellation and unwind discard the payload before receipt retirement.
pub(super) struct ConsumerReply {
    connection: Arc<super::connection::ProfileConnection>,
    reply: Option<super::connection::Reply>,
}
impl ConsumerReply {
    pub(super) fn new(
        connection: Arc<super::connection::ProfileConnection>,
        reply: super::connection::Reply,
    ) -> Self {
        Self {
            connection,
            reply: Some(reply),
        }
    }
    pub(super) fn reply(&self) -> &super::connection::Reply {
        self.reply.as_ref().expect("owned completed reply")
    }
    fn retire(
        &mut self,
        reason: pi_extension_protocol::profile_live::RetireReason,
    ) -> Result<(), pi_agent_schema::HarnessRuntimeError> {
        let Some(reply) = self.reply.take() else {
            return Ok(());
        };
        let super::connection::Reply {
            binding,
            payload,
            error: _,
        } = reply;
        drop(payload);
        self.connection.retire(&binding, reason)
    }
    pub(super) fn consumed(mut self) -> Result<(), pi_agent_schema::HarnessRuntimeError> {
        self.retire(pi_extension_protocol::profile_live::RetireReason::Consumed)
    }
}
impl Drop for ConsumerReply {
    fn drop(&mut self) {
        self.retire(pi_extension_protocol::profile_live::RetireReason::NoLongerExpected)
            .ok();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Metadata {
    Skill(Skill),
    PromptTemplate(PromptTemplate),
}
pub fn metadata(root: &str, value: &Value) -> Result<Metadata, super::native::ResourceError> {
    let mut value = value.clone();
    value
        .as_object_mut()
        .ok_or(super::native::ResourceError)?
        .insert("content".into(), Value::String(String::new()));
    match root {
        "resource.metadata:skill" => serde_json::from_value(value)
            .map(Metadata::Skill)
            .map_err(|_| super::native::ResourceError),
        "resource.metadata:prompt_template" => serde_json::from_value(value)
            .map(Metadata::PromptTemplate)
            .map_err(|_| super::native::ResourceError),
        _ => Err(super::native::ResourceError),
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct ResourceValue {
    pub id: String,
    pub version: String,
    pub kind: String,
    pub digest: String,
    pub value: Value,
}
impl ResourceValue {
    pub fn from_json(value: &Value) -> Result<Self, super::native::ResourceError> {
        let text = |name: &str| {
            value[name]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .ok_or(super::native::ResourceError)
        };
        let digest = text("digest")?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(super::native::ResourceError);
        }
        Ok(Self {
            id: text("id")?,
            version: text("version")?,
            kind: text("kind")?,
            digest,
            value: value
                .get("value")
                .cloned()
                .ok_or(super::native::ResourceError)?,
        })
    }
    pub fn json(&self) -> Value {
        json!({"id":self.id,"version":self.version,"kind":self.kind,"digest":self.digest,"value":self.value})
    }
}
#[derive(Clone, Debug)]
pub struct ResourceRevision {
    revision: String,
    scope: String,
    values: BTreeMap<(String, String), (ResourceValue, Metadata)>,
}
impl ResourceRevision {
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub fn get(&self, id: &str, version: Option<&str>) -> Option<&ResourceValue> {
        self.values
            .iter()
            .find(|((name, v), _)| name == id && version.is_none_or(|expected| expected == v))
            .map(|(_, (value, _))| value)
    }
    pub fn resources(&self) -> Resources {
        let mut skills = Vec::new();
        let mut templates = Vec::new();
        for (value, metadata) in self.values.values() {
            match metadata {
                Metadata::Skill(skill) => {
                    let mut skill = skill.clone();
                    skill.content = value.value.as_str().expect("validated text").into();
                    skills.push(skill);
                }
                Metadata::PromptTemplate(template) => {
                    let mut template = template.clone();
                    template.content = value.value.as_str().expect("validated text").into();
                    templates.push(template);
                }
            }
        }
        Resources {
            skills: Some(skills),
            prompt_templates: Some(templates),
        }
    }
}
struct State {
    current: Option<Arc<ResourceRevision>>,
    refresh: bool,
    // None means the counter was exhausted; keep refresh pending rather than
    // equating an unknown later change with an earlier observation.
    change_sequence: Option<u64>,
}
#[derive(Clone, Copy)]
struct ChangeObservation(Option<u64>);
#[derive(Clone)]
pub struct ResourceCatalog {
    scope: String,
    state: Arc<Mutex<State>>,
}
impl ResourceCatalog {
    pub fn new(scope: String) -> Result<Self, super::native::ResourceError> {
        if scope.is_empty() {
            return Err(super::native::ResourceError);
        }
        Ok(Self {
            scope,
            state: Arc::new(Mutex::new(State {
                current: None,
                refresh: true,
                change_sequence: Some(0),
            })),
        })
    }
    pub fn changed(&self, scope: &str) -> Result<(), super::native::ResourceError> {
        if scope != self.scope {
            return Err(super::native::ResourceError);
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.change_sequence = state
            .change_sequence
            .and_then(|sequence| sequence.checked_add(1));
        state.refresh = true;
        Ok(())
    }
    fn observe_change(&self) -> ChangeObservation {
        ChangeObservation(
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .change_sequence,
        )
    }
    pub fn refresh_pending(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).refresh
    }
    pub fn capture(&self) -> Result<Arc<ResourceRevision>, super::native::ResourceError> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .current
            .clone()
            .ok_or(super::native::ResourceError)
    }
    /// All reads must be the native-admitted first terminal for exactly their
    /// complete manifest tuple and expected digest. Publication is one swap.
    pub fn publish(
        &self,
        list: &ValidatedPayload,
        reads: &[ValidatedPayload],
    ) -> Result<Arc<ResourceRevision>, super::native::ResourceError> {
        Ok(self.install(self.prepare(list, reads)?, None))
    }
    fn prepare(
        &self,
        list: &ValidatedPayload,
        reads: &[ValidatedPayload],
    ) -> Result<Arc<ResourceRevision>, super::native::ResourceError> {
        if list.method != "resource.list" || list.stage != Stage::Terminal {
            return Err(super::native::ResourceError);
        }
        let revision = list.fields["revision"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(super::native::ResourceError)?
            .to_owned();
        let manifests = list.fields["resources"]
            .as_array()
            .ok_or(super::native::ResourceError)?;
        if manifests.len() != reads.len() {
            return Err(super::native::ResourceError);
        }
        let mut values = BTreeMap::new();
        for (manifest, read) in manifests.iter().zip(reads) {
            if read.method != "resource.read"
                || read.stage != Stage::Terminal
                || read.fields["digest"] != manifest["digest"]
            {
                return Err(super::native::ResourceError);
            }
            let id = manifest["id"]
                .as_str()
                .ok_or(super::native::ResourceError)?
                .to_owned();
            let version = manifest["version"]
                .as_str()
                .ok_or(super::native::ResourceError)?
                .to_owned();
            let kind = manifest["kind"]
                .as_str()
                .ok_or(super::native::ResourceError)?;
            let metadata = metadata(&format!("resource.metadata:{kind}"), &manifest["metadata"])?;
            let text = read.fields["text"]
                .as_str()
                .ok_or(super::native::ResourceError)?
                .to_owned();
            let value = ResourceValue::from_json(
                &json!({"id":id,"version":version,"kind":kind,"digest":manifest["digest"],"value":text}),
            )?;
            if values.insert((id, version), (value, metadata)).is_some() {
                return Err(super::native::ResourceError);
            }
        }
        Ok(Arc::new(ResourceRevision {
            revision,
            scope: self.scope.clone(),
            values,
        }))
    }
    fn install(
        &self,
        revision: Arc<ResourceRevision>,
        observed: Option<ChangeObservation>,
    ) -> Arc<ResourceRevision> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.current = Some(revision.clone());
        // A direct public publish retains its established clearing behavior.
        // A fetched revision clears only the changes observed before its fetch.
        state.refresh = match observed {
            None => false,
            Some(ChangeObservation(Some(sequence))) => state.change_sequence != Some(sequence),
            Some(ChangeObservation(None)) => true,
        };
        revision
    }
    /// A restore has no remote digest proof. The caller must explicitly repin
    /// by fetching a complete revision before this succeeds.
    pub fn restored_capture(
        &self,
        proof: Option<&Arc<ResourceRevision>>,
    ) -> Result<Arc<ResourceRevision>, super::native::ResourceError> {
        let current = self.capture()?;
        if proof.is_none_or(|proof| !Arc::ptr_eq(proof, &current)) {
            return Err(super::native::ResourceError);
        }
        Ok(current)
    }
}

impl<
    C: Send + Sync + 'static,
    S: pi_agent_session::storage::RuntimeStorage + 'static,
    M: pi_agent_schema::SessionMetadataRecord,
> crate::owner::RuntimeHarness<C, S, M>
{
    /// Fetch and validate a complete remote revision, then atomically install
    /// its typed native Resources. Existing runs retain their earlier Arc.
    pub async fn refresh_profile_resources(
        &self,
        connection: Arc<super::connection::ProfileConnection>,
        capability: pi_extension_protocol::profiles::wire_v1_1::CapabilityRef,
        catalog: &ResourceCatalog,
    ) -> Result<Arc<ResourceRevision>, pi_agent_schema::HarnessRuntimeError> {
        use pi_extension_protocol::{
            profile_live::{Role, encode_payload},
            profiles::wire_v1_1 as dto,
        };
        self.main.owner.admission.check()?;
        connection.register_resource_catalog(&capability, catalog.clone())?;
        let observed_change = catalog.observe_change();
        let policy = pi_ai_core::ModelHeaderPolicy::default();
        let mut native = super::native::NativeDomain::new(self.main.owner.session.codec(), &policy);
        let cancel = pi_ai_core::CancellationToken::new();
        let scope = || super::connection::CallScope {
            key: None,
            role: Role::Passive,
            parent: None,
            native_parent: None,
            permit: None,
            slot: None,
            dispatch: None,
        };
        let payload = encode_payload(
            &dto::ResourceListRequest {
                scope: catalog.scope.clone(),
                known_revision: None,
            },
            connection.max_payload_bytes(),
        )
        .map_err(|_| super::fault())?;
        let list = ConsumerReply::new(
            connection.clone(),
            connection
                .call(
                    capability.clone(),
                    "resource.list",
                    payload,
                    scope(),
                    &mut native,
                    None,
                    &cancel,
                )
                .await?,
        );
        let list_value = list
            .reply()
            .payload
            .as_ref()
            .filter(|_| list.reply().error.is_none())
            .ok_or_else(super::fault)?;
        let mut replies = vec![];
        for manifest in list_value.fields["resources"]
            .as_array()
            .ok_or_else(super::fault)?
        {
            let digest = manifest["digest"].as_str().ok_or_else(super::fault)?;
            let digest = (0..digest.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&digest[i..i + 2], 16).map_err(|_| super::fault()))
                .collect::<Result<Vec<_>, _>>()?;
            let request = dto::ResourceReadRequest {
                id: manifest["id"].as_str().ok_or_else(super::fault)?.into(),
                version: manifest["version"]
                    .as_str()
                    .ok_or_else(super::fault)?
                    .into(),
                expected_digest_sha256: digest,
            };
            let payload = encode_payload(&request, connection.max_payload_bytes())
                .map_err(|_| super::fault())?;
            let reply = ConsumerReply::new(
                connection.clone(),
                connection
                    .call(
                        capability.clone(),
                        "resource.read",
                        payload,
                        scope(),
                        &mut native,
                        None,
                        &cancel,
                    )
                    .await?,
            );
            if reply.reply().error.is_some() {
                return Err(super::fault());
            }
            replies.push(reply);
        }
        let reads = replies
            .iter()
            .map(|r| r.reply().payload.clone().ok_or_else(super::fault))
            .collect::<Result<Vec<_>, _>>()?;
        let revision = catalog
            .prepare(list_value, &reads)
            .map_err(|_| super::fault())?;
        self.install_profile_resources(revision.clone()).await?;
        let revision = catalog.install(revision, Some(observed_change));
        drop(reads);
        list.consumed()?;
        for reply in replies {
            reply.consumed()?;
        }
        Ok(revision)
    }
}
