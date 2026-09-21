//! Host-established, immutable activation for the first grantable profile (`tool.v1`).
//!
//! The transport authenticates the principal before constructing `HostAuthority`.
//! Peer bytes only advertise capabilities; they never create catalog entries,
//! permissions, limits, identities, or executable schemas. This module ends at
//! Ready and does not dispatch invocations or settle effects.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use prost::Message;
use ring::digest::{SHA256, digest};
use serde_json::Value;

use crate::jcs::{self, Limits as JsonLimits, PortableSchema};
use crate::wire::{v1_0, v1_1};

const PROFILE: &str = "tool.v1";
const MAX_DECLARATIONS: usize = 256;
const MAX_ADVERTISED_MINORS: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivationError {
    pub reason: &'static str,
}

impl ActivationError {
    const fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}

impl fmt::Display for ActivationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason)
    }
}

impl std::error::Error for ActivationError {}

type Result<T> = std::result::Result<T, ActivationError>;

/// Model-visible metadata. Its authority is the host catalog, never a manifest.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentTool {
    pub name: String,
    pub label: String,
    pub description: String,
    pub parameters: Value,
    pub execution_mode: AgentToolMode,
    pub replay: ToolReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentToolMode {
    Sequential,
    Parallel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolReplay {
    Safe,
    Never,
}

/// Host-owned JSON sources for one selected tool minor.
pub struct ToolSchemaInputs {
    pub minor: u32,
    pub arguments: Vec<u8>,
    pub progress: Vec<u8>,
    pub raw_result: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Projection {
    id: String,
    digest: [u8; 32],
    schema: PortableSchema,
}

impl Projection {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn schema(&self) -> &PortableSchema {
        &self.schema
    }
}

#[derive(Clone, Debug)]
pub struct ToolProjections {
    arguments: Projection,
    progress: Projection,
    raw_result: Projection,
}

impl ToolProjections {
    pub fn arguments(&self) -> &Projection {
        &self.arguments
    }
    pub fn progress(&self) -> &Projection {
        &self.progress
    }
    pub fn raw_result(&self) -> &Projection {
        &self.raw_result
    }
}

#[derive(Clone, Debug)]
struct ApprovedMinor {
    profile_digest: [u8; 32],
    projections: ToolProjections,
}

/// An immutable host catalog entry qualified by authenticated principal.
#[derive(Clone, Debug)]
pub struct ApprovedTool {
    principal: String,
    descriptor: AgentTool,
    minors: BTreeMap<u32, ApprovedMinor>,
}

impl ApprovedTool {
    pub fn new(
        principal: String,
        descriptor: AgentTool,
        schema_inputs: Vec<ToolSchemaInputs>,
        json_limits: JsonLimits,
    ) -> Result<Self> {
        if principal.is_empty()
            || descriptor.name.is_empty()
            || descriptor.label.is_empty()
            || descriptor.description.is_empty()
            || schema_inputs.is_empty()
            || schema_inputs.len() > 2
        {
            return Err(ActivationError::new("host_descriptor_invalid"));
        }
        let mut minors = BTreeMap::new();
        for input in schema_inputs {
            let Some(profile_digest) = profile_digest(input.minor) else {
                return Err(ActivationError::new("host_descriptor_invalid"));
            };
            let arguments =
                projection(&descriptor.name, "arguments", &input.arguments, json_limits)?;
            let progress = projection(&descriptor.name, "progress", &input.progress, json_limits)?;
            let raw_result =
                projection(&descriptor.name, "result", &input.raw_result, json_limits)?;
            let parameters: Value = serde_json::from_slice(arguments.schema.canonical().bytes())
                .map_err(|_| ActivationError::new("host_parameters_invalid"))?;
            if descriptor.parameters != parameters {
                return Err(ActivationError::new("host_parameters_invalid"));
            }
            if minors
                .insert(
                    input.minor,
                    ApprovedMinor {
                        profile_digest,
                        projections: ToolProjections {
                            arguments,
                            progress,
                            raw_result,
                        },
                    },
                )
                .is_some()
            {
                return Err(ActivationError::new("host_descriptor_invalid"));
            }
        }
        Ok(Self {
            principal,
            descriptor,
            minors,
        })
    }

    pub fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
}

fn projection(tool: &str, part: &str, source: &[u8], limits: JsonLimits) -> Result<Projection> {
    let schema = jcs::admit_schema(source, limits)
        .map_err(|_| ActivationError::new("host_descriptor_invalid"))?;
    Ok(Projection {
        id: format!("tool.{part}:{tool}"),
        digest: schema.canonical().sha256(),
        schema,
    })
}

fn profile_digest(minor: u32) -> Option<[u8; 32]> {
    let descriptor: &[u8] = match minor {
        0 => include_bytes!("../schema/descriptors/tool-1.0.pb"),
        1 => include_bytes!("../schema/descriptors/tool-1.1.pb"),
        _ => return None,
    };
    let mut bytes = Vec::with_capacity(PROFILE.len() + 8 + descriptor.len());
    bytes.extend_from_slice(PROFILE.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(format!("1.{minor}").as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(descriptor);
    Some(
        digest(&SHA256, &bytes)
            .as_ref()
            .try_into()
            .expect("SHA-256 length"),
    )
}

/// Host-supplied ceiling. Its private fields cannot be widened by a peer.
#[derive(Clone, Debug)]
pub struct HostAuthority {
    principal: String,
    generation: u64,
    core_max_minor: u32,
    tool_max_minor: u32,
    limits: v1_1::ProtocolLimits,
    tools: BTreeMap<String, ApprovedTool>,
    permissions: BTreeSet<(String, String)>,
}

impl HostAuthority {
    pub fn new(
        principal: String,
        generation: u64,
        core_max_minor: u32,
        tool_max_minor: u32,
        limits: v1_1::ProtocolLimits,
        tools: Vec<ApprovedTool>,
        permissions: Vec<v1_1::PermissionGrant>,
    ) -> Result<Self> {
        if principal.is_empty() || generation == 0 || core_max_minor > 1 || tool_max_minor > 1 {
            return Err(ActivationError::new("host_authority_invalid"));
        }
        validate_limits(&limits)?;
        if tools.len() > MAX_DECLARATIONS || permissions.len() > MAX_DECLARATIONS {
            return Err(ActivationError::new("host_authority_invalid"));
        }
        let mut catalog = BTreeMap::new();
        for tool in tools {
            if catalog.insert(tool.descriptor.name.clone(), tool).is_some() {
                return Err(ActivationError::new("host_authority_invalid"));
            }
        }
        let mut ceiling = BTreeSet::new();
        for permission in permissions {
            if permission.name.is_empty()
                || permission.scope.is_empty()
                || !ceiling.insert((permission.name, permission.scope))
            {
                return Err(ActivationError::new("host_authority_invalid"));
            }
        }
        Ok(Self {
            principal,
            generation,
            core_max_minor,
            tool_max_minor,
            limits,
            tools: catalog,
            permissions: ceiling,
        })
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn limits(&self) -> &v1_1::ProtocolLimits {
        &self.limits
    }

    /// JSON work ceilings derived from the same finite host frame/value budget.
    pub fn json_limits(&self) -> JsonLimits {
        let decoded = self.limits.aggregate_decoded_bytes as usize;
        let elements = self.limits.value_elements.min(usize::MAX as u64) as usize;
        JsonLimits {
            input_bytes: self.limits.frame_bytes as usize,
            depth: self.limits.value_depth as usize,
            nodes: elements,
            elements,
            decoded_bytes: decoded,
            output_bytes: decoded,
            schema_work: decoded.saturating_mul(8).min(usize::MAX - 1),
            instance_work: decoded.saturating_mul(8).min(usize::MAX - 1),
        }
    }
}

fn validate_limits(l: &v1_1::ProtocolLimits) -> Result<()> {
    let numbers = [
        l.pre_handshake_frame_bytes,
        l.frame_bytes,
        l.aggregate_decoded_bytes,
        u64::from(l.value_depth),
        l.value_elements,
        l.string_bytes,
        l.bytes_length,
        u64::from(l.in_flight_requests),
        l.blocking_queue_bytes,
        l.progress_queue_bytes,
        l.passive_queue_bytes,
        l.telemetry_queue_bytes,
        u64::from(l.nesting_depth),
        u64::from(l.requests_per_second),
        l.handshake_ms,
        l.cancellation_grace_ms,
        l.max_stream_credit_bytes,
    ];
    if numbers.contains(&0)
        || numbers.contains(&u64::MAX)
        || l.value_depth == u32::MAX
        || l.in_flight_requests == u32::MAX
        || l.nesting_depth == u32::MAX
        || l.requests_per_second == u32::MAX
        || l.pre_handshake_frame_bytes > l.frame_bytes
        || l.frame_bytes > usize::MAX as u64
        || l.aggregate_decoded_bytes > usize::MAX as u64
        || l.string_bytes > usize::MAX as u64
        || l.bytes_length > usize::MAX as u64
    {
        return Err(ActivationError::new("invalid_limits"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationPhase {
    AwaitHello,
    AwaitManifest,
    AwaitReady,
    Active,
    Rejected,
}

#[derive(Clone, Debug)]
pub struct GrantedTool {
    reference: v1_1::CapabilityRef,
    descriptor: AgentTool,
    projections: ToolProjections,
}

impl GrantedTool {
    pub fn reference(&self) -> &v1_1::CapabilityRef {
        &self.reference
    }
    pub fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    pub fn projections(&self) -> &ToolProjections {
        &self.projections
    }
}

#[derive(Clone, Debug)]
pub struct ActiveGrant {
    wire: v1_1::CapabilityGrant,
    tools: Vec<GrantedTool>,
    omitted: Vec<String>,
}

impl ActiveGrant {
    pub fn wire(&self) -> &v1_1::CapabilityGrant {
        &self.wire
    }
    pub fn tools(&self) -> &[GrantedTool] {
        &self.tools
    }
    pub fn omitted(&self) -> &[String] {
        &self.omitted
    }
}

/// Outbound handshake message only; it carries no executable catalog entry.
#[derive(Debug)]
pub enum WireGrant {
    V1_0(v1_0::CapabilityGrant),
    V1_1(v1_1::CapabilityGrant),
}

/// Ordered handshake state. A failed step rejects the activation.
pub struct Activation {
    host: HostAuthority,
    started_ms: u64,
    phase: ActivationPhase,
    selected: Option<v1_1::Version>,
    hello_profiles: BTreeMap<String, v1_1::ProfileRange>,
    grant: Option<ActiveGrant>,
}

impl Activation {
    pub fn new(host: HostAuthority, started_ms: u64) -> Self {
        Self {
            host,
            started_ms,
            phase: ActivationPhase::AwaitHello,
            selected: None,
            hello_profiles: BTreeMap::new(),
            grant: None,
        }
    }

    pub fn phase(&self) -> ActivationPhase {
        self.phase
    }
    pub fn selected_protocol(&self) -> Option<v1_1::Version> {
        self.selected
    }
    pub fn active_grant(&self) -> Option<&ActiveGrant> {
        (self.phase == ActivationPhase::Active)
            .then_some(self.grant.as_ref())
            .flatten()
    }

    fn check_step(&self, expected: ActivationPhase, now_ms: u64, bytes: &[u8]) -> Result<()> {
        if self.phase != expected {
            return Err(ActivationError::new("invalid_handshake_state"));
        }
        if now_ms
            .checked_sub(self.started_ms)
            .is_none_or(|elapsed| elapsed > self.host.limits.handshake_ms)
        {
            return Err(ActivationError::new("handshake_timeout"));
        }
        if bytes.len() as u64 > self.host.limits.pre_handshake_frame_bytes {
            return Err(ActivationError::new("handshake_frame_too_large"));
        }
        Ok(())
    }

    pub fn receive_hello(&mut self, bytes: &[u8], now_ms: u64) -> Result<v1_1::HelloAck> {
        let result = self.receive_hello_inner(bytes, now_ms);
        if result.is_err() {
            self.phase = ActivationPhase::Rejected;
        }
        result
    }

    fn receive_hello_inner(&mut self, bytes: &[u8], now_ms: u64) -> Result<v1_1::HelloAck> {
        self.check_step(ActivationPhase::AwaitHello, now_ms, bytes)?;
        let hello =
            v1_1::Hello::decode(bytes).map_err(|_| ActivationError::new("invalid_handshake"))?;
        let range = hello
            .protocol
            .ok_or(ActivationError::new("invalid_handshake"))?;
        let selected = select_core(range, self.host.core_max_minor)?;
        if hello.supported_profiles.len() > MAX_DECLARATIONS {
            return Err(ActivationError::new("invalid_handshake"));
        }
        let mut profiles = BTreeMap::new();
        for profile in hello.supported_profiles {
            if profile.name.is_empty()
                || !valid_range(profile.major, profile.min_minor, profile.max_minor)
                || profiles.insert(profile.name.clone(), profile).is_some()
            {
                return Err(ActivationError::new("invalid_handshake"));
            }
        }
        self.selected = Some(selected);
        self.hello_profiles = profiles;
        self.phase = ActivationPhase::AwaitManifest;
        Ok(v1_1::HelloAck {
            selected_protocol: Some(selected),
        })
    }

    pub fn receive_manifest(&mut self, bytes: &[u8], now_ms: u64) -> Result<WireGrant> {
        let result = self.receive_manifest_inner(bytes, now_ms);
        match result {
            Ok(grant) => {
                let wire = if grant
                    .wire
                    .selected_protocol
                    .expect("selected protocol")
                    .minor
                    == 0
                {
                    WireGrant::V1_0(
                        v1_0::CapabilityGrant::decode(grant.wire.encode_to_vec().as_slice())
                            .expect("common grant wire"),
                    )
                } else {
                    WireGrant::V1_1(grant.wire.clone())
                };
                self.grant = Some(grant);
                self.phase = ActivationPhase::AwaitReady;
                Ok(wire)
            }
            Err(error) => {
                self.phase = ActivationPhase::Rejected;
                Err(error)
            }
        }
    }

    fn receive_manifest_inner(&self, bytes: &[u8], now_ms: u64) -> Result<ActiveGrant> {
        self.check_step(ActivationPhase::AwaitManifest, now_ms, bytes)?;
        let selected = self.selected.expect("manifest follows Hello");
        let manifest = if selected.minor == 0 {
            let old = v1_0::Manifest::decode(bytes)
                .map_err(|_| ActivationError::new("invalid_manifest"))?;
            ManifestView::from_v1_0(old)
        } else {
            let new = v1_1::Manifest::decode(bytes)
                .map_err(|_| ActivationError::new("invalid_manifest"))?;
            ManifestView::from_v1_1(new)
        };
        self.negotiate(manifest, selected)
    }

    pub fn receive_ready(&mut self, bytes: &[u8], now_ms: u64) -> Result<()> {
        if let Err(error) = self.check_step(ActivationPhase::AwaitReady, now_ms, bytes) {
            self.phase = ActivationPhase::Rejected;
            return Err(error);
        }
        // The selected DTO matters even though Ready currently has no fields.
        if self.selected.expect("Ready follows Hello").minor == 0 {
            if v1_0::Ready::decode(bytes).is_err() {
                self.phase = ActivationPhase::Rejected;
                return Err(ActivationError::new("invalid_handshake"));
            }
        } else {
            if v1_1::Ready::decode(bytes).is_err() {
                self.phase = ActivationPhase::Rejected;
                return Err(ActivationError::new("invalid_handshake"));
            }
        }
        self.phase = ActivationPhase::Active;
        Ok(())
    }

    fn negotiate(&self, manifest: ManifestView, selected: v1_1::Version) -> Result<ActiveGrant> {
        let range = manifest
            .protocol
            .ok_or(ActivationError::new("invalid_handshake"))?;
        if !valid_range(range.major, range.min_minor, range.max_minor)
            || range.major != selected.major
            || selected.minor < range.min_minor
            || selected.minor > range.max_minor
        {
            return Err(ActivationError::new("unsupported_version"));
        }
        if manifest.declared_id.is_empty()
            || manifest.extension_version.is_empty()
            || manifest.capabilities.len() > MAX_DECLARATIONS
            || manifest.permissions.len() > MAX_DECLARATIONS
            || manifest.subscriptions.len() > MAX_DECLARATIONS
        {
            return Err(ActivationError::new("invalid_manifest"));
        }
        // Structural pass is atomic and precedes all optional omissions.
        let mut seen = BTreeSet::new();
        for cap in &manifest.capabilities {
            if !seen.insert((cap.kind.clone(), cap.name.clone())) {
                return Err(ActivationError::new("duplicate_capability"));
            }
            validate_capability(cap)?;
        }
        let mut seen_permissions = BTreeSet::new();
        for permission in &manifest.permissions {
            if permission.name.is_empty()
                || permission.scope.is_empty()
                || !seen_permissions.insert((permission.name.clone(), permission.scope.clone()))
            {
                return Err(ActivationError::new("invalid_permission"));
            }
        }
        let mut seen_subscriptions = BTreeSet::new();
        for subscription in &manifest.subscriptions {
            if subscription.scope.is_empty() || !seen_subscriptions.insert(&subscription.scope) {
                return Err(ActivationError::new("invalid_subscription"));
            }
        }
        let mut tools = Vec::new();
        let mut refs = Vec::new();
        let mut omitted = Vec::new();
        for cap in &manifest.capabilities {
            if cap.kind != "tool" || cap.profile.as_ref().is_none_or(|p| p.name != PROFILE) {
                if cap.required {
                    return Err(ActivationError::new("required_capability_denied"));
                }
                omitted.push(cap.name.clone());
                continue;
            }
            match self.select_tool(cap)? {
                Some(tool) => {
                    refs.push(tool.reference.clone());
                    tools.push(tool);
                }
                None if cap.required => {
                    return Err(ActivationError::new("required_capability_denied"));
                }
                None => omitted.push(cap.name.clone()),
            }
        }
        let mut grants = Vec::new();
        for request in manifest.permissions {
            if self
                .host
                .permissions
                .contains(&(request.name.clone(), request.scope.clone()))
            {
                grants.push(v1_1::PermissionGrant {
                    name: request.name,
                    scope: request.scope,
                });
            } else if request.required {
                return Err(ActivationError::new("required_capability_denied"));
            }
        }
        Ok(ActiveGrant {
            wire: v1_1::CapabilityGrant {
                authenticated_extension_id: self.host.principal.clone(),
                selected_protocol: Some(selected),
                selected_capabilities: refs,
                permissions: grants,
                limits: Some(self.host.limits),
                connection_generation: self.host.generation,
            },
            tools,
            omitted,
        })
    }

    fn select_tool(&self, cap: &CapabilityView) -> Result<Option<GrantedTool>> {
        let Some(host_tool) = self.host.tools.get(&cap.name) else {
            return Ok(None);
        };
        if host_tool.principal != self.host.principal {
            return Err(ActivationError::new("host_catalog_principal_mismatch"));
        }
        let profile = cap.profile.as_ref().expect("validated profile");
        let Some(hello_range) = self.hello_profiles.get(PROFILE) else {
            return Ok(None);
        };
        if hello_range.major != 1 {
            return Ok(None);
        }
        let lower = profile.min_minor.max(hello_range.min_minor);
        let upper = profile
            .max_minor
            .min(hello_range.max_minor)
            .min(self.host.tool_max_minor);
        let Some(minor) = (lower..=upper)
            .rev()
            .find(|minor| host_tool.minors.contains_key(minor))
        else {
            return Ok(None);
        };
        let approved = &host_tool.minors[&minor];
        let descriptor = cap
            .descriptors
            .iter()
            .find(|v| v.minor == minor)
            .expect("validated advertised minors");
        let schemas = cap
            .schemas
            .iter()
            .find(|v| v.minor == minor)
            .expect("validated advertised tool minors");
        let mismatch = cap.replay
            != match host_tool.descriptor.replay {
                ToolReplay::Safe => 1,
                ToolReplay::Never => 2,
            }
            || descriptor.descriptor_digest_sha256.as_slice() != approved.profile_digest
            || !projection_matches(
                schemas.arguments.as_ref().expect("validated projection"),
                &approved.projections.arguments,
            )
            || !projection_matches(
                schemas.progress.as_ref().expect("validated projection"),
                &approved.projections.progress,
            )
            || !projection_matches(
                schemas.raw_result.as_ref().expect("validated projection"),
                &approved.projections.raw_result,
            )
            || cap
                .display_label
                .as_ref()
                .is_some_and(|label| label != &host_tool.descriptor.label);
        if mismatch {
            return Ok(None);
        }
        let reference = v1_1::CapabilityRef {
            authenticated_extension_id: self.host.principal.clone(),
            kind: "tool".into(),
            name: cap.name.clone(),
            profile: Some(v1_1::ProfileVersion {
                name: PROFILE.into(),
                major: 1,
                minor,
            }),
            schema_digest_sha256: approved.profile_digest.to_vec(),
            handler_id: Some(cap.handler_id.clone()),
        };
        Ok(Some(GrantedTool {
            reference,
            descriptor: host_tool.descriptor.clone(),
            projections: approved.projections.clone(),
        }))
    }
}

fn valid_range(major: u32, min: u32, max: u32) -> bool {
    major != 0 && min <= max && max - min < MAX_ADVERTISED_MINORS
}

fn select_core(range: v1_1::VersionRange, host_max: u32) -> Result<v1_1::Version> {
    if !valid_range(range.major, range.min_minor, range.max_minor)
        || range.major != 1
        || range.min_minor > host_max
    {
        return Err(ActivationError::new("unsupported_version"));
    }
    Ok(v1_1::Version {
        major: 1,
        minor: range.max_minor.min(host_max),
    })
}

fn projection_matches(wire: &v1_1::DynamicProjectionDigest, approved: &Projection) -> bool {
    wire.projection_id == approved.id && wire.schema_digest_sha256.as_slice() == approved.digest
}

struct CapabilityView {
    kind: String,
    name: String,
    profile: Option<v1_1::ProfileRange>,
    handler_id: String,
    replay: i32,
    execution: i32,
    required: bool,
    descriptors: Vec<v1_1::SupportedProfileDigest>,
    display_label: Option<String>,
    schemas: Vec<v1_1::ToolSchemaSet>,
}

struct ManifestView {
    declared_id: String,
    extension_version: String,
    protocol: Option<v1_1::VersionRange>,
    capabilities: Vec<CapabilityView>,
    permissions: Vec<v1_1::PermissionRequest>,
    subscriptions: Vec<v1_1::EventSubscriptionRequest>,
}

impl ManifestView {
    fn from_v1_1(m: v1_1::Manifest) -> Self {
        Self {
            declared_id: m.declared_extension_id,
            extension_version: m.extension_version,
            protocol: m.protocol,
            capabilities: m
                .capabilities
                .into_iter()
                .map(|c| CapabilityView {
                    kind: c.kind,
                    name: c.name,
                    profile: c.profile,
                    handler_id: c.handler_id,
                    replay: c.replay,
                    execution: c.execution,
                    required: c.required,
                    descriptors: c.supported_descriptor_digests,
                    display_label: c.display_label,
                    schemas: c.tool_schema_sets,
                })
                .collect(),
            permissions: m.permissions,
            subscriptions: m.subscriptions,
        }
    }

    fn from_v1_0(m: v1_0::Manifest) -> Self {
        // The common fields are wire-identical; decode through the selected DTO
        // first so a 1.0 connection cannot rely on a 1.1-only label.
        let common =
            v1_1::Manifest::decode(m.encode_to_vec().as_slice()).expect("common manifest wire");
        Self::from_v1_1(common)
    }
}

fn validate_capability(cap: &CapabilityView) -> Result<()> {
    let profile = cap
        .profile
        .as_ref()
        .ok_or(ActivationError::new("non_tool_or_profile_mismatch"))?;
    if cap.kind.is_empty()
        || cap.name.is_empty()
        || cap.handler_id.is_empty()
        || profile.name.is_empty()
        || !valid_range(profile.major, profile.min_minor, profile.max_minor)
    {
        return Err(ActivationError::new("invalid_capability"));
    }
    // The seven frozen v1 profile names have exact kind/major pairs. Other
    // nonempty names remain open identities and reach support selection.
    let known_v1_kind = match profile.name.as_str() {
        "tool.v1" => Some("tool"),
        "hook.v1" => Some("hook"),
        "provider.v1" => Some("provider"),
        "event-sink.v1" => Some("event-sink"),
        "resource.v1" => Some("resource"),
        "host-action.v1" => Some("host-action"),
        "telemetry-sink.v1" => Some("telemetry-sink"),
        _ => None,
    };
    if known_v1_kind.is_some_and(|kind| kind != cap.kind || profile.major != 1) {
        return Err(ActivationError::new("non_tool_or_profile_mismatch"));
    }
    if !matches!(cap.replay, 0..=3) || !matches!(cap.execution, 0..=3) {
        return Err(ActivationError::new("unsupported_variant"));
    }
    if cap.replay == 0 || cap.execution == 0 {
        return Err(ActivationError::new("invalid_required_semantic_value"));
    }
    let supported_tool = profile.name == PROFILE;
    if supported_tool && (!matches!(cap.replay, 1 | 2) || cap.execution != 2) {
        return Err(ActivationError::new("profile_inapplicable_variant"));
    }
    validate_minors(&cap.descriptors, profile.min_minor, profile.max_minor)?;
    for item in &cap.descriptors {
        if item.descriptor_digest_sha256.len() != 32 {
            return Err(ActivationError::new("missing_profile_digest"));
        }
    }
    if cap.kind != "tool" {
        if !cap.schemas.is_empty() {
            return Err(ActivationError::new("non_tool_or_profile_mismatch"));
        }
        return Ok(());
    }
    if !supported_tool && cap.schemas.is_empty() {
        return Ok(());
    }
    if cap.schemas.len() != (profile.max_minor - profile.min_minor + 1) as usize
        || cap.schemas.iter().map(|s| s.minor).collect::<BTreeSet<_>>()
            != (profile.min_minor..=profile.max_minor).collect::<BTreeSet<_>>()
    {
        return Err(ActivationError::new(
            "duplicate_or_missing_tool_schema_minor",
        ));
    }
    for set in &cap.schemas {
        if set.tool_name != cap.name {
            return Err(ActivationError::new("tool_schema_name_mismatch"));
        }
        let fields = [&set.arguments, &set.progress, &set.raw_result];
        if fields.iter().any(|p| p.is_none()) {
            return Err(ActivationError::new("missing_projection"));
        }
        let projections = [
            set.arguments.as_ref().expect("present"),
            set.progress.as_ref().expect("present"),
            set.raw_result.as_ref().expect("present"),
        ];
        if projections
            .iter()
            .any(|p| p.projection_id.is_empty() || p.schema_digest_sha256.len() != 32)
        {
            return Err(ActivationError::new("missing_projection_id_or_digest"));
        }
        let ids: BTreeSet<_> = projections.iter().map(|p| &p.projection_id).collect();
        if ids.len() != 3 {
            return Err(ActivationError::new("duplicate_or_wrong_projection_id"));
        }
        if supported_tool {
            let expected = [
                format!("tool.arguments:{}", cap.name),
                format!("tool.progress:{}", cap.name),
                format!("tool.result:{}", cap.name),
            ];
            if projections
                .iter()
                .zip(expected.iter())
                .any(|(p, id)| &p.projection_id != id)
            {
                return Err(ActivationError::new("duplicate_or_wrong_projection_id"));
            }
        }
    }
    Ok(())
}

fn validate_minors(items: &[v1_1::SupportedProfileDigest], min: u32, max: u32) -> Result<()> {
    if items.len() != (max - min + 1) as usize
        || items.iter().map(|d| d.minor).collect::<BTreeSet<_>>()
            != (min..=max).collect::<BTreeSet<_>>()
    {
        return Err(ActivationError::new("duplicate_or_missing_profile_minor"));
    }
    Ok(())
}
