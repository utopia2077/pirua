//! Current Agent/Coding language-neutral extension bindings.
//!
//! Install `CodingExtensions` as the builder's SessionHooks, then attach Ready
//! connections. All registrations and executable tool declarations are supplied
//! by the host. A peer manifest never creates authority. Remote values pass both
//! the current closed projection and native validation before changing a session.
use crate::*;
use pi_agent::{
    AfterToolCallContext, AfterToolCallResult, AgentError, AgentMessage, AgentTool,
    AgentToolResult, BeforeToolCallContext, BeforeToolCallResult, BoxFuture, CancellationToken,
};
use pi_ai::{InputContent, JsonObject, JsonValue, Tool};
pub use pi_extension_protocol::host::{ConnectionConfig, OrderedTransport, TransportFuture};
use pi_extension_protocol::{
    activation::ActiveGrant,
    host::{self, HostConnection},
    jcs::PortableSchema,
    profiles::{NativeError, Registry, Stage, ValidatedPayload},
    wire::v1_1::CapabilityRef,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

fn error(e: impl std::fmt::Display) -> AgentError {
    AgentError::Callback(format!("extension: {e}"))
}
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
fn encode(v: &impl Serialize) -> Result<Value, AgentError> {
    serde_json::to_value(v).map_err(error)
}
fn decode<T: DeserializeOwned>(v: Value) -> Result<T, AgentError> {
    serde_json::from_value(v).map_err(error)
}
fn native<T: DeserializeOwned>(v: &Value) -> Result<T, NativeError> {
    serde_json::from_value(v.clone()).map_err(|_| NativeError::Rejected)
}
fn message(v: &Value) -> Result<AgentMessage, NativeError> {
    crate::messages::decode(v.clone()).map_err(|_| NativeError::Rejected)
}
fn messages(v: &Value) -> Result<Vec<AgentMessage>, NativeError> {
    v.as_array()
        .ok_or(NativeError::Rejected)?
        .iter()
        .map(message)
        .collect()
}
fn known(v: &Value, keys: &[&str]) -> Result<(), NativeError> {
    if v.as_object()
        .is_none_or(|m| m.keys().any(|k| !keys.contains(&k.as_str())))
    {
        Err(NativeError::Rejected)
    } else {
        Ok(())
    }
}
fn tool_result(v: &Value) -> Result<AgentToolResult, NativeError> {
    known(
        v,
        &[
            "content",
            "details",
            "structuredContent",
            "usage",
            "isError",
            "terminate",
        ],
    )?;
    let mut result: AgentToolResult = native(v)?;
    if let Some(value) = v.get("usage") {
        result.usage = Some(native::<pi_ai::Usage>(value)?);
    }
    if let Some(value) = v.get("isError") {
        result.is_error = Some(native::<bool>(value)?);
    }
    if let Some(value) = v.get("terminate") {
        result.terminate = Some(native::<bool>(value)?);
    }
    Ok(result)
}
fn require_string(v: &Value) -> Result<(), NativeError> {
    if v.as_str().is_none_or(str::is_empty) {
        Err(NativeError::Rejected)
    } else {
        Ok(())
    }
}

/// A host-selected registration, in callback source order.
#[derive(Clone)]
pub enum ExtensionRegistration {
    Hook(CapabilityRef),
    Event(CapabilityRef),
    /// Catalog metadata and exact output schemas come from the authenticated host.
    Tool {
        capability: CapabilityRef,
        declaration: Tool,
        progress: Arc<PortableSchema>,
        result: Arc<PortableSchema>,
    },
    /// Read-only current-session snapshot, with an explicit scope grant.
    Snapshot(CapabilityRef),
}
impl ExtensionRegistration {
    fn capability(&self) -> &CapabilityRef {
        match self {
            Self::Hook(c) | Self::Event(c) | Self::Snapshot(c) => c,
            Self::Tool { capability, .. } => capability,
        }
    }
}
/// Ordered hook composition. Cloning the list releases its lock before callbacks.
#[derive(Default)]
pub struct CodingExtensions {
    attachments: Mutex<Vec<Weak<AttachmentState>>>,
    session: Mutex<Option<WeakCodingSession>>,
}
static NEXT_ATTACHMENT: AtomicU64 = AtomicU64::new(1);
/// Public retirement handle. Internal invocations retain only AttachmentState,
/// so their strong references cannot postpone last-public-handle retirement.
pub struct CodingExtensionAttachment {
    state: Arc<AttachmentState>,
}
struct AttachmentState {
    connection: HostConnection,
    registrations: Vec<ExtensionRegistration>,
    source: String,
    session: WeakCodingSession,
    session_id: String,
    live: Arc<AtomicBool>,
}
impl CodingExtensions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn attach(
        &self,
        connection: HostConnection,
        session: &CodingSession,
        registrations: Vec<ExtensionRegistration>,
    ) -> Result<Arc<CodingExtensionAttachment>, AgentError> {
        let bound = lock(&self.session)
            .clone()
            .ok_or_else(|| error("extension hooks are not bound to a session"))?;
        if !bound.same_instance(&session.downgrade()) {
            return Err(error("extension hooks belong to another session instance"));
        }
        if !connection.current() || session.is_disposed() {
            return Err(error("connection/session is closed"));
        }
        let grant = connection.grant();
        let mut seen = BTreeSet::new();
        let mut tools = BTreeSet::new();
        for registration in &registrations {
            let cap = registration.capability();
            if !seen.insert((cap.name.clone(), cap.handler_id.clone()))
                || !grant.profiles().iter().any(|p| {
                    p.reference() == cap
                        && p.approved().profile() == "coding.v1"
                        && p.approved().scope() == session.session_id()
                })
            {
                return Err(error("missing or duplicate current Coding grant"));
            }
            let approved = grant
                .profiles()
                .iter()
                .find(|p| p.reference() == cap)
                .ok_or_else(|| error("missing grant"))?
                .approved();
            if approved.execution()
                != if matches!(registration, ExtensionRegistration::Tool { .. }) {
                    2
                } else {
                    1
                }
            {
                return Err(error("registration execution mode mismatch"));
            }
            match registration {
                ExtensionRegistration::Hook(c) => {
                    if !is_hook(&c.name) {
                        return Err(error("unsupported current Coding hook"));
                    }
                }
                ExtensionRegistration::Event(c) => {
                    if c.name != "event" {
                        return Err(error("event capability mismatch"));
                    }
                }
                ExtensionRegistration::Tool {
                    capability,
                    declaration,
                    ..
                } => {
                    if capability.name != declaration.name.as_str()
                        || !tools.insert(declaration.name.to_string())
                    {
                        return Err(error("tool capability/declaration mismatch"));
                    }
                }
                ExtensionRegistration::Snapshot(c) => {
                    let p = grant
                        .profiles()
                        .iter()
                        .find(|p| p.reference() == c)
                        .ok_or_else(|| error("snapshot grant missing"))?;
                    if c.name != "session.snapshot"
                        || p.approved().scope() != session.session_id()
                        || !p
                            .approved()
                            .permissions()
                            .contains(&("session.snapshot".into(), session.session_id()))
                    {
                        return Err(error(
                            "snapshot requires explicit current-session permission",
                        ));
                    }
                }
            }
        }
        let source = format!(
            "extension:{}:{}:{}",
            grant.wire().authenticated_extension_id,
            grant.wire().connection_generation,
            NEXT_ATTACHMENT.fetch_add(1, Ordering::Relaxed)
        );
        let attachment = Arc::new(AttachmentState {
            connection,
            registrations,
            source,
            session: session.downgrade(),
            session_id: session.session_id(),
            live: Arc::new(AtomicBool::new(true)),
        });
        let mut registered = Vec::new();
        for registration in &attachment.registrations {
            if let ExtensionRegistration::Tool {
                capability,
                declaration,
                progress,
                result,
            } = registration
            {
                let weak = Arc::downgrade(&attachment);
                let cap = capability.clone();
                let progress = progress.clone();
                let result = result.clone();
                let execute=Arc::new(move |inv:pi_agent::ToolInvocation|->BoxFuture<'static,Result<AgentToolResult,AgentError>> {
                    let weak=weak.clone();let cap=cap.clone();let progress=progress.clone();let result=result.clone();
                    Box::pin(async move {
                        let a=weak.upgrade().ok_or_else(||error("stale extension tool"))?;a.ensure_current()?;
                        let input=json!({"toolCallId":inv.call_id,"arguments":inv.arguments});
                        let input_copy=input.clone();let result_schema=result.clone();let progress_schema=progress.clone();let live=a.live.clone();
                        let validator:host::Validator=Arc::new(move |p|{
                            if p.stage==Stage::Request {require_string(&p.fields["toolCallId"])?;native::<JsonObject>(&p.fields["arguments"])?;if p.fields!=input_copy || !live.load(Ordering::Acquire){return Err(NativeError::Rejected)};return Ok(())}
                            let value=&p.fields["result"];tool_result(value)?;
                            let bytes=serde_json::to_vec(value).map_err(|_|NativeError::LocalFault)?;
                            let schema=if p.stage==Stage::Progress{&progress_schema}else{&result_schema};
                            schema.validate_instance(&bytes,pi_extension_protocol::jcs::Limits::default()).map_err(|_|NativeError::Rejected)?;
                            if live.load(Ordering::Acquire){Ok(())}else{Err(NativeError::Rejected)}
                        });
                        let registry=Registry::coding().map_err(error)?;let payload=host::bound(&registry,"coding.input:tool.call",&input).map_err(error)?;
                        let signal=inv.cancellation.clone();let on_update=inv.on_update.clone();let live=a.live.clone();
                        let output=a.connection.request_with_progress_credit(cap,"tool.call".into(),payload,validator,Box::pin(async move{let _ = signal.cancelled().await;}),host::ProgressCredit::Retain,move |v|{if live.load(Ordering::Acquire){let partial=tool_result(&v["result"]).map_err(|_|host::Error::NativeRejected)?;on_update(partial);}Ok(())}).await.map_err(error)?;
                        a.ensure_session_current()?;tool_result(&output["result"]).map_err(|_|error("invalid native tool result"))
                    })
                });
                let mut tool = RegisteredTool::direct(AgentTool::new(declaration.clone(), execute));
                tool.source = Some(attachment.source.clone());
                registered.push(tool);
            }
        }
        session
            .replace_tools_for_source(&attachment.source, registered)
            .map_err(error)?;
        if attachment
            .registrations
            .iter()
            .any(|r| matches!(r, ExtensionRegistration::Snapshot(_)))
        {
            let weak = Arc::downgrade(&attachment);
            if let Err(e) = attachment
                .connection
                .set_snapshot_handler(Arc::new(move || {
                    let a = weak.upgrade().ok_or(host::Error::Closed)?;
                    a.ensure_current().map_err(|_| host::Error::Closed)?;
                    let session = a.session.upgrade().ok_or(host::Error::Closed)?;
                    serde_json::to_value(session.snapshot())
                        .map_err(|_| host::Error::HostValidation)
                }))
            {
                attachment.close();
                return Err(error(e));
            }
        }
        lock(&self.attachments).push(Arc::downgrade(&attachment));
        Ok(Arc::new(CodingExtensionAttachment { state: attachment }))
    }
    fn current(&self) -> Vec<Arc<AttachmentState>> {
        let snapshot = {
            let mut list = lock(&self.attachments);
            list.retain(|a| a.strong_count() > 0);
            list.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        snapshot
            .into_iter()
            .filter(|a| {
                if a.current() {
                    true
                } else {
                    a.close();
                    false
                }
            })
            .collect()
    }
    fn hooks(&self, name: &str) -> Vec<(Arc<AttachmentState>, CapabilityRef)> {
        self.current()
            .into_iter()
            .flat_map(|a| {
                a.registrations
                    .iter()
                    .filter_map(|r| match r {
                        ExtensionRegistration::Hook(c) if c.name == name => {
                            Some((a.clone(), c.clone()))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}
impl CodingExtensionAttachment {
    pub fn grant(&self) -> &ActiveGrant {
        self.state.grant()
    }
    pub fn current(&self) -> bool {
        self.state.current()
    }
    pub fn close(&self) {
        self.state.close()
    }
}
impl AttachmentState {
    fn grant(&self) -> &ActiveGrant {
        self.connection.grant()
    }
    pub fn current(&self) -> bool {
        self.live.load(Ordering::Acquire)
            && self.connection.current()
            && self
                .session
                .upgrade()
                .is_some_and(|s| !s.is_disposed() && s.session_id() == self.session_id)
    }
    fn ensure_session_current(&self) -> Result<(), AgentError> {
        if self
            .session
            .upgrade()
            .is_some_and(|s| !s.is_disposed() && s.session_id() == self.session_id)
        {
            Ok(())
        } else {
            Err(error("stale extension context"))
        }
    }
    fn ensure_current(&self) -> Result<(), AgentError> {
        if self.current() {
            Ok(())
        } else {
            self.close();
            Err(error("stale extension attachment"))
        }
    }
    pub fn close(&self) {
        if self.live.swap(false, Ordering::AcqRel) {
            self.connection.clear_snapshot_handler();
            self.connection.close();
            if let Some(s) = self.session.upgrade() {
                let _ = s.replace_tools_for_source(&self.source, Vec::new());
            }
        }
    }
    async fn invoke(
        &self,
        cap: CapabilityRef,
        method: &str,
        input: Value,
        signal: CancellationToken,
    ) -> Result<Value, AgentError> {
        self.ensure_current()?;
        let method = method.to_owned();
        let expected = input.clone();
        let name = method.clone();
        let session = self
            .session
            .upgrade()
            .ok_or_else(|| error("stale session"))?;
        let allowed_tools = session
            .all_tools()
            .into_iter()
            .map(|t| t.declaration.name.to_string())
            .collect::<Vec<_>>();
        let snapshot = session.snapshot();
        let allowed_entries =
            crate::compaction::branch_path(&snapshot, snapshot.leaf_id.as_deref())
                .into_iter()
                .map(|e| e.id)
                .collect::<Vec<_>>();
        let live = self.live.clone();
        let validator: host::Validator = Arc::new(move |p| {
            validate_hook(&name, p, &expected, &allowed_tools, &allowed_entries)?;
            if live.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(NativeError::Rejected)
            }
        });
        let registry = Registry::coding().map_err(error)?;
        let payload =
            host::bound(&registry, &format!("coding.input:{method}"), &input).map_err(error)?;
        let result = self
            .connection
            .request(
                cap,
                method,
                payload,
                validator,
                Box::pin(async move {
                    let _ = signal.cancelled().await;
                }),
                |_| Err(host::Error::Protocol),
            )
            .await
            .map_err(error)?;
        self.ensure_session_current()?;
        Ok(result)
    }
}
impl Drop for CodingExtensionAttachment {
    fn drop(&mut self) {
        self.state.close();
    }
}
impl Drop for AttachmentState {
    fn drop(&mut self) {
        self.close();
    }
}
fn is_hook(name: &str) -> bool {
    matches!(
        name,
        "input"
            | "before_agent_start"
            | "context"
            | "message_start"
            | "message_end"
            | "before_tool"
            | "after_tool"
            | "turn_end"
            | "agent_before_settle"
            | "before_compact"
            | "before_tree"
            | "before_session_change"
            | "cache_warming_decision"
            | "prepare_loadout"
    )
}
fn validate_hook(
    name: &str,
    p: &ValidatedPayload,
    expected: &Value,
    allowed_tools: &[String],
    allowed_entries: &[String],
) -> Result<(), NativeError> {
    if p.stage == Stage::Progress {
        return Err(NativeError::Rejected);
    }
    if p.stage == Stage::Request {
        if &p.fields != expected {
            return Err(NativeError::Rejected);
        }
        match name {
            "input" => {
                native::<InputEvent>(&p.fields)?;
            }
            "before_agent_start" => {
                let e = native::<BeforeAgentStart>(&p.fields)?;
                message(&serde_json::to_value(e.message).map_err(|_| NativeError::LocalFault)?)?;
            }
            "context" => {
                messages(&p.fields["messages"])?;
            }
            "message_start" | "message_end" => {
                message(&p.fields["message"])?;
            }
            "before_tool" | "after_tool" => {
                require_string(&p.fields["toolCallId"])?;
                require_string(&p.fields["toolName"])?;
                native::<JsonObject>(&p.fields["args"])?;
                if name == "after_tool" {
                    tool_result(&p.fields["result"])?;
                }
            }
            "turn_end" | "agent_before_settle" => {
                native::<BoundaryContext>(&p.fields["value"])?;
            }
            "before_compact" => {
                native::<CompactionPreparation>(&p.fields["value"]["preparation"])?;
                native::<CompactionReason>(&p.fields["value"]["reason"])?;
            }
            "before_tree" => {
                native::<TreePreparation>(&p.fields["value"])?;
            }
            "before_session_change" => {
                native::<SessionChangeReason>(&p.fields["value"]["reason"])?;
                native::<pi_coding_session::SessionSnapshot>(&p.fields["value"]["snapshot"])?;
            }
            "cache_warming_decision" => {
                native::<CacheWarmingDecision>(&p.fields["value"])?;
            }
            "event" => {
                native::<SessionEvent>(&p.fields["value"])?;
            }
            "prepare_loadout" => {
                let v = &p.fields["value"];
                known(v, &["declared", "callable", "registered"])?;
                native::<Vec<Tool>>(&v["declared"])?;
                native::<Vec<Tool>>(&v["callable"])?;
                native::<Vec<Tool>>(&v["registered"])?;
            }
            _ => return Err(NativeError::Rejected),
        };
        return Ok(());
    }
    match name {
        "input" => {
            native::<InputResult>(&p.fields)?;
        }
        "before_agent_start" => {
            let r = native::<BeforeAgentStartResult>(&p.fields)?;
            for m in r.messages {
                message(&serde_json::to_value(m).map_err(|_| NativeError::LocalFault)?)?;
            }
            if r.active_tools
                .as_ref()
                .is_some_and(|t| t.iter().any(|n| !allowed_tools.contains(n)))
            {
                return Err(NativeError::Rejected);
            }
        }
        "context" => {
            messages(&p.fields["messages"])?;
        }
        "message_start" | "message_end" => {
            let m = message(&p.fields["message"])?;
            if Some(m.role()) != expected["message"]["role"].as_str() {
                return Err(NativeError::Rejected);
            }
        }
        "before_tool" => {
            native::<bool>(&p.fields["block"])?;
            native::<bool>(&p.fields["terminate"])?;
            if let Some(v) = p.fields.get("args").filter(|v| !v.is_null()) {
                native::<JsonObject>(v)?;
            }
        }
        "after_tool" => {
            if let Some(v) = p.fields.get("content").filter(|v| !v.is_null()) {
                native::<Vec<InputContent>>(v)?;
            }
            if let Some(v) = p.fields.get("details") {
                native::<JsonValue>(v)?;
            }
            if let Some(v) = p.fields.get("structuredContent") {
                native::<JsonValue>(v)?;
            }
            if let Some(v) = p.fields.get("usage").filter(|v| !v.is_null()) {
                native::<pi_ai::Usage>(v)?;
            }
        }
        "turn_end" | "agent_before_settle" => {
            let context = native::<BoundaryContext>(&expected["value"])?;
            let r = native::<BoundaryResult>(&p.fields["value"])?;
            validate_boundary(&context, &r)?;
        }
        "before_compact" => {
            let r = native::<BeforeCompactionResult>(&p.fields["value"])?;
            if let Some(c) = r.compaction
                && (c.summary.trim().is_empty()
                    || c.first_kept_entry_id
                        .as_ref()
                        .is_some_and(|id| !allowed_entries.contains(id)))
            {
                return Err(NativeError::Rejected);
            }
        }
        "before_tree" => {
            let r = native::<BeforeTreeResult>(&p.fields["value"])?;
            if r.summary
                .as_ref()
                .is_some_and(|s| s.summary.trim().is_empty())
            {
                return Err(NativeError::Rejected);
            }
        }
        "before_session_change" => {
            native::<bool>(&p.fields["value"])?;
        }
        "cache_warming_decision" => {
            native::<CacheWarmingAction>(&p.fields["value"])?;
        }
        "prepare_loadout" => {
            let r = native::<ToolLoadoutChanges>(&p.fields["value"])?;
            let names = expected["value"]["declared"]
                .as_array()
                .ok_or(NativeError::Rejected)?;
            if r.descriptions
                .keys()
                .chain(r.hidden_declarations.iter())
                .any(|n| !names.iter().any(|v| v["name"].as_str() == Some(n)))
            {
                return Err(NativeError::Rejected);
            }
        }
        "event" => {
            if p.fields != json!({}) {
                return Err(NativeError::Rejected);
            }
        }
        _ => return Err(NativeError::Rejected),
    };
    Ok(())
}
fn validate_boundary(
    context: &BoundaryContext,
    result: &BoundaryResult,
) -> Result<(), NativeError> {
    if result.continue_run && !context.can_continue {
        return Err(NativeError::Rejected);
    }
    let branch =
        crate::compaction::branch_path(&context.snapshot, context.snapshot.leaf_id.as_deref());
    let ids = branch
        .iter()
        .map(|e| e.id.as_str())
        .collect::<BTreeSet<_>>();
    for draft in &result.entries {
        match draft {
            SessionBoundaryDraft::Custom { custom_type, .. } => {
                if custom_type.is_empty() {
                    return Err(NativeError::Rejected);
                }
            }
            SessionBoundaryDraft::CustomMessage {
                custom_type,
                content,
                ..
            } => {
                if custom_type.is_empty() {
                    return Err(NativeError::Rejected);
                }
                if !content.is_string() {
                    native::<Vec<InputContent>>(content)?;
                }
            }
            SessionBoundaryDraft::ContextEdit {
                target_id,
                replacement,
            } => {
                if !ids.contains(target_id.as_str()) {
                    return Err(NativeError::Rejected);
                }
                let target = branch
                    .iter()
                    .find(|e| e.id == *target_id)
                    .ok_or(NativeError::Rejected)?;
                match &target.content {
                    pi_coding_session::EntryContent::Message { message: original }
                        if matches!(
                            original["role"].as_str(),
                            Some("user" | "assistant" | "toolResult")
                        ) =>
                    {
                        if let Some(replacement) = replacement {
                            let mut candidate = original.clone();
                            candidate["content"] =
                                if original["role"] != "user" && replacement.content.is_string() {
                                    json!([{"type":"text","text":replacement.content}])
                                } else {
                                    replacement.content.clone()
                                };
                            message(&candidate)?;
                        }
                    }
                    pi_coding_session::EntryContent::CustomMessage { .. } => {
                        if let Some(r) = replacement
                            && !r.content.is_string()
                        {
                            native::<Vec<InputContent>>(&r.content)?;
                        }
                    }
                    _ => return Err(NativeError::Rejected),
                }
            }
            SessionBoundaryDraft::Compaction {
                summary,
                first_kept_entry_id,
                ..
            } => {
                if summary.trim().is_empty()
                    || first_kept_entry_id
                        .as_ref()
                        .is_some_and(|id| !ids.contains(id.as_str()))
                {
                    return Err(NativeError::Rejected);
                }
            }
        }
    }
    Ok(())
}
impl SessionHooks for CodingExtensions {
    fn bind_session(&self, session: WeakCodingSession) -> Result<(), AgentError> {
        let mut bound = lock(&self.session);
        if let Some(existing) = bound.as_ref() {
            if !existing.same_instance(&session) {
                return Err(error(
                    "extension hooks are already bound to another session instance",
                ));
            }
        } else {
            *bound = Some(session);
        }
        Ok(())
    }
    fn input(
        &self,
        mut event: InputEvent,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<InputResult, AgentError>> {
        Box::pin(async move {
            let mut changed = false;
            for (a, c) in self.hooks("input") {
                match decode(
                    a.invoke(c, "input", encode(&event)?, signal.clone())
                        .await?,
                )? {
                    InputResult::Continue => {}
                    InputResult::Handled => return Ok(InputResult::Handled),
                    InputResult::Transform { text, images } => {
                        event.text = text;
                        if let Some(images) = images {
                            event.images = images
                        };
                        changed = true;
                    }
                }
            }
            Ok(if changed {
                InputResult::Transform {
                    text: event.text,
                    images: Some(event.images),
                }
            } else {
                InputResult::Continue
            })
        })
    }
    fn before_agent_start(
        &self,
        mut event: BeforeAgentStart,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeAgentStartResult, AgentError>> {
        Box::pin(async move {
            let mut result = BeforeAgentStartResult::default();
            for (a, c) in self.hooks("before_agent_start") {
                let next: BeforeAgentStartResult = decode(
                    a.invoke(c, "before_agent_start", encode(&event)?, signal.clone())
                        .await?,
                )?;
                result.messages.extend(next.messages);
                if let Some(p) = next.system_prompt {
                    event.system_prompt = p.clone();
                    result.system_prompt = Some(p)
                }
                if let Some(t) = next.active_tools {
                    event.active_tools = t.clone();
                    result.active_tools = Some(t)
                }
            }
            Ok(result)
        })
    }
    fn context(
        &self,
        mut context: Vec<AgentMessage>,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Vec<AgentMessage>, AgentError>> {
        Box::pin(async move {
            for (a, c) in self.hooks("context") {
                let r = a
                    .invoke(c, "context", json!({"messages":context}), signal.clone())
                    .await?;
                context = messages(&r["messages"]).map_err(|_| error("invalid native context"))?;
            }
            Ok(context)
        })
    }
    fn transform_message(
        &self,
        phase: MessagePhase,
        mut value: AgentMessage,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<AgentMessage, AgentError>> {
        Box::pin(async move {
            let name = if phase == MessagePhase::Start {
                "message_start"
            } else {
                "message_end"
            };
            for (a, c) in self.hooks(name) {
                let r = a
                    .invoke(c, name, json!({"message":value}), signal.clone())
                    .await?;
                value = message(&r["message"]).map_err(|_| error("invalid native message"))?;
            }
            Ok(value)
        })
    }
    fn before_tool(
        &self,
        mut event: BeforeToolCallContext,
        parent: Option<String>,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<BeforeToolCallResult>, AgentError>> {
        Box::pin(async move {
            let mut result = None;
            let mut changed_args = None;
            for (a, c) in self.hooks("before_tool") {
                let mut input = json!({"toolCallId":event.tool_call.id,"toolName":event.tool_call.name,"args":event.args});
                if let Some(p) = &parent {
                    input["parentToolCallId"] = json!(p)
                }
                let r = a.invoke(c, "before_tool", input, signal.clone()).await?;
                let patch = BeforeToolCallResult {
                    args: r
                        .get("args")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .map(decode)
                        .transpose()?,
                    block: decode(r["block"].clone())?,
                    reason: r
                        .get("reason")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .map(decode)
                        .transpose()?,
                    terminate: decode(r["terminate"].clone())?,
                };
                if let Some(args) = &patch.args {
                    event.args = args.clone();
                    changed_args = Some(args.clone());
                }
                let stop = patch.block;
                result = Some(patch);
                if stop {
                    break;
                }
            }
            if let Some(patch) = &mut result {
                patch.args = changed_args;
            }
            Ok(result)
        })
    }
    fn after_tool(
        &self,
        mut event: AfterToolCallContext,
        parent: Option<String>,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<AfterToolCallResult>, AgentError>> {
        Box::pin(async move {
            let mut changed = false;
            let mut content_changed = false;
            let mut structured_changed = false;
            let mut details_changed = false;
            let mut usage_changed = false;
            let mut error_changed = false;
            let mut terminate_changed = false;
            for (a, c) in self.hooks("after_tool") {
                let mut input = json!({"toolCallId":event.before.tool_call.id,"toolName":event.before.tool_call.name,"args":event.before.args,"result":event.result,"isError":event.is_error});
                if let Some(p) = &parent {
                    input["parentToolCallId"] = json!(p)
                }
                let r = a.invoke(c, "after_tool", input, signal.clone()).await?;
                let structured = r
                    .get("structuredContent")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .map(decode)
                    .transpose()?;
                if let Some(v) = r.get("content").filter(|v| !v.is_null()) {
                    event.result.content = decode(v.clone())?;
                    event.result.structured_content = structured;
                    content_changed = true;
                    structured_changed = true;
                } else if structured.is_some() {
                    event.result.structured_content = structured;
                    structured_changed = true;
                }
                if let Some(v) = r.get("details").filter(|v| !v.is_null()) {
                    event.result.details = Some(decode(v.clone())?);
                    details_changed = true;
                }
                if let Some(v) = r.get("usage").filter(|v| !v.is_null()) {
                    event.result.usage = Some(decode(v.clone())?);
                    usage_changed = true;
                }
                if let Some(v) = r.get("isError").filter(|v| !v.is_null()) {
                    event.is_error = decode(v.clone())?;
                    error_changed = true;
                }
                if let Some(v) = r.get("terminate").filter(|v| !v.is_null()) {
                    event.result.terminate = Some(decode(v.clone())?);
                    terminate_changed = true;
                }
                changed = true;
            }
            Ok(changed.then_some(AfterToolCallResult {
                content: content_changed.then_some(event.result.content),
                structured_content: if structured_changed {
                    event.result.structured_content
                } else {
                    None
                },
                details: if details_changed {
                    event.result.details
                } else {
                    None
                },
                usage: if usage_changed {
                    event.result.usage
                } else {
                    None
                },
                is_error: error_changed.then_some(event.is_error),
                terminate: if terminate_changed {
                    event.result.terminate
                } else {
                    None
                },
            }))
        })
    }
    fn boundary(
        &self,
        event: BoundaryContext,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BoundaryResult, AgentError>> {
        Box::pin(async move {
            let name = if event.kind == BoundaryKind::TurnEnd {
                "turn_end"
            } else {
                "agent_before_settle"
            };
            let mut result = BoundaryResult::default();
            for (a, c) in self.hooks(name) {
                let next: BoundaryResult = decode(
                    a.invoke(c, name, json!({"value":event}), signal.clone())
                        .await?["value"]
                        .clone(),
                )?;
                result.entries.extend(next.entries);
                result.continue_run |= next.continue_run;
            }
            Ok(result)
        })
    }
    fn before_compact(
        &self,
        event: CompactionPreparation,
        reason: CompactionReason,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeCompactionResult, AgentError>> {
        Box::pin(async move {
            let mut result = BeforeCompactionResult::default();
            for (a, c) in self.hooks("before_compact") {
                let next: BeforeCompactionResult = decode(
                    a.invoke(
                        c,
                        "before_compact",
                        json!({"value":{"preparation":event,"reason":reason}}),
                        signal.clone(),
                    )
                    .await?["value"]
                        .clone(),
                )?;
                if next.cancel {
                    return Ok(next);
                }
                if next.compaction.is_some() {
                    result = next
                }
            }
            Ok(result)
        })
    }
    fn before_tree(
        &self,
        mut event: TreePreparation,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<BeforeTreeResult, AgentError>> {
        Box::pin(async move {
            let mut result = BeforeTreeResult::default();
            for (a, c) in self.hooks("before_tree") {
                let next: BeforeTreeResult = decode(
                    a.invoke(c, "before_tree", json!({"value":event}), signal.clone())
                        .await?["value"]
                        .clone(),
                )?;
                if next.cancel {
                    return Ok(next);
                }
                if let Some(v) = &next.custom_instructions {
                    event.custom_instructions = Some(v.clone());
                    result.custom_instructions = Some(v.clone())
                }
                if let Some(v) = next.replace_instructions {
                    event.replace_instructions = v;
                    result.replace_instructions = Some(v)
                }
                if let Some(v) = &next.label {
                    event.label = Some(v.clone());
                    result.label = Some(v.clone())
                }
                if next.summary.is_some() {
                    result.summary = next.summary
                }
            }
            Ok(result)
        })
    }
    fn before_session_change(
        &self,
        reason: SessionChangeReason,
        snapshot: pi_coding_session::SessionSnapshot,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<bool, AgentError>> {
        Box::pin(async move {
            for (a, c) in self.hooks("before_session_change") {
                let cancel: bool = decode(
                    a.invoke(
                        c,
                        "before_session_change",
                        json!({"value":{"reason":reason,"snapshot":snapshot}}),
                        signal.clone(),
                    )
                    .await?["value"]
                        .clone(),
                )?;
                if cancel {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
    fn cache_warming_decision(
        &self,
        mut decision: CacheWarmingDecision,
    ) -> BoxFuture<'_, Result<CacheWarmingAction, AgentError>> {
        Box::pin(async move {
            for (a, c) in self.hooks("cache_warming_decision") {
                decision.action = decode(
                    a.invoke(
                        c,
                        "cache_warming_decision",
                        json!({"value":decision}),
                        CancellationToken::new(),
                    )
                    .await?["value"]
                        .clone(),
                )?;
            }
            Ok(decision.action)
        })
    }
    fn prepare_loadout(
        &self,
        loadout: ToolLoadout,
        signal: CancellationToken,
    ) -> BoxFuture<'_, Result<ToolLoadoutChanges, AgentError>> {
        Box::pin(async move {
            let mut result = ToolLoadoutChanges::default();
            let mut declared = loadout
                .declared
                .iter()
                .map(|t| t.declaration.clone())
                .collect::<Vec<_>>();
            let callable = loadout
                .callable
                .iter()
                .map(|t| t.declaration.clone())
                .collect::<Vec<_>>();
            let registered = loadout
                .registered
                .iter()
                .map(|t| t.tool.declaration.clone())
                .collect::<Vec<_>>();
            for (a, c) in self.hooks("prepare_loadout") {
                let next:ToolLoadoutChanges=decode(a.invoke(c,"prepare_loadout",json!({"value":{"declared":declared,"callable":callable,"registered":registered}}),signal.clone()).await?["value"].clone())?;
                for t in &mut declared {
                    if let Some(d) = next.descriptions.get(t.name.as_str()) {
                        t.description = d.clone()
                    }
                }
                declared.retain(|t| {
                    !next
                        .hidden_declarations
                        .iter()
                        .any(|n| n == t.name.as_str())
                });
                result.descriptions.extend(next.descriptions);
                result.hidden_declarations.extend(next.hidden_declarations);
            }
            Ok(result)
        })
    }
    fn event(&self, mut event: SessionEvent) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async move {
            if let SessionEvent::ModelSelect {
                model,
                previous_model,
            } = &mut event
            {
                model.headers = None;
                previous_model.headers = None;
            }
            let replacement = matches!(
                event,
                SessionEvent::SessionSwitch { .. } | SessionEvent::SessionFork { .. }
            );
            for a in self.current() {
                for r in &a.registrations {
                    if let ExtensionRegistration::Event(c) = r {
                        a.invoke(
                            c.clone(),
                            "event",
                            json!({"value":event}),
                            CancellationToken::new(),
                        )
                        .await?;
                    }
                }
                if replacement {
                    a.close()
                }
            }
            Ok(())
        })
    }
}
