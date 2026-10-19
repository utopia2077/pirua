//! Immutable structural conversation mounts over atomic Session observations.
use futures_util::{FutureExt, future, pin_mut};
use pi_durable_session::{
    CommitChange, CommitPublication, ConversationId, ConversationRecord, DocumentAddress,
    DocumentId, EntryRecord, Error, ErrorKind, ObservationFrame, ObservationQuery,
    ObservationSnapshot, ObservationSubscription, Op, Path, Result, Scope, Seq, Session,
    StorageWrite, TaskId, TaskRecord, WatchEnd,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Notify, mpsc, oneshot, watch};

/// Only singleton documents at these current-version1 identities belong in a mount.
pub const MOUNTED_KINDS: [&str; 5] = ["pi.agent", "pi.live", "pi.inbox", "pi.provider", "pi.usage"];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConversationView {
    pub conversation: ConversationRecord,
    pub entries: Vec<EntryRecord>,
    pub docs: BTreeMap<String, Value>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ConversationViewFrame {
    pub seq: Seq,
    pub value: Arc<ConversationView>,
    pub ops: Vec<Op>,
}

/// Shares one private mount per conversation while its handles remain alive.
#[derive(Clone)]
pub struct ConversationViews {
    session: Session,
    mounts: Arc<tokio::sync::Mutex<BTreeMap<ConversationId, Weak<Mount>>>>,
}
impl ConversationViews {
    pub fn new(session: Session) -> Self {
        Self {
            session,
            mounts: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }
    pub async fn state(&self, conversation: ConversationId) -> Result<ConversationViewState> {
        Ok(ConversationViewState {
            mount: self.mount(conversation).await?,
        })
    }
    pub async fn watch(&self, conversation: ConversationId) -> Result<ConversationViewWatch> {
        let mount = self.mount(conversation).await?;
        let mut data = mount.data.lock().expect("mount lock");
        if let Some(end) = &data.end {
            return Err(ended_error(end));
        }
        let inner = WatchInner::new(data.value.clone(), mount.clone());
        data.watches.push(Arc::downgrade(&inner));
        drop(data);
        Ok(ConversationViewWatch {
            custody: Arc::new(WatchCustody { inner }),
        })
    }
    /// Atomically attaches the runtime's lossless commit translator to the current mount.
    pub(crate) async fn attach_commits(
        &self,
        conversation: ConversationId,
    ) -> Result<ConversationCommitAttachment> {
        self.mount(conversation).await?.attach_commits().await
    }
    async fn mount(&self, conversation: ConversationId) -> Result<Arc<Mount>> {
        let mut mounts = self.mounts.lock().await;
        // Even a shared mount acquisition passes the Session line, so a sealed or
        // failed Session cannot yield a new handle. The unused atomic capture drops.
        let observation = self.session.observe(query(conversation)).await?;
        mounts.retain(|_, mount| mount.strong_count() > 0);
        if let Some(mount) = mounts.get(&conversation).and_then(Weak::upgrade) {
            drop(observation);
            mount.current().await;
            mount.ensure_open()?;
            return Ok(mount);
        }
        let (value, docs) = baseline(&observation.baseline)?;
        let (requests, commands) = mpsc::unbounded_channel();
        let mount = Arc::new(Mount {
            data: Mutex::new(MountData {
                value: Arc::new(value),
                docs,
                watches: Vec::new(),
                tasks: Arc::new(observation.baseline.tasks.clone()),
                attachments: Vec::new(),
                end: None,
            }),
            requests,
        });
        tokio::spawn(drive(
            Arc::downgrade(&mount),
            self.session.clone(),
            observation,
            commands,
        ));
        // An observation that was already hydrating when close sealed can return
        // a baseline with a closed queue. Fence driver adoption before admitting
        // the first public handle; queued termination must be visible here.
        mount.current().await;
        mount.ensure_open()?;
        mounts.insert(conversation, Arc::downgrade(&mount));
        Ok(mount)
    }
}
fn query(conversation: ConversationId) -> ObservationQuery {
    ObservationQuery {
        conversation: Some(conversation),
        conversation_records: true,
        addresses: MOUNTED_KINDS
            .iter()
            .map(|kind| DocumentAddress {
                scope: Scope::Conversation {
                    conversation_id: conversation,
                },
                kind: (*kind).into(),
                key: None,
            })
            .collect(),
        scopes: Vec::new(),
    }
}
fn baseline(snapshot: &ObservationSnapshot) -> Result<(ConversationView, Incarnations)> {
    let conversation = snapshot
        .conversation
        .clone()
        .ok_or_else(|| Error::request("unknown observed conversation"))?;
    let mut docs = BTreeMap::new();
    let mut incarnations = BTreeMap::new();
    for (address, doc) in &snapshot.documents {
        validate_version(doc.version)?;
        docs.insert(address.kind.clone(), doc.value.clone());
        incarnations.insert(address.kind.clone(), (doc.record.identity.id, doc.version));
    }
    Ok((
        ConversationView {
            conversation,
            entries: snapshot.entries.clone(),
            docs,
        },
        incarnations,
    ))
}
fn validate_version(version: u32) -> Result<()> {
    if version == 1 {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::Definition,
            "mounted document version mismatch; migrations are unsupported",
        ))
    }
}
fn ended_error(end: &WatchEnd) -> Error {
    match end {
        WatchEnd::SessionFailed(error) => error.clone(),
        _ => Error::new(ErrorKind::Closed, "conversation mount is closed"),
    }
}
type Incarnations = BTreeMap<String, (DocumentId, u32)>;
struct MountData {
    value: Arc<ConversationView>,
    docs: Incarnations,
    watches: Vec<Weak<WatchInner>>,
    tasks: Arc<BTreeMap<TaskId, TaskRecord>>,
    attachments: Vec<Weak<CommitInner>>,
    end: Option<WatchEnd>,
}
enum MountCommand {
    Current(oneshot::Sender<Arc<ConversationView>>),
    Attach(oneshot::Sender<Result<ConversationCommitAttachment>>),
}
struct Mount {
    data: Mutex<MountData>,
    requests: mpsc::UnboundedSender<MountCommand>,
}
impl Mount {
    fn ensure_open(&self) -> Result<()> {
        match &self.data.lock().expect("mount lock").end {
            Some(end) => Err(ended_error(end)),
            None => Ok(()),
        }
    }
    async fn current(&self) -> Arc<ConversationView> {
        let (send, receive) = oneshot::channel();
        if self.requests.send(MountCommand::Current(send)).is_ok()
            && let Ok(value) = receive.await
        {
            return value;
        }
        self.data.lock().expect("mount lock").value.clone()
    }
    async fn attach_commits(self: &Arc<Self>) -> Result<ConversationCommitAttachment> {
        let (send, receive) = oneshot::channel();
        if self.requests.send(MountCommand::Attach(send)).is_err() {
            return Err(self.closed_error());
        }
        receive.await.map_err(|_| self.closed_error())?
    }
    fn closed_error(&self) -> Error {
        self.data
            .lock()
            .expect("mount lock")
            .end
            .as_ref()
            .map_or_else(
                || Error::new(ErrorKind::Closed, "conversation mount is closed"),
                ended_error,
            )
    }
    fn register_commits(self: &Arc<Self>) -> Result<ConversationCommitAttachment> {
        let mut data = self.data.lock().expect("mount lock");
        if let Some(end) = &data.end {
            return Err(ended_error(end));
        }
        let baseline = ConversationCommitBaseline {
            value: data.value.clone(),
            tasks: data.tasks.clone(),
        };
        let inner = CommitInner::new(self.clone());
        data.attachments.push(Arc::downgrade(&inner));
        Ok(ConversationCommitAttachment {
            baseline,
            custody: Arc::new(CommitCustody { inner }),
        })
    }
    fn advance(&self, frame: &ObservationFrame) -> Result<()> {
        let mut data = self.data.lock().expect("mount lock");
        if data.end.is_some() {
            return Ok(());
        }
        let mut doc_ops = Vec::new();
        let mut entry_ops = Vec::new();
        let mut entries = data.value.entries.clone();
        let conversation = data.value.conversation.id;
        for change in &frame.publication.changes {
            if let CommitChange::Table(StorageWrite::Entry { value: entry }) = change
                && entry.conversation_id == conversation
            {
                let raw = serde_json::to_value(entry)?;
                if let Some(target) = entry.head {
                    let kept = entries
                        .iter()
                        .position(|e| e.head.is_none() && e.id >= target)
                        .unwrap_or(entries.len());
                    entry_ops.push(Op::Splice(vec!["entries".into()], 0, kept, vec![raw]));
                    entries = std::iter::once(entry.clone())
                        .chain(entries.into_iter().skip(kept))
                        .collect();
                } else {
                    entry_ops.push(Op::Splice(
                        vec!["entries".into()],
                        entries.len(),
                        0,
                        vec![raw],
                    ));
                    entries.push(entry.clone());
                }
                continue;
            }
            let (record, version, value, ops) = match change {
                CommitChange::Document {
                    record,
                    conversation_id: Some(id),
                    version,
                    value,
                    ops,
                    ..
                } if *id == conversation => (record, *version, value, Some(ops.as_slice())),
                CommitChange::DocumentCopy {
                    record,
                    conversation_id: id,
                    version,
                    value,
                    ..
                } if *id == conversation => (record, Some(*version), value, None),
                _ => continue,
            };
            let identity = &record.identity;
            if identity.key.is_some() || !MOUNTED_KINDS.contains(&identity.kind.as_str()) {
                continue;
            }
            let path: Path = vec!["docs".into(), identity.kind.as_str().into()];
            let mounted = data.docs.get(&identity.kind).copied();
            if let Some(version) = version {
                validate_version(version)?;
                if mounted == Some((identity.id, version)) {
                    if let Some(ops) = ops {
                        doc_ops.extend(ops.iter().map(|op| prefixed(op, &path)));
                    }
                } else {
                    data.docs
                        .insert(identity.kind.clone(), (identity.id, version));
                    // Use this immutable publication's value, including copied documents.
                    doc_ops.push(Op::Set(path, (**value).clone()));
                }
            } else if mounted.is_some_and(|(id, _)| id == identity.id) {
                data.docs.remove(&identity.kind);
                doc_ops.push(Op::Delete(path));
            }
        }
        let before = data.value.clone();
        let mut ops = doc_ops;
        ops.extend(entry_ops);
        if !ops.is_empty() {
            let raw = serde_json::to_value(before.as_ref())?;
            let raw = pi_durable_session::apply(&raw, &ops)?;
            let value: ConversationView = serde_json::from_value(raw)?;
            debug_assert_eq!(value.entries, entries);
            data.value = Arc::new(value);
            let output = ConversationViewFrame {
                seq: frame.publication.seq,
                value: data.value.clone(),
                ops: ops.clone(),
            };
            data.watches.retain(|watch| watch.strong_count() > 0);
            for watch in data.watches.iter().filter_map(Weak::upgrade) {
                watch.advance(output.clone());
            }
        }
        // This is the captured task map for this exact publication, never a later read.
        data.tasks = Arc::new(frame.snapshot.tasks.clone());
        let output = Arc::new(ConversationCommitFrame {
            before,
            after: data.value.clone(),
            ops,
            publication: frame.publication.clone(),
        });
        data.attachments
            .retain(|attachment| attachment.strong_count() > 0);
        for attachment in data.attachments.iter().filter_map(Weak::upgrade) {
            attachment.advance(output.clone());
        }
        Ok(())
    }
    fn finish(&self, end: WatchEnd) {
        let mut data = self.data.lock().expect("mount lock");
        if data.end.is_some() {
            return;
        }
        data.end = Some(end.clone());
        for watch in data.watches.drain(..).filter_map(|watch| watch.upgrade()) {
            watch.finish(end.clone());
        }
        for attachment in data
            .attachments
            .drain(..)
            .filter_map(|inner| inner.upgrade())
        {
            attachment.finish(end.clone());
        }
    }
}
fn prefixed(op: &Op, prefix: &Path) -> Op {
    let at = |path: &Path| prefix.iter().chain(path).cloned().collect();
    match op {
        Op::Replace(value) => Op::Set(prefix.clone(), value.clone()),
        Op::Set(path, value) => Op::Set(at(path), value.clone()),
        Op::Delete(path) => Op::Delete(at(path)),
        Op::Append(path, text) => Op::Append(at(path), text.clone()),
        Op::Trim(path, count) => Op::Trim(at(path), *count),
        Op::Splice(path, index, remove, items) => {
            Op::Splice(at(path), *index, *remove, items.clone())
        }
        Op::Permute(path, order) => Op::Permute(at(path), order.clone()),
    }
}
async fn drive(
    mount: Weak<Mount>,
    session: Session,
    mut observation: ObservationSubscription,
    mut commands: mpsc::UnboundedReceiver<MountCommand>,
) {
    enum Event {
        Frame(Option<Arc<ObservationFrame>>),
        Command(Option<MountCommand>),
    }
    loop {
        let event = {
            let frame = observation.next();
            let command = commands.recv();
            pin_mut!(frame, command);
            match future::select(frame, command).await {
                future::Either::Left((frame, _)) => Event::Frame(frame),
                future::Either::Right((command, _)) => Event::Command(command),
            }
        };
        let Some(mount) = mount.upgrade() else { break };
        let mut ended = false;
        let mut response = None;
        match event {
            Event::Frame(Some(frame)) => {
                if let Err(error) = mount.advance(&frame) {
                    mount.finish(WatchEnd::SessionFailed(error));
                    break;
                }
            }
            Event::Frame(None) => ended = true,
            Event::Command(None) => break,
            Event::Command(Some(command)) => {
                // A native state read drains immutable frames already committed when
                // the read reaches this worker, without rereading latest storage.
                while let Some(next) = observation.next().now_or_never() {
                    match next {
                        Some(frame) => {
                            if let Err(error) = mount.advance(&frame) {
                                mount.finish(WatchEnd::SessionFailed(error));
                                ended = true;
                                break;
                            }
                        }
                        None => {
                            ended = true;
                            break;
                        }
                    }
                }
                response = Some(command);
            }
        }
        if ended {
            mount.finish(
                session
                    .failure()
                    .map_or(WatchEnd::SessionClosed, WatchEnd::SessionFailed),
            );
        }
        // Responses and attachment registration happen only after adoption and
        // termination latching, on this driver, so current tasks/view cannot race.
        match response {
            Some(MountCommand::Current(reply)) => {
                let _ = reply.send(mount.data.lock().expect("mount lock").value.clone());
            }
            Some(MountCommand::Attach(reply)) => {
                let _ = reply.send(mount.register_commits());
            }
            None => (),
        }
        if ended {
            break;
        }
    }
}

/// One atomic baseline for the runtime's conversation event translation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConversationCommitBaseline {
    pub value: Arc<ConversationView>,
    pub tasks: Arc<BTreeMap<TaskId, TaskRecord>>,
}
/// Every complete conversation publication, including an unchanged mounted view.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConversationCommitFrame {
    pub before: Arc<ConversationView>,
    pub after: Arc<ConversationView>,
    pub ops: Vec<Op>,
    pub publication: Arc<CommitPublication>,
}
struct CommitData {
    pending: VecDeque<Arc<ConversationCommitFrame>>,
    end: Option<WatchEnd>,
}
struct CommitInner {
    data: Mutex<CommitData>,
    notify: Notify,
    ended: watch::Sender<Option<WatchEnd>>,
    delivery: tokio::sync::Mutex<()>,
    mount: Mutex<Option<Arc<Mount>>>,
}
impl CommitInner {
    fn new(mount: Arc<Mount>) -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(CommitData {
                pending: VecDeque::new(),
                end: None,
            }),
            notify: Notify::new(),
            ended: watch::channel(None).0,
            delivery: tokio::sync::Mutex::new(()),
            mount: Mutex::new(Some(mount)),
        })
    }
    fn advance(&self, frame: Arc<ConversationCommitFrame>) {
        let mut data = self.data.lock().expect("commit attachment lock");
        if data.end.is_none() {
            data.pending.push_back(frame);
            self.notify.notify_one();
        }
    }
    fn finish(&self, end: WatchEnd) {
        let mut data = self.data.lock().expect("commit attachment lock");
        if data.end.is_some() {
            return;
        }
        data.end = Some(end.clone());
        data.pending.clear();
        self.ended.send_replace(Some(end));
        self.notify.notify_one();
        drop(data);
        self.mount.lock().expect("attachment mount lock").take();
    }
    async fn next(&self) -> Option<Arc<ConversationCommitFrame>> {
        let _delivery = self.delivery.lock().await;
        loop {
            let notified = self.notify.notified();
            {
                let mut data = self.data.lock().expect("commit attachment lock");
                if data.end.is_some() {
                    return None;
                }
                if let Some(frame) = data.pending.pop_front() {
                    return Some(frame);
                }
            }
            notified.await;
        }
    }
}
// Runtime handles alone own custody. No delivery/projection task retains it.
struct CommitCustody {
    inner: Arc<CommitInner>,
}
impl Drop for CommitCustody {
    fn drop(&mut self) {
        self.inner.finish(WatchEnd::Stopped);
    }
}
/// Crate-private lossless input to runtime event translation; not a public watch.
#[derive(Clone)]
pub(crate) struct ConversationCommitAttachment {
    pub baseline: ConversationCommitBaseline,
    custody: Arc<CommitCustody>,
}
impl ConversationCommitAttachment {
    pub(crate) async fn next(&self) -> Option<Arc<ConversationCommitFrame>> {
        self.custody.inner.next().await
    }
    pub(crate) fn end(&self) -> Option<WatchEnd> {
        self.custody
            .inner
            .data
            .lock()
            .expect("commit attachment lock")
            .end
            .clone()
    }
    pub(crate) fn stop(&self) -> WatchEnd {
        self.custody.inner.finish(WatchEnd::Stopped);
        self.end().expect("stopped attachment")
    }
    pub(crate) fn cancel(&self) -> WatchEnd {
        self.custody.inner.finish(WatchEnd::Cancelled);
        self.end().expect("cancelled attachment")
    }
    pub(crate) async fn closed(&self) -> WatchEnd {
        let mut receive = self.custody.inner.ended.subscribe();
        loop {
            if let Some(end) = receive.borrow_and_update().clone() {
                return end;
            }
            if receive.changed().await.is_err() {
                return WatchEnd::Stopped;
            }
        }
    }
}

/// Disposable owned state. Read awaits adoption of already queued publications.
#[derive(Clone)]
pub struct ConversationViewState {
    mount: Arc<Mount>,
}
impl ConversationViewState {
    pub async fn value(&self) -> Arc<ConversationView> {
        self.mount.current().await
    }
    pub fn end(&self) -> Option<WatchEnd> {
        self.mount.data.lock().expect("mount lock").end.clone()
    }
}
struct WatchData {
    value: Arc<ConversationView>,
    pending: VecDeque<ConversationViewFrame>,
    end: Option<WatchEnd>,
    mode: u8,
}
struct WatchInner {
    data: Mutex<WatchData>,
    notify: Notify,
    ended: watch::Sender<Option<WatchEnd>>,
    delivery: tokio::sync::Mutex<()>,
    mount: Mutex<Option<Arc<Mount>>>,
}
impl WatchInner {
    fn new(value: Arc<ConversationView>, mount: Arc<Mount>) -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(WatchData {
                value,
                pending: VecDeque::new(),
                end: None,
                mode: 0,
            }),
            notify: Notify::new(),
            ended: watch::channel(None).0,
            delivery: tokio::sync::Mutex::new(()),
            mount: Mutex::new(Some(mount)),
        })
    }
    fn advance(&self, mut frame: ConversationViewFrame) {
        let mut data = self.data.lock().expect("view watch lock");
        if data.end.is_some() {
            return;
        }
        if data.pending.len() >= 100 {
            data.pending.clear();
            frame.ops = vec![Op::Replace(
                serde_json::to_value(frame.value.as_ref()).expect("view JSON"),
            )];
        }
        data.pending.push_back(frame);
        self.notify.notify_one();
    }
    fn finish(&self, end: WatchEnd) {
        let mut data = self.data.lock().expect("view watch lock");
        if data.end.is_some() {
            return;
        }
        data.end = Some(end.clone());
        data.pending.clear();
        self.ended.send_replace(Some(end));
        self.notify.notify_one();
        drop(data);
        self.mount.lock().expect("watch mount lock").take();
    }
    async fn next_raw(&self) -> Option<ConversationViewFrame> {
        loop {
            let notified = self.notify.notified();
            {
                let mut data = self.data.lock().expect("view watch lock");
                if data.end.is_some() {
                    return None;
                }
                if let Some(frame) = data.pending.pop_front() {
                    data.value = frame.value.clone();
                    return Some(frame);
                }
            }
            notified.await;
        }
    }
}
// Only public handle clones share this custody. Delivery workers may retain
// WatchInner while a callback finishes, but cannot keep the attachment alive.
struct WatchCustody {
    inner: Arc<WatchInner>,
}
impl Drop for WatchCustody {
    fn drop(&mut self) {
        self.inner.finish(WatchEnd::Stopped);
    }
}
/// Serialized bounded convergence watch. The acquisition revision is held until delivery.
/// Dropping its last public clone stops future delivery and releases its attachment.
#[derive(Clone)]
pub struct ConversationViewWatch {
    custody: Arc<WatchCustody>,
}
impl ConversationViewWatch {
    pub fn value(&self) -> Arc<ConversationView> {
        self.custody
            .inner
            .data
            .lock()
            .expect("view watch lock")
            .value
            .clone()
    }
    pub fn end(&self) -> Option<WatchEnd> {
        self.custody
            .inner
            .data
            .lock()
            .expect("view watch lock")
            .end
            .clone()
    }
    pub fn stop(&self) -> WatchEnd {
        self.custody.inner.finish(WatchEnd::Stopped);
        self.end().expect("stopped")
    }
    pub fn cancel(&self) -> WatchEnd {
        self.custody.inner.finish(WatchEnd::Cancelled);
        self.end().expect("cancelled")
    }
    pub async fn closed(&self) -> WatchEnd {
        let mut receive = self.custody.inner.ended.subscribe();
        loop {
            if let Some(end) = receive.borrow_and_update().clone() {
                return end;
            }
            if receive.changed().await.is_err() {
                return WatchEnd::Stopped;
            }
        }
    }
    pub async fn next(&self) -> Result<Option<ConversationViewFrame>> {
        {
            let mut data = self.custody.inner.data.lock().expect("view watch lock");
            if data.mode == 2 {
                return Err(Error::validation("view watch already has a listener"));
            }
            data.mode = 1;
        }
        let _delivery = self.custody.inner.delivery.lock().await;
        Ok(self.custody.inner.next_raw().await)
    }
    pub fn start<F, Fut>(&self, mut listener: F) -> Result<()>
    where
        F: FnMut(ConversationViewFrame) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        {
            let mut data = self.custody.inner.data.lock().expect("view watch lock");
            if data.mode != 0 || data.end.is_some() {
                return Err(Error::validation("view watch already started or ended"));
            }
            data.mode = 2;
        }
        let inner = self.custody.inner.clone();
        let worker_inner = inner.clone();
        let worker = tokio::spawn(async move {
            while let Some(frame) = worker_inner.next_raw().await {
                listener(frame).await?;
            }
            Ok::<_, Error>(())
        });
        tokio::spawn(async move {
            match worker.await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => inner.finish(WatchEnd::ListenerError(error.to_string())),
                Err(error) => inner.finish(WatchEnd::ListenerError(error.to_string())),
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests;
