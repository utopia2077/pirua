//! Neutral ordered transport owner for current language-neutral bindings.
//!
//! Host-issued request IDs are odd, peer-issued IDs are even and monotonically
//! increasing. IDs are never reused. Retired replies are discarded without
//! becoming another invocation's terminal. No runtime, credential or storage
//! authority is inferred from peer data.
use crate::{
    activation::{ActiveGrant, HostAuthority},
    endpoint::{Endpoint, EndpointEvent, SelectedEnvelope},
    profiles::{self, NativeError, NativeValidator, Stage, ValidatedPayload},
    wire::{framing, v1_1 as w, value},
};
use prost::Message;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, Semaphore, mpsc};

pub type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
/// One ordered full-duplex channel. Exactly one receive future is active.
/// Implementations must make receive cancellation-safe (select may drop it).
/// Each receive chunk is bounded by the negotiated frame byte limit plus ten
/// prefix bytes; stream adapters should read bounded chunks rather than collect.
pub trait OrderedTransport: Send + Sync + 'static {
    fn send(&self, bytes: Vec<u8>) -> TransportFuture<'_, ()>;
    fn receive(&self) -> TransportFuture<'_, Option<Vec<u8>>>;
    fn close(&self) {}
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Transport,
    Closed,
    Handshake,
    Protocol,
    Forbidden,
    Overloaded,
    Cancelled,
    Deadline,
    NativeRejected,
    HostValidation,
    Remote(w::ErrorCode),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "extension connection: {self:?}")
    }
}
impl std::error::Error for Error {}
/// Release only when the callback's return is a real downstream acknowledgement.
/// Retain bounds total progress admitted over one invocation's lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressCredit {
    Release,
    Retain,
}
#[derive(Clone, Copy, Debug)]
pub struct ConnectionConfig {
    pub io_timeout: Duration,
    pub request_timeout: Duration,
    pub lifetime: Duration,
}
impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            io_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            lifetime: Duration::from_secs(3600),
        }
    }
}
/// Required per-invocation native admission. No lock is held while this runs.
pub type Validator = Arc<dyn Fn(&ValidatedPayload) -> Result<(), NativeError> + Send + Sync>;
/// Explicit host-installed synchronous snapshot action. It must not perform I/O
/// or reenter the connection. The host rechecks captured session custody here.
pub type SnapshotHandler = Arc<dyn Fn() -> Result<serde_json::Value, Error> + Send + Sync>;
struct Native(Validator);
impl NativeValidator for Native {
    fn validate(&mut self, p: &ValidatedPayload) -> Result<(), NativeError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.0)(p)))
            .unwrap_or(Err(NativeError::LocalFault))
    }
}
#[derive(Debug)]
enum Reply {
    Started(u64),
    Progress(serde_json::Value, u64),
    Terminal(serde_json::Value),
}
struct Pending {
    capability: w::CapabilityRef,
    method: String,
    validator: Validator,
    replies: mpsc::Sender<Result<Reply, Error>>,
    sequence: u64,
    credit: u64,
    stream: bool,
    deadline: tokio::time::Instant,
}
struct Start {
    deadline: tokio::time::Instant,
    capability: w::CapabilityRef,
    method: String,
    payload: Vec<u8>,
    validator: Validator,
    replies: mpsc::Sender<Result<Reply, Error>>,
}
enum Command {
    Start(Box<Start>),
    Cancel(u64, Error),
    Credit(u64, u64),
}
struct Shared {
    current: AtomicBool,
    stop: Notify,
    commands: mpsc::Sender<Command>,
    snapshot: Mutex<Option<SnapshotHandler>>,
    seats: Arc<Semaphore>,
}
struct Lease(Weak<Shared>);
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(shared) = self.0.upgrade() {
            shared.current.store(false, Ordering::Release);
            shared.stop.notify_one();
        }
    }
}
#[derive(Clone)]
pub struct HostConnection {
    grant: ActiveGrant,
    shared: Arc<Shared>,
    config: ConnectionConfig,
    _lease: Arc<Lease>,
}
impl HostConnection {
    pub async fn connect(
        host: HostAuthority,
        transport: Arc<dyn OrderedTransport>,
        config: ConnectionConfig,
    ) -> Result<Self, Error> {
        if config.io_timeout.is_zero()
            || config.request_timeout.is_zero()
            || config.lifetime.is_zero()
            || Instant::now().checked_add(config.lifetime).is_none()
            || Instant::now().checked_add(config.request_timeout).is_none()
            || Instant::now().checked_add(config.io_timeout).is_none()
        {
            return Err(Error::Handshake);
        }
        if host.limits().blocking_queue_bytes < host.limits().frame_bytes {
            return Err(Error::Handshake);
        }
        let chunk_limit = (host.limits().frame_bytes as usize)
            .checked_add(10)
            .ok_or(Error::Handshake)?;
        let handshake_timeout = config
            .io_timeout
            .min(Duration::from_millis(host.limits().handshake_ms));
        let started = Instant::now();
        let mut endpoint = Endpoint::new(host, 0);
        let mut tail = Vec::new();
        let handshake = async {
            loop {
                if tail.is_empty() {
                    tail = transport.receive().await?.ok_or(Error::Closed)?;
                }
                if tail.len() > chunk_limit {
                    return Err(Error::Protocol);
                }
                let step = endpoint
                    .read_one(&tail, elapsed(started))
                    .map_err(|_| Error::Handshake)?;
                tail.drain(..step.consumed);
                match step.event {
                    Some(EndpointEvent::Send(frame)) => transport.send(frame.bytes).await?,
                    Some(EndpointEvent::Ready) => break,
                    Some(EndpointEvent::Active(_)) => return Err(Error::Handshake),
                    None => {}
                }
            }
            Ok(())
        };
        tokio::time::timeout(handshake_timeout, handshake)
            .await
            .map_err(|_| Error::Deadline)??;
        let grant = endpoint.active_grant().ok_or(Error::Handshake)?.clone();
        if grant
            .wire()
            .selected_protocol
            .as_ref()
            .is_none_or(|v| v.minor != 1)
        {
            return Err(Error::Handshake);
        }
        let max = grant
            .wire()
            .limits
            .as_ref()
            .ok_or(Error::Handshake)?
            .in_flight_requests as usize;
        let (commands, receiver) = mpsc::channel(max);
        let shared = Arc::new(Shared {
            current: AtomicBool::new(true),
            stop: Notify::new(),
            commands,
            snapshot: Mutex::new(None),
            // One full terminal seat is reserved per admitted caller, including
            // queued starts. The permit lives until the caller consumes/drops it.
            seats: Arc::new(Semaphore::new(
                max.min(
                    (grant
                        .wire()
                        .limits
                        .as_ref()
                        .ok_or(Error::Handshake)?
                        .blocking_queue_bytes
                        / grant
                            .wire()
                            .limits
                            .as_ref()
                            .ok_or(Error::Handshake)?
                            .frame_bytes) as usize,
                )
                .max(1),
            )),
        });
        let connection = Self {
            grant: grant.clone(),
            shared: shared.clone(),
            config,
            _lease: Arc::new(Lease(Arc::downgrade(&shared))),
        };
        tokio::spawn(async move {
            run(
                endpoint, grant, transport, shared, receiver, config, started, tail,
            )
            .await;
        });
        Ok(connection)
    }
    pub fn grant(&self) -> &ActiveGrant {
        &self.grant
    }
    pub fn current(&self) -> bool {
        self.shared.current.load(Ordering::Acquire)
    }
    pub fn close(&self) {
        self.shared.current.store(false, Ordering::Release);
        self.shared.stop.notify_one();
    }
    pub fn set_snapshot_handler(&self, handler: SnapshotHandler) -> Result<(), Error> {
        if !self.current() {
            return Err(Error::Closed);
        }
        let previous = {
            self.shared
                .snapshot
                .lock()
                .map_err(|_| Error::HostValidation)?
                .replace(handler)
        };
        drop(previous);
        Ok(())
    }
    pub fn clear_snapshot_handler(&self) {
        let previous = self.shared.snapshot.lock().ok().and_then(|mut h| h.take());
        drop(previous);
    }
    /// Every reply is validated before publication. Progress callbacks run outside
    /// protocol state and are awaited by the caller before admitting more credit.
    pub async fn request(
        &self,
        capability: w::CapabilityRef,
        method: String,
        payload: w::SchemaBoundValue,
        validator: Validator,
        cancel: Pin<Box<dyn Future<Output = ()> + Send + '_>>,
        progress: impl FnMut(serde_json::Value) -> Result<(), Error> + Send,
    ) -> Result<serde_json::Value, Error> {
        self.request_with_progress_credit(
            capability,
            method,
            payload,
            validator,
            cancel,
            ProgressCredit::Release,
            progress,
        )
        .await
    }
    /// Explicit progress admission for consumers whose synchronous callback only
    /// queues a later asynchronous acknowledgement. Coding uses Retain.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_with_progress_credit(
        &self,
        capability: w::CapabilityRef,
        method: String,
        payload: w::SchemaBoundValue,
        validator: Validator,
        cancel: Pin<Box<dyn Future<Output = ()> + Send + '_>>,
        credit: ProgressCredit,
        mut progress: impl FnMut(serde_json::Value) -> Result<(), Error> + Send,
    ) -> Result<serde_json::Value, Error> {
        self.request_inner(
            capability,
            method,
            payload,
            validator,
            cancel,
            credit,
            false,
            move |value| std::future::ready(progress(value)),
        )
        .await
    }
    /// Await each downstream persistent acknowledgement before returning credit.
    /// Cancellation seals callback admission while an already started callback
    /// settles. Existing synchronous Release/Retain consumers keep their behavior.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_with_progress_ack<F, A>(
        &self,
        capability: w::CapabilityRef,
        method: String,
        payload: w::SchemaBoundValue,
        validator: Validator,
        cancel: Pin<Box<dyn Future<Output = ()> + Send + '_>>,
        progress: F,
    ) -> Result<serde_json::Value, Error>
    where
        F: FnMut(serde_json::Value) -> A + Send,
        A: Future<Output = Result<(), Error>> + Send,
    {
        self.request_inner(
            capability,
            method,
            payload,
            validator,
            cancel,
            ProgressCredit::Release,
            true,
            progress,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn request_inner<F, A>(
        &self,
        capability: w::CapabilityRef,
        method: String,
        payload: w::SchemaBoundValue,
        validator: Validator,
        mut cancel: Pin<Box<dyn Future<Output = ()> + Send + '_>>,
        credit: ProgressCredit,
        asynchronous_ack: bool,
        mut progress: F,
    ) -> Result<serde_json::Value, Error>
    where
        F: FnMut(serde_json::Value) -> A + Send,
        A: Future<Output = Result<(), Error>> + Send,
    {
        if !self.current() {
            return Err(Error::Closed);
        }
        let deadline = tokio::time::Instant::now() + self.config.request_timeout;
        let _seat = tokio::select! {
            permit = self.shared.seats.clone().acquire_owned() => permit.map_err(|_|Error::Closed)?,
            _ = &mut cancel => return Err(Error::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(Error::Deadline),
        };
        if payload.encoded_len()
            > self
                .grant
                .wire()
                .limits
                .as_ref()
                .ok_or(Error::Protocol)?
                .frame_bytes as usize
        {
            return Err(Error::Overloaded);
        }
        let l = self.grant.wire().limits.as_ref().ok_or(Error::Protocol)?;
        let queue = (l.progress_queue_bytes / u64::from(l.in_flight_requests))
            .min(l.max_stream_credit_bytes)
            .min(usize::MAX as u64 - 2) as usize
            + 2;
        let (sender, mut receiver) = mpsc::channel(queue);
        let start = Start {
            deadline,
            capability,
            method,
            payload: payload.encode_to_vec(),
            validator,
            replies: sender,
        };
        tokio::select! {
            r = self.shared.commands.send(Command::Start(Box::new(start))) => r.map_err(|_| Error::Closed)?,
            _ = &mut cancel => return Err(Error::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(Error::Deadline),
        }
        // The first internal terminal announces the host-issued ID, never peer data.
        let admitted = tokio::select! {
            r = receiver.recv() => r,
            _ = &mut cancel => { self.close(); return Err(Error::Cancelled); },
            _ = tokio::time::sleep_until(deadline) => { self.close(); return Err(Error::Deadline); },
        };
        let id = match admitted {
            Some(Ok(Reply::Started(id))) => id,
            Some(Err(e)) => return Err(e),
            _ => return Err(Error::Closed),
        };
        let mut guard = CallGuard {
            shared: self.shared.clone(),
            id,
            settled: false,
        };
        let admit_progress = AtomicBool::new(true);
        let read = async {
            while let Some(reply) = receiver.recv().await {
                match reply? {
                    Reply::Progress(v, bytes) => {
                        if !admit_progress.load(Ordering::Acquire)
                            || (asynchronous_ack && !self.current())
                        {
                            continue;
                        }
                        progress(v).await?;
                        if credit == ProgressCredit::Release
                            && admit_progress.load(Ordering::Acquire)
                            && (!asynchronous_ack || self.current())
                        {
                            self.shared
                                .commands
                                .try_send(Command::Credit(id, bytes))
                                .map_err(|_| Error::Overloaded)?;
                        }
                    }
                    Reply::Terminal(v) => {
                        return if asynchronous_ack && !self.current() {
                            Err(Error::Closed)
                        } else {
                            Ok(v)
                        };
                    }
                    Reply::Started(_) => return Err(Error::Protocol),
                }
            }
            Err(Error::Closed)
        };
        tokio::pin!(read);
        let result = tokio::select! {
            r=&mut read=>r,
            _=&mut cancel=>{
                if asynchronous_ack {
                    admit_progress.store(false, Ordering::Release);
                    let _ = self.shared.commands.send(Command::Cancel(id,Error::Cancelled)).await;
                    match (&mut read).await {
                        Err(e) if !matches!(e, Error::Cancelled | Error::Closed) => Err(e),
                        _ => Err(Error::Cancelled),
                    }
                } else {
                let settle=async {self.shared.commands.send(Command::Cancel(id,Error::Cancelled)).await.map_err(|_|Error::Closed)?;(&mut read).await};
                match tokio::time::timeout(self.config.io_timeout,settle).await {Ok(r)=>r,Err(_)=>{self.close();Err(Error::Cancelled)}}
                }
            },
            _=tokio::time::sleep_until(deadline)=>{
                if asynchronous_ack {
                    admit_progress.store(false, Ordering::Release);
                    let _ = self.shared.commands.send(Command::Cancel(id,Error::Deadline)).await;
                    match (&mut read).await {
                        Err(e) if !matches!(e, Error::Deadline | Error::Closed) => Err(e),
                        _ => Err(Error::Deadline),
                    }
                } else {
                let settle=async {self.shared.commands.send(Command::Cancel(id,Error::Deadline)).await.map_err(|_|Error::Closed)?;(&mut read).await};
                match tokio::time::timeout(self.config.io_timeout,settle).await {Ok(r)=>r,Err(_)=>{self.close();Err(Error::Deadline)}}
                }
            }
        };
        if result.is_ok() {
            guard.settled = true;
        } else {
            guard.reason(result.clone().err().unwrap_or(Error::Closed));
            guard.settled = true;
        }
        result
    }
}
struct CallGuard {
    shared: Arc<Shared>,
    id: u64,
    settled: bool,
}
impl CallGuard {
    fn reason(&self, e: Error) {
        if self
            .shared
            .commands
            .try_send(Command::Cancel(self.id, e))
            .is_err()
        {
            self.shared.current.store(false, Ordering::Release);
            self.shared.stop.notify_one();
        }
    }
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        if !self.settled {
            self.reason(Error::Cancelled)
        }
    }
}
fn elapsed(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}
fn validation(e: profiles::Error) -> Error {
    match e {
        profiles::Error::NativeRejected => Error::NativeRejected,
        profiles::Error::HostLocalFault => Error::HostValidation,
        _ => Error::Protocol,
    }
}
fn limits(grant: &ActiveGrant) -> value::Limits {
    let l = grant.wire().limits.as_ref().expect("Ready grant");
    value::Limits {
        depth: l.value_depth as usize,
        elements: l.value_elements as usize,
        string_bytes: l.string_bytes as usize,
        bytes_length: l.bytes_length as usize,
        decoded_bytes: l.aggregate_decoded_bytes as usize,
        json_bytes: l.aggregate_decoded_bytes as usize,
    }
}
fn json_limits(grant: &ActiveGrant) -> crate::jcs::Limits {
    let l = grant.wire().limits.as_ref().expect("Ready grant");
    crate::jcs::Limits {
        input_bytes: l.frame_bytes as usize,
        output_bytes: l.aggregate_decoded_bytes as usize,
        depth: l.value_depth as usize,
        nodes: l.value_elements as usize,
        elements: l.value_elements as usize,
        decoded_bytes: l.aggregate_decoded_bytes as usize,
        schema_work: (l.aggregate_decoded_bytes as usize).saturating_mul(8),
        instance_work: (l.aggregate_decoded_bytes as usize).saturating_mul(8),
    }
}
fn validate(
    grant: &ActiveGrant,
    p: &Pending,
    stage: Stage,
    bytes: &[u8],
) -> Result<serde_json::Value, Error> {
    let contract = grant
        .profiles()
        .iter()
        .find(|g| g.reference() == &p.capability)
        .ok_or(Error::Forbidden)?;
    let candidate = contract
        .approved()
        .validate_payload(
            1,
            &p.method,
            stage,
            bytes,
            limits(grant),
            json_limits(grant),
            &mut Native(p.validator.clone()),
        )
        .map_err(validation)?;
    Ok(candidate.fields)
}
async fn send(
    transport: &Arc<dyn OrderedTransport>,
    grant: &ActiveGrant,
    frame: w::envelope::Frame,
    timeout: Duration,
) -> Result<(), Error> {
    let max = NonZeroUsize::new(
        grant
            .wire()
            .limits
            .as_ref()
            .ok_or(Error::Protocol)?
            .frame_bytes as usize,
    )
    .ok_or(Error::Protocol)?;
    let encoded =
        framing::encode(&w::Envelope { frame: Some(frame) }, max).map_err(|_| Error::Protocol)?;
    tokio::time::timeout(timeout, transport.send(encoded.bytes))
        .await
        .map_err(|_| Error::Deadline)?
}
#[allow(clippy::too_many_arguments)]
async fn run(
    mut endpoint: Endpoint,
    grant: ActiveGrant,
    transport: Arc<dyn OrderedTransport>,
    shared: Arc<Shared>,
    mut commands: mpsc::Receiver<Command>,
    config: ConnectionConfig,
    started: Instant,
    mut tail: Vec<u8>,
) {
    let mut calls: BTreeMap<u64, Pending> = BTreeMap::new();
    let mut next = 1u64;
    let mut peer_last = 0u64;
    let mut rate: VecDeque<Instant> = VecDeque::new();
    let outcome:Result<(),Error>=async {
        loop {
            if !shared.current.load(Ordering::Acquire){return Err(Error::Closed)}
            if !tail.is_empty(){
                let step=endpoint.read_one(&tail,elapsed(started)).map_err(|_|Error::Protocol)?;
                tail.drain(..step.consumed);
                if let Some(EndpointEvent::Active(frame))=step.event{
                    let SelectedEnvelope::V1_1(e)=frame.envelope else{return Err(Error::Protocol)};
                    match e.frame.ok_or(Error::Protocol)? {
                        w::envelope::Frame::Response(r)=>terminal(&grant,&mut calls,next,r.request_id,r.terminal,false)?,
                        w::envelope::Frame::StreamEnd(r)=>{
                            let t=r.terminal.map(|t|match t {w::stream_end::Terminal::Result(b)=>w::response::Terminal::Result(b),w::stream_end::Terminal::Error(e)=>w::response::Terminal::Error(e)});
                            terminal(&grant,&mut calls,next,r.request_id,t,true)?;
                        }
                        w::envelope::Frame::StreamItem(item)=>{
                            if let Some(p)=calls.get_mut(&item.request_id){
                                if !p.stream || item.sequence!=p.sequence+1 || frame.body_len as u64>p.credit{return Err(Error::Protocol)}
                                let value=validate(&grant,p,Stage::Progress,&item.payload)?;
                                p.sequence=item.sequence;p.credit-=frame.body_len as u64;
                                p.replies.try_send(Ok(Reply::Progress(value, frame.body_len as u64))).map_err(|_|Error::Overloaded)?;
                            }else if item.request_id>=next || item.request_id%2==0{return Err(Error::Protocol)}
                        }
                        w::envelope::Frame::Request(r)=>{
                            if r.request_id==0 || r.request_id%2!=0 || r.request_id<=peer_last{return Err(Error::Protocol)}
                            peer_last=r.request_id;
                            if !admit_rate(&mut rate, grant.wire().limits.as_ref().ok_or(Error::Protocol)?.requests_per_second) {return Err(Error::Overloaded)}
                            let response=snapshot_request(&grant,&shared,&r);
                            let terminal=match response{Ok(v)=>w::response::Terminal::Result(v.encode_to_vec()),Err(e)=>w::response::Terminal::Error(w::TypedError{code:match e{Error::Forbidden=>w::ErrorCode::Forbidden as i32,Error::Closed=>w::ErrorCode::StaleBinding as i32,_=>w::ErrorCode::InvalidRequest as i32},safe_message:"host action rejected".into()})};
                            send(&transport,&grant,w::envelope::Frame::Response(w::Response{request_id:r.request_id,terminal:Some(terminal)}),config.io_timeout).await?;
                        }
                        w::envelope::Frame::GoAway(_)=>return Err(Error::Closed),
                        _=>return Err(Error::Protocol),
                    }
                }
                continue;
            }
            tokio::select! {
                _=shared.stop.notified()=>return Err(Error::Closed),
                _=tokio::time::sleep(config.lifetime.saturating_sub(started.elapsed()))=>return Err(Error::Deadline),
                bytes=transport.receive()=>{tail=match bytes?{Some(bytes)=>bytes,None=>{endpoint.finish().map_err(|_|Error::Protocol)?;return Err(Error::Closed)}};if tail.is_empty() || tail.len()>grant.wire().limits.as_ref().ok_or(Error::Protocol)?.frame_bytes as usize+10{return Err(Error::Protocol)}},
                command=commands.recv()=>match command.ok_or(Error::Closed)? {
                    Command::Start(start)=>{
                        let fail=|e|{let _=start.replies.try_send(Err(e));};
                        if calls.len()>=grant.wire().limits.as_ref().ok_or(Error::Protocol)?.in_flight_requests as usize {fail(Error::Overloaded);continue;}
                        let Some(contract)=grant.profiles().iter().find(|g|g.reference()==&start.capability) else {fail(Error::Forbidden);continue};
                        let Some((direction,mode))=contract.approved().registry().method(contract.approved().profile(),&start.method) else {fail(Error::Forbidden);continue};
                        if !matches!(contract.approved().profile(),"coding.v1"|"durable.v1") || direction!=profiles::Direction::CoreToExtension {fail(Error::Forbidden);continue;}
                        if !admit_rate(&mut rate, grant.wire().limits.as_ref().ok_or(Error::Protocol)?.requests_per_second) {fail(Error::Overloaded);continue;}
                        let stream=mode==profiles::Mode::Stream;
                        let l=grant.wire().limits.as_ref().ok_or(Error::Protocol)?;
                        let credit=(l.progress_queue_bytes/u64::from(l.in_flight_requests)).min(l.max_stream_credit_bytes);
                        let p=Pending{capability:start.capability.clone(),method:start.method.clone(),validator:start.validator,replies:start.replies.clone(),sequence:0,credit,stream,deadline:start.deadline};
                        if let Err(e)=validate(&grant,&p,Stage::Request,&start.payload){let _=p.replies.try_send(Err(e));continue;}
                        let id=next;next=next.checked_add(2).ok_or(Error::Closed)?;
                        let request=w::Request{request_id:id,parent_request_id:None,capability_ref:Some(start.capability),effect_key:None,method:start.method,deadline_after_ms:Some(config.request_timeout.as_millis().min(u128::from(u64::MAX)) as u64),initial_stream_credit_bytes:stream.then_some(credit),payload:start.payload};
                        p.replies.try_send(Ok(Reply::Started(id))).map_err(|_|Error::Closed)?;
                        calls.insert(id,p);
                        send(&transport,&grant,w::envelope::Frame::Request(request),config.io_timeout).await?;
                    }
                    Command::Credit(id,bytes)=>{if let Some(p)=calls.get_mut(&id){p.credit=p.credit.checked_add(bytes).ok_or(Error::Protocol)?;send(&transport,&grant,w::envelope::Frame::Credit(w::Credit{request_id:id,additional_bytes:bytes}),config.io_timeout).await?;}}
                    Command::Cancel(id,reason)=>{if let Some(p)=calls.remove(&id){let _=p.replies.try_send(Err(reason.clone()));send(&transport,&grant,w::envelope::Frame::Cancel(w::Cancel{request_id:id,reason:if reason==Error::Deadline{w::CancelReason::CancelDeadline as i32}else{w::CancelReason::CancelUser as i32}}),config.io_timeout).await?;}}
                }
            }
        }
    }.await;
    let error = outcome.err().unwrap_or(Error::Closed);
    shared.current.store(false, Ordering::Release);
    let previous = shared.snapshot.lock().ok().and_then(|mut h| h.take());
    drop(previous);
    for (_, p) in calls {
        let _ = p.replies.try_send(Err(error.clone()));
    }
    shared.seats.close();
    transport.close();
}
fn terminal(
    grant: &ActiveGrant,
    calls: &mut BTreeMap<u64, Pending>,
    next: u64,
    id: u64,
    t: Option<w::response::Terminal>,
    stream: bool,
) -> Result<(), Error> {
    let Some(p) = calls.get(&id) else {
        return if id > 0 && id < next && id % 2 == 1 {
            Ok(())
        } else {
            Err(Error::Protocol)
        };
    };
    if p.stream != stream {
        return Err(Error::Protocol);
    }
    let mut result = match t.ok_or(Error::Protocol)? {
        w::response::Terminal::Result(bytes) => {
            validate(grant, p, Stage::Terminal, &bytes).map(Reply::Terminal)
        }
        w::response::Terminal::Error(e) => {
            let code = w::ErrorCode::try_from(e.code).map_err(|_| Error::Protocol)?;
            if code == w::ErrorCode::Unspecified {
                return Err(Error::Protocol);
            }
            Err(Error::Remote(code))
        }
    };
    if result.is_ok() && tokio::time::Instant::now() >= p.deadline {
        result = Err(Error::Deadline);
    }
    // Invalid native or structural terminals never settle the call as success.
    if matches!(
        result,
        Err(Error::NativeRejected | Error::Protocol | Error::HostValidation)
    ) {
        return Err(result.expect_err("checked error"));
    }
    let p = calls.remove(&id).ok_or(Error::Protocol)?;
    p.replies.try_send(result).map_err(|_| Error::Overloaded)?;
    Ok(())
}
fn snapshot_request(
    grant: &ActiveGrant,
    shared: &Shared,
    r: &w::Request,
) -> Result<w::SchemaBoundValue, Error> {
    let cap = r.capability_ref.as_ref().ok_or(Error::Forbidden)?;
    let profile = grant
        .profiles()
        .iter()
        .find(|g| g.reference() == cap)
        .ok_or(Error::Forbidden)?;
    if profile.approved().profile() != "coding.v1"
        || profile.approved().name() != "session.snapshot"
        || r.method != "session.snapshot"
        || r.parent_request_id.is_some()
        || r.effect_key.is_some()
        || r.initial_stream_credit_bytes.is_some()
        || r.deadline_after_ms.is_none_or(|d| d == 0)
        || !profile
            .approved()
            .permissions()
            .contains(&("session.snapshot".into(), profile.approved().scope().into()))
    {
        return Err(Error::Forbidden);
    }
    let registry = profile.approved().registry();
    let request = w::SchemaBoundValue::decode(r.payload.as_slice()).map_err(|_| Error::Protocol)?;
    registry
        .validate_v1_1(
            "coding.input:session.snapshot",
            &request,
            limits(grant),
            json_limits(grant).instance_work,
        )
        .map_err(validation)?;
    let handler = shared
        .snapshot
        .lock()
        .map_err(|_| Error::HostValidation)?
        .clone()
        .ok_or(Error::Forbidden)?;
    let snapshot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler()))
        .map_err(|_| Error::HostValidation)??;
    bound(
        registry,
        "coding.output:session.snapshot",
        &serde_json::json!({"value":snapshot}),
    )
}
/// Construct a current bound value using the selected host registry.
pub fn bound(
    registry: &profiles::Registry,
    root: &str,
    value: &serde_json::Value,
) -> Result<w::SchemaBoundValue, Error> {
    Ok(w::SchemaBoundValue {
        projection_id: root.into(),
        schema_digest_sha256: registry.digest(root).ok_or(Error::Forbidden)?.to_vec(),
        value: Some(profiles::json_wire(value).map_err(validation)?),
    })
}

fn admit_rate(rate: &mut VecDeque<Instant>, max: u32) -> bool {
    let now = Instant::now();
    while rate
        .front()
        .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(1))
    {
        rate.pop_front();
    }
    if rate.len() >= max as usize {
        return false;
    }
    rate.push_back(now);
    true
}
