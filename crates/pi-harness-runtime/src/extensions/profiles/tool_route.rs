//! Bounded demultiplexing behind the actual Tool route's sole reader/writer.
use super::connection::ProfileConnection;
#[cfg(test)]
use crate::extensions::transport::active_request_id;
#[cfg(test)]
use crate::extensions::transport::source_observer;
use crate::extensions::{
    tool_driver::{DriverError, ToolProfileIngress},
    transport::PumpError,
};
use pi_ai_core::CancellationToken;
use pi_extension_protocol::endpoint::ActiveFrame;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    task::{Poll, Waker},
};
struct PendingWrite {
    done: CancellationToken,
    result: Mutex<Option<bool>>,
    cancel: bool,
    response: bool,
    #[cfg(test)]
    bytes: usize,
    #[cfg(test)]
    generation: Option<u64>,
}
struct State {
    requests: VecDeque<ActiveFrame>,
    replies: VecDeque<ActiveFrame>,
    bytes: usize,
    closed: bool,
    request_wake: Option<Waker>,
    reply_wake: Option<Waker>,
    writes: BTreeMap<u64, Arc<PendingWrite>>,
    cancel_ack: Option<u64>,
}
pub(crate) struct ToolInbox {
    state: Mutex<State>,
    connection: Mutex<Weak<ProfileConnection>>,
    max_bytes: usize,
    max_frames: usize,
    #[cfg(test)]
    route_writer: Mutex<Option<Arc<super::service::Writer>>>,
}
impl ToolInbox {
    #[cfg(test)]
    fn observed_generation(&self) -> Option<u64> {
        self.connection
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .upgrade()
            .map(|connection| connection.grant().wire().connection_generation)
    }

    pub(crate) fn new(max_bytes: usize, max_frames: usize) -> Result<Arc<Self>, DriverError> {
        if max_bytes == 0 || max_bytes == usize::MAX || max_frames == 0 || max_frames > 256 {
            return Err(DriverError::InvalidHost);
        }
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("queue", file!(), line!(), "ToolInbox::new");
            event.field = Some("Qreceive");
            event.owner = Some("ToolInbox");
            event.capacity = Some(max_bytes);
            event.jobs = Some(max_frames);
            event.operation = Some("capacity");
            source_observer::record(event);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                requests: VecDeque::new(),
                replies: VecDeque::new(),
                bytes: 0,
                closed: false,
                request_wake: None,
                reply_wake: None,
                writes: BTreeMap::new(),
                cancel_ack: None,
            }),
            connection: Mutex::new(Weak::new()),
            max_bytes,
            max_frames,
            #[cfg(test)]
            route_writer: Mutex::new(None),
        }))
    }
    pub(crate) fn attach(&self, connection: &Arc<ProfileConnection>) {
        *self.connection.lock().unwrap_or_else(|p| p.into_inner()) = Arc::downgrade(connection);
    }
    pub(crate) fn is_cancel(&self, id: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .writes
            .get(&id)
            .is_some_and(|write| write.cancel)
    }
    pub(crate) fn take_cancel_ack(&self, id: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel_ack
            .take_if(|ack| *ack == id)
            .is_some()
    }
    pub(crate) async fn next(&self, requests: bool) -> Result<Option<ActiveFrame>, PumpError> {
        std::future::poll_fn(|cx| {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let frame = if requests {
                state.requests.pop_front()
            } else {
                state.replies.pop_front()
            };
            if let Some(frame) = frame {
                state.bytes -= frame.body_len;
                #[cfg(test)]
                {
                    let mut event = source_observer::SourceEvent::new(
                        "queue",
                        file!(),
                        line!(),
                        "ToolInbox::next",
                    );
                    event.field = Some("Qreceive");
                    event.owner = Some("ToolInbox");
                    event.generation = self.observed_generation();
                    event.operation = Some("pop");
                    event.bytes = Some(frame.body_len);
                    event.jobs = Some(state.requests.len() + state.replies.len());
                    event.request_id = active_request_id(&frame.envelope);
                    source_observer::record(event);
                }
                return Poll::Ready(Ok(Some(frame)));
            }
            if state.closed {
                return Poll::Ready(Ok(None));
            }
            let old = if requests {
                state.request_wake.replace(cx.waker().clone())
            } else {
                state.reply_wake.replace(cx.waker().clone())
            };
            drop(state);
            drop(old);
            Poll::Pending
        })
        .await
    }
    pub(crate) async fn write(
        &self,
        connection: &ProfileConnection,
        frame: pi_extension_protocol::wire::framing::EncodedFrame,
        deadline: u64,
    ) -> Result<(), pi_agent_schema::HarnessRuntimeError> {
        // The existing selected framing carries the request ID; parse one
        // bounded complete frame without interpreting its native payload.
        let (id, response, cancel) = connection.outgoing_identity(&frame)?;
        let pending = Arc::new(PendingWrite {
            done: CancellationToken::new(),
            result: Mutex::new(None),
            cancel,
            response,
            #[cfg(test)]
            bytes: frame.bytes.len(),
            #[cfg(test)]
            generation: self.observed_generation(),
        });
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed
                || state.writes.len() >= self.max_frames
                || state.writes.contains_key(&id)
            {
                return Err(super::fault());
            }
            state.writes.insert(id, pending.clone());
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "queue",
                    file!(),
                    line!(),
                    "ToolInbox::write",
                );
                event.field = Some("P_req");
                event.owner = Some("ToolInbox");
                event.operation = Some("reserve");
                event.request_id = Some(id);
                event.jobs = Some(state.writes.len());
                source_observer::record(event);
                let mut queue = source_observer::SourceEvent::new(
                    "queue",
                    file!(),
                    line!(),
                    "ToolInbox::write",
                );
                queue.field = Some("Qreceive");
                queue.owner = Some("ToolInbox");
                queue.generation = self.observed_generation();
                queue.operation = Some("write");
                queue.bytes = Some(frame.bytes.len());
                queue.jobs = Some(state.writes.len());
                queue.request_id = Some(id);
                source_observer::record(queue);
            }
        }
        #[cfg(test)]
        let enqueued = match self
            .route_writer
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            Some(writer) => writer.enqueue_on_route_for_test(
                frame,
                id,
                deadline,
                pending.generation.expect("attached route generation"),
            ),
            None => connection.enqueue_profile(frame, id, response, deadline),
        };
        #[cfg(not(test))]
        let enqueued = connection.enqueue_profile(frame, id, response, deadline);
        if enqueued.is_err() {
            self.written(id, false);
        }
        let mut done = Box::pin(pending.done.cancelled());
        let clock = connection.clock();
        let mut timer = Box::pin(clock.sleep_until(deadline));
        let completed = std::future::poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(false);
            }
            done.as_mut().poll(cx).map(|_| true)
        })
        .await;
        Self::classify_write(
            completed,
            *pending.result.lock().unwrap_or_else(|p| p.into_inner()),
        )
    }
    fn classify_write(
        completed: bool,
        result: Option<bool>,
    ) -> Result<(), pi_agent_schema::HarnessRuntimeError> {
        if completed && result == Some(true) {
            Ok(())
        } else {
            Err(super::fault())
        }
    }
}
impl ToolProfileIngress for ToolInbox {
    fn incoming(&self, frame: ActiveFrame) -> Result<(), DriverError> {
        #[cfg(test)]
        if self
            .route_writer
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            self.observe_indexed_terminal_for_test(&frame)?;
        }
        let requests = super::connection::peer_request(&frame.envelope).is_some()
            || self
                .connection
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .upgrade()
                .is_some_and(|connection| connection.passive_control(&frame.envelope));
        let wake = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let bytes = state
                .bytes
                .checked_add(frame.body_len)
                .ok_or(DriverError::Overloaded)?;
            if state.closed
                || bytes > self.max_bytes
                || state.requests.len() + state.replies.len() >= self.max_frames
            {
                return Err(DriverError::Overloaded);
            }
            state.bytes = bytes;
            #[cfg(test)]
            {
                let mut event = source_observer::SourceEvent::new(
                    "queue",
                    file!(),
                    line!(),
                    "ToolInbox::incoming",
                );
                event.field = Some("Qreceive");
                event.owner = Some("ToolInbox");
                event.generation = self.observed_generation();
                event.operation = Some("enqueue");
                event.bytes = Some(state.bytes);
                event.jobs = Some(state.requests.len() + state.replies.len() + 1);
                event.request_id = active_request_id(&frame.envelope);
                source_observer::record(event);
            }
            if requests {
                state.requests.push_back(frame);
                state.request_wake.take()
            } else {
                state.replies.push_back(frame);
                state.reply_wake.take()
            }
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    fn admit_write(&self, id: u64) -> pi_ai_core::BoxFuture<'_, bool> {
        Box::pin(async move {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .upgrade();
            let Some(connection) = connection else {
                return false;
            };
            let (response, cancel) = {
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let Some(write) = state.writes.get(&id) else {
                    return false;
                };
                (write.response, write.cancel)
            };
            connection.admit_tool_write(id, response, cancel).await
        })
    }
    fn written(&self, id: u64, completed: bool) {
        let pending = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let pending = state.writes.remove(&id);
            if completed && pending.as_ref().is_some_and(|pending| pending.cancel) {
                state.cancel_ack = Some(id);
            }
            pending
        };
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("queue", file!(), line!(), "ToolInbox::written");
            event.field = Some("P_req");
            event.owner = Some("ToolInbox");
            event.generation = self.observed_generation();
            event.operation = Some(if completed { "ack" } else { "release" });
            event.request_id = Some(id);
            event.outcome = Some(if completed { "complete" } else { "failed" });
            source_observer::record(event);
            let mut queue =
                source_observer::SourceEvent::new("queue", file!(), line!(), "ToolInbox::written");
            queue.field = Some("Qreceive");
            queue.owner = Some("ToolInbox");
            queue.generation = self.observed_generation();
            queue.operation = Some(if completed { "ack" } else { "release" });
            queue.outcome = Some(if completed { "complete" } else { "failed" });
            queue.request_id = Some(id);
            queue.bytes = pending.as_ref().map(|pending| pending.bytes);
            source_observer::record(queue);
            if let Some(pending) = &pending {
                let mut release = source_observer::SourceEvent::new(
                    "queue",
                    file!(),
                    line!(),
                    "ToolInbox::written",
                );
                release.field = Some("Qreceive");
                release.owner = Some("ToolInbox");
                release.generation = pending.generation;
                release.operation = Some("release");
                release.bytes = Some(pending.bytes);
                release.request_id = Some(id);
                source_observer::record(release);
            }
        }
        if let Some(pending) = pending {
            *pending.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(completed);
            pending.done.cancel();
        }
    }
    fn close(&self) {
        let (wake, reply, writes) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed {
                return;
            }
            state.closed = true;
            state.bytes = 0;
            state.requests.clear();
            state.replies.clear();
            (
                state.request_wake.take(),
                state.reply_wake.take(),
                std::mem::take(&mut state.writes),
            )
        };
        for pending in writes.into_values() {
            *pending.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(false);
            pending.done.cancel();
        }
        if let Some(wake) = wake {
            wake.wake();
        }
        if let Some(wake) = reply {
            wake.wake();
        }
    }
}

#[cfg(test)]
impl ToolInbox {
    fn observe_indexed_terminal_for_test(&self, frame: &ActiveFrame) -> Result<(), DriverError> {
        use pi_extension_protocol::endpoint::SelectedEnvelope;
        use pi_extension_protocol::wire::{
            echo_bounded::{IndexArena, SelectedMinor},
            echo_selected::{ActiveArm, select_active},
            v1_0, v1_1,
        };
        let (request_id, payload) = match &frame.envelope {
            SelectedEnvelope::V1_0(envelope) => match envelope.frame.as_ref() {
                Some(v1_0::envelope::Frame::StreamEnd(terminal)) => {
                    match terminal.terminal.as_ref() {
                        Some(v1_0::stream_end::Terminal::Result(payload)) => {
                            (terminal.request_id, payload)
                        }
                        _ => return Err(DriverError::MalformedPeer),
                    }
                }
                _ => return Ok(()),
            },
            SelectedEnvelope::V1_1(envelope) => match envelope.frame.as_ref() {
                Some(v1_1::envelope::Frame::StreamEnd(terminal)) => {
                    match terminal.terminal.as_ref() {
                        Some(v1_1::stream_end::Terminal::Result(payload)) => {
                            (terminal.request_id, payload)
                        }
                        _ => return Err(DriverError::MalformedPeer),
                    }
                }
                _ => return Ok(()),
            },
        };
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .upgrade()
            .ok_or(DriverError::WrongBinding)?;
        let generation = connection.grant().wire().connection_generation;
        let body = source_observer::take_active_body(request_id, generation, frame.body_len)
            .ok_or(DriverError::HostFault)?;
        let limit = std::num::NonZeroUsize::new(body.len()).ok_or(DriverError::HostFault)?;
        let mut arena = IndexArena::try_new(limit).map_err(|_| DriverError::HostFault)?;
        let view = arena
            .scan_envelope(&body)
            .map_err(|_| DriverError::MalformedPeer)?;
        let minor = if connection
            .grant()
            .wire()
            .selected_protocol
            .as_ref()
            .is_some_and(|version| version.minor == 0)
        {
            SelectedMinor::V1_0
        } else {
            SelectedMinor::V1_1
        };
        let selected = select_active(view, minor, limit).map_err(|_| DriverError::MalformedPeer)?;
        assert_eq!(selected.arm(), ActiveArm::StreamEnd);
        assert_eq!(selected.request_id().unwrap(), request_id);
        assert!(selected.terminal_present().unwrap());
        assert_eq!(
            selected.opaque_payload_span().unwrap().reborrow().unwrap(),
            payload
        );
        for operation in ["indexed_selection", "indexed_terminal"] {
            let mut event = source_observer::SourceEvent::new(
                "body_capture",
                file!(),
                line!(),
                "ToolInbox::observe_indexed_terminal_for_test",
            );
            event.field = Some("D_req");
            event.owner = Some("ToolInbox");
            event.operation = Some(operation);
            event.outcome = Some("complete");
            event.bytes = Some(selected.body_len());
            event.body_len = Some(selected.body_len());
            event.request_id = Some(selected.request_id().unwrap());
            event.generation = Some(generation);
            // StreamEnd has no sequence field; this observation does not
            // advance the production owner's zero-credit stream state.
            event.wire_sequence_before = Some(0);
            event.wire_sequence_after = Some(0);
            source_observer::record(event);
        }
        Ok(())
    }

    pub(crate) fn bridge_writer_for_test(
        &self,
        connection: &ProfileConnection,
        route: &crate::extensions::tool_driver::ReadyToolRoute,
    ) -> Arc<super::service::Writer> {
        assert!(connection.same_tool_connection(route));
        assert_eq!(self.observed_generation(), Some(route.generation()));
        let attached = self
            .connection
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .upgrade()
            .expect("attached profile connection");
        assert!(std::ptr::eq(attached.as_ref(), connection));
        let writer = super::service::Writer::new(self.max_bytes, self.max_frames).unwrap();
        let mut bridge = self.route_writer.lock().unwrap_or_else(|p| p.into_inner());
        assert!(bridge.is_none(), "one test-only bridge per ordinary inbox");
        *bridge = Some(writer.clone());
        writer
    }

    pub(crate) fn route_pending_for_test(
        &self,
        id: u64,
        bytes: usize,
        generation: u64,
    ) -> (bool, bool, pi_ai_core::BoxFuture<'static, bool>) {
        let pending = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .writes
            .get(&id)
            .cloned()
            .expect("source-owned pending route write");
        assert_eq!(pending.bytes, bytes);
        assert_eq!(pending.generation, Some(generation));
        let response = pending.response;
        let cancel = pending.cancel;
        let completion = Box::pin(async move {
            pending.done.cancelled().await;
            *pending.result.lock().unwrap_or_else(|p| p.into_inner()) == Some(true)
        });
        (response, cancel, completion)
    }

    pub(crate) fn insert_test_pending(&self, id: u64, cancel: bool) {
        let pending = Arc::new(PendingWrite {
            done: CancellationToken::new(),
            result: Mutex::new(None),
            cancel,
            response: false,
            bytes: 0,
            generation: None,
        });
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .writes
            .insert(id, pending);
    }

    pub(crate) fn has_test_pending(&self, id: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .writes
            .contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::ToolInbox;
    use pi_agent_schema::HarnessRuntimeError;

    #[test]
    fn service_write_failure_and_timeout_stay_generic_faults() {
        for (label, completed, result) in [
            ("write failure", true, Some(false)),
            ("write timeout", false, None),
        ] {
            assert!(
                matches!(
                    ToolInbox::classify_write(completed, result),
                    Err(HarnessRuntimeError::Fault(_))
                ),
                "{label}"
            );
        }
    }
}
