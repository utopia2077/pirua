//! Opt-in bounded sole writer for an actual native profile connection.
use super::{connection::ProfileConnection, tool_route::ToolInbox};
#[cfg(test)]
use crate::extensions::transport::source_observer;
use crate::extensions::{
    tool_driver::{DriverError, ToolProfileIngress},
    transport::{MonotonicClock, OrderedByteChannel},
};
use pi_extension_protocol::wire::framing::EncodedFrame;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};
struct Job {
    frame: EncodedFrame,
    id: u64,
    deadline: u64,
    #[cfg(test)]
    generation: Option<u64>,
}
struct ActiveJob {
    id: u64,
    cancel: bool,
    admitted: bool,
}
struct State {
    jobs: VecDeque<Job>,
    bytes: usize,
    closed: bool,
    peer_close_requested: bool,
    active: Option<ActiveJob>,
    wake: Option<Waker>,
}
pub(crate) struct Writer {
    state: Mutex<State>,
    max_bytes: usize,
    max_jobs: usize,
    #[cfg(test)]
    token: Mutex<Option<crate::extensions::transport::source_observer::ConnectionToken>>,
}
impl Writer {
    pub(crate) fn new(max_bytes: usize, max_jobs: usize) -> Result<Arc<Self>, DriverError> {
        if max_bytes == 0 || max_bytes == usize::MAX || max_jobs == 0 || max_jobs > 256 {
            return Err(DriverError::InvalidHost);
        }
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::new");
            event.field = Some("Qreceive");
            event.owner = Some("Writer");
            event.capacity = Some(max_bytes);
            event.jobs = Some(max_jobs);
            event.operation = Some("capacity");
            source_observer::record(event);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                jobs: VecDeque::new(),
                bytes: 0,
                closed: false,
                peer_close_requested: false,
                active: None,
                wake: None,
            }),
            max_bytes,
            max_jobs,
            #[cfg(test)]
            token: Mutex::new(None),
        }))
    }
    pub(crate) fn enqueue(
        &self,
        frame: EncodedFrame,
        id: u64,
        deadline: u64,
    ) -> Result<(), DriverError> {
        #[cfg(test)]
        let enqueue_facts;
        let wake = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let bytes = state
                .bytes
                .checked_add(frame.bytes.len())
                .ok_or(DriverError::Overloaded)?;
            if state.closed || bytes > self.max_bytes || state.jobs.len() >= self.max_jobs {
                return Err(DriverError::Overloaded);
            }
            state.bytes = bytes;
            state.jobs.push_back(Job {
                frame,
                id,
                deadline,
                #[cfg(test)]
                generation: None,
            });
            #[cfg(test)]
            {
                enqueue_facts = (state.bytes, state.jobs.len());
            }
            state.wake.take()
        };
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::enqueue");
            event.field = Some("Qreceive");
            event.owner = Some("Writer");
            event.operation = Some("enqueue");
            event.bytes = Some(enqueue_facts.0);
            event.jobs = Some(enqueue_facts.1);
            event.request_id = Some(id);
            source_observer::record(event);
        }
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    pub(crate) fn close(&self) {
        let wake = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.closed = true;
            state.jobs.clear();
            state.bytes = 0;
            state.wake.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    pub(crate) fn request_peer_close(&self, inbox: &ToolInbox) -> Option<u64> {
        let (wake, jobs, defer) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed {
                return None;
            }
            state.peer_close_requested = true;
            let defer = state
                .active
                .as_ref()
                .filter(|active| active.admitted && active.cancel)
                .map(|active| active.id);
            let jobs = std::mem::take(&mut state.jobs);
            state.bytes = 0;
            (state.wake.take(), jobs, defer)
        };
        for job in jobs {
            inbox.written(job.id, false);
        }
        if let Some(wake) = wake {
            wake.wake();
        }
        defer
    }
    #[cfg(test)]
    fn is_active(&self, id: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active
            .as_ref()
            .is_some_and(|active| active.id == id)
    }
    fn publish_and_finish<F>(
        &self,
        id: u64,
        bytes: usize,
        completed: bool,
        state_bookkeeping: F,
    ) -> bool
    where
        F: FnOnce(&State),
    {
        #[cfg(test)]
        let observation;
        let close_after_ack = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.bytes = state.bytes.saturating_sub(bytes);
            let active_matches = state
                .active
                .as_ref()
                .is_some_and(|active| active.id == id && active.admitted && active.cancel);

            // Keep the exact active Cancel visible while publication wakes the
            // caller. The close decision is made under the same Writer lock after
            // publication, so a clean EOF cannot retire or close it early.
            state_bookkeeping(&state);
            state.active.take();
            #[cfg(test)]
            {
                observation = (if completed { "ack" } else { "release" }, completed);
            }
            if completed && active_matches && state.peer_close_requested {
                state.closed = true;
                true
            } else {
                false
            }
        };
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::run");
            event.field = Some("Qreceive");
            event.owner = Some("Writer");
            event.operation = Some(observation.0);
            event.outcome = Some(if observation.1 { "complete" } else { "failed" });
            event.bytes = Some(bytes);
            event.request_id = Some(id);
            source_observer::record(event);
        }
        close_after_ack
    }
    pub(crate) async fn run(
        self: Arc<Self>,
        connection: std::sync::Weak<ProfileConnection>,
        inbox: Arc<ToolInbox>,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
    ) {
        loop {
            let job = std::future::poll_fn(|cx| {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if state.peer_close_requested {
                    return Poll::Ready(None);
                }
                if let Some(job) = state.jobs.pop_front() {
                    #[cfg(test)]
                    let pop_facts = (job.frame.bytes.len(), state.jobs.len(), job.id);
                    #[cfg(test)]
                    return Poll::Ready(Some((job, pop_facts)));
                    #[cfg(not(test))]
                    return Poll::Ready(Some(job));
                }
                if state.closed {
                    return Poll::Ready(None);
                }
                let old = state.wake.replace(cx.waker().clone());
                drop(state);
                drop(old);
                Poll::Pending
            })
            .await;
            #[cfg(test)]
            let Some((job, (pop_bytes, pop_jobs, pop_request_id))) = job else {
                break;
            };
            #[cfg(not(test))]
            let Some(job) = job else {
                break;
            };
            #[cfg(test)]
            {
                let mut event =
                    source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::run");
                event.field = Some("Qreceive");
                event.owner = Some("Writer");
                event.operation = Some("pop");
                event.bytes = Some(pop_bytes);
                event.jobs = Some(pop_jobs);
                event.request_id = Some(pop_request_id);
                source_observer::record(event);
                let mut queue =
                    source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::run");
                queue.field = Some("Qreceive");
                queue.owner = Some("Writer");
                queue.operation = Some("write");
                queue.outcome = Some("start");
                queue.bytes = Some(pop_bytes);
                queue.request_id = Some(pop_request_id);
                source_observer::record(queue);
            }
            let bytes = job.frame.bytes.len();
            let cancel = inbox.is_cancel(job.id);
            let skip = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                state.active = Some(ActiveJob {
                    id: job.id,
                    cancel,
                    admitted: false,
                });
                state.closed || state.peer_close_requested
            };
            let admitted = {
                if skip {
                    false
                } else {
                    let mut admission = inbox.admit_write(job.id);
                    let mut timer = clock.sleep_until(job.deadline);
                    let admitted = std::future::poll_fn(|cx| {
                        if timer.as_mut().poll(cx).is_ready() {
                            return Poll::Ready(false);
                        }
                        admission.as_mut().poll(cx)
                    })
                    .await;
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    if admitted && !state.closed && !state.peer_close_requested {
                        state
                            .active
                            .as_mut()
                            .expect("active service write")
                            .admitted = true;
                        true
                    } else {
                        false
                    }
                }
            };
            let completed = if admitted {
                #[cfg(test)]
                {
                    let mut event = source_observer::SourceEvent::new(
                        "interval",
                        file!(),
                        line!(),
                        "Writer::run",
                    );
                    event.field = Some("overlap");
                    event.owner = Some("Writer");
                    event.operation = Some("physical_write");
                    event.outcome = Some("start");
                    event.request_id = Some(job.id);
                    source_observer::record(event);
                    let mut queue =
                        source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::run");
                    queue.field = Some("Qreceive");
                    queue.owner = Some("Writer");
                    queue.operation = Some("write");
                    queue.outcome = Some("start");
                    queue.bytes = Some(bytes);
                    queue.request_id = Some(job.id);
                    source_observer::record(queue);
                }
                let mut write = channel.write_frame(job.frame);
                let mut timer = clock.sleep_until(job.deadline);
                std::future::poll_fn(|cx| {
                    if timer.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(false);
                    }
                    write.as_mut().poll(cx).map(|result| result.is_ok())
                })
                .await
                    && clock.now_ms() < job.deadline
            } else {
                false
            };
            #[cfg(test)]
            if admitted {
                let mut event =
                    source_observer::SourceEvent::new("interval", file!(), line!(), "Writer::run");
                event.field = Some("overlap");
                event.owner = Some("Writer");
                event.operation = Some("physical_write");
                event.outcome = Some("end");
                event.request_id = Some(job.id);
                event.bytes = Some(bytes);
                source_observer::record(event);
                let mut queue =
                    source_observer::SourceEvent::new("queue", file!(), line!(), "Writer::run");
                queue.field = Some("Qreceive");
                queue.owner = Some("Writer");
                queue.operation = Some("write");
                queue.outcome = Some("end");
                queue.bytes = Some(bytes);
                queue.request_id = Some(job.id);
                source_observer::record(queue);
            }
            // ToolInbox publication captures the Owner-derived token and records
            // queue facts, so it must happen after the Writer state guard above
            // has been released.
            let close_after_ack = self.publish_and_finish(job.id, bytes, completed, |_| {});
            inbox.written(job.id, completed);
            if close_after_ack {
                if let Some(connection) = connection.upgrade() {
                    connection.close();
                }
                break;
            }
            if !completed {
                if let Some(connection) = connection.upgrade() {
                    connection.close();
                }
                break;
            }
        }
    }

    /// Keep Writer's bounded queue, but delegate all admission and physical
    /// completion to the ordinary Tool route. This worker owns no close path.
    #[cfg(test)]
    pub(crate) async fn run_on_route_for_test(
        self: Arc<Self>,
        connection: std::sync::Weak<ProfileConnection>,
        inbox: Arc<ToolInbox>,
        route: crate::extensions::tool_driver::ReadyToolRoute,
    ) {
        let attached = connection.upgrade().expect("live bridge connection");
        assert!(attached.same_tool_connection(&route));
        assert!(Arc::ptr_eq(&attached.tool_inbox_for_test(), &inbox));
        #[cfg(test)]
        {
            *self.token.lock().unwrap_or_else(|p| p.into_inner()) = Some(
                crate::extensions::transport::source_observer::ConnectionToken::from_identity(
                    &attached.connection_identity(),
                ),
            );
        }
        let route_token = self.token.lock().unwrap_or_else(|p| p.into_inner()).clone();
        drop(attached);
        loop {
            let job = std::future::poll_fn(|cx| {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(job) = state.jobs.pop_front() {
                    return Poll::Ready(Some((job, state.jobs.len())));
                }
                if state.closed {
                    return Poll::Ready(None);
                }
                state.wake = Some(cx.waker().clone());
                Poll::Pending
            })
            .await;
            let Some((job, pop_jobs)) = job else { break };
            self.route_event_for_test(
                route_token.clone(),
                "pop",
                job.id,
                job.generation,
                job.frame.bytes.len(),
                pop_jobs,
            );
            let attached = connection.upgrade().expect("live queued bridge connection");
            assert!(attached.same_tool_connection(&route));
            assert_eq!(job.generation, Some(route.generation()));
            let (id, response, cancel) = attached.outgoing_identity(&job.frame).unwrap();
            assert_eq!(id, job.id);
            let bytes = job.frame.bytes.len();
            let (pending_response, pending_cancel, completion) =
                inbox.route_pending_for_test(id, bytes, route.generation());
            assert_eq!((response, cancel), (pending_response, pending_cancel));
            self.state.lock().unwrap_or_else(|p| p.into_inner()).active = Some(ActiveJob {
                id,
                cancel,
                admitted: false,
            });
            self.route_event_for_test(
                route_token.clone(),
                "write",
                job.id,
                job.generation,
                bytes,
                1,
            );
            let generation = job.generation;
            let deadline = job.deadline;
            let completed = if route.enqueue_profile(job.frame, id, response).is_ok() {
                let mut completion = completion;
                let clock = attached.clock();
                let mut timer = clock.sleep_until(deadline);
                std::future::poll_fn(|cx| {
                    if timer.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(false);
                    }
                    completion.as_mut().poll(cx)
                })
                .await
            } else {
                false
            };
            self.publish_and_finish(id, bytes, completed, |_| {});
            // The pending completion was published only by Route's written
            // callback; this records Writer's own retired queue bytes.
            if completed {
                self.route_event_for_test(route_token.clone(), "ack", id, generation, bytes, 0);
            }
            self.route_event_for_test(route_token.clone(), "release", id, generation, bytes, 0);
        }
    }

    #[cfg(test)]
    pub(crate) fn enqueue_on_route_for_test(
        &self,
        frame: EncodedFrame,
        id: u64,
        deadline: u64,
        generation: u64,
    ) -> Result<(), DriverError> {
        assert!(generation > 0);
        let route_token = self.token.lock().unwrap_or_else(|p| p.into_inner()).clone();
        let (wake, event_facts) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let bytes = state
                .bytes
                .checked_add(frame.bytes.len())
                .ok_or(DriverError::Overloaded)?;
            if state.closed || bytes > self.max_bytes || state.jobs.len() >= self.max_jobs {
                return Err(DriverError::Overloaded);
            }
            state.bytes = bytes;
            let job = Job {
                frame,
                id,
                deadline,
                generation: Some(generation),
            };
            let event_facts = (job.id, job.generation, state.bytes, state.jobs.len() + 1);
            state.jobs.push_back(job);
            (state.wake.take(), event_facts)
        };
        self.route_event_for_test(
            route_token,
            "enqueue",
            event_facts.0,
            event_facts.1,
            event_facts.2,
            event_facts.3,
        );
        if let Some(wake) = wake {
            wake.wake()
        }
        Ok(())
    }

    #[cfg(test)]
    fn route_event_for_test(
        &self,
        token: Option<crate::extensions::transport::source_observer::ConnectionToken>,
        operation: &'static str,
        id: u64,
        generation: Option<u64>,
        bytes: usize,
        jobs: usize,
    ) {
        let mut event = source_observer::SourceEvent::new(
            "queue",
            file!(),
            line!(),
            "Writer::run_on_route_for_test",
        );
        event.field = Some("Qreceive");
        event.owner = Some("Writer");
        event.generation = generation;
        event.request_id = Some(id);
        event.operation = Some(operation);
        event.bytes = Some(bytes);
        event.jobs = Some(jobs);
        event.token = token;
        source_observer::record(event);
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveJob, Writer};
    use crate::extensions::profiles::tool_route::ToolInbox;

    #[test]
    fn admitted_cancel_stays_active_until_positive_ack_publication() {
        let writer = Writer::new(4096, 4).unwrap();
        let inbox = ToolInbox::new(4096, 4).unwrap();
        let id = 17;
        inbox.insert_test_pending(id, true);
        writer
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active = Some(ActiveJob {
            id,
            cancel: true,
            admitted: true,
        });

        assert_eq!(writer.request_peer_close(&inbox), Some(id));
        assert!(
            writer.is_active(id),
            "held physical write remains protected"
        );
        assert!(inbox.has_test_pending(id), "ack has not published yet");

        let close_after_ack = writer.publish_and_finish(id, 0, true, |state| {
            assert!(
                state.active.as_ref().is_some_and(|active| active.id == id),
                "active exact ID survives lock-local publication bookkeeping"
            );
        });
        assert!(close_after_ack);
        <ToolInbox as crate::extensions::tool_driver::ToolProfileIngress>::written(
            &inbox, id, true,
        );
        assert!(
            !inbox.has_test_pending(id),
            "positive acknowledgement removed the exact pending write"
        );
        assert!(
            inbox.take_cancel_ack(id),
            "positive acknowledgement is retained by exact request ID"
        );
        assert!(
            !inbox.take_cancel_ack(id),
            "exact acknowledgement retires atomically after consumption"
        );
        assert!(
            !writer.is_active(id),
            "active record retires after publication"
        );
    }
}
