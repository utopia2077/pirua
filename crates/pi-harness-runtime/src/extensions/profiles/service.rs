//! Opt-in bounded sole writer for an actual native profile connection.
use super::{connection::ProfileConnection, tool_route::ToolInbox};
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
}
impl Writer {
    pub(crate) fn new(max_bytes: usize, max_jobs: usize) -> Result<Arc<Self>, DriverError> {
        if max_bytes == 0 || max_bytes == usize::MAX || max_jobs == 0 || max_jobs > 256 {
            return Err(DriverError::InvalidHost);
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
        }))
    }
    pub(crate) fn enqueue(
        &self,
        frame: EncodedFrame,
        id: u64,
        deadline: u64,
    ) -> Result<(), DriverError> {
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
            });
            state.wake.take()
        };
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
    fn publish_and_finish<F>(&self, id: u64, bytes: usize, completed: bool, publish: F) -> bool
    where
        F: FnOnce(&State),
    {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.bytes = state.bytes.saturating_sub(bytes);
        let active_matches = state
            .active
            .as_ref()
            .is_some_and(|active| active.id == id && active.admitted && active.cancel);

        // Keep the exact active Cancel visible while publication wakes the
        // caller. The close decision is made under the same Writer lock after
        // publication, so a clean EOF cannot retire or close it early.
        publish(&state);
        state.active.take();
        if completed && active_matches && state.peer_close_requested {
            state.closed = true;
            true
        } else {
            false
        }
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
            let Some(job) = job else {
                break;
            };
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
            let close_after_ack = self.publish_and_finish(job.id, bytes, completed, |_| {
                inbox.written(job.id, completed)
            });
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
                "active exact ID survives inbox.written publication"
            );
            <ToolInbox as crate::extensions::tool_driver::ToolProfileIngress>::written(
                &inbox, id, true,
            );
        });
        assert!(close_after_ack);
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
