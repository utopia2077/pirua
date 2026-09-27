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
struct State {
    jobs: VecDeque<Job>,
    bytes: usize,
    closed: bool,
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
            let admitted = {
                let mut admission = inbox.admit_write(job.id);
                let mut timer = clock.sleep_until(job.deadline);
                std::future::poll_fn(|cx| {
                    if timer.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(false);
                    }
                    admission.as_mut().poll(cx)
                })
                .await
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
            {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                state.bytes = state.bytes.saturating_sub(bytes);
            }
            inbox.written(job.id, completed);
            if !completed {
                if let Some(connection) = connection.upgrade() {
                    connection.close();
                }
                break;
            }
        }
    }
}
