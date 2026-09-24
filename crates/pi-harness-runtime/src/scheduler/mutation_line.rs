//! FIFO mutation ownership and coordinated settings-to-lane acquisition.

use super::admission::{Admission, Callbacks, State as AdmissionState};
use pi_agent_schema::HarnessRuntimeError;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// One process-local mutation order, sharing the owner's global admission seal.
#[derive(Clone)]
pub struct MutationLine {
    admission: Admission,
    state: Arc<Mutex<LineState>>,
}

#[derive(Default)]
pub(crate) struct LineState {
    held: bool,
    queue: VecDeque<u64>,
}

pub(crate) struct Ticket {
    kind: TicketKind,
    waker: Option<Waker>,
    // A release can take the Waker before its callback runs. The next poll
    // transfers that claimed notification to its latest Waker if still queued.
    claimed: bool,
    lane_queued: bool,
}

enum TicketKind {
    Single(Arc<Mutex<LineState>>),
    Pair {
        settings: Arc<Mutex<LineState>>,
        lane: Arc<Mutex<LineState>>,
    },
}

impl MutationLine {
    /// Creates an independent line under the given admission owner.
    pub fn new(admission: Admission) -> Self {
        Self {
            admission,
            state: Arc::new(Mutex::new(LineState::default())),
        }
    }

    /// Construction reserves no position. First queued poll establishes FIFO order.
    pub fn acquire(&self) -> Acquire {
        Acquire {
            line: self.clone(),
            id: None,
            done: false,
        }
    }

    /// Reserves settings before lane, then grants both lines atomically.
    /// Both lines must belong to the same admission owner.
    pub fn acquire_pair(&self, lane: &Self) -> PairAcquire {
        assert!(Arc::ptr_eq(&self.admission.inner, &lane.admission.inner));
        assert!(!Arc::ptr_eq(&self.state, &lane.state));
        PairAcquire {
            settings: self.clone(),
            lane: lane.clone(),
            id: None,
            done: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn held_for_test(&self) -> bool {
        lock_line(&self.state).held
    }
}

fn lock_line(line: &Arc<Mutex<LineState>>) -> std::sync::MutexGuard<'_, LineState> {
    line.lock().unwrap_or_else(|p| p.into_inner())
}

/// Claims an eligible head's notification. Settings promotion may also put a
/// pair into the lane queue. Every caller already owns Admission, and this
/// function acquires settings before lane when it needs both locks.
fn notify_head(state: &mut AdmissionState, line: &Arc<Mutex<LineState>>, out: &mut Callbacks) {
    let line_state = lock_line(line);
    if line_state.held {
        return;
    }
    let Some(&id) = line_state.queue.front() else {
        return;
    };
    let pair_lane = match &state
        .tickets
        .get(&id)
        .expect("queued ticket registered")
        .kind
    {
        TicketKind::Pair { settings, lane } if Arc::ptr_eq(settings, line) => Some(lane.clone()),
        _ => None,
    };
    if let Some(lane) = pair_lane {
        let mut lane_state = lock_line(&lane);
        let ticket = state.tickets.get_mut(&id).expect("queued pair registered");
        if !ticket.lane_queued {
            lane_state.queue.push_back(id);
            ticket.lane_queued = true;
        }
        if lane_state.held || lane_state.queue.front() != Some(&id) {
            return;
        }
    }
    let ticket = state
        .tickets
        .get_mut(&id)
        .expect("queued ticket registered");
    if let Some(wake) = ticket.waker.take() {
        ticket.claimed = true;
        out.wakes.push(wake);
    }
}

/// Remove exactly one queued ticket. Admission serializes this transition
/// against grant and seal. Callback ownership leaves every mutex first.
pub(crate) fn retire_ticket(
    state: &mut AdmissionState,
    id: u64,
    out: &mut Callbacks,
    sealed: bool,
) {
    let Some(mut ticket) = state.tickets.remove(&id) else {
        return;
    };
    match &ticket.kind {
        TicketKind::Single(line) => {
            {
                let mut line_state = lock_line(line);
                if let Some(position) = line_state.queue.iter().position(|item| *item == id) {
                    line_state.queue.remove(position);
                }
            }
            if !sealed {
                notify_head(state, line, out);
            }
        }
        TicketKind::Pair { settings, lane } => {
            {
                let mut settings_state = lock_line(settings);
                let mut lane_state = lock_line(lane);
                if let Some(position) = settings_state.queue.iter().position(|item| *item == id) {
                    settings_state.queue.remove(position);
                }
                if let Some(position) = lane_state.queue.iter().position(|item| *item == id) {
                    lane_state.queue.remove(position);
                }
            }
            if !sealed {
                notify_head(state, settings, out);
                notify_head(state, lane, out);
            }
        }
    }
    if let Some(wake) = ticket.waker.take() {
        if sealed {
            out.wakes.push(wake);
        } else {
            out.drops.push(wake);
        }
    }
    out.tickets.push(ticket);
}

/// Cancellation-safe single-line acquisition.
pub struct Acquire {
    line: MutationLine,
    id: Option<u64>,
    done: bool,
}

impl Future for Acquire {
    type Output = Result<MutationPermit, HarnessRuntimeError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "acquisition polled after completion");
        let mut candidate = Some(cx.waker().clone());
        // Prepare permit ownership before setting the held bit.
        let mut permit = MutationPermit::unheld(self.line.clone());
        let mut callbacks = Callbacks::new();
        let mut new_id = None;
        let result;
        {
            let mut state = self
                .line
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Err(error) = state.check() {
                if let Some(id) = self.id {
                    retire_ticket(&mut state, id, &mut callbacks, false);
                }
                result = Poll::Ready(Err(error));
            } else {
                let id = if let Some(id) = self.id {
                    id
                } else {
                    let id = state.ticket_id();
                    state.tickets.insert(
                        id,
                        Ticket {
                            kind: TicketKind::Single(self.line.state.clone()),
                            waker: None,
                            claimed: false,
                            lane_queued: false,
                        },
                    );
                    lock_line(&self.line.state).queue.push_back(id);
                    new_id = Some(id);
                    id
                };
                let ticket = state.tickets.get_mut(&id).expect("live ticket registered");
                let old = ticket.waker.replace(candidate.take().expect("candidate"));
                let mut line_state = lock_line(&self.line.state);
                if !line_state.held && line_state.queue.front() == Some(&id) {
                    line_state.queue.pop_front();
                    let mut ticket = state.tickets.remove(&id).expect("grant ticket registered");
                    permit.deferred[0] = ticket.waker.take();
                    permit.deferred[1] = old;
                    permit.released = false;
                    line_state.held = true;
                    drop(line_state);
                    // No callback-capable value is destroyed while held.
                    drop(ticket);
                    result = Poll::Ready(Ok(permit));
                } else {
                    drop(line_state);
                    if let Some(old) = old {
                        callbacks.drops.push(old);
                    }
                    let ticket = state
                        .tickets
                        .get_mut(&id)
                        .expect("pending ticket registered");
                    if ticket.claimed {
                        ticket.claimed = false;
                        if let Some(wake) = ticket.waker.take() {
                            callbacks.wakes.push(wake);
                        }
                    }
                    result = Poll::Pending;
                }
            }
        }
        if let Some(id) = new_id {
            self.id = Some(id);
        }
        if result.is_ready() {
            self.done = true;
        }
        drop(candidate);
        callbacks.dispatch();
        result
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let Some(id) = self.id else { return };
        let mut callbacks = Callbacks::new();
        {
            let mut state = self
                .line
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            retire_ticket(&mut state, id, &mut callbacks, false);
        }
        callbacks.dispatch();
    }
}

/// A single ticket that reserves settings without holding either line until
/// it can own both. First poll, not construction, establishes FIFO position.
pub struct PairAcquire {
    settings: MutationLine,
    lane: MutationLine,
    id: Option<u64>,
    done: bool,
}

impl Future for PairAcquire {
    type Output = Result<PairPermit, HarnessRuntimeError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "pair acquisition polled after completion");
        let mut candidate = Some(cx.waker().clone());
        let mut permit = PairPermit::unheld(self.settings.clone(), self.lane.clone());
        let mut callbacks = Callbacks::new();
        let mut new_id = None;
        let result;
        {
            let mut state = self
                .settings
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Err(error) = state.check() {
                if let Some(id) = self.id {
                    retire_ticket(&mut state, id, &mut callbacks, false);
                }
                result = Poll::Ready(Err(error));
            } else {
                let id = if let Some(id) = self.id {
                    id
                } else {
                    let id = state.ticket_id();
                    state.tickets.insert(
                        id,
                        Ticket {
                            kind: TicketKind::Pair {
                                settings: self.settings.state.clone(),
                                lane: self.lane.state.clone(),
                            },
                            waker: None,
                            claimed: false,
                            lane_queued: false,
                        },
                    );
                    lock_line(&self.settings.state).queue.push_back(id);
                    new_id = Some(id);
                    id
                };
                let ticket = state.tickets.get_mut(&id).expect("live pair registered");
                let old = ticket.waker.replace(candidate.take().expect("candidate"));
                let mut settings = lock_line(&self.settings.state);
                let settings_head = !settings.held && settings.queue.front() == Some(&id);
                let mut granted = false;
                if settings_head {
                    let mut lane = lock_line(&self.lane.state);
                    let ticket = state.tickets.get_mut(&id).expect("live pair registered");
                    if !ticket.lane_queued {
                        lane.queue.push_back(id);
                        ticket.lane_queued = true;
                    }
                    if !lane.held && lane.queue.front() == Some(&id) {
                        settings.queue.pop_front();
                        lane.queue.pop_front();
                        let mut ticket = state.tickets.remove(&id).expect("grant pair registered");
                        permit.deferred[0] = ticket.waker.take();
                        permit.deferred[1] = old;
                        settings.held = true;
                        lane.held = true;
                        permit.released = false;
                        granted = true;
                        drop(ticket);
                    } else if let Some(old) = old {
                        callbacks.drops.push(old);
                    }
                } else if let Some(old) = old {
                    callbacks.drops.push(old);
                }
                drop(settings);
                if granted {
                    result = Poll::Ready(Ok(permit));
                } else {
                    let ticket = state.tickets.get_mut(&id).expect("pending pair registered");
                    if ticket.claimed {
                        ticket.claimed = false;
                        if let Some(wake) = ticket.waker.take() {
                            callbacks.wakes.push(wake);
                        }
                    }
                    result = Poll::Pending;
                }
            }
        }
        if let Some(id) = new_id {
            self.id = Some(id);
        }
        if result.is_ready() {
            self.done = true;
        }
        drop(candidate);
        callbacks.dispatch();
        result
    }
}

impl Drop for PairAcquire {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let Some(id) = self.id else { return };
        let mut callbacks = Callbacks::new();
        {
            let mut state = self
                .settings
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            retire_ticket(&mut state, id, &mut callbacks, false);
        }
        callbacks.dispatch();
    }
}

/// Exclusive line ownership. This is deliberately not commit admission.
#[must_use = "dropping the permit releases the mutation line"]
pub struct MutationPermit {
    line: MutationLine,
    released: bool,
    deferred: [Option<Waker>; 2],
}

impl MutationPermit {
    fn unheld(line: MutationLine) -> Self {
        Self {
            line,
            released: true,
            deferred: [None, None],
        }
    }

    fn release_held(&mut self) -> Callbacks {
        debug_assert!(!self.released);
        let mut callbacks = Callbacks::new();
        {
            let mut state = self
                .line
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            lock_line(&self.line.state).held = false;
            self.released = true;
            notify_head(&mut state, &self.line.state, &mut callbacks);
        }
        callbacks
            .drops
            .extend(self.deferred.iter_mut().filter_map(Option::take));
        callbacks
    }

    /// Releases both lines before invoking either queued head's Waker.
    pub fn release_pair(mut first: Self, mut second: Self) {
        let mut callbacks = first.release_held();
        callbacks.append(second.release_held());
        callbacks.dispatch();
    }
}

impl Drop for MutationPermit {
    fn drop(&mut self) {
        if !self.released {
            self.release_held().dispatch();
        }
    }
}

/// Atomic ownership of settings and lane from one coordinated ticket.
#[must_use = "dropping the permit releases both mutation lines"]
pub struct PairPermit {
    settings: MutationLine,
    lane: MutationLine,
    released: bool,
    deferred: [Option<Waker>; 2],
}

impl PairPermit {
    fn unheld(settings: MutationLine, lane: MutationLine) -> Self {
        Self {
            settings,
            lane,
            released: true,
            deferred: [None, None],
        }
    }

    fn release_held(&mut self) -> Callbacks {
        debug_assert!(!self.released);
        let mut callbacks = Callbacks::new();
        {
            let mut state = self
                .settings
                .admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            {
                let mut settings = lock_line(&self.settings.state);
                let mut lane = lock_line(&self.lane.state);
                settings.held = false;
                lane.held = false;
                self.released = true;
            }
            notify_head(&mut state, &self.settings.state, &mut callbacks);
            notify_head(&mut state, &self.lane.state, &mut callbacks);
        }
        callbacks
            .drops
            .extend(self.deferred.iter_mut().filter_map(Option::take));
        callbacks
    }

    /// Release both lines before dispatching deferred Drops or successor wakes.
    pub fn release(mut self) {
        self.release_held().dispatch();
    }
}

impl Drop for PairPermit {
    fn drop(&mut self) {
        if !self.released {
            self.release_held().dispatch();
        }
    }
}
