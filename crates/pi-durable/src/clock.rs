use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

#[derive(Clone)]
pub struct Cancellation {
    pub(crate) sender: watch::Sender<bool>,
}
impl Cancellation {
    pub(crate) fn new() -> Self {
        Self {
            sender: watch::channel(false).0,
        }
    }
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
    pub async fn cancelled(&self) {
        let mut rx = self.sender.subscribe();
        if !*rx.borrow() {
            let _ = rx.changed().await;
        }
    }
    pub(crate) fn cancel(&self) {
        self.sender.send_replace(true);
    }
}
/// Cancelling is idempotent, including when a host retains the callback.
#[derive(Clone)]
pub struct Timer {
    pub(crate) disarm: Arc<dyn Fn() + Send + Sync>,
}
impl Timer {
    pub fn cancel(&self) {
        (self.disarm)();
    }
}
pub type TimerHost = Arc<dyn Fn(Arc<dyn Fn() + Send + Sync>, f64) -> Timer + Send + Sync>;
impl Timer {
    pub fn new(disarm: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            disarm: Arc::new(disarm),
        }
    }
}
pub(crate) fn native_timer(callback: Arc<dyn Fn() + Send + Sync>, delay: f64) -> Timer {
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let task = tokio::spawn(async move {
        let mut remaining = delay.max(0.0);
        while remaining > 0.0 {
            let next = remaining.min(2_147_483_647.0);
            tokio::time::sleep(Duration::from_secs_f64(next / 1000.0)).await;
            remaining -= next;
        }
        if !flag.load(Ordering::SeqCst) {
            callback();
        }
    });
    Timer::new(move || {
        cancelled.store(true, Ordering::SeqCst);
        task.abort();
    })
}
pub(crate) fn wall_clock() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1000.0
}
