use std::future::{Future, pending};
use std::pin::{Pin, pin};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use pi_ai_core::*;

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut TaskContext::from_waker(Waker::noop()))
}
#[derive(Default)]
pub struct Executor {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    reject: AtomicBool,
}
impl TaskSpawner for Executor {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject.load(Ordering::Relaxed) {
            return Err(SpawnError::new());
        }
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Executor {
    fn tick(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                self.tasks.lock().unwrap().push(task);
            }
        }
    }
    pub fn drive<F: Future>(&self, future: F) -> F::Output {
        let mut future = pin!(future);
        for _ in 0..20 {
            if let Poll::Ready(value) = poll(future.as_mut()) {
                return value;
            }
            self.tick();
        }
        panic!("future did not complete");
    }
}
pub struct TestClock;
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(pending())
    }
}
pub struct Host;
impl AuthContext for Host {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
