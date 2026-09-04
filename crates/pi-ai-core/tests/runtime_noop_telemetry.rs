use std::sync::Arc;
use std::time::Duration;

use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, RequestRuntime, SpawnError, TaskSpawner,
};

struct UncalledHost;

impl Clock for UncalledHost {
    fn now_unix_ms(&self) -> u64 {
        panic!("constructing a runtime must not read the host clock")
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        panic!("constructing a runtime must not sleep")
    }
}

impl TaskSpawner for UncalledHost {
    fn spawn(&self, _task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("constructing a runtime must not spawn work")
    }
}

#[test]
fn core_only_constructor_preserves_host_ports_and_shared_cancellation() {
    // This test deliberately imports no telemetry types: transport consumers
    // must construct the runtime through their existing core dependency alone.
    let token = CancellationToken::new();
    let clock: Arc<dyn Clock> = Arc::new(UncalledHost);
    let spawner: Arc<dyn TaskSpawner> = Arc::new(UncalledHost);
    let runtime = RequestRuntime::with_noop_telemetry(
        token.clone(),
        Arc::clone(&clock),
        Arc::clone(&spawner),
    );
    assert!(Arc::ptr_eq(&runtime.clock, &clock));
    assert!(Arc::ptr_eq(&runtime.spawner, &spawner));
    assert!(!runtime.cancellation.is_cancelled());
    token.cancel();
    assert!(runtime.cancellation.is_cancelled());
    assert!(runtime.clone().cancellation.is_cancelled());
}
