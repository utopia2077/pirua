use tokio::sync::watch;

/// Cloneable, one-way cancellation signal. Dropping a clone does not cancel it.
#[derive(Clone, Debug)]
pub struct CancellationToken(watch::Sender<bool>);

impl Default for CancellationToken {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        if *receiver.borrow_and_update() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}
