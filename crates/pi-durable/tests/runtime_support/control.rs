use super::runtime_support::ScriptState;
impl ScriptState {
    pub fn release(&self, name: &str) {
        self.released.lock().unwrap().insert(name.into());
        if let Some(g) = self.gates.lock().unwrap().get(name) {
            g.notify_waiters();
        }
    }
    pub async fn requests(&self, n: usize) {
        loop {
            let next = self.changed.notified();
            if self.requests.lock().unwrap().len() >= n {
                return;
            }
            next.await;
        }
    }
}
