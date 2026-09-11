use super::{Error, Io};
use pi_ai_core::{BoxFuture, CancellationToken};
use std::{
    future::{Future, poll_fn},
    io::{BufRead, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::Poll,
};
use tokio::sync::oneshot;
type Reply = oneshot::Sender<Result<String, Error>>;
// A request owns its unique one-shot reply. Cancellation cannot reroute a late line.
#[derive(Clone)]
pub struct Reader {
    sender: mpsc::SyncSender<Reply>,
    busy: Arc<AtomicBool>,
}
struct Question(Arc<AtomicBool>);
impl Drop for Question {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl Reader {
    pub fn new<R: BufRead + Send + 'static>(mut input: R) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::sync_channel::<Reply>(1);
        let thread = std::thread::Builder::new()
            .name("oauth-cli-stdin".to_owned())
            .spawn(move || {
                while let Ok(reply) = receiver.recv() {
                    if reply.is_closed() {
                        continue;
                    }
                    let mut line = String::new();
                    let result = match input.read_line(&mut line) {
                        Ok(0) | Err(_) => Err(Error::Interaction),
                        Ok(_) => {
                            if line.ends_with('\n') {
                                line.pop();
                                if line.ends_with('\r') {
                                    line.pop();
                                }
                            }
                            Ok(line)
                        }
                    };
                    let _ = reply.send(result);
                }
            })
            .map_err(|_| Error::Interaction)?;
        drop(thread); // Process-lifetime OS read: never joined by Tokio shutdown.
        Ok(Self {
            sender,
            busy: Arc::new(AtomicBool::new(false)),
        })
    }
    pub async fn line(
        &self,
        cancellation: &CancellationToken,
        closed: &CancellationToken,
    ) -> Result<String, Error> {
        if cancellation.is_cancelled() || closed.is_cancelled() {
            return Err(Error::Interaction);
        }
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Interaction)?;
        let _question = Question(self.busy.clone());
        let (reply, mut response) = oneshot::channel();
        self.sender
            .try_send(reply)
            .map_err(|_| Error::Interaction)?;
        let mut cancelled = Box::pin(cancellation.cancelled());
        let mut retired = Box::pin(closed.cancelled());
        poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() || retired.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(Error::Interaction));
            }
            match std::pin::Pin::new(&mut response).poll(cx) {
                Poll::Ready(Ok(value)) => Poll::Ready(value),
                Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Interaction)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}
pub struct Console {
    output: Mutex<std::io::Stdout>,
    reader: Mutex<Option<Reader>>,
    interface: Mutex<Option<CancellationToken>>,
}
impl Console {
    pub fn new() -> Self {
        Self {
            output: Mutex::new(std::io::stdout()),
            reader: Mutex::new(None),
            interface: Mutex::new(None),
        }
    }
}
impl Io for Console {
    fn open(&self) -> Result<(), Error> {
        let mut slot = self.interface.lock().map_err(|_| Error::Interaction)?;
        if slot.is_some() {
            return Err(Error::Interaction);
        }
        *slot = Some(CancellationToken::new());
        Ok(())
    }
    fn close(&self) {
        if let Ok(mut slot) = self.interface.lock()
            && let Some(token) = slot.take()
        {
            token.cancel();
        }
    }
    fn output(&self, text: &str) -> Result<(), Error> {
        let mut output = self.output.lock().map_err(|_| Error::Interaction)?;
        writeln!(output, "{text}")
            .and_then(|()| output.flush())
            .map_err(|_| Error::Interaction)
    }
    fn line<'a>(
        &'a self,
        question: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async move {
            let closed = self
                .interface
                .lock()
                .map_err(|_| Error::Interaction)?
                .clone()
                .ok_or(Error::Interaction)?;
            let reader = {
                let mut slot = self.reader.lock().map_err(|_| Error::Interaction)?;
                if slot.is_none() {
                    *slot = Some(Reader::new(std::io::BufReader::new(std::io::stdin()))?);
                }
                slot.as_ref().ok_or(Error::Interaction)?.clone()
            };
            {
                let mut output = self.output.lock().map_err(|_| Error::Interaction)?;
                write!(output, "{question}")
                    .and_then(|()| output.flush())
                    .map_err(|_| Error::Interaction)?;
            }
            reader.line(cancellation, &closed).await
        })
    }
}
