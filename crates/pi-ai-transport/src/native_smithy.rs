//! Smithy-only branches on the existing native connection/resource boundary.
use super::*;
use crate::SmithyRequestProfile;
#[path = "native_smithy_io.rs"]
mod io_profile;
use hyper::body::{Body, Frame, SizeHint};
use std::{
    future::{Future, poll_fn},
    task::Waker,
    time::Duration,
};

pub(super) fn validate(
    headers: &HttpHeaders,
    profile: &SmithyRequestProfile,
) -> Result<(), TransportError> {
    for (_, value) in headers.iter_for_wire() {
        HeaderValue::from_bytes(&latin1_header_bytes(value)?).map_err(|_| invalid())?;
    }
    if let SmithyRequestProfile::Http2 {
        pseudo_headers: pseudo,
        ..
    } = profile
    {
        pseudo
            .authority
            .parse::<hyper::http::uri::Authority>()
            .map_err(|_| invalid())?;
        if let Some(scheme) = &pseudo.scheme {
            scheme
                .parse::<hyper::http::uri::Scheme>()
                .map_err(|_| invalid())?;
        }
        let mut singles = std::collections::BTreeSet::new();
        for (name, value) in headers.iter_for_wire() {
            if crate::SmithyProtocol::Http2.is_single_value_header(name) && !singles.insert(name) {
                return Err(invalid());
            }
            if matches!(
                name,
                "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
            ) || (name == "te" && value != "trailers")
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Admission {
    Waiting,
    Send,
    Skip,
}
struct Gate {
    state: Admission,
    waiter: Option<Waker>,
}
impl Gate {
    fn settle(gate: &Arc<Mutex<Self>>, state: Admission) {
        let wake = {
            let mut g = gate.lock().unwrap_or_else(|p| p.into_inner());
            if g.state != Admission::Waiting {
                return;
            }
            g.state = state;
            g.waiter.take()
        };
        if let Some(w) = wake {
            w.wake();
        }
    }
}
#[derive(Clone)]
pub(super) struct BodyGate(Arc<Mutex<Gate>>);

/// Runs the source fixed continue grace concurrently with connection acquisition,
/// rather than starting a fresh six seconds only after TCP/TLS completes.
pub(super) struct AdmissionClock {
    gate: BodyGate,
    grace: Option<crate::policy::Timer>,
}
impl AdmissionClock {
    pub(super) fn new(profile: &SmithyRequestProfile, runtime: &RequestRuntime) -> Self {
        let wait = match profile {
            SmithyRequestProfile::Http1 { body_admission, .. }
            | SmithyRequestProfile::Http2 { body_admission, .. } => {
                *body_admission == crate::SmithyBodyAdmission::WaitForContinue
            }
        };
        let clock = runtime.clock.clone();
        let grace = wait.then(|| {
            Box::pin(async move {
                clock
                    .sleep(
                        Duration::from_millis(6000),
                        &pi_ai_core::CancellationToken::new(),
                    )
                    .await
            }) as crate::policy::Timer
        });
        Self {
            gate: BodyGate(Arc::new(Mutex::new(Gate {
                state: if wait {
                    Admission::Waiting
                } else {
                    Admission::Send
                },
                waiter: None,
            }))),
            grace,
        }
    }
    pub(super) fn gate(&self) -> BodyGate {
        self.gate.clone()
    }
    pub(super) fn run<'a>(
        mut self,
        mut operation: BoxFuture<'a, Result<HttpResponse, TransportError>>,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            poll_fn(|cx| {
                if let Some(timer) = &mut self.grace
                    && timer.as_mut().poll(cx).is_ready()
                {
                    Gate::settle(&self.gate.0, Admission::Send);
                    self.grace = None;
                }
                operation.as_mut().poll(cx)
            })
            .await
        })
    }
}
struct GatedBody {
    bytes: Option<Bytes>,
    gate: Arc<Mutex<Gate>>,
}
impl GatedBody {
    fn new(bytes: Vec<u8>, gate: Arc<Mutex<Gate>>) -> Self {
        Self {
            bytes: Some(Bytes::from(bytes)),
            gate,
        }
    }
    fn admission(&mut self, cx: &mut Context<'_>) -> Poll<Admission> {
        let next = cx.waker().clone();
        let (state, previous) = {
            let mut g = self.gate.lock().unwrap_or_else(|p| p.into_inner());
            (
                g.state,
                if g.state == Admission::Waiting {
                    g.waiter.replace(next)
                } else {
                    None
                },
            )
        };
        drop(previous);
        if state == Admission::Waiting {
            Poll::Pending
        } else {
            Poll::Ready(state)
        }
    }
}
impl Body for GatedBody {
    type Data = Bytes;
    type Error = TransportError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, TransportError>>> {
        if self.bytes.is_none() {
            return Poll::Ready(None);
        }
        match self.admission(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Admission::Send) => Poll::Ready(
                self.bytes
                    .take()
                    .filter(|b| !b.is_empty())
                    .map(|b| Ok(Frame::data(b))),
            ),
            _ => {
                self.bytes = None;
                Poll::Ready(None)
            }
        }
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.bytes.as_ref().map_or(0, |v| v.len()) as u64)
    }
    fn is_end_stream(&self) -> bool {
        self.bytes.as_ref().is_none_or(|v| v.is_empty())
    }
}

pub(super) async fn send(
    stream: Box<dyn SocketIo>,
    mut request: Request<()>,
    bytes: Vec<u8>,
    profile: SmithyRequestProfile,
    handle: Handle,
    head_limit: usize,
    body_gate: BodyGate,
) -> Result<HttpResponse, TransportError> {
    let gate = body_gate.0;
    let waiting = gate.lock().unwrap_or_else(|p| p.into_inner()).state == Admission::Waiting;
    match profile {
        SmithyRequestProfile::Http1 { connection, .. } => {
            if !request.headers().contains_key("connection") {
                request.headers_mut().insert(
                    "connection",
                    HeaderValue::from_static(match connection {
                        crate::SmithyH1Connection::Close => "close",
                        crate::SmithyH1Connection::KeepAlive => "keep-alive",
                    }),
                );
            }
            let capture = Arc::new(Mutex::new(HeadCapture::new(head_limit)));
            {
                let mut c = capture.lock().unwrap_or_else(|p| p.into_inner());
                c.capture_fields = true;
            }
            let io = ObserveHead {
                inner: stream,
                capture: capture.clone(),
            };
            let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
                .await
                .map_err(crate::smithy_error::hyper)?;
            let driver = AbortTask(handle.spawn(async move {
                let _ = connection.await;
            }));
            let body = GatedBody::new(bytes, gate.clone());
            let mut request = request.map(|_| body);
            let notify = gate.clone();
            hyper::ext::on_informational(&mut request, move |head| {
                if head.status() == 100 {
                    Gate::settle(&notify, Admission::Send);
                }
            });
            let result = sender.send_request(request).await;
            Gate::settle(&gate, Admission::Skip);
            let response = result.map_err(|error| {
                if capture.lock().unwrap_or_else(|p| p.into_inner()).exceeded {
                    TransportError::new(TransportErrorKind::LimitExceeded)
                } else {
                    crate::smithy_error::hyper(error)
                }
            })?;
            let mut c = capture.lock().unwrap_or_else(|p| p.into_inner());
            if c.invalid_fields {
                return Err(connection_error());
            }
            let phrase = c.phrase.take().ok_or_else(connection_error)?;
            let mut headers = HttpHeaders::new();
            for (name, value) in std::mem::take(&mut c.fields) {
                let name = String::from_utf8(name).map_err(|_| connection_error())?;
                let value: String = value.into_iter().map(char::from).collect();
                headers.append(name, value.trim_matches([' ', '\t']))?;
            }
            Ok(HttpResponse {
                status: response.status().as_u16(),
                status_text: phrase.into_iter().map(char::from).collect(),
                headers,
                body: Box::new(NativeBody {
                    handle,
                    body: Some(response.into_body()),
                    driver: Some(driver),
                    smithy: true,
                }),
            })
        }
        SmithyRequestProfile::Http2 {
            pseudo_headers: pseudo,
            ..
        } => {
            let mut parts = request.uri().clone().into_parts();
            parts.authority = Some(pseudo.authority.parse().map_err(|_| invalid())?);
            if let Some(scheme) = pseudo.scheme {
                parts.scheme = Some(scheme.parse().map_err(|_| invalid())?);
            }
            *request.uri_mut() = hyper::Uri::from_parts(parts).map_err(|_| invalid())?;
            let mut builder = h2::client::Builder::new();
            builder.max_header_list_size(u32::try_from(head_limit).unwrap_or(u32::MAX));
            builder.max_concurrent_streams(1);
            let (stream, head_state) = io_profile::H2Io::new(stream, head_limit);
            let (mut sender, connection) = builder
                .handshake::<_, Bytes>(stream)
                .await
                .map_err(h2_failure)?;
            let driver = AbortTask(handle.spawn(async move {
                let _ = connection.await;
            }));
            sender = poll_fn(|cx| sender.poll_ready(cx))
                .await
                .map(|_| sender)
                .map_err(h2_failure)?;
            let empty = bytes.is_empty() && !waiting;
            let (mut response, send) =
                sender.send_request(request, empty).map_err(|_| invalid())?;
            drop(sender);
            let mut upload = if empty {
                None
            } else {
                let gated = GatedBody::new(bytes, gate.clone());
                Some(AbortTask(handle.spawn(upload(send, gated))))
            };
            let mut charge = 0usize;
            let result = poll_fn(|cx| {
                for index in 0..32 {
                    match response.poll_informational(cx) {
                        Poll::Ready(Some(Ok(info))) => {
                            if let Err(e) = charge_head(&mut charge, info.headers(), head_limit) {
                                return Poll::Ready(Err(e));
                            }
                            if info.status() == 100 {
                                Gate::settle(&gate, Admission::Send);
                            }
                            if index == 31 {
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                        Poll::Ready(Some(Err(error))) => {
                            return Poll::Ready(Err(h2_failure(error)));
                        }
                        _ => break,
                    }
                }
                if let Some(task) = &mut upload {
                    match Pin::new(&mut task.0).poll(cx) {
                        Poll::Ready(Ok(Ok(()))) => upload = None,
                        Poll::Ready(Ok(Err(error))) => return Poll::Ready(Err(error)),
                        Poll::Ready(Err(_)) => {
                            return Poll::Ready(Err(crate::smithy_error::no_retry(
                                TransportErrorKind::Connection,
                            )));
                        }
                        Poll::Pending => {}
                    }
                }
                Pin::new(&mut response)
                    .poll(cx)
                    .map(|result| result.map_err(h2_failure))
            })
            .await;
            let response = result.map_err(|error| {
                if head_state.exceeded() {
                    TransportError::new(TransportErrorKind::LimitExceeded)
                } else {
                    error
                }
            })?;
            Gate::settle(&gate, Admission::Skip);
            charge_head(&mut charge, response.headers(), head_limit)?;
            head_state.complete();
            let mut headers = HttpHeaders::new();
            // h2 0.4.19 appends decoded fields; http1.5's append-only entries Vec
            // and iterator retain first-name order even across growth/rebuild.
            for (name, value) in response.headers() {
                if [value.as_bytes().first(), value.as_bytes().last()]
                    .into_iter()
                    .flatten()
                    .any(|b| matches!(b, b' ' | b'\t'))
                {
                    continue;
                }
                headers.append(
                    name.as_str(),
                    value
                        .as_bytes()
                        .iter()
                        .copied()
                        .map(char::from)
                        .collect::<String>(),
                )?;
            }
            Ok(HttpResponse {
                status: response.status().as_u16(),
                status_text: String::new(),
                headers,
                body: Box::new(H2Body {
                    handle,
                    body: Some(response.into_body()),
                    driver: Some(driver),
                    upload,
                    head_state,
                }),
            })
        }
    }
}

fn h2_failure(error: h2::Error) -> TransportError {
    // h2's client-side decoded header-list overflow emits a local REFUSED_STREAM;
    // a remote refusal remains a connection failure, not our resource policy.
    if error.is_library() && error.reason() == Some(h2::Reason::REFUSED_STREAM) {
        TransportError::new(TransportErrorKind::LimitExceeded)
    } else {
        crate::smithy_error::h2(error)
    }
}
fn charge_head(
    total: &mut usize,
    headers: &hyper::HeaderMap,
    limit: usize,
) -> Result<(), TransportError> {
    *total = total
        .checked_add(42)
        .ok_or_else(|| TransportError::new(TransportErrorKind::LimitExceeded))?;
    for (name, value) in headers {
        *total = total
            .checked_add(name.as_str().len() + value.len() + 32)
            .ok_or_else(|| TransportError::new(TransportErrorKind::LimitExceeded))?;
    }
    if *total > limit {
        Err(TransportError::new(TransportErrorKind::LimitExceeded))
    } else {
        Ok(())
    }
}
async fn upload(
    mut send: h2::SendStream<Bytes>,
    mut gated: GatedBody,
) -> Result<(), TransportError> {
    if poll_fn(|cx| gated.admission(cx)).await != Admission::Send {
        return Ok(());
    }
    let mut bytes = gated.bytes.take().unwrap_or_default();
    if bytes.is_empty() {
        send.send_data(Bytes::new(), true)
            .map_err(crate::smithy_error::h2)?;
    }
    while !bytes.is_empty() {
        send.reserve_capacity(bytes.len().min(16_384));
        let capacity = poll_fn(|cx| send.poll_capacity(cx))
            .await
            .ok_or_else(connection_error)?
            .map_err(crate::smithy_error::h2)?;
        if capacity == 0 {
            continue;
        }
        let chunk = bytes.split_to(capacity.min(bytes.len()).min(16_384));
        send.send_data(chunk, bytes.is_empty())
            .map_err(crate::smithy_error::h2)?;
    }
    Ok(())
}
struct H2Body {
    head_state: Arc<io_profile::HeadState>,
    handle: Handle,
    body: Option<h2::RecvStream>,
    driver: Option<AbortTask<()>>,
    upload: Option<AbortTask<Result<(), TransportError>>>,
}
impl ResponseBody for H2Body {
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            let Some(mut body) = self.body.take() else {
                return Ok(None);
            };
            let driver = self.driver.take();
            let upload = self.upload.take();
            let head_state = self.head_state.clone();
            let (body, driver, upload, chunk) = crate::policy::cancel_wait(
                Box::pin(on_runtime(&self.handle, async move {
                    let chunk = body.data().await.transpose().map_err(|error| {
                        if head_state.exceeded() {
                            TransportError::new(TransportErrorKind::LimitExceeded)
                        } else {
                            h2_failure(error)
                        }
                    })?;
                    if let Some(bytes) = &chunk {
                        body.flow_control()
                            .release_capacity(bytes.len())
                            .map_err(crate::smithy_error::h2)?;
                    }
                    Ok((body, driver, upload, chunk.map(|b| b.to_vec())))
                })),
                &runtime.cancellation,
            )
            .await?;
            if chunk.is_some() {
                self.body = Some(body);
                self.driver = driver;
                self.upload = upload;
            }
            Ok(chunk)
        })
    }
    fn close(&mut self) {
        self.body = None;
        self.driver = None;
        self.upload = None;
    }
}
