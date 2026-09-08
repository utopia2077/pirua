//! Smithy-only bounded frame envelopes and one Node session-close control.
//! No HPACK decoding or alternate HTTP/2 response interpretation lives here.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct HeadState {
    finished: AtomicBool,
    exceeded: AtomicBool,
    driver: Mutex<Option<Waker>>,
}
impl HeadState {
    pub(super) fn complete(&self) {
        self.finished.store(true, Ordering::SeqCst);
        let wake = self.driver.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(w) = wake {
            w.wake();
        }
    }
    pub(super) fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::SeqCst)
    }
    fn register(&self, waker: &Waker) {
        let next = waker.clone();
        let previous = self
            .driver
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(next);
        drop(previous);
    }
}

struct OutboundFrame {
    preface: usize,
    header: [u8; 9],
    used: usize,
    remaining: usize,
}
impl OutboundFrame {
    fn boundary(&self) -> bool {
        self.preface == 0 && self.used == 0 && self.remaining == 0
    }
    fn observe(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self.preface != 0 {
                let n = self.preface.min(bytes.len());
                self.preface -= n;
                bytes = &bytes[n..];
                continue;
            }
            if self.remaining != 0 {
                let n = self.remaining.min(bytes.len());
                self.remaining -= n;
                bytes = &bytes[n..];
                continue;
            }
            let n = (9 - self.used).min(bytes.len());
            self.header[self.used..self.used + n].copy_from_slice(&bytes[..n]);
            self.used += n;
            bytes = &bytes[n..];
            if self.used == 9 {
                self.remaining = (usize::from(self.header[0]) << 16)
                    | (usize::from(self.header[1]) << 8)
                    | usize::from(self.header[2]);
                self.used = 0;
            }
        }
    }
}

pub(super) struct H2Io {
    inner: Box<dyn SocketIo>,
    state: Arc<HeadState>,
    header: [u8; 9],
    used: usize,
    remaining: usize,
    charged_frame: bool,
    metadata_frame: bool,
    metadata_continuation: bool,
    metadata_bytes: usize,
    metadata_blocks: usize,
    bytes: usize,
    blocks: usize,
    limit: usize,
    outbound: OutboundFrame,
    offered_remaining: usize,
    control_offset: usize,
    control_flushed: bool,
}
impl H2Io {
    pub(super) fn new(inner: Box<dyn SocketIo>, limit: usize) -> (Self, Arc<HeadState>) {
        let state = Arc::new(HeadState {
            finished: AtomicBool::new(false),
            exceeded: AtomicBool::new(false),
            driver: Mutex::new(None),
        });
        (
            Self {
                inner,
                state: state.clone(),
                header: [0; 9],
                used: 0,
                remaining: 0,
                charged_frame: false,
                metadata_frame: false,
                metadata_continuation: false,
                metadata_bytes: 0,
                metadata_blocks: 0,
                bytes: 0,
                blocks: 0,
                limit,
                outbound: OutboundFrame {
                    preface: 24,
                    header: [0; 9],
                    used: 0,
                    remaining: 0,
                },
                offered_remaining: 0,
                control_offset: 0,
                control_flushed: false,
            },
            state,
        )
    }
    fn observe(&mut self, mut input: &[u8]) -> io::Result<()> {
        while !input.is_empty() {
            if self.remaining > 0 {
                let count = self.remaining.min(input.len());
                if self.charged_frame {
                    self.charge(count)?;
                }
                self.remaining -= count;
                input = &input[count..];
                continue;
            }
            let count = (9 - self.used).min(input.len());
            self.header[self.used..self.used + count].copy_from_slice(&input[..count]);
            self.used += count;
            input = &input[count..];
            if self.used == 9 {
                self.remaining = (usize::from(self.header[0]) << 16)
                    | (usize::from(self.header[1]) << 8)
                    | usize::from(self.header[2]);
                let kind = self.header[3];
                let stream =
                    u32::from_be_bytes(self.header[5..9].try_into().expect("fixed frame header"))
                        & 0x7fff_ffff;
                // PUSH_PROMISE continuations use the parent stream id (1), so
                // preserve the initiating envelope's budget domain explicitly.
                // h2 remains the authority for legal continuation sequencing.
                self.metadata_frame = match kind {
                    5 => true,
                    1 => stream != 1,
                    9 => self.metadata_continuation,
                    _ => false,
                };
                if matches!(kind, 1 | 5) {
                    self.metadata_continuation = self.metadata_frame && self.header[4] & 4 == 0;
                    if self.metadata_frame {
                        self.metadata_blocks = self.metadata_blocks.saturating_add(1);
                    } else if !self.state.finished.load(Ordering::SeqCst) {
                        self.blocks = self.blocks.saturating_add(1);
                    }
                } else if kind == 9 && self.header[4] & 4 != 0 {
                    self.metadata_continuation = false;
                }
                self.charged_frame = matches!(kind, 1 | 5 | 9);
                if self.charged_frame {
                    self.charge(9)?;
                }
                self.used = 0;
            }
        }
        Ok(())
    }
    fn charge(&mut self, count: usize) -> io::Result<()> {
        if self.metadata_frame {
            self.metadata_bytes = self.metadata_bytes.saturating_add(count);
        } else if !self.state.finished.load(Ordering::SeqCst) {
            self.bytes = self.bytes.saturating_add(count);
        }
        if self.bytes > self.limit
            || self.blocks > 128
            || self.metadata_bytes > self.limit
            || self.metadata_blocks > 128
        {
            self.state.exceeded.store(true, Ordering::SeqCst);
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Smithy response metadata limit",
            ))
        } else {
            Ok(())
        }
    }
    /// Node26 session.close() calls goaway(NO_ERROR,0) on this isolated,
    /// single-request connection. Finish already offered h2 frame bytes first.
    /// Only read/flush starts injection; a newly offered h2 write is not reordered
    /// ahead of its own already offered prefix. A partial control owns the writer
    /// until all17 bytes are consumed; those bytes never count as caller input.
    fn pump_control(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        const GOAWAY: [u8; 17] = [0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        if !self.state.finished.load(Ordering::SeqCst)
            || !self.outbound.boundary()
            || self.offered_remaining != 0
            || self.control_flushed
        {
            return Poll::Ready(Ok(()));
        }
        while self.control_offset < GOAWAY.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &GOAWAY[self.control_offset..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "Smithy control write closed",
                    )));
                }
                Poll::Ready(Ok(n)) => self.control_offset += n,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.control_flushed = true;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
impl AsyncRead for H2Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.state.register(cx.waker());
        // A blocked control write does not block inbound response progress.
        if let Poll::Ready(Err(error)) = self.pump_control(cx) {
            return Poll::Ready(Err(error));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(())))
            && let Err(error) = self.observe(&buf.filled()[before..])
        {
            return Poll::Ready(Err(error));
        }
        result
    }
}
impl AsyncWrite for H2Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.control_offset != 0 && !self.control_flushed {
            match self.pump_control(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.offered_remaining = self.offered_remaining.max(bytes.len());
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(n)) = result {
            self.outbound.observe(&bytes[..n]);
            self.offered_remaining -= n;
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.state.register(cx.waker());
        match self.pump_control(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_flush(cx),
            other => other,
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Cancellation/drop is allowed to retire without flushing controls.
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, sync::atomic::AtomicUsize, task::Wake};
    #[derive(Default)]
    struct State {
        written: Vec<u8>,
        read: VecDeque<u8>,
        max_write: usize,
        stop_at: usize,
        flush_blocked: bool,
        dropped: usize,
    }
    struct Wire(Arc<Mutex<State>>);
    impl Drop for Wire {
        fn drop(&mut self) {
            self.0.lock().unwrap().dropped += 1;
        }
    }
    impl AsyncRead for Wire {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let mut s = self.0.lock().unwrap();
            if s.read.is_empty() {
                return Poll::Pending;
            }
            while buf.remaining() != 0 {
                if let Some(b) = s.read.pop_front() {
                    buf.put_slice(&[b]);
                } else {
                    break;
                }
            }
            Poll::Ready(Ok(()))
        }
    }
    impl AsyncWrite for Wire {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let mut s = self.0.lock().unwrap();
            let n = bytes
                .len()
                .min(s.max_write)
                .min(s.stop_at.saturating_sub(s.written.len()));
            if n == 0 {
                return Poll::Pending;
            }
            s.written.extend_from_slice(&bytes[..n]);
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.0.lock().unwrap().flush_blocked {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    fn setup() -> (H2Io, Arc<HeadState>, Arc<Mutex<State>>) {
        let wire = Arc::new(Mutex::new(State {
            max_write: usize::MAX,
            stop_at: usize::MAX,
            ..Default::default()
        }));
        let (io, state) = H2Io::new(Box::new(Wire(wire.clone())), 64);
        (io, state, wire)
    }
    fn outbound() -> Vec<u8> {
        let mut b = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        b.extend_from_slice(&[
            0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 1, 1, 2, 3, 4,
        ]);
        b
    }
    fn write(io: &mut H2Io, bytes: &[u8]) -> usize {
        match Pin::new(io).poll_write(&mut Context::from_waker(Waker::noop()), bytes) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("{other:?}"),
        }
    }
    #[test]
    fn every_outbound_split_preserves_preface_frames_queued_bytes_and_consumed_counts() {
        let bytes = outbound();
        for split in 1..bytes.len() {
            let (mut io, state, wire) = setup();
            wire.lock().unwrap().max_write = split;
            let n = write(&mut io, &bytes);
            assert_eq!(n, split);
            state.complete();
            let mut read = [0; 1];
            assert!(
                Pin::new(&mut io)
                    .poll_read(
                        &mut Context::from_waker(Waker::noop()),
                        &mut ReadBuf::new(&mut read)
                    )
                    .is_pending()
            );
            assert_eq!(wire.lock().unwrap().written, &bytes[..split]);
            wire.lock().unwrap().max_write = 1;
            let mut count = n;
            while count < bytes.len() {
                count += write(&mut io, &bytes[count..]);
            }
            assert_eq!(count, bytes.len());
            assert!(matches!(
                Pin::new(&mut io).poll_flush(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
            let mut expected = bytes.clone();
            expected.extend_from_slice(&[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            assert_eq!(wire.lock().unwrap().written, expected);
            state.complete();
            let _ = Pin::new(&mut io).poll_flush(&mut Context::from_waker(Waker::noop()));
            assert_eq!(wire.lock().unwrap().written, expected);
            drop(io);
            assert_eq!(wire.lock().unwrap().dropped, 1);
        }
    }
    #[test]
    fn partial_control_backpressure_keeps_reads_live_and_never_consumes_caller_bytes() {
        let (mut io, state, wire) = setup();
        let bytes = outbound();
        assert_eq!(write(&mut io, &bytes), bytes.len());
        state.complete();
        wire.lock().unwrap().stop_at = bytes.len() + 5;
        assert!(
            Pin::new(&mut io)
                .poll_flush(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(io.control_offset, 5);
        wire.lock().unwrap().read.push_back(b'x');
        let mut b = [0];
        let mut read = ReadBuf::new(&mut b);
        assert!(matches!(
            Pin::new(&mut io).poll_read(&mut Context::from_waker(Waker::noop()), &mut read),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(read.filled(), b"x");
        let next = [0, 0, 0, 4, 1, 0, 0, 0, 0];
        assert!(
            Pin::new(&mut io)
                .poll_write(&mut Context::from_waker(Waker::noop()), &next)
                .is_pending()
        );
        {
            let mut s = wire.lock().unwrap();
            s.stop_at = usize::MAX;
            s.max_write = 1;
            s.flush_blocked = true;
        }
        assert!(
            Pin::new(&mut io)
                .poll_flush(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(io.control_offset, 17);
        assert!(
            Pin::new(&mut io)
                .poll_write(&mut Context::from_waker(Waker::noop()), &next)
                .is_pending()
        );
        wire.lock().unwrap().flush_blocked = false;
        let mut consumed = 0;
        while consumed < next.len() {
            consumed += write(&mut io, &next[consumed..]);
        }
        assert_eq!(consumed, next.len());
        let written = &wire.lock().unwrap().written;
        assert_eq!(&written[bytes.len() + 17..], &next);
    }
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn accepted_head_wakes_idle_reader_and_drop_releases_blocked_control() {
        let (mut io, state, wire) = setup();
        let bytes = outbound();
        write(&mut io, &bytes);
        let count = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut data = [0];
        let _ = Pin::new(&mut io).poll_read(
            &mut Context::from_waker(&waker),
            &mut ReadBuf::new(&mut data),
        );
        state.complete();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        wire.lock().unwrap().stop_at = bytes.len() + 2;
        let _ = Pin::new(&mut io).poll_read(
            &mut Context::from_waker(&waker),
            &mut ReadBuf::new(&mut data),
        );
        assert_eq!(io.control_offset, 2);
        drop(io);
        assert_eq!(wire.lock().unwrap().dropped, 1);
    }
    fn inbound(io: &mut H2Io, wire: &Arc<Mutex<State>>, bytes: &[u8]) -> io::Result<()> {
        wire.lock().unwrap().read.extend(bytes);
        let mut output = vec![0; bytes.len()];
        match Pin::new(io).poll_read(
            &mut Context::from_waker(Waker::noop()),
            &mut ReadBuf::new(&mut output),
        ) {
            Poll::Ready(result) => result,
            other => panic!("{other:?}"),
        }
    }
    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let n = payload.len();
        let mut bytes = vec![(n >> 16) as u8, (n >> 8) as u8, n as u8, kind, flags];
        bytes.extend_from_slice(&stream.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }
    #[test]
    fn metadata_continuations_keep_their_domain_across_head_acceptance_and_every_split() {
        // The promise's continuation uses stream 1, unlike pushed response headers.
        let mut metadata = frame(5, 0, 1, &[0, 0, 0, 2, 0x82]);
        metadata.extend(frame(9, 4, 1, &[0x86, 0x84]));
        metadata.extend(frame(1, 0, 2, &[0x88]));
        metadata.extend(frame(9, 4, 2, &[]));
        assert_eq!(metadata.len(), 44);
        for limit in [43, 44] {
            for split in 0..=metadata.len() {
                let (mut io, state, wire) = setup();
                io.limit = limit;
                let first = if split == 0 {
                    Ok(())
                } else {
                    inbound(&mut io, &wire, &metadata[..split])
                };
                state.complete();
                let result = first.and_then(|()| {
                    if split == metadata.len() {
                        Ok(())
                    } else {
                        inbound(&mut io, &wire, &metadata[split..])
                    }
                });
                assert_eq!(result.is_ok(), limit == 44, "limit={limit}, split={split}");
                assert_eq!(state.exceeded(), limit == 43);
                if limit == 44 {
                    assert_eq!(io.metadata_bytes, 44);
                    assert_eq!(io.metadata_blocks, 2);
                    assert_eq!(io.bytes, 0);
                    // Subsequent request trailers/CONTINUATION, SETTINGS, PING,
                    // and normal DATA cannot consume lifetime metadata allowance.
                    let mut normal = frame(1, 0, 1, &[0x88]);
                    normal.extend(frame(9, 4, 1, &[]));
                    normal.extend(frame(4, 0, 0, &[]));
                    normal.extend(frame(6, 0, 0, &[0; 8]));
                    normal.extend(frame(0, 0, 1, &[0; 16384]));
                    for _ in 0..8 {
                        inbound(&mut io, &wire, &normal).unwrap();
                    }
                    assert_eq!(io.metadata_bytes, 44);
                    assert_eq!(io.bytes, 0);
                }
                drop(io);
                assert_eq!(wire.lock().unwrap().dropped, 1);
            }
        }
    }

    #[test]
    fn envelope_charge_is_split_invariant_including_empty_continuations() {
        let mut head = vec![0, 0, 1, 1, 1, 0, 0, 0, 1, 0x88];
        for i in 0..6 {
            head.extend_from_slice(&[0, 0, 0, 9, if i == 5 { 4 } else { 0 }, 0, 0, 0, 1]);
        }
        for split in 0..=head.len() {
            let (mut io, _, _) = setup();
            io.observe(&head[..split]).unwrap();
            io.observe(&head[split..]).unwrap();
            assert_eq!(io.bytes, 64);
            let (mut short, state, _) = setup();
            short.limit = 63;
            assert!(
                short
                    .observe(&head[..split])
                    .and_then(|_| short.observe(&head[split..]))
                    .is_err()
            );
            assert!(state.exceeded());
        }
    }
}
