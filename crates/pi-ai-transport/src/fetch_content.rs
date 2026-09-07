//! Bounded Node 26.8.1 / Undici 8.10.0 network Fetch content composition.
use std::mem::size_of;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::{HttpRequest, HttpResponse, ResponseBody, TransportError, TransportErrorKind};
use brotli_decompressor::{
    Allocator, BrotliDecompressStream, BrotliResult, BrotliState, SliceWrapper, SliceWrapperMut,
};
use flate2::{Decompress, FlushDecompress, Status};
use pi_ai_core::{BoxFuture, RequestRuntime};
use url::Url;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FetchResponseSource {
    /// The injected client yields encoded network bytes.
    #[default]
    RawNetwork,
    /// A trusted binding yields a semantic Response; neither defaults nor decoding apply.
    Preconstructed,
}

#[derive(Clone, Copy, Debug)]
pub struct FetchContentLimits {
    /// Maximum pending encoded input at each layer. Never a total traffic counter.
    pub max_pending_bytes: usize,
    /// Maximum bytes returned by a decoding layer in one next call.
    pub output_chunk_bytes: usize,
    /// Maximum logical codec working storage per layer. Includes Brotli allocated
    /// cells; zlib has a fixed reservation (including gzip's <=16-KiB semantic
    /// commit queue) and Zstd derives a bounded window.
    pub max_codec_bytes: usize,
}
impl Default for FetchContentLimits {
    fn default() -> Self {
        Self {
            max_pending_bytes: 1024 * 1024,
            output_chunk_bytes: 32 * 1024,
            max_codec_bytes: 32 * 1024 * 1024,
        }
    }
}
fn limit() -> TransportError {
    TransportError::new(TransportErrorKind::LimitExceeded)
}
fn corrupt() -> TransportError {
    TransportError::new(TransportErrorKind::Connection)
}

pub(crate) fn request_defaults(request: &mut HttpRequest, url: &Url) -> Result<(), TransportError> {
    let headers = &mut request.headers;
    for (name, value) in [
        ("accept", "*/*"),
        ("accept-language", "*"),
        ("user-agent", "node"),
    ] {
        if headers.get_for_wire(name).is_none() {
            headers.set(name, value)?;
        }
    }
    headers.set("sec-fetch-mode", "cors")?;
    if headers.get_for_wire("content-length").is_none()
        && (!request.body.is_empty()
            || matches!(
                request.method.as_str(),
                "POST" | "PUT" | "PATCH" | "QUERY" | "PROPFIND" | "PROPPATCH"
            ))
    {
        headers.set("content-length", request.body.len().to_string())?;
    }
    if [
        "if-modified-since",
        "if-none-match",
        "if-unmodified-since",
        "if-match",
        "if-range",
    ]
    .iter()
    .any(|name| headers.get_for_wire(name).is_some())
    {
        for name in ["pragma", "cache-control"] {
            if headers.get_for_wire(name).is_none() {
                headers.set(name, "no-cache")?;
            }
        }
    }
    if headers.get_for_wire("range").is_some() {
        headers.append("accept-encoding", "identity")?;
    }
    if headers.get_for_wire("accept-encoding").is_none() {
        headers.set(
            "accept-encoding",
            if url.scheme() == "https" {
                "br, gzip, deflate, zstd"
            } else {
                "gzip, deflate"
            },
        )?;
    }
    headers.remove("host");
    Ok(())
}

pub(crate) fn response(
    mut response: HttpResponse,
    method: &str,
    will_follow: bool,
    limits: FetchContentLimits,
) -> Result<HttpResponse, TransportError> {
    if will_follow {
        return Ok(response);
    }
    if matches!(method, "HEAD" | "CONNECT")
        || matches!(response.status, 101 | 103 | 204 | 205 | 304)
    {
        response.body.close();
        response.body = Box::new(EmptyBody);
        return Ok(response);
    }
    let header = response
        .headers
        .get_for_fetch("content-encoding")
        .unwrap_or_default();
    if header.is_empty() {
        return Ok(response);
    }
    if header.split(',').take(6).count() > 5 {
        response.body.close();
        return Err(corrupt());
    }
    let mut kinds = Vec::new();
    for coding in header.split(',').rev() {
        kinds.push(
            match coding
                .trim_matches(crate::is_ecma_whitespace)
                .to_ascii_lowercase()
                .as_str()
            {
                "gzip" | "x-gzip" => Kind::Gzip,
                "deflate" => Kind::Deflate,
                "br" => Kind::Brotli,
                "zstd" => Kind::Zstd,
                _ => return Ok(response),
            },
        );
    }
    if limits.output_chunk_bytes == 0 || limits.output_chunk_bytes > limits.max_pending_bytes {
        response.body.close();
        return Err(limit());
    }
    for kind in kinds {
        let codec = match Codec::new(kind, limits) {
            Ok(codec) => codec,
            Err(error) => {
                response.body.close();
                return Err(error);
            }
        };
        response.body = Box::new(DecodedBody {
            inner: Some(response.body),
            codec: Some(codec),
            input: Vec::new(),
            position: 0,
            eof: false,
            finished: false,
            seen_input: false,
            gzip_output_offset: 0,
            committed: Vec::new(),
            committed_position: 0,
            limits,
        });
    }
    Ok(response)
}
struct EmptyBody;
impl ResponseBody for EmptyBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&mut self) {}
}
#[derive(Clone, Copy)]
enum Kind {
    Gzip,
    Deflate,
    Brotli,
    Zstd,
}

struct Budget {
    used: AtomicUsize,
    maximum: usize,
    exceeded: AtomicBool,
}
struct Cells<T> {
    data: Vec<T>,
    budget: Option<Arc<Budget>>,
    bytes: usize,
}
impl<T> Default for Cells<T> {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            budget: None,
            bytes: 0,
        }
    }
}
impl<T> SliceWrapper<T> for Cells<T> {
    fn slice(&self) -> &[T] {
        &self.data
    }
}
impl<T> SliceWrapperMut<T> for Cells<T> {
    fn slice_mut(&mut self) -> &mut [T] {
        &mut self.data
    }
}
impl<T> Drop for Cells<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.data));
        if let Some(b) = &self.budget {
            b.used.fetch_sub(self.bytes, Ordering::Relaxed);
        }
    }
}
struct BoundedAlloc(Arc<Budget>);
impl<T: Default + Clone> Allocator<T> for BoundedAlloc {
    type AllocatedMemory = Cells<T>;
    fn alloc_cell(&mut self, len: usize) -> Cells<T> {
        let bytes = len.checked_mul(size_of::<T>());
        let Some(bytes) = bytes.filter(|bytes| {
            self.0
                .used
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                    used.checked_add(*bytes)
                        .filter(|next| *next <= self.0.maximum)
                })
                .is_ok()
        }) else {
            self.0.exceeded.store(true, Ordering::Relaxed);
            return Cells::default();
        };
        let mut data = Vec::new();
        if data.try_reserve_exact(len).is_err() {
            self.0.used.fetch_sub(bytes, Ordering::Relaxed);
            self.0.exceeded.store(true, Ordering::Relaxed);
            return Cells::default();
        }
        data.resize(len, T::default());
        Cells {
            data,
            budget: Some(self.0.clone()),
            bytes,
        }
    }
    fn free_cell(&mut self, cell: Cells<T>) {
        drop(cell);
    }
}
type Brotli = BrotliState<BoundedAlloc, BoundedAlloc, BoundedAlloc>;
enum Codec {
    Flate {
        decoder: Option<Decompress>,
        gzip: bool,
        ended: bool,
        discard: bool,
    },
    Brotli {
        decoder: Box<Brotli>,
        budget: Arc<Budget>,
        ended: bool,
    },
    Zstd {
        decoder: zstd_safe::DCtx<'static>,
        ended: bool,
    },
}
struct Step {
    read: usize,
    written: usize,
    needs_input: bool,
}
impl Codec {
    fn is_gzip(&self) -> bool {
        matches!(self, Self::Flate { gzip: true, .. })
    }
    fn ignores_tail(&self) -> bool {
        matches!(
            self,
            Self::Flate {
                gzip: false,
                ended: true,
                ..
            } | Self::Brotli { ended: true, .. }
                | Self::Zstd { ended: true, .. }
        )
    }
    fn new(kind: Kind, limits: FetchContentLimits) -> Result<Self, TransportError> {
        Ok(match kind {
            Kind::Gzip | Kind::Deflate => {
                // Fixed 32-KiB window, decoder state and gzip's <=16-KiB
                // semantic output/commit queue together fit below 1 MiB.
                if limits.max_codec_bytes < 1024 * 1024 {
                    return Err(limit());
                }
                Self::Flate {
                    decoder: matches!(kind, Kind::Gzip).then(|| Decompress::new_gzip(15)),
                    gzip: matches!(kind, Kind::Gzip),
                    ended: false,
                    discard: false,
                }
            }
            Kind::Brotli => {
                let budget = Arc::new(Budget {
                    used: AtomicUsize::new(size_of::<Brotli>()),
                    maximum: limits.max_codec_bytes,
                    exceeded: AtomicBool::new(false),
                });
                if size_of::<Brotli>() > limits.max_codec_bytes {
                    return Err(limit());
                }
                let decoder = Box::new(Brotli::new_strict(
                    BoundedAlloc(budget.clone()),
                    BoundedAlloc(budget.clone()),
                    BoundedAlloc(budget.clone()),
                ));
                if budget.exceeded.load(Ordering::Relaxed) {
                    return Err(limit());
                }
                Self::Brotli {
                    decoder,
                    budget,
                    ended: false,
                }
            }
            Kind::Zstd => {
                // Zstd1.5.7 buffered DStream uses window + <= two 128-KiB block
                // buffers + DCtx/tables. Reserve 1 MiB for that bounded overhead.
                let window = limits
                    .max_codec_bytes
                    .checked_sub(1024 * 1024)
                    .filter(|n| *n >= 1024)
                    .ok_or_else(limit)?;
                let log = window.ilog2().min(30);
                let mut decoder = zstd_safe::DCtx::try_create().ok_or_else(limit)?;
                decoder
                    .set_parameter(zstd_safe::DParameter::WindowLogMax(log))
                    .map_err(|_| limit())?;
                Self::Zstd {
                    decoder,
                    ended: false,
                }
            }
        })
    }
    fn decode(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        eof: bool,
        seen: bool,
    ) -> Result<Step, TransportError> {
        match self {
            Self::Flate {
                decoder,
                gzip,
                ended,
                discard,
            } => {
                if *discard {
                    return Ok(Step {
                        read: input.len(),
                        written: 0,
                        needs_input: true,
                    });
                }
                if *ended {
                    if !*gzip || input.first() == Some(&0) {
                        *discard = true;
                        return Ok(Step {
                            read: input.len(),
                            written: 0,
                            needs_input: true,
                        });
                    }
                    if !input.is_empty() {
                        *decoder = Some(Decompress::new_gzip(15));
                        *ended = false;
                    } else {
                        return Ok(Step {
                            read: 0,
                            written: 0,
                            needs_input: true,
                        });
                    }
                }
                if decoder.is_none() {
                    let Some(first) = input.first() else {
                        return Ok(Step {
                            read: 0,
                            written: 0,
                            needs_input: true,
                        });
                    };
                    *decoder = Some(Decompress::new(first & 15 == 8));
                }
                let mut read = 0;
                let mut written = 0;
                loop {
                    let active = decoder.as_mut().expect("initialized decoder");
                    let before_in = active.total_in();
                    let before_out = active.total_out();
                    let status = active
                        .decompress(
                            &input[read..],
                            &mut output[written..],
                            FlushDecompress::Sync,
                        )
                        .map_err(|_| corrupt())?;
                    read += (active.total_in() - before_in) as usize;
                    written += (active.total_out() - before_out) as usize;
                    *ended = status == Status::StreamEnd;
                    // Gunzip validates concatenated members within the current
                    // input/output operation before publishing that operation.
                    // Even with zero output room, the next header/trailer can
                    // fail and invalidate this operation's not-yet-committed bytes.
                    if !*gzip || !*ended || read == input.len() {
                        break;
                    }
                    if input[read] == 0 {
                        *discard = true;
                        read = input.len();
                        break;
                    }
                    *decoder = Some(Decompress::new_gzip(15));
                    *ended = false;
                }
                Ok(Step {
                    read,
                    written,
                    needs_input: written < output.len(),
                })
            }
            Self::Brotli {
                decoder,
                budget,
                ended,
            } => {
                if *ended {
                    return Ok(Step {
                        read: input.len(),
                        written: 0,
                        needs_input: true,
                    });
                }
                let mut available_in = input.len();
                let mut input_offset = 0;
                let mut available_out = output.len();
                let mut output_offset = 0;
                let mut total_out = 0;
                let status = BrotliDecompressStream(
                    &mut available_in,
                    &mut input_offset,
                    input,
                    &mut available_out,
                    &mut output_offset,
                    output,
                    &mut total_out,
                    decoder,
                );
                if budget.exceeded.load(Ordering::Relaxed) {
                    return Err(limit());
                }
                if matches!(status, BrotliResult::ResultFailure) {
                    return Err(corrupt());
                }
                *ended = matches!(status, BrotliResult::ResultSuccess);
                Ok(Step {
                    read: input_offset,
                    written: output_offset,
                    needs_input: !matches!(status, BrotliResult::NeedsMoreOutput),
                })
            }
            Self::Zstd { decoder, ended } => {
                // Node's Zstd transform stops after the first frame, unlike
                // DCtx's default concatenated-frame streaming behavior.
                if *ended {
                    return Ok(Step {
                        read: input.len(),
                        written: 0,
                        needs_input: true,
                    });
                }
                if eof && input.is_empty() && !seen {
                    return Err(corrupt());
                }
                let mut src = zstd_safe::InBuffer::around(input);
                let mut dst = zstd_safe::OutBuffer::around(output);
                let remaining = decoder
                    .decompress_stream(&mut dst, &mut src)
                    .map_err(|code| {
                        if zstd_safe::get_error_name(code).contains("too much memory") {
                            limit()
                        } else {
                            corrupt()
                        }
                    })?;
                let written = dst.pos();
                *ended = remaining == 0;
                if eof && src.pos() == 0 && written == 0 && !*ended && seen {
                    return Err(corrupt());
                }
                Ok(Step {
                    read: src.pos(),
                    written,
                    needs_input: written < dst.capacity(),
                })
            }
        }
    }
}

struct DecodedBody {
    inner: Option<Box<dyn ResponseBody>>,
    codec: Option<Codec>,
    input: Vec<u8>,
    position: usize,
    eof: bool,
    finished: bool,
    seen_input: bool,
    // Node keeps a 16-KiB output allocation across input callbacks. The native
    // write must succeed before any of its output is committed. Public output
    // limits split that committed prefix, never change native write boundaries.
    // This fixed queue belongs to the conservative gzip working-state reserve.
    gzip_output_offset: usize,
    committed: Vec<u8>,
    committed_position: usize,
    limits: FetchContentLimits,
}
impl DecodedBody {
    fn committed_chunk(&mut self) -> Vec<u8> {
        let end = self.committed.len().min(
            self.committed_position
                .saturating_add(self.limits.output_chunk_bytes),
        );
        let chunk = self.committed[self.committed_position..end].to_vec();
        self.committed_position = end;
        if end == self.committed.len() {
            self.committed = Vec::new();
            self.committed_position = 0;
        }
        chunk
    }
}
impl ResponseBody for DecodedBody {
    fn next<'a>(
        &'a mut self,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            let result = async {
                if self.inner.is_none() || self.finished {
                    return Ok(None);
                }
                if !self.committed.is_empty() {
                    return Ok(Some(self.committed_chunk()));
                }
                if self.limits.output_chunk_bytes > isize::MAX as usize {
                    return Err(limit());
                }
                let gzip = self.codec.as_ref().expect("live decoder").is_gzip();
                let output_capacity = if gzip {
                    16_384 - self.gzip_output_offset
                } else {
                    self.limits.output_chunk_bytes
                };
                let mut output = Vec::new();
                output
                    .try_reserve_exact(output_capacity)
                    .map_err(|_| limit())?;
                output.resize(output_capacity, 0);
                loop {
                    if runtime.cancellation.is_cancelled() {
                        return Err(TransportError::new(TransportErrorKind::Cancelled));
                    }
                    // Node's transform publishes EOF when a non-Gunzip codec
                    // leaves trailing input. An exact frame boundary alone does
                    // not end the HTTP body; it still waits for data or wire EOF.
                    if self.position < self.input.len()
                        && self.codec.as_ref().expect("live decoder").ignores_tail()
                    {
                        self.finished = true;
                        return Ok(None);
                    }
                    let step = self.codec.as_mut().expect("live decoder").decode(
                        &self.input[self.position..],
                        &mut output,
                        self.eof,
                        self.seen_input,
                    )?;
                    self.position += step.read;
                    self.finished = self.position < self.input.len()
                        && self.codec.as_ref().expect("live decoder").ignores_tail();
                    if step.written > 0 {
                        output.truncate(step.written);
                        if gzip {
                            self.gzip_output_offset =
                                (self.gzip_output_offset + step.written) % 16_384;
                            if output.len() > self.limits.output_chunk_bytes {
                                self.committed = output;
                                return Ok(Some(self.committed_chunk()));
                            }
                        }
                        return Ok(Some(output));
                    }
                    if self.finished {
                        return Ok(None);
                    }
                    if self.position < self.input.len() && step.read > 0 {
                        continue;
                    }
                    if self.eof {
                        return Ok(None);
                    }
                    if !step.needs_input && step.read == 0 {
                        return Err(corrupt());
                    }
                    let next = crate::policy::cancel_wait(
                        self.inner.as_mut().expect("live body").next(runtime),
                        &runtime.cancellation,
                    )
                    .await?;
                    match next {
                        Some(next) => {
                            let remaining = self.input.len() - self.position;
                            if next.len() > self.limits.max_pending_bytes.saturating_sub(remaining)
                            {
                                return Err(limit());
                            }
                            self.seen_input |= !next.is_empty();
                            if remaining == 0 {
                                self.input = next;
                            } else {
                                self.input.drain(..self.position);
                                self.input
                                    .try_reserve_exact(next.len())
                                    .map_err(|_| limit())?;
                                self.input.extend(next);
                            }
                            self.position = 0;
                        }
                        None => self.eof = true,
                    }
                }
            }
            .await;
            if !matches!(result, Ok(Some(_))) {
                self.close();
            }
            result
        })
    }
    fn close(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            inner.close();
        }
        self.codec = None;
        self.input = Vec::new();
        self.position = 0;
        self.committed = Vec::new();
        self.committed_position = 0;
    }
}
impl Drop for DecodedBody {
    fn drop(&mut self) {
        self.close();
    }
}
