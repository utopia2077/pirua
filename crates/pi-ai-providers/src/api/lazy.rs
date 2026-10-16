//! Typed lazy setup and canonical assistant-stream forwarding.
//!
//! Setup must return a validated child wired to the original request token.
//! Arbitrary JavaScript iterables, optional results and thrown values are outside
//! this projection. Host loaders own module caching and must return promptly;
//! their owned futures perform asynchronous work. No registry or discovery runs.

use pi_ai_core::{
    ApiAdapter, ApiId, AssistantStream, AssistantStreamInit, BoxFuture, CancellationToken,
    CapabilityError, DeferredCancelRequest, DeferredFetchRequest, Model, ModelsError,
    ModelsErrorCode, RequestRuntime, SimpleStreamRequest, StreamSetupFailure, TaskSpawner,
    TextStreamRequest, create_assistant_message_event_stream,
};
use std::{
    fmt,
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
};

/// A trusted module capability; invoke synchronously, then poll owned setup.
pub trait LazyApiLoader: Send + Sync {
    /// Invoked on every operation, including repeated failures. Never blocks.
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ApiAdapter>, ModelsError>>;
}

/// Advertised capabilities, independent of the eventually loaded module.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LazyApiCapabilities {
    pub fetch_deferred: bool,
    pub cancel_deferred: bool,
}

/// Returns immediately after invoking `setup` and admitting its forwarding task.
///
/// During setup, original-token cancellation drops the setup future and settles
/// an empty aborted result. Once polling produces a child, that child owns
/// cancellation, including cancellation observed inside the completing poll.
/// Task rejection/drop produces a safe zero-usage setup failure. Dropping only
/// the consumer does not cancel the detached task, as with the source stream.
/// Panics in trusted setup/load code are not a typed error channel.
pub fn lazy_stream(
    mut init: AssistantStreamInit,
    spawner: &dyn TaskSpawner,
    setup: impl FnOnce() -> BoxFuture<'static, Result<AssistantStream, ModelsError>>,
) -> AssistantStream {
    let original = init.cancellation.clone();
    let private = CancellationToken::new();
    init.cancellation = private.clone();
    let (outer, producer) = create_assistant_message_event_stream(init);
    let mut setup = setup();
    let task_producer = producer.clone();
    let task = Box::pin(async move {
        let mut cancelled = Box::pin(original.cancelled());
        let loaded = poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            // Ready transfers ownership. Do not discard an already-owned child
            // after its original token publishes a usage-only terminal.
            setup.as_mut().poll(cx).map(Some)
        })
        .await;
        drop(setup);
        drop(cancelled);
        let mut child = match loaded {
            None => {
                private.cancel();
                return;
            }
            Some(Err(_)) => {
                let _ = task_producer.fail_setup(StreamSetupFailure::Adapter);
                return;
            }
            Some(Ok(child)) => child,
        };
        while let Some(event) = child.next().await {
            if task_producer.push(event).is_err() {
                return;
            }
        }
        // Canonical terminal publication already fixed exactly the same result;
        // no synthetic event or second end/result settlement is necessary.
    });
    if spawner.spawn(task).is_err() {
        let _ = producer.fail_setup(StreamSetupFailure::Spawn);
    }
    outer
}

/// A lazy adapter with explicit advertised identity and capabilities.
pub struct LazyApi {
    api: ApiId,
    loader: Arc<dyn LazyApiLoader>,
    capabilities: LazyApiCapabilities,
}
impl fmt::Debug for LazyApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LazyApi([REDACTED])")
    }
}
impl LazyApi {
    pub fn capabilities(&self) -> LazyApiCapabilities {
        self.capabilities
    }
}

/// Wraps an explicitly configured host loader without invoking or caching it.
pub fn lazy_api(
    api: ApiId,
    loader: Arc<dyn LazyApiLoader>,
    capabilities: LazyApiCapabilities,
) -> LazyApi {
    LazyApi {
        api,
        loader,
        capabilities,
    }
}
fn init(model: &Model, runtime: &RequestRuntime) -> AssistantStreamInit {
    AssistantStreamInit {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        clock: runtime.clock.clone(),
        cancellation: runtime.cancellation.clone(),
    }
}
impl ApiAdapter for LazyApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        let init = init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        lazy_stream(init, spawner.as_ref(), || {
            let loaded = self.loader.clone().load();
            Box::pin(async move { Ok(loaded.await?.stream(request)) })
        })
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let init = init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        lazy_stream(init, spawner.as_ref(), || {
            let loaded = self.loader.clone().load();
            Box::pin(async move { Ok(loaded.await?.stream_simple(request)) })
        })
    }
    fn fetch_deferred(
        &self,
        request: DeferredFetchRequest,
    ) -> Result<AssistantStream, CapabilityError> {
        if !self.capabilities.fetch_deferred {
            return Err(CapabilityError::Unsupported);
        }
        let init = init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        Ok(lazy_stream(init, spawner.as_ref(), || {
            let loaded = self.loader.clone().load();
            Box::pin(async move {
                loaded
                    .await?
                    .fetch_deferred(request)
                    .map_err(|_| ModelsError::new(ModelsErrorCode::Provider))
            })
        }))
    }
    fn cancel_deferred<'a>(
        &'a self,
        request: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        if !self.capabilities.cancel_deferred {
            return Box::pin(async { Err(ModelsError::new(ModelsErrorCode::Provider)) });
        }
        let loaded = self.loader.clone().load();
        Box::pin(async move { loaded.await?.cancel_deferred(request).await })
    }
}

macro_rules! family {
    ($name:ident, $id:literal) => {
        #[doc = concat!("Creates the pinned `", $id, "` lazy factory with a supplied host loader.")]
        pub fn $name(loader: Arc<dyn LazyApiLoader>) -> LazyApi {
            lazy_api(
                ApiId::new($id).expect("constant API"),
                loader,
                LazyApiCapabilities::default(),
            )
        }
    };
}
family!(anthropic_messages_api, "anthropic-messages");
family!(google_generative_ai_api, "google-generative-ai");
family!(openai_completions_api, "openai-completions");
family!(openai_responses_api, "openai-responses");
