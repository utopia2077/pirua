use std::fmt;

use pi_agent_schema::*;

#[derive(Debug)]
struct ProbeError;

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("probe")
    }
}

impl std::error::Error for ProbeError {}

type DynSession = dyn Session<Metadata = SessionMetadata>;
type DynRepo = dyn SessionRepo<
        Metadata = SessionMetadata,
        CreateOptions = SessionCreateOptions,
        ListOptions = (),
        Session = DynSession,
    >;
type DynSearch = dyn SessionSearchService<Error = ProbeError>;
type DynEffects = dyn Effects<()>;
type DynToolPhases = dyn ToolBatchPhases<(), Binding = (), Error = ProbeError>;
type DynLane = dyn AgentLane;
type DynHarness = dyn AgentHarness<(), Lane = DynLane>;
type DynFactory =
    dyn AgentHarnessFactory<(), (), Session = DynSession, Models = (), Harness = DynHarness>;
type DynStreamAssistant = dyn StreamAssistant<(), ()>;

fn assert_send_sync<T>()
where
    T: ?Sized + Send + Sync,
{
}

#[test]
fn every_dyn_facing_trait_is_object_safe() {
    assert_send_sync::<dyn Storage>();
    assert_send_sync::<DynSession>();
    assert_send_sync::<dyn SessionTree>();
    assert_send_sync::<DynRepo>();
    assert_send_sync::<DynSearch>();
    assert_send_sync::<DynEffects>();
    assert_send_sync::<DynToolPhases>();
    assert_send_sync::<DynLane>();
    assert_send_sync::<DynHarness>();
    assert_send_sync::<DynFactory>();
    assert_send_sync::<DynStreamAssistant>();
    assert_send_sync::<dyn Hooks>();
    assert_send_sync::<dyn Events>();
    assert_send_sync::<dyn WatchHandle<LaneSnapshot>>();
    assert_send_sync::<dyn EventListener>();
    assert_send_sync::<dyn AgentEventSink>();
    assert_send_sync::<dyn Cancellation>();
    assert_send_sync::<dyn ToolProgressSink>();
    assert_send_sync::<dyn ProviderMessageConverter>();
    assert_send_sync::<dyn EntryProjector>();
    assert_send_sync::<dyn AgentHarnessTool<()>>();
    assert_send_sync::<dyn HookHandler<BeforeTool>>();
    let _task: Option<Box<dyn RunWhenIdleTask>> = None;
}

fn assert_hook_spec<S, E, R>()
where
    S: HookSpec<Event = E, Result = R>,
{
}

#[test]
fn all_eleven_hook_spec_associations_are_exact() {
    assert_hook_spec::<BeforeRun, BeforeRunEvent, Option<BeforeRunResult>>();
    assert_hook_spec::<BeforeResume, BeforeResumeEvent, ()>();
    assert_hook_spec::<BeforeRunEnd, BeforeRunEndEvent, Option<BeforeRunEndResult>>();
    assert_hook_spec::<TransformContext, TransformContextEvent, Option<TransformContextResult>>();
    assert_hook_spec::<BeforeRequest, BeforeRequestEvent, Option<BeforeRequestResult>>();
    assert_hook_spec::<BeforePayload, BeforePayloadEvent, Option<BeforePayloadResult>>();
    assert_hook_spec::<AfterResponse, AfterResponseEvent, Option<AfterResponseResult>>();
    assert_hook_spec::<BeforeTool, BeforeToolEvent, Option<BeforeToolResult>>();
    assert_hook_spec::<AfterTool, AfterToolEvent, Option<AfterToolResult>>();
    assert_hook_spec::<BeforeCompaction, BeforeCompactionEvent, Option<BeforeCompactionResult>>();
    assert_hook_spec::<BeforeNavigation, BeforeNavigationEvent, Option<BeforeNavigationResult>>();

    assert_eq!(BeforeRun::NAME, HookName::BeforeRun);
    assert_eq!(BeforeResume::NAME, HookName::BeforeResume);
    assert_eq!(BeforeRunEnd::NAME, HookName::BeforeRunEnd);
    assert_eq!(TransformContext::NAME, HookName::TransformContext);
    assert_eq!(BeforeRequest::NAME, HookName::BeforeRequest);
    assert_eq!(BeforePayload::NAME, HookName::BeforePayload);
    assert_eq!(AfterResponse::NAME, HookName::AfterResponse);
    assert_eq!(BeforeTool::NAME, HookName::BeforeTool);
    assert_eq!(AfterTool::NAME, HookName::AfterTool);
    assert_eq!(BeforeCompaction::NAME, HookName::BeforeCompaction);
    assert_eq!(BeforeNavigation::NAME, HookName::BeforeNavigation);
}
