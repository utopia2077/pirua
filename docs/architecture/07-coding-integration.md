# Headless Coding integration

CodingSession composes a standalone Agent, one SessionManager, selected tools,
model/runtime services, settings, resources and SessionHooks. Public values are
detached; asynchronous callbacks do not borrow locked session state. The canonical
session manager is the sole persistence authority.

The session handles prompt/continue, steering/follow-up queues, model and thinking
selection, context projection, explicit/automatic compaction, branch summaries,
fork/navigation, retry, usage and cache warming. Hooks participate through explicit
prepare/input/context/message/tool/boundary/compaction/navigation/event seams.
Invalid typed drafts must fail before records are committed.

The common tools use an explicit ExecutionToolContext and ExecutionEnv. Filesystem
mutation ordering, output truncation/spill, process cancellation and resource
loading live below session orchestration so they can be reused by Durable.

MCP tool attachment uses an owner-scoped registry batch: collisions with another
owner reject the replacement, refresh replaces only that source, and retirement
removes only its tools. Nested MCP/CodeMode calls pass through Agent tool hooks.
The host initializes clients and supplies OAuth/network policy explicitly.

CodeMode uses the shared local worker, visible tool declarations and normalized
names. Structured results and nested images are preserved. Only successful scripts
commit store deltas, atomically, to the same session/navigation generation that
started them. Navigating away and back invalidates an old run's cursor. Same-branch
appends do not invalidate it. The adapter does not write Durable journals.

UI, CLI package management, JS extension loading, JSONL and old-data migration are
excluded. Registry construction, execution environment and credentials remain
explicit host choices. See [scope](../scope.md) and the crate documentation.
