import type { SessionSettlement, SessionEntry, SessionTreeNode, AgentMessage } from "../display/types.ts";
import type { PiSnapshot, NativeEntry, History } from "../acp/types.ts";
import { PROJECTION, RENDER, zeroUsage } from "../acp/types.ts";
import type { RecordValue } from "../acp/client.ts";
import type { AgentSessionEvent, AssistantMessage } from "../display/types.ts";

export function displayEntry(entry: NativeEntry): SessionEntry {
	return { ...structuredClone(entry), timestamp: new Date(entry.timestamp).toISOString() } as SessionEntry;
}
export class ReadonlySessionMirror {
	readonly snapshot: () => PiSnapshot;
	constructor(snapshot: () => PiSnapshot) {
		this.snapshot = snapshot;
	}
	getEntryCount(): number {
		return this.snapshot().history.entries.length;
	}
	getCwd(): string {
		return this.snapshot().history.header.cwd;
	}
	getSessionId(): string {
		return this.snapshot().sessionId;
	}
	getSessionName(): string | undefined {
		let name: string | undefined;
		for (const entry of this.snapshot().history.entries) if (entry.type === "session_info") name = entry.name;
		return name;
	}
	getHeader(): RecordValue {
		const h = this.snapshot().history.header;
		return { ...h, type: "session", timestamp: new Date(h.timestamp).toISOString() };
	}
	getLeafId(): string | null {
		return this.snapshot().history.leafId;
	}
	getEntries(): SessionEntry[] {
		return this.snapshot().history.entries.map(displayEntry);
	}
	getEntry(id: string): SessionEntry | undefined {
		const e = this.snapshot().history.entries.find((e) => e.id === id);
		return e ? displayEntry(e) : undefined;
	}
	getBranch(): SessionEntry[] {
		const entries = new Map(this.getEntries().map((e) => [e.id, e]));
		const out: SessionEntry[] = [];
		const seen = new Set();
		let id = this.getLeafId();
		while (id && !seen.has(id)) {
			seen.add(id);
			const entry = entries.get(id);
			if (!entry) break;
			out.unshift(entry);
			id = entry.parentId;
		}
		return out;
	}
	buildContextEntries(): SessionEntry[] {
		return this.snapshot().contextEntries.map((entry) => displayEntry(entry.sourceEntry));
	}
	getTree(): SessionTreeNode[] {
		const nodes = new Map(this.getEntries().map((entry) => [entry.id, { entry, children: [] } as SessionTreeNode]));
		const roots: SessionTreeNode[] = [];
		for (const node of nodes.values()) {
			const parent = node.entry.parentId ? nodes.get(node.entry.parentId) : undefined;
			if (parent) parent.children.push(node);
			else roots.push(node);
			if (node.entry.type === "label") {
				const target = nodes.get(node.entry.targetId);
				if (target) {
					target.label = node.entry.label;
					target.labelTimestamp = node.entry.timestamp;
				}
			}
		}
		return roots;
	}
}

function patchMetadata(previous: RecordValue | undefined, patch: RecordValue | null): RecordValue | undefined {
	if (patch === null) return undefined;
	const next = { ...previous };
	for (const [key, value] of Object.entries(patch)) {
		if (value === null) delete next[key];
		else next[key] = structuredClone(value);
	}
	return next;
}
export class SessionProjection {
	value: PiSnapshot;
	readonly entities = new Map<string, RecordValue>();
	readonly assistants = new Map<string, AssistantMessage>();
	private ended = new Set<string>();
	private predecessors = new Map<string, SessionEntry[]>();
	private lastSettlement: SessionSettlement | undefined;
	private toolsStarted = new Set<string>();
	private toolsEnded = new Set<string>();
	private listeners = new Set<(event: AgentSessionEvent) => void>();
	onResync: ((reason: string) => void) | undefined;
	onTerminal: ((terminal: RecordValue) => void) | undefined;
	private hydrating = false;
	private buffered: { method: string; params: RecordValue }[] = [];
	constructor(snapshot: PiSnapshot) {
		this.value = structuredClone(snapshot);
		this.install(snapshot);
	}
	subscribe(listener: (event: AgentSessionEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}
	private emit(event: AgentSessionEvent): void {
		for (const listener of this.listeners) listener(structuredClone(event));
	}
	beginHydration(): void {
		this.hydrating = true;
	}
	install(snapshot: PiSnapshot): void {
		if (
			!snapshot ||
			typeof snapshot.sessionId !== "string" ||
			!Array.isArray(snapshot.liveEntities) ||
			!Array.isArray(snapshot.history?.entries)
		)
			throw new Error("Invalid Pi snapshot");
		const previousState = this.value.state;
		const settlement = snapshot.state === "idle" ? this.snapshotSettlement(snapshot) : undefined;
		this.lastSettlement = settlement;
		const previousEpoch = this.value.epoch;
		this.value = structuredClone(snapshot);
		if (previousEpoch !== snapshot.epoch) this.predecessors.clear();
		this.entities.clear();
		this.assistants.clear();
		this.ended.clear();
		this.toolsStarted.clear();
		this.toolsEnded.clear();
		for (const entity of snapshot.liveEntities) this.update(entity, false);
		this.emit({
			type: "snapshot_reconciled",
			settlement,
			entries: snapshot.contextEntries.map((e) => displayEntry(e.sourceEntry)),
			activeEvents: this.replayActive(),
			state: snapshot.state,
			activity: structuredClone(snapshot.activity),
		});
		if (previousState !== "idle" && settlement) this.emit({ type: "agent_settled", ...settlement });
		this.hydrating = false;
		const buffered = this.buffered;
		this.buffered = [];
		for (const item of buffered) this.receive(item.method, item.params);
	}
	private stateSettlement(update: RecordValue | undefined): SessionSettlement | undefined {
		if (update?.state !== "idle") return;
		if (update.stopReason === "cancelled") return { aborted: true };
		if (update.stopReason === "error")
			return { aborted: false, errorMessage: update.error?.message ?? "Native session failed" };
		if (update.stopReason === "end_turn" || update.stopReason === "max_tokens") return { aborted: false };
	}

	private snapshotSettlement(snapshot: PiSnapshot): SessionSettlement | undefined {
		// Covered standard state updates carry outcomes for shell/compaction cancellation too.
		const terminal = [...this.buffered]
			.reverse()
			.find(
				({ method, params }) =>
					method === "session/update" &&
					params.sessionId === snapshot.sessionId &&
					params._meta?.[PROJECTION]?.epoch === snapshot.epoch &&
					params._meta[PROJECTION].sequence <= snapshot.sequence &&
					params.update?.sessionUpdate === "state_update",
			)?.params.update;
		const expressed = this.stateSettlement(terminal);
		if (expressed) return expressed;
		// A repeated idle snapshot retains its known outcome until another run/activation.
		const retained =
			this.value.state === "idle" && this.value.epoch === snapshot.epoch ? this.lastSettlement : undefined;
		// Only a newly committed canonical entry can describe the current operation.
		const previousIds = new Set(this.value.history.entries.map((entry) => entry.id));
		for (const { sourceEntry: entry } of [...snapshot.contextEntries].reverse()) {
			if (entry.type === "compaction" || entry.type === "branch_summary") return;
			if (entry.type !== "message") continue;
			if (previousIds.has(entry.id)) return retained;
			const message = entry.message;
			if (message.role === "toolResult") continue;
			if (message.role === "assistant") {
				if (message.stopReason === "aborted") return { aborted: true };
				if (message.stopReason === "error") return { aborted: false, errorMessage: message.errorMessage ?? "Error" };
				if (message.stopReason === "stop" || message.stopReason === "length") return { aborted: false };
				return;
			}
			if (message.role === "bashExecution" && typeof message.cancelled === "boolean")
				return { aborted: message.cancelled };
			if (message.role === "user") return;
		}
		return retained;
	}

	replayActive(): AgentSessionEvent[] {
		const events: AgentSessionEvent[] = [];
		for (const [id, message] of this.assistants)
			if (!this.ended.has(id)) {
				events.push(
					{ type: "message_start", message: structuredClone(message) },
					{ type: "message_update", message: structuredClone(message) },
				);
			}
		for (const entity of this.entities.values())
			if (entity.sessionUpdate === "tool_call_update") {
				const meta = entity._meta?.[RENDER] ?? {};
				const id = meta.toolCallId ?? entity.toolCallId;
				if (entity.status === "in_progress")
					events.push({
						type: "tool_execution_start",
						toolCallId: id,
						toolName: entity.name ?? entity.title ?? "tool",
						args: entity.rawInput ?? {},
					});
				if (entity.rawOutput && entity.status === "in_progress")
					events.push({
						type: "tool_execution_update",
						toolCallId: id,
						toolName: entity.name ?? "tool",
						partialResult: entity.rawOutput,
					});
			}
		return events;
	}
	receive(method: string, params: RecordValue): void {
		if (method !== "session/update" && method !== "_pirua/session/changed") return;
		if (this.hydrating) {
			this.buffered.push({ method, params: structuredClone(params) });
			return;
		}
		if (params.sessionId !== this.value.sessionId) return;
		const cursor = method === "session/update" ? params._meta?.[PROJECTION] : params;
		if (!cursor || cursor.epoch !== this.value.epoch) {
			this.onResync?.("Activation epoch changed");
			return;
		}
		if (!Number.isSafeInteger(cursor.sequence)) throw new Error("Invalid projection sequence");
		if (cursor.sequence <= this.value.sequence) return;
		if (cursor.sequence !== this.value.sequence + 1) {
			this.beginHydration();
			this.buffered.push({ method, params: structuredClone(params) });
			this.onResync?.("Projection sequence gap");
			return;
		}
		if (method === "session/update") this.update(params.update, true);
		else if (!this.change(params)) return;
		this.value.sequence = cursor.sequence;
	}
	private change(params: RecordValue): boolean {
		if (params.historyChange) {
			const change = params.historyChange;
			if (change.kind === "replace") this.value.history = structuredClone(change.history);
			else if (change.kind === "append") {
				if (change.baseRevision !== this.value.history.revision) {
					this.beginHydration();
					this.onResync?.("History revision mismatch");
					return false;
				}
				this.value.history = {
					...this.value.history,
					entries: [...this.value.history.entries, ...structuredClone(change.appended)],
					revision: change.revision,
					leafId: change.leafId,
				};
				for (const entry of change.appended) this.emit({ type: "entry_appended", entry: displayEntry(entry) });
			} else throw new Error("Invalid history change");
		}
		const before = this.value;
		const next: PiSnapshot = { ...this.value, ...structuredClone(params.changes ?? {}) };
		this.value = next;
		if (params.changes?.queue)
			this.emit({
				type: "queue_update",
				steering: next.queue.steering.map((q) => q.text),
				followUp: next.queue.followUp.map((q) => q.text),
			});
		if (params.changes?.thinkingLevel) this.emit({ type: "thinking_level_changed", level: next.thinkingLevel });
		if (params.changes?.activity) {
			if (!before.activity.compacting && next.activity.compacting)
				this.emit({ type: "compaction_start", reason: "manual" });
			if (before.activity.compacting && !next.activity.compacting) {
				const entry = [...next.history.entries].reverse().find((e) => e.type === "compaction");
				this.emit({ type: "compaction_end", reason: "manual", result: entry as any, aborted: false, willRetry: false });
			}
			if (!before.activity.retrying && next.activity.retrying)
				this.emit({
					type: "auto_retry_start",
					attempt: next.activity.retryAttempt,
					maxAttempts: next.settings.retry?.maxRetries ?? 3,
					delayMs: next.settings.retry?.baseDelayMs ?? 0,
					errorMessage: "Retrying",
				});
			if (before.activity.retrying && !next.activity.retrying)
				this.emit({ type: "auto_retry_end", success: next.state !== "idle", attempt: next.activity.retryAttempt });
		}
		return true;
	}
	private update(update: RecordValue, emit: boolean): void {
		if (!update || typeof update.sessionUpdate !== "string") throw new Error("Invalid ACP session update");
		const kind = update.sessionUpdate;
		if (kind === "state_update") {
			if (!["idle", "running", "requires_action"].includes(update.state)) return;
			const previous = this.value.state;
			this.value.state = update.state;
			this.lastSettlement = update.state === "idle" ? this.stateSettlement(update) : undefined;
			if (emit && previous === "idle" && update.state !== "idle") {
				this.emit({ type: "agent_start" });
				this.emit({ type: "turn_start" });
			}
			if (emit && previous !== "idle" && update.state === "idle") {
				for (const [id, message] of this.assistants)
					if (!this.ended.has(id)) {
						this.ended.add(id);
						if (update.stopReason === "cancelled") message.stopReason = "aborted";
						this.emit({ type: "message_end", message, previousEntries: structuredClone(this.predecessors.get(id)) });
						this.predecessors.delete(id);
					}
				this.emit({ type: "agent_end", messages: [], willRetry: false });
				this.emit({
					type: "agent_settled",
					aborted: update.stopReason === "cancelled",
					errorMessage: this.lastSettlement?.errorMessage,
				});
			}
			return;
		}
		if (kind === "config_options_update") {
			this.value.configOptions = structuredClone(update.configOptions ?? []);
			return;
		}
		if (kind === "session_info_update") {
			if (emit) this.emit({ type: "session_info_changed", name: update.title });
			return;
		}
		const chunk = kind.endsWith("_chunk");
		const wholeKind = kind === "terminal_output_chunk" ? "terminal_update" : chunk ? kind.slice(0, -6) : kind;
		const identity = update.messageId ?? update.toolCallId ?? update.terminalId;
		if (typeof identity !== "string") return;
		const key = wholeKind + ":" + identity;
		const previous = this.entities.get(key) ?? { sessionUpdate: wholeKind };
		const entity = { ...previous };
		for (const [field, value] of Object.entries(update)) {
			if (field === "sessionUpdate") continue;
			if (field === "_meta") {
				if (!chunk) entity._meta = patchMetadata(entity._meta, value as RecordValue | null);
			} else if (field === "data" && kind === "terminal_output_chunk") {
				const earlier = Buffer.from(entity.output?.data ?? "", "base64"),
					bytes = Buffer.from(String(value), "base64");
				entity.output = { ...entity.output, data: Buffer.concat([earlier, bytes]).toString("base64") };
			} else if (field === "content" && chunk) entity.content = [...(entity.content ?? []), structuredClone(value)];
			else entity[field] = structuredClone(value);
		}
		this.entities.set(key, entity);
		const storedMeta = entity._meta?.[RENDER] ?? {};
		const chunkMeta = update._meta?.[RENDER] ?? {};
		const meta = chunk ? { ...storedMeta, ...chunkMeta, message: chunkMeta.message } : storedMeta;
		if (wholeKind === "user_message") {
			if (emit && !previous.messageId)
				this.emit({
					type: "message_start",
					message: meta.message ?? { role: "user", content: entity.content ?? [], timestamp: Date.now() },
				});
			return;
		}
		if (wholeKind === "agent_message" || wholeKind === "agent_thought") {
			const nativeId = meta.nativeMessageId ?? identity;
			let message = this.assistants.get(nativeId);
			const first = !message;
			if (first && emit && meta.phase !== "end")
				this.predecessors.set(nativeId, new ReadonlySessionMirror(() => this.value).getBranch());
			if (!message) {
				message = {
					role: "assistant",
					content: [],
					api: this.value.model?.api ?? "unknown",
					provider: this.value.model?.provider ?? "unknown",
					model: this.value.model?.id ?? "unknown",
					usage: zeroUsage(),
					timestamp: Date.now(),
					stopReason: "stop",
				};
				this.assistants.set(nativeId, message);
			}
			if (meta.message?.role === "assistant") {
				message = structuredClone(meta.message) as AssistantMessage;
				this.assistants.set(nativeId, message);
			} else {
				const index = Number.isSafeInteger(meta.contentIndex)
					? meta.contentIndex
					: wholeKind === "agent_thought"
						? 0
						: message.content.findIndex((c) => c.type === "text");
				const position = index < 0 ? message.content.length : index;
				const prior = message.content[position];
				const delta = update.content?.type === "text" ? update.content.text : "";
				const text = chunk
					? (wholeKind === "agent_thought" && prior?.type === "thinking"
							? prior.thinking
							: wholeKind === "agent_message" && prior?.type === "text"
								? prior.text
								: "") + delta
					: (entity.content ?? [])
							.filter((c: RecordValue) => c?.type === "text")
							.map((c: RecordValue) => c.text)
							.join("");
				message.content[position] =
					wholeKind === "agent_thought" ? { type: "thinking", thinking: text } : { type: "text", text };
			}
			message.content = message.content.filter(Boolean);
			if (emit && first) this.emit({ type: "message_start", message });
			if (meta.phase === "end") {
				if (!this.ended.has(nativeId)) {
					this.ended.add(nativeId);
					if (emit)
						this.emit({
							type: "message_end",
							message,
							previousEntries: structuredClone(this.predecessors.get(nativeId)),
						});
					this.predecessors.delete(nativeId);
				}
			} else if (emit)
				this.emit({ type: "message_update", message, assistantMessageEvent: { type: meta.deltaType ?? "text_delta" } });
			return;
		}
		if (wholeKind === "tool_call_update") {
			const callId = meta.toolCallId ?? identity,
				name = entity.name ?? entity.title ?? "tool",
				args = entity.rawInput ?? {};
			const parent = meta.parentMessageId;
			if (parent) {
				const assistant = this.assistants.get(parent);
				if (assistant) {
					const existing = assistant.content.findIndex((c) => c.type === "toolCall" && c.id === callId);
					const tool = { type: "toolCall" as const, id: callId, name, arguments: args };
					if (existing >= 0) assistant.content[existing] = tool;
					else assistant.content.push(tool);
					if (emit) this.emit({ type: "message_update", message: assistant });
				}
			}
			if (!emit) return;
			if (entity.status === "in_progress" && !this.toolsStarted.has(identity)) {
				this.toolsStarted.add(identity);
				this.emit({ type: "tool_execution_start", toolCallId: callId, toolName: name, args });
			}
			const result = entity.rawOutput ?? {
				content: (entity.content ?? []).flatMap((c: RecordValue) => (c.type === "content" ? [c.content] : [])),
				details: undefined,
			};
			if (entity.status === "completed" || entity.status === "failed") {
				if (!this.toolsEnded.has(identity)) {
					this.toolsEnded.add(identity);
					this.emit({
						type: "tool_execution_end",
						toolCallId: callId,
						toolName: name,
						result,
						isError: entity.status === "failed",
						durationMs: meta.durationMs,
					});
				}
			} else if (entity.rawOutput)
				this.emit({ type: "tool_execution_update", toolCallId: callId, toolName: name, partialResult: result });
			return;
		}
		if (wholeKind === "terminal_update" || wholeKind === "terminal_output") {
			if (emit) this.onTerminal?.(entity);
		}
	}
}
