import { StringDecoder } from "node:string_decoder";
import { RpcError, type RecordValue } from "../acp/client.ts";
import type { PiSnapshot, SessionChange, QueuedInput } from "../acp/types.ts";
import type { TuiRuntimeHost } from "./host.ts";
import { SessionProjection, ReadonlySessionMirror } from "./mirror.ts";
import { ModelRuntime } from "./model-catalog.ts";
import { ResourceView } from "./resources.ts";
import type {
	AgentMessage,
	AgentSessionEvent,
	Model,
	ThinkingLevel,
	ImageContent,
	CompactionResult,
	SessionStats,
} from "../display/types.ts";
import { zeroUsage } from "../acp/types.ts";
import { exportSessionToHtml } from "../core/export-html/index.ts";

export class TuiSession {
	readonly host: TuiRuntimeHost;
	readonly projection: SessionProjection;
	readonly sessionManager: ReadonlySessionMirror;
	readonly modelRuntime: ModelRuntime;
	readonly resourceLoader: ResourceView;
	private events = Promise.resolve();
	private errors = new Set<(error: Error) => void>();
	private listeners = new Set<(event: AgentSessionEvent) => Promise<void> | void>();
	private unsub: () => void;
	private bashController: AbortController | undefined;
	private bashChunks: ((s: string) => void) | undefined;
	private terminalOffsets = new Map<string, number>();
	private terminalDecoders = new Map<string, StringDecoder>();
	private closed = false;
	private snapshotRequests = 0;
	private resyncing = false;
	private wakeResync: (() => void) | undefined;
	restoredImages: ImageContent[] = [];
	constructor(host: TuiRuntimeHost, snapshot: PiSnapshot) {
		this.host = host;
		this.projection = new SessionProjection(snapshot);
		this.sessionManager = new ReadonlySessionMirror(() => this.projection.value);
		this.modelRuntime = new ModelRuntime(host);
		this.resourceLoader = new ResourceView(() => this.projection.value);
		host.options.settings.setBackend(snapshot.settings);
		host.adoptAuthMethods(snapshot);
		this.unsub = this.projection.subscribe((event) => this.emit(event));
		this.projection.onResync = () => void this.resync();
		this.projection.onSessionError = (message) => this.enqueue(() => this.fail(new Error(message)));
		this.projection.onTerminal = (terminal) => {
			const output = terminal.output;
			const data = typeof output === "string" ? output : (output?.data ?? "");
			const bytes = Buffer.from(data, "base64");
			const previous = this.terminalOffsets.get(terminal.terminalId) ?? 0;
			if (bytes.length > previous) {
				let decoder = this.terminalDecoders.get(terminal.terminalId);
				if (!decoder) {
					decoder = new StringDecoder("utf8");
					this.terminalDecoders.set(terminal.terminalId, decoder);
				}
				const text = decoder.write(bytes.subarray(previous));
				if (text) this.bashChunks?.(text);
			}
			this.terminalOffsets.set(terminal.terminalId, bytes.length);
		};
	}
	get sessionId(): string {
		return this.projection.value.sessionId;
	}
	get settingsManager() {
		return this.host.options.settings;
	}
	get presentation() {
		return this.host.presentation;
	}
	get model() {
		return this.projection.value.model ?? undefined;
	}
	get routedModel(): { model: Model; reasoningLevel?: ThinkingLevel; thinkingLevel?: ThinkingLevel } | undefined {
		const r = this.projection.value.routedModel;
		return r ? { model: r.model ?? r, thinkingLevel: r.thinkingLevel } : undefined;
	}
	get thinkingLevel(): ThinkingLevel {
		return this.projection.value.thinkingLevel;
	}
	get state() {
		return {
			messages: this.messages,
			model: this.model,
			thinkingLevel: this.thinkingLevel,
			systemPrompt: this.systemPrompt,
			tools: this.projection.value.tools.map((t) => t.declaration ?? t),
			isStreaming: this.isStreaming,
		};
	}
	get messages(): AgentMessage[] {
		return structuredClone(this.projection.value.messages);
	}
	get isStreaming(): boolean {
		return this.projection.value.state !== "idle" && !this.isCompacting;
	}
	get isIdle(): boolean {
		return this.projection.value.state === "idle";
	}
	get isCompacting(): boolean {
		return this.projection.value.activity.compacting;
	}
	get isBashRunning(): boolean {
		return this.projection.value.activity.bashRunning || !!this.bashController;
	}
	get pendingMessageCount(): number {
		return this.projection.value.queue.steering.length + this.projection.value.queue.followUp.length;
	}
	get retryAttempt(): number {
		return this.projection.value.activity.retryAttempt;
	}
	get autoCompactionEnabled(): boolean {
		return this.projection.value.settings.compaction?.enabled ?? true;
	}
	get steeringMode(): "all" | "one-at-a-time" {
		return this.projection.value.settings.steeringMode ?? "one-at-a-time";
	}
	get followUpMode(): "all" | "one-at-a-time" {
		return this.projection.value.settings.followUpMode ?? "one-at-a-time";
	}
	get scopedModels(): { model: Model; thinkingLevel?: ThinkingLevel }[] {
		return this.projection.value.scopedModels.flatMap((item) => {
			const model = item.model ?? this.modelRuntime.getModel(item.provider, item.modelId);
			return model ? [{ model, thinkingLevel: item.thinkingLevel }] : [];
		});
	}
	get promptTemplates(): any[] {
		return structuredClone(this.projection.value.resources?.promptTemplates ?? []);
	}
	get skills(): any[] {
		return structuredClone(this.projection.value.resources?.skills ?? []);
	}
	get systemPrompt(): string {
		return this.projection.value.resources?.systemPrompt ?? "";
	}
	get cacheWarmingStatus(): import("../ui/formatting.ts").CacheWarmingStatus | undefined {
		return this.projection.value.resources?.cacheWarmingStatus;
	}
	getContextUsage() {
		return this.projection.value.contextUsage ?? undefined;
	}
	getAvailableThinkingLevels(): ThinkingLevel[] {
		return [...this.projection.value.availableThinkingLevels];
	}
	getSteeringMessages(): string[] {
		return this.projection.value.queue.steering.map((q) => q.text);
	}
	getFollowUpMessages(): string[] {
		return this.projection.value.queue.followUp.map((q) => q.text);
	}
	getToolDefinition(_name: string): undefined {
		return undefined;
	}
	onError(fn: (error: Error) => void): () => void {
		this.errors.add(fn);
		return () => this.errors.delete(fn);
	}
	private fail(error: unknown): void {
		const e = error instanceof Error ? error : new Error(String(error));
		for (const listener of this.errors) listener(e);
	}
	// Runs after everything already queued, so it reaches the UI behind the events it follows.
	private enqueue(task: () => Promise<void> | void): void {
		this.events = this.events
			.then(async () => {
				if (!this.closed) await task();
			})
			.catch((error) => this.fail(error));
	}
	private emit(event: AgentSessionEvent): void {
		this.enqueue(async () => {
			for (const listener of this.listeners) await listener(structuredClone(event));
		});
	}
	subscribe(listener: (event: AgentSessionEvent) => Promise<void> | void): () => void {
		this.listeners.add(listener);
		for (const event of this.projection.replayActive()) this.emit(event);
		return () => this.listeners.delete(listener);
	}
	receive(method: string, params: RecordValue): void {
		try {
			this.projection.receive(method, params);
			this.settingsManager.setBackend(this.projection.value.settings);
		} catch (error) {
			this.fail(error);
		}
	}
	// Installs a full snapshot, which also brings the host's authentication methods up to date.
	private install(snapshot: PiSnapshot): void {
		this.projection.install(snapshot);
		this.host.adoptAuthMethods(snapshot);
	}
	async refresh(signal?: AbortSignal): Promise<void> {
		this.projection.beginHydration();
		this.snapshotRequests++;
		try {
			const snapshot = await this.host.client
				.request<PiSnapshot>("_pirua/session/snapshot", { sessionId: this.sessionId }, signal)
				.finally(() => this.snapshotRequests--);
			this.install(snapshot);
		} catch (error) {
			// Another pending snapshot still covers the buffered updates; otherwise apply them now.
			if (!this.snapshotRequests) this.projection.abandonHydration();
			throw error;
		} finally {
			// The snapshot or the released updates may have changed the backend settings.
			this.settingsManager.setBackend(this.projection.value.settings);
		}
	}
	// One resynchronization at a time: immediate, then 250 ms doubling to 5 s while still stale,
	// reporting only the first failure of the episode.
	private async resync(): Promise<void> {
		if (this.resyncing) return;
		this.resyncing = true;
		let delay = 0,
			reported = false;
		try {
			while (!this.closed && this.projection.stale) {
				try {
					await this.refresh();
				} catch (error) {
					if (!reported && !this.closed) this.fail(error);
					reported = true;
				}
				if (this.closed || !this.projection.stale) break;
				delay = Math.min(delay * 2 || 250, 5000);
				await new Promise<void>((resolve) => {
					const timer = setTimeout(resolve, delay);
					this.wakeResync = () => {
						clearTimeout(timer);
						resolve();
					};
				});
				this.wakeResync = undefined;
			}
		} finally {
			this.resyncing = false;
		}
	}
	async prompt(
		text: string,
		options: { images?: ImageContent[]; streamingBehavior?: "steer" | "followUp"; [key: string]: unknown } = {},
	): Promise<RecordValue> {
		const images = [...(options.images ?? []), ...this.restoredImages];
		if (!this.isIdle) {
			if (!options.streamingBehavior)
				throw new RpcError(-32050, "Session is busy; choose steering or follow-up", { kind: "busy" });
			const result = await this.queue(text, options.streamingBehavior, images);
			this.restoredImages = [];
			return result;
		}
		const result = await this.host.client.request("session/prompt", {
			sessionId: this.sessionId,
			prompt: this.content(text, images),
		});
		this.restoredImages = [];
		return result;
	}
	private content(text: string, images: ImageContent[]): RecordValue[] {
		if (images.length && !this.host.capabilities.session?.prompt?.image)
			throw new RpcError(-32050, "Rust host does not advertise image input", { kind: "unsupported" });
		return [{ type: "text", text }, ...images.map((i) => ({ type: "image", data: i.data, mimeType: i.mimeType }))];
	}
	private async queue(text: string, delivery: "steer" | "followUp", images: ImageContent[] = []): Promise<RecordValue> {
		this.host.require("queue");
		const admission = await this.host.client.request("_pirua/session/queue", {
			sessionId: this.sessionId,
			delivery,
			prompt: this.content(text, images),
		});
		await this.refresh();
		return admission;
	}
	steer(text: string): Promise<RecordValue> {
		return this.queue(text, "steer");
	}
	followUp(text: string): Promise<RecordValue> {
		return this.queue(text, "followUp");
	}
	async clearQueue(): Promise<{ steering: string[]; followUp: string[]; removed: QueuedInput[] }> {
		this.host.require("queue");
		const result = await this.host.client.request<{ removed: QueuedInput[] }>("_pirua/session/clear_queue", {
			sessionId: this.sessionId,
		});
		this.restoredImages.push(...result.removed.flatMap((i) => i.images ?? []));
		void this.refresh().catch((error) => this.fail(error));
		return {
			steering: result.removed.filter((i) => i.delivery === "steer").map((i) => i.text),
			followUp: result.removed.filter((i) => i.delivery === "followUp").map((i) => i.text),
			removed: result.removed,
		};
	}
	async abort(): Promise<void> {
		this.host.client.notify("session/cancel", { sessionId: this.sessionId });
		await this.waitForIdle();
	}
	abortRetry(): Promise<void> {
		return this.abort();
	}
	abortCompaction(): Promise<void> {
		return this.abort();
	}
	abortBranchSummary(): Promise<void> {
		return this.abort();
	}
	async waitForIdle(): Promise<void> {
		if (this.isIdle) return;
		await new Promise<void>((resolve, reject) => {
			const unsubscribe = this.projection.subscribe((event) => {
				if (event.type === "agent_settled" || (event.type === "snapshot_reconciled" && event.state === "idle")) {
					unsubscribe();
					off();
					resolve();
				}
			});
			const off = this.host.client.onFailure((error) => {
				unsubscribe();
				off();
				reject(error);
			});
		});
	}
	async compact(instructions?: string): Promise<CompactionResult | undefined> {
		let result: CompactionResult | undefined;
		const unsubscribe = this.projection.subscribe((event) => {
			if (event.type === "compaction_end") result = event.result;
		});
		try {
			await this.prompt("/compact" + (instructions ? " " + instructions : ""));
			await this.waitForIdle();
			await this.refresh();
			return result;
		} finally {
			unsubscribe();
		}
	}
	async navigateTree(targetId: string, input: RecordValue = {}): Promise<SessionChange> {
		this.host.require("tree");
		const change = await this.host.client.request<SessionChange>("_pirua/session/navigate", {
			sessionId: this.sessionId,
			targetId,
			expectedRevision: this.projection.value.history.revision,
			...input,
		});
		return this.host.applySessionChange(change);
	}
	async setLabel(entryId: string, label?: string): Promise<void> {
		this.host.require("tree");
		const result = await this.host.client.request<{ snapshot: PiSnapshot }>("_pirua/session/label", {
			sessionId: this.sessionId,
			entryId,
			label: label ?? null,
			expectedRevision: this.projection.value.history.revision,
		});
		this.install(result.snapshot);
	}
	async setSessionName(name: string): Promise<void> {
		await this.host.renameSession(this.sessionId, name);
		await this.refresh();
	}
	async setConfig(configId: string, value: unknown): Promise<void> {
		const option = this.projection.value.configOptions.find((o) => o.configId === configId);
		if (!option)
			throw new RpcError(-32050, `Rust host does not advertise configuration '${configId}'`, { kind: "unsupported" });
		await this.host.client.request("session/set_config_option", {
			sessionId: this.sessionId,
			configId,
			type: option.type === "boolean" ? "boolean" : "id",
			value,
		});
		await this.refresh();
	}
	setAutoCompactionEnabled(value: boolean): Promise<void> {
		return this.setConfig("autoCompaction", value);
	}
	setSteeringMode(value: string): Promise<void> {
		return this.setConfig("steeringMode", value);
	}
	setFollowUpMode(value: string): Promise<void> {
		return this.setConfig("followUpMode", value);
	}
	setCacheWarmingMode(value: string): Promise<void> {
		return this.setConfig("cacheWarming", value);
	}
	setThinkingLevel(level: ThinkingLevel, options?: { persist?: boolean }): Promise<void> {
		if (options?.persist)
			return Promise.reject(
				new RpcError(-32050, "The host does not advertise persistent thinking defaults", { kind: "unsupported" }),
			);
		return this.setConfig("thinkingLevel", level);
	}
	setModel(model: Model, options?: { persist?: boolean }): Promise<void> {
		if (options?.persist)
			return Promise.reject(
				new RpcError(-32050, "The host does not advertise persistent model defaults", { kind: "unsupported" }),
			);
		return this.setConfig("model", `${model.provider}/${model.id}`);
	}
	async cycleThinkingLevel(): Promise<ThinkingLevel> {
		const choices = this.getAvailableThinkingLevels();
		const level = choices[(choices.indexOf(this.thinkingLevel) + 1) % choices.length];
		if (!level) throw new Error("Host has no thinking choices");
		await this.setThinkingLevel(level);
		return level;
	}
	async cycleModel(
		direction: "forward" | "backward" = "forward",
	): Promise<{ model: Model; thinkingLevel: ThinkingLevel } | undefined> {
		const models = this.scopedModels.length
			? this.scopedModels
			: this.modelRuntime.getAvailableSnapshot().map((model) => ({ model, thinkingLevel: undefined }));
		if (models.length < 2) return;
		const index = models.findIndex((m) => m.model.id === this.model?.id && m.model.provider === this.model?.provider),
			next = models[(index + (direction === "backward" ? -1 : 1) + models.length) % models.length];
		await this.setModel(next.model);
		if (next.thinkingLevel && next.thinkingLevel !== this.thinkingLevel)
			await this.setThinkingLevel(next.thinkingLevel);
		return { model: next.model, thinkingLevel: this.thinkingLevel };
	}
	async setScopedModels(models: { model: Model; thinkingLevel?: ThinkingLevel }[]): Promise<void> {
		this.host.require("scopedModels");
		const response = await this.host.client.request<{ snapshot: PiSnapshot }>("_pirua/session/scoped_models", {
			sessionId: this.sessionId,
			models: models.map((s) => ({ provider: s.model.provider, modelId: s.model.id, thinkingLevel: s.thinkingLevel })),
		});
		this.install(response.snapshot);
	}
	getUserMessagesForForking(): { entryId: string; text: string }[] {
		return this.sessionManager.getBranch().flatMap((e) =>
			e.type === "message" && e.message.role === "user"
				? [
						{
							entryId: e.id,
							text:
								typeof e.message.content === "string"
									? e.message.content
									: e.message.content
											.filter((c) => c.type === "text")
											.map((c) => c.text)
											.join("\n"),
						},
					]
				: [],
		);
	}
	getLastAssistantText(): string | undefined {
		for (const m of [...this.messages].reverse())
			if (m.role === "assistant")
				return m.content
					.filter((c) => c.type === "text")
					.map((c) => c.text)
					.join("\n");
		return undefined;
	}
	getSessionStats(): SessionStats {
		const stats = this.projection.value.stats ?? {
			userMessages: 0,
			assistantMessages: 0,
			toolCalls: 0,
			toolResults: 0,
			totalMessages: 0,
			usage: zeroUsage(),
		};
		const usage = stats.usage ?? zeroUsage();
		return {
			...stats,
			sessionId: this.sessionId,
			sessionFile: undefined,
			tokens: {
				input: usage.input,
				output: usage.output,
				cacheRead: usage.cacheRead,
				cacheWrite: usage.cacheWrite,
				total: usage.totalTokens,
			},
			cost: usage.cost?.total ?? 0,
		};
	}
	async reload(): Promise<void> {
		this.host.require("resources");
		const result = await this.host.client.request<{ snapshot: PiSnapshot }>("_pirua/session/reload", {
			sessionId: this.sessionId,
		});
		this.install(result.snapshot);
	}
	async executeBash(
		command: string,
		onChunk: (chunk: string) => void,
		input: { excludeFromContext?: boolean } = {},
	): Promise<any> {
		this.host.require("shell");
		if (this.bashController) throw new Error("A shell command is already running");
		const controller = new AbortController();
		this.bashController = controller;
		this.bashChunks = onChunk;
		try {
			return await this.host.client.request(
				"_pirua/session/bash",
				{ sessionId: this.sessionId, command, ...input },
				controller.signal,
			);
		} finally {
			this.bashController = undefined;
			this.bashChunks = undefined;
		}
	}
	async abortBash(): Promise<void> {
		this.bashController?.abort(new Error("Shell cancelled"));
	}
	async exportToHtml(outputPath?: string, options: { themeName?: string } = {}): Promise<string> {
		await this.refresh();
		return exportSessionToHtml(this.sessionManager, this.state, { outputPath, ...options });
	}
	dispose(): void {
		this.closed = true;
		this.wakeResync?.();
		this.unsub();
		this.listeners.clear();
		this.bashController?.abort(new Error("Session closed"));
	}
}
