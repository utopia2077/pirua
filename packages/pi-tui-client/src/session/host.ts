import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { AcpClient, RpcError, type RecordValue } from "../acp/client.ts";
import { PROFILE, type PiSnapshot, type SessionChange } from "../acp/types.ts";
import { TuiSession } from "./facade.ts";
import type { ClientSettings } from "../ui/settings.ts";
import { PresentationRegistry } from "../ui/presentation-registry.ts";
import type { SessionInfo, AuthPrompt, AuthEvent } from "../display/types.ts";

export interface HostOptions {
	cwd: string;
	settings: ClientSettings;
	child?: ChildProcessWithoutNullStreams;
}
export class TuiRuntimeHost {
	readonly client: AcpClient;
	readonly options: HostOptions;
	readonly presentation = new PresentationRegistry();
	session!: TuiSession;
	features = new Set<string>();
	authMethods: RecordValue[] = [];
	capabilities: RecordValue = {};
	authInteraction:
		| { signal: AbortSignal; prompt: (p: AuthPrompt) => Promise<string>; notify: (e: AuthEvent) => void }
		| undefined;
	private beforeInvalidate: (() => void) | undefined;
	private rebind: (() => Promise<void>) | undefined;
	private selecting = false;
	private buffered: { method: string; params: RecordValue }[] = [];
	private retiring: string | undefined;
	private retired: TuiSession | undefined;
	private lifecycle: Promise<unknown> = Promise.resolve();
	private closing = false;
	constructor(client: AcpClient, options: HostOptions) {
		this.client = client;
		this.options = options;
		client.onNotification((method, params) => {
			// The outgoing session settles in its own view while its replacement is buffered.
			const outgoing = this.retiring !== undefined && params?.sessionId === this.retiring;
			if (this.selecting && !outgoing) this.buffered.push({ method, params });
			else this.session?.receive(method, params);
		});
		client.onRequest = (method, params, signal) => this.handleClientRequest(method, params, signal);
	}
	async initialize(sessionId?: string): Promise<void> {
		const init = await this.client.request("initialize", {
			protocolVersion: 2,
			capabilities: { elicitation: { form: {}, url: {} }, _meta: { [PROFILE]: { version: 1 } } },
			info: { name: "pirua-tui", version: "0.1.0" },
		});
		if (init.protocolVersion !== 2) throw new Error("Rust host did not select ACP v2");
		const profile = init.capabilities?._meta?.[PROFILE];
		if (profile?.version !== 1 || !profile.features?.includes("snapshot"))
			throw new Error("Original Pi TUI requires the pirua.dev/tui v1 snapshot capability");
		this.features = new Set(profile.features);
		this.authMethods = init.authMethods ?? [];
		this.capabilities = init.capabilities ?? {};
		if (sessionId) await this.switchSession(sessionId);
		else await this.newSession();
	}
	require(feature: string): void {
		if (!this.features.has(feature))
			throw new RpcError(-32050, `Rust host does not advertise the ${feature} capability`, { kind: "unsupported" });
	}
	setBeforeSessionInvalidate(fn: () => void): void {
		this.beforeInvalidate = fn;
	}
	setRebindSession(fn: () => Promise<void>): void {
		this.rebind = fn;
	}
	// Session switches run one at a time, each after the previous one has finished.
	private serialize<T>(operation: () => Promise<T>): Promise<T> {
		const run = this.lifecycle.then(operation);
		this.lifecycle = run.catch(() => {});
		return run;
	}
	// Makes the session opened by `open` current (`target` is its id when known in advance). Returns
	// false when exit began first: the switch then closes what it opened instead of activating it.
	private async select(
		target: string | undefined,
		open: () => Promise<{ sessionId: string; snapshot?: PiSnapshot }>,
	): Promise<boolean> {
		if (this.closing) return false;
		const previous: TuiSession | undefined = this.session;
		this.selecting = true;
		// For the whole switch, not only while it closes, the outgoing session keeps its own updates.
		if (previous && previous.sessionId !== target) this.retiring = previous.sessionId;
		try {
			const { sessionId: id, snapshot } = await open();
			// A target that does not become current must not stay open on the server.
			const abandon = async () => {
				if (id !== previous?.sessionId) await this.closeRemote(id).catch(() => {});
			};
			let initial: PiSnapshot;
			try {
				initial = snapshot ?? (await this.client.request<PiSnapshot>("_pirua/session/snapshot", { sessionId: id }));
			} catch (error) {
				await abandon();
				throw error;
			}
			if (previous && previous.sessionId !== id && !this.closing) {
				// Like upstream teardownCurrent(): once the replacement exists, cancel and dispose the
				// outgoing native session so its turn, tools and shell cannot keep running unseen.
				this.retired = previous;
				try {
					await this.closeRemote(previous.sessionId);
				} catch {
					/* The replacement is ready; a failed close of the old session must not block the switch. */
				}
			}
			if (this.closing) {
				await abandon();
				return false;
			}
			this.beforeInvalidate?.();
			this.session = new TuiSession(this, initial);
			previous?.dispose();
		} finally {
			this.release();
		}
		await this.rebind?.();
		return true;
	}
	// Ends selection: held updates go to whichever session is current now.
	private release(): void {
		this.selecting = false;
		this.retiring = undefined;
		const buffered = this.buffered;
		this.buffered = [];
		for (const notification of buffered) this.session?.receive(notification.method, notification.params);
	}
	async newSession(_options?: unknown): Promise<{ cancelled: boolean }> {
		const activated = await this.serialize(() =>
			this.select(undefined, async () => {
				const created = await this.client.request("session/new", { cwd: this.options.cwd, mcpServers: [] });
				return { sessionId: created.sessionId };
			}),
		);
		return { cancelled: !activated };
	}
	async switchSession(sessionId: string): Promise<{ cancelled: boolean }> {
		const activated = await this.serialize(() =>
			this.select(sessionId, async () => {
				await this.client.request("session/resume", { sessionId, cwd: this.options.cwd });
				return { sessionId };
			}),
		);
		return { cancelled: !activated };
	}
	async applySessionChange(change: SessionChange): Promise<SessionChange> {
		if (change.cancelled) return change;
		const activated = await this.serialize(() => this.select(change.sessionId, async () => change));
		return activated ? change : { ...change, cancelled: true };
	}
	async fork(
		entryId: string,
		options: { position?: "before" | "at" } = {},
	): Promise<SessionChange & { selectedText?: string }> {
		this.require("tree");
		const change = await this.client.request<SessionChange>("_pirua/session/fork", {
			sessionId: this.session.sessionId,
			entryId,
			position: options.position ?? "before",
			expectedRevision: this.session.projection.value.history.revision,
		});
		await this.applySessionChange(change);
		return { ...change, selectedText: change.editorText };
	}
	async listSessions(all: boolean, signal?: AbortSignal): Promise<SessionInfo[]> {
		const result: SessionInfo[] = [];
		let cursor: string | undefined;
		do {
			const page = await this.client.request(
				"session/list",
				{ ...(!all ? { cwd: this.options.cwd } : {}), ...(cursor ? { cursor } : {}) },
				signal,
			);
			for (const item of page.sessions ?? []) {
				const meta = item._meta?.[PROFILE] ?? {};
				result.push({
					id: item.sessionId,
					path: item.sessionId,
					cwd: item.cwd,
					name: item.title ?? undefined,
					parentSessionPath: meta.parentSessionId,
					created: new Date(meta.createdAt ?? item.updatedAt ?? 0),
					modified: new Date(item.updatedAt ?? 0),
					messageCount: meta.messageCount ?? 0,
					firstMessage: meta.firstMessage ?? item.title ?? "",
					allMessagesText: meta.allMessagesText ?? item.title ?? "",
				});
			}
			cursor = page.nextCursor ?? undefined;
		} while (cursor);
		return result;
	}
	async renameSession(sessionId: string, name: string): Promise<void> {
		this.require("tree");
		await this.client.request("_pirua/session/rename", { sessionId, name });
	}
	// Waits for session/close until `expired`, by default 3 s from now.
	private async closeRemote(sessionId: string, expired?: Promise<void>): Promise<void> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				this.client.request("session/close", { sessionId }),
				expired ??
					new Promise<void>((resolve) => {
						timer = setTimeout(resolve, 3000);
					}),
			]);
		} finally {
			if (timer) clearTimeout(timer);
		}
	}
	async dispose(): Promise<void> {
		if (this.closing) return;
		this.closing = true;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const expired = new Promise<void>((resolve) => {
			timer = setTimeout(resolve, 3000);
		});
		try {
			// One 3 s bound covers an in-flight switch, which stops short of activation once closing,
			// and the close of the session that is still open after it.
			await Promise.race([this.lifecycle, expired]);
			if (this.session && this.session !== this.retired) await this.closeRemote(this.session.sessionId, expired);
		} catch {
			/* Closing must restore the terminal even after EOF. */
		} finally {
			clearTimeout(timer);
			this.session?.dispose();
			this.client.close();
			const child = this.options.child;
			if (child && child.exitCode === null) {
				child.kill("SIGTERM");
				const timer = setTimeout(() => {
					if (child.exitCode === null) child.kill("SIGKILL");
				}, 1000);
				timer.unref();
			}
		}
	}
	private async handleClientRequest(method: string, params: RecordValue, signal: AbortSignal): Promise<unknown> {
		const ui = this.presentation.ui;
		if (method === "elicitation/create") {
			const interaction = this.authInteraction;
			signal = interaction ? AbortSignal.any([signal, interaction.signal]) : signal;
			if (signal.aborted) return { action: "cancel" };
			const prompt = async (message: string, placeholder?: string): Promise<string | undefined> => {
				try {
					if (interaction) return await interaction.prompt({ type: "input", message, placeholder, signal });
					if (!ui) throw new RpcError(-32050, "TUI is not ready to receive input", { kind: "busy" });
					return await ui.input(message, placeholder, { signal });
				} catch (error) {
					if (signal.aborted) return undefined;
					throw error;
				}
			};
			if (params.mode === "url") {
				if (this.authInteraction)
					this.authInteraction.notify({ type: "auth_url", url: params.url, instructions: params.message });
				else if (ui) ui.notify(params.message + "\n" + params.url, "info");
				const accepted = ui
					? await ui.confirm(params.message, `Continue at ${params.url}`, { signal })
					: !!(await prompt(params.message + "\n" + params.url, "Press Enter after completing authentication"));
				return { action: accepted ? "accept" : "cancel" };
			}
			if (params.mode !== "form") throw new RpcError(-32602, "Unsupported elicitation mode");
			const content: RecordValue = {};
			const schema = params.requestedSchema ?? {};
			const properties = Object.entries(schema.properties ?? {}) as [string, RecordValue][];
			if (properties.length === 0) {
				// Native auth notifications use standard form elicitation, with their original
				// non-secret event retained for the existing URL/device/info presentation.
				const event = params._meta?.["pirua.dev/auth"]?.event;
				if (interaction) interaction.notify(event ?? { type: "info", message: params.message });
				else if (ui) ui.notify(params.message, "info");
				else throw new RpcError(-32050, "TUI is not ready to display authentication instructions", { kind: "busy" });
			}
			for (const [key, property] of properties) {
				if (signal.aborted) return { action: "cancel" };
				let answer: string | undefined;
				if (Array.isArray(property.enum) && ui)
					answer = await ui.select(property.title ?? key, property.enum.map(String), { signal });
				else
					answer = await prompt(
						property.title ?? property.description ?? key,
						property.default === undefined ? undefined : String(property.default),
					);
				if (answer === undefined || signal.aborted) return { action: "cancel" };
				if (property.type === "boolean")
					content[key] = answer.toLowerCase() === "true" || answer.toLowerCase() === "yes";
				else if (property.type === "number" || property.type === "integer") {
					const value = Number(answer);
					if (!Number.isFinite(value) || (property.type === "integer" && !Number.isInteger(value)))
						throw new RpcError(-32602, `Invalid numeric answer for ${key}`);
					content[key] = value;
				} else content[key] = answer;
			}
			return signal.aborted ? { action: "cancel" } : { action: "accept", content };
		}
		if (method === "session/request_permission") {
			if (!ui) throw new RpcError(-32050, "TUI is not ready for permission input", { kind: "busy" });
			const options = params.options ?? [];
			const selection = await ui.select(
				params.toolCall?.title ?? "Permission required",
				options.map((o: RecordValue) => o.name),
				{ signal },
			);
			const option = options.find((o: RecordValue) => o.name === selection);
			return { outcome: option ? { outcome: "selected", optionId: option.optionId } : { outcome: "cancelled" } };
		}
		throw new RpcError(-32601, `Unsupported client method: ${method}`);
	}
}
