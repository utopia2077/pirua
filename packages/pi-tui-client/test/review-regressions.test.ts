import { PassThrough } from "node:stream";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, it, expect, vi } from "vitest";
import { AcpClient, RpcError } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { SessionProjection } from "../src/session/mirror.ts";
import { ModelRuntime } from "../src/session/model-catalog.ts";
import { InteractiveMode } from "../src/modes/interactive/interactive-mode.ts";
import { initTheme } from "../src/modes/interactive/theme/theme.ts";
import { VirtualTerminal } from "../../pi-tui/test/virtual-terminal.ts";
import { PROJECTION, RENDER, type PiSnapshot } from "../src/acp/types.ts";
import { snapshot, assistant } from "./fixtures.ts";
import { stripAnsi } from "../src/utils/ansi.ts";
const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const update = (sequence: number, payload: any) => ({
	sessionId: "s1",
	_meta: { [PROJECTION]: { epoch: "epoch1", sequence } },
	update: payload,
});
function session(initial: PiSnapshot) {
	const input = new PassThrough(),
		output = new PassThrough();
	const client = new AcpClient(input, output);
	const host = new TuiRuntimeHost(client, {
		cwd: "/workspace/project",
		settings: new ClientSettings(join(tmpdir(), "pirua-review-unused.json")),
	});
	host.session = new TuiSession(host, initial);
	return {
		host,
		input,
		output,
		client,
		dispose: () => {
			host.session.dispose();
			client.close();
		},
	};
}

describe("review regressions", () => {
	it("settles covered idle and reconciles final assistant/tool output into an existing controller exactly once", async () => {
		initTheme("dark");
		const partial = assistant([{ type: "text", text: "partial" }]);
		const c = session(
			snapshot({
				state: "running",
				liveEntities: [
					{
						sessionUpdate: "agent_message",
						messageId: "a1",
						content: [],
						_meta: { [RENDER]: { phase: "delta", nativeMessageId: "a1", message: partial } },
					},
				],
			}),
		);
		const ui: any = new InteractiveMode(c.host, { terminal: new VirtualTerminal(100, 30), tuiMode: "regular" });
		ui.isInitialized = true;
		ui.subscribeToAgent();
		const events: string[] = [];
		c.host.session.subscribe((e) => {
			events.push(e.type);
		});
		await tick();
		const mainEscape = () => {};
		ui.retryEscapeHandler = mainEscape;
		ui.defaultEditor.onEscape = () => {};
		const final = assistant([
			{ type: "text", text: "final result" },
			{ type: "toolCall", id: "tc", name: "bash", arguments: { command: "printf output" } },
		]);
		const entries: any[] = [
			{ id: "a", parentId: null, timestamp: 1000, type: "message", message: final },
			{
				id: "t",
				parentId: "a",
				timestamp: 1001,
				type: "message",
				message: {
					role: "toolResult",
					toolCallId: "tc",
					toolName: "bash",
					content: [{ type: "text", text: "tool result" }],
					isError: false,
					timestamp: 1001,
				},
			},
		];
		const completed = snapshot({
			sequence: 2,
			state: "idle",
			history: { header: snapshot().history.header, revision: 2, leafId: "t", entries },
			contextEntries: entries.map((sourceEntry) => ({ sourceEntry, messages: [sourceEntry.message] })),
			messages: entries.map((e) => e.message),
			liveEntities: [
				{
					sessionUpdate: "agent_message",
					messageId: "a1",
					_meta: { [RENDER]: { phase: "end", nativeMessageId: "a1", message: final } },
				},
				{
					sessionUpdate: "tool_call_update",
					toolCallId: "a1:tc",
					name: "bash",
					status: "completed",
					rawInput: { command: "printf output" },
					rawOutput: { content: [{ type: "text", text: "tool result" }] },
					_meta: { [RENDER]: { toolCallId: "tc", parentMessageId: "a1" } },
				},
			],
		});
		let settled = false;
		const waiter = c.host.session.waitForIdle().then(
			() => {
				settled = true;
			},
			() => {},
		);
		c.output.on("data", (line) => {
			const request = JSON.parse(line.toString());
			queueMicrotask(() => {
				c.input.write(
					JSON.stringify({
						jsonrpc: "2.0",
						method: "session/update",
						params: update(2, { sessionUpdate: "state_update", state: "idle" }),
					}) + "\n",
				);
				c.input.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: completed }) + "\n");
			});
		});
		try {
			await c.host.session.refresh();
			await tick();
			expect(settled).toBe(true);
			const rendered = ui.chatContainer.render(100).map(stripAnsi).join("\n");
			expect(rendered.match(/final result/g)).toHaveLength(1);
			expect(rendered.match(/tool result/g)).toHaveLength(1);
			expect(rendered).not.toContain("partial");
			expect(ui.streamingComponent).toBeUndefined();
			expect(ui.pendingTools.size).toBe(0);
			expect(ui.activeStatusIndicator).toBeUndefined();
			expect(ui.defaultEditor.onEscape).toBe(mainEscape);
			await c.host.session.refresh();
			await tick();
			expect(events.filter((e) => e === "agent_settled")).toHaveLength(1);
			expect(events.filter((e) => e === "message_end" || e === "tool_execution_end")).toEqual([]);
			expect(
				ui.chatContainer
					.render(100)
					.map(stripAnsi)
					.join("\n")
					.match(/final result/g),
			).toHaveLength(1);
		} finally {
			ui.stop();
			c.dispose();
			await waiter;
		}
	});
	it("owns rejected name submissions at the synchronous editor callback boundary", async () => {
		const ui: any = Object.create(InteractiveMode.prototype);
		ui.runtimeHost = {
			session: {
				setSessionName: vi
					.fn()
					.mockRejectedValue(new RpcError(-32050, "Session foreground work is active", { kind: "busy" })),
			},
		};
		ui.defaultEditor = {};
		let text = "";
		ui.editor = {
			setText: (s: string) => {
				text = s;
			},
			getText: () => text,
		};
		ui.showError = vi.fn();
		ui.setupEditorSubmitHandler();
		await ui.defaultEditor.onSubmit("/name while-running");
		await tick();
		expect(ui.showError).toHaveBeenCalledWith("Session foreground work is active");
		expect(text).toBe("/name while-running");
	});
	it("owns async dequeue and follow-up action failures", async () => {
		const ui: any = Object.create(InteractiveMode.prototype);
		const actions = new Map();
		ui.defaultEditor = { onAction: (id: string, fn: () => void) => actions.set(id, fn) };
		ui.ui = {};
		ui.showError = vi.fn();
		ui.handleDequeue = vi.fn().mockRejectedValue(new Error("clear rejected"));
		ui.handleFollowUp = vi.fn().mockRejectedValue(new Error("queue rejected"));
		ui.setupKeyHandlers();
		actions.get("app.message.dequeue")();
		actions.get("app.message.followUp")();
		await tick();
		expect(ui.showError).toHaveBeenCalledWith("clear rejected");
		expect(ui.showError).toHaveBeenCalledWith("queue rejected");
	});
	it("routes callback-free negotiated API-key methods to the native dialog", async () => {
		const runtime = new ModelRuntime({
			authMethods: [{ type: "agent", methodId: "anthropic:apiKey", name: "Anthropic API key" }],
		} as any);
		const ui: any = Object.create(InteractiveMode.prototype);
		ui.showApiKeyLoginDialog = vi.fn();
		ui.showAmbientAuthDialog = vi.fn();
		await ui.startProviderLogin(runtime.getAuthOptions()[0]);
		expect(ui.showApiKeyLoginDialog).toHaveBeenCalledWith("anthropic", "Anthropic API key", undefined);
		expect(ui.showAmbientAuthDialog).not.toHaveBeenCalled();
	});
	it("keeps detached predecessor history per native identity when commits outrun completion callbacks", async () => {
		initTheme("dark");
		const prior = assistant([{ type: "text", text: "same text" }]);
		prior.usage = { ...prior.usage, cacheRead: 30000, totalTokens: 30000 };
		const first = assistant([{ type: "text", text: "same text" }]);
		first.model = "new-model";
		first.usage = {
			...first.usage,
			input: 30000,
			totalTokens: 30000,
			cost: { ...first.usage.cost, input: 1, total: 1 },
		};
		first.diagnostics = [
			{ type: "anthropic_input_transformations", details: { transformations: [{ type: "thinking_dropped" }] } },
		];
		const second = structuredClone(first);
		const entry = (id: string, parentId: string | null, message: any) => ({
			id,
			parentId,
			timestamp: 1000,
			type: "message",
			message,
		});
		const initial = entry("prior", null, prior);
		const c = session(
			snapshot({ history: { header: snapshot().history.header, revision: 1, leafId: "prior", entries: [initial] } }),
		);
		const completions: any[] = [];
		let release!: () => void;
		const blocked = new Promise<void>((resolve) => (release = resolve));
		c.host.session.subscribe(async (e) => {
			if (e.type === "message_start") await blocked;
			if (e.type === "message_end") completions.push(e);
		});
		c.host.session.receive(
			"session/update",
			update(1, {
				sessionUpdate: "agent_message",
				messageId: "first",
				_meta: { [RENDER]: { phase: "start", nativeMessageId: "first", message: assistant() } },
			}),
		);
		c.host.session.receive("_pirua/session/changed", {
			sessionId: "s1",
			epoch: "epoch1",
			sequence: 2,
			changes: {},
			historyChange: {
				kind: "append",
				baseRevision: 1,
				revision: 2,
				leafId: "first-entry",
				appended: [entry("first-entry", "prior", first)],
			},
		});
		c.host.session.receive(
			"session/update",
			update(3, {
				sessionUpdate: "agent_message",
				messageId: "first",
				_meta: { [RENDER]: { phase: "end", nativeMessageId: "first", message: first } },
			}),
		);
		c.host.session.receive(
			"session/update",
			update(4, {
				sessionUpdate: "agent_message",
				messageId: "second",
				_meta: { [RENDER]: { phase: "start", nativeMessageId: "second", message: assistant() } },
			}),
		);
		c.host.session.receive("_pirua/session/changed", {
			sessionId: "s1",
			epoch: "epoch1",
			sequence: 5,
			changes: {},
			historyChange: {
				kind: "append",
				baseRevision: 2,
				revision: 3,
				leafId: "second-entry",
				appended: [entry("second-entry", "first-entry", second)],
			},
		});
		c.host.session.receive(
			"session/update",
			update(6, {
				sessionUpdate: "agent_message",
				messageId: "second",
				_meta: { [RENDER]: { phase: "end", nativeMessageId: "second", message: second } },
			}),
		);
		release();
		await tick();
		try {
			expect(completions[0].previousEntries.map((e: any) => e.id)).toEqual(["prior"]);
			expect(completions[1].previousEntries.map((e: any) => e.id)).toEqual(["prior", "first-entry"]);
			const ui: any = Object.create(InteractiveMode.prototype);
			ui.runtimeHost = c.host;
			c.host.options.settings.getShowCacheMissNotices = () => true;
			ui.chatContainer = { addChild: vi.fn() };
			ui.addCacheMissNotice = vi.fn();
			ui.maybeShowThinkingDropNotice(completions[0].message, completions[0].previousEntries);
			expect(ui.chatContainer.addChild).toHaveBeenCalledTimes(2);
			ui.maybeShowCacheMissNotice(completions[0].message, completions[0].previousEntries);
			expect(ui.addCacheMissNotice).toHaveBeenCalledWith(
				expect.objectContaining({ missedTokens: 30000, modelChanged: true }),
			);
			ui.chatContainer.addChild.mockClear();
			ui.maybeShowThinkingDropNotice(completions[1].message, completions[1].previousEntries);
			expect(ui.chatContainer.addChild).not.toHaveBeenCalled();
		} finally {
			c.dispose();
		}
	});
});
