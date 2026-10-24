import { PassThrough } from "node:stream";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, it, expect } from "vitest";
import { AcpClient } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { InteractiveMode } from "../src/modes/interactive/interactive-mode.ts";
import { ToolExecutionComponent } from "../src/modes/interactive/components/tool-execution.ts";
import { initTheme } from "../src/modes/interactive/theme/theme.ts";
import { VirtualTerminal } from "../../pi-tui/test/virtual-terminal.ts";
import { snapshot, assistant } from "./fixtures.ts";
import { RENDER, PROJECTION, type PiSnapshot } from "../src/acp/types.ts";
import type { AgentSessionEvent } from "../src/display/types.ts";
import { stripAnsi } from "../src/utils/ansi.ts";
const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
function controller() {
	initTheme("dark");
	const input = new PassThrough(),
		output = new PassThrough();
	const client = new AcpClient(input, output);
	const host = new TuiRuntimeHost(client, {
		cwd: "/workspace/project",
		settings: new ClientSettings(join(tmpdir(), "pirua-hydration-unused.json")),
	});
	host.session = new TuiSession(host, snapshot({ state: "running" }));
	const ui: any = new InteractiveMode(host, { terminal: new VirtualTerminal(100, 30), tuiMode: "regular" });
	ui.isInitialized = true;
	ui.subscribeToAgent();
	const events: AgentSessionEvent[] = [];
	host.session.subscribe((e) => {
		events.push(e);
	});
	const errors: string[] = [];
	host.session.onError((e) => errors.push(e.message));
	return {
		host,
		ui,
		input,
		output,
		events,
		errors,
		dispose: () => {
			ui.stop();
			host.session.dispose();
			client.close();
		},
	};
}
const params = (sequence: number, update: any) => ({
	sessionId: "s1",
	_meta: { [PROJECTION]: { epoch: "epoch1", sequence } },
	update,
});
function withEntries(entries: any[], overrides: Partial<PiSnapshot> = {}): PiSnapshot {
	return snapshot({
		history: {
			header: snapshot().history.header,
			revision: entries.length,
			leafId: entries.at(-1)?.id ?? null,
			entries,
		},
		contextEntries: entries.map((sourceEntry) => ({
			sourceEntry,
			messages: sourceEntry.message ? [sourceEntry.message] : [],
		})),
		messages: entries.flatMap((e) => (e.message ? [e.message] : [])),
		...overrides,
	});
}
describe("active hydration reconciliation", () => {
	it("reuses one canonical tool row through repeated running snapshots and live completion", async () => {
		const c = controller();
		const message = assistant([
			{ type: "toolCall", id: "tc", name: "bash", arguments: { command: "printf running-tool" } },
		]);
		message.stopReason = "toolUse";
		const entry = { id: "entry-a", parentId: null, timestamp: 1000, type: "message", message };
		const native = {
			sessionUpdate: "agent_message",
			messageId: "a1",
			_meta: { [RENDER]: { phase: "end", nativeMessageId: "a1", entryId: entry.id, message } },
		};
		const tool = {
			sessionUpdate: "tool_call_update",
			toolCallId: "a1:tc",
			name: "bash",
			status: "in_progress",
			rawInput: { command: "printf running-tool" },
			_meta: { [RENDER]: { toolCallId: "tc", parentMessageId: "a1" } },
		};
		const active = withEntries([entry], { state: "running", sequence: 1, liveEntities: [native, tool] });
		const rows = () => c.ui.chatContainer.children.filter((child: any) => child instanceof ToolExecutionComponent);
		try {
			for (let i = 0; i < 2; i++) {
				c.host.session.projection.install(active);
				await tick();
				expect(rows()).toHaveLength(1);
				expect(c.ui.pendingTools.get("tc")).toBe(rows()[0]);
			}
			const result = { content: [{ type: "text", text: "single tool result" }] };
			c.host.session.receive("session/update", params(2, { ...tool, status: "completed", rawOutput: result }));
			await tick();
			expect(rows()).toHaveLength(1);
			expect(rows()[0].result.content).toEqual(result.content);
			expect(c.ui.pendingTools.size).toBe(0);
			const rendered = () => c.ui.chatContainer.render(100).map(stripAnsi).join("\n");
			expect(rendered().match(/single tool result/g)).toHaveLength(1);
			const resultEntry = {
				id: "entry-t",
				parentId: entry.id,
				timestamp: 1001,
				type: "message",
				message: { role: "toolResult", toolCallId: "tc", toolName: "bash", ...result, isError: false, timestamp: 1001 },
			};
			const after = withEntries([entry, resultEntry], {
				state: "running",
				sequence: 3,
				liveEntities: [native, { ...tool, status: "completed", rawOutput: result }],
			});
			for (let i = 0; i < 2; i++) {
				c.host.session.projection.install(after);
				await tick();
				expect(rows()).toHaveLength(1);
				expect(rendered().match(/single tool result/g)).toHaveLength(1);
				expect(c.ui.pendingTools.size).toBe(0);
			}
			expect(c.errors).toEqual([]);
		} finally {
			c.dispose();
		}
	});
	it.each([
		{ name: "cancelled assistant", role: "assistant", reason: "aborted", expected: "idle", aborted: true },
		{ name: "failed assistant", role: "assistant", reason: "error", expected: "error", aborted: false },
		{ name: "normal assistant", role: "assistant", reason: "stop", expected: "done", aborted: false },
		{
			name: "cancelled shell without an assistant",
			role: "bashExecution",
			reason: "cancelled",
			expected: "idle",
			aborted: true,
		},
		{
			name: "covered non-assistant cancellation before a message exists",
			role: "none",
			reason: "cancelled",
			expected: "idle",
			aborted: true,
		},
		{
			name: "covered non-assistant failure before a message exists",
			role: "none",
			reason: "error",
			expected: "error",
			aborted: false,
		},
	])("preserves $name after a covered settlement", async ({ role, reason, expected, aborted }) => {
		const c = controller();
		const message: any =
			role === "assistant"
				? assistant([{ type: "text", text: "terminal response" }])
				: {
						role: "bashExecution",
						command: "sleep",
						output: "",
						exitCode: undefined,
						cancelled: true,
						truncated: false,
						timestamp: 1000,
					};
		if (role === "assistant") {
			message.stopReason = reason;
			if (reason === "error") message.errorMessage = "deterministic failure";
		}
		const entries = role === "none" ? [] : [{ id: "entry", parentId: null, timestamp: 1000, type: "message", message }];
		const final = withEntries(entries, { state: "idle", sequence: 1 });
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
				if (role === "none")
					c.input.write(
						JSON.stringify({
							jsonrpc: "2.0",
							method: "session/update",
							params: params(1, {
								sessionUpdate: "state_update",
								state: "idle",
								stopReason: reason,
								...(reason === "error" ? { error: { code: -32000, message: "deterministic failure" } } : {}),
							}),
						}) + "\n",
					);
				c.input.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: final }) + "\n");
			});
		});
		try {
			await c.host.session.refresh();
			await tick();
			expect(settled).toBe(true);
			expect(c.ui.programStatus.currentStatus().state).toBe(expected);
			expect(c.events.filter((e) => e.type === "agent_settled")).toEqual([
				expect.objectContaining({ type: "agent_settled", aborted }),
			]);
			if (expected === "error") expect(c.ui.programStatus.currentStatus().message).toBe("deterministic failure");
			await c.host.session.refresh();
			await tick();
			expect(c.ui.programStatus.currentStatus().state).toBe(expected);
			expect(c.events.filter((e) => e.type === "agent_settled")).toHaveLength(1);
			expect(c.events.filter((e) => e.type === "message_end" || e.type === "tool_execution_end")).toEqual([]);
			expect(c.errors).toEqual([]);
		} finally {
			c.dispose();
			await waiter;
		}
	});
	it("observes idle without manufacturing success when the snapshot has no terminal outcome", async () => {
		const c = controller();
		let settled = false;
		const waiter = c.host.session.waitForIdle().then(
			() => {
				settled = true;
			},
			() => {},
		);
		try {
			c.host.session.projection.install(snapshot({ state: "idle", sequence: 1 }));
			await tick();
			expect(settled).toBe(true);
			expect(c.ui.programStatus.currentStatus().state).toBe("idle");
			expect(c.events.filter((e) => e.type === "agent_settled")).toEqual([]);
		} finally {
			c.dispose();
			await waiter;
		}
	});
});

it("does not classify current non-assistant work from a prior successful assistant", async () => {
	const c = controller();
	const prior = {
		id: "prior-response",
		parentId: null,
		timestamp: 1000,
		type: "message",
		message: assistant([{ type: "text", text: "old successful chat" }]),
	};
	const existing = withEntries([prior], { state: "running", sequence: 1 });
	c.host.session.projection.install(existing);
	await tick();
	let settled = false;
	const waiter = c.host.session.waitForIdle().then(
		() => {
			settled = true;
		},
		() => {},
	);
	try {
		c.host.session.projection.install({ ...existing, state: "idle", sequence: 2 });
		await tick();
		expect(settled).toBe(true);
		expect(c.ui.programStatus.currentStatus().state).toBe("idle");
		expect(c.events.filter((e) => e.type === "agent_settled")).toEqual([]);
	} finally {
		c.dispose();
		await waiter;
	}
});
