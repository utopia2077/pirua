import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect } from "vitest";
import { AcpClient } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import type { AgentSessionEvent } from "../src/display/types.ts";

const binary = process.env.PIRUA_TEST_SERVER;
describe.skipIf(!binary)("original UI facade with the real Rust host", () => {
	it("uses host prompt, tool, queues, configuration, tree, shell and export results", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pirua-tui-host-"));
		const child = spawn(
			binary!,
			[
				"--data-dir",
				directory,
				"--config-dir",
				join(directory, "config"),
				"--fixture-provider",
				"--fixture-delay-ms",
				"25",
			],
			{ stdio: ["pipe", "pipe", "pipe"] },
		);
		const client = new AcpClient(child.stdout, child.stdin);
		child.on("error", (e) => client.fail(e));
		const host = new TuiRuntimeHost(client, {
			child,
			cwd: directory,
			settings: new ClientSettings(join(directory, "ui.json")),
		});
		try {
			await host.initialize();
			const originalId = host.session.sessionId;
			const events: AgentSessionEvent[] = [];
			host.session.onError((error) => {
				throw error;
			});
			host.session.subscribe((event) => {
				events.push(event);
			});
			expect(host.session.sessionManager.buildContextEntries().length).toBeGreaterThan(0);
			expect(host.session.sessionManager.getHeader().timestamp).toMatch(/^\d{4}-/);
			const admitted = await host.session.prompt("hello original UI");
			expect(admitted.messageId).toEqual(expect.any(String));
			await host.session.waitForIdle();
			await host.session.refresh();
			expect(host.session.getLastAssistantText()).toContain("Fixture response: hello original UI");
			expect(
				events.some(
					(e) =>
						e.type === "message_update" &&
						e.message.role === "assistant" &&
						e.message.content.some((c) => c.type === "thinking"),
				),
			).toBe(true);
			expect(host.session.getSessionStats()).toMatchObject({
				sessionId: originalId,
				tokens: { input: expect.any(Number) },
			});
			await host.session.prompt("fixture:tool");
			await host.session.waitForIdle();
			expect(events.some((e) => e.type === "tool_execution_end" && e.toolName === "bash")).toBe(true);
			await host.session.prompt("fixture:block");
			const { InteractiveMode } = await import("../src/modes/interactive/interactive-mode.ts");
			const nameUi: any = Object.create(InteractiveMode.prototype);
			nameUi.runtimeHost = host;
			nameUi.defaultEditor = {};
			let draft = "";
			nameUi.editor = {
				getText: () => draft,
				setText: (text: string) => {
					draft = text;
				},
			};
			const nameErrors: string[] = [];
			nameUi.showError = (message: string) => nameErrors.push(message);
			nameUi.setupEditorSubmitHandler();
			await nameUi.defaultEditor.onSubmit("/name while-running");
			expect(nameErrors).toEqual(["Session foreground work is active"]);
			expect(draft).toBe("/name while-running");
			await host.session.prompt("queued text", { streamingBehavior: "followUp" });
			const removed = await host.session.clearQueue();
			expect(removed.followUp).toEqual(["queued text"]);
			await host.session.abort();
			expect(host.session.isIdle).toBe(true);
			await host.session.setSessionName("Original UI test");
			expect(host.session.sessionManager.getSessionName()).toBe("Original UI test");
			const userEntry = host.session.sessionManager
				.getBranch()
				.find((e) => e.type === "message" && e.message.role === "user")!;
			await host.session.setLabel(userEntry.id, "checkpoint");
			const tree = host.session.sessionManager.getTree();
			expect(JSON.stringify(tree)).toContain("checkpoint");
			const currentModel = host.session.model!;
			await host.session.setScopedModels([{ model: currentModel }]);
			expect(host.session.scopedModels).toHaveLength(1);
			await host.session.setConfig("autoCompaction", false);
			expect(host.session.autoCompactionEnabled).toBe(false);
			const shell = await host.session.executeBash("printf shell-fixture", () => {});
			expect(shell.output).toContain("shell-fixture");
			const exported = await host.session.exportToHtml(join(directory, "session.html"), { themeName: "dark" });
			const html = await readFile(exported, "utf8");
			const encoded = html.match(/<script id="session-data" type="application\/json">([^<]+)<\/script>/)?.[1];
			expect(encoded).toBeTruthy();
			const data = JSON.parse(Buffer.from(encoded!, "base64").toString("utf8"));
			expect(
				data.entries.some(
					(e: any) => e.message?.role === "user" && e.message.content.some((c: any) => c.text === "hello original UI"),
				),
			).toBe(true);
			const fork = await host.fork(userEntry.id, { position: "before" });
			expect(fork.sessionId).not.toBe(originalId);
			expect(fork.editorText).toBe("hello original UI");
			await host.switchSession(originalId);
			expect(host.session.sessionId).toBe(originalId);
			expect((await host.listSessions(false)).some((s) => s.id === originalId)).toBe(true);
		} finally {
			await host.dispose();
			await rm(directory, { recursive: true, force: true });
		}
	}, 20000);

	it("keeps the context of its mirror as the host's while appends carry only what they add", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pirua-tui-append-"));
		const child = spawn(
			binary!,
			["--data-dir", directory, "--config-dir", join(directory, "config"), "--fixture-provider", "--fixture-delay-ms", "5"],
			{ stdio: ["pipe", "pipe", "pipe"] },
		);
		const client = new AcpClient(child.stdout, child.stdin);
		child.on("error", (e) => client.fail(e));
		const frames: any[] = [];
		client.onNotification((method, params) => {
			if (method === "_pirua/session/changed") frames.push(params);
		});
		const host = new TuiRuntimeHost(client, {
			child,
			cwd: directory,
			settings: new ClientSettings(join(directory, "ui.json")),
		});
		try {
			await host.initialize();
			for (const text of ["first", "fixture:tool", "third"]) {
				await host.session.prompt(text);
				await host.session.waitForIdle();
			}
			// Nothing asked the host for a snapshot since the session opened: the mirror followed the frames.
			const appends = frames.filter((frame) => frame.historyChange?.kind === "append");
			expect(appends.length).toBeGreaterThanOrEqual(6);
			for (const frame of appends) {
				expect(frame.changes.messages).toBeUndefined();
				expect(frame.changes.contextEntries).toBeUndefined();
				expect(frame.historyChange.contextAppended).toHaveLength(frame.historyChange.appended.length);
			}
			expect(frames.some((frame) => frame.changes.messages !== undefined)).toBe(false);
			expect(host.session.getLastAssistantText()).toContain("Fixture response: third");
			const held = host.session.projection.value;
			const current = await client.request("_pirua/session/snapshot", { sessionId: host.session.sessionId });
			expect(held.messages.length).toBeGreaterThan(6);
			expect(held.messages).toEqual(current.messages);
			expect(held.contextEntries).toEqual(current.contextEntries);
			expect(held.history).toEqual(current.history);
		} finally {
			await host.dispose();
			await rm(directory, { recursive: true, force: true });
		}
	}, 20000);
});

describe.skipIf(!binary)("native API-key dialog without provider requests", () => {
	it("routes an advertised method through real reverse elicitation and cancels a second dialog cleanly", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pirua-tui-auth-"));
		const child = spawn(binary!, ["--data-dir", directory, "--config-dir", join(directory, "config")], {
			stdio: ["pipe", "pipe", "pipe"],
		});
		const client = new AcpClient(child.stdout, child.stdin);
		child.on("error", (e) => client.fail(e));
		const host = new TuiRuntimeHost(client, {
			child,
			cwd: directory,
			settings: new ClientSettings(join(directory, "ui.json")),
		});
		let ui: any;
		try {
			await host.initialize();
			const { InteractiveMode } = await import("../src/modes/interactive/interactive-mode.ts");
			const { VirtualTerminal } = await import("../../pi-tui/test/virtual-terminal.ts");
			const { initTheme } = await import("../src/modes/interactive/theme/theme.ts");
			initTheme("dark");
			ui = new InteractiveMode(host, { terminal: new VirtualTerminal(100, 30), tuiMode: "regular" });
			ui.isInitialized = true;
			const notices: string[] = [];
			ui.showStatus = (s: string) => notices.push(s);
			ui.showError = (s: string) => notices.push(s);
			const option = host.session.modelRuntime
				.getAuthOptions()
				.find((o) => o.id === "deepseek" && o.authType === "api_key")!;
			expect(option.method?.id).toBe("deepseek:apiKey");
			const waitForPrompt = async () => {
				for (let i = 0; i < 100; i++) {
					const dialog = ui.editorContainer.children[0];
					if (dialog?.inputResolver) return dialog;
					await new Promise((r) => setTimeout(r, 10));
				}
				throw new Error("Native reverse elicitation did not reach the original login input");
			};
			const requests: any[] = [];
			const onRequest = client.onRequest!;
			client.onRequest = async (method, params, signal) => {
				requests.push({ method, params });
				return onRequest(method, params, signal);
			};
			const login = ui.startProviderLogin(option);
			const dialog = await waitForPrompt();
			dialog.input.setValue("synthetic-secret-for-tui-test");
			dialog.input.onSubmit();
			await login;
			expect(
				requests.some(
					(r) =>
						r.method === "elicitation/create" &&
						r.params.mode === "form" &&
						r.params._meta?.["pirua.dev/auth"]?.secret === true,
				),
			).toBe(true);
			expect(notices.some((s) => s.startsWith("Saved API key for"))).toBe(true);
			expect(host.authInteraction).toBeUndefined();
			let backed = false;
			const cancel = ui.startProviderLogin(option, () => {
				backed = true;
			});
			const cancelledDialog = await waitForPrompt();
			cancelledDialog.input.onEscape();
			await cancel;
			expect(backed).toBe(true);
			expect(host.authInteraction).toBeUndefined();
			expect(ui.editorContainer.children).toEqual([ui.editor]);
			await host.session.modelRuntime.logout("deepseek");
		} finally {
			ui?.stop();
			await host.dispose();
			await rm(directory, { recursive: true, force: true });
		}
	}, 15000);
});
