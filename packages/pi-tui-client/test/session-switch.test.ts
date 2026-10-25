import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AcpClient, RpcError, type RecordValue } from "../src/acp/client.ts";
import { PROJECTION } from "../src/acp/types.ts";
import type { AgentSessionEvent } from "../src/display/types.ts";
import { TuiSession } from "../src/session/facade.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { snapshot } from "./fixtures.ts";

const binary = process.env.PIRUA_TEST_SERVER;

describe.skipIf(!binary)("session replacement with the real Rust host", () => {
	it("closes the outgoing native session after /new, /resume and /fork, but not on a same-session resume", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pirua-tui-switch-"));
		const child = spawn(
			binary!,
			["--data-dir", directory, "--config-dir", join(directory, "config"), "--fixture-provider"],
			{ stdio: ["pipe", "pipe", "pipe"] },
		);
		const client = new AcpClient(child.stdout, child.stdin);
		child.on("error", (e) => client.fail(e));
		const host = new TuiRuntimeHost(client, {
			child,
			cwd: directory,
			settings: new ClientSettings(join(directory, "ui.json")),
		});
		const open = (sessionId: string) => client.request("_pirua/session/snapshot", { sessionId });
		try {
			await host.initialize();
			const first = host.session;
			const firstId = first.sessionId;
			const settled: AgentSessionEvent[] = [];
			first.projection.subscribe((event) => {
				if (event.type === "agent_settled") settled.push(event);
			});
			// The fixture provider streams until cancelled, standing in for a slow turn.
			await first.prompt("fixture:block");
			expect(first.isStreaming).toBe(true);
			await host.newSession();
			const secondId = host.session.sessionId;
			expect(secondId).not.toBe(firstId);
			expect(host.session.isIdle).toBe(true);
			await expect(open(firstId)).rejects.toThrow(/Session is not open/);
			await expect(open(secondId)).resolves.toMatchObject({ sessionId: secondId });
			// The outgoing turn was cancelled and settled in its own view before replacement.
			expect(settled).toEqual([expect.objectContaining({ type: "agent_settled", aborted: true })]);

			await host.switchSession(firstId);
			expect(host.session.sessionId).toBe(firstId);
			// Closing persisted the cancelled turn before the native session was disposed.
			expect(host.session.messages.at(-1)).toMatchObject({ role: "assistant", stopReason: "aborted" });
			await expect(open(secondId)).rejects.toThrow(/Session is not open/);

			await host.switchSession(firstId);
			expect(host.session.sessionId).toBe(firstId);
			await expect(open(firstId)).resolves.toMatchObject({ sessionId: firstId, state: "idle" });

			const user = host.session.sessionManager
				.getBranch()
				.find((e) => e.type === "message" && e.message.role === "user")!;
			const fork = await host.fork(user.id, { position: "at" });
			expect(fork.sessionId).not.toBe(firstId);
			expect(host.session.sessionId).toBe(fork.sessionId);
			await expect(open(firstId)).rejects.toThrow(/Session is not open/);
			await expect(open(fork.sessionId)).resolves.toMatchObject({ sessionId: fork.sessionId });
		} finally {
			await host.dispose();
			await rm(directory, { recursive: true, force: true });
		}
	}, 20000);
});

type Handler = (method: string, params: RecordValue) => unknown;
function fakeHost(handler: Handler) {
	const toClient = new PassThrough(),
		fromClient = new PassThrough();
	const client = new AcpClient(toClient, fromClient);
	const requests: { method: string; params: RecordValue }[] = [];
	const write = (value: unknown) => toClient.write(JSON.stringify(value) + "\n");
	createInterface({ input: fromClient }).on("line", async (line) => {
		const message = JSON.parse(line);
		requests.push({ method: message.method, params: message.params });
		if (message.id === undefined) return;
		try {
			write({ jsonrpc: "2.0", id: message.id, result: (await handler(message.method, message.params)) ?? {} });
		} catch (error) {
			const e = error instanceof RpcError ? error : new RpcError(-32603, String(error));
			write({ jsonrpc: "2.0", id: message.id, error: { code: e.code, message: e.message } });
		}
	});
	const host = new TuiRuntimeHost(client, {
		cwd: "/workspace/project",
		settings: new ClientSettings(join(tmpdir(), "pirua-switch-unused.json")),
	});
	host.session = new TuiSession(host, snapshot());
	const notify = (sessionId: string, sequence: number, update: RecordValue) =>
		write({
			jsonrpc: "2.0",
			method: "session/update",
			params: { sessionId, _meta: { [PROJECTION]: { epoch: "epoch1", sequence } }, update },
		});
	const closes = () => requests.filter((r) => r.method === "session/close").map((r) => r.params.sessionId);
	return { client, host, requests, notify, closes };
}
const target = (id: string) =>
	snapshot({ sessionId: id, history: { ...snapshot().history, header: { ...snapshot().history.header, id } } });
const gate = () => {
	let open!: () => void;
	const opened = new Promise<void>((resolve) => {
		open = resolve;
	});
	return { opened, open };
};
const tick = async (n = 5) => {
	for (let i = 0; i < n; i++) await new Promise<void>((resolve) => setImmediate(resolve));
};
const sent = (requests: { method: string; params: RecordValue }[], method: string) =>
	requests.filter((r) => r.method === method).map((r) => r.params?.sessionId);

describe("session replacement lifecycle", () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	it("keeps the current session open when a same-session change is applied", async () => {
		const { client, host, closes } = fakeHost(() => {
			throw new RpcError(-32601, "unexpected");
		});
		const current = host.session;
		await host.applySessionChange({ sessionId: "s1", cancelled: false, snapshot: target("s1") } as any);
		await host.applySessionChange({ sessionId: "s2", cancelled: true } as any);
		expect(host.session).not.toBe(current);
		expect(host.session.sessionId).toBe("s1");
		expect(closes()).toEqual([]);
		client.close();
	});

	it("keeps the current session open and usable when the replacement cannot be created", async () => {
		const { client, host, notify, closes } = fakeHost((method) => {
			if (method === "session/new") throw new RpcError(-32000, "store unavailable");
			if (method === "session/resume") throw new RpcError(-32602, "cwd must match the persisted session");
			if (method === "_pirua/session/snapshot") throw new RpcError(-32000, "snapshot unavailable");
		});
		const current = host.session;
		await expect(host.newSession()).rejects.toThrow("store unavailable");
		await expect(host.switchSession("s2")).rejects.toThrow("cwd must match");
		expect(host.session).toBe(current);
		expect(closes()).toEqual([]);
		// Selection ended, so the current projection still receives its own updates.
		const events: AgentSessionEvent[] = [];
		current.subscribe((event) => {
			events.push(event);
		});
		notify("s1", 1, { sessionUpdate: "state_update", state: "running" });
		await vi.waitFor(() => expect(current.isStreaming).toBe(true));
		await vi.waitFor(() => expect(events.some((e) => e.type === "agent_start")).toBe(true));
		client.close();
	});

	it("closes the outgoing session before activating the replacement, routing each session's updates to its own view", async () => {
		let release!: () => void;
		const closed = new Promise<void>((resolve) => {
			release = resolve;
		});
		const { client, host, notify, closes } = fakeHost(async (method, params) => {
			if (method === "session/new") return { sessionId: "s2" };
			if (method === "_pirua/session/snapshot") return target(params.sessionId);
			if (method === "session/close") {
				await closed;
				return {};
			}
		});
		const current = host.session;
		notify("s1", 1, { sessionUpdate: "state_update", state: "running" });
		await vi.waitFor(() => expect(current.isStreaming).toBe(true));
		const created = host.newSession();
		await vi.waitFor(() => expect(closes()).toEqual(["s1"]));
		expect(host.session).toBe(current);
		// While closing, the outgoing settlement reaches the outgoing view and the new session's
		// update is held for its replacement.
		notify("s1", 2, { sessionUpdate: "state_update", state: "idle", stopReason: "cancelled" });
		notify("s2", 1, { sessionUpdate: "state_update", state: "running" });
		await vi.waitFor(() => expect(current.isIdle).toBe(true));
		release();
		await created;
		expect(host.session.sessionId).toBe("s2");
		expect(host.session.isStreaming).toBe(true);
		// Late outgoing updates are ignored by the replacement projection.
		notify("s1", 3, { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" });
		notify("s2", 2, { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" });
		await vi.waitFor(() => expect(host.session.projection.value.sequence).toBe(2));
		expect(host.session.projection.value).toMatchObject({ sessionId: "s2", state: "idle" });
		client.close();
	});

	it("switches even when closing the outgoing session fails or never answers", async () => {
		let hang = false;
		const { client, host, closes } = fakeHost((method, params) => {
			if (method === "session/resume") return {};
			if (method === "_pirua/session/snapshot") return target(params.sessionId);
			if (method === "session/close") {
				if (hang) return new Promise(() => {});
				throw new RpcError(-32000, "dispose failed");
			}
		});
		await host.switchSession("s2");
		expect(host.session.sessionId).toBe("s2");
		hang = true;
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const switched = host.switchSession("s3");
		await vi.waitFor(() => expect(closes()).toEqual(["s1", "s2"]));
		expect(host.session.sessionId).toBe("s2");
		await vi.advanceTimersByTimeAsync(3000);
		await switched;
		expect(host.session.sessionId).toBe("s3");
		client.close();
	});

	it("applies outgoing updates that arrive while the replacement is created to the outgoing view", async () => {
		const creating = gate(),
			closing = gate();
		const { client, host, requests, notify, closes } = fakeHost(async (method, params) => {
			if (method === "session/new") {
				await creating.opened;
				return { sessionId: "s2" };
			}
			if (method === "_pirua/session/snapshot") {
				if (params.sessionId === "s1") throw new RpcError(-32000, "Session is not open");
				return target(params.sessionId);
			}
			if (method === "session/close") {
				await closing.opened;
				return {};
			}
		});
		const current = host.session;
		const errors: string[] = [];
		current.onError((error) => errors.push(error.message));
		const settled: AgentSessionEvent[] = [];
		current.subscribe((event) => {
			if (event.type === "agent_settled") settled.push(event);
		});
		notify("s1", 1, { sessionUpdate: "state_update", state: "running" });
		await vi.waitFor(() => expect(current.isStreaming).toBe(true));
		// The turn keeps streaming while session/new is pending, before the outgoing session closes.
		const created = host.newSession();
		await vi.waitFor(() => expect(sent(requests, "session/new")).toHaveLength(1));
		notify("s1", 2, {
			sessionUpdate: "agent_message_chunk",
			messageId: "a1",
			content: { type: "text", text: "still streaming" },
		});
		await vi.waitFor(() => expect(current.projection.value.sequence).toBe(2));
		creating.open();
		await vi.waitFor(() => expect(closes()).toEqual(["s1"]));
		notify("s1", 3, { sessionUpdate: "state_update", state: "idle", stopReason: "cancelled" });
		await vi.waitFor(() => expect(settled).toEqual([expect.objectContaining({ aborted: true })]));
		closing.open();
		await expect(created).resolves.toEqual({ cancelled: false });
		expect(host.session.sessionId).toBe("s2");
		expect(current.projection.assistants.get("a1")?.content).toEqual([{ type: "text", text: "still streaming" }]);
		// No sequence gap, so nothing asked for a snapshot of the session being closed.
		expect(sent(requests, "_pirua/session/snapshot")).toEqual(["s2"]);
		expect(errors).toEqual([]);
		client.close();
	});

	it("runs overlapping switches one after another, closing every outgoing session exactly once", async () => {
		const closing = gate();
		const { client, host, requests, closes } = fakeHost(async (method, params) => {
			if (method === "session/new") return { sessionId: "s2" };
			if (method === "session/resume") return {};
			if (method === "_pirua/session/snapshot") return target(params.sessionId);
			if (method === "session/close") {
				await closing.opened;
				return {};
			}
		});
		const created = host.newSession();
		await vi.waitFor(() => expect(closes()).toEqual(["s1"]));
		// /resume and a fork's change arrive during the 3 s close window of the first switch.
		const resumed = host.switchSession("s3");
		const forked = host.applySessionChange({ sessionId: "s4", cancelled: false, snapshot: target("s4") } as any);
		await tick();
		expect(sent(requests, "session/resume")).toEqual([]);
		expect(host.session.sessionId).toBe("s1");
		closing.open();
		await expect(created).resolves.toEqual({ cancelled: false });
		await expect(resumed).resolves.toEqual({ cancelled: false });
		await expect(forked).resolves.toMatchObject({ sessionId: "s4", cancelled: false });
		expect(host.session.sessionId).toBe("s4");
		expect(closes()).toEqual(["s1", "s2", "s3"]);
		client.close();
	});

	it("stops a switch interrupted by exit short of activation, closing each native session once", async () => {
		const closing = gate();
		const { host, requests, closes } = fakeHost(async (method, params) => {
			if (method === "session/new") return { sessionId: "s2" };
			if (method === "_pirua/session/snapshot") return target(params.sessionId);
			if (method === "session/close") {
				await closing.opened;
				return {};
			}
		});
		const current = host.session;
		const rebind = vi.fn(async () => {});
		const invalidate = vi.fn();
		host.setRebindSession(rebind);
		host.setBeforeSessionInvalidate(invalidate);
		const created = host.newSession();
		await vi.waitFor(() => expect(closes()).toEqual(["s1"]));
		const queued = host.switchSession("s3");
		const disposed = host.dispose();
		closing.open();
		await expect(created).resolves.toEqual({ cancelled: true });
		await expect(queued).resolves.toEqual({ cancelled: true });
		await disposed;
		// The switch closed the outgoing session and then its unactivated replacement; exit closed
		// nothing twice, and nothing was resumed, activated or rebound after it began.
		expect(closes()).toEqual(["s1", "s2"]);
		expect(sent(requests, "session/resume")).toEqual([]);
		expect(host.session).toBe(current);
		expect(invalidate).not.toHaveBeenCalled();
		expect(rebind).not.toHaveBeenCalled();
	});

	it("closes a replacement created after exit began and the session that is still current", async () => {
		const creating = gate();
		const { host, requests, closes } = fakeHost(async (method, params) => {
			if (method === "session/new") {
				await creating.opened;
				return { sessionId: "s2" };
			}
			if (method === "_pirua/session/snapshot") return target(params.sessionId);
			if (method === "session/close") return {};
		});
		const current = host.session;
		const rebind = vi.fn(async () => {});
		host.setRebindSession(rebind);
		const created = host.newSession();
		await vi.waitFor(() => expect(sent(requests, "session/new")).toHaveLength(1));
		const disposed = host.dispose();
		await tick();
		expect(closes()).toEqual([]);
		creating.open();
		await expect(created).resolves.toEqual({ cancelled: true });
		await disposed;
		// Once exit began, the switch closed only what it opened; exit closed the session still current.
		expect(closes()).toEqual(["s2", "s1"]);
		expect(host.session).toBe(current);
		expect(rebind).not.toHaveBeenCalled();
	});

	it("still exits within the 3 s close bound when an in-flight switch never finishes", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const { host, requests, closes } = fakeHost((method) => {
			if (method === "session/new") return new Promise(() => {});
			if (method === "session/close") return new Promise(() => {});
		});
		const created = host.newSession().catch((error) => error.message);
		await vi.waitFor(() => expect(sent(requests, "session/new")).toHaveLength(1));
		let exited = false;
		const disposed = host.dispose().then(() => {
			exited = true;
		});
		await vi.advanceTimersByTimeAsync(3000);
		await tick();
		expect(exited).toBe(true);
		await disposed;
		// The session that is still current is asked to close even though the bound has run out.
		await vi.waitFor(() => expect(closes()).toEqual(["s1"]));
		expect(await created).toBe("ACP client closed");
	});

	it("closes a replacement whose snapshot cannot be loaded and keeps the current session", async () => {
		let notify!: (sessionId: string, sequence: number, update: RecordValue) => void;
		const { client, host, closes, ...fake } = fakeHost((method, params) => {
			if (method === "session/new") return { sessionId: "s2" };
			if (method === "session/resume") {
				// An update of the current session that arrives during its own failed re-selection.
				if (params.sessionId === "s1") notify("s1", 1, { sessionUpdate: "state_update", state: "running" });
				return {};
			}
			if (method === "_pirua/session/snapshot") throw new RpcError(-32000, "snapshot unavailable");
			if (method === "session/close") return {};
		});
		notify = fake.notify;
		const current = host.session;
		await expect(host.newSession()).rejects.toThrow("snapshot unavailable");
		await expect(host.switchSession("s3")).rejects.toThrow("snapshot unavailable");
		expect(closes()).toEqual(["s2", "s3"]);
		// The current session is never closed when its own snapshot fails, and keeps its updates.
		await expect(host.switchSession("s1")).rejects.toThrow("snapshot unavailable");
		expect(closes()).toEqual(["s2", "s3"]);
		expect(host.session).toBe(current);
		expect(current.isStreaming).toBe(true);
		fake.notify("s1", 2, { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" });
		await vi.waitFor(() => expect(current.isIdle).toBe(true));
		expect(current.projection.value.sequence).toBe(2);
		client.close();
	});
});
