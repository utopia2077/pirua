import { PassThrough } from "node:stream";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { afterEach, describe, it, expect, vi } from "vitest";
import { AcpClient } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { PROJECTION } from "../src/acp/types.ts";
import type { AgentSessionEvent } from "../src/display/types.ts";
import { snapshot } from "./fixtures.ts";

const tick = async (n = 5) => {
	for (let i = 0; i < n; i++) await new Promise<void>((resolve) => setImmediate(resolve));
};
const update = (sequence: number, payload: Record<string, unknown>) => ({
	sessionId: "s1",
	_meta: { [PROJECTION]: { epoch: "epoch1", sequence } },
	update: payload,
});
const chunk = (sequence: number, text: string) =>
	update(sequence, { sessionUpdate: "agent_message_chunk", messageId: "a1", content: { type: "text", text } });
const queued = (sequence: number, text: string) => ({
	sessionId: "s1",
	epoch: "epoch1",
	sequence,
	changes: { queue: { steering: [{ submissionId: text, delivery: "steer", text, images: [] }], followUp: [] } },
});
const notOpen = { error: { code: -32000, message: "Session is not open" } };
function harness(reply?: (request: any) => any) {
	const input = new PassThrough(),
		output = new PassThrough();
	const requests: any[] = [];
	output.on("data", (data) => {
		for (const line of data.toString().split("\n").filter(Boolean)) {
			const request = JSON.parse(line);
			requests.push(request);
			const body = reply?.(request);
			if (body) queueMicrotask(() => input.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, ...body }) + "\n"));
		}
	});
	const client = new AcpClient(input, output);
	const host = new TuiRuntimeHost(client, {
		cwd: "/workspace/project",
		settings: new ClientSettings(join(tmpdir(), "pirua-refresh-unused.json")),
	});
	const session = new TuiSession(host, snapshot());
	host.session = session;
	const errors: string[] = [];
	session.onError((e) => errors.push(e.message));
	const events: AgentSessionEvent[] = [];
	session.subscribe((e) => {
		events.push(e);
	});
	return {
		session,
		errors,
		events,
		snapshots: () => requests.filter((r) => r.method === "_pirua/session/snapshot"),
		respond: (request: any, body: any) =>
			input.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, ...body }) + "\n"),
		dispose: () => {
			session.dispose();
			client.close();
		},
	};
}
afterEach(() => {
	vi.useRealTimers();
});
describe("snapshot refresh failure", () => {
	it("applies updates buffered during a failed explicit refresh and keeps applying later ones", async () => {
		const h = harness();
		const p = h.session.projection;
		try {
			const refresh = h.session.refresh().then(
				() => "resolved",
				(error) => error.message,
			);
			await tick();
			expect(h.snapshots()).toHaveLength(1);
			h.session.receive("session/update", update(1, { sessionUpdate: "state_update", state: "running" }));
			h.session.receive("_pirua/session/changed", queued(2, "buffered steer"));
			h.session.receive("session/update", chunk(3, "covered"));
			expect(p.value.sequence).toBe(0);
			h.respond(h.snapshots()[0], notOpen);
			expect(await refresh).toBe("Session is not open");
			expect(p.value.sequence).toBe(3);
			expect(p.value.state).toBe("running");
			expect(h.session.getSteeringMessages()).toEqual(["buffered steer"]);
			expect(p.assistants.get("a1")?.content).toEqual([{ type: "text", text: "covered" }]);
			h.session.receive("session/update", chunk(4, " live"));
			h.session.receive(
				"session/update",
				update(5, { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" }),
			);
			await tick();
			expect(p.value.sequence).toBe(5);
			expect(p.value.state).toBe("idle");
			expect(p.assistants.get("a1")?.content).toEqual([{ type: "text", text: "covered live" }]);
			expect(h.events.map((e) => e.type)).toEqual(
				expect.arrayContaining(["agent_start", "queue_update", "message_start", "agent_settled"]),
			);
			expect(h.snapshots()).toHaveLength(1);
			expect(h.errors).toEqual([]);
		} finally {
			h.dispose();
		}
	});
	it("applies backend settings changed by updates released after a failed refresh", async () => {
		const h = harness();
		const settings = h.session.settingsManager;
		try {
			const refresh = h.session.refresh().then(
				() => "resolved",
				(error) => error.message,
			);
			await tick();
			h.session.receive("_pirua/session/changed", {
				sessionId: "s1",
				epoch: "epoch1",
				sequence: 1,
				changes: { settings: { ...snapshot().settings, projectTrusted: true } },
			});
			expect(settings.isProjectTrusted()).toBe(false);
			h.respond(h.snapshots()[0], notOpen);
			expect(await refresh).toBe("Session is not open");
			expect(h.session.projection.value.settings.projectTrusted).toBe(true);
			expect(settings.isProjectTrusted()).toBe(true);
		} finally {
			h.dispose();
		}
	});
	it("retries a failed resynchronization after a backoff and recovers to the new snapshot", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const h = harness();
		const p = h.session.projection;
		try {
			h.session.receive("session/update", chunk(2, "after gap"));
			await tick();
			expect(h.snapshots()).toHaveLength(1);
			h.session.receive("_pirua/session/changed", queued(3, "buffered steer"));
			h.respond(h.snapshots()[0], notOpen);
			await tick();
			expect(h.errors).toEqual(["Session is not open"]);
			expect(p.value.sequence).toBe(0);
			expect(h.snapshots()).toHaveLength(1);
			await vi.advanceTimersByTimeAsync(249);
			await tick();
			expect(h.snapshots()).toHaveLength(1);
			await vi.advanceTimersByTimeAsync(1);
			await tick();
			expect(h.snapshots()).toHaveLength(2);
			h.session.receive("session/update", chunk(4, " later"));
			h.respond(h.snapshots()[1], { result: snapshot({ sequence: 2, state: "running" }) });
			await tick();
			expect(p.stale).toBe(false);
			expect(p.value.state).toBe("running");
			expect(p.value.sequence).toBe(4);
			expect(h.session.getSteeringMessages()).toEqual(["buffered steer"]);
			h.session.receive(
				"session/update",
				update(5, { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" }),
			);
			expect(p.value.sequence).toBe(5);
			expect(p.value.state).toBe("idle");
			await vi.advanceTimersByTimeAsync(60_000);
			await tick();
			expect(h.snapshots()).toHaveLength(2);
			expect(h.errors).toEqual(["Session is not open"]);
		} finally {
			h.dispose();
		}
	});
	it("bounds a persistently failing resynchronization, reports it once, and stops when disposed", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const h = harness((request) => (request.method === "_pirua/session/snapshot" ? notOpen : undefined));
		const p = h.session.projection;
		try {
			h.session.receive("session/update", chunk(2, "after gap"));
			for (let elapsed = 0; elapsed < 10_000; elapsed += 50) {
				h.session.receive("session/update", chunk(3 + elapsed / 50, "."));
				await tick();
				await vi.advanceTimersByTimeAsync(50);
			}
			await tick();
			// Immediate attempt, then 250 ms doubling to a 5 s cap: 0, 0.25, 0.75, 1.75, 3.75, 7.75 s.
			expect(h.snapshots()).toHaveLength(6);
			expect(h.errors).toEqual(["Session is not open"]);
			expect(p.stale).toBe(true);
			expect(p.value.sequence).toBe(0);
			await vi.advanceTimersByTimeAsync(5_000);
			await tick();
			expect(h.snapshots()).toHaveLength(7);
			h.session.dispose();
			await vi.advanceTimersByTimeAsync(60_000);
			await tick();
			expect(h.snapshots()).toHaveLength(7);
			expect(h.errors).toEqual(["Session is not open"]);
		} finally {
			h.dispose();
		}
	});
});
