import { PassThrough } from "node:stream";
import { afterEach, describe, it, expect, vi } from "vitest";
import { AcpClient, CANCEL_GRACE_MS } from "../src/acp/client.ts";
const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
function connection() {
	const input = new PassThrough(),
		output = new PassThrough();
	const written: unknown[] = [];
	output.on("data", (data) => written.push(...data.toString().trim().split("\n").map(JSON.parse)));
	const client = new AcpClient(input, output);
	return { input, output, client, written };
}
describe("ACP receive pump", () => {
	it("delivers notifications while a reverse request is waiting for a UI dialog", async () => {
		const c = connection();
		const notifications: string[] = [];
		c.client.onNotification((m) => notifications.push(m));
		let finish!: (v: unknown) => void;
		c.client.onRequest = () => new Promise((r) => (finish = r));
		c.input.write(JSON.stringify({ jsonrpc: "2.0", id: 88, method: "elicitation/create", params: {} }) + "\n");
		c.input.write(JSON.stringify({ jsonrpc: "2.0", method: "session/update", params: {} }) + "\n");
		await tick();
		expect(notifications).toEqual(["session/update"]);
		expect(c.written).toHaveLength(0);
		finish({ action: "cancel" });
		await tick();
		expect(c.written).toEqual([{ jsonrpc: "2.0", id: 88, result: { action: "cancel" } }]);
		c.client.close();
	});
	it("emits no response for a notification-only batch and preserves request IDs", async () => {
		const c = connection();
		c.input.write(
			JSON.stringify([
				{ jsonrpc: "2.0", method: "notice", params: {} },
				{ jsonrpc: "2.0", method: "notice", params: {} },
			]) + "\n",
		);
		await tick();
		expect(c.written).toEqual([]);
		const promise = c.client.request("work", {});
		expect(c.written).toMatchObject([{ id: 1, method: "work" }]);
		c.input.write(JSON.stringify({ jsonrpc: "2.0", id: 1, result: { ok: true } }) + "\n");
		await expect(promise).resolves.toEqual({ ok: true });
		c.client.close();
	});
	it("rejects pending work on EOF without leaving the caller waiting", async () => {
		const c = connection();
		const pending = c.client.request("work", {});
		const expectation = expect(pending).rejects.toThrow("closed");
		c.input.end();
		await expectation;
		c.client.close();
	});
	it("sends request cancellation without replaying a potentially inserted prompt", async () => {
		const c = connection();
		const controller = new AbortController();
		const pending = c.client.request("session/prompt", { sessionId: "s1", prompt: [] }, controller.signal);
		controller.abort(new Error("cancel"));
		c.input.write(JSON.stringify({ jsonrpc: "2.0", id: 1, result: { messageId: "inserted" } }) + "\n");
		await expect(pending).resolves.toEqual({ messageId: "inserted" });
		expect(c.written).toHaveLength(2);
		expect(c.written[1]).toEqual({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 1 } });
		c.client.close();
	});
});
describe("aborted requests", () => {
	afterEach(() => vi.useRealTimers());
	it("end locally with the abort reason when the server stays silent, and ignore its late reply", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const c = connection();
		const controller = new AbortController();
		const pending = c.client.request("_pirua/session/snapshot", { sessionId: "s1" }, controller.signal);
		const outcome = pending.then(
			(value) => ({ value }),
			(error) => ({ error }),
		);
		const reason = new Error("deadline");
		controller.abort(reason);
		expect(c.written.at(-1)).toEqual({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 1 } });
		await vi.advanceTimersByTimeAsync(CANCEL_GRACE_MS - 1);
		let settled = false;
		void outcome.then(() => (settled = true));
		await Promise.resolve();
		expect(settled).toBe(false);
		await vi.advanceTimersByTimeAsync(1);
		expect(await outcome).toEqual({ error: reason });
		expect(vi.getTimerCount()).toBe(0);
		// The reply that finally arrives has nobody waiting for it and must not disturb the connection.
		const written = c.written.length;
		c.input.write(JSON.stringify({ jsonrpc: "2.0", id: 1, result: { late: true } }) + "\n");
		await vi.advanceTimersByTimeAsync(0);
		const next = c.client.request("work", {});
		c.input.write(JSON.stringify({ jsonrpc: "2.0", id: 2, result: { ok: true } }) + "\n");
		await expect(next).resolves.toEqual({ ok: true });
		expect(c.written).toHaveLength(written + 1);
		c.client.close();
	});
	it("still deliver a reply that arrives within the grace period and leave no timer behind", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const c = connection();
		const controller = new AbortController();
		const pending = c.client.request("_pirua/session/bash", { sessionId: "s1", command: "sleep 9" }, controller.signal);
		controller.abort(new Error("Shell cancelled"));
		expect(vi.getTimerCount()).toBe(1);
		await vi.advanceTimersByTimeAsync(CANCEL_GRACE_MS - 1);
		c.input.write(JSON.stringify({ jsonrpc: "2.0", id: 1, result: { cancelled: true, output: "partial" } }) + "\n");
		await expect(pending).resolves.toEqual({ cancelled: true, output: "partial" });
		expect(vi.getTimerCount()).toBe(0);
		c.client.close();
	});
});
