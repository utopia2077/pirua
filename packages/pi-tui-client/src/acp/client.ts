import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";

export type RecordValue = Record<string, any>;
export class RpcError extends Error {
	readonly code: number;
	readonly data: unknown;
	constructor(code: number, message: string, data?: unknown) {
		super(message);
		this.name = "RpcError";
		this.code = code;
		this.data = data;
	}
}
interface Pending {
	resolve: (v: any) => void;
	reject: (e: Error) => void;
	cleanup: () => void;
}
export class AcpClient {
	private nextId = 1;
	private pending = new Map<number, Pending>();
	private requests = new Map<string, AbortController>();
	private notifications = new Set<(method: string, params: RecordValue) => void>();
	private failureListeners = new Set<(error: Error) => void>();
	private failure: Error | undefined;
	private input: ReturnType<typeof createInterface>;
	onRequest: ((method: string, params: RecordValue, signal: AbortSignal) => Promise<unknown>) | undefined;
	readonly output: Writable;
	constructor(input: Readable, output: Writable) {
		this.output = output;
		this.input = createInterface({ input, crlfDelay: Infinity });
		this.input.on("line", (line) => {
			if (!line.trim()) return;
			try {
				this.receive(JSON.parse(line));
			} catch (error) {
				this.fail(error instanceof Error ? error : new Error(String(error)));
			}
		});
		this.input.on("close", () => this.fail(new Error("Rust host closed its protocol output")));
		input.on("error", (error) => this.fail(error));
		output.on("error", (error) => this.fail(error));
	}
	onNotification(listener: (method: string, params: RecordValue) => void): () => void {
		this.notifications.add(listener);
		return () => this.notifications.delete(listener);
	}
	onFailure(listener: (error: Error) => void): () => void {
		this.failureListeners.add(listener);
		if (this.failure) queueMicrotask(() => listener(this.failure!));
		return () => this.failureListeners.delete(listener);
	}
	request<T = RecordValue>(method: string, params: RecordValue, signal?: AbortSignal): Promise<T> {
		if (this.failure) return Promise.reject(this.failure);
		if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("Request cancelled"));
		const id = this.nextId++;
		return new Promise<T>((resolve, reject) => {
			const abort = () => {
				this.notify("$/cancel_request", { requestId: id });
			};
			const cleanup = () => signal?.removeEventListener("abort", abort);
			this.pending.set(id, { resolve, reject, cleanup });
			signal?.addEventListener("abort", abort, { once: true });
			try {
				this.send({ jsonrpc: "2.0", id, method, params });
			} catch (error) {
				cleanup();
				this.pending.delete(id);
				reject(error);
			}
		});
	}
	notify(method: string, params: RecordValue): void {
		if (!this.failure) this.send({ jsonrpc: "2.0", method, params });
	}
	private send(value: unknown): void {
		this.output.write(JSON.stringify(value) + "\n");
	}
	private receive(value: unknown): void {
		if (Array.isArray(value)) {
			if (value.length === 0) {
				this.send({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "Empty JSON-RPC batch" } });
				return;
			}
			const responses = value.map((v) => this.route(v));
			void Promise.all(responses)
				.then((items) => {
					const result = items.filter((x) => x !== undefined);
					if (result.length && !this.failure) this.send(result);
				})
				.catch((error) => this.fail(error));
			return;
		}
		void this.route(value)
			.then((response) => {
				if (response !== undefined && !this.failure) this.send(response);
			})
			.catch((error) => this.fail(error));
	}
	private async route(value: unknown): Promise<unknown> {
		if (!value || typeof value !== "object" || Array.isArray(value))
			return { jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid JSON-RPC message" } };
		const m = value as RecordValue;
		if (m.jsonrpc !== "2.0") throw new Error("Rust host sent an invalid JSON-RPC version");
		if (typeof m.method === "string") {
			const params = m.params ?? {};
			if (m.method === "$/cancel_request") {
				this.requests.get(String(params.requestId))?.abort(new Error("Host cancelled request"));
				return;
			}
			if (m.id === undefined) {
				for (const listener of this.notifications) listener(m.method, params);
				return;
			}
			const controller = new AbortController();
			this.requests.set(String(m.id), controller);
			try {
				if (!this.onRequest) throw new RpcError(-32601, `Unsupported client method: ${m.method}`);
				const result = await this.onRequest(m.method, params, controller.signal);
				return { jsonrpc: "2.0", id: m.id, result };
			} catch (error) {
				const e =
					error instanceof RpcError ? error : new RpcError(controller.signal.aborted ? -32800 : -32603, String(error));
				return {
					jsonrpc: "2.0",
					id: m.id,
					error: { code: e.code, message: e.message, ...(e.data === undefined ? {} : { data: e.data }) },
				};
			} finally {
				this.requests.delete(String(m.id));
			}
		}
		if (typeof m.id !== "number") return;
		const pending = this.pending.get(m.id);
		if (!pending) return;
		this.pending.delete(m.id);
		pending.cleanup();
		if (m.error) pending.reject(new RpcError(m.error.code, m.error.message, m.error.data));
		else if (Object.hasOwn(m, "result")) pending.resolve(m.result);
		else pending.reject(new Error("Malformed protocol response"));
	}
	fail(error: Error): void {
		if (this.failure) return;
		this.failure = error;
		for (const request of this.pending.values()) {
			request.cleanup();
			request.reject(error);
		}
		this.pending.clear();
		for (const r of this.requests.values()) r.abort(error);
		this.requests.clear();
		for (const listener of this.failureListeners) listener(error);
	}
	close(): void {
		this.fail(new Error("ACP client closed"));
		this.input.close();
		this.output.end();
	}
}
