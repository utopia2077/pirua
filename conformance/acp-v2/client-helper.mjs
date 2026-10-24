import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

export function deadline(promise, milliseconds, description) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out: ${description}`)), milliseconds);
    }),
  ]).finally(() => clearTimeout(timer));
}

export class RpcError extends Error {
  constructor(error) {
    super(error.message);
    this.code = error.code;
    this.data = error.data;
  }
}

/** Independent wire client: it does not import server or production frontend code. */
export class StdioClient {
  messages = [];
  frames = [];
  stderr = "";
  nextId = 1;
  pending = new Map();
  waiters = new Set();
  failure = undefined;
  exited = false;

  constructor(binary, args) {
    this.child = spawn(binary, args, { stdio: ["pipe", "pipe", "pipe"] });
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", (text) => {
      this.stderr = (this.stderr + text).slice(-8192);
    });
    this.child.on("error", (error) => this.fail(error));
    this.child.stdin.on("error", (error) => this.fail(error));
    this.exit = new Promise((resolve) => {
      this.child.on("exit", (code, signal) => {
        this.exited = true;
        this.fail(new Error(`Server exited (${code ?? signal}); ${this.stderr}`));
        resolve({ code, signal });
      });
    });
    const lines = createInterface({ input: this.child.stdout, crlfDelay: Infinity });
    lines.on("line", (line) => {
      try {
        const frame = JSON.parse(line);
        this.frames.push(frame);
        for (const message of Array.isArray(frame) ? frame : [frame]) this.receive(message);
      } catch (error) {
        this.fail(new Error(`Invalid stdout protocol frame: ${error.message}`));
      }
    });
  }

  receive(message) {
    if (!message || typeof message !== "object" || message.jsonrpc !== "2.0") {
      throw new Error("Every protocol message must use the JSON-RPC 2.0 envelope");
    }
    this.messages.push(message);
    if (Object.hasOwn(message, "id") && !Object.hasOwn(message, "method")) {
      const pending = this.pending.get(message.id);
      if (pending) {
        this.pending.delete(message.id);
        if (Object.hasOwn(message, "error")) pending.reject(new RpcError(message.error));
        else if (Object.hasOwn(message, "result")) pending.resolve(message.result);
        else pending.reject(new Error("Response is missing result/error"));
      }
    }
    for (const waiter of this.waiters) {
      if (this.messages.length - 1 >= waiter.from && waiter.predicate(message)) {
        this.waiters.delete(waiter);
        waiter.resolve(message);
      }
    }
  }

  fail(error) {
    this.failure ??= error;
    for (const { reject } of this.pending.values()) reject(error);
    this.pending.clear();
    for (const waiter of this.waiters) waiter.reject(error);
    this.waiters.clear();
  }

  raw(value) {
    if (this.failure) throw this.failure;
    this.child.stdin.write(typeof value === "string" ? value : `${JSON.stringify(value)}\n`);
  }

  allocate(method, params) {
    const id = this.nextId++;
    const response = new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
    const result = deadline(response, 15_000, `${method} response`).finally(() => this.pending.delete(id));
    return { id, message: { jsonrpc: "2.0", id, method, params }, result };
  }

  sendRequest(method, params) {
    const request = this.allocate(method, params);
    this.raw(request.message);
    return request;
  }

  request(method, params) {
    return this.sendRequest(method, params).result;
  }

  notify(method, params) {
    this.raw({ jsonrpc: "2.0", method, params });
  }

  waitFor(predicate, from = 0) {
    const existing = this.messages.slice(from).find(predicate);
    if (existing) return Promise.resolve(existing);
    if (this.failure) return Promise.reject(this.failure);
    let waiter;
    const promise = new Promise((resolve, reject) => {
      waiter = { predicate, from, resolve, reject };
      this.waiters.add(waiter);
    });
    return deadline(promise, 15_000, "protocol notification").finally(() => this.waiters.delete(waiter));
  }

  async initialize(pi = true) {
    return this.request("initialize", {
      protocolVersion: 2,
      info: { name: "pirua-wire-conformance", version: "1" },
      capabilities: pi ? { _meta: { "pirua.dev/tui": { version: 1 } } } : {},
    });
  }

  async newSession(cwd) {
    // ACP v2 new sessions default to idle. An initial idle notification is optional;
    // unlike resume, new does not require a separate synchronization notification.
    return this.request("session/new", { cwd, mcpServers: [] });
  }

  async finish() {
    this.child.stdin.end();
    return deadline(this.exit, 5000, "graceful server EOF exit");
  }

  async cleanup() {
    if (this.exited) return;
    this.child.stdin.end();
    try {
      await deadline(this.exit, 2000, "test cleanup");
    } catch {
      this.child.kill("SIGKILL");
      await this.exit;
    }
  }
}

export function isState(message, sessionId, state) {
  return message.method === "session/update" && message.params?.sessionId === sessionId
    && message.params.update?.sessionUpdate === "state_update" && message.params.update.state === state;
}

export function updates(client, from = 0) {
  return client.messages.slice(from).filter((message) => message.method === "session/update")
    .map((message) => message.params.update);
}
