import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { StdioClient, deadline, isState, updates } from "./client-helper.mjs";

const binary = process.env.PIRUA_ACP_TEST_BIN ?? resolve("target/debug/pi-acp-server");
const text = (value) => [{ type: "text", text: value }];
const prompt = (client, sessionId, value) => client.request("session/prompt", { sessionId, prompt: text(value) });
const snapshot = (client, sessionId) => client.request("_pirua/session/snapshot", { sessionId });

async function setup(t, { delay = 25, pi = true } = {}) {
  assert.ok(existsSync(binary), "Build pi-acp-server and set PIRUA_ACP_TEST_BIN to its executable");
  const directory = await mkdtemp(join(tmpdir(), "pirua-acp-conformance-"));
  const args = ["--data-dir", directory, "--config-dir", join(directory, "config"),
    "--fixture-provider", "--fixture-delay-ms", String(delay)];
  const clients = [];
  const start = () => {
    const client = new StdioClient(binary, args);
    clients.push(client);
    return client;
  };
  t.after(async () => {
    await Promise.all(clients.map((client) => client.cleanup()));
    await rm(directory, { recursive: true, force: true });
  });
  const client = start();
  const initialized = await client.initialize(pi);
  assert.equal(initialized.protocolVersion, 2);
  assert.ok(initialized.capabilities?.session);
  return { client, initialized, directory, start };
}

test("ordinary ACP client gets an early insertion receipt and cancellation outcome", async (t) => {
  const { client, directory } = await setup(t, { pi: false });
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const receipt = await prompt(client, sessionId, "fixture:block");
  assert.equal(typeof receipt.messageId, "string");
  await client.waitFor((message) => message.params?.update?.sessionUpdate === "user_message"
    && message.params.update.messageId === receipt.messageId, from);
  assert.equal(updates(client, from).filter((update) => update.state === "idle").length, 0);
  client.notify("session/cancel", { sessionId });
  const stopped = await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  assert.equal(stopped.params.update.stopReason, "cancelled");
  assert.equal(client.messages.slice(from).some((message) => message.method === "_pirua/session/changed"), false);
});

test("request cancellation before insertion returns cancelled without a user message", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const pending = client.sendRequest("session/prompt", { sessionId, prompt: text("fixture:before-insert") });
  const outcome = pending.result.then((value) => ({ value }), (error) => ({ error }));
  await client.waitFor((message) => isState(message, sessionId, "running"), from);
  client.notify("$/cancel_request", { requestId: pending.id });
  assert.equal((await outcome).error?.code, -32800);
  assert.equal((await client.waitFor((message) => isState(message, sessionId, "idle"), from)).params.update.stopReason, "cancelled");
  assert.equal(updates(client, from).some((update) => update.sessionUpdate === "user_message"), false);
});

for (const operation of ["session/cancel", "session/close"]) {
  for (const input of ["fixture:before-insert", "fixture:block"]) {
    test(`${operation} settles a batched prompt during admission (${input})`, async (t) => {
      const { client, directory } = await setup(t, { pi: false });
      const { sessionId } = await client.newSession(directory);
      const from = client.messages.length;
      const pending = client.allocate("session/prompt", { sessionId, prompt: text(input) });
      const stop = client.allocate(operation, { sessionId });
      const outcome = pending.result.then((value) => ({ value }), (error) => ({ error }));
      const stopped = Promise.all([outcome, stop.result]);
      client.raw([pending.message, stop.message]);
      let result;
      try {
        result = await deadline(stopped, 2000, `${operation} must settle pending prompt admission`);
      } finally {
        // Release the old failure path so the regression itself never leaves work alive.
        client.notify("$/cancel_request", { requestId: pending.id });
        client.notify("session/cancel", { sessionId });
        await stopped;
      }
      const idle = await client.waitFor((message) => isState(message, sessionId, "idle"), from);
      assert.equal(idle.params.update.stopReason, "cancelled");
      const users = updates(client, from).filter((update) => update.sessionUpdate === "user_message");
      if (input === "fixture:before-insert" || result[0].error) {
        assert.equal(result[0].error?.code, -32800);
        assert.equal(users.length, 0);
      } else {
        assert.equal(typeof result[0].value.messageId, "string");
        assert.deepEqual(users.map((update) => update.messageId), [result[0].value.messageId]);
      }
      const responseAt = client.messages.findIndex((message) => message.id === stop.id && Object.hasOwn(message, "result"));
      assert.ok(client.messages.indexOf(idle) < responseAt, "idle/cancelled must precede the stop reply");
    });
  }
}

test("request cancellation after insertion preserves the receipt and lets the turn finish", async (t) => {
  const { client, directory } = await setup(t, { delay: 100 });
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const pending = client.sendRequest("session/prompt", { sessionId, prompt: text("retained after request cancellation") });
  await client.waitFor((message) => message.params?.update?.sessionUpdate === "user_message", from);
  client.notify("$/cancel_request", { requestId: pending.id });
  const receipt = await pending.result;
  const idle = await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  assert.equal(idle.params.update.stopReason, "end_turn");
  assert.equal((await snapshot(client, sessionId)).history.entries.some((entry) => entry.id === receipt.messageId), true);
});

test("EOF cancels a prompt still waiting in admission", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const pending = client.sendRequest("session/prompt", { sessionId, prompt: text("fixture:before-insert") });
  const outcome = pending.result.catch((error) => error);
  await client.waitFor((message) => isState(message, sessionId, "running"));
  assert.equal((await client.finish()).code, 0);
  await outcome;
});

test("queue clear identifies duplicate inputs and cancellation does not restart them", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  await prompt(client, sessionId, "fixture:block");
  const steering = await client.request("_pirua/session/queue", { sessionId, delivery: "steer", prompt: text("same") });
  const followUp = await client.request("_pirua/session/queue", { sessionId, delivery: "followUp", prompt: text("same") });
  assert.equal(steering.disposition, "queued");
  assert.equal(followUp.disposition, "queued");
  assert.notEqual(steering.submissionId, followUp.submissionId);
  assert.equal(steering.messageId, undefined);
  const queued = await snapshot(client, sessionId);
  assert.equal(queued.queue.steering[0].submissionId, steering.submissionId);
  assert.equal(queued.queue.followUp[0].submissionId, followUp.submissionId);
  const { removed } = await client.request("_pirua/session/clear_queue", { sessionId });
  assert.deepEqual(removed.map((item) => item.submissionId).sort(), [steering.submissionId, followUp.submissionId].sort());
  await client.request("_pirua/session/queue", { sessionId, delivery: "followUp", prompt: text("cancel me") });
  client.notify("session/cancel", { sessionId });
  await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  const after = await snapshot(client, sessionId);
  assert.deepEqual(after.queue, { steering: [], followUp: [] });
  assert.equal(after.state, "idle");
  assert.equal(updates(client, from).filter((update) => update.sessionUpdate === "user_message").length, 1);
});

test("consumed identical queued inputs resolve to distinct committed message IDs", async (t) => {
  const { client, directory } = await setup(t, { delay: 250 });
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const first = await prompt(client, sessionId, "start queued work");
  const receipts = [];
  for (const delivery of ["steer", "followUp"]) {
    receipts.push(await client.request("_pirua/session/queue", { sessionId, delivery, prompt: text("same") }));
  }
  const resolutions = await Promise.all(receipts.map((receipt) => client.waitFor((message) =>
    message.method === "_pirua/session/changed" && message.params?.inputResolution?.submissionId === receipt.submissionId
    && message.params.inputResolution.outcome === "inserted", from)));
  await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  const ids = resolutions.map((message) => message.params.inputResolution.messageId);
  assert.equal(new Set([first.messageId, ...ids]).size, 3);
  const users = updates(client, from).filter((update) => update.sessionUpdate === "user_message");
  for (const id of ids) assert.ok(users.some((update) => update.messageId === id));
});

test("retained user IDs survive process restart; every resume synchronizes state", async (t) => {
  const { client, directory, start } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const receipt = await prompt(client, sessionId, "persist this message");
  await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  await client.request("session/close", { sessionId });
  assert.equal((await client.finish()).code, 0);
  const reopened = start();
  await reopened.initialize();
  const replayStart = reopened.messages.length;
  const replay = reopened.sendRequest("session/resume", { sessionId, cwd: directory, replayFrom: { type: "start" } });
  await replay.result;
  const responseAt = reopened.messages.findIndex((message) => message.id === replay.id && Object.hasOwn(message, "result"));
  const beforeResponse = reopened.messages.slice(replayStart, responseAt);
  assert.ok(beforeResponse.some((message) => message.params?.update?.messageId === receipt.messageId));
  assert.ok(beforeResponse.some((message) => isState(message, sessionId, "idle")));
  await reopened.request("session/close", { sessionId });
  const noReplayStart = reopened.messages.length;
  const resume = reopened.sendRequest("session/resume", { sessionId, cwd: directory });
  await resume.result;
  const resumed = reopened.messages.slice(noReplayStart,
    reopened.messages.findIndex((message) => message.id === resume.id && Object.hasOwn(message, "result")));
  assert.ok(resumed.some((message) => isState(message, sessionId, "idle")));
  assert.equal(resumed.some((message) => message.params?.update?.sessionUpdate === "user_message"), false);
});

test("fork before and at preserve source owner and user-entry semantics", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const receipt = await prompt(client, sessionId, "fork target");
  await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  const original = await snapshot(client, sessionId);
  const entryId = receipt.messageId;
  assert.ok(original.history.entries.some((entry) => entry.id === entryId));
  const before = await client.request("_pirua/session/fork", { sessionId, entryId, position: "before", expectedRevision: original.history.revision });
  assert.equal(before.cancelled, false);
  assert.notEqual(before.sessionId, sessionId);
  assert.equal(before.editorText, "fork target");
  assert.equal(before.snapshot.history.entries.some((entry) => entry.id === entryId), false);
  const at = await client.request("_pirua/session/fork", { sessionId, entryId, position: "at", expectedRevision: original.history.revision });
  assert.notEqual(at.sessionId, sessionId);
  assert.notEqual(at.sessionId, before.sessionId);
  assert.ok(at.snapshot.history.entries.some((entry) => entry.id === entryId));
  const stillOriginal = await snapshot(client, sessionId);
  assert.equal(stillOriginal.history.header.id, sessionId);
  assert.deepEqual(stillOriginal.history, original.history);
});

test("snapshot cursor retains materialized in-flight text already sent to the client", async (t) => {
  const { client, directory } = await setup(t, { delay: 300 });
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  await prompt(client, sessionId, "snapshot during streaming");
  const delta = await client.waitFor((message) => message.params?.update?.sessionUpdate === "agent_message_chunk", from);
  const current = await snapshot(client, sessionId);
  const cursor = delta.params._meta["pirua.dev/projection"];
  assert.equal(current.epoch, cursor.epoch);
  assert.ok(current.sequence >= cursor.sequence);
  if (current.state === "running") {
    const entity = current.liveEntities.find((item) => item.sessionUpdate === "agent_message"
      && item.messageId === delta.params.update.messageId);
    assert.ok(entity, "snapshot must include the identified assistant whose covered chunk is discarded");
    assert.ok(entity.content.map((block) => block.text ?? "").join("").includes(delta.params.update.content.text));
    const native = entity._meta["pirua.dev/render"].message;
    assert.equal(native.content[0].type, "thinking");
    assert.equal(native.content[1].type, "text");
    assert.ok(native.content[1].text.includes(delta.params.update.content.text));
  } else {
    assert.ok(current.messages.some((message) => message.role === "assistant"
      && message.content.some((block) => block.text?.includes("snapshot during streaming"))));
  }
  await client.waitFor((message) => isState(message, sessionId, "idle"), from);
});

test("native retry has one final idle transition and length retains its stop reason", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  let from = client.messages.length;
  await prompt(client, sessionId, "fixture:retry");
  const settled = await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  assert.equal(settled.params.update.stopReason, "end_turn");
  assert.equal(updates(client, from).filter((update) => update.state === "idle").length, 1);
  assert.ok(updates(client, from).some((update) => update.sessionUpdate === "agent_message_chunk"
    && update.content.text?.includes("Fixture response")));
  from = client.messages.length;
  await prompt(client, sessionId, "fixture:length");
  assert.equal((await client.waitFor((message) => isState(message, sessionId, "idle"), from)).params.update.stopReason, "max_tokens");
});

test("known malformed input and unsupported replay cursors are parameter errors", async (t) => {
  const { client, directory, start } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  await assert.rejects(client.request("session/prompt", { sessionId, prompt: [{ type: "text" }] }),
    (error) => error.code === -32602);
  await assert.rejects(client.request("session/resume", { sessionId, cwd: directory, replayFrom: { type: "unknown_cursor" } }),
    (error) => error.code === -32602);
  const oldVersion = start();
  await assert.rejects(oldVersion.request("initialize", { protocolVersion: 1, info: { name: "old-client", version: "1" } }),
    (error) => error.code === -32022 && error.data.requested === 1 && error.data.supported.includes(2));
});

test("a provider failure after insertion preserves the receipt and emits a typed error outcome", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const receipt = await prompt(client, sessionId, "fixture:error");
  assert.equal(typeof receipt.messageId, "string");
  const stopped = await client.waitFor((message) => isState(message, sessionId, "idle"), from);
  assert.equal(stopped.params.update.stopReason, "error");
  assert.equal(typeof stopped.params.update.error.code, "number");
  assert.equal(typeof stopped.params.update.error.message, "string");
  assert.ok(updates(client, from).some((update) => update.sessionUpdate === "user_message"
    && update.messageId === receipt.messageId));
});

test("unknown configuration value variants cannot perform known mutations", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const before = await snapshot(client, sessionId);
  assert.equal(before.settings.retry.enabled, true);
  await assert.rejects(client.request("session/set_config_option", {
    sessionId, configId: "autoRetry", type: "future_value_shape", value: false,
  }), (error) => typeof error.code === "number");
  const unchanged = await snapshot(client, sessionId);
  assert.equal(unchanged.settings.retry.enabled, true);
  assert.equal(unchanged.history.revision, before.history.revision);
  await client.request("session/set_config_option", {
    sessionId, configId: "autoRetry", type: "boolean", value: false,
  });
  assert.equal((await snapshot(client, sessionId)).settings.retry.enabled, false);
});

test("JSON-RPC batch results use a response array and notifications have no reply", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  const first = client.allocate("_pirua/session/snapshot", { sessionId });
  const second = client.allocate("session/list", {});
  client.raw([first.message, { jsonrpc: "2.0", method: "unknown/notification", params: {} }, second.message]);
  await Promise.all([first.result, second.result]);
  assert.ok(client.frames.some((frame) => Array.isArray(frame)
    && frame.some((message) => message.id === first.id) && frame.some((message) => message.id === second.id)));
  assert.equal(client.messages.slice(from).filter((message) => Object.hasOwn(message, "id")).length, 2);
});

test("close during work publishes cancelled idle before replying and preserves history", async (t) => {
  const { client, directory } = await setup(t);
  const { sessionId } = await client.newSession(directory);
  const from = client.messages.length;
  await prompt(client, sessionId, "fixture:block");
  const close = client.sendRequest("session/close", { sessionId });
  await close.result;
  const responseAt = client.messages.findIndex((message) => message.id === close.id && Object.hasOwn(message, "result"));
  const stoppedAt = client.messages.findIndex((message, index) => index >= from && isState(message, sessionId, "idle"));
  assert.ok(stoppedAt >= from && stoppedAt < responseAt);
  assert.equal(client.messages[stoppedAt].params.update.stopReason, "cancelled");
  const listed = await client.request("session/list", {});
  assert.ok(listed.sessions.some((session) => session.sessionId === sessionId));
  assert.equal((await client.finish()).code, 0);
});
