import { describe, it, expect } from "vitest";
import { SessionProjection } from "../src/session/mirror.ts";
import { RENDER, PROJECTION } from "../src/acp/types.ts";
import type { AgentSessionEvent } from "../src/display/types.ts";
import { assistant, snapshot } from "./fixtures.ts";
const update = (sequence: number, payload: Record<string, unknown>) => ({
	sessionId: "s1",
	_meta: { [PROJECTION]: { epoch: "epoch1", sequence } },
	update: payload,
});
const render = (index: number) => ({ [RENDER]: { phase: "delta", nativeMessageId: "a1", contentIndex: index } });
describe("original Pi presentation projection", () => {
	it("keeps distinct native text, thought, tool and later text slots during hydration", () => {
		const partial = assistant([
			{ type: "text", text: "first" },
			{ type: "thinking", thinking: "plan" },
			{ type: "toolCall", id: "tc", name: "read", arguments: { path: "a" } },
			{ type: "text", text: "last" },
		]);
		const p = new SessionProjection(snapshot());
		p.beginHydration();
		p.receive(
			"session/update",
			update(3, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "covered" },
				_meta: render(3),
			}),
		);
		p.receive(
			"session/update",
			update(5, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "!" },
				_meta: render(3),
			}),
		);
		p.install(
			snapshot({
				sequence: 4,
				state: "running",
				liveEntities: [
					{
						sessionUpdate: "agent_message",
						messageId: "a1",
						content: [
							{ type: "text", text: "first" },
							{ type: "text", text: "last" },
						],
						_meta: { [RENDER]: { phase: "delta", nativeMessageId: "a1", message: partial } },
					},
				],
			}),
		);
		expect(p.assistants.get("a1")?.content).toEqual([...partial.content.slice(0, 3), { type: "text", text: "last!" }]);
		expect(p.value.sequence).toBe(5);
		expect(p.replayActive().filter((e) => e.type === "message_start")).toHaveLength(1);
	});
	it("updates only the native slot selected by each small streaming chunk", () => {
		const p = new SessionProjection(snapshot());
		p.receive(
			"session/update",
			update(1, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "A" },
				_meta: render(0),
			}),
		);
		p.receive(
			"session/update",
			update(2, {
				sessionUpdate: "agent_thought_chunk",
				messageId: "a1",
				content: { type: "text", text: "B" },
				_meta: render(1),
			}),
		);
		p.receive(
			"session/update",
			update(3, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "C" },
				_meta: render(2),
			}),
		);
		p.receive(
			"session/update",
			update(4, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "D" },
				_meta: render(2),
			}),
		);
		expect(p.assistants.get("a1")?.content).toEqual([
			{ type: "text", text: "A" },
			{ type: "thinking", thinking: "B" },
			{ type: "text", text: "CD" },
		]);
	});
	it("patches entity metadata at top-level and keeps chunk metadata scoped to its chunk", () => {
		const p = new SessionProjection(snapshot());
		p.receive(
			"session/update",
			update(1, { sessionUpdate: "agent_message", messageId: "a1", content: [], _meta: { keep: { a: 1 }, remove: 1 } }),
		);
		p.receive(
			"session/update",
			update(2, { sessionUpdate: "agent_message", messageId: "a1", _meta: { keep: { b: 2 }, remove: null } }),
		);
		p.receive(
			"session/update",
			update(3, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "x" },
				_meta: { chunkOnly: true },
			}),
		);
		expect(p.entities.get("agent_message:a1")?._meta).toEqual({ keep: { b: 2 } });
	});
	it("preserves tool association, structured output and one completion for shared native identity", () => {
		const p = new SessionProjection(snapshot());
		const events: AgentSessionEvent[] = [];
		p.subscribe((e) => events.push(e));
		p.receive(
			"session/update",
			update(1, {
				sessionUpdate: "agent_message_chunk",
				messageId: "a1",
				content: { type: "text", text: "Hello" },
				_meta: render(0),
			}),
		);
		const meta = { phase: "delta", parentMessageId: "a1", toolCallId: "tc", durationMs: 17 };
		p.receive(
			"session/update",
			update(2, {
				sessionUpdate: "tool_call_update",
				toolCallId: "a1:tc",
				name: "edit",
				status: "in_progress",
				rawInput: { path: "a" },
				_meta: { [RENDER]: meta },
			}),
		);
		p.receive(
			"session/update",
			update(3, {
				sessionUpdate: "tool_call_update",
				toolCallId: "a1:tc",
				status: "completed",
				rawOutput: { content: [{ type: "text", text: "done" }], details: { diff: "+new" } },
				_meta: { [RENDER]: meta },
			}),
		);
		const message = assistant([
			{ type: "text", text: "Hello" },
			{ type: "toolCall", id: "tc", name: "edit", arguments: { path: "a" } },
		]);
		p.receive(
			"session/update",
			update(4, {
				sessionUpdate: "agent_message",
				messageId: "a1",
				_meta: { [RENDER]: { phase: "end", nativeMessageId: "a1", message } },
			}),
		);
		p.receive(
			"session/update",
			update(5, {
				sessionUpdate: "agent_thought",
				messageId: "a1",
				_meta: { [RENDER]: { phase: "end", nativeMessageId: "a1", message } },
			}),
		);
		expect(events.filter((e) => e.type === "message_end")).toHaveLength(1);
		expect(events.find((e) => e.type === "tool_execution_end")).toMatchObject({
			toolCallId: "tc",
			toolName: "edit",
			durationMs: 17,
			result: { details: { diff: "+new" } },
		});
	});
	it("requests resynchronization instead of applying a mismatched history append", () => {
		const p = new SessionProjection(snapshot());
		const reasons: string[] = [];
		p.onResync = (r) => reasons.push(r);
		p.receive("_pirua/session/changed", {
			sessionId: "s1",
			epoch: "epoch1",
			sequence: 1,
			changes: {},
			historyChange: { kind: "append", baseRevision: 4, revision: 5, appended: [], leafId: null },
		});
		expect(reasons).toEqual(["History revision mismatch"]);
		expect(p.value.history.revision).toBe(0);
	});
	describe("the context of a history append", () => {
		const user = (id: string, parentId: string | null, text: string) => ({
			sourceEntry: {
				id,
				parentId,
				timestamp: 1,
				type: "message",
				message: { role: "user", content: text, timestamp: 1 },
			},
			messages: [{ role: "user", content: text, timestamp: 1 }],
		});
		const held = (...entries: ReturnType<typeof user>[]) =>
			snapshot({
				history: {
					header: { id: "s1", cwd: "/workspace/project", timestamp: 0 },
					revision: entries.length,
					leafId: entries.at(-1)?.sourceEntry.id ?? null,
					entries: entries.map((e) => e.sourceEntry),
				},
				contextEntries: entries,
				messages: entries.flatMap((e) => e.messages),
			});
		const append = (sequence: number, added: ReturnType<typeof user>[], rest: Record<string, unknown> = {}) => ({
			sessionId: "s1",
			epoch: "epoch1",
			sequence,
			changes: {},
			historyChange: {
				kind: "append",
				baseRevision: 1,
				revision: 2,
				appended: added.map((e) => e.sourceEntry),
				leafId: added.at(-1)?.sourceEntry.id ?? "a",
				...rest,
			},
		});
		it("extends the held context with the entries an append adds, and only those", () => {
			const p = new SessionProjection(held(user("a", null, "first")));
			const second = user("b", "a", "second");
			p.receive("_pirua/session/changed", append(1, [second], { contextAppended: [second] }));
			expect(p.value.contextEntries.map((e) => e.sourceEntry.id)).toEqual(["a", "b"]);
			expect(p.value.messages.map((m: any) => m.content)).toEqual(["first", "second"]);
			expect(p.value.history.entries.map((e) => e.id)).toEqual(["a", "b"]);
			expect(p.value.history.revision).toBe(2);
			// Entries that add no message still join the context entries.
			const label = { sourceEntry: { id: "c", parentId: "b", timestamp: 2, type: "label", targetId: "a" }, messages: [] };
			p.receive("_pirua/session/changed", {
				...append(2, [], { baseRevision: 2, revision: 3, leafId: "c", appended: [label.sourceEntry], contextAppended: [label] }),
			});
			expect(p.value.contextEntries.map((e) => e.sourceEntry.id)).toEqual(["a", "b", "c"]);
			expect(p.value.messages).toHaveLength(2);
			expect(p.value.sequence).toBe(2);
		});
		it("does not share the held context with the frame that extended it", () => {
			const p = new SessionProjection(held(user("a", null, "first")));
			const second = user("b", "a", "second");
			const frame = append(1, [second], { contextAppended: [second] });
			p.receive("_pirua/session/changed", frame);
			(frame.historyChange.contextAppended as any)[0].messages[0].content = "changed after the fact";
			expect(p.value.messages.map((m: any) => m.content)).toEqual(["first", "second"]);
		});
		it("takes the whole context an older host sends with an append, as before", () => {
			const p = new SessionProjection(held(user("a", null, "first")));
			const second = user("b", "a", "second");
			p.receive("_pirua/session/changed", {
				...append(1, [second]),
				changes: { messages: second.messages, contextEntries: [second] },
			});
			expect(p.value.contextEntries.map((e) => e.sourceEntry.id)).toEqual(["b"]);
			expect(p.value.messages.map((m: any) => m.content)).toEqual(["second"]);
		});
		it("leaves the context alone for an append that carries neither form", () => {
			const p = new SessionProjection(held(user("a", null, "first")));
			p.receive("_pirua/session/changed", append(1, []));
			expect(p.value.contextEntries).toHaveLength(1);
			expect(p.value.messages).toHaveLength(1);
		});
	});
	it("replaces configuration options from a standard config_option_update", () => {
		const p = new SessionProjection(snapshot());
		const configOptions = [
			{
				configId: "thinkingLevel",
				name: "Thinking",
				type: "select",
				currentValue: "high",
				options: [{ value: "high", name: "High" }],
			},
		];
		p.receive("session/update", update(1, { sessionUpdate: "config_option_update", configOptions }));
		expect(p.value.configOptions).toEqual(configOptions);
		expect(p.value.sequence).toBe(1);
	});
});
