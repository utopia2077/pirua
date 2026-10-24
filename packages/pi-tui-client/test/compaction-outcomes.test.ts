import { describe, expect, it, vi } from "vitest";
import { SessionProjection } from "../src/session/mirror.ts";
import { PROJECTION, RENDER } from "../src/acp/types.ts";
import type { AgentSessionEvent } from "../src/display/types.ts";
import { snapshot } from "./fixtures.ts";
import { InteractiveMode } from "../src/modes/interactive/interactive-mode.ts";
import { PassThrough } from "node:stream";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { AcpClient } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { initTheme } from "../src/modes/interactive/theme/theme.ts";
import { stripAnsi } from "../src/utils/ansi.ts";
import { VirtualTerminal } from "../../pi-tui/test/virtual-terminal.ts";

const prior = {
	id: "prior-compact",
	parentId: null,
	type: "compaction",
	timestamp: 1000,
	summary: "OLD SUMMARY",
	tokensBefore: 1234,
	firstKeptEntryId: "kept",
};
const update = (sequence: number, value: any) => ({
	sessionId: "s1",
	_meta: { [PROJECTION]: { epoch: "epoch1", sequence } },
	update: value,
});
const status = (compactionId: string, value: string, extra: any = {}) => ({
	sessionUpdate: "compaction_update",
	compactionId,
	status: value,
	...extra,
});
const activity = (sequence: number, compacting: boolean) => ({
	sessionId: "s1",
	epoch: "epoch1",
	sequence,
	changes: { activity: { ...snapshot().activity, compacting } },
});
function projection(withPrior = true) {
	const initial = snapshot({
		state: "running",
		history: {
			...snapshot().history,
			revision: withPrior ? 1 : 0,
			leafId: withPrior ? prior.id : null,
			entries: withPrior ? [prior] : [],
		},
		activity: { ...snapshot().activity, compacting: true },
	});
	const value = new SessionProjection(initial);
	const events: AgentSessionEvent[] = [];
	value.subscribe((event) => events.push(event));
	return {
		value,
		initial,
		events,
		ends: () => events.filter((event) => event.type === "compaction_end"),
	};
}

describe("compaction operation outcomes", () => {
	it.each(["cancelled", "failed"])("does not return an old facade result after %s", async (outcome) => {
		const client = new AcpClient(new PassThrough(), new PassThrough());
		const host = new TuiRuntimeHost(client, {
			cwd: "/unused",
			settings: new ClientSettings("/unused/compaction-ui.json"),
		});
		host.session = new TuiSession(host, { ...projection().initial, state: "idle", activity: snapshot().activity });
		vi.spyOn(host.session, "prompt").mockImplementation(async () => {
			host.session.receive("session/update", update(1, status("current", "in_progress")));
			host.session.receive("_pirua/session/changed", activity(2, true));
			host.session.receive(
				"session/update",
				update(3, status("current", outcome, { error: outcome === "failed" ? "Current failure" : undefined })),
			);
			host.session.receive("_pirua/session/changed", activity(4, false));
			return {};
		});
		vi.spyOn(host.session, "refresh").mockResolvedValue();
		try {
			await expect(host.session.compact()).resolves.toBeUndefined();
		} finally {
			host.session.dispose();
			client.close();
		}
	});
	it.each([
		{ status: "cancelled", withPrior: false, aborted: true, error: undefined },
		{ status: "cancelled", withPrior: true, aborted: true, error: undefined },
		{
			status: "failed",
			withPrior: false,
			aborted: false,
			error: "Current compaction failed",
		},
		{
			status: "failed",
			withPrior: true,
			aborted: false,
			error: "Current compaction failed",
		},
	])("preserves $status with prior summary=$withPrior", async ({ status: outcome, withPrior, aborted, error }) => {
		const c = projection(withPrior);
		c.value.receive("session/update", update(1, status("current", outcome, { error })));
		c.value.receive("_pirua/session/changed", activity(2, false));
		expect(c.ends()).toEqual([
			{
				type: "compaction_end",
				reason: "manual",
				aborted,
				errorMessage: error,
				result: undefined,
				willRetry: false,
			},
		]);
		const ui: any = Object.create(InteractiveMode.prototype);
		ui.isInitialized = true;
		ui.runtimeHost = {
			session: { settingsManager: { getShowTerminalProgress: () => false } },
		};
		ui.footer = { invalidate: vi.fn() };
		ui.programStatus = { handleEvent: vi.fn() };
		ui.ui = { requestRender: vi.fn() };
		ui.clearStatusIndicator = vi.fn();
		ui.showError = vi.fn();
		ui.addMessageToChat = vi.fn();
		await ui.handleEvent(c.ends()[0]);
		expect(ui.showError).toHaveBeenCalledExactlyOnceWith(aborted ? "Compaction cancelled" : error);
		expect(ui.addMessageToChat).not.toHaveBeenCalled();
	});

	it("emits one start and completion for the active identity and ignores another operation's outcome", () => {
		const c = projection();
		c.value.receive("session/update", update(1, status("current", "in_progress")));
		c.value.receive("_pirua/session/changed", activity(2, true));
		c.value.receive("session/update", update(3, status("unrelated", "cancelled")));
		expect(c.ends()).toEqual([]);
		c.value.receive("session/update", update(4, status("current", "failed", { error: { message: "Current error" } })));
		c.value.receive("session/update", update(5, status("current", "failed", { error: { message: "Current error" } })));
		c.value.receive("_pirua/session/changed", activity(6, false));
		expect(c.events.filter((event) => event.type === "compaction_start")).toHaveLength(1);
		expect(c.ends()).toEqual([
			expect.objectContaining({
				errorMessage: "Current error",
				aborted: false,
				result: undefined,
			}),
		]);
		expect(c.value.entities.get("compaction_update:current")).toMatchObject({
			status: "failed",
			compactionId: "current",
		});
	});

	it("only attaches the committed result explicitly linked to this successful operation", () => {
		const c = projection();
		const result = {
			...prior,
			id: "current-entry",
			parentId: prior.id,
			summary: "CURRENT SUMMARY",
		};
		const unrelated = {
			...prior,
			id: "unrelated-entry",
			parentId: result.id,
			summary: "UNRELATED SUMMARY",
		};
		c.value.receive("session/update", update(1, status("current", "in_progress")));
		c.value.receive("_pirua/session/changed", {
			...activity(2, true),
			historyChange: {
				kind: "append",
				baseRevision: 1,
				revision: 3,
				leafId: unrelated.id,
				appended: [result, unrelated],
			},
		});
		c.value.receive(
			"session/update",
			update(
				3,
				status("current", "completed", {
					_meta: { [RENDER]: { entryId: result.id } },
				}),
			),
		);
		c.value.receive("_pirua/session/changed", activity(4, false));
		expect(c.ends()).toEqual([
			expect.objectContaining({
				result,
				aborted: false,
				errorMessage: undefined,
			}),
		]);
	});

	it("does not attach a prior summary when a completed outcome has no matching committed entry", () => {
		const c = projection();
		c.value.receive("session/update", update(1, status("current", "completed")));
		c.value.receive("_pirua/session/changed", activity(2, false));
		expect(c.ends()).toEqual([expect.objectContaining({ result: undefined })]);
	});

	it.each(["cancelled", "failed"])("keeps a covered %s outcome during snapshot hydration", (outcome) => {
		const c = projection();
		c.value.receive("session/update", update(1, status("current", "in_progress")));
		c.value.beginHydration();
		c.value.receive(
			"session/update",
			update(
				2,
				status("current", outcome, {
					error: outcome === "failed" ? "Hydrated failure" : undefined,
				}),
			),
		);
		c.value.receive("_pirua/session/changed", activity(3, false));
		const final = {
			...c.initial,
			sequence: 3,
			state: "idle" as const,
			activity: snapshot().activity,
			liveEntities: [],
		};
		c.value.install(final);
		c.value.install(final);
		expect(c.ends()).toEqual([
			expect.objectContaining({
				aborted: outcome === "cancelled",
				errorMessage: outcome === "failed" ? "Hydrated failure" : undefined,
				result: undefined,
			}),
		]);
	});

	it("retains the active compaction identity through a running snapshot", () => {
		const c = projection();
		c.value.install({
			...c.initial,
			sequence: 1,
			liveEntities: [status("current", "in_progress")],
		});
		c.value.receive("session/update", update(2, status("unrelated", "cancelled")));
		c.value.receive("session/update", update(3, status("current", "cancelled")));
		c.value.receive("_pirua/session/changed", activity(4, false));
		expect(c.ends()).toEqual([expect.objectContaining({ aborted: true, result: undefined })]);
	});

	it("keeps the outcome when both the operation start and failure are covered by hydration", () => {
		const c = projection();
		c.value.install({ ...c.initial, state: "idle", activity: snapshot().activity });
		c.value.beginHydration();
		c.value.receive("session/update", update(1, status("current", "in_progress")));
		c.value.receive("session/update", update(2, status("current", "failed", { error: "Covered operation failed" })));
		c.value.install({ ...c.initial, sequence: 2, state: "idle", activity: snapshot().activity });
		expect(c.ends()).toEqual([
			expect.objectContaining({ aborted: false, errorMessage: "Covered operation failed", result: undefined }),
		]);
	});

	it("recovers this operation's terminal entity from a snapshot without replaying a prior success", () => {
		const c = projection();
		c.value.receive("session/update", update(1, status("current", "in_progress")));
		c.value.install({
			...c.initial,
			sequence: 3,
			activity: snapshot().activity,
			liveEntities: [status(prior.id, "completed"), status("current", "cancelled")],
		});
		expect(c.ends()).toEqual([expect.objectContaining({ aborted: true, result: undefined })]);
		c.value.install({
			...c.initial,
			sequence: 4,
			activity: snapshot().activity,
			liveEntities: [status(prior.id, "completed")],
		});
		expect(c.ends()).toHaveLength(1);
	});
});

const binary = process.env.PIRUA_TEST_SERVER;
describe.skipIf(!binary)("native compaction completion seam", () => {
	it("returns and presents this operation's committed summary and usage after an earlier compaction", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pirua-compaction-outcome-"));
		const child = spawn(
			binary!,
			["--data-dir", join(directory, "data"), "--config-dir", join(directory, "config"), "--fixture-provider"],
			{ stdio: ["pipe", "pipe", "pipe"] },
		);
		const client = new AcpClient(child.stdout, child.stdin);
		child.on("error", (error) => client.fail(error));
		let stderr = "";
		child.stderr.on("data", (chunk) => {
			stderr += chunk;
		});
		const host = new TuiRuntimeHost(client, {
			child,
			cwd: directory,
			settings: new ClientSettings(join(directory, "ui.json")),
		});
		let ui: any;
		const standard: any[] = [],
			events: AgentSessionEvent[] = [],
			errors: string[] = [];
		client.onNotification((method, params) => {
			if (method === "session/update" && params.update?.sessionUpdate === "compaction_update")
				standard.push(params.update);
		});
		try {
			await host.initialize();
			initTheme("dark");
			host.options.settings.getShowCacheMissNotices = () => true;
			ui = new InteractiveMode(host, { terminal: new VirtualTerminal(100, 30), tuiMode: "regular" });
			ui.isInitialized = true;
			ui.setToolsExpanded(true);
			ui.subscribeToAgent();
			host.session.subscribe((event) => {
				events.push(event);
			});
			host.session.onError((error) => errors.push(error.message));
			await host.session.prompt("Before the first compaction");
			await host.session.waitForIdle();
			const first = await host.session.compact();
			await host.session.prompt("New content after the earlier summary");
			await host.session.waitForIdle();
			const current = await host.session.compact();
			await vi.waitFor(() => expect(events.filter((event) => event.type === "compaction_end")).toHaveLength(2));
			const terminal = standard.filter((event) => event.status === "completed").at(-1);
			expect(terminal._meta?.[RENDER]?.entryId).toEqual(expect.any(String));
			expect(standard.filter((event) => event.status === "in_progress").at(-1).compactionId).toBe(
				terminal.compactionId,
			);
			const committed = host.session.projection.value.history.entries.find(
				(entry) => entry.id === terminal._meta[RENDER].entryId,
			);
			expect(committed?.id).not.toBe(first?.id);
			expect(committed?.summary).toBe("Fixture compaction summary");
			expect(committed?.usage).toMatchObject({ input: 37, output: 11, totalTokens: 48, cost: { total: 0.0048 } });
			expect(current).toEqual(committed);
			expect(events.filter((event) => event.type === "compaction_end").at(-1)).toMatchObject({
				result: committed,
				aborted: false,
				errorMessage: undefined,
			});
			await vi.waitFor(() => {
				const rendered = ui.chatContainer.render(100).map(stripAnsi).join("\n");
				expect(rendered.match(/Fixture compaction summary/g)).toHaveLength(1);
				expect(rendered.match(/Compaction: 48 tokens billed/g)).toHaveLength(1);
			});
			expect(errors).toEqual([]);
			expect(stderr).toBe("");
		} finally {
			ui?.stop();
			await host.dispose();
			await rm(directory, { recursive: true, force: true });
		}
	}, 15000);
});
