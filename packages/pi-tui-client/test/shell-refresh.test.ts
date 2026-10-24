import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { AcpClient } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { InteractiveMode } from "../src/modes/interactive/interactive-mode.ts";
import { BashExecutionComponent } from "../src/modes/interactive/components/bash-execution.ts";
import { initTheme } from "../src/modes/interactive/theme/theme.ts";
import { stripAnsi } from "../src/utils/ansi.ts";
import { VirtualTerminal } from "../../pi-tui/test/virtual-terminal.ts";
import { snapshot } from "./fixtures.ts";

it("retains a completed callback row through a covered running snapshot, then replaces it once", async () => {
  initTheme("dark");
  const client = new AcpClient(new PassThrough(), new PassThrough());
  const host = new TuiRuntimeHost(client, {
    cwd: "/unused",
    settings: new ClientSettings("/unused/shell-ui.json"),
  });
  const entry = (id: string, output: string) => ({
    id,
    parentId: null,
    timestamp: 1000,
    type: "message" as const,
    message: {
      role: "bashExecution",
      command: "same-command",
      output,
      exitCode: 0,
      cancelled: false,
      truncated: false,
      timestamp: 1000,
    },
  });
  const old = entry("old-shell", "OLD OUTPUT");
  const initial = snapshot({
    history: {
      ...snapshot().history,
      entries: [old],
      revision: 1,
      leafId: old.id,
    },
    contextEntries: [{ sourceEntry: old, messages: [old.message] }],
  });
  host.session = new TuiSession(host, initial);
  const ui: any = new InteractiveMode(host, {
    terminal: new VirtualTerminal(100, 30),
    tuiMode: "regular",
  });
  ui.isInitialized = true;
  vi.spyOn(host.session, "executeBash").mockImplementation(
    async (_command, onChunk) => {
      onChunk("NEW OUTPUT");
      return {
        output: "NEW OUTPUT",
        exitCode: 0,
        cancelled: false,
        truncated: false,
      };
    },
  );
  const reconcile = () =>
    ui.handleEvent({
      type: "snapshot_reconciled",
      entries: host.session.sessionManager.buildContextEntries(),
      activeEvents: [],
      state: host.session.projection.value.state,
      activity: host.session.projection.value.activity,
    });
  const rows = () =>
    ui.chatContainer.children.filter(
      (row: any) => row instanceof BashExecutionComponent,
    );
  try {
    await ui.handleBashCommand("same-command");
    const local = rows()[0];
    expect(ui.bashComponent).toBeUndefined();
    host.session.projection.install({
      ...initial,
      sequence: 1,
      state: "running",
      activity: { ...initial.activity, bashRunning: true },
    });
    await reconcile();
    expect(rows()).toHaveLength(2);
    expect(rows()[1]).toBe(local);
    const final = entry("new-shell", "NEW OUTPUT");
    host.session.projection.install({
      ...initial,
      sequence: 2,
      history: {
        ...initial.history,
        entries: [old, final],
        revision: 2,
        leafId: final.id,
      },
      contextEntries: [old, final].map((sourceEntry) => ({
        sourceEntry,
        messages: [sourceEntry.message],
      })),
    });
    for (let i = 0; i < 2; i++) {
      await reconcile();
      expect(rows()).toHaveLength(2);
      const rendered = ui.chatContainer.render(100).map(stripAnsi).join("\n");
      expect(rendered.match(/NEW OUTPUT/g)).toHaveLength(1);
      expect(rendered.match(/OLD OUTPUT/g)).toHaveLength(1);
    }
  } finally {
    ui.stop();
    host.session.dispose();
    client.close();
  }
});

const binary = process.env.PIRUA_TEST_SERVER;
describe.skipIf(!binary)(
  "manual shell snapshot reconciliation with the real host",
  () => {
    it("keeps one row and both output chunks through /model refresh and canonical completion", async () => {
      const directory = await mkdtemp(join(tmpdir(), "pirua-shell-refresh-"));
      const child = spawn(
        binary!,
        [
          "--data-dir",
          join(directory, "data"),
          "--config-dir",
          join(directory, "config"),
          "--fixture-provider",
        ],
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
      let shell: Promise<void> | undefined;
      try {
        await host.initialize();
        initTheme("dark");
        ui = new InteractiveMode(host, {
          terminal: new VirtualTerminal(120, 30),
          tuiMode: "regular",
        });
        ui.isInitialized = true;
        ui.subscribeToAgent();
        const errors: string[] = [];
        host.session.onError((error) => errors.push(error.message));
        const rows = () =>
          ui.chatContainer.children.filter(
            (row: any) => row instanceof BashExecutionComponent,
          );
        const rendered = () =>
          ui.chatContainer.render(120).map(stripAnsi).join("\n");
        shell = ui.handleBashCommand(
          "printf 'SHELL_BEFORE\\n'; while [ ! -f release-shell ]; do sleep 0.02; done; printf 'SHELL_AFTER\\n'",
        );
        await vi.waitFor(() =>
          expect(rendered().match(/SHELL_BEFORE/g)).toHaveLength(2),
        );
        const original = rows()[0];
        for (let i = 0; i < 2; i++) {
          await ui.handleModelCommand("nonexistent-refresh-model");
          await vi.waitFor(() => {
            expect(rows()).toHaveLength(1);
            expect(rows()[0]).toBe(original);
            expect(rendered().match(/SHELL_BEFORE/g)).toHaveLength(2);
          });
        }
        await writeFile(join(directory, "release-shell"), "");
        await shell;
        await vi.waitFor(() => {
          expect(rows()).toHaveLength(1);
          expect(rendered().match(/SHELL_BEFORE/g)).toHaveLength(2);
          expect(rendered().match(/SHELL_AFTER/g)).toHaveLength(2);
        });
        await ui.handleModelCommand("nonexistent-refresh-model");
        await vi.waitFor(() => {
          expect(rows()).toHaveLength(1);
          expect(rendered().match(/SHELL_BEFORE/g)).toHaveLength(2);
          expect(rendered().match(/SHELL_AFTER/g)).toHaveLength(2);
        });
        expect(
          host.session.messages.filter(
            (message) => message.role === "bashExecution",
          ),
        ).toEqual([
          expect.objectContaining({
            output: "SHELL_BEFORE\nSHELL_AFTER\n",
            exitCode: 0,
          }),
        ]);
        expect(errors).toEqual([]);
        expect(stderr).toBe("");
      } finally {
        await writeFile(join(directory, "release-shell"), "");
        await shell;
        ui?.stop();
        await host.dispose();
        await rm(directory, { recursive: true, force: true });
      }
    }, 15000);
  },
);
