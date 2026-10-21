import { stripAnsi } from "../src/utils/ansi.ts";
import { TuiMainScreen } from "@earendil-works/pi-tui";
import { describe, it, expect } from "vitest";
import { VirtualTerminal } from "../../pi-tui/test/virtual-terminal.ts";
import { ToolExecutionComponent } from "../src/modes/interactive/components/tool-execution.ts";
import { withBuiltInRenderers } from "../src/core/tools/renderers/index.ts";
import { initTheme } from "../src/modes/interactive/theme/theme.ts";
import { PresentationRegistry } from "../src/ui/presentation-registry.ts";

describe("original tool rendering without execution modules", () => {
	it.each([
		["read", { path: "fixture.ts" }, { content: [{ type: "text", text: "const value = 1;" }] }],
		[
			"edit",
			{ path: "fixture.ts", oldText: "old", newText: "new" },
			{ content: [{ type: "text", text: "edited" }], details: { diff: "-1 old\n+1 new", firstChangedLine: 1 } },
		],
		[
			"write",
			{ path: "fixture.ts", content: "const value = 1;" },
			{ content: [{ type: "text", text: "wrote fixture" }] },
		],
		[
			"bash",
			{ command: "printf fixture" },
			{ content: [{ type: "text", text: "fixture output" }], details: { exitCode: 0 } },
		],
	])("renders %s arguments, partial output, final output and expansion", (name, args, result) => {
		initTheme("dark");
		const terminal = new VirtualTerminal(90, 25);
		const ui = new TuiMainScreen(terminal);
		const component = new ToolExecutionComponent(
			name as string,
			"call",
			args,
			{ showImages: false },
			withBuiltInRenderers(name as string, undefined),
			ui,
			"/workspace/project",
		);
		component.setArgsComplete();
		component.markExecutionStarted();
		component.updateResult({ ...result, isError: false } as any, true);
		component.updateResult({ ...result, isError: false, durationMs: 42 } as any);
		component.setExpanded(true);
		const rendered = component.render(90).map(stripAnsi).join("\n");
		expect(rendered).toContain(name === "bash" ? "$ printf fixture" : name);
		expect(rendered.length).toBeGreaterThan(name.length);
	});
	it("uses client-local registered callbacks and leaves backend execution absent", () => {
		initTheme("dark");
		const registry = new PresentationRegistry();
		let calls = 0;
		registry.tools.set("custom", {
			renderCall: () => {
				calls++;
				return undefined;
			},
		});
		const registered = registry.resolveToolRenderers("custom", () => {
			throw new Error("Unexpected fallback");
		});
		registered?.renderCall?.({}, {} as any, {} as any);
		expect(calls).toBe(1);
		expect(registered).not.toHaveProperty("execute");
	});
});
