import type {
	ToolRenderers,
	MarkdownTransformer,
	MessageRenderer,
	EntryRenderer,
	ExtensionUIContext,
} from "../display/ui-types.ts";

/** Explicit client-local registrations; this does not load backend extensions. */
export class PresentationRegistry {
	readonly tools = new Map<string, ToolRenderers>();
	readonly messages = new Map<string, MessageRenderer>();
	readonly entries = new Map<string, EntryRenderer>();
	readonly transformers: MarkdownTransformer[] = [];
	ui: ExtensionUIContext | undefined;
	bindUI(ui: ExtensionUIContext): void {
		this.ui = ui;
	}
	resolveToolRenderers(name: string, builtIn: () => ToolRenderers | undefined): ToolRenderers | undefined {
		return this.tools.get(name) ?? builtIn();
	}
	getMarkdownTransformers(): MarkdownTransformer[] {
		return [...this.transformers];
	}
	getMessageRenderer(name: string): MessageRenderer | undefined {
		return this.messages.get(name);
	}
	getEntryRenderer(name: string): EntryRenderer | undefined {
		return this.entries.get(name);
	}
	getRegisteredCommands(): {
		name: string;
		invocationName: string;
		description: string;
		sourceInfo?: any;
		getArgumentCompletions?: any;
	}[] {
		return [];
	}
	getCommand(_name: string): undefined {
		return undefined;
	}
	getCommandDiagnostics(): never[] {
		return [];
	}
	getShortcutDiagnostics(): never[] {
		return [];
	}
	getErrorHandlers(): never[] {
		return [];
	}
	findCrashSource(error: unknown): string | undefined {
		const stack = error instanceof Error ? error.stack : "";
		for (const name of this.tools.keys())
			if (stack?.includes(name)) return `Local renderer ${name} appears in the stack.`;
		return undefined;
	}
}
