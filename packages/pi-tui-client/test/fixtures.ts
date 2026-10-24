import type { PiSnapshot } from "../src/acp/types.ts";
import { zeroUsage } from "../src/acp/types.ts";
import type { AssistantMessage, Model } from "../src/display/types.ts";
export const model: Model = {
	id: "fixture",
	name: "Fixture",
	provider: "fixture",
	api: "fixture",
	baseUrl: "",
	reasoning: true,
	input: ["text", "image"],
	contextWindow: 32000,
	maxTokens: 4096,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};
export function assistant(content: AssistantMessage["content"] = []): AssistantMessage {
	return {
		role: "assistant",
		content,
		api: "fixture",
		provider: "fixture",
		model: "fixture",
		usage: zeroUsage(),
		timestamp: 1000,
		stopReason: "stop",
	};
}
export function snapshot(overrides: Partial<PiSnapshot> = {}): PiSnapshot {
	return {
		sessionId: "s1",
		epoch: "epoch1",
		sequence: 0,
		history: { header: { id: "s1", cwd: "/workspace/project", timestamp: 0 }, revision: 0, leafId: null, entries: [] },
		messages: [],
		contextEntries: [],
		liveEntities: [],
		state: "idle",
		model,
		routedModel: null,
		thinkingLevel: "medium",
		availableThinkingLevels: ["off", "medium", "high"],
		models: [model],
		scopedModels: [],
		configOptions: [
			{
				configId: "model",
				name: "Model",
				type: "select",
				currentValue: "fixture/fixture",
				options: [{ value: "fixture/fixture", name: "Fixture" }],
			},
			{
				configId: "thinkingLevel",
				name: "Thinking",
				type: "select",
				currentValue: "medium",
				options: [
					{ value: "off", name: "Off" },
					{ value: "medium", name: "Medium" },
				],
			},
		],
		queue: { steering: [], followUp: [] },
		activity: { compacting: false, retrying: false, retryAttempt: 0, bashRunning: false },
		stats: null,
		contextUsage: null,
		settings: { steeringMode: "one-at-a-time", followUpMode: "one-at-a-time", compaction: { enabled: true } },
		resources: { contextFiles: [], skills: [], promptTemplates: [], diagnostics: [] },
		tools: [],
		...overrides,
	};
}
