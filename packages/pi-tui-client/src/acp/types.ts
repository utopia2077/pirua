import type {
	AgentMessage,
	Model,
	ThinkingLevel,
	Usage,
	SessionStats,
	ContextUsage,
	ImageContent,
} from "../display/types.ts";
import type { RecordValue } from "./client.ts";
export interface NativeEntry extends RecordValue {
	id: string;
	parentId: string | null;
	timestamp: number;
	type: string;
}
export interface History {
	header: { id: string; cwd: string; timestamp: number; parentSession?: string };
	revision: number;
	leafId: string | null;
	entries: NativeEntry[];
}
export interface QueuedInput {
	submissionId: string;
	delivery: "steer" | "followUp";
	text: string;
	images: ImageContent[];
}
export interface ConfigOption extends RecordValue {
	configId: string;
	name: string;
	type: string;
	currentValue?: unknown;
}
export interface PiSnapshot {
	sessionId: string;
	epoch: string;
	sequence: number;
	history: History;
	messages: AgentMessage[];
	contextEntries: { sourceEntry: NativeEntry; messages: AgentMessage[] }[];
	liveEntities: RecordValue[];
	state: "idle" | "running" | "requires_action";
	model: Model | null;
	routedModel: RecordValue | null;
	thinkingLevel: ThinkingLevel;
	availableThinkingLevels: ThinkingLevel[];
	models: Model[];
	scopedModels: RecordValue[];
	configOptions: ConfigOption[];
	queue: { steering: QueuedInput[]; followUp: QueuedInput[] };
	activity: { compacting: boolean; retrying: boolean; retryAttempt: number; bashRunning: boolean };
	stats: SessionStats | null;
	contextUsage: ContextUsage | null;
	settings: RecordValue;
	resources: RecordValue;
	tools: RecordValue[];
}
export interface SessionChange {
	sessionId: string;
	cancelled: boolean;
	aborted?: boolean;
	editorText?: string;
	snapshot: PiSnapshot;
}
export interface RenderMeta extends RecordValue {
	phase?: "start" | "delta" | "end";
	nativeMessageId?: string;
	message?: AgentMessage;
	contentIndex?: number;
	deltaType?: string;
	toolCallId?: string;
	parentMessageId?: string;
	entryId?: string;
	durationMs?: number;
}
export const PROFILE = "pirua.dev/tui";
export const PROJECTION = "pirua.dev/projection";
export const RENDER = "pirua.dev/render";
export function zeroUsage(): Usage {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}
