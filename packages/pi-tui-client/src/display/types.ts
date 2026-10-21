/** Detached presentation contracts. No provider, agent, or persistence implementation. */
export type ThinkingLevel = "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";
export type Transport = "sse" | "websocket" | "auto";
export interface TextContent {
	type: "text";
	text: string;
	textSignature?: string;
}
export interface ImageContent {
	type: "image";
	data: string;
	mimeType: string;
}
export interface ThinkingContent {
	type: "thinking";
	thinking: string;
	thinkingSignature?: string;
	redacted?: boolean;
}
export interface ToolCall {
	type: "toolCall";
	id: string;
	name: string;
	arguments: Record<string, any>;
}
export interface Usage {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
	totalTokens: number;
	cacheWrite1h?: number;
	reasoning?: number;
	cost: { input: number; output: number; cacheRead: number; cacheWrite: number; total: number };
}
export interface Model<TApi = string> {
	id: string;
	name: string;
	provider: string;
	api: TApi;
	baseUrl: string;
	reasoning: boolean;
	input: string[];
	contextWindow: number;
	maxTokens: number;
	cost: { input: number; output: number; cacheRead: number; cacheWrite: number };
	thinking?: Record<string, string | number>;
	type?: string;
	[key: string]: any;
}
export interface AssistantMessage {
	role: "assistant";
	content: (TextContent | ThinkingContent | ToolCall)[];
	api: string;
	provider: string;
	model: string;
	usage: Usage;
	timestamp: number;
	stopReason: "stop" | "length" | "toolUse" | "error" | "aborted";
	errorMessage?: string;
	diagnostics?: { type: string; details?: any }[];
	[key: string]: any;
}
export interface UserMessage {
	role: "user";
	content: string | (TextContent | ImageContent)[];
	timestamp: number;
}
export interface ToolResultMessage {
	role: "toolResult";
	toolCallId: string;
	toolName: string;
	content: (TextContent | ImageContent)[];
	details?: any;
	isError: boolean;
	timestamp: number;
	durationMs?: number;
	usage?: Usage;
}
export interface SystemMessage {
	role: "system";
	content: string;
	timestamp?: number;
}
export type Message = AssistantMessage | UserMessage | ToolResultMessage | SystemMessage;
export interface CustomMessage<T = unknown> {
	role: "custom";
	customType: string;
	content: string | (TextContent | ImageContent)[];
	display: boolean;
	details?: T;
	timestamp: number;
}
export interface BashExecutionMessage {
	role: "bashExecution";
	command: string;
	output: string;
	exitCode: number | undefined;
	cancelled: boolean;
	truncated: boolean;
	fullOutputPath?: string;
	timestamp: number;
	excludeFromContext?: boolean;
}
export interface CompactionSummaryMessage {
	role: "compactionSummary";
	summary: string;
	tokensBefore: number;
	timestamp: number;
}
export interface BranchSummaryMessage {
	role: "branchSummary";
	summary: string;
	fromId: string | null;
	timestamp: number;
}
export type AgentMessage =
	| Message
	| CustomMessage
	| BashExecutionMessage
	| CompactionSummaryMessage
	| BranchSummaryMessage;
export interface ToolResult {
	content: (TextContent | ImageContent)[];
	details?: any;
	isError?: boolean;
	durationMs?: number;
	usage?: Usage;
}
export interface ToolDeclaration {
	name: string;
	description: string;
	parameters: Record<string, unknown>;
}
export interface AgentState {
	messages: AgentMessage[];
	model?: Model;
	thinkingLevel: ThinkingLevel;
	systemPrompt: string;
	tools: ToolDeclaration[];
	isStreaming: boolean;
}
export interface SessionEntryBase {
	id: string;
	parentId: string | null;
	timestamp: string;
}
export type SessionEntry = SessionEntryBase &
	(
		| { type: "message"; message: AgentMessage }
		| { type: "thinking_level_change"; thinkingLevel: string }
		| { type: "model_change"; provider: string; modelId: string }
		| { type: "usage"; kind: string; provider: string; model: string; usage: Usage; note?: string }
		| {
				type: "compaction";
				summary: string;
				firstKeptEntryId: string;
				tokensBefore: number;
				details?: any;
				fromHook?: boolean;
				usage?: Usage;
				systemMessage?: SystemMessage;
		  }
		| { type: "branch_summary"; fromId: string; summary: string; details?: any; fromHook?: boolean; usage?: Usage }
		| { type: "custom"; customType: string; data?: any }
		| { type: "label"; targetId: string; label?: string }
		| { type: "session_info"; name: string }
		| {
				type: "custom_message";
				customType: string;
				content: string | (TextContent | ImageContent)[];
				display: boolean;
				details?: any;
		  }
		| { type: "context_edit"; targetId: string; replacement: { content: unknown } | null }
	);
export type UsageEntry = Extract<SessionEntry, { type: "usage" }>;
export type CompactionEntry = Extract<SessionEntry, { type: "compaction" }>;
export type CustomEntry<T = unknown> = Extract<SessionEntry, { type: "custom" }> & { data?: T };
export interface SessionTreeNode {
	entry: SessionEntry;
	children: SessionTreeNode[];
	label?: string;
	labelTimestamp?: string;
}
export interface SessionInfo {
	id: string;
	path: string;
	cwd: string;
	name?: string;
	parentSessionPath?: string;
	created: Date;
	modified: Date;
	messageCount: number;
	firstMessage: string;
	allMessagesText: string;
}
export type SessionListProgress = (loaded: number, total: number, partialSessions?: SessionInfo[]) => void;
export interface ContextUsage {
	tokens: number | null;
	contextWindow: number;
	percent: number | null;
}
export interface ResourceDiagnostic {
	type: "error" | "warning" | "collision";
	message: string;
	path?: string;
	collision?: any;
}
export interface AuthInfoLink {
	label: string;
	url: string;
}
export interface OAuthDeviceCodeInfo {
	verificationUri: string;
	userCode: string;
	verificationUriComplete?: string;
	expiresIn?: number;
	interval?: number;
}
export type AuthPrompt =
	| { type: "input" | "manual_code"; message: string; placeholder?: string; signal?: AbortSignal }
	| { type: "select"; message: string; options: { id: string; label: string }[]; signal?: AbortSignal };
export type AuthEvent =
	| { type: "auth_url"; url: string; instructions?: string }
	| ({ type: "device_code" } & OAuthDeviceCodeInfo)
	| { type: "info"; message: string; links?: AuthInfoLink[] }
	| { type: "progress"; message: string };
export interface ModelsRefreshResult {
	errors: Map<string, unknown>;
	aborted?: boolean;
	[key: string]: unknown;
}
export interface ModelAuthMethod {
	type?: string;
	label?: string;
	description?: string;
	id?: string;
	isSubscription?: boolean;
	fields?: unknown[];
	[key: string]: any;
}
export type Api = string;
export type Provider = string;
export type CompactionResult = {
	summary: string;
	firstKeptEntryId?: string;
	tokensBefore: number;
	usage?: Usage;
	[key: string]: any;
};
export type SessionStats = {
	userMessages: number;
	assistantMessages: number;
	toolCalls: number;
	toolResults: number;
	totalMessages: number;
	usage: Usage;
	[key: string]: any;
};
export interface SessionSettlement {
	aborted: boolean;
	errorMessage?: string;
}
export type AgentSessionEvent =
	| {
			type: "snapshot_reconciled";
			settlement?: SessionSettlement;
			entries: SessionEntry[];
			activeEvents: AgentSessionEvent[];
			state: "idle" | "running" | "requires_action";
			activity: { compacting: boolean; retrying: boolean; retryAttempt: number; bashRunning: boolean };
	  }
	| {
			type:
				| "agent_start"
				| "turn_start"
				| "turn_end"
				| "queue_update"
				| "session_info_changed"
				| "thinking_level_changed"
				| "summarization_retry_finished";
			[key: string]: any;
	  }
	| {
			type: "message_start" | "message_update" | "message_end";
			message: AgentMessage;
			assistantMessageEvent?: any;
			previousEntries?: SessionEntry[];
	  }
	| {
			type: "tool_execution_start" | "tool_execution_update" | "tool_execution_end";
			toolCallId: string;
			toolName: string;
			args?: any;
			partialResult?: ToolResult;
			result?: ToolResult;
			isError?: boolean;
			durationMs?: number;
			parentToolCallId?: string;
	  }
	| { type: "agent_end"; messages: AgentMessage[]; willRetry: boolean }
	| ({ type: "agent_settled" } & SessionSettlement)
	| { type: "entry_appended"; entry: SessionEntry }
	| {
			type: "compaction_start" | "compaction_end";
			reason: "manual" | "threshold" | "overflow";
			result?: CompactionResult;
			aborted?: boolean;
			willRetry?: boolean;
			errorMessage?: string;
	  }
	| {
			type:
				| "auto_retry_start"
				| "auto_retry_end"
				| "summarization_retry_scheduled"
				| "summarization_retry_attempt_start"
				| "bash_execution_update";
			[key: string]: any;
	  };

export function modelsAreEqual(a: Model | undefined, b: Model | undefined): boolean {
	return a?.provider === b?.provider && a?.id === b?.id;
}
export function getSupportedThinkingLevels(model: Model): ThinkingLevel[] {
	return model.thinkingLevels ?? (model.reasoning ? ["off", "minimal", "low", "medium", "high", "xhigh"] : ["off"]);
}
export function getModelType(model: Model): string {
	return model.type ?? "chat";
}
export function isModelType(model: Model, type: string): boolean {
	return getModelType(model) === type;
}

export type ToolName = "read" | "bash" | "powershell" | "edit" | "write" | "grep" | "find" | "ls";
export type {
	BashToolDetails,
	EditToolDetails,
	FindToolDetails,
	GrepToolDetails,
	LsToolDetails,
	ReadToolDetails,
} from "./tool-details.ts";
export type { EntryRenderer, MarkdownTransformContext } from "./ui-types.ts";
export type { ParsedSkillBlock } from "../ui/constants.ts";
export interface PathMetadata {
	source: string;
	scope: "user" | "project" | "temporary";
	origin: "package" | "top-level";
	baseDir?: string;
}
export interface RuntimeDiagnostic {
	type: "error" | "warning" | "info";
	message: string;
}
export type ApiKeyAuth = ModelAuthMethod;
export type OAuthAuth = ModelAuthMethod;
export interface AuthCheck {
	type: "oauth" | "api_key";
	source?: string;
	configured?: boolean;
	label?: string;
}
