import type { Model, ThinkingLevel, Api, UsageEntry } from "../display/types.ts";
import { modelsAreEqual } from "../display/types.ts";
import { minimatch } from "minimatch";
import chalk from "chalk";
import { THINKING_LEVEL_OPTIONS, DEFAULT_THINKING_LEVEL } from "./constants.ts";
function isValidThinkingLevel(value: string): value is ThinkingLevel {
	return THINKING_LEVEL_OPTIONS.includes(value as ThinkingLevel);
}
export interface ScopedModel {
	model: Model<Api>;
	/** Thinking level if explicitly specified in pattern (e.g., "model:high"), undefined otherwise */
	thinkingLevel?: ThinkingLevel;
}

export interface ModelReferenceRequest {
	/** Bare model id or `provider/id`, optionally followed by `:<thinking>`. */
	text: string;
	/** Only consider models from this provider. */
	provider?: string;
	/**
	 * A trailing `:<suffix>` is only considered when the full text matches no model, since ids such as
	 * OpenRouter's `qwen/qwen3-coder:exacto` contain colons.
	 * - `none`: the suffix stays part of the id
	 * - `strict`: a valid thinking level is parsed; any other suffix stays part of the id
	 * - `lenient`: like `strict`, but an invalid suffix is dropped with a warning
	 */
	thinkingSuffix: "none" | "strict" | "lenient";
	/** Match id and name substrings when no exact match exists. */
	fuzzy: boolean;
}

export type ModelReferenceResult =
	| { status: "resolved"; model: Model<Api>; thinkingLevel?: ThinkingLevel; warning?: string }
	| {
			status: "ambiguous";
			candidates: Model<Api>[];
			usableCount: number;
			thinkingLevel?: ThinkingLevel;
			warning?: string;
	  }
	| {
			status: "not-found";
			/** Set when a provider is explicit or named by a `provider/` prefix, for building a custom model id. */
			target?: ModelReferenceTarget;
	  };

export interface ModelReferenceTarget {
	provider: string;
	providerModels: [Model<Api>, ...Model<Api>[]];
	/** Text after the provider prefix as typed, including any `:<suffix>`. */
	modelText: string;
	/** `modelText` without a valid thinking suffix (unless `thinkingSuffix` is `none`). */
	modelId: string;
	thinkingLevel?: ThinkingLevel;
}

type ResolvedOrAmbiguous = Exclude<ModelReferenceResult, { status: "not-found" }>;

function splitThinkingSuffix(text: string): { base: string; thinkingLevel: ThinkingLevel } | undefined {
	const colonIndex = text.lastIndexOf(":");
	const suffix = text.substring(colonIndex + 1);
	if (colonIndex === -1 || !isValidThinkingLevel(suffix)) return undefined;
	return { base: text.substring(0, colonIndex), thinkingLevel: suffix };
}

function splitProviderPrefix(
	text: string,
	models: readonly Model<Api>[],
): { provider: string; modelText: string } | undefined {
	const slashIndex = text.indexOf("/");
	if (slashIndex === -1) return undefined;
	const prefix = text.substring(0, slashIndex).trim().toLowerCase();
	const modelText = text.substring(slashIndex + 1).trim();
	if (!prefix || !modelText) return undefined;
	const provider = models.find((model) => model.provider.toLowerCase() === prefix)?.provider;
	return provider ? { provider, modelText } : undefined;
}

export function resolveModelReference(
	request: ModelReferenceRequest,
	models: readonly Model<Api>[],
	usable: (provider: string) => boolean,
): ModelReferenceResult {
	const text = request.text.trim();
	if (!text) return { status: "not-found" };
	const constraint = request.provider?.toLowerCase();
	const pool = constraint ? models.filter((model) => model.provider.toLowerCase() === constraint) : models;
	const result = resolveText(text, request, pool, usable);
	if (result.status !== "not-found") return result;

	const split = splitProviderPrefix(text, pool);
	const anchor = constraint ? pool[0] : pool.find((model) => model.provider === split?.provider);
	if (!anchor) return result;
	const modelText = split?.modelText ?? text;
	const suffix = request.thinkingSuffix === "none" ? undefined : splitThinkingSuffix(modelText);
	return {
		status: "not-found",
		target: {
			provider: anchor.provider,
			providerModels: [anchor, ...pool.filter((model) => model.provider === anchor.provider && model !== anchor)],
			modelText,
			modelId: suffix?.base ?? modelText,
			thinkingLevel: suffix?.thinkingLevel,
		},
	};
}

export function findExactModelReferenceMatch(
	modelReference: string,
	models: readonly Model<Api>[],
): Model<Api> | undefined {
	const result = resolveModelReference(
		{ text: modelReference, thinkingSuffix: "none", fuzzy: false },
		models,
		() => true,
	);
	return result.status === "resolved" ? result.model : undefined;
}

function resolveText(
	text: string,
	request: ModelReferenceRequest,
	pool: readonly Model<Api>[],
	usable: (provider: string) => boolean,
): ModelReferenceResult {
	const reference = selectReference(text, pool, request.fuzzy, usable);
	if (reference) return reference;

	if (request.fuzzy) {
		const fuzzy = selectFuzzy(fuzzyMatches(text, pool), usable);
		if (fuzzy) return fuzzy;
	}

	if (request.thinkingSuffix === "none") return { status: "not-found" };
	const thinking = splitThinkingSuffix(text);
	if (thinking) {
		const inner = resolveText(thinking.base, request, pool, usable);
		if (inner.status === "not-found") return inner;
		// Only use this thinking level if no warning from inner recursion
		return { ...inner, thinkingLevel: inner.warning ? undefined : thinking.thinkingLevel };
	}
	// Keep an invalid suffix as part of the id and fail, rather than resolve a different model
	if (request.thinkingSuffix === "strict") return { status: "not-found" };
	const colonIndex = text.lastIndexOf(":");
	if (colonIndex === -1) return { status: "not-found" };

	const suffix = text.substring(colonIndex + 1);
	const inner = resolveText(text.substring(0, colonIndex), request, pool, usable);
	if (inner.status === "not-found") return inner;
	return {
		...inner,
		thinkingLevel: undefined,
		warning: `Invalid thinking level "${suffix}" in pattern "${text}". Using default instead.`,
	};
}

function selectReference(
	text: string,
	pool: readonly Model<Api>[],
	fuzzy: boolean,
	usable: (provider: string) => boolean,
): ResolvedOrAmbiguous | undefined {
	const lowerText = text.toLowerCase();
	const raw = pool.filter((model) => model.id.toLowerCase() === lowerText);
	const split = splitProviderPrefix(text, pool);
	let named: Model<Api> | undefined;
	if (split) {
		const providerModels = pool.filter((model) => model.provider === split.provider);
		const lowerModelText = split.modelText.toLowerCase();
		// The provider's own literal id (OpenRouter's `openrouter/free`) beats its fuzzy matches
		named =
			providerModels.find((model) => model.id.toLowerCase() === lowerModelText) ??
			raw.find((model) => model.provider === split.provider) ??
			(fuzzy ? bestFuzzyMatch(fuzzyMatches(split.modelText, providerModels)) : undefined);
	}

	const others = raw.filter((model) => model !== named);
	const candidates = named ? [named, ...others] : others;
	if (candidates.length === 0) return undefined;
	if (new Set(candidates.map((model) => model.provider)).size === 1)
		return { status: "resolved", model: candidates[0] };
	if (named && usable(named.provider)) return { status: "resolved", model: named };
	const usableOthers = others.filter((model) => usable(model.provider));
	if (usableOthers.length === 1) return { status: "resolved", model: usableOthers[0] };
	if (usableOthers.length === 0 && named) return { status: "resolved", model: named };
	return { status: "ambiguous", candidates, usableCount: usableOthers.length };
}

function fuzzyMatches(text: string, models: readonly Model<Api>[]): Model<Api>[] {
	const lowerText = text.toLowerCase();
	return models.filter(
		(model) => model.id.toLowerCase().includes(lowerText) || model.name?.toLowerCase().includes(lowerText),
	);
}

function selectFuzzy(matches: Model<Api>[], usable: (provider: string) => boolean): ResolvedOrAmbiguous | undefined {
	if (matches.length === 0) return undefined;
	const multipleProviders = new Set(matches.map((model) => model.provider)).size > 1;
	const usableMatches = multipleProviders ? matches.filter((model) => usable(model.provider)) : [];
	const pool = usableMatches.length > 0 ? usableMatches : matches;
	const best = bestFuzzyMatch(pool);
	if (!best) return undefined;
	// The same id on several providers would otherwise be decided by catalog order
	const tied = pool.filter((model) => model.id === best.id);
	if (new Set(tied.map((model) => model.provider)).size === 1) return { status: "resolved", model: best };
	return {
		status: "ambiguous",
		candidates: tied,
		usableCount: usableMatches.length > 0 ? tied.length : 0,
	};
}

function bestFuzzyMatch(matches: readonly Model<Api>[]): Model<Api> | undefined {
	const aliases = matches.filter((model) => isAlias(model.id));
	return [...(aliases.length > 0 ? aliases : matches)].sort((a, b) => b.id.localeCompare(a.id))[0];
}

function isAlias(id: string): boolean {
	return id.endsWith("-latest") || !/-\d{8}$/.test(id);
}

export interface ModelScopeDiagnostic {
	type: "warning";
	code: "no-match" | "invalid-thinking-level" | "ambiguous";
	message: string;
	pattern: string;
}

export interface ResolveModelScopeResult {
	scopedModels: ScopedModel[];
	diagnostics: ModelScopeDiagnostic[];
}

export function resolveModelScopeFromModels(
	patterns: string[],
	models: readonly Model<Api>[],
): ResolveModelScopeResult {
	const scopedModels: ScopedModel[] = [];
	const diagnostics: ModelScopeDiagnostic[] = [];
	const add = (model: Model<Api>, thinkingLevel: ThinkingLevel | undefined) => {
		if (!scopedModels.some((scoped) => modelsAreEqual(scoped.model, model))) {
			scopedModels.push({ model, thinkingLevel });
		}
	};
	const noMatch = (pattern: string) =>
		diagnostics.push({ type: "warning", code: "no-match", message: `No models match pattern "${pattern}"`, pattern });
	const ambiguous = (pattern: string, candidates: readonly Model<Api>[]) => {
		const providers = [...new Set(candidates.map((m) => m.provider))].sort((a, b) => a.localeCompare(b));
		diagnostics.push({
			type: "warning",
			code: "ambiguous",
			message: `Pattern "${pattern}" matches several providers: ${providers.join(", ")}. Use provider/id to choose one.`,
			pattern,
		});
	};

	for (const pattern of patterns) {
		// Check if pattern contains glob characters
		if (pattern.includes("*") || pattern.includes("?") || pattern.includes("[")) {
			// Extract optional thinking level suffix (e.g., "provider/*:high")
			const thinking = splitThinkingSuffix(pattern);
			const globPattern = thinking?.base ?? pattern;
			const thinkingLevel = thinking?.thinkingLevel;

			// Ids such as `bracketed-model[1m]` are not globs
			const exact = resolveModelReference(
				{ text: globPattern, thinkingSuffix: "none", fuzzy: false },
				models,
				() => true,
			);
			let matches: readonly Model<Api>[] = [];
			if (exact.status === "resolved") matches = [exact.model];
			else if (exact.status === "ambiguous") {
				matches = exact.candidates;
				ambiguous(pattern, matches);
			} else {
				// Match against "provider/modelId" format OR just model ID
				// This allows "*sonnet*" to match without requiring "anthropic/*sonnet*"
				matches = models.filter(
					(m) =>
						minimatch(`${m.provider}/${m.id}`, globPattern, { nocase: true }) ||
						minimatch(m.id, globPattern, { nocase: true }),
				);
			}
			if (matches.length === 0) noMatch(pattern);
			for (const model of matches) add(model, thinkingLevel);
			continue;
		}

		const result = resolveModelReference({ text: pattern, thinkingSuffix: "lenient", fuzzy: true }, models, () => true);
		if (result.status === "not-found") {
			noMatch(pattern);
			continue;
		}
		if (result.warning) {
			diagnostics.push({ type: "warning", code: "invalid-thinking-level", message: result.warning, pattern });
		}
		if (result.status === "ambiguous") ambiguous(pattern, result.candidates);
		const matches = result.status === "resolved" ? [result.model] : result.candidates;
		for (const model of matches) add(model, result.thinkingLevel);
	}

	return { scopedModels, diagnostics };
}
const CACHE_WARMING_MINIMUM_EXPECTED_SAVINGS = 0.05;

export type CacheWarmingAction = "warm" | "stop";

export interface CacheWarmingDecision {
	/** "streaming" while the agent run that sent the request is still active. */
	phase: "streaming" | "idle";
	/** Price of this refresh: a cache read of the prompt plus one output token. */
	warmCost: number;
	/** Extra price of the next real request if the cache entry is lost. */
	missCost: number;
	/** Estimated chance that a real request arrives before the entry expires. */
	continuationProbability: number;
	/** `continuationProbability * missCost - warmCost`. */
	expectedSavings: number;
	/** False when the prompt size or the model's prices are unknown. */
	economicsAvailable: boolean;
	/** Pi's decision: "warm" when `expectedSavings` is at least $0.05. */
	action: CacheWarmingAction;
}

export interface CacheWarmingStatus {
	/** "scheduled": a refresh timer is armed; "refreshing": a warm request is in flight. */
	state: "inactive" | "scheduled" | "refreshing";
	/** Why nothing is scheduled. */
	reason?: string;
	nextWarmAt?: number;
	/** The pending decision, or the decision that stopped warming. */
	decision?: CacheWarmingDecision;
	/** True when an extension changed `decision.action`. */
	extensionOverride?: boolean;
}

function formatDollars(value: number): string {
	return value < 0 ? `-$${Math.abs(value).toFixed(3)}` : `$${value.toFixed(3)}`;
}

function formatCacheWarmingEconomics(decision: CacheWarmingDecision): string {
	if (!decision.economicsAvailable) return "cache economics unavailable";
	const probability = Math.round(decision.continuationProbability * 100);
	const probabilityText =
		decision.phase === "streaming"
			? `${probability}% continuation probability while agent is running`
			: `${probability}% continuation probability`;
	const comparison = decision.action === "warm" ? ">=" : "<";
	return `${probabilityText}, expected savings ${formatDollars(decision.expectedSavings)} ${comparison} $${CACHE_WARMING_MINIMUM_EXPECTED_SAVINGS.toFixed(3)}`;
}

function formatCacheWarmingDecisionTime(nextWarmAt: number | undefined, now: number): string {
	if (nextWarmAt === undefined || nextWarmAt <= now) return "Decision now";
	let remainingSeconds = Math.ceil((nextWarmAt - now) / 1000);
	const hours = Math.floor(remainingSeconds / 3600);
	remainingSeconds %= 3600;
	const minutes = Math.floor(remainingSeconds / 60);
	const seconds = remainingSeconds % 60;
	const parts: string[] = [];
	if (hours > 0) parts.push(`${hours}h`);
	if (minutes > 0) parts.push(`${minutes}m`);
	if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);
	return `Decision in ${parts.join(" ")}`;
}

export function formatCacheWarmingStatus(status: CacheWarmingStatus, now = Date.now()): string {
	const decision = status.decision;
	// A decision is attached once pi (or an extension) acted on it; "inactive"
	// without one never got that far.
	if (!decision || (status.state === "inactive" && !decision.economicsAvailable && !status.extensionOverride)) {
		return `Inactive (${status.reason ?? "unknown reason"})`;
	}
	const details = status.extensionOverride
		? `extension override, ${formatCacheWarmingEconomics(decision)}`
		: `${formatCacheWarmingEconomics(decision)} -> ${decision.action}`;
	if (status.state === "inactive") return `Stopped (${details})`;
	if (status.state === "refreshing") return `Warming cache (${details})`;
	return `${formatCacheWarmingDecisionTime(status.nextWarmAt, now)} (${details})`;
}

export function formatCacheWarmingUsage(entry: UsageEntry): string {
	const note = entry.note ? ` (${entry.note})` : "";
	const cost = entry.usage.cost.total.toFixed(6).replace(/(\.\d{3}\d*?)0+$/, "$1");
	return `Cache warmed${note}: $${cost}`;
}
