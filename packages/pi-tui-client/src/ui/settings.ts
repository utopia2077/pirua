import { mkdirSync, readFileSync, writeFileSync, renameSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import type { ThinkingLevel, Model, Transport } from "../display/types.ts";
import type { TerminalCapabilities, ScrollViewScrollbar, WheelScrollLines } from "@earendil-works/pi-tui";
import { SETTINGS_DEFAULTS } from "../core/settings-defaults.ts";
import { parseHttpIdleTimeoutMs, DEFAULT_HTTP_IDLE_TIMEOUT_MS } from "./constants.ts";
import { normalizePath } from "../utils/paths.ts";
export type CacheWarmingMode = "off" | "streaming" | "idle";
export const CACHE_WARMING_MODES = ["off", "streaming", "idle"] as const;
export type MermaidRenderingMode = "off" | "streaming" | "final";
export type DefaultProjectTrust = "ask" | "always" | "never";
export type FullscreenExitOutput = "resume-hint" | "transcript";
export type QuietStartup = boolean | "header" | "details";
export type SessionShareDestination = "auto" | "gist" | "radius";
export type TuiMode = "regular" | "fullscreen";
export type WarningSettings = { anthropicExtraUsage?: boolean };
export type TransportSetting = Transport;
export type Settings = Record<string, any>;
const LOCAL_KEYS = new Set([
	"lastChangelogVersion",
	"theme",
	"hideThinkingBlock",
	"showCacheMissNotices",
	"externalEditor",
	"quietStartup",
	"collapseChangelog",
	"enableSkillCommands",
	"terminal",
	"tuiMode",
	"fullscreenExitOutput",
	"fullscreenScrollbar",
	"fullscreenCopyOnSelect",
	"fullscreenWheelScrollLines",
	"doubleEscapeAction",
	"treeFilterMode",
	"showHardwareCursor",
	"editorPaddingX",
	"outputPad",
	"autocompleteMaxVisible",
	"markdown",
	"warnings",
]);
const VALID_TUI_MODE_SET = new Set(["regular", "fullscreen"]);
export class ClientSettings {
	private globalSettings: Settings = {};
	private backend: Settings = {};
	private get settings(): Settings {
		return { ...this.backend, ...this.globalSettings };
	}
	private file: string;
	constructor(file: string) {
		this.file = file;
		try {
			const parsed = JSON.parse(readFileSync(file, "utf8"));
			this.globalSettings = Object.fromEntries(Object.entries(parsed).filter(([key]) => LOCAL_KEYS.has(key)));
		} catch (e) {
			if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
		}
	}
	setBackend(settings: Settings): void {
		this.backend = structuredClone(settings);
	}
	private save(): void {
		mkdirSync(dirname(this.file), { recursive: true });
		const temporary = this.file + "." + randomUUID() + ".tmp";
		writeFileSync(temporary, JSON.stringify(this.globalSettings, null, 2) + "\n", { mode: 0o600 });
		renameSync(temporary, this.file);
	}
	getGlobalSettings(): Settings {
		return structuredClone(this.globalSettings);
	}
	getProjectSettings(): Settings {
		return structuredClone(this.backend.projectSettings ?? {});
	}
	isProjectTrusted(): boolean {
		return this.backend.projectTrusted === true;
	}
	getLastChangelogVersion(): string | undefined {
		return this.settings.lastChangelogVersion;
	}
	setLastChangelogVersion(version: string): void {
		this.globalSettings.lastChangelogVersion = version;
		this.save();
	}
	getDefaultProvider(): string | undefined {
		return this.settings.defaultProvider;
	}
	getDefaultModel(): string | undefined {
		return this.settings.defaultModel;
	}
	getThemeSetting(): string | undefined {
		const value = this.settings.theme;
		if (typeof value === "string") return value;
		return undefined;
	}
	getTheme(): string | undefined {
		const theme = this.getThemeSetting();
		return theme?.includes("/") ? undefined : theme;
	}
	setTheme(theme: string): void {
		this.globalSettings.theme = theme;
		this.save();
	}
	getDefaultThinkingLevel(): ThinkingLevel | undefined {
		return this.settings.defaultThinkingLevel;
	}
	getAllModelThinkingLevels(): Record<string, ThinkingLevel> {
		return { ...(this.settings.modelThinkingLevels ?? {}) };
	}
	setModelThinkingLevel(provider: string, modelId: string, level: ThinkingLevel): void {
		throw new Error("Execution setting ModelThinkingLevel requires a supported backend configuration option");
	}
	removeModelThinkingLevel(_provider: string, _modelId: string): void {
		throw new Error("Model thinking defaults require a supported backend configuration option");
	}
	getTransport(): TransportSetting {
		return this.settings.transport ?? SETTINGS_DEFAULTS.transport;
	}
	setTransport(transport: TransportSetting): void {
		throw new Error("Execution setting Transport requires a supported backend configuration option");
	}
	getSessionShareDestination(): SessionShareDestination {
		const destination = this.settings.sessionShareDestination;
		return destination === "gist" || destination === "radius" ? destination : SETTINGS_DEFAULTS.sessionShareDestination;
	}
	setSessionShareDestination(destination: SessionShareDestination): void {
		throw new Error("Execution setting SessionShareDestination requires a supported backend configuration option");
	}
	getBranchSummarySkipPrompt(): boolean {
		return this.settings.branchSummary?.skipPrompt ?? SETTINGS_DEFAULTS.branchSummary.skipPrompt;
	}
	getHttpIdleTimeoutMs(): number {
		return parseHttpIdleTimeoutMs(this.settings.httpIdleTimeoutMs) ?? DEFAULT_HTTP_IDLE_TIMEOUT_MS;
	}
	setHttpIdleTimeoutMs(timeoutMs: number): void {
		throw new Error("Execution setting HttpIdleTimeoutMs requires a supported backend configuration option");
	}
	getCacheWarmingMode(): CacheWarmingMode {
		const mode = this.backend.cacheWarming;
		return mode !== undefined && CACHE_WARMING_MODES.includes(mode) ? mode : SETTINGS_DEFAULTS.cacheWarming;
	}
	getHideThinkingBlock(): boolean {
		return this.settings.hideThinkingBlock ?? SETTINGS_DEFAULTS.hideThinkingBlock;
	}
	getShowCacheMissNotices(): boolean {
		return this.settings.showCacheMissNotices ?? SETTINGS_DEFAULTS.showCacheMissNotices;
	}
	getExternalEditorCommand(): string {
		const configuredEditor = this.settings.externalEditor;
		if (typeof configuredEditor === "string" && configuredEditor.trim() !== "") {
			return configuredEditor;
		}
		const environmentEditor = process.env.VISUAL || process.env.EDITOR;
		if (environmentEditor) {
			return environmentEditor;
		}
		return process.platform === "win32" ? "notepad" : "nano";
	}
	setHideThinkingBlock(hide: boolean): void {
		this.globalSettings.hideThinkingBlock = hide;
		this.save();
	}
	setShowCacheMissNotices(show: boolean): void {
		this.globalSettings.showCacheMissNotices = show;
		this.save();
	}
	getQuietStartup(): QuietStartup {
		const value = this.settings.quietStartup;
		return value === true || value === "header" ? value : SETTINGS_DEFAULTS.quietStartup;
	}
	setQuietStartup(quiet: QuietStartup): void {
		this.globalSettings.quietStartup = quiet;
		this.save();
	}
	getDefaultProjectTrust(): DefaultProjectTrust {
		const value = this.globalSettings.defaultProjectTrust;
		return value === "always" || value === "never" ? value : SETTINGS_DEFAULTS.defaultProjectTrust;
	}
	setDefaultProjectTrust(defaultProjectTrust: DefaultProjectTrust): void {
		throw new Error("Execution setting DefaultProjectTrust requires a supported backend configuration option");
	}
	getCollapseChangelog(): boolean {
		return this.settings.collapseChangelog ?? SETTINGS_DEFAULTS.collapseChangelog;
	}
	setCollapseChangelog(collapse: boolean): void {
		this.globalSettings.collapseChangelog = collapse;
		this.save();
	}
	getEnableInstallTelemetry(): boolean {
		return this.settings.enableInstallTelemetry ?? SETTINGS_DEFAULTS.enableInstallTelemetry;
	}
	setEnableInstallTelemetry(enabled: boolean): void {
		throw new Error("Execution setting EnableInstallTelemetry requires a supported backend configuration option");
	}
	getOrCreateDeviceId(): string {
		if (!this.globalSettings.deviceId) {
			this.globalSettings.deviceId = randomUUID();
			this.save();
		}
		return this.globalSettings.deviceId;
	}
	setPackages(packages: unknown[]): void {
		throw new Error("Execution setting Packages requires a supported backend configuration option");
	}
	setProjectPackages(packages: unknown[]): void {
		throw new Error("Execution setting ProjectPackages requires a supported backend configuration option");
	}
	setExtensionPaths(paths: string[]): void {
		throw new Error("Execution setting ExtensionPaths requires a supported backend configuration option");
	}
	setProjectExtensionPaths(paths: string[]): void {
		throw new Error("Execution setting ProjectExtensionPaths requires a supported backend configuration option");
	}
	setSkillPaths(paths: string[]): void {
		throw new Error("Execution setting SkillPaths requires a supported backend configuration option");
	}
	setProjectSkillPaths(paths: string[]): void {
		throw new Error("Execution setting ProjectSkillPaths requires a supported backend configuration option");
	}
	setPromptTemplatePaths(paths: string[]): void {
		throw new Error("Execution setting PromptTemplatePaths requires a supported backend configuration option");
	}
	setProjectPromptTemplatePaths(paths: string[]): void {
		throw new Error("Execution setting ProjectPromptTemplatePaths requires a supported backend configuration option");
	}
	setThemePaths(paths: string[]): void {
		throw new Error("Execution setting ThemePaths requires a supported backend configuration option");
	}
	setProjectThemePaths(paths: string[]): void {
		throw new Error("Execution setting ProjectThemePaths requires a supported backend configuration option");
	}
	getEnableSkillCommands(): boolean {
		return this.settings.enableSkillCommands ?? SETTINGS_DEFAULTS.enableSkillCommands;
	}
	setEnableSkillCommands(enabled: boolean): void {
		this.globalSettings.enableSkillCommands = enabled;
		this.save();
	}
	getTerminalCapabilityOverrides(): Partial<TerminalCapabilities> {
		const terminal = this.settings.terminal;
		const images = terminal?.images;
		return {
			...(images === "kitty" || images === "iterm2" ? { images } : images === false ? { images: null } : {}),
			...(typeof terminal?.trueColor === "boolean" ? { trueColor: terminal.trueColor } : {}),
			...(typeof terminal?.hyperlinks === "boolean" ? { hyperlinks: terminal.hyperlinks } : {}),
		};
	}
	getShowImages(): boolean {
		return this.settings.terminal?.showImages ?? SETTINGS_DEFAULTS.terminal.showImages;
	}
	setShowImages(show: boolean): void {
		if (!this.globalSettings.terminal) {
			this.globalSettings.terminal = {};
		}
		this.globalSettings.terminal.showImages = show;
		this.save();
	}
	getImageWidthCells(): number {
		const width = this.settings.terminal?.imageWidthCells;
		if (typeof width !== "number" || !Number.isFinite(width)) {
			return SETTINGS_DEFAULTS.terminal.imageWidthCells;
		}
		return Math.max(1, Math.floor(width));
	}
	setImageWidthCells(width: number): void {
		if (!this.globalSettings.terminal) {
			this.globalSettings.terminal = {};
		}
		this.globalSettings.terminal.imageWidthCells = Math.max(1, Math.floor(width));
		this.save();
	}
	getClearOnShrink(): boolean {
		// Settings takes precedence, then env var, then default false
		if (this.settings.terminal?.clearOnShrink !== undefined) {
			return this.settings.terminal.clearOnShrink;
		}
		if (process.env.PI_CLEAR_ON_SHRINK === "1") return true;
		return SETTINGS_DEFAULTS.terminal.clearOnShrink;
	}
	setClearOnShrink(enabled: boolean): void {
		if (!this.globalSettings.terminal) {
			this.globalSettings.terminal = {};
		}
		this.globalSettings.terminal.clearOnShrink = enabled;
		this.save();
	}
	getShowTerminalProgress(): boolean {
		return this.settings.terminal?.showTerminalProgress ?? SETTINGS_DEFAULTS.terminal.showTerminalProgress;
	}
	setShowTerminalProgress(enabled: boolean): void {
		if (!this.globalSettings.terminal) {
			this.globalSettings.terminal = {};
		}
		this.globalSettings.terminal.showTerminalProgress = enabled;
		this.save();
	}
	getTuiMode(): TuiMode {
		return this.settings.tuiMode === "regular" ? "regular" : SETTINGS_DEFAULTS.tuiMode;
	}
	setTuiMode(mode: TuiMode): void {
		this.globalSettings.tuiMode = mode;
		this.save();
	}
	getFullscreenExitOutput(): FullscreenExitOutput {
		return this.settings.fullscreenExitOutput === "resume-hint"
			? "resume-hint"
			: SETTINGS_DEFAULTS.fullscreenExitOutput;
	}
	setFullscreenExitOutput(output: FullscreenExitOutput): void {
		this.globalSettings.fullscreenExitOutput = output;
		this.save();
	}
	getFullscreenScrollbar(): ScrollViewScrollbar {
		const mode = this.settings.fullscreenScrollbar;
		return mode === "always" || mode === "hidden" ? mode : SETTINGS_DEFAULTS.fullscreenScrollbar;
	}
	setFullscreenScrollbar(mode: ScrollViewScrollbar): void {
		this.globalSettings.fullscreenScrollbar = mode;
		this.save();
	}
	getFullscreenCopyOnSelect(): boolean {
		return this.settings.fullscreenCopyOnSelect ?? SETTINGS_DEFAULTS.fullscreenCopyOnSelect;
	}
	setFullscreenCopyOnSelect(enabled: boolean): void {
		this.globalSettings.fullscreenCopyOnSelect = enabled;
		this.save();
	}
	getFullscreenWheelScrollLines(): WheelScrollLines {
		const lines = this.settings.fullscreenWheelScrollLines;
		return typeof lines === "number" && Number.isFinite(lines)
			? Math.max(1, Math.min(100, Math.floor(lines)))
			: SETTINGS_DEFAULTS.fullscreenWheelScrollLines;
	}
	setFullscreenWheelScrollLines(lines: WheelScrollLines): void {
		this.globalSettings.fullscreenWheelScrollLines =
			lines === "auto" ? lines : Math.max(1, Math.min(100, Math.floor(lines)));
		this.save();
	}
	getImageAutoResize(): boolean {
		return this.settings.images?.autoResize ?? SETTINGS_DEFAULTS.images.autoResize;
	}
	setImageAutoResize(enabled: boolean): void {
		throw new Error("Execution setting ImageAutoResize requires a supported backend configuration option");
	}
	getBlockImages(): boolean {
		return this.settings.images?.blockImages ?? SETTINGS_DEFAULTS.images.blockImages;
	}
	setBlockImages(blocked: boolean): void {
		throw new Error("Execution setting BlockImages requires a supported backend configuration option");
	}
	getEnabledModels(): string[] | undefined {
		return this.settings.enabledModels;
	}
	setEnabledModels(patterns: string[] | undefined): void {
		throw new Error("Execution setting EnabledModels requires a supported backend configuration option");
	}
	getDoubleEscapeAction(): "fork" | "tree" | "none" {
		return this.settings.doubleEscapeAction ?? SETTINGS_DEFAULTS.doubleEscapeAction;
	}
	setDoubleEscapeAction(action: "fork" | "tree" | "none"): void {
		this.globalSettings.doubleEscapeAction = action;
		this.save();
	}
	getTreeFilterMode(): "default" | "no-tools" | "user-only" | "labeled-only" | "all" {
		const mode = this.settings.treeFilterMode;
		const valid = ["default", "no-tools", "user-only", "labeled-only", "all"];
		return mode && valid.includes(mode) ? mode : SETTINGS_DEFAULTS.treeFilterMode;
	}
	setTreeFilterMode(mode: "default" | "no-tools" | "user-only" | "labeled-only" | "all"): void {
		this.globalSettings.treeFilterMode = mode;
		this.save();
	}
	getShowHardwareCursor(): boolean {
		return this.settings.showHardwareCursor ?? process.env.PI_HARDWARE_CURSOR === "1";
	}
	setShowHardwareCursor(enabled: boolean): void {
		this.globalSettings.showHardwareCursor = enabled;
		this.save();
	}
	getEditorPaddingX(): number {
		return this.settings.editorPaddingX ?? SETTINGS_DEFAULTS.editorPaddingX;
	}
	setEditorPaddingX(padding: number): void {
		this.globalSettings.editorPaddingX = Math.max(0, Math.min(3, Math.floor(padding)));
		this.save();
	}
	getOutputPad(): 0 | 1 {
		return this.settings.outputPad === 0 ? 0 : SETTINGS_DEFAULTS.outputPad;
	}
	setOutputPad(padding: 0 | 1): void {
		this.globalSettings.outputPad = padding;
		this.save();
	}
	getAutocompleteMaxVisible(): number {
		return this.settings.autocompleteMaxVisible ?? SETTINGS_DEFAULTS.autocompleteMaxVisible;
	}
	setAutocompleteMaxVisible(maxVisible: number): void {
		this.globalSettings.autocompleteMaxVisible = Math.max(3, Math.min(20, Math.floor(maxVisible)));
		this.save();
	}
	getCodeBlockIndent(): string {
		return this.settings.markdown?.codeBlockIndent ?? SETTINGS_DEFAULTS.markdown.codeBlockIndent;
	}
	getMermaidRenderingMode(): MermaidRenderingMode {
		const mode = this.settings.markdown?.mermaid;
		return mode === "off" || mode === "final" ? mode : SETTINGS_DEFAULTS.markdown.mermaid;
	}
	setMermaidRenderingMode(mode: MermaidRenderingMode): void {
		this.globalSettings.markdown ??= {};
		this.globalSettings.markdown.mermaid = mode;
		this.save();
	}
	getWarnings(): WarningSettings {
		return { ...(this.settings.warnings ?? {}) };
	}
	setWarnings(warnings: WarningSettings): void {
		this.globalSettings.warnings = { ...warnings };
		this.save();
	}
}

export type { ClientSettings as SettingsManager };
