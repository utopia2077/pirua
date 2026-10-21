import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { homedir } from "node:os";
export const APP_NAME = "pirua-tui";
export const APP_TITLE = "Pirua · Pi TUI";
export const VERSION = "0.1.0";
export const CONFIG_DIR_NAME = ".pirua";
const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export function getAgentDir(): string {
	return process.env.PIRUA_UI_DATA_DIR ?? join(homedir(), ".local", "share", "pirua", "tui");
}
export function getThemesDir(): string {
	return join(packageRoot, "dist", "modes", "interactive", "theme");
}
export function getCustomThemesDir(): string {
	return join(getAgentDir(), "themes");
}
export function getAssetsDir(): string {
	return join(packageRoot, "dist", "modes", "interactive", "assets");
}
export function getExportTemplateDir(): string {
	return join(packageRoot, "dist", "core", "export-html");
}
export function getReadmePath(): string {
	return join(packageRoot, "README.md");
}
export function getDocsPath(): string {
	return getReadmePath();
}
export function getChangelogPath(): string {
	return join(packageRoot, "CHANGELOG.md");
}
export function getDebugLogPath(): string {
	return join(getAgentDir(), "debug.log");
}
export function getAuthPath(): string {
	return "the Rust host credential store";
}

export function getBundledInteractiveAssetPath(name: string): string {
	return join(getAssetsDir(), name);
}
