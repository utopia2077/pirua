import { spawnSync } from "node:child_process";
export interface ToolStatus {
	type: "warning" | "info";
	message: string;
}
/** Resolve client-local completion helpers; backend tools are owned by Rust. */
export async function findUiTool(name: string, onStatus?: (status: ToolStatus) => void): Promise<string | null> {
	const result = spawnSync(process.platform === "win32" ? "where" : "which", [name], {
		encoding: "utf8",
		timeout: 2000,
		windowsHide: true,
	});
	if (result.status === 0) return result.stdout.trim().split(/\r?\n/)[0] || null;
	if (name === "fd")
		onStatus?.({
			type: "warning",
			message: "fd is unavailable; install fd for fast file completion. Rust coding tools remain available.",
		});
	return null;
}
