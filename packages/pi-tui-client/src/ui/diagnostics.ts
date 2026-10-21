import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { getAgentDir } from "../config.ts";
export function takeUnnotifiedCrash(): { timestamp: number; message: string } | undefined {
	try {
		const file = join(getAgentDir(), "crash.log"),
			notified = join(getAgentDir(), "crash.notified");
		const lines = readFileSync(file, "utf8").trim().split("\n");
		const last = JSON.parse(lines.at(-1)!);
		let previous = 0;
		try {
			previous = Number(readFileSync(notified, "utf8"));
		} catch {
			/* First notification. */
		}
		if (last.timestamp <= previous) return;
		writeFileSync(notified, String(last.timestamp), { mode: 0o600 });
		return last;
	} catch {
		return;
	}
}
