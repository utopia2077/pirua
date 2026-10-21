import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect } from "vitest";
import { ClientSettings } from "../src/ui/settings.ts";

describe("client settings ownership", () => {
	it("persists presentation preferences without overriding host execution settings", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pirua-ui-settings-"));
		const file = join(directory, "settings.json");
		try {
			await writeFile(
				file,
				JSON.stringify({
					theme: "light",
					steeringMode: "all",
					cacheWarming: "idle",
					images: { blockImages: true },
					modelThinkingLevels: { "fixture/model": "high" },
				}),
			);
			const settings = new ClientSettings(file);
			settings.setBackend({ steeringMode: "one-at-a-time", cacheWarming: "off", images: { blockImages: false } });
			expect(settings.getTheme()).toBe("light");
			expect(settings.getBlockImages()).toBe(false);
			expect(settings.getCacheWarmingMode()).toBe("off");
			expect(() => settings.setEnabledModels(["fixture/model"])).toThrow("backend");
			expect(() => settings.removeModelThinkingLevel("fixture", "model")).toThrow("backend");
			settings.setTheme("dark");
			settings.setShowImages(false);
			expect(JSON.parse(await readFile(file, "utf8"))).toEqual({ theme: "dark", terminal: { showImages: false } });
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
});
