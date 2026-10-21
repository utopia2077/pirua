import { cp, mkdir, chmod } from "node:fs/promises";
for (const directory of ["modes/interactive/theme", "modes/interactive/assets", "core/export-html"]) {
	await mkdir(`dist/${directory}`, { recursive: true });
	await cp(`src/${directory}`, `dist/${directory}`, { recursive: true, filter: (source) => !source.endsWith(".ts") });
}
await chmod("dist/main.js", 0o755);
