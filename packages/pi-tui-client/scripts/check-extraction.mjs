import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const packageRoot = join(root, "packages/pi-tui-client");
const manifest = JSON.parse(readFileSync(join(packageRoot, "upstream-source.json"), "utf8"));
assert.equal(manifest.commit, "4a080252f06caf63a525d7873c5d94d3c23887e0");
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
for (const file of manifest.files) {
	assert.equal(
		hash(readFileSync(join(root, file.destination))),
		file.outputSha256,
		`Adapted source changed without provenance update: ${file.destination}`,
	);
}
const config = ts.readConfigFile(join(packageRoot, "tsconfig.json"), ts.sys.readFile);
const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, packageRoot);
const program = ts.createProgram(parsed.fileNames, parsed.options);
const forbidden =
	/@earendil-works\/(?:pi-ai|pi-agent-core)|(?:^|\/)(?:agent-session(?:-runtime)?|session-manager|settings-manager|auth-storage|resource-loader|extension-loader|extension-runner)(?:\.ts)?$/;
let files = 0;
for (const source of program.getSourceFiles()) {
	if (!source.fileName.startsWith(join(packageRoot, "src"))) continue;
	files++;
	for (const statement of source.statements) {
		if (
			(ts.isImportDeclaration(statement) || ts.isExportDeclaration(statement)) &&
			statement.moduleSpecifier &&
			ts.isStringLiteral(statement.moduleSpecifier)
		) {
			const specifier = statement.moduleSpecifier.text;
			assert(!forbidden.test(specifier), `Backend import: ${specifier}`);
			if (specifier.startsWith("."))
				assert(
					existsSync(resolve(dirname(source.fileName), specifier)),
					`Unresolved local presentation dependency: ${specifier}`,
				);
		}
	}
}
for (const file of [
	"modes/interactive/theme/dark.json",
	"modes/interactive/theme/light.json",
	"core/export-html/template.html",
	"core/export-html/template.js",
	"core/export-html/template.css",
	"core/export-html/vendor/marked.min.js",
]) {
	assert(existsSync(join(packageRoot, "dist", file)), `Missing built asset: ${file}`);
}
const pkg = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
assert(!Object.keys(pkg.dependencies).some((dependency) => /pi-ai|pi-agent-core|coding-agent/.test(dependency)));
console.log(
	`Verified ${manifest.files.length} pinned source records, ${files} presentation modules, backend import boundary and built assets.`,
);
