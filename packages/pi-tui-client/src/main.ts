#!/usr/bin/env node
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { AcpClient } from "./acp/client.ts";
import { TuiRuntimeHost } from "./session/host.ts";
import { ClientSettings } from "./ui/settings.ts";
import { InteractiveMode } from "./modes/interactive/interactive-mode.ts";
import { setThemeJsonValidator } from "./modes/interactive/theme/theme.ts";
import { validateThemeJson } from "./modes/interactive/theme/theme-schema.ts";
import { getAgentDir } from "./config.ts";

export interface LaunchOptions {
	server: string;
	serverArgs: string[];
	cwd: string;
	sessionId?: string;
	initialMessage?: string;
	tuiMode?: "regular" | "fullscreen";
}
export function parseArgs(args: string[]): LaunchOptions | undefined {
	const options: LaunchOptions = {
		server: process.env.PIRUA_ACP_SERVER ?? "pi-acp-server",
		serverArgs: [],
		cwd: process.cwd(),
	};
	for (let i = 0; i < args.length; i++) {
		const flag = args[i];
		const value = () => {
			const v = args[++i];
			if (v === undefined) throw new Error(`Missing value for ${flag}`);
			return v;
		};
		if (flag === "--help" || flag === "-h") {
			process.stdout.write(
				"pirua-tui [--server PATH] [--server-arg ARG] [--data-dir DIR] [--cwd DIR] [--session ID] [--prompt TEXT] [--regular|--fullscreen]\n\nThe server uses configured native credentials. Pass deterministic provider flags explicitly with --server-arg for local verification.\n",
			);
			return;
		}
		if (flag === "--server") options.server = value();
		else if (flag === "--server-arg") options.serverArgs.push(value());
		else if (flag === "--data-dir") options.serverArgs.push("--data-dir", value());
		else if (flag === "--cwd") options.cwd = value();
		else if (flag === "--session") options.sessionId = value();
		else if (flag === "--prompt") options.initialMessage = value();
		else if (flag === "--regular") options.tuiMode = "regular";
		else if (flag === "--fullscreen") options.tuiMode = "fullscreen";
		else throw new Error(`Unknown argument: ${flag}`);
	}
	return options;
}
export async function launch(options: LaunchOptions): Promise<void> {
	setThemeJsonValidator(validateThemeJson);
	const child = spawn(options.server, options.serverArgs, { cwd: options.cwd, stdio: ["pipe", "pipe", "pipe"] });
	const client = new AcpClient(child.stdout, child.stdin);
	let stderr = "";
	child.stderr.on("data", (data) => {
		stderr = (stderr + data.toString()).slice(-16000);
	});
	child.on("error", (error) => client.fail(new Error(`Cannot launch Rust host '${options.server}': ${error.message}`)));
	child.on("exit", (code, signal) =>
		client.fail(new Error(`Rust host exited (${signal ?? code})${stderr.trim() ? ": " + stderr.trim() : ""}`)),
	);
	const host = new TuiRuntimeHost(client, {
		cwd: options.cwd,
		settings: new ClientSettings(join(getAgentDir(), "settings.json")),
		child,
	});
	let ui: InteractiveMode | undefined;
	let off: () => void = () => {};
	try {
		await host.initialize(options.sessionId);
		ui = new InteractiveMode(host, { initialMessage: options.initialMessage, tuiMode: options.tuiMode });
		const failure = new Promise<never>((_resolve, reject) => {
			off = client.onFailure((error) => reject(error));
		});
		await Promise.race([ui.run(), failure]);
	} finally {
		off();
		ui?.stop();
		await host.dispose();
	}
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	try {
		const options = parseArgs(process.argv.slice(2));
		if (options) await launch(options);
	} catch (error) {
		process.stderr.write(`pirua-tui: ${error instanceof Error ? error.message : String(error)}\n`);
		process.exitCode = 1;
	}
}
