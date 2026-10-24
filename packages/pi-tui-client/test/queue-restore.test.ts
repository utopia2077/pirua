import { PassThrough } from "node:stream";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect } from "vitest";
import { AcpClient } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { snapshot } from "./fixtures.ts";

it("restores only host-confirmed queue removals and carries each removed image once", async () => {
	const directory = await mkdtemp(join(tmpdir(), "pirua-tui-queue-"));
	const input = new PassThrough(),
		output = new PassThrough();
	const requests: any[] = [];
	const image = { type: "image", data: "aW1hZ2U=", mimeType: "image/png" };
	const initial = snapshot({
		queue: {
			steering: [{ submissionId: "already-consumed", delivery: "steer", text: "stale mirror text", images: [] }],
			followUp: [],
		},
	});
	output.on("data", (line) => {
		const request = JSON.parse(line.toString());
		requests.push(request);
		let result: any;
		if (request.method === "_pirua/session/clear_queue")
			result = {
				removed: [
					{ submissionId: "actually-removed", delivery: "followUp", text: "returned receipt", images: [image] },
				],
			};
		else if (request.method === "_pirua/session/snapshot") result = snapshot();
		else if (request.method === "session/prompt") result = { messageId: "inserted" };
		else throw new Error("Unexpected method: " + request.method);
		queueMicrotask(() => input.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }) + "\n"));
	});
	const client = new AcpClient(input, output);
	const host = new TuiRuntimeHost(client, { cwd: directory, settings: new ClientSettings(join(directory, "ui.json")) });
	host.features.add("queue");
	host.capabilities = { session: { prompt: { image: true } } };
	host.session = new TuiSession(host, initial);
	try {
		const removed = await host.session.clearQueue();
		expect(removed.steering).toEqual([]);
		expect(removed.followUp).toEqual(["returned receipt"]);
		await host.session.refresh();
		await host.session.prompt("restored text");
		await host.session.prompt("next text");
		const prompts = requests.filter((r) => r.method === "session/prompt");
		expect(prompts[0].params.prompt).toEqual([{ type: "text", text: "restored text" }, image]);
		expect(prompts[1].params.prompt).toEqual([{ type: "text", text: "next text" }]);
	} finally {
		host.session.dispose();
		client.close();
		await rm(directory, { recursive: true, force: true });
	}
});
