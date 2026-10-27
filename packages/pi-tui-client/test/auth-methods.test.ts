import { PassThrough } from "node:stream";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { beforeAll, describe, expect, it } from "vitest";
import { AcpClient, type RecordValue } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { TuiSession } from "../src/session/facade.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import { FooterComponent } from "../src/modes/interactive/components/footer.ts";
import { initTheme } from "../src/modes/interactive/theme/theme.ts";
import { stripAnsi } from "../src/utils/ansi.ts";
import { snapshot } from "./fixtures.ts";

// An authentication method as the host advertises it, `_meta` included.
const method = (provider: string, kind: "oauth" | "apiKey", auth?: RecordValue) => ({
	type: "agent",
	methodId: `${provider}:${kind}`,
	name: `${provider} ${kind}`,
	...(auth ? { _meta: { "pirua.dev/auth": auth } } : {}),
});
const oauthStatus = { type: "oauth", source: "OAuth" };
const keyStatus = { type: "api_key", source: "FIXTURE_API_KEY" };

function harness(reply: (request: RecordValue) => RecordValue | undefined = () => undefined) {
	const input = new PassThrough(),
		output = new PassThrough();
	const requests: RecordValue[] = [];
	output.on("data", (data) => {
		for (const line of data.toString().split("\n").filter(Boolean)) {
			const request = JSON.parse(line);
			requests.push(request);
			const body = reply(request);
			if (body && request.id !== undefined)
				queueMicrotask(() => input.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, ...body }) + "\n"));
		}
	});
	const client = new AcpClient(input, output);
	const host = new TuiRuntimeHost(client, {
		cwd: "/workspace/project",
		settings: new ClientSettings(join(tmpdir(), "pirua-auth-unused.json")),
	});
	host.session = new TuiSession(host, snapshot());
	return { host, requests, dispose: () => (host.session.dispose(), client.close()) };
}
const footerText = (host: TuiRuntimeHost) =>
	new FooterComponent(host.session, {
		getGitBranch: () => "main",
		getExtensionStatuses: () => new Map<string, string>(),
		getAvailableProviderCount: () => 1,
		onBranchChange: () => () => {},
	})
		.render(120)
		.map(stripAnsi)
		.join("\n");

describe("authentication method metadata", () => {
	beforeAll(() => initTheme(undefined, false));

	it("reads the provider's subscription and configured status from the method metadata", () => {
		const h = harness();
		h.host.authMethods = [
			method("anthropic", "oauth", { subscription: true, status: oauthStatus }),
			method("anthropic", "apiKey", { subscription: true, status: oauthStatus }),
			method("deepseek", "apiKey", { subscription: false }),
			method("legacy", "oauth"),
		];
		expect(h.host.session.modelRuntime.getAuthOptions()).toEqual([
			expect.objectContaining({ id: "anthropic", authType: "oauth", subscription: true, status: oauthStatus }),
			expect.objectContaining({ id: "anthropic", authType: "api_key", subscription: true, status: oauthStatus }),
			expect.objectContaining({ id: "deepseek", authType: "api_key", subscription: false, status: undefined }),
			// A host that does not describe its methods leaves both unset, as before.
			expect.objectContaining({ id: "legacy", authType: "oauth", subscription: undefined, status: undefined }),
		]);
		h.dispose();
	});

	it("counts a subscription only when the configured credential is OAuth, like upstream isUsingSubscription", () => {
		const h = harness();
		const runtime = h.host.session.modelRuntime;
		h.host.authMethods = [
			method("oauth-sub", "oauth", { subscription: true, status: oauthStatus }),
			method("oauth-account", "oauth", { subscription: false, status: oauthStatus }),
			method("key-on-sub", "oauth", { subscription: true, status: keyStatus }),
			method("key-on-sub", "apiKey", { subscription: true, status: keyStatus }),
			method("unconfigured", "oauth", { subscription: true }),
			method("legacy", "oauth"),
		];
		expect(
			["oauth-sub", "oauth-account", "key-on-sub", "unconfigured", "legacy", "unknown"].map((id) =>
				runtime.isUsingSubscription(id),
			),
		).toEqual([true, false, false, false, false, false]);
		h.dispose();
	});

	it("replaces the host's methods from every snapshot that carries them, and keeps them for one that does not", async () => {
		const configured = [method("fixture", "oauth", { subscription: true, status: oauthStatus })];
		const h = harness((request) => {
			if (request.method === "_pirua/session/snapshot") return { result: snapshot({ authMethods: configured }) };
			if (request.method === "_pirua/session/reload") return { result: { snapshot: snapshot({ authMethods: [] }) } };
			if (request.method === "_pirua/session/label") return { result: { snapshot: snapshot() } };
		});
		try {
			h.host.features = new Set(["tree", "resources"]);
			// A session opened by /new, /resume or /fork starts from a snapshot that already carries the list.
			h.host.session.dispose();
			h.host.session = new TuiSession(h.host, snapshot({ authMethods: [method("fixture", "oauth")] }));
			expect(h.host.authMethods).toEqual([method("fixture", "oauth")]);
			h.host.authMethods = [method("fixture", "oauth", { subscription: true })];
			expect(footerText(h.host)).not.toContain("(sub)");
			await h.host.session.refresh();
			expect(h.host.authMethods).toEqual(configured);
			expect(footerText(h.host)).toContain("(sub)");
			// An older host sends no list: what is known stays.
			await h.host.session.setLabel("e1", "mark");
			expect(h.host.authMethods).toEqual(configured);
			// An empty list is an answer too.
			await h.host.session.reload();
			expect(h.host.authMethods).toEqual([]);
			expect(footerText(h.host)).not.toContain("(sub)");
		} finally {
			h.dispose();
		}
	});

	it("shows the configured state after login, and drops it after logout", async () => {
		let loggedIn = false;
		const h = harness((request) => {
			if (request.method === "auth/login") loggedIn = true;
			if (request.method === "auth/logout") loggedIn = false;
			if (request.method === "_pirua/session/snapshot")
				return {
					result: snapshot({
						authMethods: [
							method("fixture", "oauth", { subscription: true, ...(loggedIn ? { status: oauthStatus } : {}) }),
						],
					}),
				};
			if (request.method.startsWith("auth/")) return { result: {} };
		});
		try {
			h.host.authMethods = [method("fixture", "oauth", { subscription: true })];
			const runtime = h.host.session.modelRuntime;
			const interaction = { signal: new AbortController().signal, prompt: async () => "", notify: () => {} };
			await runtime.login("fixture", "oauth", interaction);
			expect(runtime.getAuthOptions()[0]).toMatchObject({ status: oauthStatus, subscription: true });
			expect(runtime.isUsingSubscription("fixture")).toBe(true);
			expect(footerText(h.host)).toContain("(sub)");
			await runtime.logout("fixture");
			expect(runtime.getAuthOptions()[0]!.status).toBeUndefined();
			expect(runtime.isUsingSubscription("fixture")).toBe(false);
			expect(footerText(h.host)).not.toContain("(sub)");
		} finally {
			h.dispose();
		}
	});
});
