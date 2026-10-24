import type { TuiRuntimeHost } from "./host.ts";
import type { Model, ModelsRefreshResult, AuthPrompt, AuthEvent } from "../display/types.ts";
import type { AuthSelectorProvider } from "../modes/interactive/components/oauth-selector.ts";
import { RpcError } from "../acp/client.ts";

export class ModelRuntime {
	readonly host: TuiRuntimeHost;
	constructor(host: TuiRuntimeHost) {
		this.host = host;
	}
	getAvailableSnapshot(): Model[] {
		return structuredClone(this.host.session.projection.value.models);
	}
	getModel(provider: string, id: string): Model | undefined {
		return this.getAvailableSnapshot().find((m) => m.provider === provider && m.id === id);
	}
	getError(): string | undefined {
		return (
			this.host.session.projection.value.resources?.diagnostics
				?.filter((d: any) => d.type === "error")
				.map((d: any) => d.message)
				.join("\n") || undefined
		);
	}
	async refresh(options: { signal?: AbortSignal } = {}): Promise<ModelsRefreshResult> {
		await this.host.session.refresh(options.signal);
		return { errors: new Map(), aborted: false };
	}
	getAuthOptions(): AuthSelectorProvider[] {
		return this.host.authMethods
			.filter((m) => m.type === "agent")
			.map((m) => {
				const parts = m.methodId.split(":");
				const authType = parts.at(-1)?.toLowerCase() === "oauth" ? "oauth" : "api_key";
				return {
					id: parts.slice(0, -1).join(":"),
					name: m.name,
					authType,
					method: { id: m.methodId, description: m.description },
					status: m._meta?.["pirua.dev/auth"]?.status,
				};
			});
	}
	async login(
		provider: string,
		method: "api_key" | "oauth",
		interaction: { signal: AbortSignal; prompt: (p: AuthPrompt) => Promise<string>; notify: (e: AuthEvent) => void },
		_options?: unknown,
	): Promise<void> {
		const selected = this.getAuthOptions().find((o) => o.id === provider && o.authType === method);
		if (!selected?.method?.id)
			throw new RpcError(-32050, "Rust host does not advertise that authentication method", { kind: "unsupported" });
		this.host.authInteraction = interaction;
		try {
			await this.host.client.request("auth/login", { methodId: selected.method.id }, interaction.signal);
			await this.host.session.refresh();
		} catch (error) {
			if (interaction.signal.aborted) throw new Error("Login cancelled", { cause: error });
			throw error;
		} finally {
			this.host.authInteraction = undefined;
		}
	}
	async logout(_provider: string, options: { signal?: AbortSignal } = {}): Promise<void> {
		if (!this.host.authMethods.length)
			throw new RpcError(-32050, "Rust host does not advertise logout", { kind: "unsupported" });
		await this.host.client.request("auth/logout", {}, options.signal);
		await this.host.session.refresh();
	}
	isUsingSubscription(provider: string): boolean {
		return !!this.getAuthOptions().find((o) => o.id === provider && o.subscription && o.status);
	}
	isUsingOAuth(provider: string): boolean {
		return this.getAuthOptions().some((o) => o.id === provider && o.status?.type === "oauth");
	}
}
