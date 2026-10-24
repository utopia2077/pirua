import type { PiSnapshot } from "../acp/types.ts";
import type { ResourceDiagnostic } from "../display/types.ts";
export class ResourceView {
	readonly snapshot: () => PiSnapshot;
	constructor(snapshot: () => PiSnapshot) {
		this.snapshot = snapshot;
	}
	getDiagnostics(): ResourceDiagnostic[] {
		return structuredClone(this.snapshot().resources?.diagnostics ?? []);
	}
	getSkills(): { skills: any[]; diagnostics: ResourceDiagnostic[] } {
		return { skills: structuredClone(this.snapshot().resources?.skills ?? []), diagnostics: [] };
	}
	getPrompts(): { prompts: any[]; diagnostics: ResourceDiagnostic[] } {
		return { prompts: structuredClone(this.snapshot().resources?.promptTemplates ?? []), diagnostics: [] };
	}
	getThemes(): { themes: any[]; diagnostics: ResourceDiagnostic[] } {
		return { themes: [], diagnostics: [] };
	}
	getAgentsFiles(): { agentsFiles: any[] } {
		return { agentsFiles: structuredClone(this.snapshot().resources?.contextFiles ?? []) };
	}
	getSystemPromptSource(): undefined {
		return undefined;
	}
	getAppendSystemPromptSources(): never[] {
		return [];
	}
	getExtensions(): { extensions: any[]; errors: any[]; warnings: any[]; diagnostics: any[] } {
		return { extensions: [], errors: [], warnings: [], diagnostics: this.getDiagnostics() };
	}
}
